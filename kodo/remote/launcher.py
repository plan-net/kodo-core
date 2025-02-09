import os
import sys
from typing import Tuple, Union, Any, Dict
from bson.objectid import ObjectId
import pydantic
import platform
import ray
from ray.util.queue import Queue
from asyncio.subprocess import create_subprocess_exec
import os.path
import aiofiles
from subprocess import Popen, DEVNULL
from litestar.datastructures import State
from pathlib import Path
import logging
import debugpy
from kodo.datatypes import LaunchResult, Flow, DynamicModel
from kodo.common import Launch
from kodo import helper
import kodo
from kodo.log import logger, LOG_FORMAT


WINDOWS = ("Scripts", "python.exe")
LINUX = ("bin", "python")

VENV_MODULE = "kodo.remote.enter"
EXEC_MODULE = "kodo.remote.executor"
RAY_NAMESPACE = "kodo"
if os.environ.get("DEBUGPY_RUNNING", False):
    RAY_ENV = {"env_vars": {"RAY_DEBUG": "1"}} 
else:
    RAY_ENV = {}
EVENT_LOG = "event.log"
STDOUT_FILE = "stdout.log"
STDERR_FILE = "stderr.log"
STOP_FILE = "_done_"
KILL_FILE = "_killed_"

PENDING_STATE = "pending"  # set by node service with request to launch
BOOTING_STATE = "booting"  # set by detached process on the node service
STARTING_STATE = "starting"  # set by remote process controlled by detached process
RUNNING_STATE = "running"  # set by sub process controlled by remote process
STOPPING_STATE = "stopping"  # set by remote process controlled by detached process
RETURNING_STATE = "returning"  # set by detached process on the node service when remote returns
BREAK_STATE = "break"  # set by detached process on the node service when the event queue is empty
COMPLETED_STATE = "completed"  # set by detached process on the node service on success
ERROR_STATE = "error"  # set by detached process on the node service on failure

FINAL_STATE = (ERROR_STATE, COMPLETED_STATE)


def parse_factory(state: State, entry_point: str) -> Tuple[str, str, str, str]:
    if entry_point.count(":") == 2:
        venv, module, flow = entry_point.split(":", 2)
        cwd = os.path.abspath(os.path.join(state.env_home, venv))
        if platform.system() == "Windows":
            py = WINDOWS
        else:
            py = LINUX
        env = os.path.join(cwd, state.venv_dir, *py)
    else:
        module, flow = entry_point.split(":", 1)
        cwd = os.path.abspath(".")
        env = sys.executable
    return env, cwd, module, flow


@ray.remote
class LaunchStream:
    
    def __init__(self):
        self._data = {}
        self._body = None
        self._launch = None
        self._debug = []

    def ready(self):
        return True

    def initialise(
            self, 
            fid: str, 
            executable: str, 
            cwd: str,
            module: str,
            flow: str,
            inputs: dict) -> None:
        # if not debugpy.is_client_connected():
        #     debugpy.listen(("localhost", 63257))
        # debugpy.wait_for_client() 
        self._data = {
            "fid": fid,
            "executable": executable,
            "cwd": cwd,
            "module": module,
            "flow": flow,
            "inputs": inputs,
        }

    def get_data(self) -> dict:
        launch = self.get_launch()
        is_launch = launch is not None
        return {**self._data, **{
            "payload": launch if is_launch else self.get_body(),
            "is_launch": is_launch
        }}

    def add_debug(self, debug: str) -> None:
        self._debug.append(debug)

    def get_debug(self) -> list:
        return self._debug
    
    def set_body(self, body: str) -> None:
        self._body = body

    def get_body(self) -> str:
        return self._body
    
    def set_launch(self, instance: Launch) -> None:
        self._launch = {
            "args": instance.args, 
            "inputs": instance.inputs
        }

    def get_launch(self) -> Union[dict, None]:
        return self._launch


# @ray.remote
# def enter(server: str, actor: LaunchStream) -> LaunchResult:
#     # if not debugpy.is_client_connected():
#     #     debugpy.listen(("localhost", 63255))
#     # debugpy.wait_for_client() 
#     data: dict = ray.get(actor.get_data.remote())  # type: ignore
#     proc = Popen([
#         data["executable"], "-m", VENV_MODULE, "enter", server, data["fid"]], 
#         stdout=DEVNULL, stderr=DEVNULL, cwd=data["cwd"])
#     proc.wait()
#     ret: dict = ray.get(actor.get_data.remote())  # type: ignore
#     return LaunchResult(
#         fid=ret["fid"], 
#         payload=ret["payload"], 
#         is_launch=ret["is_launch"], 
#         success=proc.returncode == 0)

def ev_format(value: Union[pydantic.BaseModel, Dict[str, Any]]) -> str:
    if isinstance(value, pydantic.BaseModel):
        ret = value.model_dump_json()
    elif isinstance(value, dict):
        ret = DynamicModel(value).model_dump_json()
    else:
        ret = f"invalid type {type(value)}: {value}"
    return f"{helper.now().isoformat()} {ret}\n"


async def ev_write(
        f, value: Union[pydantic.BaseModel, Dict[str, Any]]) -> None:
    await f.write(ev_format(value))


async def _create_event_data(
        state: State, 
        flow: Flow, 
        result: LaunchResult,
        executable:str,
        cwd:str,
        module:str,
        flow_name:str) -> str:
    assert result.fid
    event_path = Path(state.exec_data).joinpath(result.fid)
    event_path.mkdir(exist_ok=True, parents=False)
    event_log = event_path.joinpath(EVENT_LOG)
    fh = await aiofiles.open(event_log, "w")
    await ev_write(fh, {"version": kodo.__version__})
    await ev_write(fh, {"flow": flow})
    await ev_write(fh, {"launch": result})
    await ev_write(fh, {"environment": {
        "executable": executable,
        "cwd": cwd,
        "module": module,
        "flow_name": flow_name,
    }})
    await ev_write(fh, {"status": PENDING_STATE})
    await fh.close()
    return str(event_path)


async def launch(state: State, flow: Flow, inputs: dict) -> LaunchResult:
    t0 = helper.now()
    ray.init(
        address=state.ray_server, 
        ignore_reinit_error=True,
        namespace=RAY_NAMESPACE,
        configure_logging=True,
        logging_level=logging.getLevelName(logger.level),
        logging_format=LOG_FORMAT,
        log_to_driver=True,
        runtime_env=RAY_ENV
    )
    t1 = helper.now()
    logger.info(f"ray init: {t1 - t0}")
    fid = str(ObjectId())
    logger.info(f"booting {flow.url},  fid: {fid}")
    actor = LaunchStream.options(name=f"{fid}.launch").remote()  # type: ignore
    t2 = helper.now()
    logger.info(f"actor init: {t2 - t0}")
    ray.get(actor.ready.remote())
    executable, cwd, module, flow_name = parse_factory(state, str(flow.entry))
    ray.get(actor.initialise.remote(
        fid=fid, executable=executable, cwd=cwd, module=module, flow=flow_name, 
        inputs=inputs))
    t3 = helper.now()
    logger.info(f"actor ready: {t3 - t2}")
    #meth = enter.remote(state.ray_server, actor)
    proc = Popen([
        executable, "-m", VENV_MODULE, "enter", state.ray_server, fid], 
        stdout=DEVNULL, stderr=DEVNULL, cwd=cwd)
    proc.wait()
    t4 = helper.now()
    logger.info(f"proc done: {t4 - t3}")
    ret: dict = ray.get(actor.get_data.remote())  # type: ignore
    result = LaunchResult(
        fid=ret["fid"], 
        payload=ret["payload"], 
        is_launch=ret["is_launch"], 
        success=proc.returncode == 0)
    t5 = helper.now()
    logger.info(f"shutdown: {t5 - t4}")
    # result = await meth
    if result.is_launch:
        exec_path = await _create_event_data(
            state=state, 
            flow=flow, 
            result=result,
            executable=executable,
            cwd=cwd,
            module=module,
            flow_name=flow_name)
        # start and detach
        proc = Popen([
            sys.executable, "-m", EXEC_MODULE, state.ray_server, exec_path], 
            stdout=DEVNULL, stderr=DEVNULL)
        logger.info(
            f"launched {flow.url}, fid: {fid}, pid={proc.pid}"
            f"in {helper.now() - t0}")
    else:
        logger.info(
            f"entered {flow.url} with fid={fid} after {helper.now() - t0}")
    out = ray.get(actor.get_debug.remote())
    logger.info(f"returning: {out}")
    ray.shutdown()
    return result
