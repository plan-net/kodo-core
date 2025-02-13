import logging
import os.path
import platform
import sys
from pathlib import Path
from subprocess import DEVNULL, Popen
from typing import Any, Dict, Tuple, Union

import aiofiles
import pydantic
import ray
from bson.objectid import ObjectId
from litestar.datastructures import State

import kodo.core
from kodo.adapter import now, Flow, Launch
from kodo.datatypes import DynamicModel, LaunchResult
from kodo.log import LOG_FORMAT, logger


WINDOWS = ("Scripts", "python.exe")
LINUX = ("bin", "python")

VENV_MODULE = "kodo.remote.enter"
EXEC_MODULE = "kodo.remote.executor"
EVENT_LOG = "event.log"
STDOUT_FILE = "stdout.log"
STDERR_FILE = "stderr.log"
STOP_FILE = "_done_"
KILL_FILE = "_killed_"

# set by node service with request to launch
PENDING_STATE = "pending"  
# set by detached process on the node service
BOOTING_STATE = "booting"  
# set by remote process controlled by detached process
STARTING_STATE = "starting"  
# set by sub process controlled by remote process
RUNNING_STATE = "running"  
# set by remote process controlled by detached process
STOPPING_STATE = "stopping"  
# set by detached process on the node service when remote returns
RETURNING_STATE = "returning"  
# set by detached process on the node service when the event queue is empty
BREAK_STATE = "break"  
# set by detached process on the node service on success
COMPLETED_STATE = "completed"  
# set by detached process on the node service on failure
ERROR_STATE = "error"  
KILLED_STATE = "killed"
DIED_STATE = "died"
INITIAL_STATE = (PENDING_STATE, BOOTING_STATE)
FINAL_STATE = (ERROR_STATE, COMPLETED_STATE, KILLED_STATE, DIED_STATE)


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
        self._error = None

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

    def set_body(self, body: str) -> None:
        self._body = body

    def get_body(self) -> str:
        return self._body
    
    def set_error(self, message: str) -> None:
        self._error = message

    def get_error(self) -> str:
        return self._error
    
    def set_launch(self, instance: Launch) -> None:
        self._launch = {
            "args": instance.args, 
            "inputs": instance.inputs
        }

    def get_launch(self) -> Union[dict, None]:
        return self._launch


def ev_format(value: Union[pydantic.BaseModel, Dict[str, Any]]) -> str:
    if isinstance(value, pydantic.BaseModel):
        ret = value.model_dump_json()
    elif isinstance(value, dict):
        ret = DynamicModel(value).model_dump_json()
    else:
        ret = f"invalid type {type(value)}: {value}"
    return f"{now().isoformat()} {ret}\n"


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
    await ev_write(fh, {"version": {
        "core": kodo.core.__version__,
        "common": kodo.common.__version__
    }})
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
    t0 = now()
    ray.init(
        address=state.ray_server, 
        ignore_reinit_error=True,
        namespace=kodo.adapter.RAY_NAMESPACE,
        configure_logging=True,
        logging_level=logging.ERROR,
        logging_format=LOG_FORMAT,
        log_to_driver=True,
        runtime_env=kodo.adapter.RAY_ENV
    )
    fid = str(ObjectId())
    logger.info(f"booting {flow.url}, fid: {fid}")
    actor = LaunchStream.options(name=f"{fid}.launch").remote()  # type: ignore
    ray.get(actor.ready.remote())
    executable, cwd, module, flow_name = parse_factory(state, str(flow.entry))
    ray.get(actor.initialise.remote(
        fid=fid, 
        executable=executable, 
        cwd=cwd, 
        module=module, 
        flow=flow_name, 
        inputs=inputs))
    proc = Popen(
        [executable, "-m", VENV_MODULE, "enter", state.ray_server, fid], 
        stdout=DEVNULL, stderr=DEVNULL, cwd=cwd)
    proc.wait()
    if proc.returncode != 0:
        error = ray.get(actor.get_error.remote())
        logger.error(f"returncode {proc.returncode}: {error}")
    ret: dict = ray.get(actor.get_data.remote())  # type: ignore
    result = LaunchResult(
        fid=ret["fid"], 
        payload=ret["payload"], 
        is_launch=ret["is_launch"], 
        success=proc.returncode == 0)
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
        proc = Popen(
            [sys.executable, "-m", EXEC_MODULE, state.ray_server, exec_path],
            stdout=DEVNULL, stderr=DEVNULL, start_new_session=True)
        logger.info(
            f"launched {flow.url}, fid: {fid}, pid: {proc.pid}"
            f"in {now() - t0}")
    else:
        logger.info(
            f"entered {flow.url} with fid={fid} after {now() - t0}")
    ray.shutdown()
    return result
