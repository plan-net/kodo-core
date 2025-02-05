import sys
from typing import Tuple, Union
from bson.objectid import ObjectId
import platform
import ray
from ray.util.queue import Queue
import os.path
from subprocess import Popen, DEVNULL
from litestar.datastructures import State

import debugpy
from kodo.datatypes import LaunchResult
from kodo.common import Launch


WINDOWS = ("Scripts", "python.exe")
LINUX = ("bin", "python")

RAY_NAMESPACE = "kodo"
RAY_ENV = {"env_vars": {"RAY_DEBUG": "1"}}


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
        self.fid = None
        self.server = None
        self.executable = None
        self.module = None
        self.flow = None
        self.inputs = None
        self.executable = None
        self.cwd = None
        self.events = Queue()
        self.body = []
        self.payload = None

    def initialise(
            self, 
            fid: str, 
            executable: str, 
            cwd: str,
            module: str,
            flow: str,
            inputs: dict) -> None:
        self.fid = fid
        self.executable = executable
        self.cwd = cwd
        self.module = module
        self.flow = flow
        self.inputs = inputs

    def get_fid(self) -> str:
        return self.fid

    def get_executable(self) -> str:
        return self.executable

    def get_cwd(self) -> str:
        return self.cwd

    def get_module(self) -> str:
        return self.module

    def get_flow(self) -> str:
        return self.flow

    def get_inputs(self) -> dict:
        return self.inputs
    
    def add_body(self, body: str) -> None:
        self.body.append(body)

    def get_body(self) -> str:
        return "\n".join(self.body)
    
    def add_launch(self, instance: Launch) -> None:
        self.payload = {"args": instance.args, "inputs": instance.inputs}

    def get_launch(self) -> Union[dict, None]:
        return self.payload

    def get_success(self) -> bool:
        return True

    # def enqueue(self, key, value):
    #     self.events.put({key: value})

    # def dequeue(self):
    #     if self.events.empty():
    #         return None
    #     return self.events.get(block=True, timeout=0.1)
    
    # def count(self):
    #     return self.events.qsize()

    def ready(self):
        return True


def remote_process(server: str, actor_name: str):
    
    import time
    time.sleep(0.5)




@ray.remote
def enter(server: str, actor: LaunchStream) -> LaunchResult:
    if not debugpy.is_client_connected():
        debugpy.listen(("localhost", 63255))
    debugpy.wait_for_client() 
    proc = Popen([
        ray.get(actor.get_executable.remote()),  # type: ignore
        "-m", "kodo.remote.enter",
        server, 
        ray.get(actor.get_fid.remote())],  # type: ignore
        stdout=DEVNULL, stderr=DEVNULL,
        cwd=ray.get(actor.get_cwd.remote()))  # type: ignore
    proc.wait()
    body = ray.get(actor.get_body.remote())  # type: ignore
    payload = ray.get(actor.get_launch.remote())  # type: ignore
    fid = ray.get(actor.get_fid.remote())  # type: ignore
    return LaunchResult(
        fid=fid if payload is not None else None,  # type: ignore
        is_launch=True if payload is not None else False,
        payload=payload if payload else body,  # type: ignore
        success=proc.returncode == 0)


async def launch(state: State, entry_point: str, inputs: dict) -> LaunchResult:
    fid = str(ObjectId())
    actor = LaunchStream.options(name=fid).remote()  # type: ignore
    ray.get(actor.ready.remote())
    executable, cwd, module, flow = parse_factory(state, entry_point)
    actor.initialise.remote(
        fid=fid, executable=executable, cwd=cwd, module=module, flow=flow, 
        inputs=inputs)
    meth = enter.remote(state.ray_server, actor)
    unready = [meth]
    while unready:
        ready, unready = ray.wait(unready, timeout=0.25)
        ret = ray.get(ready)
    return ret[0]



