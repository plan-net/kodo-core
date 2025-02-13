import os
import sys
from subprocess import PIPE, Popen

import ray
from ray.util.queue import Queue

from kodo import helper
from kodo.remote.launcher import (BOOTING_STATE, BREAK_STATE, COMPLETED_STATE,
                                  ERROR_STATE, RAY_ENV, RAY_NAMESPACE,
                                  RETURNING_STATE, STARTING_STATE,
                                  STOPPING_STATE, VENV_MODULE)
from kodo.remote.result import ExecutionResult


@ray.remote
class EventStream:
    def __init__(self):
        self._events = Queue()
        self._body = None
        self._launch = None
        self._data = {}

    def initialise(
            self, 
            server: str, 
            exec_path: str, 
            launch: dict, 
            environment: dict):
        self._data = {
            "server": server,
            "exec_path": exec_path,
            "launch": launch,
            "environment": environment
        }

    def get_data(self):
        return self._data

    def ready(self):
        return True

    def enqueue(self, **kw):
        for k, v in kw.items():
            self._events.put({k: v})

    def dequeue(self):
        if self._events.empty():
            return None
        return self._events.get(block=True)
    
    def count(self):
        return self._events.qsize()


@ray.remote
def execute(actor: EventStream, actor_name: str):
    # import debugpy
    # if not debugpy.is_client_connected():
    #     debugpy.listen(("localhost", 63255))
    #     debugpy.wait_for_client() 
    ray.get(actor.enqueue.remote(status=STARTING_STATE))  # type: ignore
    data: dict = ray.get(actor.get_data.remote())  # type: ignore
    executable = data["environment"]["executable"]
    server = data["server"]
    fid = data["launch"].fid
    cwd = data["environment"]["cwd"]
    proc = Popen(
        [executable, "-m", VENV_MODULE, "execute", server, fid], 
        stdout=PIPE, stderr=sys.stdout, cwd=cwd)
    if proc.stdout:
        for line in proc.stdout:
            actor.enqueue.remote(  # type: ignore
                stdout=line.decode("utf-8").rstrip())
    ret = proc.wait()
    ray.get(actor.enqueue.remote(returncode=ret))  # type: ignore
    ray.get(actor.enqueue.remote(status=STOPPING_STATE))  # type: ignore
    return True


def main(debug: bool, server: str, exec_path: str) -> None:
    # This is the detached, isolated processing running on the node
    ray.init(address=server, 
             ignore_reinit_error=True, 
             namespace=RAY_NAMESPACE,
             runtime_env=RAY_ENV)
    result = ExecutionResult(exec_path)    
    result.read()
    result.open_write()
    actor_name = f"{result.launch.fid}.exec"
    actor = EventStream.options(name=actor_name).remote()  # type: ignore
    ray.get(actor.ready.remote())
    actor.initialise.remote(
        server=server, 
        exec_path=exec_path,
        launch=result.launch, 
        environment=result.environment)
    if debug:
        ray.get(actor.enqueue.remote(debug="running in debug mode"))  # type: ignore
    ray.get(actor.enqueue.remote(status=BOOTING_STATE))  # type: ignore
    ctx = ray.get_runtime_context()
    ray.get(actor.enqueue.remote(driver={
        "node_id": ctx.get_node_id(),
        "job_id": ctx.get_job_id(),
        "pid": os.getpid(),
        "ppid": os.getppid()
    }))
    unready = [execute.remote(actor, actor_name)]
    ret_value = None
    try:
        while True:
            if unready:
                ready, unready = ray.wait(unready, timeout=0.001)
                if ready and ret_value is None:
                    ret_value = ray.get(ready)
                    result.write({"status": RETURNING_STATE})
            dequeue = ray.get(actor.dequeue.remote())
            if dequeue:
                result.write(dequeue)
                if list(dequeue.keys())[0] == "error":
                    raise RuntimeError("execute failed")
            elif ret_value:
                result.write({"status": BREAK_STATE})
                break
    except RuntimeError as exc:
        result.write({"status": ERROR_STATE})
    except Exception as exc:
        result.write({"error": f"{exc.__class__.__name__}: {exc}"})
        result.write({"status": ERROR_STATE})
    else:
        result.write({"status": COMPLETED_STATE})
    finally:
        ray.shutdown()
        result.close_write()


if __name__ == "__main__":
    if helper.is_debug():
        main(True, *sys.argv[1:3])
        sys.exit(0)
    else:
        pid = os.fork()
        if pid > 0:
            os.waitpid(pid, 0) 
            sys.exit(0)
        os.setsid()
        pid = os.fork()
        if pid > 0:
            os.waitpid(pid, 0)
            os._exit(0)
        main(False, *sys.argv[1:3])
