import debugpy
import sys
import ray
import os
from subprocess import Popen, PIPE, DEVNULL
from ray.util.queue import Queue
from typing import Any
import time
from kodo.remote.launcher import (RAY_NAMESPACE, RAY_ENV, VENV_MODULE, 
                                  COMPLETED_STATE, STOPPING_STATE, ERROR_STATE, 
                                  RUNNING_STATE, STARTING_STATE, BREAK_STATE, 
                                  RETURNING_STATE, BOOTING_STATE, EXEC_MODULE)
from kodo.remote.result import ExecutionResult
from kodo import helper


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
    # This is the process running remote
    # if not debugpy.is_client_connected():
    #     debugpy.listen(("localhost", 63256))
    # debugpy.wait_for_client() 
    # execute.remote ownership is:
    ray.get(actor.enqueue.remote(status=STARTING_STATE))  # type: ignore
    data: dict = ray.get(actor.get_data.remote())  # type: ignore
    # executable = data["environment"]["executable"]
    # server = data["server"]
    # fid = data["launch"].fid
    # cwd = data["environment"]["cwd"]
    # os.system(f"cd {cwd}; {executable} -m {VENV_MODULE} execute {server} {fid} 1>/Users/raum/temp/1.out 2>/Users/raum/temp/2.out")
    proc = Popen([
        data["environment"]["executable"], "-m", VENV_MODULE, "execute", 
        data["server"], data["launch"].fid], stdout=PIPE, stderr=sys.stdout, 
        cwd=data["environment"]["cwd"])
    if proc.stdout:
        for line in proc.stdout:
            actor.enqueue.remote(  # type: ignore
                stdout=line.decode("utf-8").rstrip())
    # if proc.stderr:
    #     for line in proc.stderr:
    #         actor.enqueue.remote(  # type: ignore
    #             stderr=line.decode("utf-8").rstrip())
    ret = proc.wait()
    print(ret)
    ray.get(actor.enqueue.remote(status=STOPPING_STATE))  # type: ignore
    return True


def main(server: str, exec_path: str) -> None:
    # This is the detached, isolated processing running on the node
    ray.init(address=server, ignore_reinit_error=True, namespace=RAY_NAMESPACE,
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
    ray.get(actor.enqueue.remote(status=BOOTING_STATE))  # type: ignore
    unready_exec = [execute.remote(actor, actor_name)]
    ret_value = None
    done = False
    try:
        while True:
            if unready_exec:
                ready_exec, unready_exec = ray.wait(unready_exec, timeout=0.001)
                if ready_exec and ret_value is None:
                    ret_value = ray.get(ready_exec)
                    result.write({"status": RETURNING_STATE})
            elif not done:
                result.write({"status": BREAK_STATE})
                done = True

            q = ray.get(actor.dequeue.remote())
            if q:
                result.write(q)
            elif ret_value:
                result.write({"status": BREAK_STATE})
                break

            # ready_queue, _ = ray.wait([actor.dequeue.remote()], timeout=0.001)
            # if ready_queue:
            #     queue_value = ray.get(ready_queue)
            #     leave = False
            #     for qv in queue_value:
            #         if qv:
            #            result.write(qv)
            #         else:
            #             leave = True
            #             #break
            #     if leave and done:
            #         break

                # if "status" in q:
                #     status = q.get("status", "")
                #     result.write({"status": status})
                # else:
                #     result.write({"body": q})
    except Exception as exc:
        result.write({"status": ERROR_STATE})
        result.write({"error": f"{exc.__class__.__name__}: {exc}"})
    else:
        result.write({"status": COMPLETED_STATE})
    finally:
        ray.shutdown()
        result.close_write()
    # flow: Any = flow_factory(data["module"], data["flow"])
    # callback = flow.get_register("enter") 
    # ret = callback(data["inputs"])
    # if isinstance(ret, Launch):
    #     ray.get(actor.set_launch.remote(ret))
    # else:
    #     ray.get(actor.set_body.remote(ret))


if __name__ == "__main__":
    main(*sys.argv[1:3])
