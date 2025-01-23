import os
import time
import traceback
from multiprocessing import Process
from multiprocessing import Queue as M_Queue
from pathlib import Path
from typing import Union
import sys
import ray
from ray.util.queue import Queue as R_Queue
import httpx

import kodo.error
from kodo import helper
from kodo.datatypes import InternalOption, WorkerMode
from kodo.worker.base import FlowProcess
from kodo.worker.flow import flow_factory
from kodo.log import logger


def _execute(event_stream_file: Path, event):
    try:
        event.put(("data", {"status": "running"}))
        flow = flow_factory(event_stream_file, event)
        flow.instrument()
        result = flow.run()  # type: ignore
        event.put(("data", {"status": "stopping"}))
        flow.finish(result)
    except Exception as exc:
        event.put(("error", {"exception": traceback.format_exc()}))


@ray.remote
def execute_ray(*args, **kwargs):
    # import debugpy
    # debugpy.listen(("localhost", 5678))
    # debugpy.wait_for_client() 
    _execute(*args, **kwargs)


def execute_process(*args, **kwargs):
    _execute(*args, **kwargs)


class FlowExecution(FlowProcess):

    def communicate(self, mode: Union[WorkerMode, str]) -> None:
        option = InternalOption()
        t0 = helper.now()
        status = None
        self._ev_write("data", 
            dict(
                status="booting", 
                pid=os.getpid(), 
                ppid=os.getppid(),
                executor="ray" if option.RAY else "thread"
            ))
        success = self.run_ray() if option.RAY else self.run_process()
        if success:
            status = "finished"
        else:
            status = "error"
        t1 = helper.now()
        self._ev_write("data", 
            dict(status=status, runtime=(t1 - t0).total_seconds()))

    def run_ray(self) -> bool:
        try:
            resp = httpx.get("http://localhost:8265")
        except Exception as exc:
            self._ev_write("error", {"exception": traceback.format_exc()})
            return False
        else:
            ray.init(address="auto")
        event_queue: R_Queue = R_Queue()
        task = execute_ray.remote(self.event_log, event_queue)
        ctx = ray.get_runtime_context()
        self._ev_write("data", {
            "job_id": ctx.get_job_id(),
            "node_id": ctx.get_node_id()
        })
        success = True
        while True:
            done, _ = ray.wait([task], timeout=1)
            while not event_queue.empty():
                kind, data = event_queue.get()
                if kind == "error":
                    success = False
                self._ev_write(kind, data)
            if done:
                result = ray.get(done[0])
                break
        return success
    
    def run_process(self) -> bool:
        event_queue: M_Queue = M_Queue()
        thread = Process(
            target=execute_process, 
            args=(self.event_log, event_queue))
        thread.start()
        success = True
        while thread.is_alive() or not event_queue.empty():
            while not event_queue.empty():
                kind, data = event_queue.get()
                if kind == "error":
                    success = False
                self._ev_write(kind, data)
            time.sleep(0.5)
        thread.join()
        return success
