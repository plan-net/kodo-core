import os
import time
import traceback
from multiprocessing import Process
from multiprocessing import Queue as M_Queue
from pathlib import Path
from typing import Union

import ray
from ray.util.queue import Queue as R_Queue

import kodo.error
from kodo import helper
from kodo.datatypes import InternalOption, WorkerMode
from kodo.worker.base import FlowProcess
from kodo.worker.flow import flow_factory

runtime_env = {"pip": ["emoji"]}

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

@ray.remote(runtime_env=runtime_env)
def execute_ray(*args, **kwargs):
    _execute(*args, **kwargs)

def execute_process(*args, **kwargs):
    # debugpy.listen(("localhost", 5678))
    # debugpy.wait_for_client() 
    _execute(*args, **kwargs)


class FlowExecution(FlowProcess):

    def communicate(self, mode: Union[WorkerMode, str]) -> None:
        option = InternalOption()
        self._ev_write("data", 
            dict(status="booting", pid=os.getpid(), ppid=os.getppid()))
        if not (self.event_log and self.fid):
            raise kodo.error.SetupError(f"missing event log and/or fid")
        t0 = helper.now()
        status = None
        success = self.run_ray() if option.RAY else self.run_process()
        if success:
            status = "finished"
        else:
            status = "error"
        t1 = helper.now()
        self._ev_write("data", 
            dict(status=status, runtime=(t1 - t0).total_seconds()))

    def run_ray(self) -> bool:
        ray.init(address="auto")
        event_queue: R_Queue = R_Queue()
        task = execute_ray.remote(self.event_log, event_queue)
        success = True
        while True:
            done, _pending = ray.wait([task], timeout=1)
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
