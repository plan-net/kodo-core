import os
from typing import Union, Callable, Any
from pathlib import Path
#from threading import Thread
from multiprocessing import Process
import inspect
# import crewai

from multiprocessing import Queue as M_Queue
from ray.util.queue import Queue as R_Queue
import ray

from kodo import helper
from kodo.datatypes import WorkerMode
from kodo.worker.base import FlowProcess
import kodo.error


import debugpy  # type: ignore
import sys
from kodo.worker.flow import Flow


runtime_env = {"pip": ["emoji"]}

@ray.remote(runtime_env=runtime_env)
def execute(event_stream_file: Path, event):
    # debugpy.listen(("localhost", 5678))
    # debugpy.wait_for_client() 
    flow: Flow = Flow(event_stream_file, event)
    flow.load()
    flow.instrument()
    return flow.run()
class FlowExecution(FlowProcess):

    def communicate(self, mode: Union[WorkerMode, str]) -> None:
        self.event(
            "data", status="bootup", pid=os.getpid(), ppid=os.getppid())
        if not (self.event_log and self.fid):
            raise kodo.error.SetupError(f"missing event log and/or fid")
        try:
            ray.init(address="auto")
        except:
            ray.init()
        t0 = helper.now()
        # event_queue: M_Queue = M_Queue()
        # thread = Process(
        #     target=execute, 
        #     args=(self.event_log, event_queue))
        # thread.start()
        # while thread.is_alive() or not event_queue.empty():
        #     while not event_queue.empty():
        #         kind, data = event_queue.get()
        #         self.event(kind, **data)
        # thread.join()
        event_queue = R_Queue()
        task = execute.remote(self.event_log, event_queue)
        while True:
            done, pending = ray.wait([task], timeout=1)
            while not event_queue.empty():
                kind, data = event_queue.get()
                self.event(kind, **data)
            if done:
                result = ray.get(done[0])
                break
        t1 = helper.now()
        self.event(
            "data", status="finished", runtime=(t1 - t0).total_seconds())

