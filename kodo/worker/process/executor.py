import os
import sys
import time
import traceback
from multiprocessing import Process, Queue as M_Queue
from pathlib import Path
from typing import Dict, Optional

import asyncio
import aiofiles
import httpx

import ray
from ray.util.queue import Queue as R_Queue

import kodo
from kodo import helper
from kodo.datatypes import MODE, DynamicModel, Flow, InternalOption
from kodo.worker.process.caller import SubProcess


STDOUT_FILE = "stdout.log"
STDERR_FILE = "stderr.log"
EVENT_STREAM = "event.log"
STOP_FILE = "_done_"
KILL_FILE = "_killed_"

PENDING_STATE = "pending"
BOOTING_STATE = "booting"
RUNNING_STATE = "running"
STOPPING_STATE = "stopping"
FINISHED_STATE = "finished"
ERROR_STATE = "error"
DIED_STATE = "died"

FIX = "@_ks_@"
FINAL_STATE = (FINISHED_STATE, ERROR_STATE, DIED_STATE)


def _execute(event_stream_file: Path, event):
    from kodo.worker.instrument.flow import flow_factory
    try:
        event.put(("data", {"status": RUNNING_STATE}))
        flow = flow_factory(event_stream_file, event)
        flow.instrument()
        result = flow.run()  # type: ignore
        event.put(("data", {"status": STOPPING_STATE}))
        flow.finish(result)
    except Exception as exc:
        event.put(("error", {"exception": traceback.format_exc()}))


@ray.remote
def execute_ray(*args, **kwargs):
    _execute(*args, **kwargs)


def execute_process(*args, **kwargs):
    _execute(*args, **kwargs)



class FlowExecutor:

    def __init__(
            self,
            factory: str,
            fid: str):
        option = InternalOption()
        self.factory: str = factory
        self.fid: str = fid
        path = Path(option.EXEC_DATA or ".").joinpath(self.fid).joinpath
        self.event_log = path(EVENT_STREAM)
        self.stdout_log = path(STDOUT_FILE)
        self.stderr_log = path(STDERR_FILE)
        self.stop_file = path(STOP_FILE)
        self.use_ray = option.RAY
        self.ray_server = option.RAY_SERVER
        self.ray_dashboard = option.RAY_DASHBOARD

    def prepare(self, inputs: Optional[Dict] = None) -> None:
        self.event_log.parent.mkdir(exist_ok=True, parents=True)
        self.event_log.touch()
        self._ev_write("data", {"version": kodo.__version__})
        self._ev_write("data", {"entry_point": self.factory})
        self._ev_write("data", {"fid": str(self.fid)})
        self._ev_write("data", {"inputs": inputs or {}})

    async def _aev_write(self, kind: str, data: Dict):
        async with aiofiles.open(self.event_log, "a") as f:  # type: ignore
            dump = DynamicModel(data).model_dump_json()
            now = helper.now().isoformat()
            await f.write(f"{now} {kind} {dump}\n")

    def _ev_write(self, kind: str, data: Dict):
        with open(self.event_log, "a") as f:  # type: ignore
            dump = DynamicModel(data).model_dump_json()
            now = helper.now().isoformat()
            f.write(f"{now} {kind} {dump}\n")

    async def enter(self, flow: Flow) -> SubProcess:
        # while not self.event_log.exists():
        #     await asyncio.sleep(0.1)
        await self._aev_write("data", {"flow": flow.model_dump()})
        await self._aev_write("data", dict(status=PENDING_STATE))
        with (open(self.stdout_log, 'wb') as stdout_file, 
              open(self.stderr_log, 'wb') as stderr_file):
            process = SubProcess(
                MODE.EXECUTE, self.factory, self.fid,
                stdin=None, stdout=stdout_file, stderr=stderr_file)
            await process.open()
        await process.wait()
        return process

    def communicate(self) -> None:
        t0 = helper.now()
        status = None
        self._ev_write("data", {"status": BOOTING_STATE})
        self._ev_write("data", {"pid": os.getpid()})
        self._ev_write("data", {"ppid": os.getppid()})
        executor = "ray" if self.use_ray else "thread"
        self._ev_write("data", {"executor": executor})
        success = self.run_ray() if self.use_ray else self.run_process()
        if success:
            status = FINISHED_STATE
        else:
            status = ERROR_STATE
        t1 = helper.now()
        self._ev_write("data", {"status": status})
        self._ev_write("data", {"runtime": (t1 - t0).total_seconds()})
        self.stop_file.touch()

    def run_ray(self) -> bool:
        try:
            httpx.get(str(self.ray_dashboard))
        except Exception as exc:
            self._ev_write("error", {"exception": traceback.format_exc()})
            return False
        else:
            ray.init(address=self.ray_server)
        event_queue: R_Queue = R_Queue()
        task = execute_ray.remote(self.event_log, event_queue)
        ctx = ray.get_runtime_context()
        self._ev_write("data", {"ray": {
            "job_id": ctx.get_job_id(),
            "node_id": ctx.get_node_id()
        }})
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


