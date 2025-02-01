import asyncio
import datetime
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Dict, List, Union

import psutil
from bson.objectid import ObjectId
from litestar.datastructures import State
from litestar.types import SSEData

from kodo import helper
from kodo.datatypes import DynamicModel, Flow
from kodo.log import logger
import aiofiles
from kodo.worker.process.base import (DIED_STATE, EVENT_STREAM, FINAL_STATE, KILL_FILE,
                              RUNNING_STATE, STDERR_FILE, STDOUT_FILE,
                              STOP_FILE, STOPPING_STATE)


class ExecutionResult:

    def __init__(self, state: State, fid: Union[str, ObjectId]):
        folder = Path(state.exec_data).joinpath(str(fid))
        self.event_file = folder.joinpath(EVENT_STREAM)
        self.stdout_file = folder.joinpath(STDOUT_FILE)
        self.stderr_file = folder.joinpath(STDERR_FILE)
        self.stop_file = folder.joinpath(STOP_FILE)
        self.kill_file = folder.joinpath(KILL_FILE)
        self._state = state
        self._data: Dict = {}
        self._status: List = []
        self._result: List = []
        self.flow: Any = None

    def kill(self):
        self.kill_file.touch()

    async def read(self):
        if not self.event_file.exists():
            logger.error(f"event file {self.event_file} not found")
            return
        async with aiofiles.open(self.event_file, "r") as fh:
            async for line in fh:
                line = line.rstrip()
                if not line:
                    continue
                s_timestamp, action, s_data = line.split(" ", 2)
                timestamp = datetime.datetime.fromisoformat(s_timestamp)
                data = DynamicModel.model_validate_json(s_data)
                if action == "data":
                    for k, v in data.root.items():
                        if k == "status":
                            self._status.append({
                                "timestamp": timestamp, "value": v})
                            if len(self._status) > 1:
                                upd = self._status[-2]
                                upd["duration"] = timestamp - upd["timestamp"]
                        elif k == "flow":
                            self.flow = Flow(**v)
                        else:
                            self._data[k] = v

    async def verify(self):
        status = self.status()
        alive = self.check_alive()
        if status not in FINAL_STATE:
            if not self.pid or not alive:
                async with aiofiles.open(self.event_file, "a") as f:  
                    dump = DynamicModel(
                        {"status": DIED_STATE}).model_dump_json()
                    now = helper.now()
                    await f.write(f"{now.isoformat()} data {dump}\n")
                    self._status.append({
                        "timestamp": now, "value": DIED_STATE})
                    logger.error(f"flow {self.fid}, pid {self.pid} died")
        return alive

    def __getattr__(self, name):
        return self._data.get(name, None)
    
    def _findtime(self, *args):
        for stat in self._status:
            if stat["value"] in args:
                return stat["timestamp"]
        return None

    def tearup(self):
        if self._status:
            t0 = self._status[0]["timestamp"]
            t1 = self._findtime(RUNNING_STATE) or helper.now()
            return (t1 - t0).total_seconds()
        return None

    def teardown(self):
        if self._status:
            t0 = self._findtime(STOPPING_STATE)
            if t0:
                t1 = self._status[-1]["timestamp"]
                return (t1 - t0).total_seconds()
        return None

    def runtime(self):
        t0 = self._findtime(RUNNING_STATE)
        if t0:
            t1 = self._findtime(STOPPING_STATE, DIED_STATE) or helper.now()
            return (t1 - t0).total_seconds()
        return None

    def total_time(self):
        if self._status:
            now = helper.now()
            t0 = self._status[0]["timestamp"] or now
            t1 = self._findtime(*FINAL_STATE) or now
            return (t1 - t0).total_seconds()
        return None
    
    def inactive_time(self):
        if self.status() not in FINAL_STATE:
            files = [self.stdout_file, self.stderr_file, self.event_file]
            last_modified = datetime.datetime.fromtimestamp(
                max(file.stat().st_mtime for file in files)
            )
            now = datetime.datetime.now().replace(tzinfo=None)
            return (now - last_modified).total_seconds()
        return None
    
    def status(self):
        if self._status:
            return self._status[-1]["value"]
        return None

    def check_alive(self):
        try:
            proc = psutil.Process(self.pid)
            if proc.is_running():
                return True
        except:
            return False

    def start_time(self):
        if self._status:
            return self._status[0]["timestamp"]
        return None

    def end_time(self):
        return self._findtime(*FINAL_STATE)

    async def _stream(self, 
                      file: Path, 
                      split: bool=False) -> AsyncGenerator[SSEData, None]:
        await self.read()
        async with aiofiles.open(file, "r") as fh:
            next_check = helper.now() + datetime.timedelta(seconds=5)
            while not self._state.exit:
                line = await fh.readline()
                if line:
                    if split:
                        timestamp, action, data = line.split(" ", 2)
                        yield {
                            "data": timestamp + ': ' + data.rstrip(), 
                            "event": action}
                    else:
                        yield {"data": line.rstrip(), "event": "line"}
                    await asyncio.sleep(0.01)
                else:
                    if self.stop_file.exists():
                        yield {"data": "process closed", "event": "eof"}
                        break
                    elif self.kill_file.exists():
                        yield {"data": "process killed", "event": "eof"}
                        break
                    elif self.status() not in FINAL_STATE:
                        if helper.now() > next_check:
                            next_check = helper.now() + datetime.timedelta(
                                seconds=5)
                            if not self.check_alive():
                                yield {"data": "process died", "event": "eof"}
                                break
                    await asyncio.sleep(0.1)

    async def stream_stdout(self):
        return self._stream(self.stdout_file)

    async def stream_stderr(self):
        return self._stream(self.stderr_file)

    async def stream_event(self):
        return self._stream(self.event_file, split=True)


