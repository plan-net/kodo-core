import asyncio
import datetime
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Dict, List, Union

from bson.objectid import ObjectId
from litestar.datastructures import State
from litestar.types import SSEData

from kodo import helper
from kodo.datatypes import DynamicModel, Flow
from kodo.worker.base import (EVENT_STREAM, KILL_FILE, STDERR_FILE,
                              STDOUT_FILE, STOP_FILE)


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

    def read(self):
        fh = self.event_file.open("r")
        for line in fh:
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
            t1 = self._findtime("running") or helper.now()
            return (t1 - t0).total_seconds()
        return None

    def teardown(self):
        if self._status:
            t0 = self._findtime("stopping")
            if t0:
                t1 = self._status[-1]["timestamp"]
                return (t1 - t0).total_seconds()
        return None

    def runtime(self):
        t0 = self._findtime("running")
        if t0:
            t1 = self._findtime("stopping") or helper.now()
            return (t1 - t0).total_seconds()
        return None

    def total_time(self):
        if self._status:
            now = helper.now()
            t0 = self._status[0]["timestamp"] or now
            t1 = self._findtime("finished", "error") or now
            return (t1 - t0).total_seconds()
        return None
    
    def inactive_time(self):
        if self.status() not in ("finished", "error"):
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

    async def _stream(self, 
                      file: Path, 
                      resolve: bool=False) -> AsyncGenerator[SSEData, None]:
        fh = file.open("r")
        while not self._state.exit:
            line = fh.readline()
            if line:
                if resolve:
                    timestamp, action, data = line.split(" ", 2)
                    yield {
                        "data": timestamp + ': ' + data.rstrip(), 
                        "event": action}
                else:
                    yield {"data": line.rstrip(), "event": "line"}
            else:
                if self.stop_file.exists():
                    yield {"data": "process closed", "event": "eof"}
                    break
                elif self.kill_file.exists():
                    yield {"data": "process killed", "event": "eof"}
                    break
                await asyncio.sleep(0.1)
        fh.close()

    async def stream_stdout(self) -> AsyncGenerator[SSEData, None]:
        return self._stream(self.stdout_file)

    async def stream_stderr(self) -> AsyncGenerator[SSEData, None]:
        return self._stream(self.stderr_file)

    async def stream_event(self) -> AsyncGenerator[SSEData, None]:
        return self._stream(self.event_file, resolve=True)
