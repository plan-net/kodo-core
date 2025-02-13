import asyncio
import datetime
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Union

import aiofiles
import psutil
import pydantic
from litestar.response import ServerSentEventMessage
from litestar.types import SSEData

from kodo import helper
from kodo.datatypes import DynamicModel, Flow, LaunchResult
from kodo.log import logger
from kodo.remote.launcher import (BREAK_STATE, DIED_STATE, EVENT_LOG,
                                  FINAL_STATE, ev_format, INITIAL_STATE, KILLED_STATE)


class ExecutionResult:

    def __init__(self, event_folder: Union[str, Path]):
        folder = Path(event_folder)
        self.event_log = folder.joinpath(EVENT_LOG)
        self._status: list = []
        self._result: list = []
        self._wh = None
        self.data: dict = {
            "status": None,
            "version": None,
            "flow": None,
            "has_final": False,
            "launch": None,
            "environment": {
                "executable": None,
                "cwd": None,
                "module": None,
                "flow_name": None
            },
            "progress": None,
            "timing": {
                "tearup": None,
                "pending": None,
                "booting": None,
                "starting": None,
                "running": None,
                "stopping": None,
                "returning": None,
                "finished": None,
                "teardown": None,
            },
            "duration": {
                "tearup": None,
                "running": None,
                "teardown": None,
                "total": None
            }
        }

    def open_write(self):
        if self._wh is None:
            self._wh = self.event_log.open("a")

    def close_write(self):
        if self._wh is None:
            self._wh.close()

    def write(self, value: Union[dict, pydantic.BaseModel]) -> None:
        if self._wh is not None:
            self._wh.write(ev_format(value))
            self._wh.flush()

    def _readfile(self):
        with self.event_log.open("r") as fh:
            for line in fh:
                line = line.rstrip()
                if not line:
                    continue
                s_timestamp, s_data = line.split(" ", 1)
                timestamp = datetime.datetime.fromisoformat(s_timestamp)
                data = DynamicModel.model_validate_json(s_data)
                keys = list(data.root.keys())
                if len(keys) == 1:
                    toplevel = keys[0]
                    yield timestamp, keys[0], data.root[toplevel]
        
    async def _areadfile(self):
        first = last = None
        async with aiofiles.open(self.event_log, "r") as fh:
            async for line in fh:
                line = line.rstrip()
                if not line:
                    continue
                s_timestamp, s_data = line.split(" ", 1)
                timestamp = datetime.datetime.fromisoformat(s_timestamp)
                if first is None: first = timestamp
                if last is None: last = timestamp
                data = DynamicModel.model_validate_json(s_data)
                keys = list(data.root.keys())
                if len(keys) == 1:
                    toplevel = keys[0]
                    yield timestamp, keys[0], data.root[toplevel]
        self.data["timing"]["tearup"] = first
        self.data["timing"]["teardown"] = last

    def read(self) -> None:
        first = last = None
        for timestamp, action, value in self._readfile():
            if first is None: first = timestamp
            if last is None: last = timestamp
            self._map(timestamp, action, value, self.data)
        self.data["timing"]["tearup"] = first
        self.data["timing"]["teardown"] = last
        
    async def aread(self) -> None:
        first = last = None
        async for timestamp, action, value in self._areadfile():
            if first is None: first = timestamp
            last = timestamp
            self._map(timestamp, action, value, self.data)
        self.data["timing"]["tearup"] = first
        self.data["timing"]["teardown"] = last
        self._calculate_duration()

    def _delta(self, key0: str, key1: str) -> Union[datetime.timedelta, None]:
        t0 = self.data["timing"].get(key0, None)
        t1 = self.data["timing"].get(key1, None)
        if t0:
            if t1:
                delta = t1 - t0
            else:
                delta = helper.now() - t0
            return delta.total_seconds()
        else: 
            return None

    def _calculate_duration(self) -> None:
        self.data["duration"]["tearup"] = self._delta("tearup", "running")
        self.data["duration"]["running"] = self._delta("running", "stopping")
        self.data["duration"]["teardown"] = self._delta("stopping", "teardown")
        self.data["duration"]["total"] = self._delta("tearup", "teardown")

    async def final_result(self) -> Union[dict, None]:
        async for _, action, value in self._areadfile():
            if action == "final":
                return value
        return None

    async def result(self) -> dict[str, Any]:
        ret: dict = {"result": [], "final": None}
        async for _, action, value in self._areadfile():
            if action == "result":
                ret["result"].append(value)
            elif action == "final":
                ret["final"] = value
        return ret

    def _map(
            self, 
            timestamp: datetime.datetime, 
            action: str, 
            value: Any, 
            data: dict) -> None:
        if action in ("status", "version"):
            self.data[action] = value
            if action == "status":
                if value not in BREAK_STATE:
                    if value in FINAL_STATE:
                        value = "finished"
                    self.data["timing"][value] = timestamp
        elif action == "flow":
            self.data["flow"] = Flow(**value)
        elif action == "final":
            self.data["has_final"] = True
        elif action == "launch":
            self.data["launch"] = LaunchResult(**value)
        elif action == "environment":
            if isinstance(self.data["environment"], dict):
                self.data["environment"] = {
                    k: value.get(k) for k in self.data["environment"]}
        elif action == "progress":
            self.data["progress"] = value.get("value")

    async def get_progress(self) -> dict[str, Any]:
        status = None
        progress = None
        driver = None
        async for _, action, value in self._areadfile():
            if action == "status":
                status = value
            elif action == "progress":
                progress = value
            elif action == "driver":
                driver = value
        return {
            "status": status,
            "progresss": progress,
            "driver": driver
        }

    async def stream(self, *args) -> AsyncGenerator[SSEData, None]:
        done = False
        interval = None
        status = None
        async with aiofiles.open(self.event_log, "r") as fh:
            while True:
                line = await fh.readline()
                if not line:
                    await asyncio.sleep(0.1)
                    if done:
                        break
                line = line.rstrip()
                if not line:
                    if not interval or helper.now() > interval:
                        interval = helper.now() + datetime.timedelta(seconds=5)
                        alive = await self.check_alive()
                        if alive is not None and not alive:
                            done = True
                    continue
                timestamp, s_data = line.split(" ", 1)
                data = DynamicModel.model_validate_json(s_data)
                keys = list(data.root.keys())
                if len(keys) == 1:
                    toplevel = keys[0]
                    value = data.root[toplevel]
                    if toplevel == "status":
                        if value in FINAL_STATE:
                            done = True
                        status = value
                    if args and toplevel not in args:
                        continue
                    yield ServerSentEventMessage(data=line)
        yield ServerSentEventMessage(event="eof", data="Stream closed")
        return

    async def kill(self) -> bool:
        ret = await self.get_progress()
        if ret["driver"]:
            pid = ret["driver"].get("pid", None)
            if pid:
                try:
                    proc = psutil.Process(pid)
                    proc.terminate()
                    self.open_write()
                    self.write({"status": KILLED_STATE})
                    self.write({"stdout": "process killed by user"})
                    self.close_write()
                    return True
                except:
                    pass
        return False
    
    async def check_alive(self) -> Union[bool, None]:
        logger.info(f"checking {self.event_log}")
        ret = await self.get_progress()
        if ret["driver"]:
            pid = ret["driver"].get("pid", None)
            if pid:
                try:
                    psutil.Process(pid)
                    return True
                except:
                    pass
        if ret["status"] not in set(FINAL_STATE).union(INITIAL_STATE):
            self.open_write()
            self.write({"status": DIED_STATE})
            self.close_write()
            return False
        return None
    
    def __getattr__(self, name):
        return self.data.get(name, None)
