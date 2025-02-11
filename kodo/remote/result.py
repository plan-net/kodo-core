import asyncio
import datetime
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Dict, List, Union
import pydantic
import aiofiles
import psutil
from bson.objectid import ObjectId
from litestar.datastructures import State
from litestar.types import SSEData

from kodo import helper
from kodo.datatypes import DynamicModel, Flow, LaunchResult
from kodo.log import logger
from kodo.remote.launcher import (
    EVENT_LOG, ev_format, FINAL_STATE, BREAK_STATE)


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
        
    def __getattr__(self, name):
        return self.data.get(name, None)

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

# from typing import Union, Any, Dict, List, Optional
# from pathlib import Path
# import pydantic
# import datetime
# from kodo.datatypes import DynamicModel, Flow, LaunchResult
# from kodo.remote.launcher import (EVENT_LOG, PENDING_STATE, 
#                                   RUNNING_STATE, STOPPING_STATE, 
#                                   COMPLETED_STATE, ERROR_STATE, FINAL_STATE, ev_format)
# from kodo import helper


# class ExecutionResult:

#     def __init__(self, event_folder: str):
#         folder = Path(event_folder)
#         self._last: Optional[datetime.datetime] = None
#         self.event_log = folder.joinpath(EVENT_LOG)
#         self._data: dict = {}
#         self._status: list = []
#         self._result: list = []
#         self._wh = None

#     def open_write(self):
#         if self._wh is None:
#             self._wh = self.event_log.open("a")

#     def close_write(self):
#         if self._wh is None:
#             self._wh.close()

#     def write(self, value: Union[dict, pydantic.BaseModel]) -> None:
#         if self._wh is not None:
#             self._wh.write(ev_format(value))
#             self._wh.flush()

#     def read(self) -> None:
#         with self.event_log.open("r") as el:
#             while True:
#                 line = el.readline()
#                 if line:
#                     ts, ds = line.split(" ", 1)
#                     timestamp = datetime.datetime.fromisoformat(ts)
#                     self._last = timestamp
#                     data = DynamicModel.model_validate_json(ds)
#                     keys = list(data.root.keys())
#                     if len(keys) == 1:
#                         key = keys.pop(0)
#                         if key == "status":
#                             self._status.append((timestamp, data.root[key]))
#                         elif key == "result":
#                             self._result.append((timestamp, data.root[key]))
#                         else:
#                             self._data[key] = data.root[key]
#                     else:
#                         raise RuntimeError(
#                             "data must have single top level key")
#                 else:
#                     break

#     @property
#     def status(self):
#         if self._status:
#             return self._status[-1][1]
#         return None
    
#     @property
#     def flow(self):
#         flow = self._data.get("flow", None)
#         if flow:
#             return Flow(**flow)
#         return None

#     @property
#     def launch(self):
#         launch = self._data.get("launch", None)
#         if launch:
#             return LaunchResult(**launch)
#         return None

#     def active(self):
#         return self.status not in FINAL_STATE

#     def _findtime(self, *args):
#         for stat in self._status:
#             if stat[1] in args:
#                 return stat[0]
#         return None

#     def _timedelta(self, status0, *status1) -> Optional[datetime.timedelta]:
#         t0 = self._findtime(status0)
#         if t0:
#             t1 = self._findtime(*status1)
#             if t1:
#                 return t1 - t0
#             if self.active():
#                 return helper.now() - t0
#             return self._last - t0
#         return None

#     def tearup_time(self) -> Optional[datetime.timedelta]:
#         return self._timedelta(PENDING_STATE, RUNNING_STATE)
    
#     def running_time(self) -> Optional[datetime.timedelta]:
#         return self._timedelta(RUNNING_STATE, STOPPING_STATE)

#     def teardown_time(self) -> Optional[datetime.timedelta]:
#         return self._timedelta(STOPPING_STATE, *FINAL_STATE)

#     def total_time(self) -> Optional[datetime.timedelta]:
#         return self._timedelta(PENDING_STATE, *FINAL_STATE)
    
#     def inactive_time(self) -> Optional[datetime.timedelta]:
#         if self.status in FINAL_STATE:
#             return None
#         return helper.now() - self._last
    
#     def __getattr__(self, name):
#         return self._data.get(name, None)
