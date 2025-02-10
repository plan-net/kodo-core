import asyncio
import datetime
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any, Dict, List, Union

import aiofiles
import psutil
from bson.objectid import ObjectId
from litestar.datastructures import State
from litestar.types import SSEData

from kodo import helper
from kodo.datatypes import DynamicModel, Flow
from kodo.log import logger
from kodo.worker.process.executor import (DIED_STATE, EVENT_STREAM,
                                          FINAL_STATE, KILL_FILE,
                                          PENDING_STATE, RUNNING_STATE,
                                          STDERR_FILE, STDOUT_FILE, STOP_FILE,
                                          STOPPING_STATE)


class ExecutionResult:

    def __init__(self, state: State, fid: Union[str, ObjectId]):
        folder = Path(state.exec_data).joinpath(str(fid))
        self.event_file = folder.joinpath(EVENT_STREAM)
        self._state = state
        self._data: Dict = {}
        self._status: List = []
        self._result: List = []
        self.flow: Any = None

    async def _readfile(self):
        if not self.event_file.exists():
            return
        async with aiofiles.open(self.event_file, "r") as fh:
            async for line in fh:
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

    async def status(self) -> dict:
        ret = {
            "status": None,
            "version": None,
            "flow": {
                "name": None,
                "description": None,
                "author": None,
                "tags": None,
                "entry": None
            },
            "has_final": False,
            "launch": {
                "fid": None,
                "payload": None,
                "success": None
            },
            "progress": None
        }
        async for timestamp, action, value in self._readfile():
            if action in ("status", "version"):
                ret[action] = value
            elif action == "flow":
                if isinstance(value, dict):
                    ret["flow"] = {k: value.get(k) for k in ret["flow"]}  # ignore: type
            elif action == "final":
                ret["has_final"] = True
            elif action == "launch":
                if isinstance(value, dict):
                    ret["launch"] = {k: value.get(k) for k in ret["launch"]}  # ignore: type  
            elif action == "progress":
                ret["progress"] = value.get("value")
        return ret
