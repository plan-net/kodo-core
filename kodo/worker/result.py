from typing import Union, List, Dict
from pathlib import Path
from io import TextIOWrapper
import datetime
from kodo.datatypes import DynamicModel, Flow
from kodo import helper


class ExecutionResult:

    def __init__(self, file: TextIOWrapper):
        self.event_file = file
        self._data: Dict = {}
        self._status: List = []
        self._result: List = []
        self.flow = None

    def read(self):
        for line in self.event_file:
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
    
    def runtime(self):
        if self._status:
            t0 = self._status[0]["timestamp"]
            if self._status[-1]["value"] not in ("finished", "error"):
                return helper.now() - t0
            return self._status[-1]["timestamp"] - t0
        
    def status(self):
        if self._status:
            return self._status[-1]["value"]
        return None