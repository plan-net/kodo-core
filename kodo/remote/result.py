from typing import Union, Any, Dict, List, Optional
from pathlib import Path
import pydantic
import datetime
from kodo.datatypes import DynamicModel, Flow, LaunchResult
from kodo.remote.launcher import (EVENT_LOG, PENDING_STATE, 
                                  RUNNING_STATE, STOPPING_STATE, 
                                  COMPLETED_STATE, ERROR_STATE, FINAL_STATE, ev_format)
from kodo import helper


class ExecutionResult:

    def __init__(self, event_folder: str):
        folder = Path(event_folder)
        self._last: Optional[datetime.datetime] = None
        self.event_log = folder.joinpath(EVENT_LOG)
        self._data: dict = {}
        self._status: list = []
        self._result: list = []
        self._wh = None

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

    def read(self) -> None:
        with self.event_log.open("r") as el:
            while True:
                line = el.readline()
                if line:
                    ts, ds = line.split(" ", 1)
                    timestamp = datetime.datetime.fromisoformat(ts)
                    self._last = timestamp
                    data = DynamicModel.model_validate_json(ds)
                    keys = list(data.root.keys())
                    if len(keys) == 1:
                        key = keys.pop(0)
                        if key == "status":
                            self._status.append((timestamp, data.root[key]))
                        elif key == "result":
                            self._result.append((timestamp, data.root[key]))
                        else:
                            self._data[key] = data.root[key]
                    else:
                        raise RuntimeError(
                            "data must have single top level key")
                else:
                    break

    @property
    def status(self):
        if self._status:
            return self._status[-1][1]
        return None
    
    @property
    def flow(self):
        flow = self._data.get("flow", None)
        if flow:
            return Flow(**flow)
        return None

    @property
    def launch(self):
        launch = self._data.get("launch", None)
        if launch:
            return LaunchResult(**launch)
        return None

    def active(self):
        return self.status not in FINAL_STATE

    def _findtime(self, *args):
        for stat in self._status:
            if stat[1] in args:
                return stat[0]
        return None

    def _timedelta(self, status0, *status1) -> Optional[datetime.timedelta]:
        t0 = self._findtime(status0)
        if t0:
            t1 = self._findtime(*status1)
            if t1:
                return t1 - t0
            if self.active():
                return helper.now() - t0
            return self._last - t0
        return None

    def tearup_time(self) -> Optional[datetime.timedelta]:
        return self._timedelta(PENDING_STATE, RUNNING_STATE)
    
    def running_time(self) -> Optional[datetime.timedelta]:
        return self._timedelta(RUNNING_STATE, STOPPING_STATE)

    def teardown_time(self) -> Optional[datetime.timedelta]:
        return self._timedelta(STOPPING_STATE, *FINAL_STATE)

    def total_time(self) -> Optional[datetime.timedelta]:
        return self._timedelta(PENDING_STATE, *FINAL_STATE)
    
    def inactive_time(self) -> Optional[datetime.timedelta]:
        if self.status in FINAL_STATE:
            return None
        return helper.now() - self._last
    
    def __getattr__(self, name):
        return self._data.get(name, None)
