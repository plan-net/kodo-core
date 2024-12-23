from typing import Optional, Any
from uuid import uuid4, UUID
from pathlib import Path
import kodo.helper
import kodo.config
import kodo.error

EVENT_STREAM_SUFFIX = ".ev"


class Worker:

    def __init__(self, 
                 entry_point: str, 
                 fid: Optional[UUID]=None,
                 exec_path: Optional[str]=None):
        self.entry_point = entry_point
        factory = kodo.helper.parse_factory(entry_point)
        self.flow = factory
        self.exec_path = Path(exec_path or kodo.config.setting.EXEC_DATA)
        self.fid = fid
        self.event_file = None

    async def welcome(self):
        # takes place on the node
        # returns HTML to POST (instantiate)
        callback = self.flow.get_register("welcome")
        return await callback()

    async def trigger(self, **kwargs):
        with open(self.event_file, "a") as f:
            f.write(f"{kwargs}\n")

    async def instantiate(self):
        # takes place on the node
        # creates the fid, log folder, and event stream, state _starting_
        self.fid = uuid4()
        self.exec_path.mkdir(parents=True, exist_ok=True)
        self.event_file = self.exec_path.joinpath(
            str(self.fid)).with_suffix(EVENT_STREAM_SUFFIX)
        self.trigger(version=kodo.__version__, entry_point=self.entry_point)
        return self.fid

    def execute(self):
        # takes place remote on the worker
        # creates an Actor on the remote flow to stream events from worker
        return

