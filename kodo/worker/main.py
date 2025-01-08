from typing import Optional, Any, Callable
from uuid import uuid4, UUID
from pathlib import Path
from litestar import Request
from litestar.response import Redirect
from litestar.datastructures import FormMultiDict
import kodo.helper
import kodo.error
import kodo.interrupt
import kodo.worker.loader
from kodo.log import logger

EVENT_STREAM_SUFFIX = ".ev"


class Worker:

    def __init__(self, 
                 entry_point: str, 
                 exec_path: str,
                 fid: Optional[UUID]=None):
        self.entry_point = entry_point
        factory: Any = kodo.helper.parse_factory(entry_point)
        self.flow: kodo.worker.loader.FlowDecorator = factory
        self.exec_path = Path(exec_path)
        self.fid = fid
        self.event_file: Optional[Path] = None

    async def welcome(self, form:Optional[FormMultiDict]=None):
        # takes place on the node
        # returns HTML to POST (instantiate)

        # stays in the loop until kodo.interrupt.LaunchFlow is raised
        if self.flow is None:
            callback = kodo.worker.loader.DEFAULT_LANDING_PAGE
        else:
            callback = self.flow.get_register("welcome")
        try:
            ret = await callback(form)
            return ret
        except kodo.interrupt.LaunchFlow as e:
            logger
            self.fid = await self.instantiate(e.inputs)
            await self.start_execution()
            return Redirect(f"/flow/state/{self.fid}")
        #callback = self.flow.get_register("enter")
        return self.fid

    async def trigger(self, **kwargs):
        with open(self.event_file, "a") as f:
            f.write(f"{kwargs}\n")

    async def instantiate(self, inputs: Optional[dict]=None):
        # takes place on the node
        # creates the fid, log folder, and event stream, state _starting_
        self.fid = uuid4()
        if self.exec_path is None:
            raise kodo.error.SetupError(f"Invalid exec_path: {self.exec_path}")
        self.exec_path.mkdir(parents=True, exist_ok=True)
        if inputs is None:
            inputs = {}
        self.event_file = self.exec_path.joinpath(
            str(self.fid)).with_suffix(EVENT_STREAM_SUFFIX)
        await self.trigger(
            version=kodo.__version__, 
            entry_point=self.entry_point,
            fid=str(self.fid))
        await self.trigger(inputs=inputs)
        logger.info(
            f"successfully instantiated flow {self.entry_point}: {self.fid}")
        return self.fid

    async def start_execution(self):
        # takes place remote on the worker
        # creates an Actor on the remote flow to stream events from worker
        # this is where we detach from parent process
        logger.info(
            f"start execution {self.entry_point}: {self.fid} now")
        return

