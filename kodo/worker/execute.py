import os
from typing import Union, Callable, Any
from pathlib import Path

import crewai

from kodo import helper
from kodo.datatypes import WorkerMode
from kodo.worker.base import FlowProcess
import kodo.error

def instrument_crew(crew: crewai.Crew):
    pass

def instrument_flow(flow: crewai.Flow):
    pass

def instrument_function(fun: Callable):
    pass

# @remote
# def execute(entry_point: str, event_stream_file: Path, fid: str, actor):
#     obj: Any = helper.parse_factory(entry_point)
#     flow = obj.flow
#     if isinstance(flow, crewai.Crew):
#         instrument_crew(flow, actor)
#         flow.kickoff()
#     elif isinstance(flow, crewai.Flow):
#         instrument_flow(flow)
#         flow.kickoff()
#     elif callable(flow):
#         instrument_function(flow)
#         flow()


class FlowExecution(FlowProcess):

    def communicate(self, mode: Union[WorkerMode, str]) -> None:
        self.event(
            "data", status="running", pid=os.getpid(), ppid=os.getppid())
        t0 = helper.now()
        # ray.init(address="auto")
        # actor init!
        if self.event_log and self.fid:
            actor = None
            # try:
            #     execute(self.factory, self.event_log, self.fid, actor)
            # except Exception as exc:
            #     pass
            # while execute:
            #     read from events
            #     save events to event log
        else:
            raise kodo.error.SetupError(f"missing event log and/or fid")
        t1 = helper.now()
        self.event(
            "data", status="finished", runtime=(t1 - t0).total_seconds())

