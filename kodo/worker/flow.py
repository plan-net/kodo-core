import inspect
import queue
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

import crewai
import ray.util.queue

import kodo.error
from kodo import helper
from kodo.datatypes import DynamicModel


class Flow:

    def __init__(
            self, 
            flow: Any,
            state: dict, 
            event: Union[queue.Queue, ray.util.queue.Queue]):
        self._event = event
        self._state = state
        self.flow = flow
        self.run: Optional[Callable] = None
        self.memo: Dict = {}

    def __getattr__(self, name):
        return self._state.get(name)

    @staticmethod
    def _create_action_method(name: str):
        def method(self, *args, **kwargs):
            self._action(name, *args, **kwargs)
        return method

    def instrument(self):
        raise NotImplementedError()

    def finish(self, *args, **kwargs) -> None:
        raise NotImplementedError()

    def _action(self, kind, data: dict):
        self._event.put((kind, data))

    result = _create_action_method("result")
    final = _create_action_method("final")
    data = _create_action_method("data")
    error = _create_action_method("error")


class FlowCallable(Flow):

    def instrument(self) -> None:
        sig = inspect.signature(self.flow)
        bound_args = sig.bind_partial()
        if 'inputs' in sig.parameters:
            bound_args.arguments['inputs'] = self.inputs
        if 'event' in sig.parameters:
            bound_args.arguments['event'] = self
        bound_args.apply_defaults()
        self.run: Callable = lambda: self.flow(
            *bound_args.args, **bound_args.kwargs)

    def finish(self, *args, **kwargs) -> None:
        pass

class FlowCrewAI(Flow):

    def instrument(self) -> None:
        if self.flow.step_callback:
            self.memo["step_callback"] = self.flow.step_callback
        if self.flow.task_callback:
            self.memo["task_callback"] = self.flow.task_callback
        self.flow.step_callback = self._crewai_step_callback
        self.flow.task_callback = self._crewai_task_callback
        self.run: Callable = lambda: self.flow.kickoff(self.inputs)

    def _callback(self, cbname: str, *args, **kwargs) -> None:
        if cbcall:=self.memo.get(cbname, None):
            cbcall(*args, **kwargs)
        data = args[0]
        if hasattr(data, "model_dump"):
            self.result({data.__class__.__name__: data.model_dump()})
        else:
            self.result({data.__class__.__name__: data.__dict__})

    def _crewai_step_callback(self, *args, **kwargs) -> None:
        self._callback("step_callback", *args, **kwargs)
# 
    def _crewai_task_callback(self, *args, **kwargs) -> None:
        self._callback("task_callback", *args, **kwargs)

    def finish(self, *args, **kwargs) -> None:
        for result in args:
            self.final({result.__class__.__name__: result.model_dump()})


def flow_factory(
        event_stream_file: Union[Path, str], 
        event: Union[queue.Queue, ray.util.queue.Queue]) -> Flow:
    state = {}
    with open(event_stream_file, "r") as f:
        for line in f:
            _, action, body = line.split(" ", 2)
            if action == "data":
                data = DynamicModel.model_validate_json(body)
                for k, v in data.root.items():
                    state[k] = v
            # else:
            #     raise RuntimeError(f"unknown action: {action}")

    entry_point = str(state.get("entry_point"))
    flow: Any = helper.parse_factory(entry_point)
    obj = flow.flow
    if callable(obj):
        return FlowCallable(obj, state, event)
    elif isinstance(obj, crewai.Crew):
        return FlowCrewAI(obj, state, event)
    else:
        raise kodo.error.SetupError(f"unknown entry point: {entry_point}")
