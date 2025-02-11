import inspect
import sys
import os
from typing import Any, Callable, Optional

import crewai
import ray.actor

import kodo.error
from kodo import helper
from kodo.common import Launch
from kodo.remote.launcher import RAY_ENV, RAY_NAMESPACE, RUNNING_STATE


def flow_factory(module_name: str, flow: str) -> Callable:
    module = __import__(module_name)
    components = module_name.split('.')
    for comp in components[1:]:
        module = getattr(module, comp)
    return getattr(module, flow)


class FlowCallable:

    def __init__(
            self, 
            flow: Any,
            args: tuple,
            inputs: dict,
            actor: ray.actor.ActorHandle):
        self.actor = actor
        self.flow = flow
        self.inputs = inputs
        self.args = args
        self.run: Optional[Callable] = None
        self.memo: dict = {}

    @staticmethod
    def _create_action_method(key: str):
        def method(self, value):
            self._action(key, value)
        return method

    def _action(self, key, value):
        self.actor.enqueue.remote(**{key: value})

    result = _create_action_method("result")
    meta = _create_action_method("meta")
    progress = _create_action_method("progress")
    final = _create_action_method("final")
    error = _create_action_method("error")

    def instrument(self) -> None:
        sig = inspect.signature(self.flow)
        bound_args = sig.bind_partial()
        if 'inputs' in sig.parameters:
            bound_args.arguments['inputs'] = self.inputs
        if 'flow' in sig.parameters:
            bound_args.arguments['flow'] = self
        bound_args.apply_defaults()
        self.run: Callable = lambda: self.flow(
            *bound_args.args, **bound_args.kwargs)

    def finish(self, *args, **kwargs):
        pass

class FlowCrewAI(FlowCallable):

    def instrument(self) -> None:
        if self.flow.step_callback:
            self.memo["step_callback"] = self.flow.step_callback
        if self.flow.task_callback:
            self.memo["task_callback"] = self.flow.task_callback
        self.flow.step_callback = self._crewai_step_callback
        self.flow.task_callback = self._crewai_task_callback
        self.run: Callable = lambda: self.flow.kickoff(self.inputs)
        for agent in self.flow.agents:
            dump = {
                "role": agent.role,
                "goal": agent.goal,
                "backstory": agent.backstory,
                "tools": []
            }
            for tool in agent.tools:
                dump["tools"].append({
                    "name": tool.name,
                    "description": tool.description
                })
            self.meta({"agent": dump})
        self.memo["task"] = {}
        for task in self.flow.tasks:
            dump = {
                "name": task.name,
                "description": task.description,
                "expected_output": task.expected_output,
                "agent": task.agent.role,
                "tools": []
            }
            for tool in agent.tools:
                dump["tools"].append({
                    "name": tool.name,
                    "description": tool.description
                })
            self.meta({"task": dump})
            self.memo["task"][task.name] = []

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
        task = args[0]
        if task.name not in self.memo["task"]:
            self.memo["task"][task.name] = []
        self.memo["task"][task.name].append(helper.now())
        done = sum([len(lst) for t, lst in self.memo["task"].items()])
        total = sum([len(lst) or 1 for t, lst in self.memo["task"].items()])
        self.progress({
            "done": done, 
            "pending": total - done, 
            "total": total,
            "value": done / total
        })

    def finish(self, *args, **kwargs):
        for result in args:
            self.final({result.__class__.__name__: result.model_dump()})
            

def execute_flow(fid: str) -> None:
    ray.init(
        address=server, 
        ignore_reinit_error=True, 
        namespace=RAY_NAMESPACE,
        runtime_env=RAY_ENV)
    actor = ray.get_actor(f"{fid}.exec")
    data = ray.get(actor.get_data.remote())
    flow_name = data["environment"]["flow_name"]
    module = data["environment"]["module"]
    flow_obj: Any = flow_factory(module, flow_name)
    inputs = data["launch"].payload
    ray.get(actor.enqueue.remote(status=RUNNING_STATE))  # type: ignore
    ctx = ray.get_runtime_context()
    ray.get(actor.enqueue.remote(remote={
        "node_id": ctx.get_node_id(),
        "job_id": ctx.get_job_id(),
        "pid": os.getpid(),
        "ppid": os.getppid()
    }))
    flow = flow_obj.flow
    cls: Any
    if isinstance(flow, crewai.Crew):
        cls = FlowCrewAI
    elif hasattr(flow, "is_crew_class") and flow.is_crew_class:
        flow = flow().crew()
        cls = FlowCrewAI
    elif callable(flow):
        cls = FlowCallable
    else:
        raise kodo.error.SetupError(f"not found: {module}:{flow_name}")
    runner = cls(flow, inputs["args"], inputs["inputs"], actor)
    try:
        runner.instrument()
        if runner.run is not None:
            ret = runner.run()
            runner.finish(ret)
        else:
            raise kodo.error.SetupError(f"no runner: {module}:{flow_name}")
    except Exception as exc:
        runner.error({f"{exc.__class__.__name__}": str(exc)})
    finally:
        ray.shutdown()


def enter_flow(fid: str) -> None:
    ray.init(address=server, namespace=RAY_NAMESPACE, runtime_env=RAY_ENV)
    actor = ray.get_actor(f"{fid}.launch")
    ray.get(actor.ready.remote())
    data = ray.get(actor.get_data.remote())
    flow: Any = flow_factory(data["module"], data["flow"])
    callback = flow.get_register("enter") 
    try:
        ret = callback(data["inputs"])
        if isinstance(ret, Launch):
            ray.get(actor.set_launch.remote(ret))
        else:
            ray.get(actor.set_body.remote(ret))
    except Exception as exc:
        ray.get(actor.set_error(f"{exc.__class__.__name__}: {exc}"))
    ray.shutdown()


if __name__ == "__main__":
    mode, server, fid = sys.argv[1:4]
    if mode == "execute":
        execute_flow(fid)
    elif mode == "enter":
        enter_flow(fid)
    else:
        raise ValueError(f"Unknown mode: {mode}")


