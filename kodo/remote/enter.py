from kodo import helper
tt0 = helper.now()
import inspect
import sys
from typing import Any, Callable, Optional

import crewai
import ray.actor

import kodo.error
from kodo.common import Launch
from kodo.remote.launcher import RAY_ENV, RAY_NAMESPACE, RUNNING_STATE
tt1 = helper.now()


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
    final = _create_action_method("final")
    data = _create_action_method("data")
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
        print("OK")
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

    def finish(self, *args, **kwargs):
        for result in args:
            self.actor.enqueue.remote(
                final={result.__class__.__name__: result.model_dump()})
            

def execute_flow(fid: str) -> None:
    actor = ray.get_actor(f"{fid}.exec")
    data = ray.get(actor.get_data.remote())
    flow_name = data["environment"]["flow_name"]
    module = data["environment"]["module"]
    flow_obj: Any = flow_factory(module, flow_name)
    inputs = data["launch"].payload
    # ray.get(actor.enqueue.remote(executable=sys.executable))  # type: ignore
    ray.get(actor.enqueue.remote(status=RUNNING_STATE))  # type: ignore
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
    runner.instrument()
    if runner.run is not None:
        ret = runner.run()
        runner.finish(ret)
    else:
        raise kodo.error.SetupError(f"no runner: {module}:{flow_name}")


def enter_flow(fid: str) -> None:
    t0 = helper.now()
    actor = ray.get_actor(f"{fid}.launch")
    ray.get(actor.ready.remote())
    actor.add_debug.remote(f"startup: {tt1-tt0}")
    t1 = helper.now()
    actor.add_debug.remote(f"actor ready: {t1-t0}")
    data = ray.get(actor.get_data.remote())
    t2 = helper.now()
    actor.add_debug.remote(f"get data: {t2-t1}")
    flow: Any = flow_factory(data["module"], data["flow"])
    callback = flow.get_register("enter") 
    ret = callback(data["inputs"])
    t3 = helper.now()
    actor.add_debug.remote(f"return from callback: {t3-t2}")
    if isinstance(ret, Launch):
        ray.get(actor.set_launch.remote(ret))
    else:
        ray.get(actor.set_body.remote(ret))


if __name__ == "__main__":
    mode, server, fid = sys.argv[1:4]
    ray.init(address=server, ignore_reinit_error=True, 
             namespace=RAY_NAMESPACE, runtime_env=RAY_ENV)        
    if mode == "execute":
        execute_flow(fid)
    elif mode == "enter":
        enter_flow(fid)
    else:
        raise ValueError(f"Unknown mode: {mode}")
    ray.shutdown()
