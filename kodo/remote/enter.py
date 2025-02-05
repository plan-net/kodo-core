import sys
import ray
from typing import Callable
from kodo.remote.launcher import RAY_NAMESPACE, RAY_ENV
from kodo.common import Launch

import debugpy


def flow_factory(module_name: str, flow: str) -> Callable:
    module = __import__(module_name)
    components = module_name.split('.')
    for comp in components[1:]:
        module = getattr(module, comp)
    return getattr(module, flow)


if __name__ == "__main__":
    if not debugpy.is_client_connected():
        debugpy.listen(("localhost", 63256))
    debugpy.wait_for_client() 
    server, actor_name = sys.argv[1:3]
    ray.init(
        address=server, 
        ignore_reinit_error=True,
        namespace=RAY_NAMESPACE,
        runtime_env=RAY_ENV
    )        
    actor = ray.get_actor(actor_name)
    module_name = ray.get(actor.get_module.remote())
    flow_name = ray.get(actor.get_flow.remote())
    inputs = ray.get(actor.get_inputs.remote())
    flow = flow_factory(module_name, flow_name)
    callback = flow.get_register("enter")  # type: ignore
    ret = callback(inputs)
    if isinstance(ret, Launch):
        ray.get(actor.add_launch.remote(ret))
    else:
        ray.get(actor.add_body.remote(ret))
    ray.shutdown()
