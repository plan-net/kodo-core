import functools
import inspect

from kodo.datatypes import Flow


async def DEFAULT_LANDING_PAGE():
    return """
"<p>
    The developer did not yet implement a user 
    interface for this agent. Please check back 
    later.
</p>"
"""


class FlowHandler:

    def __init__(self, flow, **kwargs):
        self.fields = Flow(**kwargs)
        self.flow = flow
        self._register = {"enter": DEFAULT_LANDING_PAGE}

    def __getattr__(self, name):
        return self.fields.get(name)

    def enter(self, func=None):
        @functools.wraps(func)
        def wrapper(form=None, method=None, *args, **kwargs):
            if form is None:
                form = {}
            if method is None:
                method = "GET"
            sig = inspect.signature(func)
            bound_args = sig.bind_partial(*args, **kwargs)
            if 'form' in sig.parameters:
                bound_args.arguments['form'] = form
            if 'method' in sig.parameters:
                bound_args.arguments['method'] = method
            bound_args.apply_defaults()
            return func(*bound_args.args, **bound_args.kwargs)
        # self._kwprops = {**self._kwargs, **kwargs}
        self._register['enter'] = wrapper
        return wrapper

    def get_register(self, key):
        return self._register[key]


