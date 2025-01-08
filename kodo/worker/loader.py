import functools
import inspect
import os
from typing import Any, Callable, Dict, Optional, List, Union
import pickle
from pathlib import Path
from litestar.datastructures import State
import yaml
import json

import kodo.helper
from kodo.error import DuplicateFlowError, SetupError
import kodo.datatypes
from kodo.log import logger


async def DEFAULT_LANDING_PAGE(form):
    return """
"<p>
    The developer did not yet implement a user 
    interface for this agent. Please check back 
    later.
</p>"
"""


class FlowDecorator:
    def __init__(self, flow, url=None, name=None, description=None):
        self.flow = flow
        self.name = name or "annonymous"
        self.description = description or "no description provided."
        self.tags = []
        self.url = url
        self._register = {
            "welcome": DEFAULT_LANDING_PAGE
        }

    def welcome(self, func=None):
        @functools.wraps(func)
        async def wrapper(form=None, *args, **kwargs):
            if form is None:
                form = {}
            sig = inspect.signature(func)
            bound_args = sig.bind_partial(*args, **kwargs)
            if 'form' in sig.parameters:
                bound_args.arguments['form'] = form
            bound_args.apply_defaults()
            return await func(*bound_args.args, **bound_args.kwargs)

        self._register['welcome'] = wrapper
        return wrapper

    def enter(self, func=None):
        @functools.wraps(func)
        async def wrapper(form=None, *args, **kwargs):
            if form is None:
                form = {}
            sig = inspect.signature(func)
            bound_args = sig.bind_partial(*args, **kwargs)
            if 'form' in sig.parameters:
                bound_args.arguments['form'] = form
            bound_args.apply_defaults()
            return await func(*bound_args.args, **bound_args.kwargs)

        self._register['enter'] = wrapper
        return wrapper

    def get_register(self, key):
        return self._register[key]


def publish(flow, **kwargs):
    return FlowDecorator(flow, **kwargs)


class Loader:

    def __init__(self): 
        self.option = None
        self.flows = []

    def setup(self, **kwargs) -> None:
        """
        setup command line options as iKODO_* environment variables
        (before application launch)
        """
        env = kodo.datatypes.EnvironmentOption()
        self.option = kodo.datatypes.CommandOption(**{
            **{k: v for k, v in env.model_dump().items() if v is not None},
            **{k.upper(): v for k, v in kwargs.items() if v is not None},
        })
        if self.option.URL and isinstance(self.option.CONNECT, list):
            if self.option.URL in self.option.CONNECT:
                self.option.CONNECT.remove(self.option.URL)                    
        #self.state.connection = {str(host): None for host in opt.CONNECT or []}
        if self.option.LOADER:
            if Path(self.option.LOADER).exists():
                if Path(self.option.LOADER).is_file():
                    self.load_option_file()
                else:
                    self.load_option_dir()
            else:
                factory = kodo.helper.parse_factory(self.option.LOADER)
                factory(self)
        for k, v in self.option.model_dump().items():
            if v is not None:
                if isinstance(v, list):
                    v = json.dumps(v)
                else:
                    v = str(v)
                os.environ[f"iKODO_{k}"] = v

    def load_option_str(self) -> None:
        self._load_option(yaml.safe_load(self.option.LOADER))

    def load_option_file(self) -> None:
        with open(self.option.LOADER, "r") as file:
            self._load_option(yaml.safe_load(file))

    def load_option_dir(self) -> None:
        return  

    def _load_option(self, records: List[Dict[str, Any]]) -> None:
        for record in records:
            for key, value in record.items():
                if key.upper() == "FLOWS":
                    self.flows = value
                else:
                    setattr(self.option, key.upper(), value)

    def load(self) -> State:
        """
        load state from iKODO_* environment variables and merge with
        KODO_* environment variables, process the loader option
        """
        env = kodo.datatypes.EnvironmentOption()
        self.option = kodo.datatypes.InternalOption(**{
            **{k: v for k, v in env.model_dump().items() if v is not None},
            **kodo.datatypes.InternalOption().model_dump()
        })
        if self.option.LOADER:
            if Path(self.option.LOADER).exists():
                if Path(self.option.LOADER).is_file():
                    self.load_option_file()
                else:
                    self.load_option_dir()
            else:
                factory = kodo.helper.parse_factory(self.option.LOADER)
                factory(self)
        state = self.default_state()
        for field in self.option.__fields__:
            value = getattr(self.option, field)
            if value is not None:
                setattr(state, field.lower(), value)
        connect = self.option.CONNECT or []
        state.connection = {str(host): None for host in connect or []}
        state.flows = {}
        for flow in self.flows:
            # todo: isolate flow instantiation in a dedicated process
            if isinstance(flow, dict):
                f = kodo.datatypes.Flow(**flow)
            else:
                raise RuntimeError("Flows not implemented, yet")
            state.flows[f.url] = f
        if not state.cache_reset:
            self.load_from_cache(state)
        return state

    def default_state(self) -> State:
        return State({
            # settings:
            "url": None,
            "organization": None,
            "connection": None,
            "registry": None,
            "feed": None,
            "cache_data": None,
            "cache_reset": None,
            "screen_level": None,
            "log_file": None,
            "log_file_level": None,
            "exec_data": None,
            "timeout": None,
            "retry": None,
            # runtime:
            "registers": {},
            "flows": {},
            "providers": {},
            # "entry_points": {},
            "event": 0,
            "heartbeat": None,
            "status": None
        })

    @staticmethod
    def save_to_cache(state: State) -> None:
        dump = kodo.datatypes.ProviderDump(
            url=state.url,
            organization=state.organization,
            connection=state.connection.keys(),
            feed=state.feed,
            providers=state.providers,
            registers=state.registers
        )
        file = Path(state.cache_data)
        with file.open("w") as fh:
            fh.write(dump.model_dump_json())
        logger.debug(f"saved cache {file}")
        
        
    def load_from_cache(self, state) -> bool:
        file = Path(state.cache_data)
        if not file.exists():
            return False
        with file.open("r") as fh:
            data = fh.read()
        dump = kodo.datatypes.ProviderDump.model_validate_json(data)
        for attr in ('url', 'organization', 'feed'):
            cache_value = getattr(dump, attr, None)
            curr_value = getattr(state, attr)
            if cache_value != curr_value:
                logger.warning(f"{attr}: {cache_value} != {curr_value}")
        connection = sorted(state.connection.keys())
        if connection != sorted(dump.connection):
            logger.warning(f"connection: {connection} != {dump.connection}")
        state.providers = dump.providers
        state.registers = {r: None for r in state.registers.keys()}
        return True

    # def flows_dict(self):
    #     return {
    #         f["url"]: kodo.datatypes.Flow(
    #             url=f["url"], 
    #             name=f["name"],
    #             author=f["author"],
    #             description=f["description"],
    #             tags=f["tags"]
    #         )
    #         for f in self.flows.values()
    #     }

    # def entry_points_dict(self):    
    #     return {f["url"]: f["entry_point"] for f in self.flows.values()}

    def add_flow(self, 
               entry_point: Any, 
               name: Optional[str]=None, 
               url: Optional[str]=None,
               description: Optional[str]=None,
               author: Optional[str]=None,
               tags: Optional[List[str]]=None) -> None:
        """
        add flow to .flow by url and instantiate the object
        """
        # todo: this call need to be isolated in a dedicated process
        if isinstance(entry_point, str):
            obj = kodo.helper.parse_factory(entry_point)
        else:
            obj = entry_point
        url = getattr(obj, "url", url)
        name = getattr(obj, "name", name)
        if name is None:
            raise RuntimeError("Flow name is required")
        if url is None:
            url = name.replace(" ", "-")
        if not url.startswith("/"):
            url = "/" + url
        if url.endswith("/"):
            url = url[:-1]
        # if url in self.flows:
        #     raise DuplicateFlowError(f"Flow with name '{url}' already exists")
        self.flows.append({
            "name": name,
            "url": url,
            "description": description,
            "author": author,
            "entry_point": entry_point,
            "tags": tags,
            "instance": obj
        })


def default_loader(self: Loader) -> None:
    pass
