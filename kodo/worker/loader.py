import functools
import inspect
import os
from typing import Any, Callable, Dict, Optional, List
import pickle
from pathlib import Path

import kodo.helper
from kodo.error import DuplicateFlowError
import kodo.types


async def DEFAULT_LANDING_PAGE():
    return """
"<p>
    The developer did not yet implement a user 
    interface for this agent. Please check back 
    later.
</p>"
"""

def publish(flow, **kwargs):
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
            async def wrapper(*args, **kwargs):
                #print(f"[WELCOME] Flow '{self.flow}': Executing {func.__name__}...")
                return await func(*args, **kwargs)
            self._register['welcome'] = wrapper
            return wrapper

        def get_register(self, key):
            return self._register[key]


    return FlowDecorator(flow, **kwargs)


class Loader:

    def __init__(self): 
        self.flows = {}
        # self.nodes = {}
        self.providers = {}

    def reload(self):
        # todo: discover flows
        pass

    def load_from_cache(self, filename: str):
        if Path(filename).exists():
            with Path(filename).open("r") as file:
                data = file.read()
            dump = kodo.types.ProviderDump.model_validate_json(data)
            self.providers = dump.providers


    def flows_dict(self):
        return {
            f["url"]: kodo.types.Flow(
                url=f["url"], 
                name=f["name"],
                author=f["author"],
                description=f["description"],
                tags=f["tags"]
            )
            for f in self.flows.values()
        }

    # def nodes_dict(self):
    #     return {}

    def providers_dict(self):
        return self.providers

    def entry_points_dict(self):    
        return {f["url"]: f["entry_point"] for f in self.flows.values()}

    def add_flow(self, 
               name: str, 
               url: str,
               entry_point: Any, 
               description: Optional[str]=None,
               author: Optional[str]=None,
               tags: Optional[List[str]]=None) -> None:
        """
        add flow to .flow by url and instantiate the object
        """
        if url is None:
            url = name.replace(" ", "-")
        if not url.startswith("/"):
            url = "/" + url
        if url.endswith("/"):
            url = url[:-1]
        if url in self.flows:
            raise DuplicateFlowError(f"Flow with name '{url}' already exists")
        if isinstance(entry_point, str):
            obj = kodo.helper.parse_factory(entry_point)
        else:
            obj = entry_point
        self.flows[url] = {
            "name": name,
            "url": url,
            "description": description,
            "author": author,
            "entry_point": entry_point,
            "tags": tags,
            "instance": obj
        }
