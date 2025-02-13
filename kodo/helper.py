import asyncio
import datetime
import socket
import sys
from typing import Callable, List

from litestar import MediaType, Request
from litestar.exceptions import HTTPException
from litestar.status_codes import HTTP_400_BAD_REQUEST


def check_ray(server):
    host, port = server.split(":")
    port = int(port)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(5)
        return sock.connect_ex((host, port)) == 0


def parse_factory(entry_point: str) -> Callable:
    """
    Split the loader string into module and callback and return the imported 
    callback. If no loader is provided, return the default loader.
    """
    module_name, obj = entry_point.split(":", 1)
    module = __import__(module_name)
    components = module_name.split('.')
    for comp in components[1:]:
        module = getattr(module, comp)
    return getattr(module, obj)


def clean_url(url: str) -> str:
    return url if url.startswith("/") else f"/{url}"


class Backoff:
    """
    Implements an exponential backoff mechanism for retrying operations.

    Attributes:
    -----------
    timer : float
        The initial sleep time in seconds.
    sleep : float
        The current sleep time in seconds.
    """

    def __init__(self, sleep: float = 0.5, maximum: float = 8):
        self.max = maximum
        self.timer = sleep
        self.sleep = sleep

    async def wait(self) -> None:
        await asyncio.sleep(self.sleep)
        self.sleep = self.max if self.sleep > self.max else self.sleep * 2


def stat(nodes):
    return f"nodes: {len(nodes)}, flows: {sum([len(n.flows) for n in nodes])}"



def wants_html(request: Request) -> bool:
    wanted_type = request.query_params.get("format", None)
    if wanted_type:
        if wanted_type == "json":
            return False
        elif wanted_type == "html":
            return True
        else:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Invalid format parameter, not json/html.")
    provided_types: List[str] = [MediaType.JSON, MediaType.HTML]
    preferred_type = request.accept.best_match(
        provided_types, default=MediaType.JSON) or MediaType.JSON
    return preferred_type == MediaType.HTML