import asyncio
import datetime
from typing import Callable


def now():
    # use this function for now to align on UTC
    return datetime.datetime.utcnow()


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

    def __init__(self, sleep: float = 0.5):
        self.max = 8
        self.timer = sleep
        self.sleep = sleep

    async def wait(self) -> None:
        await asyncio.sleep(self.sleep)
        self.sleep = self.max if self.sleep > self.max else self.sleep * 2


def stat(nodes):
    return f"nodes: {len(nodes)}, " \
        f"flows: {sum([len(n.flows) for n in nodes])}"
