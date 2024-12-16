import asyncio
import datetime
from typing import Callable


def now():
    # use this function for now to align on UTC
    return datetime.datetime.utcnow()


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
        self.timer = sleep
        self.sleep = sleep

    async def wait(self) -> None:
        await asyncio.sleep(self.sleep)
        self.sleep = self.timer if self.sleep > 8 else self.sleep * 2


def parse_factory(loader: str) -> Callable:
    """
    Split the loader string into module and callback and return the imported 
    callback. If no loader is provided, return the default loader.
    """
    module, callback = loader.split(":", 1)
    imported_module = __import__(module, fromlist=[callback])
    return getattr(imported_module, callback)
