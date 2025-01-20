from typing import List, Optional

from kodo.worker.handler import FlowHandler


class Launch:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.inputs = kwargs


def publish(
        flow,
        url: str,
        name: str,
        description: str,
        author: Optional[str] = None,
        tags: Optional[List[str]] = None) -> FlowHandler:
    return FlowHandler(
        flow, url=url, name=name, description=description,
        author=author, tags=tags)
