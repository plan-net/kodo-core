from typing import List

from litestar import Controller
from litestar.datastructures import State

import kodo.helper as helper
from kodo.datatypes import DefaultResponse, Node
from kodo.log import logger


class BaseController(Controller):

    @property
    def qualname(self) -> str:
        return ":".join([
            self.__class__.__module__, self.__class__.__name__])


def default_response(
        state: State,
        *message: str) -> DefaultResponse:
    if message:
        for m in message:
            logger.info(f"{state.url}: client message: {m}")
    return DefaultResponse(**{
        "url": state.url,
        "organization": state.organization,
        "registry": state.registry,
        "feed": state.feed,
        "connection": state.connection,
        "started_at": state.started_at,
        "idle": state.event == 0,
        "now": helper.now(),
        "message": message,
        "ray": state.ray
    })


def build_registry(state: State) -> List[Node]:
    nodes = {}
    if state.flows:
        nodes[state.url] = Node(
            url=state.url,
            organization=state.organization,
            created=state.started_at,
            modified=state.started_at,
            flows=list(state.flows.values())
        )
    for provider in state.providers.values():
        for node in provider.nodes:
            if node.url not in nodes:
                node.created = provider.created
                node.modified = provider.modified
                nodes[node.url] = node
    return list(nodes.values())
