import asyncio
from typing import Optional

import httpx
from litestar.datastructures import State
from litestar.events import listener
from litestar.exceptions import NotFoundException
from litestar.status_codes import (HTTP_200_OK, HTTP_201_CREATED,
                                   HTTP_404_NOT_FOUND)

import kodo.datatypes
import kodo.helper as helper
import kodo.service.controller
import kodo.service.signal
from kodo.log import logger


async def send(endpoint, retry=None, **kwargs) -> dict:
    backoff = helper.Backoff()
    trial = 0
    while retry in (None, -1) or trial < retry:
        trial += 1
        client = httpx.AsyncClient()
        try:
            response = await client.post(endpoint, **kwargs)
            if response.status_code in (HTTP_200_OK, HTTP_201_CREATED):
                logger.debug(f"succeed with {endpoint}")
                return response.json()
        except:
            logger.warning(f"failed with {endpoint}")
        await backoff.wait()
    logger.error(f"failed with {endpoint}")
    return {}


def emit(app, event, *args, **kwargs) -> None:
    app.state.event += 1
    app.emit(event, *args, **kwargs)
    logger.debug(f"semaphore +1 to {app.state.event} for {event}")


def release(state: State) -> None:
    state.event -= 1
    logger.debug(f"semaphore -1 to {state.event}")


@listener("connect")
async def connect(url, state) -> None:
    # outbound data
    default = kodo.service.controller.default_response(state)
    nodes = kodo.service.controller.build_registry(state)
    logger.info(f"{url}/connect with {helper.stat(nodes)}")
    resp = await send(
        f"{url}/connect", 
        data=kodo.datatypes.Connect(
            **default.model_dump(), nodes=nodes).model_dump_json(),
        retry=state.retry,
        timeout=state.timeout)
    state.connection[url] = helper.now()
    if state.feed:
        feedback = kodo.datatypes.Connect(**resp)
        modified = helper.now()
        if feedback.url in state.providers:
            created = state.providers[feedback.url].created
        else:
            created = modified
        provider = kodo.datatypes.Provider(
            url=feedback.url,
            organization=feedback.organization,
            feed=feedback.feed,
            created=created,
            modified=modified,
            nodes=feedback.nodes
        )
        state.providers[provider.url] = provider
        logger.info(f"feedback from {feedback.url} with {helper.stat(nodes)}")
    kodo.service.signal.release(state)
    logger.debug(f"{url}/connect complete")


@listener("update")
async def update(
        url: str,
        state: State,
        record: kodo.datatypes.Connect) -> None:
    logger.debug(f"update to {url} on {record.url}")
    resp = await send(
        f"{url}/update", 
        data=record.model_dump_json(),
        timeout=state.timeout)
    kodo.service.signal.release(state)
    logger.debug(f"{url}/update complete")


@listener("reconnect")
async def reconnect(url, state: State) -> None:
    # outbound data
    logger.info(f"{url}/reconnect")
    default = kodo.service.controller.default_response(state)
    default.message.append(f"please reconnect")
    resp = await send(
        f"{url}/reconnect", 
        data=default.model_dump_json(),
        retry=state.retry,
        timeout=state.timeout)
    kodo.service.signal.release(state)
    logger.debug(f"{url}/reconnect complete")
