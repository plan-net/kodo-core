from typing import Optional
from uuid import uuid4, UUID
import pickle
import httpx
import pandas as pd
from pathlib import Path

from litestar import Litestar, Request, Response, get, post
from litestar.datastructures import State
from litestar.events import listener
from litestar.status_codes import HTTP_200_OK, HTTP_404_NOT_FOUND

import kodo.types
from kodo import helper
from kodo.config import logger, setting
from kodo.service.controller import Controller


# todo: disallow cyclic connections

def _build_provider(state: State) -> kodo.types.ProviderOffer:
    payload = state.nodes
    for provider_url, provider in state.providers.items():
        if provider_url != state.url:
            for node in provider.nodes.values():
                if node.url in payload:
                    continue
                payload[node.url] = node
    return kodo.types.ProviderOffer(
        url=state.url, 
        organization=state.organization,
        feed=state.registry, 
        nodes=payload)


def _default_response(state: State, *message: str) -> Response:
    if message:
        for m in message:
            logger.info(f"client message: {m}")
    return Response(content={
        "url": state.url,
        "organization": state.organization,
        "node": state.node,
        "registry": state.registry,
        "provider": state.provider,
        "connection": state.connection,
        "started_at": state.started_at,
        "idle": state.event == 0,
        "now": helper.now(),
        "message": message
    }, status_code=HTTP_200_OK)


async def _connect(endpoint, state, url, **kwargs) -> dict | None:
    logger.info(f"{state.url} establishes connection ({endpoint})")
    backoff = helper.Backoff()
    while True:
        client = httpx.AsyncClient()
        try:
            response = await client.post(
                endpoint, timeout=setting.REQUEST_TIMEOUT, **kwargs)
            if response.status_code == HTTP_200_OK:
                state.connection[url] = helper.now()
                logger.info(f"{state.url} successfully connected to {url}")
                return response.json()
            elif response.status_code == HTTP_404_NOT_FOUND:
                logger.error(f"{state.url} failed to connect to {url}, "
                             f"which is not a regsitry/provider")
                return None
        except:
            pass
        await backoff.wait()


def _get_peers(state: State) -> list[kodo.types.Provider]:
    peers = []
    for provider in state.providers.values():
        if provider.url != state.url:
            peers.append(provider)
    if peers:
        peer_list = ", ".join([p.url for p in peers])
        logger.info(f"{state.url} has {len(peers)} peers: {peer_list}")
    return peers


def _emit(app, event, *args, **kwargs) -> None:
    # emit litestar application event and increase the semaphore to identify
    # the number of open events
    app.state.event += 1
    app.emit(event, *args, **kwargs)
    logger.info(f"increase semaphore to {app.state.event}")


def _release(state: State):
    state.event -= 1
    logger.info(f"decrease semaphore to {state.event}")


async def _cache_data(state: State) -> None:
    dump = kodo.types.ProviderDump(
        url=state.url,
        organization=state.organization,
        feed=state.registry,
        nodes=state.nodes,
        providers=state.providers
    )
    Path(state.cache).open("w").write(dump.model_dump_json(indent=2))
    # pickle.dump({
    #     "providers": state.providers,
    #     "nodes": state.nodes
    # }, Path(state.cache).open("wb"))

    # with Path(state.cache).open("rb") as file:
    #     data = pickle.load(file)
    # return kodo.types.ProviderDump(
    #     url=state.url,
    #     organization=state.organization,
    #     feed=state.registry,
    #     nodes=data["nodes"],
    #     providers=data["providers"]
    # )


# event listeners

@listener("connect_node")
async def connect_node(url, state) -> None:
    """
    Connects the node with the registry or provider. No response is expected.
    This event is triggered at node .startup().
    """
    data = kodo.types.Node(
        url=state.url, 
        organization=state.organization, 
        flows=state.flows,
        status=state.status)
    await _connect(f"{url}/register", state, url, data=data.model_dump_json())
    _release(state)
    logger.info(f"{state.url} node startup complete")


@listener("connect_registry")
async def connect_registry(url, state) -> None:
    """
    Connects the registry or provider with another registry and delivers the
    masqeraded ProviderOffer. In return the other registry delivers it's 
    ProviderOffer which is saved to state.providers.
    This event is triggered at node .startup().
    """
    data = _build_provider(state)
    response = await _connect(
        f"{url}/connect", state, url, data=data.model_dump_json())
    # if this is a registry, then send response if requested
    if state.registry:
        # a registry expects response
        if isinstance(response, dict):
            feedback = kodo.types.ProviderOffer(**response["providers"])
            modified = helper.now()
            if feedback.url in state.providers:
                created = state.providers[feedback.url].created
            else:
                created = modified
            if feedback.url == state.url:
                raise RuntimeError("unexpected system behavior")
            state.providers[feedback.url] = kodo.types.Provider(
                created=created, modified=modified, heartbeat=modified,
                **response["providers"])
    _release(state)
    await _cache_data(state)
    logger.info(f"{state.url} registry startup complete")



@listener("update_node")
async def update_node(
    url: str, 
    state: State, 
    record: kodo.types.Node | kodo.types.ProviderOffer) -> None:
    """
    Forwards a node (reconnect) received with /register or a node update
    received from a provider and packaged in ProviderOffer to a connected 
    providers.
    """
    if isinstance(record, kodo.types.Node):
        record = kodo.types.ProviderOffer(
            url=state.url,
            organization=state.organization,
            feed=False,  # no response expected at this stage
            nodes={record.url: record}
        )
    await _connect(
        f"{url}/update/node", state, url, data=record.model_dump_json())
    await _cache_data(state)
    _release(state)
    logger.info(f"{state.url} update from node {url} completed")


@listener("update_registry")
async def update_registry(
    url: str, 
    state: State, 
    record: kodo.types.Provider) -> None:
    """
    Forwards a provider update received with /connect to all connected 
    providers.
    """
    await _connect(
        f"{url}/update/registry", state, url, data=record.model_dump_json())
    await _cache_data(state)
    _release(state)
    logger.info(f"{state.url} update from registry {url} completed")


# controller

class NodeConnector(Controller):
    path = "/"

    @staticmethod
    def startup(app: Litestar) -> None:
        app.state.started_at = helper.now()
        NodeConnector.connect_registry(app)
        NodeConnector.node_discovery(app)

    @staticmethod
    def connect_registry(app: Litestar) -> None:
        if not app.state.connection:
            return
        if app.state.registry:
            # registry event
            event = "connect_registry"
        else:
            # node event
            event = "connect_node"
        for url in app.state.connection:
            _emit(app, event, url, app.state)

    @staticmethod
    def node_discovery(app: Litestar) -> None:
        # todo: implement
        if not app.state.node:
            return
        logger.info("node discovery")
        return

    @staticmethod
    def shutdown(app: Litestar) -> None:
        logger.info("shutdown now")

    @get()
    async def home(self, request: Request, state: State) -> Response:
        return _default_response(state)

    # providers only

    @get("/map")
    async def providers_map(self, state: State) -> kodo.types.ProviderDump:
        return kodo.types.ProviderDump(
            url=state.url,
            organization=state.organization,
            feed=state.registry,
            nodes=state.nodes,
            providers=state.providers
        )

    # nodes only

    @post("/register")
    async def register(
            self,
            state: State,
            request: Request,
            data: Optional[kodo.types.Node] = None) -> Response:
        """
        Register a node with the registry, receives Node, saves
        Node in state.nodes and returns status.
        """
        if not state.registry:
            logger.warning(f"{state.url} is not a registry")
            return Response(content={}, status_code=HTTP_404_NOT_FOUND)
        message = []
        if data:
            data.modified = helper.now()
            if data.url in state.nodes:
                previous = state.nodes[data.url].created
                data.created = state.nodes[data.url].created
                message.append(
                    f"registered node {data.url} (seen previously {previous})")
            else:
                data.created = data.modified
                message.append(
                    f"registered node {data.url} (first visit)")
            data.heartbeat = data.modified
            state.nodes[data.url] = kodo.types.Node(**data.model_dump())
            for provider in _get_peers(state):
                if state.url == provider.url:
                    raise RuntimeError("unexpected system behavior")
                _emit(request.app, "update_node", provider.url, state, data)
                message.append(f"feed forward node to {provider.url}")
        return _default_response(state, *message)

    # providers and registries only

    @post("/connect")
    async def connect(
            self,
            state: State,
            request: Request,
            data: Optional[kodo.types.ProviderOffer] = None) -> Response:
        """
        Connect a registry or provider with the registry, receive
        ProviderOffer, save Provider into state.providers and return the
        masqueraded ProviderOffer. Informs other providers about the update.
        """
        if not state.registry:
            logger.warning(f"{state.url} is not a registry")
            return Response(content={}, status_code=HTTP_404_NOT_FOUND)
        message = []
        if data:
            modified = helper.now()
            if data.url in state.providers:
                previous = state.providers[data.url].created
                created = state.providers[data.url].created
                message.append(
                    f"registry {data.url} connected "
                    f"(seen previously {previous})")
            else:
                created = modified
                message.append(f"registry {data.url} connected (first visit)")
            # create Provider from ProviderOffer
            record = kodo.types.Provider(
                created=created, 
                modified=modified, 
                heartbeat=modified,
                connect=True,
                **data.model_dump())

            # save in own registry
            state.providers[data.url] = record
            await _cache_data(state)

            masquerade = kodo.types.Provider(**record.model_dump())
            masquerade.url = state.url
            # inform connected registries
            for provider in _get_peers(state):
                # skip current client
                if provider.url != record.url:
                    _emit(
                        request.app, "update_registry",
                        provider.url, state, masquerade)
                    message.append(
                        f"feed forward {record.url} to {provider.url}")
            if record.feed:
                # client requests response (is a registry)
                logger.info(f"{state.url} return is providers")
                return Response(
                    content={
                        "providers": _build_provider(state).model_dump(),
                        "message": message
                    }, status_code=HTTP_200_OK)
        return _default_response(state, *message)

    # all (nodes, providers, registries)

    @get("/flows")
    async def flows(self, state: State, dedup: bool = True) -> Response:
        """
        Return all flows from the nodes and providers masquerading the
        sourcing registry. Returns a pandas DataFrame.
        """
        source = None
        if state.registry:
            source = _build_provider(state)
        else:
            source = kodo.types.ProviderOffer(
                url=state.url, 
                feed=False, nodes={
                    state.url: kodo.types.Node(
                        url=state.url, 
                        organization=state.organization,
                        flows=state.flows,
                        status=state.status)
                })
        rows = []
        for node in source.nodes.values():
            for url, flow in node.flows.items():
                rows.append({
                    "source": state.url,
                    "node": node.url,
                    "created": node.created,
                    "modified": node.modified,
                    "heartbeat": node.heartbeat,
                    "url": flow.url,
                    "name": flow.name,
                    "author": flow.author,
                    "description": flow.description,
                    "tags": flow.tags
                })
        df = pd.DataFrame(rows)
        if not df.empty:
            #df.sort_values(["source", "node", "url"], inplace=True)
            df.sort_values(["name", "node", "url"], inplace=True)
        logger.info(f"{state.url} retrieved {df.shape} flows")
        return Response(content=df.to_dict(), status_code=HTTP_200_OK)

    # registries and providers

    @post("/update/node")
    async def post_node(
        self,
        state: State,
        request: Request,
        data: Optional[kodo.types.ProviderOffer] = None) -> Response:
        """
        update node data from provider
        """
        if not state.registry:
            logger.warning(f"{state.url} is not a registry")
            return Response(content={}, status_code=HTTP_404_NOT_FOUND)
        message = []
        if data:
            modified = helper.now()
            if data.url in state.providers:
                provider = state.providers[data.url]
                # iterate over new nodes
                for node in data.nodes.values():
                    if node.url in provider.nodes:
                        message.append(f"updated node {data.url}")
                    else:
                        message.append(f"created node {data.url}")
                    provider.nodes[node.url] = node
            else:
                raise RuntimeError("unexpected system behavior")

            masquerade = kodo.types.ProviderOffer(**data.model_dump())
            masquerade.url = state.url
            # inform connected registries
            for provider in _get_peers(state):
                # skip current client
                if provider.url == state.url:
                    raise RuntimeError("unexpected system behavior")
                elif provider.url != data.url:
                    _emit(
                        request.app, "update_node",
                        provider.url, state, masquerade)
                    message.append(f"feed forward {
                                   data.url} to {provider.url}")

        return _default_response(state, *message)

    @post("/update/registry")
    async def post_registry(
        self, 
        state: State, 
        data: Optional[kodo.types.Provider] = None) -> Response:
        """
        update registry data from provider
        """
        if not state.registry:
            logger.warning(f"{state.url} is not a registry")
            return Response(content={}, status_code=HTTP_404_NOT_FOUND)
        message = []
        if data:
            modified = helper.now()
            if data.url in state.providers:
                created = state.providers[data.url].created
                message.append(f"updated provider {data.url}")
            else:
                created = modified
                message.append(f"updated provider {data.url}")
            state.providers[data.url] = data
        return _default_response(state, *message)

    @get("/flow/{path:path}")
    async def visit_flow(self, path: str, state: State) -> Response:
        if state.registry:
            return Response(content={}, status_code=HTTP_404_NOT_FOUND)
        if path in state.entry_points:
            # todo: call flow's method decorated with @welcome
            entry = state.entry_points[path]
            return Response(content={"entry_point": entry}, status_code=HTTP_200_OK)
        return Response(content={}, status_code=HTTP_404_NOT_FOUND)

    @post("/flow/create/{path:path}")
    async def create_flow_execution(self, path: str, state: State) -> Response:
        if state.registry:
            return Response(content={}, status_code=HTTP_404_NOT_FOUND)
        entry = state.entry_points.get(path)
        # todo: call flow's method decorated with @entry
        #       detach from parent process
        #       return fid
        # todo: persist the flow with fid
        fid = uuid4()
        return Response(content={
            "entry_point": entry,
            "fid": fid
        }, status_code=HTTP_200_OK)
        # raise Forbidden if prerequisites fail

    @post("/flow/launch/{fid:uuid}")
    async def launch_flow(self, fid: UUID, state: State) -> Response:
        if state.registry:
            return Response(content={}, status_code=HTTP_404_NOT_FOUND)
        # todo: load flow from fid
        return Response(content={"fid": fid}, status_code=HTTP_200_OK)
        # raise Forbidden if prerequisites fail