from typing import Union, Optional, Literal, List
import signal
import sys

from litestar import Litestar, Request, get, post, delete, Response
from litestar.datastructures import State
from litestar.exceptions import NotFoundException
from litestar.status_codes import HTTP_201_CREATED
import httpx

import kodo
import kodo.helper as helper
from kodo.log import logger
import kodo.service.controller
import kodo.service.signal
import kodo.worker.loader
from kodo.service.flow import build_df, sort_df, filter_df


def signal_handler(signal, frame):
    print(f' -- interrupt triggers shutdown')
    sys.exit(4)


class NodeConnector(kodo.service.controller.Controller):
    path = "/"

    @staticmethod
    def startup(app: Litestar) -> None:
        app.state.started_at = helper.now()
        for url in app.state.connection:
            kodo.service.signal.emit(app, "connect", url, app.state)
        if app.state.registry:
            for provider in app.state.providers.values():
                kodo.service.signal.emit(
                    app, "reconnect", provider.url, app.state)
            logger.info(
                f"registry startup complete (feed is {app.state.feed})")
        else:
            logger.info("node startup complete")
        signal.signal(signal.SIGINT, signal_handler)

    @staticmethod
    def shutdown(app: Litestar) -> None:
        logger.info(f"shutdown now")

    @get("/",
        summary="Node Status Overview",
        description="Returns general state information about the Kodosumi registry or node, including startup time and status.",
        tags=["Status", "Registry"],
        response_model=kodo.datatypes.DefaultResponse)
    async def home(
            self,
            request: Request,
            state: State) -> kodo.datatypes.DefaultResponse:
        return kodo.service.controller.default_response(state)

    @get("/map",
        summary="Provider and Connection Map",
        description="Provides detailed runtime data on registered providers, active connections, and node registers within the Kodosumi system.",
        tags=["Monitoring", "Registry"],
        response_model=kodo.datatypes.ProviderMap)
    async def get_map(self, state: State) -> kodo.datatypes.ProviderMap:
        default = kodo.service.controller.default_response(state).model_dump()
        default["providers"] = state.providers.values()
        default["connection"] = state.connection
        default["registers"] = state.registers
        logger.debug(
            f"return /map providers: {len(state.providers.values())}, "
            f"registers: {len(state.registers)}, "
            f"and connection: {len(state.connection)}")
        return kodo.datatypes.ProviderMap(**default)

    @get("/connect",
        summary="Registry Node Connections (Preview)",
        description="Provides a preview of connected registry nodes and their current data within the Kodosumi system.",
        tags=["Connections", "Registry"],
        response_model=kodo.datatypes.Connect)
    async def get_connect(self, state: State) -> kodo.datatypes.Connect:
        default = kodo.service.controller.default_response(state)
        nodes = kodo.service.controller.build_registry(state)
        logger.debug(f"return /connect with {helper.stat(nodes)}")
        return kodo.datatypes.Connect(**default.model_dump(), nodes=nodes)

    @post("/connect",
        summary="Establish Connection to Registry",
        description="Connect a node or registry to the Kodosumi registry. Updates the registry state and synchronizes with peers if applicable.",
        tags=["Connections", "Registry"],
        response_model=Union[kodo.datatypes.Connect, kodo.datatypes.DefaultResponse])
    async def connect(
            self,
            state: State,
            request: Request,
            data: kodo.datatypes.Connect) -> Union[
                kodo.datatypes.Connect, kodo.datatypes.DefaultResponse]:
        # inbound data
        if not state.registry:
            raise NotFoundException()
        modified = helper.now()
        if data.url in state.providers:
            created = state.providers[data.url].created
        else:
            created = modified
        provider = kodo.datatypes.Provider(
            url=data.url,
            organization=data.organization,
            feed=data.feed,
            created=created,
            modified=modified,
            nodes=data.nodes
        )
        state.providers[provider.url] = provider
        state.registers[provider.url] = modified
        kodo.worker.loader.Loader.save_to_cache(state)
        logger.info(
            f"connect from {data.url} with {helper.stat(data.nodes)}, "
            f"feed {data.feed} to organization {data.organization}")
        default = kodo.service.controller.default_response(state)

        if state.feed:
            # peers update
            feed = kodo.datatypes.Connect(
                **default.model_dump(), nodes=data.nodes)
            for peer in state.providers.values():
                if peer.feed:
                    if peer.url != data.url:
                        logger.debug(
                            f"broadcast {peer.url}/connect "
                            f"with {helper.stat(data.nodes)}")
                        kodo.service.signal.emit(
                            request.app, "update", peer.url, state, feed)
        # outbound data
        if data.feed:
            if not state.registry:
                raise NotFoundException()
            nodes = kodo.service.controller.build_registry(state)
            logger.debug(f"return to {data.url} with {helper.stat(nodes)}")
            return kodo.datatypes.Connect(**default.model_dump(), nodes=nodes)
        return default

    @post("/disconnect",
        summary="Disconnect from Registry",
        description="Disconnects a provider or specific nodes from the Kodosumi registry and updates peers if necessary.",
        tags=["Connections", "Registry"],
        response_model=kodo.datatypes.DefaultResponse,
        status_code=201)
    async def godown(
            self,
            state: State,
            request: Request,
            data: kodo.datatypes.Disconnect) -> kodo.datatypes.DefaultResponse:
        logger.info(f"/disconnect from {data.provider}")
        if data.provider in state.providers:
            provider = state.providers[data.provider]
            if provider.url in data.url:
                data.url = [
                    node.url for node in state.providers[data.provider].nodes
                ]
                del state.providers[data.provider]
                logger.info(f"removed provider: {data.provider}")
            else:
                before = len(provider.nodes)
                provider.nodes = [
                    node for node in provider.nodes if node.url not in data.url
                ]
                if len(provider.nodes) != before:
                    logger.info(
                        f"removed nodes: {before - len(provider.nodes)}")
            kodo.worker.loader.Loader.save_to_cache(state)
            logger.debug(f"{state.feed}")
            if state.feed:
                for peer in state.providers.values():
                    if peer.feed and peer.url != data.provider:
                        logger.debug(
                            f"broadcast {peer.url}/disconnect of {data.url}")
                        try:
                            resp = httpx.post(
                                f"{peer.url}/disconnect",
                                json=kodo.datatypes.Disconnect(
                                    provider=state.url,
                                    url=data.url).model_dump(),
                                timeout=None)
                            logger.info(
                                f"disconnect from {peer.url}: {resp.json()}")
                        except:
                            logger.error(f"disconnect from {peer.url} failed")
        else:
            raise NotFoundException
        return kodo.service.controller.default_response(state)

    @delete("/connect",
        summary="Disconnect All Active Connections",
        description="Forcefully disconnects the node from all active connections in the Kodosumi registry.",
        tags=["Connections", "Registry"],
        status_code=204
        )
    async def disconnect(
            self,
            state: State,
            request: Request) -> None:
        # if not state.connection:
        #     logger.info("No active connections to disconnect.")
        #     return kodo.datatypes.DefaultResponse(
        #         url=state.url,
        #         registry=state.registry,
        #         feed=state.feed,
        #         idle=True,
        #         now=helper.now(),
        #         message=["No active connections to disconnect."]
        # )
        for url in state.connection.keys():
            try:
                resp = httpx.post(
                    f"{url}/disconnect",
                    json=kodo.datatypes.Disconnect(
                        provider=state.url,
                        url=[state.url]).model_dump(),
                    timeout=None)
                logger.debug(resp.status_code)
                if resp.status_code != HTTP_201_CREATED:
                    logger.error(
                        f"disconnect from {url} failed: {resp.json()}")
            except Exception as e:
                logger.error(f"disconnect from {url} failed: {e}")

        logger.info("Successfully disconnected from all connections.")
        
    @post("/reconnect",
        summary="Reconnect to Registry",
        description="Reconnects a node or registry to the Kodosumi network by emitting a connection signal.",
        tags=["Connections", "Registry"],
        status_code=200)
    async def reconnect(
            self,
            state: State,
            request: Request,
            data: kodo.datatypes.DefaultResponse) -> kodo.datatypes.DefaultResponse:
        logger.info(f"Reconnecting to registry at {data.url}")
        try:
            kodo.service.signal.emit(request.app, "connect", data.url, state)
            return kodo.service.controller.default_response(state)
        except Exception as e:
            logger.error(f"Failed to reconnect to {data.url}: {e}")
            return kodo.datatypes.DefaultResponse(
                url=state.url,
                registry=state.registry,
                feed=state.feed,
                idle=True,
                now=helper.now(),
                message=[f"Failed to reconnect to {data.url}: {e}"]
            )

    @post("/update",
        summary="Update Node Information",
        description="Updates the node data in the Kodosumi registry and synchronizes the changes with peers if applicable.",
        tags=["Connections", "Registry"],
        response_model=kodo.datatypes.DefaultResponse,
        status_code=200)
    async def update(
            self,
            state: State,
            request: Request,
            data: kodo.datatypes.Connect) -> kodo.datatypes.DefaultResponse:
        # inbound data
        if not state.registry:
            raise NotFoundException()
        modified = helper.now()
        if data.url not in state.providers:
            raise NotFoundException(detail=f"Provider {data.url} not found in the registry.")
        logger.info(f"update from {data.url} with {helper.stat(data.nodes)}")
        # shuffle in new node data
        for node in data.nodes:
            idx = [i for i, j in enumerate(
                state.providers[data.url].nodes) if j.url == node.url
            ]
            if idx:
                state.providers[data.url].nodes[idx[0]] = node
                logger.debug(f"update [{idx[0]}] on {
                             node.url} from {data.url}")
            else:
                state.providers[data.url].nodes.append(node)
                logger.debug(f"insert on {node.url} from {data.url}")
        kodo.worker.loader.Loader.save_to_cache(state)
        default = kodo.service.controller.default_response(state)
        if state.feed:
            # peers update
            feed = kodo.datatypes.Connect(
                **default.model_dump(), nodes=data.nodes)
            MAX_RETRIES = 3
            for peer in state.providers.values():
                if peer.feed and peer.url != data.url:
                    for attempt in range(MAX_RETRIES):
                        try:
                            logger.debug(f"Attempt {attempt + 1}: broadcasting /update to {peer.url}")
                            kodo.service.signal.emit(request.app, "update", peer.url, state, feed)
                            break  # Exit loop if successful
                        except Exception as e:
                            logger.error(f"Attempt {attempt + 1} failed to update {peer.url}: {e}")
                            if attempt == MAX_RETRIES - 1:
                                logger.error(f"Max retries reached for {peer.url}. Giving up.")
        return default

    @get("/flows",
        summary="Retrieve Active Flows",
        description=(
         "Returns all active flows from nodes and providers in the Kodosumi registry. "
         "Supports filtering, sorting, and pagination.\n\n"
         "**Query Parameters:**\n"
         "- `q` (str, optional): Filter flows by a search query.\n"
         "- `by` (str, optional): Field to sort flows by.\n"
         "- `pp` (int, default=10): Items per page for pagination.\n"
         "- `p` (int, default=0): Page number for pagination.\n"
         "- `format` (json or html, optional): Response format (JSON or HTML)."
        ),
        tags=["Monitoring", "Flows"])
    async def flows(
            self,
            state: State,
            request: Request,
            q: Optional[str] = None,
            by: Optional[str] = None,
            pp: int = 10,
            p: int = 0,
            format: Optional[Literal["json", "html"]] = None) -> Response:
        """
        Return all flows from the nodes and providers masquerading the
        sourcing registry. Returns a pandas DataFrame.
        """
        df = build_df(state)
        total = df.shape[0]
        df, query = filter_df(df, q)
        filtered = df.shape[0]
        df, sort_by = sort_df(df, by)
        df.reset_index(drop=True, inplace=True)
        df = df.iloc[p * pp: (p + 1) * pp]
        logger.debug(
            f"return /flows with page {p}/{int(total/pp)} "
            f"and {df.shape[0]}/{total} records")
        if (("text/html" in request.headers.get("accept", "")
             and format != "json") | (format == "html")):
            return Response(content=df.to_html(), media_type="text/html")
        return Response(content={
            "total": total,
            "filtered": filtered,
            "p": p,
            "pp": pp,
            "items": df.to_dict('records'),
            "by": ", ".join(sort_by),
            "q": query
        })

    @get("/counts",
        summary="Retrieve Node and Flow Counts",
        description="Provides total counts of nodes and flows grouped by organization and tags in the Kodosumi registry.",
        tags=["Monitoring", "Statistics"])
    async def counts(
            self, 
            state: State, 
            request: Request,
            format: Optional[Literal["json", "html"]] = None) -> Response:
        df = build_df(state)
        return Response(
            content={
                "total": df.shape[0],
                "organization": df.groupby(
                    ["organization"]).name.nunique().to_dict(),
                "tags": df.explode("tags").groupby(
                    "tags").name.nunique().to_dict()
            }
        )
    