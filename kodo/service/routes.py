import datetime
import signal
import sys
from typing import List, Literal, Optional, Union
from litestar import Litestar, Request, get, post, delete, Response, MediaType
from litestar.datastructures import State
from litestar.exceptions import NotFoundException, HTTPException
from litestar.response import Template
from litestar.enums import RequestEncodingType
from litestar.status_codes import (
    HTTP_201_CREATED,
    HTTP_204_NO_CONTENT,
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
    HTTP_200_OK
)
import httpx

import kodo
import kodo.helper as helper
import kodo.service.controller
import kodo.service.signal
from kodo.datatypes import MODE
import kodo.worker.loader
from kodo.worker.act import FlowAction
from kodo.datatypes import (IPCresult, WorkerMode, DefaultResponse, 
                            ProviderMap, Connect, Provider, Disconnect, 
                            NodeInfo)
from kodo.log import logger
from kodo.service.flow import build_df, filter_df, sort_df, flow_welcome_url


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
         description=("Returns general state information about the Kodosumi "
                      "registry or node, including startup time and status."),
         tags=["Status", "Registry", "Node"],
         response_model=kodo.datatypes.DefaultResponse)
    async def home(
            self,
            request: Request,
            state: State) -> DefaultResponse:
        return kodo.service.controller.default_response(state)

    @get("/map",
         summary="Provider and Connection Map",
         description=("Provides detailed runtime data on registered providers, active "
                      "connections, and node registers within the Kodosumi system."),
         tags=["Monitoring", "Registry", "Node"],
         response_model=kodo.datatypes.ProviderMap,
         status_code=HTTP_200_OK)
    async def get_map(self, state: State) -> ProviderMap:
        default = kodo.service.controller.default_response(state).model_dump()
        default["providers"] = state.providers.values()
        default["connection"] = state.connection
        default["registers"] = state.registers
        logger.debug(
            f"return /map providers: {len(state.providers.values())}, "
            f"registers: {len(state.registers)}, "
            f"and connection: {len(state.connection)}")
        return ProviderMap(**default)

    @get("/connect",
         summary="Registry Node Connections (Preview)",
         description=("Provides a preview of connected registry nodes "
                      "and their current data within the Kodosumi system."),
         tags=["Connections", "Registry", "Node"],
         response_model=kodo.datatypes.Connect,
         status_code=HTTP_200_OK)
    async def get_connect(self, state: State) -> Connect:
        default = kodo.service.controller.default_response(state)

        nodes = kodo.service.controller.build_registry(state)
        logger.debug(f"Returning /connect with {helper.stat(nodes)}")

        return kodo.datatypes.Connect(**default.model_dump(), nodes=nodes)

    @post("/connect",
          summary="Establish Connection to Registry",
          description=("Connect a node or registry to a Kodosumi registry. "
                       "Updates the registry state and synchronizes with peers if applicable."),
          tags=["Connections", "Registry", "Node"],
          response_model=Union[kodo.datatypes.Connect,
                               kodo.datatypes.DefaultResponse],
          status_code=HTTP_200_OK)
    async def connect(
            self,
            state: State,
            request: Request,
            data: Connect) -> Union[Connect, DefaultResponse]:
        # inbound data
        logger.info(f"Attempting to connect node {data.url} to the registry.")
        if not state.registry:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Connection attempt failed: Node is not a registry."
            )
        modified = helper.now()

        if data.url in state.providers:
            created = state.providers[data.url].created
            logger.info(f"Updating existing provider {data.url}")
        else:
            created = modified
            logger.info(f"Registering new provider {data.url}")
        provider = kodo.datatypes.Provider(
            url=data.url,
            organization=data.organization,
            feed=data.feed,
            created=created,
            modified=modified,
            nodes=data.nodes)
        state.providers[provider.url] = provider
        state.registers[provider.url] = modified
        kodo.worker.loader.Loader.save_to_cache(state)
        logger.info(
            f"Connected {data.url} with {helper.stat(data.nodes)}; "
            f"feed={data.feed} to organization {data.organization}")
        default = kodo.service.controller.default_response(state)
        # Notify peers if feed is enabled
        if state.feed:
            # peers update
            feed = Connect(
                **default.model_dump(), nodes=data.nodes)
            for peer in state.providers.values():
                if peer.feed:
                    if peer.url != data.url:
                        logger.debug(
                            f"broadcast {peer.url}/connect "
                            f"with {helper.stat(data.nodes)}")
                        kodo.service.signal.emit(
                            request.app, "update", peer.url, state, feed)
        # Return node list if feed is enabled
        if data.feed:
            nodes = kodo.service.controller.build_registry(state)
            logger.debug(f"return to {data.url} with {helper.stat(nodes)}")
            return Connect(**default.model_dump(), nodes=nodes)
        return default

    @post("/disconnect",
          summary="Disconnect from Registry",
          description="Disconnects a provider or specific nodes from a registry and updates peers if necessary.",
          tags=["Connections", "Registry", "Node"],
          response_model=kodo.datatypes.DefaultResponse,
          status_code=HTTP_200_OK)
    async def godown(
            self,
            state: State,
            request: Request,
            data: Disconnect) -> dict:
        logger.info(f"/disconnect from {data.provider}")
        if data.provider not in state.providers:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail=f"Provider {data.provider} not found in the registry."
            )
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
                    except Exception as e:
                        logger.error(f"disconnect from {peer.url} failed: {e}")
        return data.model_dump()

    @delete("/connect",
            summary="Disconnect All Active Connections",
            description="Forcefully disconnects the node from all active connections in the Kodosumi registry.",
            tags=["Connections", "Node"],
            status_code=HTTP_204_NO_CONTENT
            )
    async def disconnect(
            self,
            state: State,
            request: Request) -> None:
        # Check if there are active connections
        if not state.connection:
            logger.info("No active connections to disconnect.")
        # Attempt to disconnect from all active connections
        for url in state.connection.keys():
            try:
                resp = httpx.post(
                    f"{url}/disconnect",
                    json=Disconnect(
                        provider=state.url, url=[state.url]).model_dump(),
                    timeout=None)
                logger.debug(
                    f"Disconnect response from {url}: {resp.status_code}")
                # Log failure if the disconnect wasn't successful
                if resp.status_code not in (
                        HTTP_200_OK, HTTP_201_CREATED, HTTP_204_NO_CONTENT):
                    logger.error(
                        f"disconnect from {url} failed: {resp.json()}")
            except Exception as e:
                logger.error(f"disconnect from {url} failed: {e}")

        logger.info("Successfully disconnected from all connections.")

    @post("/reconnect",
          summary="Reconnect to Registry",
          description="Reconnects a node or registry to a registry by emitting a connection signal.",
          tags=["Connections", "Registry", "Node"],
          status_code=HTTP_200_OK)
    async def reconnect(
            self,
            state: State,
            request: Request,
            data: kodo.datatypes.DefaultResponse) -> DefaultResponse:
        kodo.service.signal.emit(request.app, "connect", data.url, state)
        logger.info(f"Successfully reconnected to {data.url}")
        return kodo.service.controller.default_response(state)

    @post("/update",
          summary="Update Node Information",
          description="Updates the node data in a registry and synchronizes the changes with peers if applicable.",
          tags=["Connections", "Registry"],
          response_model=kodo.datatypes.DefaultResponse,
          status_code=HTTP_200_OK)
    async def update(
            self,
            state: State,
            request: Request,
            data: Connect) -> DefaultResponse:
        # inbound data
        if not state.registry:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Current node is not a registry. Cannot process updates."
            )

        modified = helper.now()
        # Check if the provider exists in the registry
        if data.url not in state.providers:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail=f"Provider {data.url} not found in the registry."
            )
        logger.info(f"Update received from {data.url} with {
                    helper.stat(data.nodes)}")
        # Update or insert node data
        for node in data.nodes:
            idx = [i for i, j in enumerate(
                state.providers[data.url].nodes) if j.url == node.url
            ]
            if idx:
                state.providers[data.url].nodes[idx[0]] = node
                logger.debug(f"Updated node [{idx[0]}] at {
                                node.url} from {data.url}")
            else:
                state.providers[data.url].nodes.append(node)
                logger.debug(f"Inserted new node at {
                                node.url} from {data.url}")
        # Save updated state to cache
        kodo.worker.loader.Loader.save_to_cache(state)
        default = kodo.service.controller.default_response(state)
        # Notify peers if feed is enabled
        if state.feed:
            # peers update
            feed = Connect(**default.model_dump(), nodes=data.nodes)
            for peer in state.providers.values():
                if peer.feed and peer.url != data.url:
                    # Backoff for update method
                    backoff = helper.Backoff(sleep=0.5)
                    while True:
                        try:
                            logger.debug(
                                f"Broadcasting /update to {peer.url}")
                            kodo.service.signal.emit(
                                request.app, "update", peer.url, state, feed)
                            break  # Exit loop if successful
                        except Exception as e:
                            logger.error(f"Failed to update {peer.url}: {e}")
                            # Check if max backoff is reached
                            if backoff.sleep >= backoff.max:
                                logger.error(f"Max retries reached for {
                                                peer.url}. Aborting update.")
                                raise HTTPException(
                                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                                    detail=f"Failed to update peer node at {
                                        peer.url} after multiple retries.")
                            else:
                                logger.warning(f"Retrying update to {peer.url} in {
                                                backoff.sleep} seconds...")
                                await backoff.wait()  # Wait with exponential backoff
        logger.info(f"Update process completed for provider {data.url}")
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
            format: Optional[Literal["json", "html"]] = None) -> Union[
                Response, Template]:
        """
        Return all flows from the nodes and providers masquerading the
        sourcing registry. Returns a pandas DataFrame.
        """
        # Validate pagination parameters
        if pp <= 0 or p < 0:
            logger.warning(f"Invalid pagination params: pp={pp}, p={p}")
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Pagination parameters must be positive integers."
            )
        # Build the DataFrame of all flows
        df = build_df(state)
        total = df.shape[0]
        # Apply filtering
        df, query = filter_df(df, q)
        filtered = df.shape[0]
        # Apply sorting
        df, sort_by = sort_df(df, by)
        df.reset_index(drop=True, inplace=True)
        # Apply pagination
        df = df.iloc[p * pp: (p + 1) * pp]
        logger.debug(
            f"return /flows with page {p}/{int(total/pp)} "
            f"and {df.shape[0]}/{total} records")

        # Handle HTML or JSON response
        if (("text/html" in request.headers.get("accept", "")
             and format != "json") | (format == "html")):
            return Template(
                template_name="explore.html", 
                context={
                    "dataframe": df,
                    "result": None,
                    "total": total,
                    "filtered": filtered,
                    "p": p,
                    "pp": pp,
                    "q": q},
                status_code=HTTP_200_OK)
        return Response(content={
            "total": total,
            "filtered": filtered,
            "p": p,
            "pp": pp,
            "items": df.to_dict('records'),
            "by": ", ".join(sort_by),
            "q": query
        })

    async def _ipc(
            self, 
            mode: WorkerMode, 
            state: State, 
            name: str, 
            data=None) -> Template:
        t0 = helper.now()
        if name.startswith("/"):
            url = name
        else:
            url = f"/{name}"
        if url not in state.flows:
            raise NotFoundException(name)
        flow = state.flows[url]
        ipc = FlowAction(flow.entry, state.exec_data)
        ret = ipc.enter(mode, data)
        t = helper.now() - t0
        logger.debug(
            f"{mode} `{flow.name}` ({flow.entry}), "
            f"bootup in {t}: {'OK' if ret.returncode == 0 else 'ERROR'}")
        if ret.returncode == 0:
            if ret.fid:
                t0 = helper.now()
                ipc.run()
                template_file = "launch.html"
                status = HTTP_201_CREATED
                t = helper.now() - t0
                logger.info(
                    f"{mode} `{flow.name}` ({flow.entry}) in {t}: {ret.fid}")
            else:
                status = HTTP_200_OK
                template_file = "enter.html"
        else:   
            status = HTTP_400_BAD_REQUEST
            template_file = "error.html"
        node = NodeInfo(
            url=state.url, organization=state.organization,)
        return Template(
            template_name=template_file, 
            context={
                "result": ret,
                "flow": flow,
                "node": node,
                "url": flow_welcome_url(node.url, flow.url)
            },
            status_code=status)

    @post("/flows/{path:path}")
    async def launch_flow(
            self,
            state: State,
            request: Request,
            path: str) -> Union[Response, Template]:
        if request.headers.get("content-type") == RequestEncodingType.JSON:
            data = await request.json()
        else:
            data = await request.form()
        provided_types: List[str] = [MediaType.JSON, MediaType.HTML]
        preferred_type = request.accept.best_match(
            provided_types, default=MediaType.JSON)
        ret = await self._ipc(MODE.LAUNCH, state, path, data)
        if preferred_type == MediaType.JSON:
            return Response(
                content={
                    "flow": ret.context["flow"].model_dump(),
                    "fid": ret.context["result"].fid,
                    "returncode": ret.context["result"].returncode,
                    "success": bool(ret.context["result"].fid),
                    # "result": ret.context["result"].model_dump(),
                    "node": ret.context["node"].model_dump(),
                    "url": ret.context["url"]
                }, media_type=MediaType.JSON)
        return ret

    @get("/flows/{path:path}")
    async def enter_flow(
            self,
            state: State,
            path: str) -> Template:
        return await self._ipc(MODE.ENTER,state, path, None)

    @get("/counts",
         summary="Retrieve Node and Flow Counts",
         description="Provides total counts of nodes and flows grouped by organization and tags in the Kodosumi registry.",
         tags=["Monitoring", "Statistics"])
    async def counts(
            self,
            state: State,
            request: Request,
            format: Optional[Literal["json", "html"]] = None) -> Response:
        """
        Retrieves counts of nodes and flows grouped by organization and tags.
        Supports optional response format (JSON or HTML).
        """
        # Validate the format parameter
        if format not in (None, "json", "html"):
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="Invalid format. Allowed values are 'json' or 'html'."
            )
        # Build the DataFrame of all flows
        df = build_df(state)
        total_count = df.shape[0]

        # Group by organization and tags for counts
        organization_counts = df.groupby(
            "organization").name.nunique().to_dict()
        tag_counts = df.explode("tags").groupby(
            "tags").name.nunique().to_dict()

        logger.debug(
            f"Retrieved counts: {total_count} total, "
            f"{len(organization_counts)} organizations, {
                len(tag_counts)} tags."
        )
        # Prepare the response content
        content = {
            "total": total_count,
            "organization": organization_counts,
            "tags": tag_counts
        }
        return Response(content=content)
