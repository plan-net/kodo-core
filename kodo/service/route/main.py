import os
import signal
from typing import Union, Optional, Literal

import httpx
import ray
from litestar import Litestar, Request, delete, get, post, MediaType
from litestar.response import Template, Response
from litestar.datastructures import State
from litestar.exceptions import HTTPException
from litestar.status_codes import (HTTP_200_OK, HTTP_201_CREATED,
                                   HTTP_204_NO_CONTENT, HTTP_400_BAD_REQUEST,
                                   HTTP_404_NOT_FOUND,
                                   HTTP_500_INTERNAL_SERVER_ERROR)

import kodo.helper as helper
from kodo.helper import wants_html
import kodo.service.controller
import kodo.service.signal
import kodo.worker.loader
from kodo.datatypes import (Connect, DefaultResponse, Disconnect, Provider,
                            ProviderMap)
from kodo.log import logger


class NodeControl(kodo.service.controller.Controller):

    @staticmethod
    async def startup(app: Litestar) -> None:
        app.state.started_at = helper.now()
        for url in app.state.connection:
            kodo.service.signal.emit(app, "connect", url, app.state)
        message: str
        if app.state.registry:
            for provider in app.state.providers.values():
                kodo.service.signal.emit(
                    app, "reconnect", provider.url, app.state)
            message = f"registry (feed is {app.state.feed})"
        else:
            message = f"node"
        logger.info(
            f"{message} startup complete with {len(app.state.flows)} flows "
            f"(pid {os.getpid()}, ppid {os.getppid()})")

        original_handler = {
            signal.SIGINT: signal.getsignal(signal.SIGINT),
            signal.SIGTERM: signal.getsignal(signal.SIGTERM)}
        
        def signal_handler(signal, frame):
            app.state.exit = True
            if original_handler.get(signal):
                original_handler[signal](signal, frame)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    @staticmethod
    def shutdown(app: Litestar) -> None:
        ray.shutdown()
        logger.info(f"shutdown now")

    @delete("/kill")
    async def kill(self) -> None:
        logger.debug(f"received SIGHUP, killing me")
        os.kill(os.getpid(), signal.SIGTERM)

    @get("/home",
         summary="Node Status Overview",
         description=("Returns general state information about the Kodosumi "
                      "registry or node, including startup time and status."),
         tags=["Status", "Registry", "Node"],
         response_model=DefaultResponse)
    async def home(
            self,
            request: Request,
            state: State) -> Union[Template, Response, DefaultResponse]:
        if wants_html(request):
            return Template(
                template_name="home.html",
                context={"organization": state.organization,
                         "version": kodo.__version__},
                status_code=HTTP_200_OK,
                media_type=MediaType.HTML)
        return kodo.service.controller.default_response(state)

    @get("/map",
         summary="Provider and Connection Map",
         description=("Provides detailed runtime data on registered providers, active "
                      "connections, and node registers within the Kodosumi system."),
         tags=["Monitoring", "Registry", "Node"],
         response_model=ProviderMap,
         status_code=HTTP_200_OK)
    async def get_map(self, state: State) -> ProviderMap:
        default = kodo.service.controller.default_response(state).model_dump()
        default["providers"] = state.providers.values()
        default["connection"] = state.connection
        default["registers"] = state.registers
        active_registers = sum([1 for r in state.registers.values() if r])
        active_connection = sum([1 for c in state.connection.values() if c])
        logger.debug(
            f"return /map providers: {len(state.providers.values())}, "
            f"registers: {active_registers}/{len(state.registers)}, "
            f"connection: {active_connection}/{len(state.connection)}")
        return ProviderMap(**default)

    @get("/connect",
         summary="Registry Node Connections (Preview)",
         description=("Provides a preview of connected registry nodes "
                      "and their current data within the Kodosumi system."),
         tags=["Connections", "Registry", "Node"],
         response_model=Connect,
         status_code=HTTP_200_OK)
    async def get_connect(self, state: State) -> Connect:
        default = kodo.service.controller.default_response(state)

        nodes = kodo.service.controller.build_registry(state)
        logger.debug(f"Returning /connect with {helper.stat(nodes)}")

        return Connect(**default.model_dump(), nodes=nodes)

    @post("/connect",
          summary="Establish Connection to Registry",
          description=("Connect a node or registry to a Kodosumi registry. "
                       "Updates the registry state and synchronizes with peers if applicable."),
          tags=["Connections", "Registry", "Node"],
          response_model=Union[Connect, DefaultResponse],
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
        provider = Provider(
            url=data.url,
            organization=data.organization,
            feed=data.feed,
            created=created,
            modified=modified,
            nodes=data.nodes)
        state.providers[provider.url] = provider
        state.registers[provider.url] = modified
        await kodo.worker.loader.Loader.save_to_cache(state)
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
          response_model=DefaultResponse,
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
        await kodo.worker.loader.Loader.save_to_cache(state)
        if state.feed:
            for peer in state.providers.values():
                if peer.feed and peer.url != data.provider:
                    logger.debug(
                        f"broadcast {peer.url}/disconnect of {data.url}")
                    try:
                        resp = httpx.post(
                            f"{peer.url}/disconnect",
                            json=Disconnect(
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
            status_code=HTTP_204_NO_CONTENT)
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
            data: DefaultResponse) -> DefaultResponse:
        kodo.service.signal.emit(request.app, "connect", data.url, state)
        logger.info(f"Successfully reconnected to {data.url}")
        return kodo.service.controller.default_response(state)

    @post("/update",
          summary="Update Node Information",
          description="Updates the node data in a registry and synchronizes the changes with peers if applicable.",
          tags=["Connections", "Registry"],
          response_model=DefaultResponse,
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
        await kodo.worker.loader.Loader.save_to_cache(state)
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
                                logger.warning(
                                    f"Retrying update to {peer.url} "
                                    f"in {backoff.sleep} seconds...")
                                await backoff.wait()  # Wait with exponential backoff
        logger.info(f"Update process completed for provider {data.url}")
        return default
