import json
import os
import urllib
from typing import Optional, List

import uvicorn
from litestar import Litestar
from litestar.datastructures import State

import kodo.helper
import kodo.types
from kodo.config import logger, setting
from kodo.service.routes import NodeConnector
from kodo.service.routes import connect_registry, connect_node
from kodo.service.routes import update_registry, update_node


# providers, nodes and flows are empty
DEFAULT_LOADER = "kodo.service.node:_default_loader"


def _default_loader():
    return {
        "flows": None,
        "nodes": None,
        "providers": None
    }


def _load_flows(flows: List[dict] | None) -> dict[str, kodo.types.Flow]:
    # instantiate flow objects
    if flows:
        return {f["name"]: kodo.types.Flow(**f) for f in flows}
    return {}


def _load_nodes_from_cache(
        nodes: List[dict] | None) -> dict[str, kodo.types.Node]:
    # todo: instantiate node objects from .cache
    return {}


def _load_providers_from_cache(
        providers: List[dict] | None) -> dict[str, kodo.types.ProviderOffer]:
    # todo: instantiate provider objects from .cache
    return {}


def create_app(
    url: str | None = None,
    connect: Optional[List[str]] = None,
    node: bool = True,
    registry: bool = True,
    provider: bool = False,
    explorer: bool = False,
    loader: str | None = DEFAULT_LOADER,
        debug: bool = True) -> Litestar:
    """
    Factory method to instantiate the litestar application object and to load
    the environment, nodes, providers.

    variations:

    | type          | node       | registry | provider | explorer |
    | ------------- | ---------- | -------- | -------- | -------- |
    | dev node      | yes        | yes      | no       | yes      |
    | agentic node  | yes        | no       | no       | no       |
    | registry node | (optional) | yes      | yes      | yes      |
    | masumi node   | no         | no       | yes      | no       |
    | sokosumi node | no         | no       | no       | yes      |
    """
    # Load settings from environment variables
    # assumption: all iKODO_* environment variables are used internally by
    #             kodosumi and must not be set by the user directly!
    data = {

        # node url including port
        "URL": os.environ.get("iKODO_URL", url),
        "DEBUG": os.environ.get("iKODO_DEBUG", debug),
        # the list of target registriy URLs for this node, registry or provider
        "CONNECT": json.loads(
            os.environ.get("iKODO_CONNECT", '{}'))
                or (connect if connect else []
        ),

        # indicates if this node offers flows on request /flows and /flow
        "NODE": os.environ.get("iKODO_NODE", node),

        # indicates if this node is a registry for nodes, other registries and
        # registry providers to /register (nodes) and /connect (registries and
        # providers) and to /update/registry and /update/node on changes
        "REGISTRY": os.environ.get("iKODO_REGISTRY", registry),

        # indicates if this node is a provider for nodes to /register, but no
        # /connect
        "PROVIDER": os.environ.get("iKODO_PROVIDER", provider),

        # indicates if this node responds to /flows
        "EXPLORER": os.environ.get("iKODO_EXPLORER", explorer),

        # the loader callback to load the initial .nodes and .providers
        "LOADER": os.environ.get("iKODO_LOADER", loader)
    }

    env = kodo.types.InternalEnviron(**data)  # type: ignore
    state = State({
        "url": env.URL,
        "node": env.NODE,
        "registry": env.REGISTRY,
        "provider": env.PROVIDER,
        "explorer": env.EXPLORER,
        # the other registries which reponded to /connect from this node
        # (with None or timestamp)
        "connection": {str(host): None for host in env.CONNECT or []},
        # this registry connected to other registries
        "connected": {},
        # flows records (nodes only)
        "flows": {},
        # registry records
        "nodes": {},
        # provider registry records
        "providers": {},
        # semaphore for pending event processings
        "event": 0
    })
    # validate loader callback
    callback = kodo.helper.parse_factory(env.LOADER or DEFAULT_LOADER)
    load = callback()
    if not state.node and load["flows"]:
        raise ValueError("flows allowed for nodes only")
    if not state.registry and (load["nodes"] or load["providers"]):
        raise ValueError(
            "nodes/providers allowed for registry/provider only")
    if not state.registry and load["nodes"]:
        raise ValueError("nodes allowed for registry/provider only")
    # load initial data from .cache
    if state.node:
        state.flows = _load_flows(load["flows"])
    if state.registry or state.provider:
        state.nodes = _load_nodes_from_cache(load["nodes"])
        state.providers = _load_providers_from_cache(load["providers"])

    app = Litestar(
        route_handlers=[NodeConnector],
        on_startup=[NodeConnector.startup],
        on_shutdown=[NodeConnector.shutdown],
        listeners=[
            connect_node,
            connect_registry,
            update_registry,
            update_node
        ],
        state=state,
        debug=debug
    )
    logger.info(f"{state.url} successfully started with mode "
                + "+".join([kind for kind in (
                    "node", "registry", "provider", "explorer")
                    if state[kind]]))
    return app


def run_service(
        url: str = setting.SERVER,
        connect: Optional[List[str]] = setting.REGISTRY,
        node: bool = True,
        registry: bool = True,
        provider: bool = False,
        explorer: bool = False,
        loader: Optional[str] = DEFAULT_LOADER,
        reload: bool = setting.RELOAD,
        debug: bool = True) -> None:
    """
    Main kodosumi service method parsing and initialising the environment to
    create the litestar application object, initialise the environment and
    launch uvicorn application server.
    """
    server = urllib.parse.urlparse(url)
    if server.hostname is None:
        raise ValueError("Invalid server URL, missing hostname")
    if server.port is None:
        raise ValueError("Invalid server URL, missing port")
    if not (node or registry or provider or explorer):
        raise ValueError("Missing mode and node purpose")
    if connect is None:
        connect = []
    env_settings = kodo.types.InternalEnviron(
        URL=url,
        CONNECT=connect,
        NODE=node,
        REGISTRY=registry,
        PROVIDER=provider,
        EXPLORER=explorer,
        LOADER=loader,
        DEBUG=debug
    )
    # forward to create_app with iKODO_* environment variables
    for k, v in env_settings.model_dump().items():
        if v is not None:
            os.environ[f"iKODO_{k}"] = json.dumps(
                v) if k == "CONNECT" else str(v)
    # final validation before we fork threads
    # note: it get hards to stop a thread
    callback = kodo.helper.parse_factory(env_settings.LOADER or DEFAULT_LOADER)
    uvicorn.run(
        "kodo.service.node:create_app",
        host=server.hostname,
        port=server.port,
        reload=reload,
        factory=True,
        log_config=None,
        log_level=None
    )


if __name__ == "__main__":
    run_service()
