import json
import os
import urllib
from pathlib import Path
from typing import Optional, List

import uvicorn
from litestar import Litestar
from litestar.datastructures import State

import kodo.helper
import kodo.types
import kodo.worker.loader
from kodo.config import logger, setting
from kodo.service.routes import NodeConnector, connect_registry, connect_node, update_registry, update_node


# providers, nodes and flows are empty
DEFAULT_LOADER = "kodo.service.node:_default_loader"


def _default_loader(state: State) -> kodo.worker.loader.Loader:
    loader = kodo.worker.loader.Loader()
    loader.reload()
    cache = loader.load_from_cache(state.cache)
    return loader


def create_app(
    url: str | None = None,
    connect: Optional[List[str]] = None,
    organization: Optional[str] = None,
    node: bool = True,
    registry: bool = True,
    provider: bool = False,
    explorer: bool = False,
    loader: str | None = DEFAULT_LOADER,
    cache: str | None = setting.CACHE_DATA,
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
        # node owner
        "ORGANIZATION": os.environ.get("iKODO_ORGANIZATION", organization),
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
        # the loader callback to load the initial .flows, .nodes and .providers
        "LOADER": os.environ.get("iKODO_LOADER", loader),
        # registry cache directory
        "CACHE": os.environ.get("iKODO_CACHE", cache)
    }

    env = kodo.types.InternalEnviron(**data)  # type: ignore
    state = State({
        "url": env.URL,
        "organization": env.ORGANIZATION,
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
        # flow entry points (nodes only)
        "entry_points": {},
        # semaphore for pending event processings
        "event": 0,
        "heartbeat": None,
        "status": "unknown",
        "cache": env.CACHE
    })
    # load flows, nodes, providers
    callback = kodo.helper.parse_factory(env.LOADER or DEFAULT_LOADER)
    oload: kodo.worker.loader.Loader = callback(state)
    if not state.node and oload.flows:
        raise ValueError("flows allowed for nodes only")
    if not state.registry and oload.providers:
        raise ValueError(
            "nodes/providers allowed for registry/provider only")
    # load initial data
    if state.node:
        state.flows = oload.flows_dict()
        state.entry_points = oload.entry_points_dict()
    if state.registry or state.provider:
        state.providers = oload.providers_dict()
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
    if Path(state.cache).exists():
        logger.info(f"{state.url} loading from cache {state.cache}")
    logger.info(f"{state.url} successfully started with mode "
                + "+".join([kind for kind in (
                    "node", "registry", "provider", "explorer")
                    if state[kind]]))
    return app


def run_service(
        url: str = setting.SERVER,
        organization: Optional[str] = None,
        connect: Optional[List[str]] = setting.REGISTRY,
        node: bool = True,
        registry: bool = True,
        provider: bool = False,
        explorer: bool = False,
        loader: Optional[str] = DEFAULT_LOADER,
        cache: Optional[str] = setting.CACHE_DATA,
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
    if cache:
        Path(cache).parent.mkdir(parents=True, exist_ok=True)
    env_settings = kodo.types.InternalEnviron(
        URL=url,
        ORGANIZATION=organization,
        CONNECT=connect,
        NODE=node,
        REGISTRY=registry,
        PROVIDER=provider,
        EXPLORER=explorer,
        LOADER=loader,
        CACHE=cache,
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
