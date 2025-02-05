import urllib
from pathlib import Path
import traceback

import uvicorn
from litestar import Litestar, Response, Request
from litestar.config.cors import CORSConfig
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.openapi.config import OpenAPIConfig
from litestar.openapi.plugins import SwaggerRenderPlugin
from litestar.static_files import create_static_files_router
from litestar.template.config import TemplateConfig
from litestar.exceptions import HTTPException
from litestar.middleware.base import DefineMiddleware

import kodo.log
import kodo.service.signal
import kodo.service.security
import kodo.worker.loader
from kodo.log import logger
from kodo.service.route.main import NodeControl
from kodo.service.route.flow import FlowControl
from kodo.service.route.execute import ExecutionControl


DEFAULT_LOADER = "kodo.worker.loader:default_loader"


def app_exception_handler(request: Request, exc: Exception) -> Response:
    if isinstance(exc, HTTPException):
        status_code = exc.status_code
        detail = exc.detail
    else:
        status_code = 500
        detail = repr(exc)
    logger.error(f"server error: {status_code} {detail}")
    return Response(
        content={
            "error": "server error",
            "path": request.url.path,
            "detail": detail,
            "status_code": status_code,
            "stacktrace": traceback.format_exc(),
        },
        status_code=500,
    )


def create_app(**kwargs) -> Litestar:
    loader = kodo.worker.loader.Loader()
    state = loader.load()
    amw = DefineMiddleware(kodo.service.security.JWTAuthMiddleware, exclude=["/docs"])
    app = Litestar(
        cors_config=CORSConfig(allow_origins=state.cors_origins),
        route_handlers=[
            NodeControl,
            FlowControl,
            ExecutionControl,
            create_static_files_router(
                path="/static", directories=[Path(__file__).parent / "static"]
            ),
        ],
        on_startup=[NodeControl.startup],
        on_shutdown=[NodeControl.shutdown],
        middleware=[amw],
        listeners=[
            kodo.service.signal.connect,
            kodo.service.signal.update,
            kodo.service.signal.reconnect,
        ],
        state=state,
        template_config=TemplateConfig(
            directory=Path(__file__).parent / "templates", engine=JinjaTemplateEngine
        ),
        openapi_config=OpenAPIConfig(
            title="Kodosumi API",
            description="API documentation for the Kodosumi mesh.",
            version=kodo.__version__,
            render_plugins=[SwaggerRenderPlugin()],
            path="/docs",
        ),
        exception_handlers={Exception: app_exception_handler},
        debug=False,
    )

    kodo.log.identifier = state.url
    kodo.log.setup_logger(
        log_file=state.log_file,
        log_file_level=state.log_file_level,
        screen_level=state.screen_level,
    )
    while state.log_queue:
        level, message = state.log_queue.pop(0)
        logger.log(level, message)
    logger.info(
        f"startup with providers: {len(state.providers)}, "
        f"connection: {len(state.connection)}, "
        f"log level: {state.screen_level}, "
        f"executor: {'ray' if state.ray else 'thread'}"
    )
    if state.cache_reset:
        if Path(state.cache_data).exists():
            logger.warning(f"reset cache {state.cache_data}")
            Path(state.cache_data).unlink()
    return app


def run_service(**kwargs) -> None:
    """
    Main kodosumi service method to create the litestar application object,
    initialise the environment and launch uvicorn application server.

    Parameters:
        url: str
        organization: str
        connect: List[str]
        registry: bool
        feed: bool
        loader: Union[str, Callable]
        cache_data: str
        cache_reset: bool
        screen_level: str
        log_file: str
        log_file_level: str
        reload: bool
    """

    loader = kodo.worker.loader.Loader()
    # the user passes a loader: callable or str (factory/file or directory)
    # the loader passes kwargs to iKODO_* environment variables
    loader.setup(**kwargs)
    server = urllib.parse.urlparse(loader.option.URL)
    if server.hostname is None:
        raise ValueError("Invalid server URL, missing hostname")
    if server.port is None:
        raise ValueError("Invalid server URL, missing port")
    if loader.option.FEED and not loader.option.REGISTRY:
        raise ValueError("Cannot feed (True) as a node")
    if loader.option.CONNECT is None:
        loader.option.CONNECT = []
    uvicorn.run(
        "kodo.service.node:create_app",
        host="0.0.0.0",
        port=int(server.port),
        reload=bool(loader.option.RELOAD),
        factory=True,
        log_config={"version": 1, "loggers": {}},
    )


if __name__ == "__main__":
    run_service(reload=True)
