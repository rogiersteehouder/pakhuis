"""Pakhuis

A json document storage service with limited search capability.
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-11-28"
__version__ = "1.1"

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse

from .config import Config
from . import webservice, auth


async def json_error(request: Request, exc: HTTPException):
    """Error handler to show json instead of text"""
    detail = getattr(exc, "detail", None)
    if detail is None:
        detail = str(exc)
    return JSONResponse(
        {"code": getattr(exc, "status_code", 500), "detail": detail},
        status_code=getattr(exc, "status_code", 500),
        headers=getattr(exc, "headers", {}),
    )


def make_app(cfg: Config, debug: bool = False) -> Starlette:
    # webservice
    ws = webservice.Webservice(cfg)

    routes = [
        Route("/", ws.root, methods=["GET"]),
        Route("/_ping", ws.ping, methods=["GET", "HEAD"]),
        Route("/_cleanup", ws.cleanup, methods=["GET"]),
        Route("/_sync", ws.sync_list, methods=["GET"]),
        Route("/{_bin}", ws.bin, methods=["GET"]),
        Route("/{_bin}", ws.delete_bin, methods=["DELETE"]),
        Route("/{_bin}", ws.doc, methods=["POST"]),
        Route("/{_bin}/_cleanup", ws.cleanup, methods=["GET"]),
        Route("/{_bin}/_config", ws.bin_config, methods=["GET", "PUT"]),
        Route("/{_bin}/_index", ws.bin_index, methods=["GET", "PUT"]),
        Route("/{_bin}/_index/values", ws.bin_index_values, methods=["GET"]),
        Route("/{_bin}/_search", ws.bin_search, methods=["GET", "POST"]),
        Route("/{_bin}/_sync", ws.sync_list, methods=["GET"]),
        Route("/{_bin}/{_id}", ws.doc, methods=["GET", "PUT", "PATCH"]),
        Route("/{_bin}/{_id}", ws.delete_doc, methods=["DELETE"]),
        Route("/{_bin}/{_id}/_meta", ws.doc_meta, methods=["GET"]),
        Route("/{_bin}/{_id}/_history", ws.doc_history, methods=["GET"]),
    ]
    middleware = []
    if cfg.auth:
        middleware.append(
            Middleware(
                AuthenticationMiddleware,
                backend=auth.BasicAuthBackend(cfg),
                on_error=auth.on_auth_error,
            )
        )
    return Starlette(
        debug=debug,
        routes=routes,
        on_startup=[],
        middleware=middleware,
        exception_handlers={404: json_error, 500: json_error},
    )
