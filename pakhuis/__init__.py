"""Pakhuis

A json document storage service with limited search capability.
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-12-15"
__version__ = "1.2"

from loguru import logger
from starlette.applications import Starlette
from starlette import status
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

import poorthuis
from poorthuis.dict import DictAccounts

from .config import Config
from . import webservice


async def json_error(request: Request, exc: HTTPException):
    """Error handler to show json instead of text"""
    detail = getattr(exc, "detail", None)
    if detail is None:
        detail = str(exc)
    return JSONResponse(
        {
            "code": getattr(exc, "status_code", status.HTTP_500_INTERNAL_SERVER_ERROR),
            "detail": detail,
        },
        status_code=getattr(exc, "status_code", status.HTTP_500_INTERNAL_SERVER_ERROR),
        headers=getattr(exc, "headers", {}),
    )


def make_app(cfg: Config, debug: bool = False) -> Starlette:
    # webservice
    ws = webservice.Webservice(cfg.instance / cfg.pakhuis.database)

    routes = ws.routes

    ### Poorthuis
    middleware = []
    if cfg.poorthuis.accounts:
        middleware.append(poorthuis.middleware(DictAccounts(cfg.poorthuis.accounts)))

    ### Server
    app = Starlette(
        debug=debug,
        routes=routes,
        on_startup=[],
        middleware=middleware,
        exception_handlers={404: json_error, 500: json_error},
    )
    return app
