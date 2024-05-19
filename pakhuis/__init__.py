"""Pakhuis.

A json document storage service with limited search capability.
"""

__author__ = "Rogier Steehouder"
__date__ = "2024-05-09"
__version__ = "2.0.1"

from pathlib import Path

from starlette.applications import Starlette
from starlette import status
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from .webservice import PakhuisService


async def json_error(request: Request, exc: HTTPException):
    """Error handler to show json instead of text."""
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


def make_app(
    database_path: Path = Path("pakhuis.db"),
    *,
    debug: bool = False
) -> Starlette:
    """Create the Starlette app."""
    # webservice
    ws = PakhuisService(database_path)

    ### Server
    app = Starlette(
        debug=debug,
        routes=ws.routes,
        exception_handlers={404: json_error, 500: json_error},
    )
    return app
