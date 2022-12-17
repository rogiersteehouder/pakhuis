"""Poorthuis: a Starlette authentication backend for http basic authentication.
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-12-12"
__version__ = "0.1"

from typing import Callable

from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware

from .auth import BasicAuthBackend, on_auth_error, crypt_context


def middleware(accounts: Callable[[str], str]) -> Middleware:
    return Middleware(
        AuthenticationMiddleware,
        backend=BasicAuthBackend(accounts),
        on_error=on_auth_error,
    )
