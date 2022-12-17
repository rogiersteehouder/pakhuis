"""Poorthuis: a Starlette authentication backend for http basic authentication.
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-12-12"
__version__ = "0.1"

import base64
import binascii
from typing import Callable

from loguru import logger
from passlib.context import CryptContext
from starlette.authentication import (
    AuthCredentials,
    AuthenticationBackend,
    AuthenticationError,
    SimpleUser,
)
from starlette.requests import Request
from starlette.responses import JSONResponse


# Cryptographic context
# See: https://passlib.readthedocs.io/en/stable/narr/context-tutorial.html
crypt_context = CryptContext(["pbkdf2_sha256"])


def on_auth_error(request: Request, exc: Exception):
    """Authentication error with json response"""
    return JSONResponse(
        {"code": 401, "detail": str(exc)},
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="Poorthuis"'},
    )


# See: https://www.starlette.io/authentication/
class BasicAuthBackend(AuthenticationBackend):
    """Basic authentication for Starlette"""

    context = crypt_context

    def __init__(self, accounts: Callable[[str], str] = None):
        self.logger = logger.bind(logtype="poorthuis")
        self.accounts = accounts

    async def authenticate(self, conn):
        """Password middleware for Starlette"""
        if "Authorization" not in conn.headers:
            raise AuthenticationError("Basic Authorization required")

        auth = conn.headers["Authorization"]
        try:
            scheme, credentials = auth.split()
            if scheme.lower() != "basic":
                raise AuthenticationError("Basic Authorization required")
            decoded = base64.b64decode(credentials).decode("ascii")
        except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
            self.logger.warning(
                "Problem getting Credentials: {}", exc.__class__.__name__
            )
            raise AuthenticationError("Invalid Authorization") from exc

        username, _, password = decoded.partition(":")

        if self.accounts is None:
            self.logger.error("No account lookup defined")
            raise AuthenticationError("Invalid Authorization")

        hash = self.accounts(username)
        ok = self.context.verify(password, hash)
        if not ok:
            self.logger.warning("Invalid Authorization for {}", username)
            raise AuthenticationError("Invalid Authorization")
        if self.context.needs_update(hash):
            self.logger.warning("Hash needs update for username {}", username)

        return AuthCredentials(["authenticated"]), SimpleUser(username)
