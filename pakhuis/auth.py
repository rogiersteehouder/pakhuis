"""Basic authentication with password only
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-11-20"
__version__ = "2.1"

import base64
import binascii

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

from . import config


def on_auth_error(request: Request, exc: Exception):
    """Authentication error"""
    return JSONResponse(
        {"code": 401, "detail": str(exc)},
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="Pakhuis json storage"'},
    )


# See: https://www.starlette.io/authentication/
class BasicAuthBackend(AuthenticationBackend):
    """Basic single user authentication for Starlette"""

    context = CryptContext(["pbkdf2_sha256"])

    def __init__(self, cfg: config.Config):
        self.logger = logger.bind(logtype="pakhuis.auth")
        self.hashes = cfg.auth

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
            raise AuthenticationError("Invalid Authorization") from exc

        username, _, password = decoded.partition(":")

        if username not in self.hashes:
            self.logger.warning("Invalid password")
            raise AuthenticationError("Invalid Authorization")
        hash = self.hashes[username]
        ok = self.context.verify(password, hash)
        if not ok:
            self.logger.warning("Invalid password")
            raise AuthenticationError("Invalid Authorization")
        if self.context.needs_update(hash):
            self.logger.warning("Hash needs update")

        return AuthCredentials(["authenticated"]), SimpleUser(username)


if __name__ == "__main__":
    # Run as script to generate password hash for your config file
    import getpass

    user = input("Username: ")
    pwd = getpass.getpass("Password: ")
    context = CryptContext(["pbkdf2_sha256"])
    print("Add this to your config:")
    print('{} = "{}"'.format(user.lower(), context.hash(pwd)))
