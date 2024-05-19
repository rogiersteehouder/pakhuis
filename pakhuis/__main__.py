"""Pakhuis.

A json document storage service with limited search capability.
"""

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Optional

import typer
import uvicorn

from . import __author__, __date__, __version__, make_app
from .log import logger, LogLevels, init_logger
from .tomlconfig import ConfigReader

from icecream import ic


#####
# Config
#####

@dataclass
class PakhuisConfig:
    """Pakhuis config."""

    database: Path = Path("pakhuis.db")


@dataclass
class ServerConfig:
    """Server config."""

    host: str = "localhost"
    port: int = 80
    ssl_key: Path | None = None
    ssl_cert: Path | None = None


@dataclass
class MainConfig:
    """Main program config."""

    pakhuis: PakhuisConfig = field(default_factory=PakhuisConfig)
    server: ServerConfig = field(default_factory=ServerConfig)



#####
# Main application
#####


def main(
    *,
    cfg: Annotated[Path, typer.Option(help="Configuration file.")] = Path(
        "config.toml"
    ),
    log_dir: Annotated[Optional[Path], typer.Option(help="Log directory.")] = None,
    loglevel: Annotated[
        LogLevels, typer.Option(case_sensitive=False, help="Log level.")
    ] = "info",
    version: Annotated[bool, typer.Option(help="Print version and exit.")] = False,
):
    """Pakhuis.

    A json document storage service with limited search capability.
    """
    if version:
        print(__version__)
        return

    instance = cfg.resolve().parent
    if log_dir is None:
        log_dir = instance / "log"

    debug = init_logger(loglevel, log_dir)

    with logger.catch(onerror=lambda _: sys.exit(1)):
        logger.info("Start server")

        config = ConfigReader.from_file(MainConfig, cfg)

        app = make_app(instance / config.pakhuis.database, debug=debug)

        if config.server.ssl_key and config.server.ssl_cert:
            ssl_key = instance / config.server.ssl_key
            ssl_cert = instance / config.server.ssl_cert
            ssl = ssl_key.exists() and ssl_cert.exists()
        else:
            ssl_key = None
            ssl_cert = None
            ssl = False
        logger.success(
            "Serving on {}://{}:{}",
            "https" if ssl else "http",
            config.server.host,
            config.server.port,
        )

        try:
            uvicorn.run(
                app,
                host=config.server.host,
                port=config.server.port,
                log_config={"version": 1, "disable_existing_loggers": False},
                log_level="debug",  # log everything, let loguru handle the filtering
                ssl_keyfile=ssl_key,
                ssl_certfile=ssl_cert,
            )
        except KeyboardInterrupt:
            pass

        logger.success("Complete")


if __name__ == "__main__":
    typer.run(main)
