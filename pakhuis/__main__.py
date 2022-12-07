#!/usr/bin/env python3
# encoding: UTF-8

"""Pakhuis

A json document storage service with limited search capability.
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-11-28"
__version__ = "1.1"

import sys
import logging
from pathlib import Path

import click
import uvicorn
from loguru import logger

from . import make_app, Config


class InterceptHandler(logging.Handler):
    """Redirect everything to loguru"""

    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).bind(
            logtype=record.name
        ).log(level, record.getMessage())


@click.command()
@click.option(
    "--loglevel",
    type=click.Choice(logger._core.levels.keys(), case_sensitive=False),
    default="success",
    help="""Log level""",
)
@click.option(
    "-c",
    "--cfg",
    "cfg_file",
    type=click.Path(exists=True, path_type=Path),
    default="config.toml",
    help="""Configuration file""",
)
def main(loglevel: str, cfg_file: Path):
    # logging
    logger.configure(handlers=[], extra={"logtype": "main"})
    loglevel = logger.level(loglevel.upper())
    debug = loglevel.no <= logger.level("DEBUG").no

    # install handler in stdlib logging to redirect to loguru (see loguru docs)
    logging.basicConfig(handlers=[InterceptHandler()], level=0)
    # and redirect uvicorn logging to the default logger
    logging.getLogger("uvicorn").handlers = []
    logging.getLogger("uvicorn.access").handlers = []
    logging.getLogger("uvicorn.access").propagate = True

    # console log - only log to console while debugging
    if debug:
        logfmt = "<light-black>{time:YYYY-MM-DD HH:mm:ss}</light-black> | <level>{level: <8}</level> | {extra[logtype]: <12} | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - {message}"
        logger.add(sys.stderr, format=logfmt, level=loglevel.name)

    with logger.catch(onerror=lambda _: sys.exit(1)):

        # config file
        cfg = Config.parse_file(cfg_file)

        # file log
        if cfg.app.logdir:
            if debug:
                logfmt = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[logtype]: <12} | {name}:{function}:{line} - {message}"
            else:
                logfmt = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {extra[logtype]: <12} | {message}"
            logger.add(
                cfg.app.logdir / (__package__ + "-{time:YYYY-MM-DD}.log"),
                format=logfmt,
                level=loglevel.name,
                enqueue=True,
                encoding="utf-8",
                rotation="00:00",
                retention=5,
            )

        # webservice
        app = make_app(cfg, debug=debug)

        protocol = (
            "https"
            if (cfg.server.ssl_key is not None and cfg.server.ssl_cert is not None)
            else "http"
        )
        logger.success(
            "Serving on {}://{}:{}", protocol, cfg.server.host, cfg.server.port
        )

        uvicorn.run(
            app,
            host=cfg.server.host,
            port=cfg.server.port,
            log_config=dict(version=1, disable_existing_loggers=False),
            log_level="debug",  # log everything, then let loguru handle the filtering
            ssl_keyfile=cfg.server.ssl_key,
            ssl_certfile=cfg.server.ssl_cert,
        )

        logger.success("Complete")


if __name__ == "__main__":
    main()
