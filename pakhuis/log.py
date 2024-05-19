"""Logging with loguru."""

import sys
import logging
from enum import StrEnum
from pathlib import Path

from loguru import logger


class InterceptHandler(logging.Handler):
    """Redirect everything to loguru."""

    def emit(self, record):
        """Emit message."""
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


# log levels as an enum for use with typer
LogLevels = StrEnum("LogLevels", list(logger._core.levels.keys()))

# log format strings
LOGFMT_CONSOLE = (
    "<light-black>{time:YYYY-MM-DD HH:mm:ss}</light-black>"
    " | <level>{level: <8}</level>"
    " | {extra[logtype]: <12}"
    " | {message}"
)
LOGFMT_CONSOLE_DEBUG = (
    "<light-black>{time:YYYY-MM-DD HH:mm:ss}</light-black>"
    " | <level>{level: <8}</level>"
    " | {extra[logtype]: <12}"
    " | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>"
    " - {message}"
)
LOGFMT_FILE = (
    "{time:YYYY-MM-DD HH:mm:ss}"
    " | {level: <8}"
    " | {extra[logtype]: <12}"
    " | {message}"
)
LOGFMT_FILE_DEBUG = (
    "{time:YYYY-MM-DD HH:mm:ss}"
    " | {level: <8}"
    " | {extra[logtype]: <12}"
    " | {name}:{function}:{line}"
    " - {message}"
)


def init_logger(loglevel: LogLevels, log_dir: Path | None = None, rotation: str = "00:00", retention: int = 5) -> bool:
    """Initialize the logger."""
    # install handler in stdlib logging to redirect to loguru (see loguru docs)
    logging.basicConfig(handlers=[InterceptHandler()], level=0)
    # and redirect uvicorn logging to the default logger
    logging.getLogger("uvicorn").handlers = []
    logging.getLogger("uvicorn.access").handlers = []
    logging.getLogger("uvicorn.access").propagate = True

    # debug flag changes behavior at level DEBUG or TRACE
    debug = logger.level(loglevel.name).no <= logger.level("DEBUG").no

    # remove default handlers and add logtype for identifiable subloggers
    # create a sublogger with: logger.bind(logtype="...")
    logger.configure(handlers=[], extra={"logtype": "main"})

    if debug or log_dir is None:
        # console log
        logger.add(
            sys.stderr,
            format=LOGFMT_CONSOLE_DEBUG if debug else LOGFMT_CONSOLE,
            level=loglevel.name,
        )

    if log_dir is not None:
        # file log
        logger.add(
            log_dir / (__package__ + "-{time:YYYY-MM-DD}.log"),
            format=LOGFMT_FILE_DEBUG if debug else LOGFMT_FILE,
            level=loglevel.name,
            enqueue=True,
            encoding="utf-8",
            rotation=rotation,
            retention=retention,
        )

        # rotation and retention is only checked in long-running processes
        # when they reach rotation time.
        # so we do it at startup in case they missed something.
        logs = list(log_dir.glob(f"{__package__}-*.log"))
        if len(logs) > retention:
            for log in sorted(logs, key=lambda p: (-p.stat().st_mtime, p))[retention:]:
                log.unlink(missing_ok=True)

    # debug flag may be useful in the application
    return debug
