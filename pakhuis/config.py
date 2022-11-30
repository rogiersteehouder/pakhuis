"""Configuration
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-11-20"
__version__ = "1.0"

from pathlib import Path
from typing import Optional, Any, Dict

import pydantic
from loguru import logger

# tomllib in python >= 3.11
try:
    import tomllib
except ImportError:
    import tomli as tomllib


class AppConfig(pydantic.BaseModel):
    """Application config"""
    logdir: Optional[Path] = None


class ServerConfig(pydantic.BaseModel):
    """Server config"""
    host: str = "localhost"
    port: int = 8001
    ssl_key: Optional[Path] = None
    ssl_cert: Optional[Path] = None


class DatabaseConfig(pydantic.BaseModel):
    """Database config"""
    path: Path = Path("pakhuis.db")


class Config(pydantic.BaseModel):
    """Main config"""
    def __init__(self, **kwargs):
        if "logger" in kwargs and isinstance(kwargs["logger"], logger.__class__):
            self.Config._logger = kwargs.pop("logger").bind(logtype="pakhuis.config")
        else:
            self.Config._logger = logger.bind(logtype="pakhuis.config")
        super().__init__(**kwargs)

    @classmethod
    def parse_file(cls, cfg_file: Path) -> Dict[str, Any]:
        """Load config from toml file."""
        #self.Config._logger.info("Loading config from {}", cfg_file)
        #return tomllib.loads(cfg_file.read_text())
        return cls.parse_obj(tomllib.loads(cfg_file.read_text()))

    app: AppConfig = pydantic.Field(default_factory=AppConfig)
    server: ServerConfig = pydantic.Field(default_factory=ServerConfig)
    auth: Optional[dict] = None
    database: DatabaseConfig = pydantic.Field(default_factory=DatabaseConfig)
