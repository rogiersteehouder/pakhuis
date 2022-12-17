from pathlib import Path
from typing import Optional, Any, Dict

import pydantic
from loguru import logger

# tomllib in python >= 3.11
try:
    import tomllib
except ImportError:
    import tomli as tomllib


class ServerConfig(pydantic.BaseModel):
    """Server config"""
    host: str = "localhost"
    port: int = 8001
    ssl_key: Optional[Path] = None
    ssl_cert: Optional[Path] = None


class PoorthuisConfig(pydantic.BaseModel):
    """Poorthuis config"""

    accounts: dict = pydantic.Field(default_factory=dict)


class PakhuisConfig(pydantic.BaseModel):
    """Database config"""

    database: str = "pakhuis.db"


class Config(pydantic.BaseModel):
    """Main config"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def parse_file(cls, cfg_file: Path) -> "Config":
        """Load config from toml file."""
        obj = cls.parse_obj(tomllib.loads(cfg_file.read_text()))
        obj.instance = cfg_file.parent
        return obj

    instance: Path = Path(".")
    server: ServerConfig = pydantic.Field(default_factory=ServerConfig)
    poorthuis: PoorthuisConfig = pydantic.Field(default_factory=PoorthuisConfig)
    pakhuis: PakhuisConfig = pydantic.Field(default_factory=PakhuisConfig)
