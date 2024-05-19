"""Read configuration from toml file(s)."""

# ruff: noqa: TRY003

import tomllib
from dataclasses import is_dataclass, fields
from types import GenericAlias, UnionType
from typing import Any
from pathlib import Path


class ConfigError(Exception):
    """Configuration Error."""


class ConfigReader:
    """Read configuration from toml file(s)."""

    def __init__(self, cfg_file: Path):
        """Read configuration from toml file(s)."""
        self.cfg_file = cfg_file

    @staticmethod
    def read_data_from_file(cfg_file: Path) -> dict:
        """Read data from toml file."""
        if cfg_file.exists():
            return tomllib.loads(cfg_file.read_text(encoding="utf-8"))
        return {}

    @classmethod
    def from_file[
        Dataclass_or_dict
    ](
        cls,
        target: type[Dataclass_or_dict] = type[dict],
        cfg_file: Path | None = None,
        name: str | None = None,
    ) -> Dataclass_or_dict:
        """Read configuration from toml file(s)."""
        if cfg_file is None:
            # no config file: assume same name as script with suffix .toml
            import __main__

            cfg_file = Path(__main__.__file__).with_suffix(".toml")

        data = cls.read_data_from_file(cfg_file)
        if is_dataclass(target):
            return cls(cfg_file).convert_dc(data, target, name)
        return cls(cfg_file).convert_dict(data, target, name)

    def get_value(self, val: Any, target: type, name: str) -> Any:
        """Produce the correct value for a setting."""
        # UnionType: assume the first one (unless None)
        # this accepts a type hint of "SomeClass | None" and do the right thing
        if isinstance(target, UnionType):
            if target.__args__[-1] == type(None) and val is None:
                return None
            return self.get_value(val, target.__args__[0], name)
        # this accepts a type hint with subscript, such as "dict[str, Any]"
        if isinstance(target, GenericAlias):
            if target.__origin__ == dict:
                return self.convert_dict(val, target, name)
            if target.__origin__ == list:
                return [
                    self.get_value(x, target.__args__[0], f"{name}[{i}]")
                    for i, x in enumerate(val)
                ]
            return val
        # this accepts a dataclass type hint
        if is_dataclass(target):
            return self.convert_dc(val, target, name)
        # otherwize, use the given value as-is
        return val

    def convert_dict(
        self, data: dict[str, Any], target: type[dict], name: str | None = None
    ) -> dict:
        """Process a dictionary target."""
        if not isinstance(data, dict):
            raise ConfigError(f"Expecting dict got {type(data)} for {name}")

        # name for error message
        if name is None:
            name = "configuration"

        # include functionality: { _include = "toml file" } is replaced with
        # the content of the toml file (relative to the config file it is found in)
        if len(data) == 1 and "_include" in data:
            return ConfigReader.from_file(
                target, self.cfg_file.parent / data["_include"], name
            )

        # if the dict has specific type, like "dict[str, SomeClass]"
        if hasattr(target, "__args__"):
            return {
                k: self.get_value(v, target.__args__[1], f"{name}[{k}]")
                for k, v in data.items()
            }
        # if not, just use the data as-is
        return data

    def convert_dc[
        Dataclass
    ](
        self, data: dict[str, Any], target: type[Dataclass], name: str | None = None
    ) -> Dataclass:
        """Convert a dict to a dataclass instance."""
        if not is_dataclass(target):
            raise ConfigError(f"{target} should be a dataclass.")
        if not isinstance(data, dict):
            raise ConfigError(f"Expecting dict got {type(data)} for {name}")

        # name for error message
        if name is None:
            name = target.__name__

        # include functionality: { _include = "toml file" } is replaced with
        # the content of the toml file (relative to the config file it is found in)
        if len(data) == 1 and "_include" in data:
            return ConfigReader.from_file(
                target, self.cfg_file.parent / data["_include"], name
            )

        # use only the items from the dict that are in the dataclass
        dataclass_data = {}
        for fld in fields(target):
            if fld.name not in data:
                continue
            dataclass_data[fld.name] = self.get_value(
                data[fld.name], fld.type, f"{name}.{fld.name}"
            )
        try:
            return target(**dataclass_data)
        except TypeError as e:
            # if a required field in the dataclass is missing
            raise ConfigError(f"Missing required setting in {name}") from e
        except AssertionError as e:
            # use assert in __post_init__ for field constraints
            raise ConfigError(f"Invalid value in {name}") from e
