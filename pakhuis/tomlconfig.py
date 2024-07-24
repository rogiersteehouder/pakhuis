"""Read configuration from toml file(s)."""

# ruff: noqa: TRY003

__author__ = "Rogier Steehouder <rogiersteehouder@gmail.com>"
__date__ = "2024-05-26"
__version__ = "1.2"

import tomllib
from collections.abc import Callable
from dataclasses import is_dataclass, fields
from types import GenericAlias, UnionType
from typing import Any, ClassVar
from pathlib import Path


class ConfigError(Exception):
    """Configuration Error."""


class ConfigReader:
    """Read configuration from toml file(s)."""

    translators: ClassVar[dict[str, Callable[[Any], Any]]] = {
        "Path": Path,
    }

    def __init__(self, cfg_file: Path) -> None:
        """Read configuration from toml file(s)."""
        self.cfg_file = cfg_file

    @staticmethod
    def read_data_from_file(cfg_file: Path) -> dict:
        """Read data from toml file."""
        return tomllib.loads(cfg_file.read_text(encoding="utf-8"))

    @classmethod
    def from_file[
        Dataclass_or_dict
    ](
        cls,
        target: type[Dataclass_or_dict],
        cfg_file: Path | None = None,
        name: str | None = None,
    ) -> Dataclass_or_dict:
        """Read configuration from toml file(s)."""
        if cfg_file is None:
            # no config file, assume same name as script with suffix .toml
            import __main__

            cfg_file = Path(__main__.__file__).with_suffix(".toml")

        data = cls.read_data_from_file(cfg_file)
        return cls(cfg_file).get_value(data, target, name)

    def get_value(  # noqa: PLR0911, PLR0912
        self, val: Any, target: type, name: str | None = None
    ) -> Any:
        """Produce the correct value for a setting."""
        # name for error message
        if name is None:
            name = "configuration"

        # include functionality: { _include = "tomlfile[:section]" } is replaced with
        # the content of the toml file (relative to the config file it is found in)
        # optionally only the subsection of the resulting dict
        if isinstance(val, dict) and len(val) == 1 and "_include" in val:
            fn, _, section = val["_include"].partition(":")
            val = ConfigReader.from_file(target, self.cfg_file.parent / fn, name)
            if section:
                val = val[section]

        # UnionType: assume the first one (unless None)
        # this accepts a type hint of "SomeClass | None" and do the right thing
        if isinstance(target, UnionType):
            if target.__args__[-1] == type(None) and val is None:
                return None
            return self.get_value(val, target.__args__[0], name)

        # GenericAlias: a type hint with subscript, such as "dict[str, Any]"
        if isinstance(target, GenericAlias):
            # container is a dict: use dict conversion
            if target.__origin__ == dict:
                return self.convert_dict(val, target, name)
            # container is a list: convert each item to the correct type
            if target.__origin__ == list:
                return [
                    self.get_value(x, target.__args__[0], f"{name}[{i}]")
                    for i, x in enumerate(val)
                ]
            # other container: use as-is
            if isinstance(val, target.__origin__):
                return val
            # problem
            raise ConfigError(f"Expecting {target} got {type(val)} for {name}")

        # unspecified dict
        if target == dict:
            return self.convert_dict(val, target, name)

        # dataclass
        if is_dataclass(target):
            return self.convert_dataclass(val, target, name)

        # otherwize, use the given value as-is
        if target == Any:
            return val
        if isinstance(val, target):
            return val
        if target.__name__ in self.translators:
            return self.translators[target.__name__](val)
        # problem
        raise ConfigError(f"Expecting {target} got {type(val)} for {name}")

    def convert_dict(
        self, data: dict[str, Any], target: type[dict], name: str | None = None
    ) -> dict:
        """Process a dictionary target."""
        if not isinstance(data, dict):
            raise ConfigError(f"Expecting {dict} got {type(data)} for {name}")

        # name for error message
        if name is None:
            name = "configuration"

        # if the dict has specific type, like "dict[str, SomeClass]"
        if hasattr(target, "__args__"):
            return {
                k: self.get_value(v, target.__args__[1], f"{name}[{k}]")
                for k, v in data.items()
            }

        # if not, just use the data as-is
        return data

    def convert_dataclass[
        Dataclass
    ](
        self, data: dict[str, Any], target: type[Dataclass], name: str | None = None
    ) -> Dataclass:
        """Convert a dict to a dataclass instance."""
        if not is_dataclass(target):
            raise ConfigError(f"{target} should be a dataclass.")

        if not isinstance(data, dict):
            # if it is not a dict, assume it is the dataclass' first parameter
            data = {fields(target)[0].name: data}

        # name for error message
        if name is None:
            name = target.__name__

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
