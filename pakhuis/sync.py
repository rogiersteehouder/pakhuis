"""Pakhuis sync.

Synchronizes documents on two pakhuis services.
Sync by date/time only, no compare of content.

Note:
This will not update index definitions, since there is no way to determine
which is the latest. It will create an index definition if the whole bin is
copied.
"""

__author__ = "Rogier Steehouder"
__date__ = "2024-05-09"
__version__ = "2.0.1"

import sys
import getpass
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Optional

import httpx
import typer

from .log import logger, LogLevels, init_logger
from .tomlconfig import ConfigReader


def raise_for_status(response: httpx.Response):
    """Error for 4xx and 5xx status codes."""
    if response.status_code == 401:
        logger.error("Authorization failed")
        sys.exit(1)
    response.raise_for_status()


#####
# Config
#####


@dataclass
class ServerConfig:
    """Server configuration."""

    url: str
    name: str = ""
    username: str | None = None
    password: str | None = None

    def __post_init__(self):
        """Post init processing."""
        self.url = self.url.rstrip("/")


@dataclass
class SyncConfig:
    """Sync config."""

    default1: str = ""
    default2: str = ""
    servers: dict[str, ServerConfig] = field(default_factory=dict)


#####
# Sync
#####


class ServerSync:
    """Sync two pakhuis servers."""

    def __init__(self, cli: httpx.Client) -> None:
        """Sync two pakhuis servers."""
        self.cli = cli

    def run(self, first: ServerConfig, second: ServerConfig) -> None:
        """Sync two pakhuis servers."""
        first_content = self.cli.get(
            f"{first.url}/_sync", auth=(first.username, first.password)
        ).json()
        second_content = self.cli.get(
            f"{second['url']}/_sync", auth=(second["username"], second["password"])
        ).json()

        first_bins = set(first_content.keys())
        second_bins = set(second_content.keys())

        # copy missing bins in total
        for _bin in first_bins - second_bins:
            self.copy_bin(_bin, first, second)
        for _bin in second_bins - first_bins:
            self.copy_bin(_bin, second, first)

        # compare bins
        for _bin in first_bins & second_bins:
            first_items = set(first_content[_bin].keys())
            second_items = set(second_content[_bin].keys())

            # copy missing items in total, skipping inactive items
            for _id in first_items - second_items:
                if first_content[_bin][_id]["active"]:
                    self.copy_item(
                        _bin, _id, first_content[_bin][_id]["dttm"], first, second
                    )
            for _id in second_items - first_items:
                if second_content[_bin][_id]["active"]:
                    self.copy_item(
                        _bin, _id, second_content[_bin][_id]["dttm"], second, first
                    )

            # compare items
            for _id in first_items & second_items:
                first_item = first_content[_bin][_id]
                second_item = second_content[_bin][_id]
                # do not sync inactive items
                if not (first_item["active"] or second_item["active"]):
                    continue

                # copy last date/time to the other server
                if first_item["dttm"] > second_item["dttm"]:
                    if first_item["active"]:
                        self.copy_item(_bin, _id, first_item["dttm"], first, second)
                    else:
                        self.del_item(_bin, _id, second)
                elif first_item["dttm"] < second_item["dttm"]:
                    if second_item["active"]:
                        self.copy_item(_bin, _id, second_item["dttm"], second, first)
                    else:
                        self.del_item(_bin, _id, first)

    def copy_bin(self, _bin: str, from_server: ServerConfig, to_server: ServerConfig):
        """Copy a whole bin."""
        logger.info(f"Copy bin {_bin} from {from_server.name} to {to_server.name}")
        bin_content = self.cli.get(
            f"{from_server.url}/{_bin}",
            params={"full": True, "index": True},
            auth=(from_server.username, from_server.password),
        ).json()
        bin_sync = self.cli.get(
            f"{from_server.url}/{_bin}/_sync",
            auth=(from_server.username, from_server.password),
        ).json()

        url = f"{to_server.url}/{_bin}"
        auth = (to_server.username, to_server.password)

        content = bin_content.get("_index")
        if content:
            self.cli.put(f"{url}/_index", json=content, auth=auth)

        for _id, content in bin_content["items"].items():
            self.cli.put(
                f"{url}/{_id}",
                params={"dttm": bin_sync[_bin][_id]["dttm"]},
                json=content,
                auth=auth,
            )

    def copy_item(
        self,
        _bin: str,
        _id: str,
        dttm: str,
        from_server: ServerConfig,
        to_server: ServerConfig,
    ):
        """Copy an item in a bin."""
        logger.info(
            f"Copy item {_bin}/{_id} from {from_server.name} to {to_server.name}"
        )
        content = self.cli.get(
            f"{from_server.url}/{_bin}/{_id}",
            auth=(from_server.username, from_server.password),
        ).json()
        self.cli.put(
            f"{to_server.url}/{_bin}/{_id}",
            params={"dttm": dttm},
            json=content,
            auth=(to_server.username, to_server.password),
        )

    def del_item(self, _bin: str, _id: str, server: ServerConfig):
        """Delete an item."""
        logger.info(f"Delete item {_bin}/{_id} from {server.name}")
        self.cli.delete(
            f"{server.url}/{_bin}/{_id}", auth=(server.username, server.password)
        )


def main(
    *,
    cfg_file: Annotated[
        Optional[Path], typer.Option("--cfg", help="Config file")
    ] = Path("pakhuis-servers.toml"),
    cleanup: Annotated[
        bool, typer.Option(help="Send cleanup command to server 1 (no sync)")
    ] = False,
    server1: Annotated[
        Optional[str],
        typer.Option(
            "-1", "--first", help="Server 1 (as defined in config)", show_default=False
        ),
    ] = None,
    server2: Annotated[
        Optional[str],
        typer.Option(
            "-2", "--second", help="Server 2 (as defined in config)", show_default=False
        ),
    ] = None,
    loglevel: LogLevels = "info",
):
    """Pakhuis sync.

    Synchronizes documents on two pakhuis services.
    Sync by date/time only, no compare of content.
    """
    init_logger(loglevel, log_dir=None)

    with logger.catch(onerror=lambda _: sys.exit(1)):
        logger.info("Start sync")

        ### config file
        cfg = ConfigReader.from_file(SyncConfig, cfg_file)
        for k, v in cfg.servers:
            if not v.name:
                v.name = k

        first = cfg.servers[server1 or cfg.default1]
        if not first.username:
            first.username = input(f"[{first.name}] Username: ")
        if not first.password:
            first.password = getpass.getpass(
                f"[{first.username}@{first.name}] Password: "
            )

        with httpx.Client(
            verify=False,
            follow_redirects=True,
            event_hooks={"response": [raise_for_status]},
        ) as cli:
            if cleanup:
                cli.get(
                    f"{first.url}/_cleanup",
                    params={"days": 180},
                    auth=(first.username, first.password),
                )

            else:
                second = cfg.servers[server2 or cfg.default2]
                if not second.username:
                    second.username = input(f"[{second.name}] Username: ")
                if not second.password:
                    second.password = getpass.getpass(
                        f"[{second.username}@{second.name}] Password: "
                    )

                ServerSync(cli).run(first, second)

        logger.success("Complete")


if __name__ == "__main__":
    typer.run(main)
