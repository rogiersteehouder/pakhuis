"""Pakhuis sync

Synchronizes documents on two pakhuis services. Sync by date/time only, no compare of content.

NOTE:
This will not update index definitions, since there is no way to determine which is the latest.
It will create an index definition if the whole bin is copied.
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-12-16"
__version__ = "1.0"

import sys
import getpass
import pathlib

import click
import httpx
from loguru import logger

try:
    import tomllib
except ImportError:
    import tomli as tomllib


def raise_for_status(response: httpx.Response):
    """Error for 4xx and 5xx status codes"""
    if response.status_code == 401:
        logger.error("Authorization failed")
        sys.exit(1)
    response.raise_for_status()


class ServerSync:
    def __init__(self, cli):
        self.cli = cli

        self.pwmgr = False

    def __enter__(self) -> "ServerSync":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        return False

    def run(self, first, second):
        first_content = self.cli.get(
            f"{first['url']}/_sync", auth=(first["username"], first["password"])
        ).json()
        second_content = self.cli.get(
            f"{second['url']}/_sync", auth=(second["username"], second["password"])
        ).json()

        first_bins = set(first_content.keys())
        second_bins = set(second_content.keys())

        if "pwmgr" in first_bins | second_bins:
            self.pwmgr = True

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
                # do not sync inactive items
                if (
                    first_content[_bin][_id]["active"]
                    or second_content[_bin][_id]["active"]
                ):
                    # copy last date/time to the other server
                    if (
                        first_content[_bin][_id]["dttm"]
                        > second_content[_bin][_id]["dttm"]
                    ):
                        if first_content[_bin][_id]["active"]:
                            self.copy_item(
                                _bin,
                                _id,
                                first_content[_bin][_id]["dttm"],
                                first,
                                second,
                            )
                        else:
                            self.del_item(_bin, _id, second)
                    elif (
                        first_content[_bin][_id]["dttm"]
                        < second_content[_bin][_id]["dttm"]
                    ):
                        if second_content[_bin][_id]["active"]:
                            self.copy_item(
                                _bin,
                                _id,
                                second_content[_bin][_id]["dttm"],
                                second,
                                first,
                            )
                        else:
                            self.del_item(_bin, _id, first)

    def copy_bin(self, _bin, from_server, to_server):
        logger.info(
            f"Copy bin {_bin} from {from_server['name']} to {to_server['name']}"
        )
        bin_content = self.cli.get(
            "{url}/{bin}".format(bin=_bin, **from_server),
            params={"full": True, "index": True},
            auth=(from_server["username"], from_server["password"]),
        ).json()
        bin_sync = self.cli.get(
            "{url}/{bin}/_sync".format(bin=_bin, **from_server),
            auth=(from_server["username"], from_server["password"]),
        ).json()

        url = "{url}/{bin}".format(bin=_bin, **to_server)

        content = bin_content.get("_index")
        if content:
            self.cli.put(
                f"{url}/_index",
                json=content,
                auth=(to_server["username"], to_server["password"]),
            )

        for _id, content in bin_content["items"].items():
            self.cli.put(
                f"{url}/{_id}",
                params={"dttm": bin_sync[_bin][_id]["dttm"]},
                json=content,
                auth=(to_server["username"], to_server["password"]),
            )

    def copy_item(self, _bin, _id, dttm, from_server, to_server):
        logger.info(
            f"Copy item {_bin}/{_id} from {from_server['name']} to {to_server['name']}"
        )
        content = self.cli.get(
            "{url}/{bin}/{id}".format(bin=_bin, id=_id, **from_server),
            auth=(from_server["username"], from_server["password"]),
        ).json()
        self.cli.put(
            "{url}/{bin}/{id}".format(bin=_bin, id=_id, **to_server),
            params={"dttm": dttm},
            json=content,
            auth=(to_server["username"], to_server["password"]),
        )

    def del_item(self, _bin, _id, server):
        logger.info(f"Delete item {_bin}/{_id} from {server['name']}")
        self.cli.delete(
            "{url}/{bin}/{id}".format(bin=_bin, id=_id, **server),
            auth=(server["username"], server["password"]),
        )


# Click documentation: https://click.palletsprojects.com/
@click.command()
@click.option(
    "--loglevel",
    type=click.Choice(logger._core.levels.keys(), case_sensitive=False),
    default="info",
    help="""Log level""",
)
@click.option(
    "--cfg",
    "cfg_file",
    type=click.Path(
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        path_type=pathlib.Path,
    ),
    default=pathlib.Path("pakhuis-servers.toml"),
    help="""Config file""",
)
@click.option("-1", "s1", help="Server 1 (as defined in config)")
@click.option("-2", "s2", help="Server 2 (as defined in config)")
@click.option(
    "--list",
    "pwmgr_list",
    is_flag=True,
    default=False,
    help="""Only refresh pwmgr list""",
)
@click.option(
    "--cleanup",
    is_flag=True,
    default=False,
    help="""Send cleanup command to both servers""",
)
def main(cfg_file, loglevel, s1, s2, pwmgr_list, cleanup):
    # loguru documentation: https://loguru.readthedocs.io/
    logger.remove()
    logger.add(
        sys.stderr,
        format="<light-black>{time:YYYY-MM-DD HH:mm:ss}</light-black> | <level>{level: <8}</level> | {message}",
        level=loglevel.upper(),
    )
    with logger.catch(onerror=lambda _: sys.exit(1)):

        logger.info("Start")

        ### config file
        cfg = tomllib.loads(cfg_file.read_text())

        if s1 is None:
            s1 = cfg["default-1"]
        first = cfg["servers"][s1]
        first["url"] = first["url"].strip("/")
        first["name"] = s1

        if s2 is None:
            s2 = cfg["default-2"]
        second = cfg["servers"][s2]
        second["url"] = second["url"].strip("/")
        second["name"] = s2

        ### http requests
        if "username" not in first:
            first["username"] = input("[{}] Username: ".format(s1))
        if "password" not in first:
            first["password"] = getpass.getpass(
                "[{}@{}] Password: ".format(first["username"], s1)
            )

        if not pwmgr_list:
            if "username" not in second:
                second["username"] = input("[{}] Username: ".format(s2))
            if "password" not in second:
                second["password"] = getpass.getpass(
                    "[{}@{}] Password: ".format(second["username"], s2)
                )

        # httpx documentation: https://www.python-httpx.org/
        with httpx.Client(
            verify=False,
            follow_redirects=True,
            event_hooks={"response": [raise_for_status]},
        ) as cli:

            with ServerSync(cfg, cli) as s:
                s.run()
                if s.pwmgr:
                    pwmgr_list = True

            if cleanup:
                for srv in (cfg["servers"]["first"], cfg["servers"]["second"]):
                    cli.get(
                        f"{srv['url']}/_cleanup",
                        params={"days": 180},
                        auth=(srv["username"], srv["password"]),
                    )

        logger.success("Complete")


if __name__ == "__main__":
    main()
