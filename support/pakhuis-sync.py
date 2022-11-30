#!/usr/bin/env python3
# encoding: UTF-8

"""Pakhuis sync

Synchronizes documents on two pakhuis services. Sync by date/time only, no compare of content.

NOTE:
This will not update index definitions, since there is no way to determine which is the latest.
It will create an index definition if the whole bin is copied.
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-11-29"
__version__ = "0.1"

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


def raise_for_status(response):
    """Error for 4xx and 5xx status codes"""
    response.raise_for_status()


class ServerSync:
    def __init__(self, cfg, cli):
        self.cfg = cfg
        self.cli = cli

    def run(self):
        first = self.cfg["servers"]["first"]
        second = self.cfg["servers"]["second"]

        first["url"] = first["url"].strip('/')
        second["url"] = second["url"].strip('/')

        first_content = self.cli.get(f"{first['url']}/_sync", auth=(first["username"], first["password"])).json()
        second_content = self.cli.get(f"{second['url']}/_sync", auth=(second["username"], second["password"])).json()

        first_bins = set(first_content.keys())
        second_bins = set(second_content.keys())

        # copy missing bins in total
        for _bin in (first_bins - second_bins):
            self.copy_bin(_bin, first, second)
        for _bin in (second_bins - first_bins):
            self.copy_bin(_bin, second, first)

        # compare bins
        for _bin in (first_bins & second_bins):
            first_items = set(first_content[_bin].keys())
            second_items = set(second_content[_bin].keys())

            # copy missing items in total, skipping inactive items
            for _id in (first_items - second_items):
                if first_content[_bin][_id]["active"]:
                    self.copy_item(_bin, _id, first, second)
            for _id in (second_items - first_items):
                if second_content[_bin][_id]["active"]:
                    self.copy_item(_bin, _id, second, first)

            # compare items
            for _id in (first_items & second_items):
                # do not sync inactive items
                if first_content[_bin][_id]["active"] or second_content[_bin][_id]["active"]:
                    # copy last date/time to the other server
                    if first_content[_bin][_id]["dttm"] > second_content[_bin][_id]["dttm"]:
                        if first_content[_bin][_id]["active"]:
                            self.copy_item(_bin, _id, first, second)
                        else:
                            self.del_item(_bin, _id, second)
                    elif first_content[_bin][_id]["dttm"] < second_content[_bin][_id]["dttm"]:
                        if second_content[_bin][_id]["active"]:
                            self.copy_item(_bin, _id, second, first)
                        else:
                            self.del_item(_bin, _id, first)

    def copy_bin(self, _bin, from_server, to_server):
        logger.info(f"Copy bin {_bin} from {from_server['name']} to {to_server['name']}")
        bin_content = self.cli.get("{url}/{bin}".format(bin=_bin, **from_server), params={ "full": True, "index": True }, auth=(from_server["username"], from_server["password"])).json()
        url = "{url}/{bin}".format(bin=_bin, **to_server)
        content = bin_content.get("_index")
        if content:
            self.cli.put(f"{url}/_index", json=content, auth=(to_server["username"], to_server["password"]))
        for _id, content in bin_content["items"].items():
            self.cli.put(f"{url}/{_id}", json=content, auth=(to_server["username"], to_server["password"]))

    def copy_item(self, _bin, _id, from_server, to_server):
        logger.info(f"Copy item {_bin}/{_id} from {from_server['name']} to {to_server['name']}")
        content = self.cli.get("{url}/{bin}/{id}".format(bin=_bin, id=_id, **from_server), auth=(from_server["username"], from_server["password"])).json()
        self.cli.put("{url}/{bin}/{id}".format(bin=_bin, id=_id, **to_server), json=content, auth=(to_server["username"], to_server["password"]))

    def del_item(self, _bin, _id, server):
        logger.info(f"Delete item {_bin}/{_id} from {server['name']}")
        self.cli.delete("{url}/{bin}/{id}".format(bin=_bin, id=_id, **server), auth=(server["username"], server["password"]))


# Click documentation: https://click.palletsprojects.com/
@click.command()
@click.option(
    "--loglevel",
    type=click.Choice(logger._core.levels.keys(), case_sensitive=False),
    default="warning",
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
    default=pathlib.Path(__file__).with_suffix(".toml"),
    help="""Config file"""
)
def main(cfg_file, loglevel):
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
        first = cfg["servers"]["first"]
        second = cfg["servers"]["second"]

        ### http requests
        if "username" not in first:
            first["username"] = input("[{}] Username: ".format(first["name"]))
        if "password" not in first:
            first["password"] = getpass.getpass("[{}@{}] Password: ".format(first["username"], first["name"]))


        if "username" not in second:
            second["username"] = input("[{}] Username: ".format(second["name"]))
        if "password" not in second:
            second["password"] = getpass.getpass("[{}@{}] Password: ".format(second["username"], second["name"]))

        # httpx documentation: https://www.python-httpx.org/
        with httpx.Client(
            verify=False,
            follow_redirects=True,
            event_hooks={"response": [raise_for_status]},
        ) as cli:

            s = ServerSync(cfg, cli)
            s.run()

        logger.success("Complete")


if __name__ == "__main__":
    main()