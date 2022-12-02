"""Database interface

Also does much of the heavy lifting for the webservice.
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-11-28"
__version__ = "1.1"

import datetime
import json
import hashlib
import sqlite3
from pathlib import Path
from typing import Any, Tuple

from jsonpointer import resolve_pointer
from loguru import logger


class NOTFOUND:
    pass


class SearchWhere:
    """Build the sql criteria from a search object"""
    def __init__(self, table_name: str = "SEARCH_VALUES", alias: str = "P."):
        self.table_name = table_name
        self.base_ops = {
            "and": "and",
            "or": "or",
        }
        if alias and not alias.endswith("."):
            alias = f"{alias}."
        self.alias = alias

    def set_keys(self, keys: list):
        ops = {}
        for key in keys:
            ops[key["key"]] = key["type"]
        ops.update(self.base_ops)
        self.ops = ops

    def process(self, doc: dict) -> Tuple[str, list]:
        where, params = self.op_and("", doc)
        if where:
            return (where, params)
        else:
            return ("", [])

    def aggregate(self, agg: str, key: str, value: dict) -> Tuple[str, list]:
        if not isinstance(value, dict):
            raise ValueError()
        where = []
        params = []
        for k, v in value.items():
            w, p = getattr(self, f"op_{self.ops[k]}")(k, v)
            where.append(w)
            params.extend(p)
        return ("(\n{}\n)".format(f"\n{agg} ".join(where)), params)

    def op_and(self, key: str, value: dict) -> Tuple[str, list]:
        return self.aggregate("and", key, value)

    def op_or(self, key: str, value: dict) -> Tuple[str, list]:
        return self.aggregate("or", key, value)

    def op_eq(self, key: str, value: Any) -> Tuple[str, list]:
        return (
            f"exists (select 1 from {self.table_name} where BIN = {self.alias}BIN and ID = {self.alias}ID and KEY = ? and VALUE = ?)",
            [key, json.dumps(value)],
        )

    def op_lt(self, key: str, value: Any) -> Tuple[str, list]:
        return (
            f"exists (select 1 from {self.table_name} where BIN = {self.alias}BIN and ID = {self.alias}ID and KEY = ? and VALUE > ?)",
            [key, json.dumps(value)],
        )

    def op_lte(self, key: str, value: Any) -> Tuple[str, list]:
        return (
            f"exists (select 1 from {self.table_name} where BIN = {self.alias}BIN and ID = {self.alias}ID and KEY = ? and VALUE >= ?)",
            [key, json.dumps(value)],
        )

    def op_gt(self, key: str, value: Any) -> Tuple[str, list]:
        return (
            f"exists (select 1 from {self.table_name} where BIN = {self.alias}BIN and ID = {self.alias}ID and KEY = ? and VALUE < ?)",
            [key, json.dumps(value)],
        )

    def op_gte(self, key: str, value: Any) -> Tuple[str, list]:
        return (
            f"exists (select 1 from {self.table_name} where BIN = {self.alias}BIN and ID = {self.alias}ID and KEY = ? and VALUE <= ?)",
            [key, json.dumps(value)],
        )

    def op_in_list(self, key: str, value: Any) -> Tuple[str, list]:
        # in_list is processed differently when generating the index, searching is the same as eq
        return self.op_eq(key, value)

    def op_glob(self, key: str, value: Any) -> Tuple[str, list]:
        # text search on value: assume string and remove leading and trailing quotes
        return (
            f"""exists (select 1 from {self.table_name} where BIN = {self.alias}BIN and ID = {self.alias}ID and KEY = ? and trim(VALUE, '"') glob ?)""",
            [key, value],
        )

    def op_like(self, key: str, value: Any) -> Tuple[str, list]:
        # text search on value: assume string and remove leading and trailing quotes
        return (
            f"""exists (select 1 from {self.table_name} where BIN = {self.alias}BIN and ID = {self.alias}ID and KEY = ? and trim(VALUE, '"') like ?)""",
            [key, value],
        )


class Database:
    _version = (1, 1)

    def __init__(self, path: Path):
        self._logger = logger.bind(logtype="pakhuis.database")
        self.path = path
        if not path.exists() or self.version() != self._version:
            self.init_db()
        self.search_where = SearchWhere()
        self.bin_cfg: dict[str, dict] = {}

    def init_db(self):
        with sqlite3.connect(self.path) as conn:
            if self.version() < (1, 0):
                conn.executescript(
                    """
                    begin;
                    create table CONFIG ("VERSION" text);
                    create table PAKHUIS ("BIN" text, "ID" text, "DTTM" text default CURRENT_TIMESTAMP, "STATUS" text default "A", "CONTENT" text);
                    create table BIN_CONFIG ("BIN" text, "INCLUDE_ID" text default "N");
                    create table SEARCH_KEYS ("BIN" text, "KEY" text, "PATH" text, "TYPE" text);
                    create table SEARCH_VALUES ("BIN" text, "KEY" text, "ID" text, "VALUE" text);
                    insert into CONFIG values('1.1');
                    commit;
                    """
                )
            if self.version() < (1, 1):
                # 1.0 -> 1.1: bin config
                conn.executescript(
                    """
                    begin;
                    create table BIN_CONFIG ("BIN" text, "INCLUDE_ID" text default "N");
                    update CONFIG set VERSION = '1.1';
                    commit;
                    """
                )

        if self.version() != self._version:
            raise Exception("Database version does not match.")

    def version(self) -> str:
        self._logger.debug("Get database version")
        if not self.path.exists():
            return (0, 0)
        else:
            with sqlite3.connect(self.path) as conn:
                try:
                    for row in conn.execute("""select VERSION from CONFIG"""):
                        return tuple(int(x) for x in row[0].split("."))
                except:
                    return (0, 0)

    def _get_index_keys(self, conn: sqlite3.Connection, _bin: str) -> list:
        result = []
        for row in conn.execute(
            """select KEY, PATH, TYPE from SEARCH_KEYS where BIN = ?""", (_bin,)
        ):
            result.append({"key": row[0], "path": row[1], "type": row[2]})
        return result

    def _set_index_item(
        self, conn: sqlite3.Connection, _bin: str, _id: str, content: Any
    ):
        self._logger.trace("Setting index for item {} in {}", _id, _bin)
        conn.execute(
            """delete from SEARCH_VALUES where BIN = ? and ID = ?""", (_bin, _id)
        )
        if content is None:
            return
        for key in self._get_index_keys(conn, _bin):
            if key["type"] == "in_list":
                for v in resolve_pointer(content, key["path"], default=None):
                    conn.execute(
                        """insert into SEARCH_VALUES (BIN, KEY, ID, VALUE) values (?, ?, ?, ?)""",
                        (_bin, key["key"], _id, json.dumps(v)),
                    )
            else:
                conn.execute(
                    """insert into SEARCH_VALUES (BIN, KEY, ID, VALUE) values (?, ?, ?, ?)""",
                    (
                        _bin,
                        key["key"],
                        _id,
                        json.dumps(resolve_pointer(content, key["path"], default=None)),
                    ),
                )

    def get_bins(self) -> dict:
        self._logger.debug("List of bins")
        result = {"count": 0, "items": []}
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select distinct P.BIN from PAKHUIS P where P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A' order by 1"""
            ):
                result["items"].append(row[0])
        result["count"] = len(result["items"])
        return result

    def get_bin(self, _bin: str) -> dict:
        self._logger.debug("List of items in {}", _bin)
        result = {"count": 0, "items": []}
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select P.ID from PAKHUIS P where P.BIN = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A' order by 1""",
                (_bin,),
            ):
                result["items"].append(row[0])
        result["count"] = len(result["items"])
        return result

    def set_bin_config(self, _bin: str, cfg: dict):
        self._logger.debug("New config for {}", _bin)
        self.bin_cfg.pop(_bin, None)
        with sqlite3.connect(self.path) as conn:
            conn.execute("""delete from BIN_CONFIG where BIN = ?""", (_bin,))
            include_id = cfg.get("include_id", "N")
            if isinstance(include_id, bool):
                include_id = "Y" if include_id else "N"
            conn.execute(
                """insert into BIN_CONFIG (BIN, INCLUDE_ID) values (?, ?)""",
                (_bin, include_id),
            )

    def get_bin_config(self, _bin: str) -> dict:
        if _bin not in self.bin_cfg:
            self._logger.debug("Get config for {}", _bin)
            with sqlite3.connect(self.path) as conn:
                include_id = False
                for row in conn.execute("""select INCLUDE_ID from BIN_CONFIG where BIN = ?""", (_bin,)):
                    include_id = (row[0] == "Y")
            self.bin_cfg[_bin] = { "include_id": include_id }
        return self.bin_cfg[_bin]

    def set_index(self, _bin: str, index: dict):
        self._logger.debug("New index on {}", _bin)
        with sqlite3.connect(self.path) as conn:
            conn.execute("""delete from SEARCH_KEYS where BIN = ?""", (_bin,))
            for k, v in index.items():
                if isinstance(v, str):
                    v = {"path": v}
                conn.execute(
                    """insert into SEARCH_KEYS (BIN, KEY, PATH, TYPE) values (?, ?, ?, ?)""",
                    (_bin, k, v["path"], v.get("type", "eq")),
                )
        self.refresh_index(_bin)

    def get_index(self, _bin: str) -> dict:
        self._logger.debug("Get index on {}", _bin)
        result = {}
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select KEY, PATH, TYPE from SEARCH_KEYS where BIN = ?""", (_bin,)
            ):
                result[row[0]] = {"path": row[1], "type": row[2]}
        return result

    def get_index_values(self, _bin: str, key: str) -> list:
        self._logger.debug("Get index values for {} on {}", key, _bin)
        result = []
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select distinct VALUE from SEARCH_VALUES where BIN = ? and KEY = ? order by VALUE asc""", (_bin, key)
            ):
                result.append(json.loads(row[0]))
        return result

    def refresh_index(self, _bin: str):
        self._logger.debug("Refresh index on {}", _bin)
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select P.ID, P.CONTENT from PAKHUIS P where P.BIN = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A'""",
                (_bin,),
            ):
                self._set_index_item(conn, _bin, row[0], json.loads(row[1]))

    def set_item(self, _bin: str, _id: str, content: Any, dttm: datetime.datetime = None):
        self._logger.debug("Set item {} in {}", _id, _bin)
        with sqlite3.connect(self.path) as conn:
            if dttm is None:
                conn.execute(
                    """insert into PAKHUIS (BIN, ID, CONTENT) values (?, ?, ?)""",
                    (_bin, _id, json.dumps(content)),
                )
            else:
                conn.execute(
                    """insert into PAKHUIS (BIN, ID, DTTM, CONTENT) values (?, ?, ?, ?)""",
                    (_bin, _id, dttm, json.dumps(content)),
                )
            self._set_index_item(conn, _bin, _id, content)

    def get_items(self, _bin: str):
        self._logger.debug("List of items in {}", _bin)
        result = {"count": 0, "items": {}}
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select P.ID, P.CONTENT from PAKHUIS P where P.BIN = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A' order by 1""",
                (_bin,),
            ):
                content = json.loads(row[1])
                if self.get_bin_config(_bin)['include_id']:
                    content["id"] = row[0]
                result["items"][row[0]] = content
        result["count"] = len(result["items"])
        return result

    def get_item(self, _bin: str, _id: str) -> Any:
        self._logger.debug("Get item {} from {}", _id, _bin)
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select P.CONTENT from PAKHUIS P where P.BIN = ? and P.ID = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A'""",
                (_bin, _id),
            ):
                content = json.loads(row[0])
                if self.get_bin_config(_bin)['include_id']:
                    content["id"] = _id
                return content
        return NOTFOUND

    def get_item_meta(self, _bin: str, _id: str) -> Any:
        self._logger.debug("Get item {} from {}", _id, _bin)
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select P.DTTM, P.CONTENT from PAKHUIS P where P.BIN = ? and P.ID = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A'""",
                (_bin, _id),
            ):
                return {
                    "bin": _bin,
                    "id": _id,
                    "dttm": row[0],
                    "size": len(row[1]),
                    "md5": hashlib.md5(row[1].encode("utf-8")).hexdigest(),
                }
        return NOTFOUND

    def get_item_history(self, _bin: str, _id: str) -> dict:
        self._logger.debug("Get history for item {} from {}", _id, _bin)
        result = []
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(
                """select P.DTTM, P.STATUS, P.CONTENT from PAKHUIS P where P.BIN = ? and P.ID = ? order by 1""",
                (_bin, _id),
            ):
                result.append(
                    {
                        "dttm": row[0],
                        "active": (row[1] == "A"),
                        "content": json.loads(row[2]),
                    }
                )
        return result if result else NOTFOUND

    def search_items(self, _bin: str, search: dict) -> dict:
        self._logger.debug("Search items from {}", _bin)
        result = {"count": 0, "items": {}}
        with sqlite3.connect(self.path) as conn:
            self.search_where.set_keys(self._get_index_keys(conn, _bin))
            where, params = self.search_where.process(search)
            if not where:
                where = "1=1"
            params.insert(0, _bin)
            for row in conn.execute(
                f"""select P.ID, P.CONTENT from PAKHUIS P where P.BIN = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A' AND {where} order by 1""",
                params,
            ):
                content = json.loads(row[1])
                if self.get_bin_config(_bin)['include_id']:
                    content["id"] = row[0]
                result["items"][row[0]] = content
        result["count"] = len(result["items"])
        return result

    def del_item(self, _bin: str, _id: str):
        self._logger.debug("Delete item {} from {}", _id, _bin)
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """insert into PAKHUIS (BIN, ID, STATUS, CONTENT) values (?, ?, 'I', 'null')""",
                (_bin, _id),
            )
            self._set_index_item(conn, _bin, _id, None)

    def del_bin(self, _bin: str):
        self._logger.debug("Delete {}", _bin)
        with sqlite3.connect(self.path) as conn:
            conn.execute("""delete from PAKHUIS where BIN = ?""", (_bin,))
            conn.execute("""delete from BIN_CONFIG where BIN = ?""", (_bin,))
            conn.execute("""delete from SEARCH_KEYS where BIN = ?""", (_bin,))
            conn.execute("""delete from SEARCH_VALUES where BIN = ?""", (_bin,))

    def sync_list(self, _bin: str = "") -> dict:
        self._logger.debug("Sync list for {}", _bin if _bin else "all")
        if _bin:
            where = "P.BIN = ? and"
            params = [_bin]
        else:
            where = ""
            params = []
        result = {}
        with sqlite3.connect(self.path) as conn:
            for row in conn.execute(f"""select P.BIN, P.ID, P.DTTM, P.STATUS from PAKHUIS P where {where} P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) order by 1, 2""", params):
                bin_obj = result.setdefault(row[0], dict())
                bin_obj[row[1]] = { "dttm": row[2], "active": (row[3] == 'A') }
        return result
