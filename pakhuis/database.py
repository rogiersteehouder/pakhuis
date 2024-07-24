"""Database abstraction for pakhuis."""

# ruff: noqa: E501, RUF012, PLE1205

import datetime
import json
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from jsonpointer import resolve_pointer

from .log import logger


# sqlite3 datetime conversion
# see https://docs.python.org/3/library/sqlite3.html#adapter-and-converter-recipes


def adapt_datetime_iso(val: datetime.datetime) -> str:
    """Adapt datetime.datetime to ISO format."""
    return val.isoformat(" ", timespec="seconds")


sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)


def convert_datetime(val: bytes) -> datetime.datetime:
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.datetime.fromisoformat(val.decode())


sqlite3.register_converter("datetime", convert_datetime)


class NOTFOUND:
    """Item not found (to distinguish from null/None)."""


class SearchWhere:
    """Build sql criteria from a search dict."""

    def __init__(self) -> None:
        """Build sql criteria from a search dict."""
        self.base_ops = {
            "and": "and",
            "or": "or",
        }
        self.ops: dict[str, str] = {}

        # this comes by a lot
        self._base_crit = (
            "select 1 from SEARCH_VALUES where BIN = P.BIN and ID = P.ID and KEY = ?"
        )

    def set_keys(self, keys: list[dict[str, str]]) -> None:
        """Set the search keys."""
        ops = {k["key"]: k["type"] for k in keys}
        ops.update(self.base_ops)
        self.ops = ops

    def process(self, search: dict) -> tuple[str, list]:
        """Convert a search dict into sql criteria."""
        where, params = self.op_and("", search)
        if where:
            return where, params
        else:
            return "", []

    def aggregate(self, agg: str, key: str, value: dict) -> tuple[str, list]:
        """Aggregate multiple criteria by an operator (usually and/or)."""
        where = []
        params = []
        for k, v in value.items():
            w, p = getattr(self, f"op_{self.ops[k]}")(k, v)
            where.append(w)
            params.extend(p)
        return "(\n{}\n)".format(f"\n{agg} ".join(where)), params

    def op_and(self, key: str, value: dict) -> tuple[str, list]:
        """Operator: and."""
        return self.aggregate("and", key, value)

    def op_or(self, key: str, value: dict) -> tuple[str, list]:
        """Operator: or."""
        return self.aggregate("or", key, value)

    def op_eq(self, key: str, value: Any) -> tuple[str, list]:
        """Operator: eq (equal to)."""
        return (f"exists({self._base_crit} and ? = VALUE))", [key, json.dumps(value)])

    def op_lt(self, key: str, value: Any) -> tuple[str, list]:
        """Operator: lt (less than)."""
        return (f"exists({self._base_crit} and ? < VALUE))", [key, json.dumps(value)])

    def op_lte(self, key: str, value: Any) -> tuple[str, list]:
        """Operator: lte (less than or equal to)."""
        return (f"exists({self._base_crit} and ? <= VALUE))", [key, json.dumps(value)])

    def op_gt(self, key: str, value: Any) -> tuple[str, list]:
        """Operator: gt (greater than)."""
        return (f"exists({self._base_crit} and ? > VALUE))", [key, json.dumps(value)])

    def op_gte(self, key: str, value: Any) -> tuple[str, list]:
        """Operator: gte (greater than or equal to)."""
        return (f"exists({self._base_crit} and ? >= VALUE))", [key, json.dumps(value)])

    def op_in_list(self, key: str, value: Any) -> tuple[str, list]:
        """Operator: in_list (value occurs in json list)."""
        # in_list is processed differently when generating the index,
        # searching is the same as eq
        return self.op_eq(key, value)

    def op_glob(self, key: str, value: str) -> tuple[str, list]:
        """Operator: glob (text search by glob)."""
        # text search on value: assume string and remove leading and trailing quotes
        return (
            f"""exists({self._base_crit} and trim(VALUE, '"') glob ?)""",
            [key, value],
        )

    def op_like(self, key: str, value: str) -> tuple[str, list]:
        """Operator: like (text search by sql like)."""
        # text search on value: assume string and remove leading and trailing quotes
        return (
            f"""exists({self._base_crit} and trim(VALUE, '"') like ?)""",
            [key, value],
        )


class PakhuisDatabase:
    """Pakhuis database."""

    _version = (1, 2)

    # config for a bin: key in dict: (key in db, default value)
    _bin_cfg_keys = {
        "include_id": ("include_id", "N"),
    }

    # sql queries
    _queries = {
        "get_bins": """select distinct P.BIN from PAKHUIS P where P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A' order by 1""",
        "get_bin_item_ids": """select P.ID from PAKHUIS P where P.BIN = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A' order by 1""",
        "get_bin_items": """select P.ID, P.CONTENT from PAKHUIS P where P.BIN = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A' order by 1""",
        "set_bin_config": """insert or replace into BIN_CONFIG (BIN, INCLUDE_ID) values (?, ?)""",
        "get_bin_config": """select INCLUDE_ID from BIN_CONFIG where BIN = ?""",
        "set_item": """insert into PAKHUIS (BIN, ID, DTTM, CONTENT) values (?, ?, ?, ?)""",
        "get_item": """select P.CONTENT from PAKHUIS P where P.BIN = ? and P.ID = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A'""",
        "get_item_history": """select P.DTTM, P.STATUS, P.CONTENT from PAKHUIS P where P.BIN = ? and P.ID = ? order by 1""",
        "get_index_keys": """select KEY, PATH, TYPE from SEARCH_KEYS where BIN = ?""",
        "set_index_item_del": """delete from SEARCH_VALUES where BIN = ? and ID = ?""",
        "set_index_item_ins": """insert into SEARCH_VALUES (BIN, KEY, ID, VALUE) values (?, ?, ?, ?)""",
        "set_index_del": """delete from SEARCH_KEYS where BIN = ?""",
        "set_index_ins": """insert into SEARCH_KEYS (BIN, KEY, PATH, TYPE) values (?, ?, ?, ?)""",
        "get_index": """select KEY, PATH, TYPE from SEARCH_KEYS where BIN = ?""",
        "get_index_values": """select distinct VALUE from SEARCH_VALUES where BIN = ? and KEY = ? order by VALUE asc""",
        "search_items": """select P.ID, P.CONTENT from PAKHUIS P where P.BIN = ? and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) and P.STATUS = 'A' and 1=1 order by 1""",
        "sync_list": """select P.BIN, P.ID, P.DTTM, P.STATUS from PAKHUIS P where 1=1 and P.DTTM = (select max(DTTM) from PAKHUIS where BIN = P.BIN and ID = P.ID) order by 1, 2""",
        "cleanup": """delete from PAKHUIS where 1=1 and DTTM < ? and (STATUS = "I" or exists (select 1 from PAKHUIS P where P.BIN = PAKHUIS.BIN and P.ID = PAKHUIS.ID and P.DTTM > PAKHUIS.DTTM))""",
    }

    ### Init
    def __init__(self, path: Path) -> None:
        """Pakhuis database."""
        self._logger = logger.bind(logtype="pakhuis.database")
        self.path = path
        self.bin_cfg: dict[str, dict[str, Any]] = {}
        self.search_where = SearchWhere()

        v = self.version()
        if v != self._version:
            from .database_init import init_db

            with self.connect() as conn:
                init_db(conn, v)

    ### Support methods
    def _get_content(self, _bin: str, row: sqlite3.Row) -> Any:
        """Extract json content from the database row."""
        content = json.loads(row["content"])
        if self.get_bin_config(_bin)["include_id"]:
            content["id"] = row["id"]
        return content

    def _get_index_keys(
        self, conn: sqlite3.Connection, _bin: str
    ) -> list[dict[str, str]]:
        """Get the index items for a bin."""
        result = []
        for row in conn.execute(self._queries["get_index_keys"], (_bin,)):
            result.append({"key": row["key"], "path": row["path"], "type": row["type"]})
        return result

    def _set_index_item(
        self, conn: sqlite3.Connection, _bin: str, _id: str, content: Any
    ):
        """Set index values for an item in a bin."""
        self._logger.trace("Setting index for item {} in {}", _id, _bin)
        conn.execute(self._queries["set_index_item_del"], (_bin, _id))
        if content is None:
            return
        for key in self._get_index_keys(conn, _bin):
            if key["type"] == "in_list":
                for v in resolve_pointer(content, key["path"], default=None):
                    conn.execute(
                        self._queries["set_index_item_ins"],
                        (_bin, key["key"], _id, json.dumps(v)),
                    )
            else:
                v = resolve_pointer(content, key["path"], default=None)
                conn.execute(
                    self._queries["set_index_item_ins"],
                    (_bin, key["key"], _id, json.dumps(v)),
                )

    ### Database connection
    @contextmanager
    def connect(self) -> Generator[sqlite3.Connection, None, None]:
        """Open database connection."""
        with sqlite3.connect(self.path) as conn:
            conn.row_factory = sqlite3.Row
            yield conn

    ### Database version
    def version(self) -> tuple[int, int]:
        """Database version."""
        self._logger.debug("Get database version")
        if not self.path.exists():
            return (0, 0)
        with self.connect() as conn:
            try:
                row = conn.execute("""select VERSION from CONFIG""").fetchone()
                return tuple(int(x) for x in row[0].split("."))
            except sqlite3.Error:
                return (0, 0)

    ### Bins
    def get_bins(self) -> list[str]:
        """List of bins."""
        self._logger.debug("List of bins")
        result = []
        with self.connect() as conn:
            result = [row[0] for row in conn.execute(self._queries["get_bins"])]
        return result

    def get_bin_item_ids(self, _bin: str) -> list[str]:
        """List of item ids in a bin."""
        self._logger.debug("List of item ids in {}", _bin)
        result = []
        with self.connect() as conn:
            result = [
                row[0]
                for row in conn.execute(self._queries["get_bin_item_ids"], (_bin,))
            ]
        return result

    def get_bin_items(self, _bin: str) -> dict[str, Any]:
        """List of items in a bin."""
        self._logger.debug("List of items in {}", _bin)
        result = {}
        with self.connect() as conn:
            for row in conn.execute(self._queries["get_bin_items"], (_bin,)):
                result[row["id"]] = self._get_content(_bin, row)
        return result

    def del_bin(self, _bin: str):
        """Remove a bin (no history!)."""
        self._logger.debug("Remove bin {}", _bin)
        with self.connect() as conn:
            conn.execute("""delete from PAKHUIS where BIN = ?""", (_bin,))
            conn.execute("""delete from BIN_CONFIG where BIN = ?""", (_bin,))
            conn.execute("""delete from SEARCH_KEYS where BIN = ?""", (_bin,))
            conn.execute("""delete from SEARCH_VALUES where BIN = ?""", (_bin,))

    ### Bin config
    def set_bin_config(self, _bin: str, cfg: dict[str, str]) -> None:
        """Set the config for a bin."""
        self._logger.debug("Set config for {}", _bin)
        self.bin_cfg.pop(_bin, None)
        with self.connect() as conn:
            conn.execute(
                self._queries["set_bin_config"],
                (_bin, "Y" if cfg["include_id"] in ("Y", True) else "N"),
            )

    def get_bin_config(self, _bin: str) -> dict[str, Any]:
        """Get the config for a bin."""
        if _bin not in self.bin_cfg:
            self._logger.debug("Get config for {}", _bin)
            with self.connect() as conn:
                row = conn.execute(self._queries["get_bin_config"], (_bin,)).fetchone()
                if row:
                    result = {k: row[v] for k, (v, _) in self._bin_cfg_keys.items()}
                else:
                    result = {k: v for k, (_, v) in self._bin_cfg_keys.items()}
            result["include_id"] = result["include_id"] == "Y"
            self.bin_cfg[_bin] = result
        return self.bin_cfg[_bin]

    ### Items
    def set_item(
        self, _bin: str, _id: str, content: Any, dttm: datetime.datetime | None = None
    ) -> None:
        """Set an item in a bin."""
        if dttm is None:
            dttm = datetime.datetime.now(tz=datetime.UTC)
        self._logger.debug("Set item {} in {} with stamp {}", _id, _bin, dttm)
        with self.connect() as conn:
            conn.execute(
                self._queries["set_item"], (_bin, _id, dttm, json.dumps(content))
            )
            self._set_index_item(conn, _bin, _id, content)

    def get_item(self, _bin: str, _id: str) -> Any:
        """Get an item from a bin."""
        self._logger.debug("Get item {} from {}", _id, _bin)
        with self.connect() as conn:
            row = conn.execute(self._queries["get_item"], (_bin, _id)).fetchone()
            if row:
                return self._get_content(_bin, row)
        return NOTFOUND

    def get_item_history(self, _bin: str, _id: str) -> list[dict[str, Any]]:
        """Get an item from a bin with history."""
        self._logger.debug("Get item {} from {} with history", _id, _bin)
        result = []
        with self.connect() as conn:
            for row in conn.execute(self._queries["get_item_history"], (_bin, _id)):
                result.append(
                    {
                        "dttm": row["dttm"],
                        "active": row["status"] == "A",
                        "content": self._get_content(_bin, row),
                    }
                )
        return result or NOTFOUND

    def del_item(
        self, _bin: str, _id: str, dttm: datetime.datetime | None = None
    ) -> None:
        """Remove an item from a bin."""
        if dttm is None:
            dttm = datetime.datetime.now(tz=datetime.UTC)
        self._logger.debug("Remove item {} from {} with stamp {}", _id, _bin, dttm)
        with self.connect() as conn:
            conn.execute(self._queries["set_item"], (_bin, _id, dttm, ""))
            self._set_index_item(conn, _bin, _id, None)

    ### Search indexes
    def set_index(self, _bin: str, index: dict[str, str | dict[str, str]]):
        """Set the index on a bin."""
        # single string is the same as {"path": <string>}
        # for the rest, see the SearchWhere class above
        self._logger.debug("New index on {}", _bin)
        with self.connect() as conn:
            conn.execute(self._queries["set_index_del"], (_bin,))
            for k, v in index.items():
                if isinstance(v, str):
                    v = {"path": v}  # noqa: PLW2901
                conn.execute(
                    self._queries["set_index_ins"],
                    (_bin, k, v["path"], v.get("type", "eq")),
                )
        self.refresh_index(_bin)

    def get_index(self, _bin: str) -> dict[str, dict[str, str]]:
        """Get the index on a bin."""
        self._logger.debug("Get index on {}", _bin)
        result = {}
        with self.connect() as conn:
            for row in conn.execute(self._queries["get_index"], (_bin,)):
                result[row["key"]] = {"path": row["path"], "type": row["type"]}
        return result

    def get_index_values(self, _bin: str, key: str) -> list:
        """Get the values in the index for a key (useful for select lists)."""
        self._logger.debug("Get index values for {} on {}", key, _bin)
        result = []
        with self.connect() as conn:
            for row in conn.execute(self._queries["get_index_values"], (_bin, key)):
                result.append(json.loads(row[0]))
        return result

    def refresh_index(self, _bin: str):
        """Refresh the index on a bin."""
        self._logger.debug("Refresh index on {}", _bin)
        with self.connect() as conn:
            for row in conn.execute(self._queries["get_bin_items"], (_bin,)):
                self._set_index_item(conn, _bin, row["id"], json.loads(row["content"]))

    ### Search
    def search_items(self, _bin: str, search: dict) -> dict[str, Any]:
        """Search items in a bin (requires an index)."""
        self._logger.debug("Search items from {}", _bin)
        result = {}
        with self.connect() as conn:
            self.search_where.set_keys(self._get_index_keys(conn, _bin))
            where, params = self.search_where.process(search)
            params.insert(0, _bin)
            q = self._queries["search_items"]
            if where:
                q = q.replace("1=1", where)
            for row in conn.execute(q, params):
                content = self._get_content(_bin, row)
                result[row["id"]] = content
        return result

    ### Sync list
    def sync_list(self, _bin: str = "") -> dict[str, dict[str, dict[str, Any]]]:
        """Generate a sync list for a bin (or everything)."""
        self._logger.debug("Sync list for {}", _bin if _bin else "all")

        q = self._queries["sync_list"]
        if _bin:
            q = q.replace("1=1", "P.BIN = ?")
            params = (_bin,)
        else:
            params = ()

        result = {}
        with self.connect() as conn:
            for row in conn.execute(q, params):
                bin_obj = result.setdefault(row["bin"], {})
                bin_obj[row["id"]] = {
                    "dttm": row["dttm"],
                    "active": (row["status"] == "A"),
                }
        return result

    ### Cleanup
    def cleanup(self, _bin: str = "", *, dt: datetime.date) -> int:
        """Cleanup: remove items older than the given date if it is not the current active item."""
        self._logger.debug(
            "Cleanup items in {} before {:%Y-%m-%d}", _bin if _bin else "all", dt
        )

        q = self._queries["cleanup"]
        if _bin:
            q = q.replace("1=1", "P.BIN = ?")
            params = (_bin,)
        else:
            params = ()

        result = 0
        with self.connect() as conn:
            cur = conn.execute(q, params)
            result = cur.rowcount
        return result
