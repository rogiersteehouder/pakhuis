"""Pakhuis webservice."""

import datetime
import uuid
from pathlib import Path

import jsonpatch
from starlette import status
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from . import __version__, database
from .log import logger


class Params:
    """Common parameters and easy access to uncommon ones."""

    def __init__(self, request: Request):
        """Common parameters and easy access to uncommon ones."""
        self.bin = request.path_params.get("_bin", "")
        self.id = request.path_params.get("_id", "")
        self.query_params = request.query_params

        self.user = request.headers.get("Caddy-Auth-User")

        self.get = request.method == "GET"
        self.post = request.method == "POST"
        self.put = request.method == "PUT"
        self.patch = request.method == "PATCH"
        self.delete = request.method == "DELETE"

        self.json = request.json

    def q(self, key: str, default: str = "") -> str:
        """Query parameters."""
        return self.query_params.get(key, default)


class PakhuisService:
    """Pakhuis Webservice."""

    def __init__(self, database_path: Path) -> None:
        """Pakhuis Webservice."""
        self._logger = logger.bind(logtype="pakhuis.webservice")
        self.db = database.PakhuisDatabase(database_path)
        self.routes = [
            Route("/", self.root, methods=["GET"]),
            Route("/_ping", self.ping, methods=["GET", "HEAD"]),
            Route("/_cleanup", self.cleanup, methods=["GET"]),
            Route("/_sync", self.sync_list, methods=["GET"]),
            Route("/{_bin}", self.bin, methods=["GET"]),
            Route("/{_bin}", self.delete_bin, methods=["DELETE"]),
            Route("/{_bin}", self.doc, methods=["POST"]),
            Route("/{_bin}/_cleanup", self.cleanup, methods=["GET"]),
            Route("/{_bin}/_config", self.bin_config, methods=["GET", "PUT"]),
            Route("/{_bin}/_index", self.bin_index, methods=["GET", "PUT"]),
            Route("/{_bin}/_index/values", self.bin_index_values, methods=["GET"]),
            Route("/{_bin}/_search", self.bin_search, methods=["GET", "POST"]),
            Route("/{_bin}/_sync", self.sync_list, methods=["GET"]),
            Route("/{_bin}/{_id}", self.doc, methods=["GET", "PUT", "PATCH"]),
            Route("/{_bin}/{_id}", self.delete_doc, methods=["DELETE"]),
            Route("/{_bin}/{_id}/_history", self.doc_history, methods=["GET"]),
        ]

    async def ping(self, request: Request):
        """Ping: show that the service works and return version info."""
        p = Params(request)
        v = "{}.{}".format(*self.db.version())
        return JSONResponse(
            {
                "app": "Pakhuis",
                "version": __version__,
                "db": v,
                "user": p.user,
            },
            status_code=status.HTTP_200_OK,
        )

    async def root(self, request: Request):
        """Root: list of bins."""
        items = self.db.get_bins()
        return JSONResponse(
            {"count": len(items), "items": items}, status_code=status.HTTP_200_OK
        )

    async def bin(self, request: Request):
        """List of items in a bin.

        query parameters:
            full: give a full list (with content) instead of just id
            index: include the index definition
        """
        p = Params(request)

        items = (
            self.db.get_bin_items(p.bin)
            if p.q("full")
            else self.db.get_bin_item_ids(p.bin)
        )
        index = self.db.get_index(p.bin) if p.q("index") else None

        result = {"count": len(items), "items": items}
        if index:
            result["_index"] = index

        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def bin_config(self, request: Request):
        """Get or set the bin config."""
        p = Params(request)
        code = status.HTTP_200_OK
        if p.put:
            cfg = await p.json()
            self.db.set_bin_config(p.bin, cfg)
            code = status.HTTP_201_CREATED
        result = self.db.get_bin_config(p.bin)
        return JSONResponse(result, status_code=code)

    async def bin_index(self, request: Request):
        """Get or set the bin index definition."""
        p = Params(request)
        code = status.HTTP_200_OK
        if p.put:
            index = await p.json()
            self.db.set_index(p.bin, index)
            code = status.HTTP_201_CREATED
        result = self.db.get_index(p.bin)
        return JSONResponse(result, status_code=code)

    async def bin_index_values(self, request: Request):
        """Get index values for a given index key (useful for select lists)."""
        p = Params(request)
        key = p.q("key")
        result = self.db.get_index_values(p.bin, key)
        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def bin_search(self, request: Request):
        """Search a bin with a search object."""
        p = Params(request)
        if p.get:
            srch = {}
            for k, v in p.query_params.items():
                srch[k] = v
        elif p.post:
            srch = await p.json()

        if not srch:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Empty search")

        try:
            items = self.db.search_items(p.bin, srch)
        except KeyError as exc:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, f"Not a search key: {exc.args[0]}"
            ) from exc

        return JSONResponse(
            {"count": len(items), "items": items}, status_code=status.HTTP_200_OK
        )

    async def doc(self, request: Request):
        """Get, set or patch a json document.

        query parameters:
            dttm: date/time of the document (iso format)
        """
        p = Params(request)
        dttm = p.q("dttm")
        if dttm:
            dttm = datetime.datetime.fromisoformat(dttm)
        code = status.HTTP_200_OK

        # add or overwrite
        if p.put or p.post:
            if not p.id:
                p.id = str(uuid.uuid4())
            content = await p.json()
            self.db.set_item(p.bin, p.id, content, dttm)
            code = status.HTTP_201_CREATED

        # patch existing
        elif p.patch:
            patch = await p.json()
            content = self.db.get_item(p.bin, p.id)
            if content is database.NOTFOUND:
                raise HTTPException(
                    status.HTTP_404_NOT_FOUND,
                    f"Item {p.id} not found in {p.bin}",
                )
            jsonpatch.apply_patch(content, patch, in_place=True)
            self.db.set_item(p.bin, p.id, content)

        # show
        result = self.db.get_item(p.bin, p.id)
        if (p.put or p.post) and result is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                f"Storing {p.id} in {p.bin} failed",
            )
        elif result is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, f"Item {p.id} not found in {p.bin}"
            )

        if p.put or p.post:
            result = {"id": p.id, "content": result}
        return JSONResponse(result, status_code=code)

    async def delete_doc(self, request: Request):
        """Delete a json document (mark as inactive in history)."""
        p = Params(request)
        content = self.db.get_item(p.bin, p.id)
        if content is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, f"Item {p.id} not found in {p.bin}"
            )
        self.db.del_item(p.bin, p.id)
        return Response(None, status_code=status.HTTP_204_NO_CONTENT)

    async def delete_bin(self, request: Request):
        """Delete a bin (remove from database, no history left!)."""
        p = Params(request)
        self.db.del_bin(p.bin)
        return Response(None, status_code=status.HTTP_204_NO_CONTENT)

    async def doc_history(self, request: Request):
        """Full history of a json document."""
        p = Params(request)
        result = self.db.get_item_history(p.bin, p.id)
        if result is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, f"Item {p.id} not found in {p.bin}"
            )
        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def sync_list(self, request: Request):
        """Sync list."""
        p = Params(request)
        return JSONResponse(self.db.sync_list(p.bin), status_code=status.HTTP_200_OK)

    async def cleanup(self, request: Request):
        """Cleanup.

        query parameters:
            dt: remove before this date (is format)
            days: remove older than this number of days
        """
        p = Params(request)
        if "dt" in p.query_params:
            dt = datetime.date.fromisofromat(p.q("dt"))
        elif "days" in p.query_params:
            dt = datetime.date.today() - datetime.timedelta(days=int(p.q("days")))
        else:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Date parameter missing")
        count = self.db.cleanup(p.bin, dt=dt)
        return JSONResponse({"count": count}, status_code=status.HTTP_200_OK)
