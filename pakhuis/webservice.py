import datetime
import uuid
from pathlib import Path

import jsonpatch
from loguru import logger
from starlette import status
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from . import __version__, database


class Params:
    """Common parameters and easy access to uncommon ones."""

    def __init__(self, request: Request):
        self.bin = request.path_params.get("_bin", "")
        self.id = request.path_params.get("_id", "")
        self.query_params = request.query_params

        self.get = request.method == "GET"
        self.post = request.method == "POST"
        self.put = request.method == "PUT"
        self.patch = request.method == "PATCH"
        self.delete = request.method == "DELETE"

        self.json = request.json

    def q(self, key: str, default: str = "") -> str:
        return self.query_params.get(key, default)


class Webservice:
    def __init__(self, database_path: Path):
        self._logger = logger.bind(logtype="pakhuis.webservice")
        self.db = database.Database(database_path)
        self.routes = [
            Route("/", self.root, methods=["GET"], name="root"),
            Route("/_ping", self.ping, methods=["GET", "HEAD"], name="ping"),
            Route("/_cleanup", self.cleanup, methods=["GET"], name="cleanup"),
            Route("/_sync", self.sync_list, methods=["GET"], name="sync"),
            Route("/{_bin}", self.bin, methods=["GET"], name="bin"),
            Route("/{_bin}", self.delete_bin, methods=["DELETE"]),
            Route("/{_bin}", self.doc, methods=["POST"], name="post_doc"),
            Route(
                "/{_bin}/_cleanup", self.cleanup, methods=["GET"], name="bin_cleanup"
            ),
            Route(
                "/{_bin}/_config",
                self.bin_config,
                methods=["GET", "PUT"],
                name="bin_config",
            ),
            Route(
                "/{_bin}/_index",
                self.bin_index,
                methods=["GET", "PUT"],
                name="bin_index",
            ),
            Route(
                "/{_bin}/_index/values",
                self.bin_index_values,
                methods=["GET"],
                name="bin_values",
            ),
            Route(
                "/{_bin}/_search",
                self.bin_search,
                methods=["GET", "POST"],
                name="bin_search",
            ),
            Route("/{_bin}/_sync", self.sync_list, methods=["GET"], name="bin_sync"),
            Route(
                "/{_bin}/{_id}", self.doc, methods=["GET", "PUT", "PATCH"], name="doc"
            ),
            Route("/{_bin}/{_id}", self.delete_doc, methods=["DELETE"]),
            Route(
                "/{_bin}/{_id}/_meta", self.doc_meta, methods=["GET"], name="doc_meta"
            ),
            Route(
                "/{_bin}/{_id}/_history",
                self.doc_history,
                methods=["GET"],
                name="doc_history",
            ),
        ]

    async def ping(self, request: Request):
        result = "{}.{}".format(*self.db.version())
        return JSONResponse(
            {
                "app": "Pakhuis",
                "version": __version__,
                "db": result,
                "user": request.user.display_name,
            },
            status_code=status.HTTP_200_OK,
        )

    async def root(self, request: Request):
        result = self.db.get_bins()
        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def bin(self, request: Request):
        p = Params(request)
        if p.q("full"):
            result = self.db.get_items(p.bin)
            if request.query_params.get("index"):
                result["_index"] = self.db.get_index(p.bin)
        else:
            result = self.db.get_bin(p.bin)
        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def bin_config(self, request: Request):
        p = Params(request)
        code = status.HTTP_200_OK
        if p.put:
            cfg = await p.json()
            self.db.set_bin_config(p.bin, cfg)
            code = status.HTTP_201_CREATED
        result = self.db.get_bin_config(p.bin)
        return JSONResponse(result, status_code=code)

    async def bin_index(self, request: Request):
        p = Params(request)
        code = status.HTTP_200_OK
        if p.put:
            index = await p.json()
            self.db.set_index(p.bin, index)
            code = status.HTTP_201_CREATED
        result = self.db.get_index(p.bin)
        return JSONResponse(result, status_code=code)

    async def bin_index_values(self, request: Request):
        p = Params(request)
        key = p.q("key")
        result = self.db.get_index_values(p.bin, key)
        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def bin_search(self, request: Request):
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
            result = self.db.search_items(p.bin, srch)
        except KeyError as exc:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Not a search key: {}".format(exc.args[0])
            )
        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def doc(self, request: Request):
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
                    "Item {} not found in {}".format(p.id, p.bin),
                )
            jsonpatch.apply_patch(content, patch, in_place=True)
            self.db.set_item(p.bin, p.id, content)

        # show
        result = self.db.get_item(p.bin, p.id)
        if (p.put or p.post) and result is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Storing {} in {} failed".format(p.id, p.bin),
            )
        elif result is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, "Item {} not found in {}".format(p.id, p.bin)
            )

        if p.put or p.post:
            result = {"id": p.id, "content": result}
        return JSONResponse(result, status_code=code)

    async def delete_doc(self, request: Request):
        p = Params(request)
        content = self.db.get_item(p.bin, p.id)
        if content is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, "Item {} not found in {}".format(p.id, p.bin)
            )
        self.db.del_item(p.bin, p.id)
        return Response(None, status_code=status.HTTP_204_NO_CONTENT)

    async def delete_bin(self, request: Request):
        p = Params(request)
        self.db.del_bin(p.bin)
        return Response(None, status_code=status.HTTP_204_NO_CONTENT)

    async def doc_history(self, request: Request):
        p = Params(request)
        result = self.db.get_item_history(p.bin, p.id)
        if result is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, "Item {} not found in {}".format(p.id, p.bin)
            )
        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def doc_meta(self, request: Request):
        p = Params(request)
        result = self.db.get_item_meta(p.bin, p.id)
        if result is database.NOTFOUND:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, "Item {} not found in {}".format(p.id, p.bin)
            )
        return JSONResponse(result, status_code=status.HTTP_200_OK)

    async def sync_list(self, request: Request):
        p = Params(request)
        return JSONResponse(self.db.sync_list(p.bin), status_code=status.HTTP_200_OK)

    async def cleanup(self, request: Request):
        p = Params(request)
        if "dt" in p.query_params:
            dt = datetime.date.fromisofromat(p.q("dt"))
        elif "days" in p.query_params:
            dt = datetime.date.today() - datetime.timedelta(days=int(p.q("days")))
        else:
            HTTPException(status.HTTP_400_BAD_REQUEST, "Date parameter missing")
        return JSONResponse(self.db.cleanup(p.bin, dt), status_code=status.HTTP_200_OK)
