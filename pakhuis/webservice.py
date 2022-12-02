"""Webservice
"""

__author__ = "Rogier Steehouder"
__date__ = "2022-11-20"
__version__ = "1.1"

import datetime
import uuid

import jsonpatch
from loguru import logger
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from . import config, database


class Webservice:
    def __init__(self, cfg: config.Config):
        self._logger = logger.bind(logtype="pakhuis.webservice")
        self.cfg = cfg
        self.db = database.Database(cfg.database.path)

    async def ping(self, request: Request):
        result = self.db.version()
        return JSONResponse({"pakhuis": __version__, "db": result, "user": request.user.display_name}, status_code=200)

    async def root(self, request: Request):
        result = self.db.get_bins()
        return JSONResponse(result, status_code=200)

    async def bin(self, request: Request):
        _bin = request.path_params["_bin"]
        if request.query_params.get("full"):
            result = self.db.get_items(_bin)
            if request.query_params.get("index"):
                result["_index"] = self.db.get_index(_bin)
        else:
            result = self.db.get_bin(_bin)
        return JSONResponse(result, status_code=200)

    async def bin_config(self, request: Request):
        _bin = request.path_params["_bin"]
        status = 200
        if request.method == "PUT":
            cfg = await request.json()
            self.db.set_bin_config(_bin, cfg)
            status = 201
        result = self.db.get_bin_config(_bin)
        return JSONResponse(result, status_code=status)

    async def bin_index(self, request: Request):
        _bin = request.path_params["_bin"]
        status = 200
        if request.method == "PUT":
            index = await request.json()
            self.db.set_index(_bin, index)
            status = 201
        result = self.db.get_index(_bin)
        return JSONResponse(result, status_code=status)

    async def bin_index_values(self, request: Request):
        _bin = request.path_params["_bin"]
        key = request.query_params.get("key", "")
        status = 200
        result = self.db.get_index_values(_bin, key)
        return JSONResponse(result, status_code=status)

    async def bin_search(self, request: Request):
        _bin = request.path_params["_bin"]
        if request.method == "POST":
            srch = await request.json()
        elif request.method == "GET":
            srch = {}
            for k, v in request.query_params.items():
                srch[k] = v
        if not srch:
            raise HTTPException(404, "Empty search")
        result = self.db.search_items(_bin, srch)
        return JSONResponse(result, status_code=200)

    async def doc(self, request: Request):
        _bin = request.path_params["_bin"]
        _id = request.path_params.get("_id", "")
        dttm = request.query_params.get("dttm", None)
        if dttm:
            dttm = datetime.datetime.fromisoformat(dttm)
        code = 200

        # add or overwrite
        add_mode = (request.method in ("PUT", "POST"))
        if add_mode:
            if not _id:
                _id = str(uuid.uuid4())
            content = await request.json()
            self.db.set_item(_bin, _id, content, dttm)
            code = 201

        # patch existing
        elif request.method == "PATCH":
            patch = await request.json()
            content = self.db.get_item(_bin, _id)
            if content is database.NOTFOUND:
                raise HTTPException(404, "Item {} not found in {}".format(_id, _bin))
            jsonpatch.apply_patch(content, patch, in_place=True)
            self.db.set_item(_bin, _id, content)

        # show
        result = self.db.get_item(_bin, _id)
        if add_mode and result is database.NOTFOUND:
            raise HTTPException(500, "Storing {} in {} failed".format(_id, _bin))
        elif result is database.NOTFOUND:
            raise HTTPException(404, "Item {} not found in {}".format(_id, _bin))

        if add_mode:
            result = { "id": _id, "content": result }
        return JSONResponse(result, status_code=code)

    async def delete_doc(self, request: Request):
        _bin = request.path_params["_bin"]
        _id = request.path_params["_id"]
        content = self.db.get_item(_bin, _id)
        if content is database.NOTFOUND:
            raise HTTPException(404, "Item {} not found in {}".format(_id, _bin))
        self.db.del_item(_bin, _id)
        return Response(None, status_code=204)

    async def delete_bin(self, request: Request):
        _bin = request.path_params["_bin"]
        self.db.del_bin(_bin)
        return Response(None, status_code=204)

    async def doc_history(self, request: Request):
        _bin = request.path_params["_bin"]
        _id = request.path_params["_id"]
        result = self.db.get_item_history(_bin, _id)
        if result is database.NOTFOUND:
            raise HTTPException(404, "Item {} not found in {}".format(_id, _bin))
        return JSONResponse(result, status_code=200)

    async def doc_meta(self, request: Request):
        _bin = request.path_params["_bin"]
        _id = request.path_params["_id"]
        result = self.db.get_item_meta(_bin, _id)
        if result is database.NOTFOUND:
            raise HTTPException(404, "Item {} not found in {}".format(_id, _bin))
        return JSONResponse(result, status_code=200)

    async def sync_list(self, request: Request):
        _bin = request.path_params.get("_bin", "")
        return JSONResponse(self.db.sync_list(_bin), status_code=200)
