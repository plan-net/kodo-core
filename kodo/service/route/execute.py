from typing import Literal, Optional, Union

from litestar import Request, Response, get
from litestar.datastructures import State
from litestar.response import Template
import os
from bson import ObjectId
from pathlib import Path

import kodo.service.controller
from kodo.worker.base import EVENT_STREAM, STDERR_FILE, STDOUT_FILE
from kodo.worker.result import ExecutionResult

class ExecutionControl(kodo.service.controller.Controller):
    path = "/flow"

    # @get("/")
    # async def listing(
    #         self,
    #         state: State,
    #         request: Request,
    #         q: Optional[str] = None,
    #         by: Optional[str] = None,
    #         pp: int = 10,
    #         p: int = 0,
    #         format: Optional[Literal["json", "html"]] = None) -> Union[
    #             Response, Template]:
    #     """
    #     Return all flow executions Returns a pandas DataFrame.
    #     """
    #     exec_path = Path(state.exec_data)
    #     execs = {}
    #     for folder_path in exec_path.iterdir():
    #         if folder_path.is_dir():
    #             try:
    #                 obj_id = ObjectId(folder_path.name)
    #                 execs[str(obj_id)] = obj_id.generation_time
    #                 ev = ExecutionResult(folder_path.joinpath(EVENT_STREAM))
    #                 stdout = folder_path.joinpath(STDOUT_FILE)
    #                 stderr = folder_path.joinpath(STDERR_FILE)
    #                 execs[str(obj_id)] = {
    #                     "generation_time": obj_id.generation_time,
    #                     "state": ev,
    #                     "stdout": stdout.stat().st_size if stdout.exists() else 0,
    #                     "stderr": stderr.stat().st_size if stderr.exists() else 0
    #                 }
    #             except Exception:
    #                 continue
    #     return Response(content=execs)

    @get("/{fid:str}")
    async def detail(
            self,
            state: State,
            fid: str,
            format: Optional[Literal["json", "html"]] = None) -> Union[
                Response, Template]:
        oid = ObjectId(fid)
        folder = Path(state.exec_data).joinpath(fid)
        ev_file = folder.joinpath(EVENT_STREAM)
        stdout_file = folder.joinpath(STDOUT_FILE)
        stderr_file = folder.joinpath(STDERR_FILE)
        ev_data = ExecutionResult(ev_file.open("r"))
        ev_data.read()
        assert ev_data.flow
        def file_size(file: Path) -> Union[int, None]:
            return file.stat().st_size if file.exists() else None

        return Response(content={
            "status": ev_data.status(),
            "runtime": ev_data.runtime().total_seconds(),
            "version": ev_data.version,
            "entry_point": ev_data.entry_point,
            "flow": ev_data.flow.model_dump(),
            "fid": ev_data.fid,
            "executor": ev_data.executor,
            "ray": ev_data.ray,
            "stdout": file_size(stdout_file),
            "stderr": file_size(stderr_file)
        })