from pathlib import Path
from typing import Literal, Optional, Union

from bson import ObjectId
from litestar import Response, get
from litestar.datastructures import State
from litestar.response import ServerSentEvent, Template

import kodo.service.controller
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
        fid = ObjectId(fid)
        result = ExecutionResult(state, fid)
        result.read()
        assert result.flow

        def file_size(file: Path) -> Union[int, None]:
            return file.stat().st_size if file.exists() else None

        return Response(content={
            "status": result.status(),
            "total": result.total_time(),
            "bootup": result.tearup(),
            "runtime": result.runtime(),
            "teardown": result.teardown(),
            "version": result.version,
            "entry_point": result.entry_point,
            "flow": result.flow.model_dump(),
            "fid": result.fid,
            "executor": result.executor,
            "ray": result.ray,
            "stdout": file_size(result.stdout_file),
            "stderr": file_size(result.stderr_file),
            "inactive": result.inactive_time(),
            "pid": result.pid,
            "ppid": result.ppid
        })

    @get("/{fid:str}/stdout")
    async def stream_stdout(self, state: State, fid: str) -> ServerSentEvent:
        result = ExecutionResult(state, fid)
        return ServerSentEvent(await result.stream_stdout())
    
    @get("/{fid:str}/stderr")
    async def stream_stderr(self, state: State, fid: str) -> ServerSentEvent:
        result = ExecutionResult(state, fid)
        return ServerSentEvent(await result.stream_stderr())

    @get("/{fid:str}/event")
    async def stream_event(self, state: State, fid: str) -> ServerSentEvent:
        result = ExecutionResult(state, fid)
        return ServerSentEvent(await result.stream_event())
