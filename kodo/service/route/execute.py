from pathlib import Path
from typing import List, Literal, Optional, Union
import shutil
from bson import ObjectId
from litestar import MediaType, Request, Response, get, post
from litestar.datastructures import State
from litestar.exceptions import NotFoundException
from litestar.response import Template, ServerSentEvent

import kodo.service.controller
from kodo.log import logger
from kodo.remote.launcher import FINAL_STATE, KILLED_STATE
from kodo.remote.result import ExecutionResult
from kodo import helper

class ExecutionControl(kodo.service.controller.Controller):

    @get("/")
    async def listing(
            self,
            state: State,
            request: Request,
            pp: int = 10,
            p: int = 0,
            format: Optional[Literal["json", "html"]] = None) -> Union[
                Response, Template]:
        exec_path = Path(state.exec_data)
        execs = []
        for folder_path in exec_path.iterdir():
            if folder_path.is_dir():
                try:
                    execs.append((ObjectId(folder_path.name).generation_time,
                                  folder_path.name))
                except Exception:
                    continue
        execs.sort(reverse=True)
        total = len(execs)
        page: List = []
        skip = p * pp
        while len(page) < pp and execs:
            if skip > 0:
                skip -= 1
                execs.pop(0)
                continue
            _, fid = execs.pop(0)
            t0 = helper.now()
            result = ExecutionResult(exec_path.joinpath(fid))
            await result.aread()
            alive: Union[bool, None] = None
            if result.status in FINAL_STATE:
                alive = False
            else:
                alive = await result.check_alive()
            if result.launch.fid is None:
                logger.error(f"flow {fid} has no fid")
                continue
            if result.flow is None:
                logger.error(f"flow {fid} has no flow")
                continue
            page.append({
                "fid": result.launch.fid,
                "status": result.status,
                "launch": result.launch,
                "version": result.version,
                "has_final": result.has_final,
                "timing": result.timing,
                "duration": result.duration,
                "progress": result.progress,
                "flow": result.flow,
                "alive": alive
            })
            size = result.event_log.stat().st_size
            logger.debug(f"flow {fid} ({size}) loaded in {helper.now() - t0}")
        ret = {
            "items": page,
            "total": total,
            "p": p,
            "pp": pp,
        }
        if helper.wants_html(request):
            return Template(template_name="jobs.html", context=ret)
        return Response(content=ret)

    @get("/{fid:str}")
    async def detail(
            self,
            state: State,
            request: Request,
            fid: str) -> Union[Response]:
        result = ExecutionResult(Path(state.exec_data).joinpath(fid))
        try:
            await result.aread()
        except FileNotFoundError:
            raise NotFoundException(f"flow {fid} not found")
        if helper.wants_html(request):
            return Template(template_name="status.html", 
                            context=result.data)
        return Response(content=result.data)

    @get("/{fid:str}/final")
    async def final_result(
            self,
            state: State,
            request: Request,
            fid: str) -> Response:
        result = ExecutionResult(Path(state.exec_data).joinpath(fid))
        try:
            await result.aread()
            final = await result.final_result()
        except FileNotFoundError:
            raise NotFoundException(f"flow {fid} not found")
        if helper.wants_html(request):
            pass
        return Response(content=final)

    @get("/{fid:str}/result")
    async def result(
            self,
            state: State,
            request: Request,
            fid: str) -> Response:
        result = ExecutionResult(Path(state.exec_data).joinpath(fid))
        try:
            await result.aread()
            ret = await result.result()
        except FileNotFoundError:
            raise NotFoundException(f"flow {fid} not found")
        provided_types: List[str] = [MediaType.JSON, MediaType.HTML]
        preferred_type = request.accept.best_match(
            provided_types, default=MediaType.JSON)
        return Response(content=ret)

    @get("/{fid:str}/progress")
    async def progress(
            self,
            state: State,
            request: Request,
            fid: str) -> Response:
        result = ExecutionResult(Path(state.exec_data).joinpath(fid))
        try:
            ret = await result.get_progress()
        except FileNotFoundError:
            raise NotFoundException(f"flow {fid} not found")
        provided_types: List[str] = [MediaType.JSON, MediaType.HTML]
        preferred_type = request.accept.best_match(
            provided_types, default=MediaType.JSON)
        return Response(content=ret)

    @post("/{fid:str}/kill")
    async def kill(
            self,
            state: State,
            request: Request,
            fid: str) -> bool:
        result = ExecutionResult(Path(state.exec_data).joinpath(fid))
        logger.warning(f"POST /flows/{fid}/kill")
        try:
            await result.kill()
        except FileNotFoundError:
            raise NotFoundException(f"flow {fid} not found")
        return True

    @post("/{fid:str}/remove")
    async def remove(
            self,
            state: State,
            request: Request,
            fid: str) -> bool:
        result = ExecutionResult(Path(state.exec_data).joinpath(fid))
        logger.warning(f"POST /flows/{fid}/remove")
        try:
            await result.aread()
            if result.status not in FINAL_STATE:
                raise Exception(f"flow {fid} is still running")
            shutil.rmtree(str(result.event_log.parent))
        except FileNotFoundError:
            raise NotFoundException(f"flow {fid} not found")
        return True

    @get("/{fid:str}/event")
    async def stream_event(self, state: State, fid: str) -> ServerSentEvent:
        logger.info(f"STREAM /flows/{fid}/event")
        result = ExecutionResult(Path(state.exec_data).joinpath(fid))
        return ServerSentEvent(result.stream())

    @get("/{fid:str}/progress/stream")
    async def stream_progress(self, state: State, fid: str) -> ServerSentEvent:
        logger.info(f"STREAM /flows/{fid}/progress/stream")
        result = ExecutionResult(Path(state.exec_data).joinpath(fid))
        return ServerSentEvent(result.stream("progress", "status"))
