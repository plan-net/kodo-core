import shutil
from pathlib import Path
from typing import List, Literal, Optional, Union

import psutil
from bson import ObjectId
from litestar import MediaType, Request, Response, delete, get
from litestar.datastructures import State
from litestar.response import ServerSentEvent, Template

import kodo.service.controller
from kodo.log import logger
from kodo.worker.formatter import ResultFormatter
from kodo.worker.result import FINAL_STATE, ExecutionResult


class ExecutionControl(kodo.service.controller.Controller):
    path = "/flow"

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
            result = ExecutionResult(state, fid)
            await result.read()
            if result.status() in FINAL_STATE:
                alive = None
            else:
                alive = result.check_alive()
            if result.fid is None:
                logger.error(f"flow {fid} has no fid")
                continue
            if result.flow is None:
                logger.error(f"flow {fid} has no flow")
                # shutil.rmtree(result.event_file.parent)
                continue
            page.append({
                "fid": result.fid,
                "status": result.status(),
                "start_time": result.start_time(),
                "end_time": result.end_time(),
                "total": result.total_time(),
                "flow": result.flow.model_dump(),
                "inactive": result.inactive_time(),
                "alive": alive
            })
        provided_types: List[str] = [MediaType.JSON, MediaType.HTML]
        preferred_type = request.accept.best_match(
            provided_types, default=MediaType.JSON)
        ret = {
            "result": page,
            "total": total,
            "p": p,
            "pp": pp,
        }
        if preferred_type == MediaType.JSON:
            return Response(content=ret)
        return Template(template_name="jobs.html", context=ret)

    @get("/{fid:str}")
    async def detail(
            self,
            state: State,
            request: Request,
            fid: str,
            format: Optional[Literal["json", "html", "htmx"]] = None) -> Union[
                Response, Template]:
        fid = ObjectId(fid)
        result = ExecutionResult(state, fid)
        await result.read()
        await result.verify()
        assert result.flow

        def file_size(file: Path) -> Union[int, None]:
            return file.stat().st_size if file.exists() else None

        provided_types: List[str] = [MediaType.JSON, MediaType.HTML]
        preferred_type = request.accept.best_match(
            provided_types, default=MediaType.JSON)
        ret = {
            "start_time": result.start_time(),
            "end_time": result.end_time(),
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
            "ppid": result.ppid,
        }
        if format == "htmx":
            return Template(template_name="status.htmx", context=ret)
        elif preferred_type == MediaType.JSON or format == "json":
            return Response(content=ret)
        return Template(template_name="status.html", context=ret)

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

    @get("/{fid:str}/event/html")
    async def html_event(self, state: State, fid: str) -> ServerSentEvent:
        result = ExecutionResult(state, fid)
        async def process_stream():
            formatter = ResultFormatter()
            stream = await result.stream_event()
            async for event in stream:
                out = formatter.format(event)
                if out:
                    yield {"data": out, "event": "html"}
            yield {"data": "end of process", "event": "eof"}
            logger.info("return from stream")
        return ServerSentEvent(process_stream())

    @delete("/{fid:str}/kill")
    async def kill_flow(self, state: State, fid: str) -> None:
        result = ExecutionResult(state, fid)
        await result.read()
        if result.pid:
            logger.warning(f"request to kill flow {fid} with pid {result.pid}")
            try:
                proc = psutil.Process(result.pid)
                if proc.is_running():
                    for child in proc.children(recursive=True):
                        logger.warning(f"kill child {child.pid}")
                        child.terminate()
                    proc.terminate()
                    logger.warning(f"killed flow {fid} with pid {result.pid}")
            except:
                logger.error(f"failed to kill flow {fid}")
        else:
            logger.error(f"request to kill flow {fid} with no pid")
        result.kill()


    @delete("/{fid:str}/remove")
    async def remove_flow(self, state: State, fid: str) -> None:
        result = ExecutionResult(state, fid)
        await result.read()
        if result.status() not in FINAL_STATE:
            raise Exception(f"flow {fid} is still running")
        shutil.rmtree(result.event_file.parent)
