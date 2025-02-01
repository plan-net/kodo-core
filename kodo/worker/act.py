import base64
import os
import sys
from subprocess import Popen
from typing import Any, AsyncGenerator, Generator, Optional, Union
from asyncio.subprocess import create_subprocess_exec, Process

from bson import ObjectId
from litestar.datastructures import FormMultiDict, UploadFile

import kodo.datatypes
import kodo.error
from kodo import helper
from kodo.common import Launch
from kodo.datatypes import MODE, Flow, IPCinput, IPCresult, WorkerMode
from kodo.worker.base import (EVENT_STREAM, IPC_MODULE, PENDING_STATE,
                              STDERR_FILE, STDOUT_FILE)
from kodo.worker.process import FlowInterProcess

class FlowAction(FlowInterProcess):

    def build_form(self, data: FormMultiDict) -> Generator[str, None, None]:
        if data:
            for key, value in data.items():
                if isinstance(value, UploadFile):
                    value.file.seek(0)
                    content = value.file.read()
                    value.file.close()
                    value = {
                        "filename": value.filename,
                        "content_type": value.content_type,
                        "content": base64.b64encode(content).decode()
                    }
                yield "form: " + IPCinput(
                    key=key, value=value).model_dump_json()

    async def run(self, flow: Flow) -> Process:
        # takes place on the node
        # creates the detached worker process to execute the flow
        assert self.exec_path is not None, "exec_path is None"
        assert self.fid is not None, "fid is None"
        event_data = self.exec_path.joinpath(str(self.fid))
        event_data.mkdir(exist_ok=True, parents=True)
        self.event_log = event_data.joinpath(EVENT_STREAM)
        self.event_log.touch()
        await self._aev_write("data", {"flow": flow.model_dump()})
        await self._aev_write("data", dict(status=PENDING_STATE))
        environ = os.environ.copy()
        stdout_log = event_data.joinpath(STDOUT_FILE)
        stderr_log = event_data.joinpath(STDERR_FILE)
        with (open(stdout_log, 'wb') as stdout_file, 
              open(stderr_log, 'wb') as stderr_file):
            process = await create_subprocess_exec(
                sys.executable, "-m", IPC_MODULE, MODE.EXECUTE, self.factory, 
                str(self.exec_path or ""), self.fid or "", stdout=stdout_file, stderr=stderr_file, env=environ)
        await process.wait()
        return process

    def communicate(self, mode: Union[WorkerMode, str]) -> None:
        # is executed in the worker subprocess
        if self.factory is None:
            return
        flow: Any = helper.parse_factory(self.factory)
        data = self.parse_form_data()
        callback = flow.get_register("enter")
        ret = callback(data, mode)
        if isinstance(ret, Launch):
            self.create_flow(ret.inputs)
            self.write_msg(str(self.fid), "launch")
        elif isinstance(ret, str):
            for line in ret.split("\n"):
                self.write_msg(line)
        else:
            raise kodo.error.ImplementationError(
                f"{mode} must return str, got {ret.__class__.__name__}")

    async def enter(
            self,
            mode: WorkerMode,
            data: Optional[FormMultiDict] = None) -> IPCresult:
        ret = await self.parse_msg(mode, data, self.build_form)
        return ret

    def create_flow(self, inputs: Optional[dict] = None):
        # takes place on the node
        # creates the fid, log folder, and event stream, state _starting_
        inputs = inputs or {}
        fid = ObjectId()
        self.create_event_stream(str(fid))
        self._ev_write("data", 
            dict(version=kodo.__version__, entry_point=self.factory, 
                fid=str(self.fid)))
        self._ev_write("data", dict(inputs=inputs or {}))

