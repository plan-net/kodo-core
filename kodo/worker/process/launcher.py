import base64
import sys
from typing import (Any, AsyncGenerator, Callable, Dict, Generator, List,
                    Optional)

from bson import ObjectId
from litestar.datastructures import FormMultiDict, UploadFile

from kodo import helper
from kodo.common import Launch
from kodo.datatypes import MODE, IPCinput, IPCresult
from kodo.worker.process.executor import FIX
from kodo.worker.process.caller import SubProcess
from kodo.worker.process.executor import FlowExecutor

class FlowLauncher:

    def __init__(
            self,
            factory: str):
        self.factory: str = factory

    async def enter(
            self, data: Optional[dict] = None) -> IPCresult:
        return await self.parse_msg(data, self.build_form)

    def build_form(self, data: dict) -> Generator[str, None, None]:
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

    async def parse_msg(
            self, 
            data: Optional[dict]=None, 
            callback:Optional[Callable]=None) -> IPCresult:
        ret: Dict[str, List] = {
            "content": [],
            "returncode": [],
            "stderr": [],
            "launch": [],
            "logging": []
        }
        async for line in self.spawn(data or {}, callback):
            line = line.rstrip()
            if line.startswith(FIX):
                parts = line.split(FIX)
                if parts[0] == '':
                    action, *payload = FIX.join(parts[1:-1]).split(":", 1)
                    if action and payload:
                        if action in ret:
                            if len(payload[0]) == int(parts[-1]):
                                ret[action].append(payload[0])
                                continue
                        elif action == "test":
                            continue
            ret["stderr"].append(f"?> {line}")
        fid = ""
        if ret["launch"]:
            fid = ret["launch"].pop()
        return IPCresult(
            content="\n".join(ret["content"]),
            returncode=int(ret["returncode"].pop(-1)),
            stderr="\n".join(ret["stderr"]),
            fid=fid,
            logging=ret["logging"])

    async def spawn(
            self,
            data: dict,
            inputs: Optional[Callable]=None) -> AsyncGenerator[str, None]:
        process = SubProcess(MODE.LAUNCH, self.factory, "")
        await process.open()
        if process.stdin:
            if inputs is not None:
                for line in inputs(data):
                    line += "\n"
                    await process.write_stdin(line.encode("utf-8"))
            await process.close_stdin()
        if process.stdout:
            async for line in self._read(process.read_stdout):
                yield line
        if process.stderr:
            async for line in self._read(process.read_stderr):
                yield self.format_msg(line, "stderr")
        await process.wait()
        yield self.format_msg(str(process.returncode), "returncode")

    async def _read(
            self, 
            read_func: Callable[[], Any]) -> AsyncGenerator[str, None]:
        buffer = ""
        stop = False
        while not stop:
            line = await read_func()
            if line:
                buffer += line.decode("utf-8")
            else:
                stop = True
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                yield line
        if buffer:
            yield buffer

    def format_msg(self, line: str, key: str) -> str:
        line = line.rstrip()
        return f"{FIX}{key}:{line}{FIX}{len(line)}\n"

    def communicate(self) -> None:
        if self.factory is None:
            return
        print(f"this is python: {sys.executable}")
        flow: Any = helper.parse_factory(self.factory)
        data = self.parse_form_data()
        callback = flow.get_register("enter")
        ret = callback(data)
        if isinstance(ret, Launch):
            fid = str(ObjectId())
            executor = FlowExecutor(self.factory, fid)
            executor.prepare(ret.inputs)
            self.write_msg(fid, "launch")
        else:
            for line in str(ret).split("\n"):
                self.write_msg(line, "content")

    def parse_form_data(self) -> FormMultiDict:
        form = {}
        for line in sys.stdin:
            if line.startswith("form: "):
                form_data = IPCinput.model_validate_json(line[6:])
                if form_data.value and isinstance(form_data.value, dict):
                    form[form_data.key] = UploadFile(
                        filename=form_data.value["filename"],
                        file_data=base64.b64decode(
                            form_data.value["content"]),
                        content_type=form_data.value["content_type"])
                else:
                    form[form_data.key] = form_data.value
        return FormMultiDict(form)

    def write_msg(self, line: str, key: str) -> None:
        com_line = self.format_msg(line, key)
        sys.stdout.write(com_line)
        sys.stdout.flush()

