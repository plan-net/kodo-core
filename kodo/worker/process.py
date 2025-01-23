import base64
import os
import sys
from subprocess import PIPE, Popen
from typing import Callable, Dict, Generator, List, Optional

from litestar.datastructures import FormMultiDict, UploadFile

from kodo.datatypes import DynamicModel, IPCinput, IPCresult, WorkerMode
from kodo.worker.base import FIX, IPC_MODULE, FlowProcess


class FlowInterProcess(FlowProcess):

    def parse_msg(self, mode: WorkerMode, data, callback) -> IPCresult:
        # executed on the node
        ret: Dict[str, List] = {
            "content": [],
            "returncode": [],
            "stderr": [],
            "launch": [],
            "logging": []
        }
        for line in self.spawn(mode, data, callback):
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
        if ret["launch"]:
            self.fid = ret["launch"].pop()
        else:
            self.fid = ""
        return IPCresult(
            content="\n".join(ret["content"]),
            returncode=int(ret["returncode"].pop(-1)),
            stderr="\n".join(ret["stderr"]),
            fid=self.fid,
            logging=ret["logging"])

    def spawn(
            self,
            mode: WorkerMode,
            data: Optional[FormMultiDict],
            inputs: Callable) -> Generator[str, None, None]:
        # executed on the node
        # generic method to sub-process the worker with `inputs` generator
        # output is streamed to the caller
        environ = os.environ.copy()
        process = Popen(
            [sys.executable, "-m", IPC_MODULE, mode, self.factory,
             str(self.exec_path or ""), self.fid or ""], stdin=PIPE,
            stdout=PIPE, stderr=PIPE, text=True, env=environ)
        if process.stdin:
            if inputs is not None:
                for line in inputs(data):
                    process.stdin.write(line + "\n")
            process.stdin.close()
        if process.stdout:
            for buffer in process.stdout:
                if buffer.rstrip():
                    yield buffer
            process.stdout.close()
        process.wait()
        if process.stderr:
            stderr = process.stderr.read()
            yield self.format_msg(stderr, "stderr")
        yield self.format_msg(str(process.returncode), "returncode")

    def format_msg(self, line: str, key: str) -> str:
        # subprocess helper tool
        line = line.rstrip()
        return f"{FIX}{key}:{line}{FIX}{len(line)}\n"

    def write_msg(self, line, key=None) -> None:
        # subprocess helper tool
        com_line = self.format_msg(line, key or "content")
        sys.stdout.write(com_line)
        sys.stdout.flush()

    def log_msg(self, level: int, message: str) -> None:
        # subprocess helper tool
        payload = {"level": level, "message": message}
        self.write_msg(DynamicModel(payload).model_dump_json(), "logging")

    def parse_form_data(self) -> FormMultiDict:
        # executed in the worker node process
        # parse the form data from the worker subprocess
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
