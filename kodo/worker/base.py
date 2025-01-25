from pathlib import Path
from typing import Dict, Optional, Union

from kodo import helper
from kodo.datatypes import DynamicModel, WorkerMode

EVENT_STREAM = "event.log"
STDOUT_FILE = "stdout.log"
STDERR_FILE = "stderr.log"
STOP_FILE = "_done_"
KILL_FILE = "_killed_"
IPC_MODULE = "kodo.worker.main"
FIX = "@_ks_@"


class FlowProcess:

    def __init__(
            self,
            factory: str,
            exec_path: Optional[Path] = None,
            fid: Optional[str] = None):
        self.factory: str = factory
        self.exec_path: Optional[Path] = Path(exec_path) if exec_path else None
        self.fid: Optional[str] = fid or None
        self.event_log: Optional[Path] = None
        self.create_event_stream(self.fid)

    def create_event_stream(self, fid: Optional[str] = None):
        if fid and isinstance(self.exec_path, Path):
            self.fid = fid
            flow_data = self.exec_path.joinpath(str(self.fid))
            self.event_log = flow_data.joinpath(EVENT_STREAM)
            flow_data.mkdir(exist_ok=True, parents=True)
            self.event_log = flow_data.joinpath(EVENT_STREAM)

    def _ev_write(self, kind: str, data: Dict):
        # executed in the subprocess
        # access is at this stage exclusive
        # value is dictionary
        with open(self.event_log, "a") as f:  # type: ignore
            dump = DynamicModel(data).model_dump_json()
            now = helper.now().isoformat()
            f.write(f"{now} {kind} {dump}\n")

    def communicate(self, mode: Union[WorkerMode, str]) -> None:
        raise NotImplementedError()

    def create_stop_file(self) -> None:
        if self.fid and isinstance(self.exec_path, Path):
            stop_file = self.exec_path.joinpath(str(self.fid), STOP_FILE)
            stop_file.touch()

