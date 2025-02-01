import os
import sys
from pathlib import Path

import kodo.error
from kodo.datatypes import MODE
from kodo.worker.process.act import FlowAction
from kodo.worker.process.execute import FlowExecution
from kodo.worker.process.base import FlowProcess


if __name__ == "__main__":
    mode, factory, exec_path, fid = sys.argv[1:5]
    worker: FlowProcess
    if mode == MODE.EXECUTE:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
        os.setsid()
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
        worker = FlowExecution(factory, Path(exec_path), fid)
    elif mode in (MODE.ENTER, MODE.LAUNCH):
        worker = FlowAction(factory, Path(exec_path))
    else:
        raise kodo.error.ImplementationError(f"unknown mode: {mode}")
    worker.communicate(mode)
