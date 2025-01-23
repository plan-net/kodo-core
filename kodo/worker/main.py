import sys
from pathlib import Path

import kodo.error
from kodo.datatypes import MODE
from kodo.worker.act import FlowAction
from kodo.worker.execute import FlowExecution
from kodo.worker.loader import FlowDiscovery
from kodo.worker.process import FlowProcess

if __name__ == "__main__":
    mode, factory, exec_path, fid = sys.argv[1:5]
    worker: FlowProcess
    if mode == MODE.DISCOVER:
        worker = FlowDiscovery(factory)
    elif mode == MODE.EXECUTE:
        worker = FlowExecution(factory, Path(exec_path), fid)
    elif mode in (MODE.ENTER, MODE.LAUNCH):
        worker = FlowAction(factory, Path(exec_path))
    else:
        raise kodo.error.ImplementationError(f"unknown mode: {mode}")
    worker.communicate(mode)
