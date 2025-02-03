import os
import sys
from typing import Any

import kodo.error
from kodo.datatypes import MODE
from kodo.worker.process.executor import FlowExecutor
from kodo.worker.process.launcher import FlowLauncher


if __name__ == "__main__":
    mode, factory, fid = sys.argv[1:4]
    worker: Any
    if mode == MODE.EXECUTE:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
        os.setsid()
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
        worker = FlowExecutor(factory, fid)
    elif mode == MODE.LAUNCH:
        worker = FlowLauncher(factory)
    else:
        raise kodo.error.ImplementationError(f"unknown mode: {mode}")
    worker.communicate()
