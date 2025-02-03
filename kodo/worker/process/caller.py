import os
from asyncio.subprocess import create_subprocess_exec
from subprocess import PIPE, Popen
import sys
from pathlib import Path
import platform

from kodo.log import logger
from kodo.datatypes import InternalOption

IPC_MODULE = 'kodo.worker.main'
WINDOWS_EXECUTABLE = ('Scripts', 'python.exe')
LINUX_EXECUTABLE = ('bin', 'python')

class SubProcess:

    def __init__(
            self,
            mode,
            factory,
            fid=None,
            stdin=None,
            stdout=None,
            stderr=None,
            remote=False):
        self.remote = remote
        self.mode = mode
        self.factory = factory
        self.fid = fid or ""
        self._stdin = stdin
        self._stdout = stdout
        self._stderr = stderr
        option = InternalOption()
        self.sync = option.SYNC
        self.env_home = Path(option.ENV_HOME)
        self.venv = option.VENV_DIR
        self.process = None

    @property
    def stdin(self):
        return self.process.stdin
    
    @property
    def stdout(self):
        return self.process.stdout
    
    @property
    def stderr(self):
        return self.process.stderr

    @property
    def pid(self):
        return self.process.pid

    @property
    def ppid(self):
        return self.process.ppid

    async def open(self):
        kw = {
            "stdin": self._stdin or PIPE,
            "stdout": self._stdout or PIPE,
            "stderr": self._stderr or PIPE
        }
        if not self.remote:
            kw["env"] = os.environ.copy()
        parts = self.factory.split(":")
        if len(parts) == 3:
            venv, *f = parts
            factory = ":".join(f)
        elif len(parts) == 2:
            venv, factory = "", ":".join(parts)
        else:
            venv = ""
            factory = self.factory
        if venv:
            kw["cwd"] = str(self.env_home.joinpath(venv))
            home = self.env_home.joinpath(venv, self.venv).joinpath
            if platform.system().lower() == "windows":
                executable = str(home(*WINDOWS_EXECUTABLE))
            else:
                executable = str(home(*LINUX_EXECUTABLE))
        else:
            executable = str(sys.executable)
        executable = str(sys.executable)
        # logger.info(f"run with {executable}: {kw}")
        if self.sync:
            self.process = Popen(
                [executable, "-m", IPC_MODULE, self.mode, factory, self.fid], 
                **kw)
        else:
            self.process = await create_subprocess_exec(
                executable, "-m", IPC_MODULE, self.mode, factory, self.fid,
                **kw)

    async def wait(self) -> int:
        if self.sync:
            return self.process.wait()
        return await self.process.wait()
    
    @property
    def returncode(self) -> int:
        return self.process.returncode
    
    async def write_stdin(self, data: bytes) -> None:
        self.stdin.write(data)

    async def close_stdin(self) -> None:
        if self.sync:
            self.stdin.flush()
        else:
            await self.stdin.drain()
        self.stdin.close()

    async def read_stdout(self) -> bytes:
        if self.sync:
            return self.stdout.read(1024)
        return await self.stdout.read(1024)
    
    async def read_stderr(self) -> bytes:
        if self.sync:
            return self.stderr.read(1024)
        return await self.stderr.read(1024)