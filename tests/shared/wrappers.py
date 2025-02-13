import multiprocessing
import os
import time
import traceback
from pathlib import Path

import httpx
import pytest
import ray

from kodo.remote.launcher import RAY_ENV, RAY_NAMESPACE
from kodo.service.node import run_service


class Process(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            raise

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


class Service:

    def __init__(self, url, **kwargs):
        self.url = url
        kwargs["url"] = self.url
        kwargs["reload"] = False
        cache_file = self.url.split("://")[1].replace(":", "_")
        kwargs["cache_data"] = f"./data/{cache_file}"
        kwargs["cache_reset"] = kwargs.get("cache_reset", True)
        kwargs["screen_level"] = "DEBUG"
        self.process = Process(target=run_service, kwargs=kwargs)
        self.cache_file = Path(kwargs["cache_data"])

    def start(self):
        self.process.start()
        while True:
            try:
                resp = httpx.get(f"{self.url}/home", timeout=600)
                if resp.status_code == 200:
                    break
            except:
                pass
            time.sleep(0.5)
            # asyncio.sleep(0.5)

    def wait(self):
        while True:
            try:
                resp = httpx.get(f"{self.url}/home", timeout=600)
                if resp.status_code == 200:
                    if resp.json()["idle"]:
                        break
            except:
                pass
            time.sleep(0.5)
            # asyncio.sleep(0.5)

    def stop(self, cache=False):
        self.process.terminate()
        self.process.join()
        if not cache:
            self.cache_file.unlink(missing_ok=True)


@pytest.fixture(autouse=True)
def cleanup():
    yield
    for proc in multiprocessing.active_children():
        proc.terminate()
        proc.join()


@pytest.fixture(scope="session", autouse=True)
def use_ray():
    os.system("ray start --head")
    ray.init(
        address="localhost:6379", 
        ignore_reinit_error=True,
        namespace=RAY_NAMESPACE,
        configure_logging=True,
        logging_level="DEBUG",
        log_to_driver=True,
        runtime_env=RAY_ENV
    )
    yield
    ray.shutdown()
    os.system("ray stop")
