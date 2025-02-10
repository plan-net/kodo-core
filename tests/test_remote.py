import asyncio
import datetime
import os
from pathlib import Path

import httpx
import pytest
import ray
from litestar.datastructures import State

import kodo.remote.result
from kodo import helper
from kodo.datatypes import Flow, Option
from kodo.remote.launcher import RAY_ENV, RAY_NAMESPACE, launch, parse_factory
from kodo.remote.result import ExecutionResult
from tests.test_worker import Service


@pytest.fixture()
def use_ray():
    os.system("ray start --head")
    ray.init(
        address="localhost:6379", 
        ignore_reinit_error=False,
        namespace=RAY_NAMESPACE,
        configure_logging=True,
        logging_level="DEBUG",
        log_to_driver=True,
        runtime_env=RAY_ENV
    )
    yield
    ray.shutdown()
    os.system("ray stop")


def test_parse_factory():
    option = Option()
    env, cwd, module, flow = parse_factory(
        State(dict(env_home=option.ENV_HOME, venv_dir=option.VENV_DIR)), 
        "ec2:env2.example:flow")
    assert env == str(Path('./data/environ/ec2/.venv/bin/python').absolute())
    assert cwd == str(Path('./data/environ/ec2').absolute())
    assert module == "env2.example"
    assert flow == "flow"


async def test_launch(use_ray):
    option = Option()
    state = State(dict(
        ray_server=option.RAY_SERVER,
        env_home=option.ENV_HOME, 
        venv_dir=option.VENV_DIR,
        exec_data=option.EXEC_DATA))
    flow = Flow(url="/test", name="Flow Name", entry="ec2:env2.example:flow")
    resp = await launch(state, flow, {})
    assert resp.fid is not None
    assert resp.is_launch is False
    assert resp.success is True
    assert "<h2>Func2</h2><input" in resp.payload
    resp = await launch(state, flow, {"submit": "GO"})
    assert resp.fid is not None
    assert resp.is_launch is True
    assert resp.payload is not None
    assert "/data/environ/ec2/.venv" in resp.payload["inputs"]["python"]
    assert resp.payload["inputs"]["foo"] == "bar"
    assert resp.payload["args"] == (1, 2, 3)
    assert resp.success is True


async def test_launch2(use_ray):
    option = Option()
    state = State(dict(
        ray_server=option.RAY_SERVER,
        env_home=option.ENV_HOME, 
        venv_dir=option.VENV_DIR))
    flow = Flow(url="/test", name="Flow Name", entry="tests.test_worker:flow3")
    resp = await launch(state, flow, {})
    assert resp.fid is not None
    assert resp.is_launch is False
    assert resp.success is True
    assert "This flow runs for a specified time" in resp.payload

async def has_final(fid, timeout=30):
    t0 = helper.now()
    while True:
        resp = httpx.get(
            f"http://localhost:3371/flow/{fid}", timeout=None, 
            headers={"Accept": "application/json"})
        assert resp.status_code == 200
        if resp.json()["has_final"]:
            print("O", end="")
            break
        print("w", end="")
        await asyncio.sleep(1)
        if helper.now() - t0 > datetime.timedelta(seconds=timeout):
            print("X")
            raise RuntimeError()


async def test_launch_request(use_ray):
    node = Service(
        url="http://localhost:3371", 
        loader="tests/example3/node4.yaml")
    node.start()
    resp = httpx.post("http://localhost:3371/flows/flow1", timeout=None,
                      data={"submit": "submit", "runtime": 10},
                      headers={"Accept": "application/json"})
    fid = resp.headers["location"].split("/")[-1]
    final = await has_final(fid)
    node.stop()


async def test_launch_request2(use_ray):
    node = Service(
        url="http://localhost:3371", 
        loader="tests/example3/node4.yaml")
    node.start()
    resp = httpx.post("http://localhost:3371/flows/flow1", timeout=None,
                      data={"submit": "submit", "runtime": 10},
                      headers={"Accept": "application/json"})
    fid = resp.headers["location"].split("/")[-1]
    assert fid is not None
    result = ExecutionResult(f"./data/exec/{fid}")
    while True:
        result.read()
        if result.status in ("completed", "error"):
            break
        await asyncio.sleep(1)
    result.read()
    assert result.status == "completed"
    node.stop()

# async def test_load_result():
#     result = kodo.remote.result.ExecutionResult(
#         Path(__file__).parent.joinpath("./assets"))
#     result.read()
#     # assert result.tearup_time() == datetime.timedelta(seconds=1)
#     assert result.tearup_time() == datetime.timedelta(seconds=2)
#     assert result.running_time() == datetime.timedelta(seconds=3)
#     assert result.teardown_time() == datetime.timedelta(seconds=4)
#     assert result.total_time() == datetime.timedelta(seconds=9)
#     assert result.inactive_time() is None
#     print(result.flow)
#     print(result.launch)
#     print("OK")


async def test_launch_request20(use_ray):
    node = Service(
        url="http://localhost:3371", 
        loader="tests/example3/node4.yaml")
    node.start()
    err = 0
    for i in range(20):
        resp = httpx.post("http://localhost:3371/flows/flow1", timeout=None,
                        data={"submit": "submit", "runtime": 3},
                        headers={"Accept": "application/json"})
        url = resp.headers["location"]
        print(f"{i}, err: {err}", end=": ")
        t0 = helper.now()
        while True:
            resp = httpx.get(
                f"http://localhost:3371{url}", timeout=None, 
                headers={"Accept": "application/json"})
            assert resp.status_code == 200
            if resp.json()["has_final"]:
                print("O", end="")
                break
            print("w", end="")
            await asyncio.sleep(1)
            if helper.now() - t0 > datetime.timedelta(seconds=30):
                print("X"*10)
                err += 1
                break
    node.stop()

