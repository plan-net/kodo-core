import asyncio
import datetime
import os
import sys
import time
from pathlib import Path

import httpx
import psutil
import pytest
import ray
from litestar.datastructures import State

from kodo import helper
from kodo.common import Launch, publish
from kodo.datatypes import Flow, Option
from kodo.remote.enter import FlowCallable
from kodo.remote.launcher import launch, parse_factory
from kodo.remote.result import ExecutionResult
from tests.test_node import Service
from tests.shared import *


def test_parse_factory():
    option = Option()
    env, cwd, module, flow = parse_factory(
        State(dict(env_home=option.ENV_HOME, venv_dir=option.VENV_DIR)), 
        "ec2:env2.example:flow")
    assert env == str(Path('./data/environ/ec2/.venv/bin/python').absolute())
    assert cwd == str(Path('./data/environ/ec2').absolute())
    assert module == "env2.example"
    assert flow == "flow"


async def test_launch():
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


def execute3(inputs, flow: FlowCallable):
    t0 = datetime.datetime.now()
    i = 0
    runtime = inputs.get("runtime", None)
    while True:
        i += 1
        if i % 1000 == 0:
            flow.result(dict(test=f"intermediate result {i}"))
        print(f"debug: {i}")
        time.sleep(0.005)
        if runtime:
            delta = (datetime.datetime.now() - t0).total_seconds()
            if delta > int(runtime):
                flow.set_progress(1, 1)
                break
            flow.set_progress(delta, runtime)
        else:
            break
    flow.final({"message": f"Hallo Welt: final i == {i}"})
    return "OK"

flow3 = publish(
    execute3, url="/execute", name="Test Flow", description="missing")


@flow3.enter
def flow3_page(form):
    if form.get("submit"):
        runtime = form.get("runtime", None)
        if runtime is None: runtime = 10
        return Launch(runtime=int(runtime))
    return f"""
        <p>This flow runs for a specified time (in seconds).</p> 
        <p>
            Booting up and launching the flow adds a couple of seconds, though.
        </p> 
        <input type="text" name="runtime" placeholder="10">
        <input type="submit" name="submit">
    """


async def test_launch2():
    option = Option()
    state = State(dict(
        ray_server=option.RAY_SERVER,
        env_home=option.ENV_HOME, 
        venv_dir=option.VENV_DIR))
    flow = Flow(url="/test", name="Flow Name", entry="tests.test_remote:flow3")
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


def execute_inactive(inputs, event: FlowCallable):
    event.result({"test": "I am active"})
    time.sleep(5)
    print("I am active, again")
    time.sleep(3)
    event.final({"message": "Hallo Welt"})

flow5 = publish(
    execute_inactive, url="/execute", name="Test Flow", description="missing")


@flow5.enter
def flow5_page(form):
    if form.get("submit"):
        return Launch()
    return f"""
        <p>This flow runs 5, then 3 seconds.</p> 
        <p>
            Watch the <i>inactive</i> timer.
        </p> 
        <input type="submit" name="submit">
    """

def stream_execute(inputs, flow: FlowCallable):
    flow.result(dict(test=f"TEST RESULT"))
    print("This is printed")
    print("This is printed to sys.stderr", file=sys.stderr)
    sys.stdout.write("This is written to sys.stdout\n")
    sys.stderr.write("This is written to sys.stderr\n")
    for i in range(3000):
        print(f"printing intermediate result {i}")
        flow.result(dict(result=f"intermediate result {i}"))
    return "OK"


stream_flow = publish(
    stream_execute, url="/execute_remote", name="Ray Test",
    description="cannot be empty")

@stream_flow.enter
def stream_landing_page(form):
    topic = form.get("topic")
    if topic:
        return Launch(topic=topic)
    error = ""
    if form.get("submit") and not form.get("topic"):
        error = "<p><mark><b>Please tell me!</b></mark></p>" 
    return f"""
        <p>This is a simple flow which executes a function. The function
        prints a few lines to <code>STDOUT</code> and <code>STDERR</code>.</p>
        <p>But before we continue, tell me something about you!</p>
        { error }
        <input type="text" name="topic" placeholder="how are you?">
        <input type="submit" name="submit">
    """


async def test_launch_request():
    node = Service(
        url="http://localhost:3371", 
        loader="tests/example3/node4.yaml")
    node.start()
    resp = httpx.post("http://localhost:3371/flows/flow1", timeout=None,
                      data={"submit": "submit", "runtime": 10},
                      headers={"Accept": "application/json"})
    fid = resp.headers["location"].split("/")[-1]
    await has_final(fid)
    node.stop()


async def test_launch_request2():
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

async def test_kill():
    node = Service(
        url="http://localhost:3371", 
        loader="tests/example3/node4.yaml")
    node.start()
    resp = httpx.post("http://localhost:3371/flows/flow1", timeout=None,
                      data={"submit": "submit", "runtime": 30},
                      headers={"Accept": "application/json"})
    fid = resp.headers["location"].split("/")[-1]
    assert fid is not None
    collect = []
    result = ExecutionResult(f"./data/exec/{fid}")
    n = 0
    while n < 3:
        pro = await result.get_progress()
        collect.append(pro)
        if pro.get("driver"):
            pid = pro["driver"].get("pid", None)
            if pid:
                p = psutil.Process(pid)
                print(n, pid, p.status())
                assert p.status() == "running"
                n += 1
        if pro["status"] in ("completed", "error"):
            break
        await asyncio.sleep(0.5)
    resp = httpx.post(f"http://localhost:3371/flow/{fid}/kill", timeout=None)
    assert resp.status_code == 201
    pro = await result.get_progress()
    pid = pro["driver"]["pid"]
    while True:
        try:
            p = psutil.Process(pid)
        except:
            print(pid, "not found")
            break
        else:
            print("check", pid, p.status())
            if p.status() == "zombie":
                break
            #assert p.status() == "zombie"
        await asyncio.sleep(0.1)
    node.stop()

async def test_launch_request10():
    node = Service(
        url="http://localhost:3371", 
        loader="tests/example3/node4.yaml")
    node.start()
    err = 0
    for i in range(5):
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
    assert err == 0



