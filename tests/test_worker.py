import datetime
import os
import time
import sys
from io import BytesIO, StringIO
from subprocess import check_call
from typing import Optional

import httpx
import numpy as np
import pandas as pd
from litestar.datastructures import FormMultiDict, UploadFile

import kodo.worker.process.launcher
from kodo.common import Launch, publish
from kodo.worker.instrument.flow import Flow
from kodo.worker.instrument.result import ExecutionResult
from tests.test_node import Service


def execute(inputs, event: Flow):
    event.result(dict(test="hello world"))
    for i in range(5000):
        event.result(dict(test=f"intermediate result {i}"))
    event.final({"message": "Hallo Welt"})
    return "OK"

flow1 = publish(
    execute, url="/execute", name="Test Executor",
    description="none")

@flow1.enter
def landing_page1(form, method):
    field2 = form.get("field3")
    file2 = form.get("file2")
    if file2:
        df = pd.read_excel(file2.file)
        assert df.shape == (1000, 10)
    return f"hello world: METHOD {method}, {field2.__class__.__name__}"


async def test_ipc(tmp_path):
    worker = kodo.worker.process.launcher.FlowLauncher("tests.test_worker:flow1", tmp_path)
    data = np.random.rand(1000, 10)
    df = pd.DataFrame(data, columns=[f"col{i}" for i in range(10)])
    bin = BytesIO()
    df.to_excel(bin, index=False)
    bin.seek(0)
    form_data = FormMultiDict({
        "field1": "value1",
        "field2": datetime.datetime.now(),
        "field3": 123.456,
        "file1": UploadFile(
            filename="filename1.txt", 
            file_data=b"file content 1", 
            content_type="text/plain"
            ),
        "file2": UploadFile(
            filename="filename2.txt", 
            file_data=bin.read(), 
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    })
    response = await worker.enter("enter", form_data)
    assert response.content == "hello world: METHOD enter, float"
    response = await worker.enter("enter")
    assert response.content == "hello world: METHOD enter, NoneType"
    #response = worker.launch()


flow2 = publish(
    execute, url="/execute", name="Test Flow", description="missing")


@flow2.enter
def landing_page2(form, method):
    if form.get("submit"):
        return Launch()
    return f'<input type="submit" name="submit">'


def loader1():
    return """
- url: http://localhost:3367
- organization: Launch Test
- registry: false
- feed: false
- screen_level: debug
- flows:
  - entry: tests.test_worker:flow2
"""


async def test_launch(tmp_path):
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_worker:loader1")
    node.start()
    resp = httpx.get("http://localhost:3367/flows/execute", timeout=None)
    assert '<input type="submit" name="submit">' in resp.content.decode("utf-8")
    resp = httpx.post(
        "http://localhost:3367/flows/execute", data={"submit": "submit"},
        headers={"Accept": "text/html"}, timeout=None)   
    assert resp.status_code == 302
    # assert "NEW Launch" in resp.content.decode("utf-8")
    resp = httpx.get("http://localhost:3367/flows?format=json", timeout=None)
    assert resp.json()["total"] == 1
    flow = resp.json()["items"][0]
    assert flow["url"] == "http://localhost:3367/flows/execute"
    assert flow["name"] == "Test Flow"
    assert flow["description"] == "missing"
    node.stop()


async def test_option(tmp_path):
    node = Service(
        url="http://localhost:3367", 
        loader="tests/assets/flow0.yaml")
    node.start()
    resp = httpx.get("http://localhost:3367/flows?format=json", timeout=None)
    assert resp.json()["total"] == 20
    resp = httpx.get("http://localhost:3367/flows?format=json&q=test", timeout=None)
    assert resp.json()["total"] == 20
    assert resp.json()["filtered"] == 1
    flow = resp.json()["items"][0]
    assert flow["url"] == "http://localhost:3367/flows/test/flow"
    assert flow["name"] == "Experimental Test Flow"
    assert flow["description"] == "Technical Test Flow"
    node.stop()

    # worker = kodo.worker.main.IPCworker("tests.test_worker:flow2", tmp_path)
    # response = worker.welcome()
    # assert response.content == "hello world"


flow4 = publish(
    execute, url="/execute", name="Test Flow", description="missing")


@flow4.enter
def landing_page3(form, method):
    if form.get("submit"):
        print("GO NOW")
        return Launch()
    print("NOT, YET")
    check_call([sys.executable, "-c", "import sys; print('This is stdout'); print('This is stderr', file=sys.stderr)"])
    return f'<input type="submit" name="submit">'



def loader4():
    return """
- url: http://localhost:3367
- registry: false
- feed: false
- screen_level: debug
- flows:
  - entry: tests.test_worker:flow4
"""

async def test_launch_printer(tmp_path):
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_worker:loader4")
    node.start()
    resp = httpx.get("http://localhost:3367/flows/execute", timeout=None)
    assert '<input type="submit" name="submit">' in resp.content.decode("utf-8")
    resp = httpx.post(
        "http://localhost:3367/flows/execute", data={"submit": "submit"},
        headers={"Accept": "text/html"}, timeout=None)
    assert resp.status_code == 302
    # assert "NEW Launch" in resp.content.decode("utf-8")
    node.stop()


def ray_loader():
    return """
- url: http://localhost:3367
- organization: Launch Test
- registry: false
- feed: false
- screen_level: debug
- flows:
  - entry: tests.test_worker:ray_flow
"""

ray_flow = publish(
    execute, url="/execute_remote", name="Ray Test",
    description="cannot be empty")

@ray_flow.enter
def ray_landing_page(form):
    topic = form.get("topic")
    if topic:
        return Launch(topic=topic)
    return f'<input type="submit" name="submit"><input type="text" name="topic">'


async def test_thread_startup(tmp_path):
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_worker:ray_loader")
    node.start()
    resp = httpx.get("http://localhost:3367/flows/execute_remote", timeout=None)
    assert 'submit' in resp.content.decode("utf-8")
    assert 'topic' in resp.content.decode("utf-8")

    resp = httpx.post(
        "http://localhost:3367/flows/execute_remote", 
        data={"topic": "The more you know, the more you realize you don't know."},
        headers={"Accept": "application/json"}, timeout=None)
    assert resp.json()["flow"]["name"] == "Ray Test"
    assert resp.json()["returncode"] == 0
    assert resp.json()["success"]
    assert resp.json()["fid"] is not None
    import time
    while True:
        if '"status":"finished"' in open(resp.json()["event_log"], "r").read():
            break
        time.sleep(1)
    node.stop()


async def test_ray_startup():
    os.system("ray start --head")
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_worker:ray_loader", ray=True)
    node.start()
    resp = httpx.get("http://localhost:3367/flows/execute_remote", timeout=None)
    assert 'submit' in resp.content.decode("utf-8")
    assert 'topic' in resp.content.decode("utf-8")
    resp = httpx.post(
        "http://localhost:3367/flows/execute_remote", 
        data={"topic": "The more you know, the more you realize you don't know."},
        headers={"Accept": "application/json"}, timeout=None)
    assert resp.json()["flow"]["name"] == "Ray Test"
    assert resp.json()["returncode"] == 0
    assert resp.json()["success"]
    assert resp.json()["fid"] is not None
    import time
    while True:
        if '"status":"finished"' in open(resp.json()["event_log"], "r").read():
            break
        time.sleep(1)
    node.stop()
    os.system("ray stop")




def stream_loader():
    return """
- url: http://localhost:3367
- organization: Launch Test
- registry: false
- feed: false
- screen_level: debug
- flows:
  - entry: tests.test_worker:stream_flow
"""

def stream_execute(inputs, event: Flow):
    event.result(dict(test=f"TEST RESULT"))
    print("This is printed")
    print("This is printed to sys.stderr", file=sys.stderr)
    sys.stdout.write("This is written to sys.stdout\n")
    sys.stderr.write("This is written to sys.stderr\n")
    for i in range(3000):
        print(f"printing intermediate result {i}")
        event.result(dict(result=f"intermediate result {i}"))
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

async def test_stream_thread():
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_worker:stream_loader", ray=False)
    node.start()
    resp = httpx.post(
        "http://localhost:3367/flows/execute_remote", 
        data={"topic": "Test"},
        headers={"Accept": "application/json"}, timeout=None)
    while True:
        content = open(resp.json()["event_log"], "r").read()
        if '"status":"finished"' in content or '"status":"error"' in content:
            break
        time.sleep(1)
    node.stop()


async def test_execution():
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_worker:loader1")
    node.start()
    resp = httpx.get("http://localhost:3367/flows/execute", timeout=None)
    assert '<input type="submit" name="submit">' in resp.content.decode("utf-8")
    resp = httpx.post(
        "http://localhost:3367/flows/execute", data={"submit": "submit"},
        headers={"Accept": "application/json"}, timeout=None)   
    fid = resp.json()["fid"]
    while True:
        resp = httpx.get(f"http://localhost:3367/flow/{fid}", timeout=None)
        if resp.json().get("status", "unknown") == "finished":
            break
        time.sleep(0.1)
    # assert resp.json()["total"] == 1
    # flow = resp.json()["items"][0]
    # assert flow["url"] == "http://localhost:3367/flows/execute"
    # assert flow["name"] == "Test Flow"
    # assert flow["description"] == "missing"
    node.stop()


# def test_thread_aggregate():
#     inputs = """
# 2025-01-24T12:22:30.000000+00:00 data {"version":"0.0.0","entry_point":"tests.test_worker:ray_flow","fid":"679386076dd659cb0cb5665f"}
# 2025-01-24T12:22:31.639765+00:00 data {"inputs":{"topic":"The more you know, the more you realize you don't know."}}
# 2025-01-24T12:22:31.922876+00:00 data {"flow":{"url":"/execute_remote","name":"Ray Test","description":"cannot be empty","author":null,"tags":null,"entry":"tests.test_worker:ray_flow"}}
# 2025-01-24T12:22:32.000000+00:00 data {"status":"pending"}
# 2025-01-24T12:22:34.048620+00:00 data {"status":"booting","pid":51987,"ppid":51969,"executor":"thread"}
# 2025-01-24T12:22:36.594510+00:00 data {"status":"running"}
# 2025-01-24T12:22:36.597864+00:00 result {"test":"hello world"}
# 2025-01-24T12:22:36.600946+00:00 result {"test":"intermediate result 0"}
# 2025-01-24T12:22:36.604978+00:00 result {"test":"intermediate result 1"}
# 2025-01-24T12:22:36.608375+00:00 result {"test":"intermediate result 2"}
# 2025-01-24T12:22:43.962309+00:00 final {"message":"Hallo Welt"}
# 2025-01-24T12:22:43.964003+00:00 data {"status":"stopping"}
# 2025-01-24T12:22:45.000000+00:00 data {"status":"finished","runtime":10.421818}
#     """
#     stream = StringIO(inputs)
#     result = ExecutionResult(stream)
#     result.read()
#     assert result.runtime().total_seconds() == 13.0
#     assert result.status() == "finished"

# def test_thread_aggregate_no_end():
#     inputs = """
# 2025-01-24T12:22:30.000000+00:00 data {"version":"0.0.0","entry_point":"tests.test_worker:ray_flow","fid":"679386076dd659cb0cb5665f"}
# 2025-01-24T12:22:31.639765+00:00 data {"inputs":{"topic":"The more you know, the more you realize you don't know."}}
# 2025-01-24T12:22:31.922876+00:00 data {"flow":{"url":"/execute_remote","name":"Ray Test","description":"cannot be empty","author":null,"tags":null,"entry":"tests.test_worker:ray_flow"}}
# 2025-01-24T12:22:32.000000+00:00 data {"status":"pending"}
# 2025-01-24T12:22:34.048620+00:00 data {"status":"booting","pid":51987,"ppid":51969,"executor":"thread"}
# 2025-01-24T12:22:36.594510+00:00 data {"status":"running"}
# 2025-01-24T12:22:36.597864+00:00 result {"test":"hello world"}
#     """
#     stream = StringIO(inputs)
#     result = ExecutionResult(stream)
#     result.read()
#     assert result.runtime().total_seconds() > 20.0
#     assert result.status() == "running"

def execute3(inputs, flow: Flow):
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
            if (datetime.datetime.now() - t0).total_seconds() > int(runtime):
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

# def test_ray_aggregate():
#     inputs = """
# 2025-01-24T12:49:17.698192+00:00 data {"version":"0.0.0","entry_point":"tests.test_worker:ray_flow","fid":"67938c4d6dd659d16f138ddb"}
# 2025-01-24T12:49:17.703251+00:00 data {"inputs":{"topic":"The more you know, the more you realize you don't know."}}
# 2025-01-24T12:49:18.025376+00:00 data {"flow":{"url":"/execute_remote","name":"Ray Test","description":"cannot be empty","author":null,"tags":null,"entry":"tests.test_worker:ray_flow"}}
# 2025-01-24T12:49:18.030333+00:00 data {"status":"pending"}
# 2025-01-24T12:49:20.047854+00:00 data {"status":"booting","pid":53616,"ppid":53604,"executor":"ray"}
# 2025-01-24T12:49:20.908578+00:00 data {"ray":{"job_id":"01000000","node_id":"b56e74021ef8db670ff6539e12f47185206691a122c0ae22fae5e99a"}}
# 2025-01-24T12:49:22.920251+00:00 data {"status":"running"}
# 2025-01-24T12:49:23.926188+00:00 result {"test":"hello world"}
# 2025-01-24T12:49:23.933733+00:00 result {"test":"intermediate result 0"}
# 2025-01-24T12:49:23.938961+00:00 result {"test":"intermediate result 1"}
# 2025-01-24T12:49:23.943943+00:00 result {"test":"intermediate result 2"}
# 2025-01-24T12:49:51.096653+00:00 final {"message":"Hallo Welt"}
# 2025-01-24T12:49:51.101215+00:00 data {"status":"stopping"}
# 2025-01-24T12:49:51.105603+00:00 data {"status":"finished","runtime":31.055105}
# """
#     stream = StringIO(inputs)
#     result = ExecutionResult(stream)
#     result.read()
#     assert result.ray["job_id"] == "01000000"
#     assert result.ray["node_id"] == "b56e74021ef8db670ff6539e12f47185206691a122c0ae22fae5e99a"


async def test_execution_state():
    from httpx_sse import aconnect_sse
    node = Service(
        url="http://localhost:3371", 
        loader="tests/example3/node4.yaml")
    node.start()
    resp = httpx.post("http://localhost:3371/flows/flow1", timeout=None,
                      data={"submit": "submit", "runtime": 15},
                      headers={"Accept": "application/json"})
    fid = resp.json()["fid"]
    lines = []
    async with httpx.AsyncClient(
            timeout=None) as client:
        async with aconnect_sse(
            client, "GET", f"http://localhost:3371/flow/{fid}/stdout") as src:
            async for sse in src.aiter_sse():
                lines.append(sse.data)
    t = [int(i.split()[1]) for i in lines if i.startswith("debug")]
    assert t[-1] == len(t)
    assert lines[0] == "debug: 1"
    while True:
        resp = httpx.get(
            f"http://localhost:3371/flow/{fid}", timeout=None,
            headers={"Accept": "application/json"})
        status = resp.json().get("status", "unknown")
        if status == "finished":
            break
        time.sleep(0.1)
    node.stop()


def execute_inactive(inputs, event: Flow):
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



def flow5_loader():
    return """
- url: http://localhost:3371
- registry: false
- feed: false
- screen_level: debug
- flows:
  - entry: tests.test_worker:flow5
"""

async def test_inactive():
    from httpx_sse import aconnect_sse
    node = Service(
        url="http://localhost:3371", 
        loader="tests.test_worker:flow5_loader")
    node.start()
    resp = httpx.post("http://localhost:3371/flows/execute", timeout=None,
                      data={"submit": "submit"},
                      headers={"Accept": "application/json"})
    fid = resp.json()["fid"]
    inactive = []
    while True:
        resp = httpx.get(
            f"http://localhost:3371/flow/{fid}", timeout=None,
            headers={"Accept": "application/json"})
        status = resp.json().get("status", "unknown")
        inactive.append(resp.json().get("inactive", None))
        if status == "finished":
            break
        time.sleep(0.1)
    node.stop()
