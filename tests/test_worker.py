import datetime
import sys
from io import BytesIO
from subprocess import check_call
from typing import Optional

import httpx
import numpy as np
import pandas as pd
from litestar.datastructures import FormMultiDict, UploadFile

import kodo.worker.act
from kodo.common import publish
from kodo.common import Launch
from tests.test_node import Service


def execute(inputs: Optional[dict]=None):
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
    worker = kodo.worker.act.FlowAction("tests.test_worker:flow1", tmp_path)
    data = np.random.rand(1000, 10)
    df = pd.DataFrame(data, columns=[f"col{i}" for i in range(10)])
    bin = BytesIO()
    df.to_excel(bin, index=False)
    bin.seek(0)
    form_data = FormMultiDict(
        {
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
        }
    )
    response = worker.enter("enter", form_data)
    assert response.content == "hello world: METHOD enter, float"
    response = worker.enter("enter")
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
    assert "NEW Launch" in resp.content.decode("utf-8")
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
    assert "NEW Launch" in resp.content.decode("utf-8")
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

def execute_remote(inputs: Optional[dict]=None):
    return "OK"

ray_flow = publish(
    execute, url="/execute_remote", name="Ray Test",
    description="cannot be empty")

@ray_flow.enter
def ray_landing_page(form):
    topic = form.get("topic")
    if topic:
        return Launch(topic=topic)
    return f'<input type="submit" name="submit"><input type="text" name="topic">'


async def test_ray_startup(tmp_path):
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
    node.stop()
