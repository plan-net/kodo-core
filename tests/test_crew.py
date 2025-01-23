import httpx
import time
import os
import pytest
from tests.test_node import Service

def _online(url: str):
    try:
        return httpx.get(url, timeout=1)
    except:
        return None

def ollama_online():
    try:
        _online("http://127.0.0.1:11434")
        return True
    except:
        return False

def ray_online():
    return _online("http://127.0.0.1:8265/")


def test_prerequisites():
    assert ollama_online()
    # assert ray_online()

def crew_loader():
    return """
- url: http://localhost:3367
- organization: Launch Test
- registry: false
- feed: false
- screen_level: debug
- flows:
  - entry: tests.simple:flow
"""

@pytest.mark.timeout(300)
async def test_crewai_with_thread():
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_crew:crew_loader")
    node.start()
    resp = httpx.get("http://localhost:3367/flows/test/hymn1", timeout=None)
    assert 'submit' in resp.content.decode("utf-8")
    assert 'topic' in resp.content.decode("utf-8")

    resp = httpx.post(
        "http://localhost:3367/flows/test/hymn1", 
        data={"topic": "The more you know, the more you realize you don't know."},
        headers={"Accept": "application/json"}, timeout=None)
    assert resp.json()["flow"]["name"] == "Hymn Creator"
    assert resp.json()["returncode"] == 0
    assert resp.json()["success"]
    assert resp.json()["fid"] is not None
    while True:
        content = open(resp.json()["event_log"], "r").read()
        if '"status":"finished"' in content or '"status":"error"' in content:
            break
        time.sleep(1)
    node.stop()


@pytest.mark.timeout(300)
async def test_crewai_with_ray():
    os.system("ray start --head")
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_crew:crew_loader", ray=True)
    node.start()
    resp = httpx.get("http://localhost:3367/flows/test/hymn1", timeout=None)
    assert 'submit' in resp.content.decode("utf-8")
    assert 'topic' in resp.content.decode("utf-8")
    resp = httpx.post(
        "http://localhost:3367/flows/test/hymn1", 
        data={"topic": "The more you know, the more you realize you don't know."},
        headers={"Accept": "application/json"}, timeout=None)
    assert resp.json()["flow"]["name"] == "Hymn Creator"
    assert resp.json()["returncode"] == 0
    assert resp.json()["success"]
    assert resp.json()["fid"] is not None
    while True:
        content = open(resp.json()["event_log"], "r").read()
        if '"status":"finished"' in content or '"status":"error"' in content:
            break
        time.sleep(1)
    node.stop()
    os.system("ray stop")

