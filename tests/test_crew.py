import httpx
import time

from tests.test_node import Service


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

async def test_crewai():
    node = Service(
        url="http://localhost:3367", 
        loader="tests.test_crew:crew_loader",)
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


async def test_crewai_with_ray():
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

