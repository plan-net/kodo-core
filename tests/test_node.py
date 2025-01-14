import multiprocessing
import pytest
import urllib.parse
import httpx
import asyncio
from pathlib import Path
import pandas as pd
import time
import traceback
from tests.assets.agent50 import data as test_data
from kodo.service.node import run_service
from litestar.datastructures import State
import logging
import kodo.worker.loader

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
        kwargs["screen_level"] = 'DEBUG'
        self.process = Process(target=run_service, kwargs=kwargs)
        self.cache_file = Path(kwargs["cache_data"])
        
    def start(self):
        self.process.start()
        while True:
            try:
                resp = httpx.get(self.url, timeout=600)
                if resp.status_code == 200:
                    break
            except:
                pass
            time.sleep(0.5)
            #asyncio.sleep(0.5)

    def wait(self):
        while True:
            try:
                resp = httpx.get(self.url, timeout=600)
                if resp.status_code == 200:
                    if resp.json()["idle"]:
                        break
            except:
                pass
            time.sleep(0.5)
            #asyncio.sleep(0.5)

    def stop(self, cache=False):
        self.process.terminate()
        self.process.join()
        if not cache:
            self.cache_file.unlink(missing_ok=True)

def test_url():
    server = urllib.parse.urlparse("http://localhost:3366")
    assert server.hostname == "localhost"
    assert server.port == 3366
    assert server.scheme == "http"


empty = None

def loader1(self: kodo.worker.loader.Loader) -> None:
    self.flows = [
        {
            "entry": "tests.test_node:empty",
            "url": "/test",
            "name": "Test"
        }
    ]
    # self.add_flow(
    #     url="/test",
    #     name="Test",
    #     entry_point="tests.test_node:empty",
    # )

def loader2(self: kodo.worker.loader.Loader) -> None:
    self.flows = [
        {
            "entry": "tests.test_node:empty",
            "url": "/test2",
            "name": "Test 2"
        }
    ]
    # self.add_flow(
    #     url="/test2",
    #     name="Test 2",
    #     entry_point="tests.test_node:empty",
    # )

def loader3(self: kodo.worker.loader.Loader) -> None:
    self.flows = [
        {
            "entry": "tests.test_node:empty",
            "url": "/test3",
            "name": "Test 3"
        }
    ]
    # self.add_flow(
    #     url="/test3",
    #     name="Test 3",
    #     entry_point="tests.test_node:empty",
    # )

def _load_prop(self, start, end):
    for rec in test_data[start:end]:
        rec["entry_point"] = "tests.test_node:empty"
        self.add_flow(**rec)

def loader4(self: kodo.worker.loader.Loader) -> None:
    return _load_prop(self, 0, 50)

async def test_node():
    node1 = Service(
        url="http://localhost:3370", 
        organization="node1",
        registry=True,
        feed=True,
        loader="tests.test_node:loader1"
    )
    node1.start()
    resp = httpx.get(f"{node1.url}/connect", timeout=None)
    assert len(resp.json()["nodes"][0]["flows"]) == 1
    registry2 = Service(
        url="http://localhost:3371", 
        organization="registry2",
        registry=True,
        feed=True,
        connect=[node1.url],
        loader="tests.test_node:loader2"
    )
    registry2.start()
    registry2.wait()
    resp1 = httpx.get(f"{node1.url}/connect", timeout=None)
    resp2 = httpx.get(f"{registry2.url}/connect", timeout=None)
    registry2.wait()
    
    assert len(resp1.json()["nodes"]) == 2
    assert len(resp2.json()["nodes"]) == 2

    node3 = Service(
        url="http://localhost:3372", 
        connect=["http://localhost:3373"],
        organization="node2",
        registry=False,
        feed=False,
        loader="tests.test_node:loader3"
    )
    node3.start()

    registry4 = Service(
        url="http://localhost:3373", 
        organization="registry3",
        registry=True,
        feed=True,
        connect=[node1.url]
    )
    registry4.start()
    registry4.wait()
    node3.wait()

    resp1 = httpx.get(f"{node1.url}/connect", timeout=None)
    resp2 = httpx.get(f"{registry2.url}/connect", timeout=None)
    resp3 = httpx.get(f"{node3.url}/connect", timeout=None)
    resp4 = httpx.get(f"{registry4.url}/connect", timeout=None)
    assert sorted([n["url"] for n in resp1.json()["nodes"]]) == sorted([n["url"] for n in resp2.json()["nodes"]])
    assert sorted([n["url"] for n in resp1.json()["nodes"]]) == sorted([n["url"] for n in resp4.json()["nodes"]])
    assert len(resp1.json()["nodes"]) == 3
    assert len(resp2.json()["nodes"]) == 3
    assert len(resp3.json()["nodes"]) == 1
    assert len(resp4.json()["nodes"]) == 3
    node1.stop()
    registry2.stop()
    node3.stop()
    registry4.stop()


async def test_cascade():
    registry1 = Service(
        url="http://localhost:3370", 
        organization="registry1",
        registry=True,
        feed=True
    )
    registry1.start()

    node11 = Service(
        url="http://localhost:3371", 
        connect=[registry1.url],
        organization="node11",
        registry=False,
        feed=False,
        loader="tests.test_node:loader1",
    )
    node11.start()

    registry2 = Service(
        url="http://localhost:3372", 
        connect=[registry1.url],
        organization="registry2",
        registry=True,
        feed=True
    )
    registry2.start()

    registry3 = Service(
        url="http://localhost:3373", 
        connect=[registry2.url],
        organization="registry3",
        registry=True,
        feed=True
    )
    registry3.start()

    node31 = Service(
        url="http://localhost:3374", 
        connect=[registry3.url],
        organization="node31",
        registry=False,
        feed=False,
        loader="tests.test_node:loader2",
    )
    node31.start()
    for proc in (registry1, node11, registry2, registry3, node31):
        proc.wait()
    resp = {}
    for proc in (registry1, node11, registry2, registry3, node31):
        resp[proc.url] = httpx.get(f"{proc.url}/connect", timeout=None)
    ret = [(k, sorted([n["url"] for n in v.json()["nodes"]])) for k, v in sorted(resp.items())]
    expected = [ 
        ('http://localhost:3370', ['http://localhost:3371', 'http://localhost:3374']), 
        ('http://localhost:3371', ['http://localhost:3371']), 
        ('http://localhost:3372', ['http://localhost:3371', 'http://localhost:3374']), 
        ('http://localhost:3373', ['http://localhost:3371', 'http://localhost:3374']), 
        ('http://localhost:3374', ['http://localhost:3374'])
    ]
    assert ret == expected
    for proc in (registry1, node11, registry2, registry3, node31):
        proc.stop()



async def test_query():
    node = Service(
        url="http://localhost:3370", 
        organization="node",
        registry=True,
        feed=True,
        loader="tests.test_node:loader4",
    )
    node.start()
    node.wait()
    resp = httpx.get(f"{node.url}/flows", timeout=None)
    assert len(resp.json()["items"]) == 10
    assert resp.json()["total"] == 50 

    p0 = httpx.get(f"{node.url}/flows?pp=15&p=0").json()
    p1 = httpx.get(f"{node.url}/flows?pp=15&p=1").json()
    p2 = httpx.get(f"{node.url}/flows?pp=15&p=2").json()
    p3 = httpx.get(f"{node.url}/flows?pp=15&p=3").json()
    p4 = httpx.get(f"{node.url}/flows?pp=15&p=4").json()
    df0 = pd.DataFrame(p0["items"])
    df1 = pd.DataFrame(p1["items"])
    df2 = pd.DataFrame(p2["items"])
    df3 = pd.DataFrame(p3["items"])
    df4 = pd.DataFrame(p4["items"])
    assert df0.shape[0] == 15
    assert df1.shape[0] == 15
    assert df2.shape[0] == 15
    assert df3.shape[0] == 5
    assert df4.shape[0] == 0
    # names = pd.concat([df0, df1, df2, df3, df4]).name.tolist()
    # assert names == sorted([i["name"] for i in test_data])
    # p = httpx.get(f"{node.url}/flows?pp=15&p=0&q=organization=='Mediaplus'").json()
    # df = pd.DataFrame(p["items"])    
    # assert df.organization.unique().tolist() == ["Mediaplus"]
    # assert df.shape[0] == 7
    # p = httpx.get(f"{node.url}/flows?pp=15&p=0&q=organization=='Mediaplus'&by=author,name:d").json()
    # df = pd.DataFrame(p["items"])    
    # assert ['Agent Hades', 'Agent Demeter', 'Agent Zeus', 'Agent Hestia', 
            # 'Agent Hephaestus', 'Agent Poseidon', 'Agent Dionysus'] == df.name.tolist()
    node.stop()

def loader4(self: kodo.worker.loader.Loader) -> None:
    return _load_prop(self, 0, 50)

def prop1(self):
    return _load_prop(self, 0, 8)

def prop2(self):
    return _load_prop(self, 8, 20)

def prop3(self):
    return _load_prop(self, 20, 32)

def prop4(self):
    return _load_prop(self, 32, 39)

def prop5(self):
    return _load_prop(self, 39, 50)

async def test_query2():
    nodes = []
    port = 3370
    for orga, meth in (
            ("Serviceplan", "prop1"), 
            ("Serviceplan HR", "prop2"), 
            ("Serviceplan PR", "prop3"), 
            ("Mediaplus", "prop4"),
            ("MP Research", "prop5")):
        node = Service(
            url=f"http://localhost:{port}", 
            connect=[f"http://localhost:3369"],
            organization=orga,
            registry=False,
            feed=False,
            loader=f"tests.test_node:{meth}",
        )
        port += 1
        node.start()
        nodes.append(node)
    registry = Service(
        url=f"http://localhost:3369", 
        organization="Plan.Net Journey",
        registry=True,
        feed=True,
    )
    registry.start()
    for node in nodes:
        node.wait()
    registry.wait()
    
    resp = httpx.get(f"{registry.url}/flows", timeout=None)
    assert len(resp.json()["items"]) == 10
    assert resp.json()["total"] == 50 

    def query(q: str):
        resp = httpx.get(
            f"{registry.url}/flows?q=" + urllib.parse.quote(q), timeout=None)
        assert resp.status_code == 200
        return resp.json()

    resp = query('organization=="Serviceplan"')
    assert query('organization=="Serviceplan"')["filtered"] == 8
    assert query('organization=="Mediaplus"')["filtered"] == 7
    assert query('organization=="Serviceplan HR"')["filtered"] == 12
    assert query('organization=="Serviceplan PR"')["filtered"] == 12
    assert query('organization=="MP Research"')["filtered"] == 11
    assert query('organization=="MP Research" & name ~"agent m"')["filtered"] == 2
    assert query('organization=="Serviceplan HR" & author ~ "Meryl"')["filtered"] == 2
    assert query('author ~ "Meryl"')["filtered"] == 9
    assert query('Meryl')["filtered"] == 9
    assert query('author == ""')["filtered"] == 1
    assert query('combat')["filtered"] == 2
    assert query('@tag("operations")')["filtered"] == 5
    assert query('@tag("operations") & @tag("rescue")')["filtered"] == 1
    assert query('@tag("operations") | @tag("maintenance")')["filtered"] == 6

    for node in nodes:
        node.stop()
    registry.stop()


# def test_example(caplog):
#     import kodo.config
#     logger = logging.getLogger("kodo")
#     with caplog.at_level(logging.INFO):
#         logger.info("This is an info message")
#     assert "This is an info message" in caplog.text

async def test_node_status():
    node = Service(url="http://localhost:3370", registry=True, feed=True)
    node.start()
    resp = httpx.get(f"{node.url}/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["url"] == node.url
    assert data["idle"] is True
    node.stop()

async def test_provider_map():
    node = Service(url="http://localhost:3370", registry=True, feed=True)
    node.start()
    resp = httpx.get(f"{node.url}/map")
    assert resp.status_code == 200
    data = resp.json()
    assert "providers" in data
    assert "connection" in data
    node.stop()

async def test_connect():
    registry1 = Service(
        url="http://localhost:3370", 
        organization="registry1",
        registry=True,
        feed=True,
        loader="tests.test_node:loader1",
    )
    registry1.start()

    resp = httpx.get(f"{registry1.url}/flows", timeout=None)
    assert resp.json()["total"] == 1

    registry2 = Service(
        url="http://localhost:3371", 
        connect=[registry1.url],
        organization="registry2",
        registry=True,
        feed=True
    )
    registry2.start()
    registry2.wait()
    resp = httpx.get(f"{registry2.url}/flows", timeout=None)
    assert resp.json()["total"] == 1
    
    node = Service(
        url="http://localhost:3372", 
        connect=[registry2.url],
        organization="node1",
        registry=False,
        feed=False,
        loader="tests.test_node:loader2",
    )
    node.start()
    for server in (registry1, registry2, node):
        server.wait()
    resp1 = httpx.get(f"{registry1.url}/flows", timeout=None)
    assert resp1.json()["total"] == 2
    resp2 = httpx.get(f"{registry2.url}/flows", timeout=None)
    assert resp2.json()["total"] == 2

    node.stop()

    node = Service(
        url="http://localhost:3372", 
        connect=[registry2.url],
        organization="node1",
        registry=False,
        feed=False,
        loader="tests.test_node:loader2",
    )
    node.start()
    for server in (registry1, registry2, node):
        server.wait()
    resp1 = httpx.get(f"{registry1.url}/flows", timeout=None)
    assert resp1.json()["total"] == 2
    resp2 = httpx.get(f"{registry2.url}/flows", timeout=None)
    assert resp2.json()["total"] == 2

    node1 = Service(
        url="http://localhost:3373", 
        connect=[registry1.url],
        organization="node2",
        registry=False,
        feed=False,
        loader="tests.test_node:loader3",
    )
    node1.start()
    for server in (registry1, registry2, node, node1):
        server.wait()
    resp1 = httpx.get(f"{registry1.url}/flows", timeout=None)
    assert resp1.json()["total"] == 3
    resp2 = httpx.get(f"{registry2.url}/flows", timeout=None)
    assert resp2.json()["total"] == 3

    registry1.stop()
    registry2.stop()
    node.stop()
    node1.stop()

async def _3registries():
    registry3 = Service(
        url="http://localhost:3370", 
        organization="registry3",
        registry=True,
        feed=True
    )
    registry3.start()
    registry2 = Service(
        url="http://localhost:3371", 
        connect=[registry3.url],
        organization="registry2",
        registry=True,
        feed=True
    )
    registry2.start()
    registry1 = Service(
        url="http://localhost:3372", 
        connect=[registry2.url],
        organization="registry1",
        registry=True,
        feed=True
    )
    registry1.start()
    for service in (registry1, registry2, registry3):
        service.wait()
    for service in (registry1, registry2, registry3):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 0
    return registry1, registry2, registry3

async def test_3registries():
    registry1, registry2, registry3 = await _3registries()
    for service in (registry1, registry2, registry3):
        service.stop()
    
async def test_nodes_connect(gimme=False):
    registry1, registry2, registry3 = await _3registries()
    node1 = Service(
        url="http://localhost:3373", 
        connect=[registry1.url],
        feed=False,
        organization="node1",
        loader="tests.test_node:loader1"
    )
    node1.start()
    node2 = Service(
        url="http://localhost:3374", 
        connect=[registry2.url],
        feed=False,
        organization="node2",
        loader="tests.test_node:loader2"
    )
    node2.start()
    node3 = Service(
        url="http://localhost:3375", 
        connect=[registry3.url],
        feed=False,
        organization="node3",
        loader="tests.test_node:loader3"
    )
    node3.start()
    for service in (registry1, registry2, registry3, node1, node2, node3):
        service.wait()

    for service in (registry1, registry2, registry3):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 3
    if gimme:
        return (registry1, registry2, registry3, node1, node2, node3)
    for service in (registry1, registry2, registry3, node1, node2, node3):
        service.stop()


async def test_nodes_disconnect():
    (
        registry1, registry2, registry3, 
        node1, node2, node3
    ) = await test_nodes_connect(gimme=True)

    for service in (registry1, registry2, registry3):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        print(resp1.json()["total"] == 3)

    for service in (registry1, registry2, registry3):
        resp1 = httpx.get(f"{service.url}/map", timeout=None)

    resp = httpx.delete(f"{node2.url}/connect", timeout=None)

    for service in (registry1, registry2, registry3, node1, node2, node3):
        service.wait()

    for service in (registry1, registry2, registry3):
        resp1 = httpx.get(f"{service.url}/map", timeout=None)

    for service in (registry1, registry2, registry3, node1, node2, node3):
        service.wait()

    for service in (registry1, registry2, registry3):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 2

    for service in (registry1, registry2, registry3, node1, node2, node3):
        service.stop()

async def test_4registries(gimme=False):
    (
        registry1, registry2, registry3, 
        node1, node2, node3
    ) = await test_nodes_connect(gimme=True)
    registry4 = Service(
        url="http://localhost:3376", 
        connect=[registry3.url],
        registry=True,
        feed=True,
        organization="registry4"
    )
    registry4.start()
    node4 = Service(
        url="http://localhost:3377", 
        connect=[registry4.url],
        feed=False,
        organization="node4",
        loader="tests.test_node:loader4"
    )
    node4.start()
    peers = (
        registry1, node1, 
        registry2, node2, 
        registry3, node3, 
        registry4, node4
    )
    for service in peers:
        service.wait()

    for service in (registry1, registry2, registry3, registry4):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 53

    if gimme:
        return peers

    for service in peers:
        service.stop()

async def test_node2_off(gimme=False):
    peers = (
        registry1, node1, # 3372 - 3373
        registry2, node2, # 3371 - 3374
        registry3, node3, # 3370 - 3375
        registry4, node4  # 3376 - 3377
    ) = await test_4registries(gimme=True)
    
    resp = httpx.delete(f"{node2.url}/connect", timeout=None)

    for service in (registry1, registry2, registry3, registry4):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 52

    if gimme:
        return peers

    for service in peers:
        service.stop()

async def test_node_4_1_off():
    peers = (
        registry1, node1, # 3372 - 3373
        registry2, node2, # 3371 - 3374
        registry3, node3, # 3370 - 3375
        registry4, node4  # 3376 - 3377
    ) = await test_node2_off(gimme=True)

    resp = httpx.delete(f"{node4.url}/connect", timeout=None)

    for service in (registry1, registry2, registry3, registry4):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 2

    resp = httpx.delete(f"{node1.url}/connect", timeout=None)

    for service in (registry1, registry2, registry3, registry4):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 1

    resp = httpx.delete(f"{node3.url}/connect", timeout=None)

    for service in (registry1, registry2, registry3, registry4):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 0

    for service in peers:
        service.stop()


async def test_registry1_off():
    peers = (
        registry1, node1, # 3372 - 3373
        registry2, node2, # 3371 - 3374
        registry3, node3, # 3370 - 3375
        registry4, node4  # 3376 - 3377
    ) = await test_4registries(gimme=True)

    resp = httpx.delete(f"{registry1.url}/connect", timeout=None)

    for service in (registry2, registry3, registry4):
        resp1 = httpx.get(f"{service.url}/flows", timeout=None)
        assert resp1.json()["total"] == 52
    
    for service in peers:
        service.stop()


def test_mass_disconnect():
    registry1 = Service(
        url="http://localhost:3370", 
        registry=True, 
        feed=True,
        cache_reset=True,
        organization="registry1"
    )
    registry1.start()

    registry2 = Service(
        url="http://localhost:3371",
        connect=[registry1.url],
        registry=True, 
        feed=True,
        cache_reset=True,
        organization="registry2"
    )
    registry2.start()

    registry3 = Service(
        url="http://localhost:3372",
        connect=[registry2.url],
        registry=True, 
        feed=True,
        cache_reset=True,
        organization="registry3"
    )
    registry3.start()

    node1 = Service(
        url="http://localhost:3373",
        connect=[registry3.url],
        organization="node1",
        registry=False,
        feed=False,
        cache_reset=True,
        loader="tests.test_node:loader1"
    )
    node1.start()

    time.sleep(20)
    resp_before = httpx.get(f"{registry1.url}/connect")
    assert resp_before.status_code == 200
    assert len(resp_before.json()["nodes"]) > 0

    resp = httpx.delete(f"{node1.url}/connect")
    assert resp.status_code == 204
    
    time.sleep(20)

    resp_after = httpx.get(f"{registry1.url}/connect")
    assert resp_after.status_code == 200
    assert len(resp_after.json()["nodes"]) == 0

    registry1.stop()
    registry2.stop()
    registry3.stop()
    node1.stop()

async def test_cache():
    def mk_registry(**kwargs):
        return Service(
            url="http://localhost:3370", 
            organization="registry",
            registry=True,
            feed=True,
            **kwargs
        )
    def mk_node(**kwargs):
        return Service(
            url="http://localhost:3371", 
            connect=[registry.url],
            organization="node",
            registry=False,
            feed=False,
            **kwargs
        )
    registry = mk_registry(loader="tests.test_node:loader1")
    registry.start()

    node = mk_node(loader="tests.test_node:loader2")
    node.start()

    resp = httpx.get(f"{registry.url}/flows", timeout=None)
    assert resp.json()["total"] == 2
    assert [i["url"] for i in resp.json()["items"]] == [
        "http://localhost:3370/flows/test", "http://localhost:3371/flows/test2"]
    
    registry.stop(cache=True)
    registry = mk_registry(loader="tests.test_node:loader1", cache_reset=False)
    registry.start()
    registry.wait()
    node.wait()

    resp = httpx.get(f"{registry.url}/flows", timeout=None)
    assert resp.json()["total"] == 2
    assert [i["url"] for i in resp.json()["items"]] == [
        "http://localhost:3370/flows/test", "http://localhost:3371/flows/test2"]

    node.stop(cache=True)

    registry.stop(cache=True)
    registry = mk_registry(loader="tests.test_node:loader1", cache_reset=False)
    registry.start()
    # time.sleep(6)

    resp = httpx.get(f"{registry.url}/flows", timeout=None)
    assert resp.json()["total"] == 2
    assert [i["url"] for i in resp.json()["items"]] == [
        "http://localhost:3370/flows/test", "http://localhost:3371/flows/test2"]

    node = mk_node(loader="tests.test_node:loader3")
    node.start()

    registry.wait()
    node.wait()

    resp = httpx.get(f"{registry.url}/flows", timeout=None)
    assert resp.json()["total"] == 2
    assert [i["url"] for i in resp.json()["items"]] == [
        "http://localhost:3370/flows/test", "http://localhost:3371/flows/test3"]
    node.stop()
    registry.stop()

def sp_loader(self) -> None:
    return _load_prop(self, 0, 12)

def mp_loader(self) -> None:
    return _load_prop(self, 12, 25)

def mp_forschung_loader(self) -> None:
    return _load_prop(self, 25, 40)

def pn_loader(self) -> None:
    return _load_prop(self, 40, 50)

async def test_mesh():
    def make_sp_registry(**kwargs):
        return Service(
            url="http://localhost:3370", 
            organization="serviceplan registry",
            registry=True,
            feed=True,
            **kwargs
        )
    def make_sp_node(**kwargs):
        return Service(
            url="http://localhost:3371", 
            connect=["http://localhost:3370"],
            organization="servicplan agents",
            registry=False,
            feed=False,
            loader="tests.test_node:sp_loader",
            **kwargs
        )
    def make_mp_registry(**kwargs):
        return Service(
            url="http://localhost:3372", 
            connect=["http://localhost:3370"],  # serviceplan registry
            organization="mediaplus registry",
            registry=True,
            feed=True,
            **kwargs
        )
    def make_mp_node(**kwargs):
        return Service(
            url="http://localhost:3373", 
            connect=["http://localhost:3372"],
            organization="mediaplus agents",  # mediaplus registry
            registry=False,
            feed=False,
            loader="tests.test_node:mp_loader",
            **kwargs
        )
    def make_mp_forschung(**kwargs):
        return Service(
            url="http://localhost:3374", 
            connect=["http://localhost:3372"],  # mediaplus registry
            organization="mp research agents",
            registry=False,
            feed=False,
            loader="tests.test_node:mp_forschung_loader",
            **kwargs
        )
    def make_pn_node(**kwargs):
        return Service(
            url="http://localhost:3375", 
            connect=["http://localhost:3370"],  # serviceplan registry
            organization="plan.net agents",
            registry=True,
            feed=True,
            loader="tests.test_node:pn_loader",
            **kwargs
        )

    sp_registry = make_sp_registry()
    sp_node = make_sp_node()
    sp_registry.start()
    sp_node.start()

    mp_registry = make_mp_registry()
    mp_node = make_mp_node()
    mp_resarch = make_mp_forschung()

    mp_registry.start()
    mp_node.start()
    mp_resarch.start()

    pn_node = make_pn_node()
    pn_node.start()

    peers = (sp_registry, sp_node, mp_registry, mp_node, mp_resarch, pn_node)
    registries = (sp_registry, mp_registry, pn_node)

    for peer in peers:
        peer.wait()

    for registry in registries:
        resp = httpx.get(f"{registry.url}/flows", timeout=None)
        assert resp.json()["total"] == 50

    resp = httpx.get(f"{sp_node.url}/flows", timeout=None)
    assert resp.json()["total"] == 12

    resp = httpx.delete(f"{sp_node.url}/connect", timeout=None)
    sp_node.stop()

    for registry in registries:
        resp = httpx.get(f"{registry.url}/flows", timeout=None)
        assert resp.json()["total"] == 50 - 12

    sp_node = make_sp_node()
    sp_node.start()

    peers = (sp_registry, sp_node, mp_registry, mp_node, mp_resarch, pn_node)
    for peer in peers:
        peer.wait()

    for registry in registries:
        resp = httpx.get(f"{registry.url}/flows", timeout=None)
        assert resp.json()["total"] == 50

    #assert [i["url"] for i in resp.json()["items"]] == ["/test", "/test3"]
    expected = {
        "total": 50,
        "organization": {
            "mediaplus agents": 13,
            "mp research agents": 15,
            "plan.net agents": 10,
            "servicplan agents": 12
        },
        "tags": {
            "advanced": 1,
            "aerial": 1,
            "analyst": 1,
            "assessment": 1,
            "asset": 1,
            "chases": 1,
            "combat": 2,
            "commander": 1,
            "communications": 1,
            "control": 1,
            "coordinator": 1,
            "crisis": 1,
            "cultural": 1,
            "cybersecurity": 1,
            "disaster": 1,
            "disguise": 1,
            "driver": 1,
            "encryption": 1,
            "expert": 6,
            "exploit": 1,
            "explosives": 1,
            "field": 1,
            "handler": 1,
            "hardware": 1,
            "hazardous": 1,
            "heavy": 1,
            "high-speed": 1,
            "home": 1,
            "hostage": 1,
            "infiltration": 1,
            "intelligence": 2,
            "journey": 1,
            "liaison": 2,
            "lifting": 1,
            "logistics": 1,
            "maintenance": 1,
            "manager": 1,
            "maritime": 1,
            "materials": 1,
            "medic": 1,
            "mission": 1,
            "monitoring": 1,
            "negotiation": 1,
            "negotiator": 1,
            "operational": 1,
            "operations": 5,
            "operative": 1,
            "oratory": 1,
            "personnel": 1,
            "planner": 2,
            "planning": 1,
            "problem": 1,
            "protection": 1,
            "reconnaissance": 1,
            "rescue": 1,
            "response": 1,
            "security": 1,
            "signal": 1,
            "skills": 1,
            "solver": 1,
            "specialist": 4,
            "stealth": 1,
            "strategy": 1,
            "surveillance": 2,
            "systems": 1,
            "tactical": 2,
            "tech": 1,
            "technician": 1,
            "threat": 1,
            "trainer": 1,
            "unarmed": 1,
            "undercover": 1,
            "vehicle": 1,
            "visual": 1,
            "weapons": 2,
            "x-ray": 1,
            "youth": 1,
            "zero-day": 1,
            "zonal": 1
        }   
    }
    resp = httpx.get(f"{registry.url}/counts", timeout=None)
    assert resp.json() == expected
    for peer in peers:
        peer.stop()