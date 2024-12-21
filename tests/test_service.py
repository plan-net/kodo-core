import asyncio
import datetime
import multiprocessing
import urllib
import json
import httpx
import pandas as pd
import pytest
import traceback
from pathlib import Path

from litestar.datastructures import State

import kodo.service.node
import kodo.types
from kodo import helper
from kodo.config import setting
import kodo.worker.loader
import kodo.worker.main


def _create_service(
        port, connect, node, registry, provider, explorer, loader, debug,
        organization, cache):
    kodo.service.node.run_service(
        url=f"http://localhost:{port}",
        connect=connect,
        node=node,
        registry=registry,
        provider=provider,
        explorer=explorer,
        loader=loader,
        cache=cache,
        organization=organization,
        reload=False,
        debug=debug)


PORT = 3370

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


class Controller:

    def __init__(self, tmp_path, *args, **kwargs):
        self._port = PORT
        self.server = []
        self.client = []
        self.tmp_path = tmp_path

    async def node(self, *args, **kwargs):
        return await self._start(
            *args, node=True, registry=False, provider=False, explorer=False,
            **kwargs)

    async def registry(self, *args, **kwargs):
        return await self._start(
            *args, node=False, registry=True, provider=False, explorer=False,
            **kwargs)

    async def provider(self, *args, **kwargs):
        return await self._start(
            *args, node=False, registry=True, provider=True, explorer=False,
            **kwargs)

    async def _start(self, node, registry, provider, explorer, loader=None,
                     connect=None, debug=True, organization=None, **kwargs):
        port = self._port
        cache_file = kwargs.get("cache", self.tmp_path / f"port_{port}.pkl")
        self._port += 1
        proc = Process(
            target=_create_service,
            kwargs=dict(
                port=port,
                connect=connect,
                node=node,
                registry=registry,
                provider=provider,
                explorer=explorer,
                loader=loader,
                organization=organization,
                cache=str(cache_file),
                debug=debug
            )
        )
        self.server.append(proc)
        proc.start()
        backoff = helper.Backoff()
        while proc.is_alive():
            try:
                client = httpx.AsyncClient(base_url=f"http://localhost:{port}")
                response = await client.get(
                    "/", timeout=setting.REQUEST_TIMEOUT)
                if response.status_code == 200:
                    self.client.append(client)
                    return client
            except:
                pass
            await backoff.wait()
        if proc.exception:
            raise proc.exception[0].__class__(proc.exception[0].args)
        raise RuntimeError("unexpected error")

    def close(self):
        while self.server:
            server = self.server.pop()
            server.terminate()
            server.join()

    async def _send(self, meth, server, url, status_code, **kwargs):
        while True:
            try:
                call = getattr(server, meth)
                resp = await call(url, **kwargs)
            except:
                pass
            else:
                assert resp.status_code == status_code
                js = resp.json()
                return js
            await asyncio.sleep(0.5)

    async def get(self, server, url, status_code, **kwargs):
        return await self._send("get", server, url, status_code, params=kwargs)

    async def post(self, server, url, status_code, **kwargs):
        return await self._send("post", server, url, status_code, **kwargs)

    async def _idle(self, *clients):
        await asyncio.sleep(0.5)
        if not clients:
            clients = self.client
        is_idle = True
        for client in clients:
            js = await self.get(client, "/", 200)
            if not js["idle"]:
                is_idle = False
                break
        return is_idle

    async def idle(self, *clients):
        while True:
            if await self._idle(*clients):
                break


@pytest.fixture
def controller(tmp_path):
    cntrl = Controller(tmp_path)
    yield cntrl
    cntrl.close()


def test_url():
    server = urllib.parse.urlparse("http://localhost:3366")
    assert server.hostname == "localhost"
    assert server.port == 3366
    assert server.scheme == "http"


async def test_start_service(controller):
    node = await controller.node()
    js = await controller.get(node, "/", 200)
    assert js["url"] == 'http://localhost:3370'
    assert js["node"] is True
    assert js["registry"] is False
    assert js["provider"] is False
    assert js["connection"] == {}
    assert js["started_at"] < js["now"]

    node = await controller.registry()
    js = await controller.get(node, "/", 200)
    assert js["url"] == 'http://localhost:3371'
    assert js["node"] is False
    assert js["registry"] is True
    assert js["provider"] is False
    assert js["connection"] == {}
    assert js["started_at"] < js["now"]

    node = await controller.provider()
    js = await controller.get(node, "/", 200)
    assert js["url"] == 'http://localhost:3372'
    assert js["node"] is False
    assert js["registry"] is True
    assert js["provider"] is True
    assert js["connection"] == {}
    assert js["started_at"] < js["now"]


# @pytest.mark.timeout(5)


async def test_connect_registry(controller):

    registry = await controller.registry()
    await controller.get(registry, "/", 200)
    node = await controller.node(connect=[registry.base_url])
    while True:
        js = await controller.get(node, "/", 200)
        ts = js["connection"].get("http://localhost:3370", None)
        if ts is not None:
            break


async def test_connect_provider(controller):

    registry = await controller.provider()
    await controller.get(registry, "/", 200)
    node = await controller.node(connect=[registry.base_url])
    while True:
        js = await controller.get(node, "/", 200)
        ts = js["connection"].get("http://localhost:3370", None)
        if ts is not None:
            break


async def test_connect_node(controller):
    node = await controller.node()
    await controller.get(node, "/", 200)
    await controller.post(node, "/connect", 404)
    await controller.post(node, "/register", 404)


async def test_connect(controller):
    registry1 = await controller.provider()
    resp = await controller.get(registry1, "/", 200)
    registry2 = await controller.provider()
    resp = await controller.get(registry2, "/", 200)
    connect = [registry1.base_url, registry2.base_url]
    node = await controller.node(connect=connect)
    for url in connect:
        while True:
            js = await controller.get(node, "/", 200)
            ts1 = js["connection"].get(registry1.base_url, None)
            ts2 = js["connection"].get(registry2.base_url, None)
            if ts1 is not None and ts2 is not None:
                break


async def test_connect_many(controller):
    registry1 = await controller.provider()
    await controller.get(registry1, "/", 200)
    registry2 = await controller.provider()
    await controller.get(registry2, "/", 200)
    nodes = []
    connect = [registry1.base_url, registry2.base_url]
    for i in range(10):
        node = await controller.node(connect=connect)
        nodes.append(node)
    for node in nodes:
        js = await controller.get(node, "/", 200)
        ts1 = js["connection"].get(registry1.base_url, None)
        ts2 = js["connection"].get(registry2.base_url, None)
        assert ts1 is not None and ts2 is not None


def loader1(state: State=None) -> kodo.worker.loader.Loader:
    loader = kodo.worker.loader.Loader()
    return loader


async def test_loader(controller):
    registry1 = await controller.registry()
    await controller.get(registry1, "/", 200)
    node1 = await controller.node(loader="tests.test_service:loader1")
    await controller.get(node1, "/", 200)


async def test_no_loader(controller):
    registry1 = await controller.registry()
    await controller.get(registry1, "/", 200)
    with pytest.raises(AttributeError):
        node1 = await controller.node(loader="tests.test_service:loader_NF")


async def test_load_no_flows(controller):
    # registry and provider have no flows
    with pytest.raises(ValueError):
        registry = await controller.registry(loader="tests.test_service:loader3")
    with pytest.raises(ValueError):
        registry = await controller.provider(loader="tests.test_service:loader3")


empty = None


def loader3(*args, **kwargs) -> kodo.worker.loader.Loader:
    loader = kodo.worker.loader.Loader()
    for i in range(1, 4):
        loader.add_flow(
            name=f"Test {i}", 
            entry_point=f"tests.test_service:empty", 
            url=f"/test{i}"
        )
    return loader


async def test_flows_model():
    loader = loader3()
    flows = loader.flows_dict()
    node = kodo.types.Node(url="http://hello", flows=flows)


async def test_load_node(controller):
    registry = await controller.registry()
    node = await controller.node(
        connect=[registry.base_url],
        loader="tests.test_service:loader3")
    js = await controller.get(node, "/", 200)
    print(js)
    js = await controller.get(registry, "/", 200)
    print(js)


def loader4(*args, **kwargs) -> kodo.worker.loader.Loader:
    loader = kodo.worker.loader.Loader()
    for i in range(4, 7):
        loader.add_flow(
            name=f"Test {i}", 
            entry_point=f"tests.test_service:empty", 
            url=f"/test{i}"
        )
    return loader


async def test_node_register(controller):
    registry1 = await controller.registry()  # 3370
    registry2 = await controller.registry()  # 3371
    node1 = await controller.node(  # 3372
        connect=[registry1.base_url],
        loader="tests.test_service:loader3")
    node2 = await controller.node(  # 3373
        connect=[registry2.base_url],
        loader="tests.test_service:loader4")
    # await controller.idle()
    js = await controller.get(registry1, "/", 200)
    assert js["connection"] == {}
    js = await controller.get(registry2, "/", 200)
    assert js["connection"] == {}
    js = await controller.get(node1, "/", 200)
    assert js["connection"].get(registry1.base_url, None) is not None
    assert js["connection"].get(registry2.base_url, None) is None
    js = await controller.get(node2, "/", 200)
    assert js["connection"].get(registry1.base_url, None) is None
    assert js["connection"].get(registry2.base_url, None) is not None
    js = await controller.get(registry1, "/flows", 200)
    assert [f["name"] for f in js["items"]] == ['Test 1', 'Test 2', 'Test 3']
    js = await controller.get(registry2, "/flows", 200)
    assert [f["name"] for f in js["items"]] == ['Test 4', 'Test 5', 'Test 6']


def loader5(*args, **kwargs) -> kodo.worker.loader.Loader:
    loader = kodo.worker.loader.Loader()
    for i in range(7, 10):
        loader.add_flow(
            name=f"Test {i}", 
            entry_point=f"tests.test_service:empty", 
            url=f"/test{i}"
        )
    return loader


async def test_post_update(controller):
    registry0 = await controller.registry()
    provider = kodo.types.Provider(**{
        'url': 'http://localhost:3370', 
        'feed': True, 
        'nodes': {}, 
        'created': datetime.datetime(2024, 12, 7, 23, 19, 25, 92078), 
        'modified': datetime.datetime(2024, 12, 7, 23, 28, 35, 430829), 
        'heartbeat': datetime.datetime(2024, 12, 7, 23, 28, 35, 430830)})
    client = httpx.AsyncClient()
    kwargs = {
        "data": provider.model_dump_json()
    }
    response = await client.post(
        "http://localhost:3370/update", timeout=setting.REQUEST_TIMEOUT,
        **kwargs)
    print(response)


def one_flow(*args, **kwargs) -> kodo.worker.loader.Loader:
    loader = kodo.worker.loader.Loader()
    loader.add_flow(
        name=f"Test 1", 
        entry_point=f"tests.test_service:empty", 
        url=f"/test1"
    )
    return loader


async def test_registry_scenario0(controller):
    registryJ = await controller.registry()  # 3370
    registryH = await controller.registry()  # 3371
    registryI = await controller.registry(
        connect=["http://localhost:3370", "http://localhost:3371"])  # 3372
    # await controller.idle()
    registryK = await controller.registry(connect=[registryI.base_url])  # 3373

    nodeH01 = await controller.node(
        connect=[registryH.base_url],
        loader="tests.test_service:one_flow")

    await controller.idle()

    jsH = await controller.get(registryH, "/map", 200)
    jsI = await controller.get(registryI, "/map", 200)
    jsJ = await controller.get(registryJ, "/map", 200)
    jsK = await controller.get(registryK, "/map", 200)

    assert list(jsH["providers"]) == [registryI.base_url]
    assert sorted(list(jsI["providers"])) == sorted(
        [str(i) for i in [registryH.base_url, registryJ.base_url, registryK.base_url]])
    assert list(jsJ["providers"]) == [registryI.base_url]
    assert list(jsK["providers"]) == [registryI.base_url]

    flowsH = await controller.get(registryH, "/flows", 200)
    flowsI = await controller.get(registryI, "/flows", 200)
    flowsJ = await controller.get(registryJ, "/flows", 200)
    flowsK = await controller.get(registryK, "/flows", 200)

    assert pd.DataFrame(flowsH["items"]).registry_url.tolist() == [str(registryH.base_url)]
    assert pd.DataFrame(flowsI["items"]).registry_url.tolist() == [str(registryI.base_url)]
    assert pd.DataFrame(flowsJ["items"]).registry_url.tolist() == [str(registryJ.base_url)]
    assert pd.DataFrame(flowsK["items"]).registry_url.tolist() == [str(registryK.base_url)]

    for js in (flowsI["items"], flowsJ["items"], flowsK["items"]):
        assert pd.DataFrame(flowsH["items"]).drop(columns="registry_url").equals(
            pd.DataFrame(js).drop(columns="registry_url"))

async def test_registry_scenario(controller):
    registryI = await controller.registry(
        connect=["http://localhost:3371", "http://localhost:3372"])  # 3370
    registryJ = await controller.registry()  # 3371
    registryH = await controller.registry()  # 3372
    # await controller.idle()
    registryK = await controller.registry(
        connect=[registryI.base_url])  # 3373

    nodeH01 = await controller.node(
        connect=[registryH.base_url],
        loader="tests.test_service:one_flow")

    await controller.idle()

    jsH = await controller.get(registryH, "/map", 200)
    jsI = await controller.get(registryI, "/map", 200)
    jsJ = await controller.get(registryJ, "/map", 200)
    jsK = await controller.get(registryK, "/map", 200)

    assert list(jsH["providers"]) == [registryI.base_url]
    assert sorted(list(jsI["providers"])) == sorted(
        [str(i) for i in [registryH.base_url, registryJ.base_url, registryK.base_url]])
    assert list(jsJ["providers"]) == [registryI.base_url]
    assert list(jsK["providers"]) == [registryI.base_url]

    flowsH = await controller.get(registryH, "/flows", 200)
    flowsI = await controller.get(registryI, "/flows", 200)
    flowsJ = await controller.get(registryJ, "/flows", 200)
    flowsK = await controller.get(registryK, "/flows", 200)

    assert pd.DataFrame(flowsH["items"]).registry_url.tolist() == [str(registryH.base_url)]
    assert pd.DataFrame(flowsI["items"]).registry_url.tolist() == [str(registryI.base_url)]
    assert pd.DataFrame(flowsJ["items"]).registry_url.tolist() == [str(registryJ.base_url)]
    assert pd.DataFrame(flowsK["items"]).registry_url.tolist() == [str(registryK.base_url)]

    for js in (flowsI["items"], flowsJ["items"], flowsK["items"]):
        assert pd.DataFrame(flowsH["items"]).drop(columns="registry_url").equals(
            pd.DataFrame(js).drop(columns="registry_url"))

    nodeI02 = await controller.node(
        connect=[registryI.base_url],
        loader="tests.test_service:loader3")  # 3375

    await controller.idle()

    flowsH = await controller.get(registryH, "/flows", 200)
    flowsI = await controller.get(registryI, "/flows", 200)
    flowsJ = await controller.get(registryJ, "/flows", 200)
    flowsK = await controller.get(registryK, "/flows", 200)
    for js in (flowsI["items"], flowsJ["items"], flowsK["items"]):
        assert pd.DataFrame(flowsH["items"]).drop(columns="registry_url").equals(
            pd.DataFrame(js).drop(columns="registry_url"))

    nodeHI = await controller.node(
        connect=[registryJ.base_url, registryK.base_url],
        loader="tests.test_service:loader4")

    await controller.idle()

    flowsH = await controller.get(registryH, "/flows", 200)
    flowsI = await controller.get(registryI, "/flows", 200)
    flowsJ = await controller.get(registryJ, "/flows", 200)
    flowsK = await controller.get(registryK, "/flows", 200)
    pd.DataFrame(flowsK["items"]).drop(columns="registry_url").sort_values(["node_url"]).equals(
        pd.DataFrame(flowsK["items"]).drop(columns="registry_url").sort_values(["node_url"]))

    cmp = pd.DataFrame(flowsH["items"])[["node_url", "name", "url"]]
    cmp.sort_values(["node_url"], inplace=True)
    for js in (flowsI["items"], flowsJ["items"], flowsK["items"]):
        df = pd.DataFrame(js)[["node_url", "name", "url"]]
        df.sort_values(["node_url"], inplace=True)
        assert cmp.equals(df)


def loader6(*args, **kwargs) -> kodo.worker.loader.Loader:
    loader = kodo.worker.loader.Loader()
    for i in range(10, 13):
        loader.add_flow(
            name=f"Test {i}", 
            entry_point=f"tests.test_service:empty", 
            url=f"/test{i}"
        )
    return loader


async def test_registry_scenario2(controller):
    await test_registry_scenario(controller)
    registryI, registryJ, registryH, registryK = controller.client[0:4]
    nodeI03 = await controller.node(connect=[registryI.base_url],
                                    loader="tests.test_service:loader5")
    flowsH = await controller.get(registryH, "/flows", 200)
    flowsI = await controller.get(registryI, "/flows", 200)
    flowsJ = await controller.get(registryJ, "/flows", 200)
    flowsK = await controller.get(registryK, "/flows", 200)
    flowsH = flowsH["items"]
    flowsI = flowsI["items"]
    flowsJ = flowsJ["items"]
    flowsK = flowsK["items"]
    pd.DataFrame(flowsK).drop(columns="registry_url").sort_values(["node_url"]).equals(
        pd.DataFrame(flowsK).drop(columns="registry_url").sort_values(["node_url"]))
    cmp = pd.DataFrame(flowsH)[["node_url", "name", "url"]]
    cmp.sort_values(["node_url"], inplace=True)
    for js in (flowsI, flowsJ, flowsK):
        df = pd.DataFrame(js)[["node_url", "name", "url"]]
        df.sort_values(["node_url"], inplace=True)
        assert cmp.equals(df)
    registryL = await controller.registry(
        # , "http://localhost:3373"]) # 3370
        connect=["http://localhost:3372"])

    nodeL04 = await controller.node(connect=[registryL.base_url],
                                    loader="tests.test_service:loader6")
    await controller.idle()

    flowsL = await controller.get(registryL, "/flows", 200)
    flowsL = flowsL["items"]
    dfL = pd.DataFrame(flowsL).sort_values(["node_url"])
    dfL = dfL[["node_url", "name", "url"]]
    dfL.sort_values(["node_url"], inplace=True)

    flowsH = await controller.get(registryH, "/flows", 200)
    flowsH = flowsH["items"]
    cmp = pd.DataFrame(flowsH)[["node_url", "name", "url"]]
    cmp.reset_index(drop=True).equals(dfL.reset_index(drop=True))


async def test_flow_visit(controller):
    node = await controller.node(
        connect=[],
        loader="tests.test_service:loader3")
    await controller.idle()
    for i in range(1, 4):
        resp = await controller.get(node, f"/flow/test{i}", 200)
    resp = await controller.get(node, "/flow/test4", 404)

# def random_flow(n: int=5):
#     for i in range(n):
#         loader.add_flow(
#             name=f"Test {i}", 
#             entry_point=f"tests.test_service:empty", 
#             url=f"/test{i}"
#         )
#     return loader


from tests.assets.agent50 import data as test_data

def _load_prop(start, end):
    worker = kodo.worker.loader.Loader()
    for rec in test_data[start:end]:
        rec["entry_point"] ="tests.test_service:empty"
        worker.add_flow(**rec)
    return worker


def test_load_prop():
    data1 = _load_prop(0, 8)
    data2 = _load_prop(8, 20)
    data3 = _load_prop(20, 32)
    data4 = _load_prop(32, 39)
    data5 = _load_prop(39, 50)


def prop1(*args, **kwargs):
    return _load_prop(0, 8)


def prop2(*args, **kwargs):
    return _load_prop(8, 20)


def prop3(*args, **kwargs):
    return _load_prop(20, 32)


def prop4(*args, **kwargs):
    return _load_prop(32, 39)


def prop5(*args, **kwargs):
    return _load_prop(39, 50)


async def test_registry_props(controller):
    registry1 = await controller.registry(organization="Serviceplan")
    node1 = await controller.node(
        connect=[registry1.base_url],
        organization="Serviceplan",
        loader="tests.test_service:prop1")
    node2 = await controller.node(
        connect=[registry1.base_url],
        organization="Serviceplan HR",
        loader="tests.test_service:prop2")
    node3 = await controller.node(
        connect=[registry1.base_url],
        organization="Serviceplan PR",
        loader="tests.test_service:prop3")
    registry2 = await controller.registry(
        organization="Mediaplus", connect=[registry1.base_url])
    node4 = await controller.node(
        connect=[registry2.base_url],
        organization="Mediaplus",
        loader="tests.test_service:prop4")
    node5 = await controller.node(
        connect=[registry2.base_url],
        organization="MP Research",
        loader="tests.test_service:prop5")
    await controller.idle()
    js1 = await controller.get(registry1, "/map", 200)
    # json.dump(js1, open("js1-map.json", "w"), indent=2)
    js2 = await controller.get(registry2, "/map", 200)    
    # json.dump(js2, open("js2-map.json", "w"), indent=2)
    js1 = await controller.get(registry1, "/flows", 200)
    # json.dump(js1, open("js1-flows.json", "w"), indent=2)
    js2 = await controller.get(registry2, "/flows", 200)        
    # json.dump(js1, open("js2-flows.json", "w"), indent=2)
    df1 = pd.DataFrame(js1["items"])
    df2 = pd.DataFrame(js2["items"])    
    cols = ["node_url", "name", "url", "author", "description", "tags", 
            "created", "modified", "heartbeat"]
    assert df1[cols].reset_index(drop=True).equals(
        df2[cols].reset_index(drop=True))
    return registry1


async def test_registry_cache(controller, tmp_path):
    registry1 = await test_registry_props(controller)
    registry3 = await controller.registry(
        organization="Plan.Net Journey", connect=[registry1.base_url])
    js3 = await controller.get(registry3, "/map", 200)        
    # json.dump(js3, open("js3-maps.json", "w"), indent=2)
    js3 = await controller.get(registry3, "/flows", 200)
    df3 = pd.DataFrame(js3["items"])
    js1 = await controller.get(registry1, "/flows", 200)
    df1 = pd.DataFrame(js1["items"])
    assert df1.drop(columns=["registry_url"]).reset_index(drop=True).equals(
        df3.drop(columns=["registry_url"]).reset_index(drop=True))
    # hack to cache file:
    cache = (
        controller.tmp_path 
        / f"port_{str(registry3.base_url).split(":")[2]}.pkl")
    registry4 = await controller.registry(
        organization="Plan.Net Journey", connect=[registry1.base_url],
        cache=str(cache))
    js4 = await controller.get(registry4, "/flows", 200)
    df4 = pd.DataFrame(js4["items"])
    df4.drop(columns=["registry_url"]).reset_index(drop=True).equals(
        df1.drop(columns=["registry_url"]).reset_index(drop=True))   
    data = kodo.types.ProviderDump.model_validate_json(
        Path(cache).open("r").read())
    Path(f"{setting.CACHE_DATA}.test").open("w").write(
        data.model_dump_json(indent=2))