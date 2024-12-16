import asyncio
import datetime
import multiprocessing
import urllib

import httpx
import pandas as pd
import pytest

import kodo.service.node
import kodo.types
from kodo import helper
from kodo.config import setting


def _create_service(
        port, connect, node, registry, provider, explorer, loader, debug):
    kodo.service.node.run_service(
        url=f"http://localhost:{port}",
        connect=connect,
        node=node,
        registry=registry,
        provider=provider,
        explorer=explorer,
        loader=loader,
        reload=False,
        debug=debug)
    print("OK")


PORT = 3370


class Controller:

    def __init__(self, *args, **kwargs):
        self._port = PORT
        self.server = []
        self.client = []

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
                     connect=None, debug=True, **kwargs):
        port = self._port
        self._port += 1

        proc = multiprocessing.Process(
            target=_create_service,
            kwargs=dict(
                port=port,
                connect=connect,
                node=node,
                registry=registry,
                provider=provider,
                explorer=explorer,
                loader=loader,
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
        raise RuntimeError("service did not start")

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
def controller():
    cntrl = Controller()
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


def loader1():
    return {
        "flows": None,
        "nodes": None,
        "providers": None
    }


async def test_loader(controller):
    registry1 = await controller.registry()
    await controller.get(registry1, "/", 200)
    node1 = await controller.node(loader="tests.test_service:loader1")
    await controller.get(node1, "/", 200)


async def test_no_loader(controller):
    registry1 = await controller.registry()
    await controller.get(registry1, "/", 200)
    with pytest.raises(RuntimeError):
        node1 = await controller.node(loader="tests.test_service:loader_NF")


def loader2():
    return {
        "flows": [
            {"url": "/test1", "name": "Test 1"},
            {"url": "/test2", "name": "Test 2"},
            {"url": "/test3", "name": "Test 3"}
        ],
        "nodes": None,
        "providers": None
    }


async def test_load_no_flows(controller):
    # registry and provider have no flows
    with pytest.raises(RuntimeError):
        registry = await controller.registry(loader="tests.test_service:loader2")
    with pytest.raises(RuntimeError):
        registry = await controller.provider(loader="tests.test_service:loader2")


def loader3():
    return {
        "flows": [
            {"url": "/test1", "name": "Test 1"},
            {"url": "/test2", "name": "Test 2"},
            {"url": "/test3", "name": "Test 3"}
        ],
        "nodes": None,
        "providers": None
    }


async def test_flows_model():
    flows = {f["name"]: kodo.types.Flow(**f) for f in loader3()["flows"]}
    node = kodo.types.Node(url="http://hello", flows=flows)
    print(node)
    # flows = {f["name"]: kodo.types.Flow(**f) for f in loader3()["flows"]}
    # offer = kodo.types.Node(url="http://hello", flows=flows)
    # node = kodo.types.Node(**offer.dict(), created=helper.now(), modified=helper.now())
    # print(node)


async def test_load_node(controller):
    registry = await controller.registry()
    node = await controller.node(
        connect=[registry.base_url],
        loader="tests.test_service:loader3")
    js = await controller.get(node, "/", 200)
    print(js)
    js = await controller.get(registry, "/", 200)
    print(js)


def loader4():
    return {
        "flows": [
            {"url": "/test4", "name": "Test 4"},
            {"url": "/test5", "name": "Test 5"},
            {"url": "/test6", "name": "Test 6"}
        ],
        "nodes": None,
        "providers": None
    }


async def test_node_register(controller):
    registry1 = await controller.registry()
    registry2 = await controller.registry()
    node1 = await controller.node(
        connect=[registry1.base_url],
        loader="tests.test_service:loader3")
    node2 = await controller.node(
        connect=[registry2.base_url],
        loader="tests.test_service:loader4")
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
    assert list(js["name"].values()) == ['Test 1', 'Test 2', 'Test 3']
    js = await controller.get(registry2, "/flows", 200)
    assert list(js["name"].values()) == ['Test 4', 'Test 5', 'Test 6']


# def loader5():
#     return {
#         "flows": [{"url": f"/test{i}", "name": f"Test {i}"} for i in range(10)],
#         "nodes": None,
#         "providers": None
#     }

# @pytest.mark.skip
# async def test_registry_connect(controller):
#     registry1 = await controller.registry()
#     node = await controller.node(
#         connect=[registry1.base_url],
#         loader="tests.test_service:loader5")
#     registry2 = await controller.registry(connect=[registry1.base_url])
#     js1 = await controller.get(registry1, "/flows", 200)
#     assert len(pd.DataFrame(js1)) == 10
#     assert list(pd.DataFrame(js1).source.unique()) == [registry1.base_url]
#     js2 = await controller.get(registry2, "/flows", 200)
#     assert len(pd.DataFrame(js2)) == 10
#     assert list(pd.DataFrame(js2).source.unique()) == [registry2.base_url]


def loader5():
    return {
        "flows": [
            {"url": "/test7", "name": "Test 7"},
            {"url": "/test8", "name": "Test 8"},
            {"url": "/test9", "name": "Test 9"}
        ],
        "nodes": None,
        "providers": None
    }


@pytest.mark.skip
async def test_registry_connect2(controller):
    # registry1 with 2 nodes
    registry0 = await controller.registry()  # 3370
    js = await controller.get(registry0, "/", 200)
    node1 = await controller.node(  # 3371
        connect=[registry0.base_url],
        loader="tests.test_service:loader3")
    js = await controller.get(node1, "/", 200)
    node2 = await controller.node(  # 3372
        connect=[registry0.base_url],
        loader="tests.test_service:loader4")
    js = await controller.get(node2, "/", 200)
    js1 = await controller.get(registry0, "/flows", 200)
    df1 = pd.DataFrame(js1)
    assert sorted(list(df1.node.unique())) == sorted(
        [str(node1.base_url), str(node2.base_url)])
    # registry2 with 1 node
    registry3 = await controller.registry(connect=[registry0.base_url])  # 3373
    asyncio.sleep(1)
    js = await controller.get(registry3, "/", 200)
    node4 = await controller.node(  # 3374
        connect=[registry3.base_url],
        loader="tests.test_service:loader5")
    js = await controller.get(node4, "/", 200)
    js2 = await controller.get(registry3, "/flows", 200)
    df2 = pd.DataFrame(js2)
    assert sorted(list(df2.node.unique())) == sorted(
        [str(node1.base_url), str(node2.base_url), str(node4.base_url)])
    # registry3 without nodes
    registry5 = await controller.registry(connect=[registry3.base_url])  # 3375
    js = await controller.get(registry5, "/", 200)
    js3 = await controller.get(registry5, "/flows", 200)
    df3 = pd.DataFrame(js3)
    assert sorted(list(df3.node.unique())) == sorted(
        [str(node1.base_url), str(node2.base_url), str(node4.base_url)])
    assert df1.drop(columns=["source"]).equals(df2.drop(columns=["source"]))
    assert df2.drop(columns=["source"]).equals(df3.drop(columns=["source"]))


async def test_post_update(controller):
    registry0 = await controller.registry()
    provider = kodo.types.Provider(**{'url': 'http://localhost:3370', 'feed': True, 'nodes': {}, 'created': datetime.datetime(
        2024, 12, 7, 23, 19, 25, 92078), 'modified': datetime.datetime(2024, 12, 7, 23, 28, 35, 430829)})
    client = httpx.AsyncClient()
    kwargs = {
        "data": provider.model_dump_json()
    }
    response = await client.post(
        "http://localhost:3370/update", timeout=setting.REQUEST_TIMEOUT,
        **kwargs)
    print(response)


@pytest.mark.skip
async def test_registry_connect3(controller):
    # registry1 with 2 nodes
    # 3370
    registry0 = await controller.registry(connect=["http://localhost:3371"])
    js = await controller.get(registry0, "/", 200)
    assert not js["idle"]
    # 3371
    registry1 = await controller.registry(connect=["http://localhost:3370"])
    await controller.idle()


@pytest.mark.skip
async def test_registry_connect4(controller):
    # registry1 with 2 nodes
    registry0 = await controller.registry()  # 3370
    js = await controller.get(registry0, "/", 200)
    node01 = await controller.node(  # 3371
        connect=[registry0.base_url],
        loader="tests.test_service:loader3")
    await controller.idle()
    js = await controller.get(registry0, "/", 200)
    assert js["idle"]
    js0 = await controller.get(registry0, "/flows", 200)
    js1 = await controller.get(node01, "/flows", 200)
    df0 = pd.DataFrame(js0)
    df1 = pd.DataFrame(js1)
    cols = ["node", "name", "url"]
    assert df0[cols].equals(df1[cols])

    # registry1 connects to registry0 and synchronizes the flows
    registry1 = await controller.registry(connect=[registry0.base_url])  # 3372
    js = await controller.get(registry1, "/", 200)
    await controller.idle()
    js1 = await controller.get(registry1, "/flows", 200)
    df1 = pd.DataFrame(js1)
    assert df0[cols].equals(df1[cols])

    # another node connects to registry0
    node02 = await controller.node(  # 3371
        connect=[registry0.base_url],
        loader="tests.test_service:loader4")
    await controller.idle()
    js0 = await controller.get(registry0, "/flows", 200)
    df0 = pd.DataFrame(js0)
    js1 = await controller.get(registry1, "/flows", 200)
    df1 = pd.DataFrame(js1)

    print("OK")


def one_flow():
    return {
        "flows": [
            {"url": "/test1", "name": "Test 1"}
        ],
        "nodes": None,
        "providers": None
    }


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

    assert pd.DataFrame(flowsH).source.tolist() == [str(registryH.base_url)]
    assert pd.DataFrame(flowsI).source.tolist() == [str(registryI.base_url)]
    assert pd.DataFrame(flowsJ).source.tolist() == [str(registryJ.base_url)]
    assert pd.DataFrame(flowsK).source.tolist() == [str(registryK.base_url)]

    for js in (flowsI, flowsJ, flowsK):
        assert pd.DataFrame(flowsH).drop(columns="source").equals(
            pd.DataFrame(js).drop(columns="source"))


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

    assert pd.DataFrame(flowsH).source.tolist() == [str(registryH.base_url)]
    assert pd.DataFrame(flowsI).source.tolist() == [str(registryI.base_url)]
    assert pd.DataFrame(flowsJ).source.tolist() == [str(registryJ.base_url)]
    assert pd.DataFrame(flowsK).source.tolist() == [str(registryK.base_url)]

    for js in (flowsI, flowsJ, flowsK):
        assert pd.DataFrame(flowsH).drop(columns="source").equals(
            pd.DataFrame(js).drop(columns="source"))

    nodeI02 = await controller.node(
        connect=[registryI.base_url],
        loader="tests.test_service:loader3")  # 3375

    await controller.idle()

    flowsH = await controller.get(registryH, "/flows", 200)
    flowsI = await controller.get(registryI, "/flows", 200)
    flowsJ = await controller.get(registryJ, "/flows", 200)
    flowsK = await controller.get(registryK, "/flows", 200)
    for js in (flowsI, flowsJ, flowsK):
        assert pd.DataFrame(flowsH).drop(columns="source").equals(
            pd.DataFrame(js).drop(columns="source"))

    nodeHI = await controller.node(
        connect=[registryJ.base_url, registryK.base_url],
        loader="tests.test_service:loader4")

    await controller.idle()

    flowsH = await controller.get(registryH, "/flows", 200)
    flowsI = await controller.get(registryI, "/flows", 200)
    flowsJ = await controller.get(registryJ, "/flows", 200)
    flowsK = await controller.get(registryK, "/flows", 200)
    pd.DataFrame(flowsK).drop(columns="source").sort_values(["node"]).equals(
        pd.DataFrame(flowsK).drop(columns="source").sort_values(["node"]))

    cmp = pd.DataFrame(flowsH)[["node", "name", "url"]]
    cmp.sort_values(["node"], inplace=True)
    for js in (flowsI, flowsJ, flowsK):
        df = pd.DataFrame(js)[["node", "name", "url"]]
        df.sort_values(["node"], inplace=True)
        assert cmp.equals(df)


def loader6():
    return {
        "flows": [
            {"url": "/test10", "name": "Test 10"},
            {"url": "/test11", "name": "Test 11"},
            {"url": "/test12", "name": "Test 12"}
        ],
        "nodes": None,
        "providers": None
    }


async def test_registry_scenario2(controller):
    await test_registry_scenario(controller)
    registryI, registryJ, registryH, registryK = controller.client[0:4]
    nodeI03 = await controller.node(connect=[registryI.base_url],
                                    loader="tests.test_service:loader5")
    flowsH = await controller.get(registryH, "/flows", 200)
    flowsI = await controller.get(registryI, "/flows", 200)
    flowsJ = await controller.get(registryJ, "/flows", 200)
    flowsK = await controller.get(registryK, "/flows", 200)
    pd.DataFrame(flowsK).drop(columns="source").sort_values(["node"]).equals(
        pd.DataFrame(flowsK).drop(columns="source").sort_values(["node"]))
    cmp = pd.DataFrame(flowsH)[["node", "name", "url"]]
    cmp.sort_values(["node"], inplace=True)
    for js in (flowsI, flowsJ, flowsK):
        df = pd.DataFrame(js)[["node", "name", "url"]]
        df.sort_values(["node"], inplace=True)
        assert cmp.equals(df)
    registryL = await controller.registry(
        # , "http://localhost:3373"]) # 3370
        connect=["http://localhost:3372"])

    nodeL04 = await controller.node(connect=[registryL.base_url],
                                    loader="tests.test_service:loader6")
    await controller.idle()

    flowsL = await controller.get(registryL, "/flows", 200)
    dfL = pd.DataFrame(flowsL).sort_values(["node"])
    dfL = dfL[["node", "name", "url"]]
    dfL.sort_values(["node"], inplace=True)

    flowsH = await controller.get(registryH, "/flows", 200)
    cmp = pd.DataFrame(flowsH)[["node", "name", "url"]]
    cmp.reset_index(drop=True).equals(dfL.reset_index(drop=True))
