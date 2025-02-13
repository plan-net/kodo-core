import json
import os
import tempfile
import threading

import httpx
import jwt
import pandas as pd
import pytest
import requests
from pytest_httpserver import HTTPServer, httpserver

from kodo.service.node import run_service
from kodo.service.security import JWKS, JWTAuthMiddleware, validate_jwt
from tests.shared import *


def _rsa_key_pair():
    with open("tests/assets/private.key.json", "rb") as f:
        private_key = json.load(f)
    with open("tests/assets/public.cert.json", "rb") as f:
        jwks_cert = json.load(f)
    return private_key, jwks_cert


@pytest.fixture(scope="module")
def rsa_key_pair():
    return _rsa_key_pair()


@pytest.fixture(scope="module")
def jwks():
    with open("tests/assets/jwks_certs.json") as f:
        return json.load(f)


def create_auth_header(roles=["registry"]):
    KID = "12345"
    private_key, _ = _rsa_key_pair()
    token = jwt.encode(
        {"sub": "1234567890", "name": "John Doe", "aud": "kodo", "email": "john.doe@example.com",
         "resource_access": {"kodo": {"roles": roles}}},
        jwt.algorithms.RSAAlgorithm.from_jwk(private_key),
        algorithm="RS256",
        headers={"kid": KID},
    )
    return {"Authorization": f"Bearer {token}"}


def test_jwks_fetch_wrong_scheme():
    with pytest.raises(ValueError):
        jwks = JWKS("ftp://example.com/jwks.json")


def test_jwks_fetch_http(httpserver: HTTPServer, jwks):
    httpserver.expect_request("/foobar").respond_with_json(jwks)
    jwks = JWKS(httpserver.url_for("/foobar"))
    assert len(jwks.keys)


def test_jwks_fetch_file(jwks):
    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(jwks, f)
        f.seek(0)
        jwks = JWKS("file://" + f.name)
        assert len(jwks.keys)


def test_validate_jwt(rsa_key_pair, jwks):
    KID = "12345"
    private_key, _ = rsa_key_pair
    jwks = JWKS("file://" + os.path.abspath("tests/assets/public.cert.json"))
    token = jwt.encode(
        {"sub": "1234567890", "name": "John Doe", "aud": "kodo",},
        jwt.algorithms.RSAAlgorithm.from_jwk(private_key),
        algorithm="RS256",
        headers={"kid": KID},
    )
    decoded = validate_jwt(token, "kodo", jwks)
    assert decoded["sub"] == "1234567890"
    assert decoded["name"] == "John Doe"


async def test_authenticated_query():
    node = Service(
        url="http://localhost:3370",
        organization="node",
        registry=True,
        feed=True,
        loader="tests.test_node:loader4",
        auth_jwks_url="file://" + os.path.abspath("tests/assets/public.cert.json"),
        auth_audience="kodo",
    )
    node.start()
    node.wait()
    auth_header = create_auth_header()
    resp = httpx.get(f"{node.url}/flows?pp=10", 
                     timeout=None, headers=auth_header)
    assert len(resp.json()["items"]) == 10
    assert resp.json()["total"] == 50
    assert resp.status_code == 200

    p0 = httpx.get(f"{node.url}/flows?pp=15&p=0", headers=auth_header).json()
    df0 = pd.DataFrame(p0["items"])
    assert df0.shape[0] == 15


async def test_wrong_audience():
    node = Service(
        url="http://localhost:3370",
        organization="node",
        registry=True,
        feed=True,
        loader="tests.test_node:loader4",
        auth_jwks_url="file://" + os.path.abspath("tests/assets/public.cert.json"),
        auth_audience="kodo2",
    )
    node.start()
    node.wait()
    auth_header = create_auth_header()
    resp = httpx.get(f"{node.url}/flows", timeout=None, headers=auth_header)
    assert resp.status_code == 401


async def test_wrong_roles():
    node = Service(
        url="http://localhost:3370",
        organization="node",
        registry=True,
        feed=True,
        loader="tests.test_node:loader4",
        auth_jwks_url="file://" + os.path.abspath("tests/assets/public.cert.json"),
        auth_audience="kodo",
    )
    node.start()
    node.wait()
    auth_header = create_auth_header(roles=["stapler"])
    resp = httpx.get(f"{node.url}/flows", timeout=None, headers=auth_header)
    assert resp.status_code == 403
