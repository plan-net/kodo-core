import pytest
import json
import tempfile
import jwt
import os
import threading
import requests

import httpx
import pandas as pd

from kodo.service.security import JWTAuthMiddleware, JWKS, validate_jwt
from kodo.service.node import run_service
from pytest_httpserver import HTTPServer, httpserver

from tests.shared import *


@pytest.fixture(scope="module")
def rsa_key_pair():
    with open("tests/assets/private.key.json", "rb") as f:
        private_key = json.load(f)
    with open("tests/assets/public.cert.json", "rb") as f:
        jwks_cert = json.load(f)
    return private_key, jwks_cert


@pytest.fixture(scope="function")
def auth_header(rsa_key_pair):
    KID = "12345"
    private_key, _ = rsa_key_pair
    token = jwt.encode(
        {"sub": "1234567890", "name": "John Doe", "aud": "kodo", "email": "john.doe@example.com",
         "resource_access": {"kodo": {"roles": ["registry"]}}},
        jwt.algorithms.RSAAlgorithm.from_jwk(private_key),
        algorithm="RS256",
        headers={"kid": KID},
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture(scope="module")
def jwks():
    with open("tests/assets/jwks_certs.json") as f:
        return json.load(f)


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


async def test_authenticated_query(auth_header):
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
    resp = httpx.get(f"{node.url}/flows", timeout=None, headers=auth_header)
    assert len(resp.json()["items"]) == 10
    assert resp.json()["total"] == 50

    p0 = httpx.get(f"{node.url}/flows?pp=15&p=0", headers=auth_header).json()
    df0 = pd.DataFrame(p0["items"])
    assert df0.shape[0] == 15
