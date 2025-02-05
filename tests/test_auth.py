import pytest
import json
import tempfile
import jwt
import os

from kodo.service.security import JWTAuthMiddleware, JWKS, validate_jwt
from pytest_httpserver import HTTPServer, httpserver


@pytest.fixture(scope="module")
def rsa_key_pair():
    with open("tests/assets/private.key.json", "rb") as f:
        private_key = json.load(f)
    with open("tests/assets/public.cert.json", "rb") as f:
        jwks_cert = json.load(f)
    return private_key, jwks_cert


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
    private_key, public_key = rsa_key_pair
    jwks = JWKS("file://" + os.path.abspath("tests/assets/public.cert.json"))
    token = jwt.encode(
        {"sub": "1234567890", "name": "John Doe"},
        jwt.algorithms.RSAAlgorithm.from_jwk(private_key),
        algorithm="RS256",
        headers={"kid": KID},
    )
    decoded = validate_jwt(token, jwks)
    assert decoded["sub"] == "1234567890"
    assert decoded["name"] == "John Doe"
