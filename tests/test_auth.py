import pytest
import json
import tempfile

from kodo.service.security import JWTAuthMiddleware, JWKS
from pytest_httpserver import HTTPServer, httpserver


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
    assert len(jwks.key_list)

def test_jwks_fetch_file(jwks):
    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(jwks, f)
        f.seek(0)
        jwks = JWKS("file://" + f.name)
        assert len(jwks.key_list)
