import os
from typing import Any, Protocol

from litestar import Request
from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException, PermissionDeniedException
from litestar.middleware import (AbstractAuthenticationMiddleware,
                                 AuthenticationResult)
from litestar.types import ASGIApp, Receive, Scope, Send

from kodo.datatypes import User
from kodo.log import logger

from .jwt import JWKS, validate_jwt

ROLE_FLOWS = "flows"
ROLE_REGISTRY = "registry"


class RoleValidatorMiddleware(Protocol):
    def __init__(self, app: ASGIApp, allowed_roles : list[str]) -> None:
        self.app = app
        self.allowed_roles = allowed_roles

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            req = Request(scope)
            user : User = req.user
            if not user:
                raise NotAuthorizedException()
            has_role = False
            for role in user.roles:
                if role in self.allowed_roles:
                    has_role = True
                    break
            if not has_role:
                logger.debug(f"User {user.name} does not have the required role.")
                raise PermissionDeniedException()
        await self.app(scope, receive, send)


class JWTAuthMiddleware(AbstractAuthenticationMiddleware):
    jwks: str
    audience: str
    jwt_role_path: list[str]

    def map_claims_to_user(self, claims: dict) -> User: 
        dict_node = claims
        try:
            # Traverse the dict to get the roles
            for step in JWTAuthMiddleware.jwt_role_path:
                dict_node = dict_node[step]
            roles = dict_node
            u = User(**claims, roles=roles)
        except Exception as e:
            raise NotAuthorizedException()
        return u

    async def authenticate_request(
        self, connection: ASGIConnection
    ) -> AuthenticationResult:
        auth_header = connection.headers.get("Authorization")
        if not auth_header:
            raise NotAuthorizedException()

        auth_header = auth_header.split(" ")
        if len(auth_header) != 2 or auth_header[0].lower() != "bearer":
            raise NotAuthorizedException()

        token = validate_jwt(
            auth_header[1], JWTAuthMiddleware.audience, JWTAuthMiddleware.jwks
        )
        if not token:
            raise NotAuthorizedException()

        return AuthenticationResult(auth=token, user=self.map_claims_to_user(token))


def jwt_middleware_factory(state):
    """Class factory function to pass arguments to the JWTAuthMiddleware\n
    Litestar docs suggest a different way to pass arguments to custom middleware:
    https://docs.litestar.dev/2/usage/middleware/creating-middleware.html#using-definemiddleware-to-pass-arguments
    but it does not seem to apply to AuthMiddleware"""
    jwks = JWKS(state.auth_jwks_url)
    # We do not expect those params to change, so let's set them as class attributes
    JWTAuthMiddleware.jwks = jwks
    JWTAuthMiddleware.audience = state.auth_audience if "auth_audience" in state else state.url

    # Split and substitute the %AUD% token in the jwt path now to avoid doing it every time
    path = []
    for step in state.auth_jwt_role_path.split("/"):
        if step == "%AUD%":
            path.append(JWTAuthMiddleware.audience)
        else:
            path.append(step)
    JWTAuthMiddleware.jwt_role_path = path

    return JWTAuthMiddleware
