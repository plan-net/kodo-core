from litestar.middleware import (
    AbstractAuthenticationMiddleware,
    AuthenticationResult,
)
from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException
from .jwt import JWKS, validate_jwt
from kodo.datatypes import User
import os

from kodo.log import logger

ROLE_FLOWS="flows"
ROLE_REGISTRY="registry"

class RoleValidatorMiddleware(AbstractAuthenticationMiddleware):...
    

class JWTAuthMiddleware(AbstractAuthenticationMiddleware):
    jwks: str
    audience: str

    def map_claims_to_user(self, claims: dict) -> User:
        return User(**claims)

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



def jwt_middleware_factory(jwks_url, audience):
    """Class factory function to pass arguments to the JWTAuthMiddleware\n
    Litestar docs suggest a different way to pass arguments to custom middleware:
    https://docs.litestar.dev/2/usage/middleware/creating-middleware.html#using-definemiddleware-to-pass-arguments
    but it does not seem to apply to AuthMiddleware"""
    jwks = JWKS(jwks_url)
    # We do not expect those params to change, so let's set them as class attributes
    JWTAuthMiddleware.jwks = jwks
    JWTAuthMiddleware.audience = audience
    return JWTAuthMiddleware
