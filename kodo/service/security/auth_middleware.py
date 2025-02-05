from litestar.middleware import (
    AbstractAuthenticationMiddleware,
    AuthenticationResult,
)
from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException
from .jwt import JWKS, validate_jwt
import os

jwks_url = os.getenv("KODO_JWKS_URL")

if jwks_url:
    jwks = JWKS(jwks_url)


class JWTAuthMiddleware(AbstractAuthenticationMiddleware):
    async def authenticate_request(
        self, connection: ASGIConnection
    ) -> AuthenticationResult:
        if not jwks_url:
            return AuthenticationResult(
                auth={"sub": "1234567890", "name": "John Doe"},
                user={"name": "John Doe", "email": "john.doe@domain.eu"},
            )

        auth_header = connection.headers.get("Authorization")
        if not auth_header:
            raise NotAuthorizedException()

        # decode the token, the result is a ``Token`` model instance
        token = validate_jwt(auth_header.split(" ")[1], jwks)
        return AuthenticationResult(
            auth=token,
            user={"name": "John Doe", "email": "john.doe@domain.eu"},
        )
