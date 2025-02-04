from litestar.middleware import (
    AbstractAuthenticationMiddleware,
    AuthenticationResult,
)
from litestar.connection import ASGIConnection
from .jwt import JWKS



class JWTAuthMiddleware(AbstractAuthenticationMiddleware):
    def __init__(self, jwks: JWKS):
        self.jwks = jwks

    async def authenticate_request(
        self, connection: ASGIConnection
    ) -> AuthenticationResult:
        return AuthenticationResult(
            authenticated=True,
            user={"name": "John Doe", "email": "john.doe@domain.eu"},
        )
    

            

