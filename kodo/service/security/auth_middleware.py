from litestar.middleware import (
    AbstractAuthenticationMiddleware,
    AuthenticationResult,
)
from litestar.connection import ASGIConnection
from litestar.exceptions import NotAuthorizedException
from .jwt import JWKS, validate_jwt



class JWTAuthMiddleware(AbstractAuthenticationMiddleware):
    def __init__(self, jwks: JWKS):
        self.jwks = jwks

    async def authenticate_request(self, connection: ASGIConnection) -> AuthenticationResult:
        
        auth_header = connection.headers.get("Authentication")
        if not auth_header:
            raise NotAuthorizedException()

        # decode the token, the result is a ``Token`` model instance
        token = validate_jwt(auth_header, self.jwks)
        return AuthenticationResult(
            authenticated=True,
            user={"name": "John Doe", "email": "john.doe@domain.eu"},
        )
    

            

