import requests
import jwt
from jwt import PyJWTError
from litestar.exceptions import NotAuthorizedException

from urllib.parse import urlparse
import json


class JWKS:
    def __init__(self, jwks_url: str):
        try:
            p = urlparse(jwks_url)
            if p.scheme in ["http", "https"]:
                self.key_list = requests.get(jwks_url).json()["keys"]
            elif p.scheme == "file":
                with open(p.path) as f:
                    self.key_list = json.load(f)["keys"]
            else:
                raise ValueError("Invalid URL scheme")
        except Exception as e:
            print(f"Error reading JWKS: {e}")
            raise

    def key_for_id(self, kid : str) -> any:
        for key in self.key_list:
            if key['kid'] == kid:
                return jwt.algorithms.RSAAlgorithm.from_jwk(key)

# Function to validate the JWT token
def validate_jwt(token):
    try:
        # Decode the JWT token to extract the header
        unverified_header = jwt.get_unverified_header(token)
        if unverified_header is None:
            raise ValueError("Token is missing a header")

        # Get the public key using the kid from the JWT header
        public_key = get_public_key(unverified_header['kid'])

        # Decode and verify the JWT token using the public key
        decoded_token = jwt.decode(
            token,
            public_key,
            algorithms=['RS256'],
            audience=CLIENT_ID,  # Check that the audience is correct
            issuer=f'{KEYCLOAK_SERVER}/realms/{REALM}'  # Check that the issuer is correct
        )

        print("Token is valid")
        print("Decoded token:", decoded_token)
        return decoded_token

    except PyJWTError as e:
        print(f"JWT validation failed: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None