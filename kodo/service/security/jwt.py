import json
from urllib.parse import urlparse

import jwt
import requests
from jwt import PyJWTError

from kodo.log import logger


class JWKS:
    def __init__(self, jwks_url: str):
        """Accepts a URL to a JWKS endpoint or a file containing JWKS data"""
        try:
            p = urlparse(jwks_url)
            if p.scheme in ["http", "https"]:
                self.keys = self._ltd(requests.get(jwks_url).json()["keys"])
            elif p.scheme == "file":
                with open(p.path) as f:
                    self.keys = self._ltd(json.load(f)["keys"])
            else:
                raise ValueError("Invalid URL scheme")
        except Exception as e:
            logger.error(f"Error reading JWKS: {e}")
            raise

    def _ltd(self, key_list):
        return {
            key["kid"]: jwt.algorithms.RSAAlgorithm.from_jwk(key) for key in key_list
        }

    def __getitem__(self, index):
        return self.keys[index]


# Function to validate the JWT token
def validate_jwt(token, audience : str, jwks: JWKS):
    try:
        # Decode the JWT token to extract the header
        unverified_header = jwt.get_unverified_header(token)
        if unverified_header is None:
            raise ValueError("Token is missing a header")

        # Get the public key using the kid from the JWT header
        public_key = jwks[unverified_header["kid"]]

        # Decode and verify the JWT token using the public key
        decoded_token = jwt.decode(
            token,
            public_key,
            algorithms=["RS256", "RS384", "RS512"],
            audience=audience
        )
        return decoded_token

    except PyJWTError as e:
        return None
    except Exception as e:
        return None
