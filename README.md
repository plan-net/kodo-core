# kodo-core

## kodosumi core package

The kodosumi framework is under development.

### kodosumi api documentation

The Swagger UI is reachable via the /docs endpoint

### kodosumi api documentation

The Swagger UI is reachable via the /docs endpoint

## Setup

    cd ~
    mkdir Project
    cd Project
    git clone https://github.com/plan-net/kodo-core.git
    cd kodo-core
    git checkout develop
    python3 -m venv .venv  # this might be different depending on your OS Python installation

    # on macos:
    # ~~~~~~~~~~~
    source .venv/bin/activate
    pip install -e .
    touch .env
    code .env

    # on windows:
    # ~~~~~~~~~~~
    .venv\Scripts\activate
    pip install -e .
    type nul > .env
    code .env

Open up file `~/Project/kodo-core/.env` and define environment variables:

    OPENAI_API_KEY=sk-proj-...XYZ
    OTEL_SDK_DISABLED=true
    LITELLM_LOG=WARNING

Run tests after `pip install .[tests]` with `pytest -v`.

For examples see folder `tests`. You need to install _jupyter_ with 
`pip install jupyter` to run the examples.

## Security
### Authentication
Security is disabled unless `KODO_AUTH_JWKS_URL` environmental variable is set.

Authentication is based on JWT with asymetric cypher (RSA : RS256, RS384, RS512). Signature is verified against public key which are provided in JWKS format. The location of JKWS file is indicated in `KODO_AUTH_JWKS_URL`. Supported schemas are "http", "https, and "file".

The auth middleware is veryfing the `aud` field of an JWT token. Field value may be passed as `KODO_AUTH_AUDIENCE` environmental variable. If it is missing the middleware will default to "url" (whatever the form it was passed in).

### Roles and authorization
Currently there are 2 roles required by certain endpoints:
- "registry"
- "flows"

Roles have to come in the jwt token. The exact location of the list of roles in jwt token can be provided by  environmental variable. The default may be found in [kodo/datatypes.py](kodo/datatypes.py)

The substring`%AUD%` will be substitured for the same value as the expected `aud`. The example below shows us what claims should be present in jwt token if the `AUTH_JWT_ROLE_PATH` is set to `resource_access/%AUD%/roles`and `KODO_AUTH_AUDIENCE` to `node-registry`:
```json
  ...
  "aud" : "node-registry",
  ...
  "resource_access": {
    "node-registry": {
      "roles": [
        "registry",
        "flows"
      ]
    }
  },
  ...
```
If the `KODO_AUTH_AUDIENCE` is not set the "url" will be used.