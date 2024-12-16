# kodo-core

## kodosumi core package

The kodosumi framework is under development.pip 

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
