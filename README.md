# kodo-core

## kodosumi core package

The kodosumi framework is under development.pip 

## installation of preliminary version

> supports crewai based on yaml at the moment

### prerequisites

1. Access [`kodo-core` at GitHub](https://github.com/plan-net/kodo-core/tree/cli).
2. Python minimum version 3.9, [download and install from there](https://www.python.org/downloads/).
3. Make sure **Git** is installed on your computer, [download and install from here](https://git-scm.com/downloads).
4. A text/code editor, i.e. [Visual Code for mac+win](https://code.visualstudio.com/download).

### Setup

Clone **kodosumi**, checkout branch `cli`, install and setup your environment. Open up a terminal and type:

    cd ~
    mkdir Project
    cd Project
    git clone https://github.com/plan-net/kodo-core.git
    cd kodo-core
    git checkout cli
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
    VAULT=/Users/raum/Project/pyrob4/bib/mrau

Then `check`, `run` and `render` a flow with

    koco check ./tests/assets/test.yaml
    koco run ./tests/assets/test.yaml
    koco render --latest

You can write the output to a file

    koco render --latest --target output.md

And overwrite the file if it exists

    koco render --latest --target output.md --force

You can change output format to HTML and directly open the `--target` file:

    koco render --latest --format html --target output.html --open

You can even open it with your preferred obsidian vault given 

1. the environment variable `VAULT` contains the absolute path to your vault, 
2. the vault name is equal to the last leaf of the vault `VAULT` path and 
3. the `--target` can be created inside your vault, 

For example if `VAULT=/Users/raum/Project/pyrob4/bib/mrau` in `.env`, then the following command opens Obsidian.

    koco render --latest --format obsidian --open --target Untitled.md

Of cause you can request rendering of a specific file if it exists, for example:

    koco render data/run/20241118215004.ev --target output.html --force --open

See the results in `./output.html`.
