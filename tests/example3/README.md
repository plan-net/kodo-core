## Start Services

We need 3 services:

1. `kodosumi` registry/node service
2. [Ray Cluster service](https://ray.io) featuring concurrent, distributed execution of agentic flows.
3. [Ollama LLM service](https://ollama.com/) featuring local LLM

All services are based on the installation procedure describe in [this README](../../README.md).

The following commands reiterate this setup.

```bash
mkdir demo
cd demo
git clone https://github.com/plan-net/kodo-core.git
cd kodo-core
git checkout mrau.flow
python3 -m venv .venv
source .venv/bin/activate
pip install .[tests]
```

### Start Ray Cluster

```bash
ray start --head --verbose
```

### Start local LLM server

If you prefer to connect to a local _Ollama Server_ start the LLM service with

```bash
ollama ls
```

### Start node service

Use `kodosumi` configuration file delivered with test `tests` repository.

```bash
python -m kodo.cli service tests/example3/node4.yaml --ray
```

## `kodosumi` screens

* [Flow Explorer](http://localhost:3371/flows)
* [Jobs Review](http://localhost:3371/flow)

