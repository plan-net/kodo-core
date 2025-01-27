## Start Services

We need 3 services:

1. `kodosumi` registry/node service
2. [Ray Cluster service](https://ray.io) featuring concurrent, distributed execution of agentic flows.

All services are based on the installation procedure described in [this README](../../README.md).

The following commands reiterate this setup with branch `mrau.flow`.

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

### Setup OpenAI API Key

Create a file `.env`. Inside this file specify the OpenAI key with

```
OPENAI_API_KEY=sk-...
```

### Start node service

Use `kodosumi` configuration file delivered with `tests/example3` directory.

```bash
python -m kodo.cli service tests/example3/node4.yaml --ray
```

## `kodosumi` screens

* [Flow Explorer](http://localhost:3371/flows)
* [Jobs Review](http://localhost:3371/flow)
