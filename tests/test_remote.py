from litestar.datastructures import State
from kodo.remote.launcher import parse_factory, launch
from kodo.datatypes import Option
from pathlib import Path
import ray


def test_parse_factory():
    option = Option()
    env, cwd, module, flow = parse_factory(
        State(dict(env_home=option.ENV_HOME, venv_dir=option.VENV_DIR)), 
        "ec2:env2.example:flow")
    assert env == str(Path('./data/environ/ec2/.venv/bin/python').absolute())
    assert cwd == str(Path('./data/environ/ec2').absolute())
    assert module == "env2.example"
    assert flow == "flow"


async def test_launch():
    ray.init(
        address="localhost:6379", 
        ignore_reinit_error=False,
        namespace="kodo",
        configure_logging=True,
        logging_level="DEBUG",
        log_to_driver=True,
        runtime_env={"env_vars": {"RAY_DEBUG": "1"}}
    )        
    option = Option()
    state = State(dict(
        ray_server=option.RAY_SERVER,
        env_home=option.ENV_HOME, 
        venv_dir=option.VENV_DIR))
    resp = await launch(state, "ec2:env2.example:flow", {})
    assert resp.fid is None
    assert resp.is_launch is False
    assert resp.success is True
    assert "<h2>Func2</h2><input" in resp.payload
    resp = await launch(state, "ec2:env2.example:flow", {"submit": "GO"})
    assert resp.fid is not None
    assert resp.is_launch is True
    assert resp.payload is not None
    assert "/data/environ/ec2/.venv" in resp.payload["inputs"]["python"]
    assert resp.payload["inputs"]["foo"] == "bar"
    assert resp.payload["args"] == (1, 2, 3)
    assert resp.success is True
