import re
from uuid import UUID

import kodo.config
import kodo.worker.main

from tests.crew2 import flow as flow2
from tests.crew3 import flow as flow3


async def test_instrument():
    worker = kodo.worker.main.Worker("tests.test_worker:flow2")
    assert bool(
        re.match(r".+check.+back.+later.+", await worker.welcome(), re.DOTALL)
    )



async def test_launch(tmp_path):
    worker = kodo.worker.main.Worker(
        "tests.test_worker:flow3",     
        exec_path=tmp_path / "exec"
    )
    assert "hello world" == await worker.welcome()
    fid = await worker.instantiate()
    assert isinstance(fid, UUID)

    worker = kodo.worker.main.Worker(
        "tests.test_worker:flow3",     
        exec_path=tmp_path / "exec"
    )


