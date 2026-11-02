import shutil

import pytest


@pytest.fixture(autouse=True)
def node() -> None:
    if shutil.which("npm") is None:
        pytest.fail("tests: the live tier needs node and npm")
