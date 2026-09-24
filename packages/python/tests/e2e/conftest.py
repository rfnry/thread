import shutil

import pytest


@pytest.fixture(autouse=True)
def node() -> None:
    if shutil.which("npm") is None:
        pytest.skip("npm is not installed")
