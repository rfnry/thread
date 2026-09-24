from collections.abc import AsyncIterator
from typing import Any

import pytest

from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.storage import Storage


@pytest.fixture(params=["inmemory"])
async def storage(request: Any) -> AsyncIterator[Storage]:
    yield InMemoryStorage()
