import os
from collections.abc import AsyncIterator
from typing import Any

import pytest

from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.storage import Storage

TABLES = "thread_events, thread_members, thread_threads"


async def postgres_pool() -> Any:
    url = os.environ.get("RFNRY_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("RFNRY_TEST_POSTGRES_URL is not set")
    asyncpg = pytest.importorskip("asyncpg")
    return await asyncpg.create_pool(url, min_size=2, max_size=10)


async def fresh(pool: Any) -> PostgresStorage:
    storage = PostgresStorage(pool)
    await storage.initialize()
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {TABLES}")
    return storage


@pytest.fixture(params=["inmemory", pytest.param("postgres", marks=pytest.mark.postgres)])
async def storage(request: Any) -> AsyncIterator[Storage]:
    if request.param == "inmemory":
        yield InMemoryStorage()
        return
    pool = await postgres_pool()
    yield await fresh(pool)
    await pool.close()
