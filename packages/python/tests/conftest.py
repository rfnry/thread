import asyncio
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest
from database import SCHEMA, URL, Schemas, admin, open_pool

from rfnry_thread import migrations
from rfnry_thread.postgres import PostgresStorage

START = (
    "start the database with `docker start rfnry-thread-pg` and set "
    "RFNRY_TEST_POSTGRES_URL=postgresql://thread:thread@127.0.0.1:55436/thread"
)


async def create() -> None:
    await admin(f"CREATE SCHEMA {SCHEMA}")
    pool = await open_pool()
    try:
        async with pool.acquire() as conn:
            await migrations.apply(conn, migrations.plan_create)
    finally:
        await pool.close()


def pytest_sessionstart(session: pytest.Session) -> None:
    if not URL:
        raise pytest.UsageError(f"tests: RFNRY_TEST_POSTGRES_URL is not set; {START}")
    try:
        asyncio.run(create())
    except (
        OSError,
        asyncpg.PostgresError,
        asyncpg.InterfaceError,
        ValueError,
        TimeoutError,
    ) as error:
        raise pytest.UsageError(
            f"tests: cannot connect to RFNRY_TEST_POSTGRES_URL ({type(error).__name__}: {error}); {START}"
        ) from None


def pytest_sessionfinish(session: pytest.Session) -> None:
    asyncio.run(admin(f"DROP SCHEMA {SCHEMA} CASCADE"))


@pytest.fixture
async def pool() -> AsyncIterator[Any]:
    pool = await open_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM thread_threads")
    yield pool
    await asyncio.wait_for(pool.close(), 5)


@pytest.fixture
def storage(pool: Any) -> PostgresStorage:
    return PostgresStorage(pool)


@pytest.fixture
async def schemas() -> AsyncIterator[Schemas]:
    held = Schemas()
    yield held
    await held.close()
