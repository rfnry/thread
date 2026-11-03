import asyncio
from collections.abc import Generator
from typing import Any


class Counting:
    def __init__(self, pool: Any) -> None:
        self.pool = pool
        self.acquired = 0

    def acquire(self) -> Any:
        self.acquired += 1
        return self.pool.acquire()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.pool, name)


class Refusing:
    def __init__(self, pool: Any) -> None:
        self.pool = pool
        self.refuse = False

    def acquire(self) -> Any:
        if self.refuse:
            self.refuse = False
            raise OSError("the pool is closed")
        return self.pool.acquire()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.pool, name)


class Stalling:
    def __init__(self, pool: Any) -> None:
        self.pool = pool
        self.armed = False
        self.holding = False
        self.resumed = asyncio.Event()

    def stall(self) -> None:
        self.armed = True

    def resume(self) -> None:
        self.resumed.set()

    def acquire(self) -> Lent:
        return Lent(self, self.pool.acquire())

    def __getattr__(self, name: str) -> Any:
        return getattr(self.pool, name)


class Lent:
    def __init__(self, owner: Stalling, inner: Any) -> None:
        self.owner = owner
        self.inner = inner

    def __await__(self) -> Generator[Any, None, Any]:
        return (yield from self.inner.__await__())

    async def __aenter__(self) -> Held:
        return Held(self.owner, await self.inner.__aenter__())

    async def __aexit__(self, *exc: Any) -> Any:
        return await self.inner.__aexit__(*exc)


class Held:
    def __init__(self, owner: Stalling, conn: Any) -> None:
        self.owner = owner
        self.conn = conn

    async def executemany(self, *args: Any) -> Any:
        if self.owner.armed:
            self.owner.armed = False
            self.owner.holding = True
            try:
                await self.owner.resumed.wait()
            finally:
                self.owner.holding = False
        return await self.conn.executemany(*args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.conn, name)
