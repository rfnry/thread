import asyncio
from collections.abc import Callable


async def until(check: Callable[[], bool], timeout: float = 2.0) -> None:
    async def wait() -> None:
        while not check():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), timeout)
