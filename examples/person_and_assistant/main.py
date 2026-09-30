import asyncio

from client import person
from harness import assistant, build_engine
from server import build_server, listen


async def main() -> None:
    server = build_server()
    engine = build_engine()

    async with listen(server):
        helper = asyncio.create_task(assistant(server, engine))
        await person()
        helper.cancel()


asyncio.run(main())
