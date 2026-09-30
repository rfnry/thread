import asyncio

from server import build_server, echo, listen


async def main() -> None:
    server = build_server()

    async with listen(server):
        await echo(server)


asyncio.run(main())
