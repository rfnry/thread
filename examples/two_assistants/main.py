import asyncio

from harness import build_engines, speaker
from server import build_server


async def main() -> None:
    server = build_server()
    optimist_engine, skeptic_engine = build_engines()
    skeptic = asyncio.create_task(speaker(server, skeptic_engine, "skeptic", opens=False))
    await asyncio.sleep(0.1)
    await speaker(server, optimist_engine, "optimist", opens=True)
    await skeptic


asyncio.run(main())
