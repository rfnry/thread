import asyncio
import os
import signal
import sys
import uuid
from functools import partial
from http import HTTPStatus
from typing import Any

import asyncpg
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request, Response
from websockets.typing import Subprotocol

from rfnry_thread import (
    Custom,
    Identity,
    Invited,
    Message,
    PostgresStorage,
    Server,
    Text,
    migrations,
)

URL = os.environ.get("RFNRY_TEST_POSTGRES_URL", "")
SCHEMA = f"thread_client_{uuid.uuid4().hex[:12]}"


def who(request: Request | None) -> Identity | None:
    offered = (
        []
        if request is None
        else [p.strip() for p in request.headers.get("Sec-WebSocket-Protocol", "").split(",")]
    )
    if len(offered) != 2 or offered[0] != "thread" or not offered[1]:
        return None
    return Identity(role="user", id=offered[1], name=offered[1].title())


def upgrade(socket: ServerConnection, request: Request) -> Response | None:
    return None if who(request) else socket.respond(HTTPStatus.UNAUTHORIZED, "who are you?\n")


class Socket:
    def __init__(self, socket: ServerConnection, open_sockets: list[Socket]) -> None:
        self.socket = socket
        open_sockets.append(self)

    async def receive_text(self) -> str:
        data = await self.socket.recv()
        return data if isinstance(data, str) else data.decode()

    async def send_text(self, data: str) -> None:
        await self.socket.send(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        await self.socket.close(code, reason or "")


async def assistant(server: Server, open_sockets: list[Socket]) -> None:
    me = await server.connect(
        identity=Identity(role="assistant", id="bot-helper", name="Helper"), scope="acme"
    )
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        elif isinstance(item, Custom) and item.name == "drop":
            for socket in list(open_sockets):
                await socket.socket.close()
            open_sockets.clear()
        elif isinstance(item, Message) and item.author.role == "user":
            await me.working(item.thread_id, label="Reading rules/refunds.md")
            await me.step(
                item.thread_id,
                name="Read",
                input={"path": "rules/refunds.md"},
                output=[Text(text="A double charge is refunded in full.")],
                duration_ms=3,
                in_reply_to=item.id,
            )
            async with me.stream(
                item.thread_id, in_reply_to=item.id, metadata={"run_id": "run-1"}
            ) as reply:
                for chunk in ["Refunded ", "in full."]:
                    await reply.write(chunk)
            await me.idle(item.thread_id)


async def listen(storage: PostgresStorage, port: int) -> None:
    server = Server(storage)
    open_sockets: list[Socket] = []

    async def handler(socket: ServerConnection) -> None:
        identity = who(socket.request)
        assert identity is not None
        if identity.id == "refused":
            await socket.close(1008, "session expired")
            return
        await server.serve(Socket(socket, open_sockets), identity=identity, scope="acme")

    stopped = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stopped.set)
    async with serve(
        handler, "127.0.0.1", port, subprotocols=[Subprotocol("thread")], process_request=upgrade
    ) as listening:
        helper = asyncio.create_task(assistant(server, open_sockets))
        print(f"listening {next(iter(listening.sockets)).getsockname()[1]}", flush=True)
        await stopped.wait()
        helper.cancel()
        await asyncio.gather(helper, return_exceptions=True)


async def admin(statement: str) -> None:
    conn = await asyncpg.connect(URL, timeout=5)
    try:
        await conn.execute(statement)
    finally:
        await conn.close()


async def main(port: int) -> None:
    await admin(f'CREATE SCHEMA "{SCHEMA}"')
    try:
        pool: Any = await asyncpg.create_pool(
            URL, min_size=1, max_size=4, timeout=5, server_settings={"search_path": SCHEMA}
        )
        try:
            async with pool.acquire() as conn:
                await migrations.apply(conn, migrations.plan_create)
                await migrations.apply(conn, partial(migrations.plan_search, on=True))
            await listen(PostgresStorage(pool), port)
        finally:
            await asyncio.wait_for(pool.close(), 5)
    finally:
        await admin(f'DROP SCHEMA "{SCHEMA}" CASCADE')


if __name__ == "__main__":
    if not URL:
        sys.exit("tests: the TypeScript client's server needs RFNRY_TEST_POSTGRES_URL")
    asyncio.run(main(int(sys.argv[1])))
