import asyncio
import os

import asyncpg
from rfnry_thread import Client, Identity, Message, PostgresStorage, Server, Working
from websockets.asyncio.server import serve

PEOPLE = {
    "ana": Identity(role="user", id="ana", name="Ana"),
    "bruno": Identity(role="user", id="bruno", name="Bruno"),
}


class Socket:
    def __init__(self, socket):
        self.socket = socket

    async def receive_text(self):
        return await self.socket.recv()

    async def send_text(self, data):
        await self.socket.send(data)

    async def close(self, code=1000, reason=None):
        await self.socket.close(code, reason or "")


def who(request):
    return PEOPLE.get(request.headers.get("Authorization", "").removeprefix("Bearer "))


def upgrade(connection, request):
    return None if who(request) else connection.respond(401, "who are you?\n")


async def replica(pool, port):
    storage = PostgresStorage(pool)
    await storage.initialize()
    server = Server(storage)
    await server.start()

    async def handler(socket):
        await server.serve(Socket(socket), identity=who(socket.request), scope="demo")

    return server, await serve(handler, "127.0.0.1", port, process_request=upgrade)


async def main() -> None:
    url = os.environ["RFNRY_EXAMPLES_POSTGRES_URL"]
    one, two = await asyncpg.create_pool(url), await asyncpg.create_pool(url)
    (first, first_socket), (second, second_socket) = (
        await replica(one, 8771),
        await replica(two, 8772),
    )

    ana = await Client.connect("ws://127.0.0.1:8771", headers={"Authorization": "Bearer ana"})
    bruno = await Client.connect("ws://127.0.0.1:8772", headers={"Authorization": "Bearer bruno"})
    thread = await ana.create_thread(thread_id="across")
    await ana.add_member(thread.id, bruno.identity)
    await ana.join(thread.id)
    await bruno.join(thread.id)
    await ana.working(thread.id, label="typing")
    await ana.send(thread.id, "Hello from replica one.")

    async for item in bruno.events():
        if isinstance(item, Working):
            print(f"bruno sees ana {item.label} (a signal mirrored through NOTIFY)")
        if isinstance(item, Message):
            print(f"bruno, on replica two, reads seq {item.seq}: {item.content[0].text}")
            break

    await ana.close()
    await bruno.close()
    for server, socket in ((first, first_socket), (second, second_socket)):
        await server.stop()
        socket.close()
        await socket.wait_closed()
    await server.delete_scope("demo")
    await one.close()
    await two.close()


asyncio.run(main())
