import os

from rfnry_thread import Identity, PostgresStorage, Server
from websockets.asyncio.server import serve

DSN = os.environ.get("THREAD_POSTGRES_URL", "postgresql://thread:thread@127.0.0.1:55436/thread")

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
