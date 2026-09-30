from rfnry_thread import Identity, InMemoryStorage, Server
from websockets.asyncio.server import serve

PEOPLE = {"ana-token": Identity(role="user", id="ana", name="Ana")}


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


def build_server():
    return Server(InMemoryStorage())


def listen(server):
    async def handler(socket):
        await server.serve(Socket(socket), identity=who(socket.request), scope="demo")

    return serve(handler, "127.0.0.1", 8765, process_request=upgrade)
