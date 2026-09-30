from rfnry_thread import Identity, InMemoryStorage, Server
from websockets.asyncio.server import serve


class Socket:
    def __init__(self, socket):
        self.socket = socket

    async def receive_text(self):
        return await self.socket.recv()

    async def send_text(self, data):
        await self.socket.send(data)

    async def close(self, code=1000, reason=None):
        await self.socket.close(code, reason or "")


YOU = Identity(role="user", id="you", name="You")


def upgrade(connection, request):
    if request.headers.get("Authorization") != "Bearer you":
        return connection.respond(401, "who are you?\n")
    return None


def build_server():
    return Server(InMemoryStorage())


def listen(server):
    async def handler(socket):
        await server.serve(Socket(socket), identity=YOU, scope="demo")

    return serve(handler, "127.0.0.1", 8766, process_request=upgrade)
