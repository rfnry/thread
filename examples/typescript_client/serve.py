import asyncio

from rfnry_thread import Identity, InMemoryStorage, Invited, Message, Server
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


BROWSER = Identity(role="user", id="browser", name="Browser")


def offered(request):
    return [p.strip() for p in request.headers.get("Sec-WebSocket-Protocol", "").split(",")]


def upgrade(connection, request):
    return (
        None
        if offered(request) == ["thread", "browser"]
        else connection.respond(401, "who are you?\n")
    )


async def echo(server):
    me = await server.connect(Identity(role="assistant", id="echo", name="Echo"), scope="demo")
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        elif isinstance(item, Message):
            async with me.stream(item.thread_id, in_reply_to=item.id) as reply:
                for word in f"You said: {item.content[0].text}".split():
                    await reply.write(word + " ")
                    await asyncio.sleep(0.05)


async def main() -> None:
    server = Server(InMemoryStorage())

    async def handler(socket):
        await server.serve(Socket(socket), identity=BROWSER, scope="demo")

    async with serve(handler, "127.0.0.1", 8780, subprotocols=["thread"], process_request=upgrade):
        await echo(server)


asyncio.run(main())
