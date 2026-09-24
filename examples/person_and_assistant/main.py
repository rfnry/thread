import asyncio

from rfnry_engine import Engine, TextDelta
from rfnry_engine import Message as Turn
from rfnry_thread import (
    Client,
    Identity,
    InMemoryStorage,
    Invited,
    Message,
    Server,
    StreamDelta,
    resolve,
)
from websockets.asyncio.server import serve

from providers import AnthropicProvider


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


async def assistant(server, engine):
    me = await server.connect(
        Identity(role="assistant", id="assistant", name="Assistant"), scope="demo"
    )
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        if not (isinstance(item, Message) and item.author.role == "user"):
            continue
        earlier = [
            e
            for e in resolve(await me.history(item.thread_id, before=item.seq))
            if isinstance(e, Message)
        ]
        history = [
            Turn(
                role="user" if e.author.role == "user" else "assistant",
                content=[part.model_dump() for part in e.content],
            )
            for e in earlier
        ]
        async with me.stream(item.thread_id, in_reply_to=item.id) as reply:

            async def show(event):
                if isinstance(event, TextDelta):
                    await reply.write(event.text)

            run = await engine.run(
                "assistant", scope="demo", input=item.content, history=history, on_event=show
            )
            await reply.finish(run.reply)


async def person():
    you = await Client.connect("ws://127.0.0.1:8766", headers={"Authorization": "Bearer you"})
    thread = await you.create_thread(thread_id="chat")
    await you.add_member(thread.id, Identity(role="assistant", id="assistant", name="Assistant"))
    await you.join(thread.id)

    async def show():
        async for item in you.events():
            if isinstance(item, StreamDelta):
                print(item.text, end="", flush=True)
            elif isinstance(item, Message):
                print()

    printer = asyncio.create_task(show())
    while (line := await asyncio.to_thread(input, "you: ")).strip():
        await you.send(thread.id, line)
        await asyncio.sleep(0.2)
        print("assistant: ", end="", flush=True)
    printer.cancel()
    await you.close()


async def main() -> None:
    server = Server(InMemoryStorage())
    engine = Engine("agents", generation=AnthropicProvider())

    async def handler(socket):
        await server.serve(Socket(socket), identity=YOU, scope="demo")

    async with serve(handler, "127.0.0.1", 8766, process_request=upgrade):
        helper = asyncio.create_task(assistant(server, engine))
        await person()
        helper.cancel()


asyncio.run(main())
