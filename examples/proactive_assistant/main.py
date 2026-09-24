import asyncio

from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server, resolve
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


async def main() -> None:
    server = Server(InMemoryStorage())

    async def handler(socket):
        await server.serve(Socket(socket), identity=who(socket.request), scope="demo")

    async with serve(handler, "127.0.0.1", 8765, process_request=upgrade):
        ana = await Client.connect(
            "ws://127.0.0.1:8765", headers={"Authorization": "Bearer ana-token"}
        )
        reminders = await server.connect(
            identity=Identity(role="assistant", id="reminders", name="Reminders"), scope="demo"
        )

        renewal = await reminders.create_thread(thread_id="renewal-ana-2026-10")
        await reminders.add_member(renewal.id, ana.identity)
        await reminders.join(renewal.id)
        print("present before:", [i.id for i in await reminders.present(renewal.id)])
        await reminders.send(renewal.id, "Your plan renews Friday. Switch to yearly and save 15%?")

        async for item in ana.events():
            if isinstance(item, Invited):
                print(f"ana was invited to {item.thread.id} by {item.sender.name}")
                await ana.join(item.thread_id, since=0)
                break
        for event in resolve(await ana.history(renewal.id)):
            if isinstance(event, Message):
                print(f"  {event.author.name}: {event.content[0].text}")
        print("present after:", [i.id for i in await reminders.present(renewal.id)])

        await ana.send(renewal.id, "Yes, switch it.")
        async for item in reminders.events():
            if isinstance(item, Message):
                print(f"reminders heard: {item.content[0].text}")
                break
        await ana.close()
        await reminders.close()


asyncio.run(main())
