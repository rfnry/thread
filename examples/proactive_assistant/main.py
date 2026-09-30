import asyncio

from rfnry_thread import Identity, Invited, Message, resolve

from client import connect_ana
from server import build_server, listen


async def main() -> None:
    server = build_server()

    async with listen(server):
        ana = await connect_ana()
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
