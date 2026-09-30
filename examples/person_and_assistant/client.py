import asyncio

from rfnry_thread import Client, Identity, Message, StreamDelta


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
