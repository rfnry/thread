import asyncio

from rfnry_thread import Identity, Message, resolve

from harness import answer, build_engine
from server import build_server


async def main() -> None:
    server = build_server()
    engine = build_engine()
    ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="demo")
    helper = await server.connect(
        identity=Identity(role="assistant", id="support", name="Support"), scope="demo"
    )
    thread = await ana.create_thread(thread_id="invoice")
    await ana.add_member(thread.id, helper.identity)
    await helper.join(thread.id)
    question = await ana.send(thread.id, "My invoice was charged twice. What happens now?")

    await answer(engine, helper, thread.id, question)

    for event in resolve(await ana.history(thread.id)):
        if isinstance(event, Message):
            print(f"{event.author.name}: {event.content[0].text}")
        else:
            print(f"  [{event.kind} {getattr(event, 'name', '')}]")
    await ana.close()
    await helper.close()


asyncio.run(main())
