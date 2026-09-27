import asyncio

from rfnry_engine import Engine
from rfnry_thread import Identity, InMemoryStorage, Invited, Message, Server

from providers import AnthropicProvider, GoogleProvider

TURNS = 3


async def speaker(server, engine, name, opens):
    me = await server.connect(
        identity=Identity(role="assistant", id=name, name=name.title()), scope="demo"
    )
    spoken = 0
    if opens:
        thread = await me.create_thread(thread_id="debate")
        await me.add_member(thread.id, Identity(role="assistant", id="skeptic", name="Skeptic"))
        await me.join(thread.id)
        await me.send(thread.id, "Proposal: the team moves to a four-day week.")
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id, since=0)
        elif isinstance(item, Message) and spoken < TURNS:
            run = await engine.run(name, scope="demo", input=item.content)
            await me.send(item.thread_id, run.reply, in_reply_to=item.id)
            print(f"{name}: {run.reply}\n")
            spoken += 1
        if spoken == TURNS:
            return


async def main() -> None:
    server = Server(InMemoryStorage())
    claude = Engine("agents", generation=AnthropicProvider())
    gemini = Engine("agents", generation=GoogleProvider())
    skeptic = asyncio.create_task(speaker(server, gemini, "skeptic", opens=False))
    await asyncio.sleep(0.1)
    await speaker(server, claude, "optimist", opens=True)
    await skeptic


asyncio.run(main())
