import asyncio

from rfnry_engine import Stop, TextDelta, ToolCallStarted
from rfnry_thread import Cancel, Client, Identity, InMemoryStorage, Invited, Message, Server

import harness

SCOPE = "acme-home"
ASSISTANT = Identity(role="assistant", id="support", name="Acme Home support")

server = Server(InMemoryStorage())
conversations: dict[str, list] = {}
turns: dict[str, asyncio.Lock] = {}
stops: dict[str, Stop] = {}
running: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str) -> None:
    await server.serve(socket, identity=person(name), scope=SCOPE)


async def open_thread(name: str) -> dict:
    customer = person(name)
    thread = await server.create_thread(SCOPE, thread_id=f"support-{customer.id}")
    await server.add_member(SCOPE, thread.id, customer)
    await server.add_member(SCOPE, thread.id, ASSISTANT)
    return thread.model_dump(mode="json")


async def listen() -> None:
    me = await server.connect(identity=ASSISTANT, scope=SCOPE)
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id, since=0)
        elif isinstance(item, Message):
            task = asyncio.create_task(answer(me, item))
            running.add(task)
            task.add_done_callback(running.discard)
        elif isinstance(item, Cancel) and item.to.id == ASSISTANT.id and item.thread_id in stops:
            stops[item.thread_id].now()


async def answer(me: Client, message: Message) -> None:
    thread_id = message.thread_id
    async with turns.setdefault(thread_id, asyncio.Lock()):
        stop = stops[thread_id] = Stop()
        async with me.stream(thread_id, in_reply_to=message.id) as reply:

            async def show(event) -> None:
                if isinstance(event, TextDelta):
                    await reply.write(event.text)
                elif isinstance(event, ToolCallStarted):
                    await me.working(thread_id, label=event.tool)

            engine_run = await harness.run(
                message.content, conversations.get(thread_id, []), on_event=show, stop=stop
            )
            conversations[thread_id] = engine_run.conversation
            for step in engine_run.steps:
                await me.step(thread_id, name=step.tool, input=step.args, in_reply_to=message.id)
            await reply.finish(engine_run.reply or "Stopped.")
        await me.idle(thread_id)
