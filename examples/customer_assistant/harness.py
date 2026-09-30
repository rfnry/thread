import asyncio
from datetime import date

from rfnry_engine import Engine, Stop, TextDelta, Tool, ToolCallStarted
from rfnry_thread import Cancel, Client, Identity, InMemoryStorage, Invited, Message, Server, Text

from providers import AnthropicProvider
from tools import lookup_order

SCOPE = "acme-home"
ASSISTANT = Identity(role="assistant", id="support", name="Acme Home support")

server = Server(InMemoryStorage())
engine = Engine(
    "agents", generation=AnthropicProvider(), tools=[Tool("lookup_order", body=lookup_order)]
)
conversations: dict[str, list] = {}
inboxes: dict[str, asyncio.Queue] = {}
stops: dict[str, Stop] = {}
working: set[asyncio.Task] = set()


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
            inboxes.setdefault(item.thread_id, asyncio.Queue()).put_nowait(item.content)
            if item.thread_id not in stops:
                stops[item.thread_id] = Stop()
                task = asyncio.create_task(answer(me, item))
                working.add(task)
                task.add_done_callback(working.discard)
        elif isinstance(item, Cancel) and item.to.id == me.identity.id and item.thread_id in stops:
            stops[item.thread_id].now()


async def answer(me: Client, message: Message) -> None:
    inbox = inboxes[message.thread_id]
    try:
        while not inbox.empty():
            said = [part for _ in range(inbox.qsize()) for part in inbox.get_nowait()]
            await reply(me, message, said)
            stops[message.thread_id] = Stop()
    finally:
        del stops[message.thread_id]
        await me.idle(message.thread_id)


async def reply(me: Client, message: Message, said: list) -> None:
    thread_id = message.thread_id
    async with me.stream(thread_id, in_reply_to=message.id) as stream:

        async def show(event) -> None:
            if isinstance(event, TextDelta):
                await stream.write(event.text)
            elif isinstance(event, ToolCallStarted):
                await me.working(thread_id, label=event.tool)

        run = await engine.run(
            "support",
            scope=SCOPE,
            input=said,
            history=conversations.get(thread_id, []),
            context=f"Today is {date.today().isoformat()}.",
            on_event=show,
            stop=stops[thread_id],
            inbox=inboxes[thread_id],
        )
        conversations[thread_id] = run.conversation
        for step in run.steps:
            await me.step(
                thread_id,
                name=step.tool,
                input=step.args,
                output=[Text(text=step.result)] if isinstance(step.result, str) else [],
                is_error=step.is_error,
                duration_ms=step.duration_ms,
                in_reply_to=message.id,
            )
        await stream.finish(run.reply or "Stopped.")
