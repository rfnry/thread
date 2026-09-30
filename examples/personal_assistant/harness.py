import asyncio
from datetime import date

from rfnry_engine import Engine, ToolCallStarted
from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server

from providers import AnthropicProvider

SCOPE = "jordan"
ASSISTANT = Identity(role="assistant", id="assistant", name="Assistant")

server = Server(InMemoryStorage())
engine = Engine("agents", generation=AnthropicProvider())
assistants: dict[str, Client] = {}
conversations: dict[str, list] = {}
working: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str) -> None:
    await server.serve(socket, identity=person(name), scope=SCOPE)


async def nudge(name: str, about: str) -> dict:
    me, someone = assistants[SCOPE], person(name)
    thread = await me.create_thread(metadata={"reminder": about})
    await me.add_member(thread.id, someone)
    await me.join(thread.id)
    await me.working(thread.id, label="Writing a reminder")
    run = await engine.run(
        "assistant",
        scope=SCOPE,
        input=f"Write {someone.name} a reminder, the first message of a new conversation: {about}",
        context=f"Today is {date.today().isoformat()}.",
    )
    conversations[thread.id] = run.conversation
    await me.send(thread.id, run.reply)
    await me.idle(thread.id)
    return {"thread_id": thread.id, "reply": run.reply, "run_id": run.run_id}


async def listen() -> None:
    me = assistants[SCOPE] = await server.connect(identity=ASSISTANT, scope=SCOPE)
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id, since=0)
        elif isinstance(item, Message):
            task = asyncio.create_task(answer(me, item))
            working.add(task)
            task.add_done_callback(working.discard)


async def answer(me: Client, message: Message) -> None:
    async def show(event) -> None:
        if isinstance(event, ToolCallStarted):
            await me.working(message.thread_id, label=event.tool)

    run = await engine.run(
        "assistant",
        scope=SCOPE,
        input=message.content,
        history=conversations.get(message.thread_id, []),
        context=f"Today is {date.today().isoformat()}.",
        on_event=show,
    )
    conversations[message.thread_id] = run.conversation
    await me.send(message.thread_id, run.reply, in_reply_to=message.id)
    await me.idle(message.thread_id)
