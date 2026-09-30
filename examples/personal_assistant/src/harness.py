import asyncio
from datetime import date
from pathlib import Path

from rfnry_engine import Engine
from rfnry_thread import Client, Identity, InMemoryStorage, Message, Server

from providers import AnthropicProvider

ROOT = Path(__file__).resolve().parent.parent
SCOPE = "jordan"
ASSISTANT = Identity(role="assistant", id="assistant", name="Assistant")

server = Server(InMemoryStorage())
engine = Engine(ROOT / "agents", generation=AnthropicProvider())
assistants: dict[str, Client] = {}
conversations: dict[str, list] = {}
running: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str) -> None:
    await server.serve(socket, identity=person(name), scope=SCOPE)


async def nudge(name: str, about: str) -> dict:
    me, someone = assistants[SCOPE], person(name)
    thread = await me.create_thread(metadata={"about": about})
    await me.add_member(thread.id, someone)
    await me.join(thread.id)
    await me.working(thread.id, label="Writing")
    run = await engine.run(
        "assistant",
        scope=SCOPE,
        input=f"Write to {someone.name} first, opening a new conversation: {about}",
        context=f"Today is {date.today().isoformat()}.",
    )
    conversations[thread.id] = run.conversation
    await me.send(thread.id, run.reply)
    await me.idle(thread.id)
    return {"thread_id": thread.id, "reply": run.reply, "run_id": run.run_id}


async def listen() -> None:
    me = assistants[SCOPE] = await server.connect(identity=ASSISTANT, scope=SCOPE)
    async for item in me.events():
        if isinstance(item, Message):
            task = asyncio.create_task(answer(me, item))
            running.add(task)
            task.add_done_callback(running.discard)


async def answer(me: Client, message: Message) -> None:
    await me.working(message.thread_id, label="Thinking")
    run = await engine.run(
        "assistant",
        scope=SCOPE,
        input=message.content,
        history=conversations.get(message.thread_id, []),
        context=f"Today is {date.today().isoformat()}.",
    )
    conversations[message.thread_id] = run.conversation
    await me.send(message.thread_id, run.reply, in_reply_to=message.id)
    await me.idle(message.thread_id)
