import asyncio
import json
from pathlib import Path

from rfnry_engine import Engine, ToolCallStarted
from rfnry_thread import (
    Client,
    ForbiddenError,
    Identity,
    InMemoryStorage,
    Invited,
    Message,
    Server,
    Text,
    Thread,
    ThreadTools,
)

from providers import AnthropicProvider

PLANTS = json.loads(Path("data/plants.json").read_text())["plants"]
ASSISTANT = Identity(role="assistant", id="maintenance", name="Maintenance assistant")

server = Server(InMemoryStorage())
engine = Engine("agents", generation=AnthropicProvider())
assistants: dict[str, Client] = {}
working: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str, plant: str) -> None:
    await server.serve(socket, identity=person(name), scope=plant)


async def channels(plant: str) -> list[dict]:
    return [
        {
            "line": line,
            "machines": machines,
            "elevated": [
                thread.model_dump(mode="json")
                for thread in await server.list_threads(
                    plant, metadata={"line": line, "elevated": True}
                )
            ],
        }
        for line, machines in PLANTS[plant]["lines"].items()
    ]


async def ask(plant: str, name: str, line: str, title: str, task: str | None) -> dict:
    technician = person(name)
    metadata = {"line": line, "title": title, "task": task, "technician": technician.id}
    thread = await server.create_thread(plant, metadata=metadata | {"elevated": False})
    await server.add_member(plant, thread.id, technician)
    await server.add_member(plant, thread.id, ASSISTANT)
    return thread.model_dump(mode="json")


async def elevate(plant: str, name: str, thread_id: str) -> dict:
    thread = await assistants[plant].get_thread(thread_id)
    if thread.metadata["technician"] != person(name).id:
        raise ForbiddenError(f"harness: only the technician who asked elevates {thread_id!r}")
    elevated = await server.update_thread(plant, thread_id, thread.metadata | {"elevated": True})
    return elevated.model_dump(mode="json")


async def join(plant: str, name: str, thread_id: str) -> dict:
    thread = await assistants[plant].get_thread(thread_id)
    if not thread.metadata["elevated"]:
        raise ForbiddenError(f"harness: thread {thread_id!r} is private until it is elevated")
    await server.add_member(plant, thread_id, person(name))
    return thread.model_dump(mode="json")


async def listen() -> None:
    await asyncio.gather(*(assist(plant) for plant in PLANTS))


async def assist(plant: str) -> None:
    me = assistants[plant] = await server.connect(identity=ASSISTANT, scope=plant)
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id, since=0)
        elif isinstance(item, Message):
            thread = await me.get_thread(item.thread_id)
            if item.author.id == thread.metadata["technician"]:
                task = asyncio.create_task(answer(me, thread, item))
                working.add(task)
                task.add_done_callback(working.discard)


async def answer(me: Client, thread: Thread, message: Message) -> None:
    async def show(event) -> None:
        if isinstance(event, ToolCallStarted):
            await me.working(thread.id, label=event.tool)

    tools = ThreadTools(
        me, thread_id=thread.id, triggered_by=message.author, in_reply_to=message.id
    )
    facts = json.dumps(PLANTS[me.scope], indent=2, ensure_ascii=False)
    run = await engine.run(
        "maintenance",
        scope=me.scope,
        input=message.content,
        task=thread.metadata["task"],
        context=f"{facts}\n\nThis thread is about {thread.metadata['line']}.",
        tools=tools.only("read_recent"),
        on_event=show,
    )
    for step in run.steps:
        await me.step(
            thread.id,
            name=step.tool,
            input=step.args,
            output=[Text(text=step.result)] if isinstance(step.result, str) else [],
            is_error=step.is_error,
            duration_ms=step.duration_ms,
            in_reply_to=message.id,
        )
    await me.send(thread.id, run.reply, in_reply_to=message.id)
    await me.idle(thread.id)
