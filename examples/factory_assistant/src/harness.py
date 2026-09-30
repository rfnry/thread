import asyncio
import json
from pathlib import Path

from rfnry_engine import Engine
from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server, ThreadTools

from providers import AnthropicProvider

ROOT = Path(__file__).resolve().parent.parent
PLANT = json.loads((ROOT / "data" / "plant.json").read_text())["plant"]
FACTS = json.dumps(PLANT, indent=2, ensure_ascii=False)
SCOPE = "plant-reno"
ASSISTANT = Identity(role="assistant", id="maintenance", name="Maintenance")

server = Server(InMemoryStorage())
engine = Engine(ROOT / "agents", generation=AnthropicProvider())
running: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str) -> None:
    await server.serve(socket, identity=person(name), scope=SCOPE)


async def channels() -> list[dict]:
    found = await server.list_threads(SCOPE, metadata={"channel": True})
    return [channel.model_dump(mode="json") for channel in found]


async def join(name: str, channel_id: str) -> dict:
    member = await server.add_member(SCOPE, channel_id, person(name))
    return member.model_dump(mode="json")


async def listen() -> None:
    me = await server.connect(identity=ASSISTANT, scope=SCOPE)
    for line, machines in PLANT["lines"].items():
        metadata = {"channel": True, "line": line, "machines": machines}
        channel = await server.create_thread(SCOPE, line.replace(" ", "-"), metadata)
        await server.add_member(SCOPE, channel.id, ASSISTANT)
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        elif isinstance(item, Message) and "@maintenance" in said(item).lower():
            task = asyncio.create_task(answer(me, item))
            running.add(task)
            task.add_done_callback(running.discard)


def said(message: Message) -> str:
    return "\n".join(part.text for part in message.content if part.type == "text")


async def answer(me: Client, message: Message) -> None:
    await me.working(message.thread_id, label="Reading the channel")
    channel = await me.get_thread(message.thread_id)
    tools = ThreadTools(
        me, thread_id=channel.id, triggered_by=message.author, in_reply_to=message.id
    )
    run = await engine.run(
        "maintenance",
        scope=SCOPE,
        input=f"{message.author.name}: {said(message)}",
        context=f"{FACTS}\n\nThis channel is {channel.metadata['line']}.",
        tools=tools.only("read_recent"),
    )
    await me.send(channel.id, run.reply, in_reply_to=message.id, triggered_by=message.author)
    await me.idle(channel.id)
