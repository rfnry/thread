import asyncio

from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server, Text, Thread

import harness
from utils import mask, slug

SCOPE = "clinic-northside"
ASSISTANT = Identity(role="assistant", id="nurse-line", name="Nurse line assistant")


async def redact(author: Identity, thread: Thread, content: list) -> list:
    return [Text(text=mask(part.text)) if part.type == "text" else part for part in content]


server = Server(InMemoryStorage(), redact=redact)
conversations: dict[str, list] = {}
running: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str) -> None:
    await server.serve(socket, identity=person(name), scope=SCOPE)


async def open_thread(name: str, patient: str, said: str) -> dict:
    thread_id = f"patient-{slug(patient)}"
    thread = await server.create_thread(SCOPE, thread_id, {"patient": patient})
    for member in (person(name), ASSISTANT):
        await server.add_member(SCOPE, thread.id, member)
    desk = await server.connect(identity=person(name), scope=SCOPE)
    message = await desk.send(thread.id, said)
    await desk.close()
    return {"thread": thread.model_dump(mode="json"), "message": message.model_dump(mode="json")}


async def threads() -> list[dict]:
    return [thread.model_dump(mode="json") for thread in await server.list_threads(SCOPE)]


async def join(name: str, thread_id: str) -> dict:
    member = await server.add_member(SCOPE, thread_id, person(name))
    return member.model_dump(mode="json")


async def listen() -> None:
    me = await server.connect(identity=ASSISTANT, scope=SCOPE)
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id, since=0)
        elif isinstance(item, Message):
            task = asyncio.create_task(answer(me, item))
            running.add(task)
            task.add_done_callback(running.discard)


async def answer(me: Client, message: Message) -> None:
    await me.working(message.thread_id, label="Preparing the callback")
    said = "\n".join(part.text for part in message.content if part.type == "text")
    engine_run = await harness.run(
        f"{message.author.name}: {said}", conversations.get(message.thread_id, [])
    )
    conversations[message.thread_id] = engine_run.conversation
    await me.send(message.thread_id, engine_run.reply, in_reply_to=message.id)
    await me.idle(message.thread_id)
