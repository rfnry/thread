import asyncio
from datetime import datetime
from pathlib import Path

import rfnry_engine
from rfnry_engine import Engine, Refinery, Review, ToolCallStarted
from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server, Text

from providers import decision, generation

FIRMS = ("firm-acme", "firm-brightline")
CONTRACTS = {path.stem: path.read_text() for path in Path("data/contracts").glob("*.md")}
ASSISTANT = Identity(role="assistant", id="reviewer", name="Contract reviewer")


class Folder:
    def __init__(self, root: str) -> None:
        self.root = Path(root)

    async def put(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


server = Server(InMemoryStorage())
refinery = Refinery(rfnry_engine.InMemoryStorage())
engine = Engine("agents", generation=generation(), decision=decision(), refinery=refinery)
conversations: dict[str, list] = {}
watermarks: dict[str, datetime | None] = {}
working: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str, firm: str) -> None:
    await server.serve(socket, identity=person(name), scope=firm)


async def open_matter(firm: str, name: str, contract: str) -> dict:
    thread = await server.create_thread(
        firm, thread_id=f"matter-{contract}", metadata={"contract": contract}
    )
    await server.add_member(firm, thread.id, person(name))
    await server.add_member(firm, thread.id, ASSISTANT)
    return thread.model_dump(mode="json")


async def review_run(firm: str, run_id: str, feedback: Review) -> dict:
    label = await refinery.review(firm, run_id, feedback)
    return label.model_dump(mode="json", exclude_none=True)


async def export(firm: str) -> dict:
    exported = await refinery.export(firm, Folder("datasets"), since=watermarks.get(firm))
    watermarks[firm] = exported.watermark
    return exported.model_dump(mode="json")


async def listen() -> None:
    await asyncio.gather(*(assist(firm) for firm in FIRMS))


async def assist(firm: str) -> None:
    me = await server.connect(identity=ASSISTANT, scope=firm)
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id, since=0)
        elif isinstance(item, Message):
            task = asyncio.create_task(answer(me, item))
            working.add(task)
            task.add_done_callback(working.discard)


async def answer(me: Client, message: Message) -> None:
    thread = await me.get_thread(message.thread_id)
    contract = thread.metadata["contract"]

    async def show(event) -> None:
        if isinstance(event, ToolCallStarted):
            await me.working(thread.id, label=f"{event.agent}: {event.tool}")

    run = await engine.run(
        "reviewer",
        scope=me.scope,
        input=message.content,
        history=conversations.get(thread.id, []),
        context=f"The matter's contract, {contract}:\n\n{CONTRACTS[contract]}",
        on_event=show,
    )
    conversations[thread.id] = run.conversation
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
    await me.send(thread.id, run.reply, in_reply_to=message.id, metadata={"run_id": run.run_id})
    await me.idle(thread.id)
