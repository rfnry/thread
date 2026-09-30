import asyncio
import shutil
from collections import defaultdict
from pathlib import Path
from uuid import uuid4

from rfnry_engine import Engine, ToolCall, ToolCallStarted, Workspace
from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server, Text

from providers import AnthropicProvider

SCOPE = "acme-books"
ASSISTANT = Identity(role="assistant", id="bookkeeper", name="Bookkeeper")

server = Server(InMemoryStorage())
engine = Engine("agents", generation=AnthropicProvider())
conversations: dict[str, list] = {}
folders: dict[str, Path] = {}
turns: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
approvals: dict[str, asyncio.Future[bool]] = {}
working: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str) -> None:
    await server.serve(socket, identity=person(name), scope=SCOPE)


def workspace(thread_id: str) -> Workspace:
    if thread_id not in folders:
        folders[thread_id] = Path(".work") / uuid4().hex[:8]
        folders[thread_id].mkdir(parents=True)
        shutil.copy("data/ledger.csv", folders[thread_id] / "ledger.csv")
    return Workspace(folders[thread_id], unsandboxed=True)


async def listen() -> None:
    me = await server.connect(identity=ASSISTANT, scope=SCOPE)
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id, since=0)
        elif isinstance(item, Message) and "allow" in item.metadata:
            if decision := approvals.pop(item.in_reply_to, None):
                decision.set_result(item.metadata["allow"] is True)
        elif isinstance(item, Message):
            task = asyncio.create_task(work(me, item))
            working.add(task)
            task.add_done_callback(working.discard)


async def work(me: Client, message: Message) -> None:
    thread_id = message.thread_id

    async def approve(agent: str, call: ToolCall) -> bool:
        if call.name != "Shell":
            return True
        card = await me.send(
            thread_id, call.args["command"], in_reply_to=message.id, metadata={"approval": "Shell"}
        )
        approvals[card.id] = asyncio.get_running_loop().create_future()
        await me.working(thread_id, label="Waiting for your approval")
        return await approvals[card.id]

    async def show(event) -> None:
        if isinstance(event, ToolCallStarted):
            await me.working(thread_id, label=event.tool)

    async with turns[thread_id]:
        run = await engine.run(
            "bookkeeper",
            scope=SCOPE,
            input=message.content,
            history=conversations.get(thread_id, []),
            workspace=workspace(thread_id),
            approve=approve,
            on_event=show,
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
        await me.send(thread_id, run.reply, in_reply_to=message.id)
        await me.idle(thread_id)
