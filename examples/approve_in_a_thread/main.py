import asyncio
import tempfile
from pathlib import Path

from rfnry_engine import Engine, ToolCall, Workspace
from rfnry_thread import Client, Identity, InMemoryStorage, Message, Server, resolve

from providers import AnthropicProvider

SALES = "region,amount\nnorth,120\nsouth,80\nnorth,40\neast,200\nsouth,30\neast,15\n"
ANSWERS = ["allow", "always"]
ASKED = {"Shell": ("run", "command"), "Write": ("write", "path"), "Edit": ("edit", "path")}


def text(message: Message) -> str:
    return message.content[0].text


class Approvals:
    def __init__(self, analyst: Client, thread_id: str) -> None:
        self.analyst, self.thread_id = analyst, thread_id
        self.always = False

    async def __call__(self, agent: str, call: ToolCall) -> bool:
        if call.name not in ASKED or self.always:
            return True
        verb, what = ASKED[call.name]
        target = call.args.get(what, "")
        ask = f"{call.name}: I want to {verb} `{target}`. Reply allow, deny or always."
        request = await self.analyst.send(self.thread_id, ask)
        async for item in self.analyst.events():
            if isinstance(item, Message) and item.in_reply_to == request.id:
                answer = text(item).strip().lower()
                if answer == "always":
                    self.always = True
                return answer in ("allow", "always")
        return False


async def ana_answers(ana: Client, thread_id: str) -> None:
    async for item in ana.events():
        if isinstance(item, Message) and text(item).endswith("Reply allow, deny or always."):
            await ana.send(thread_id, ANSWERS.pop(0) if ANSWERS else "deny", in_reply_to=item.id)


async def main() -> None:
    server = Server(InMemoryStorage())
    engine = Engine("agents", generation=AnthropicProvider())
    ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="demo")
    analyst = await server.connect(
        identity=Identity(role="assistant", id="analyst", name="Analyst"), scope="demo"
    )
    thread = await ana.create_thread(thread_id="sales")
    await ana.add_member(thread.id, analyst.identity)
    await ana.join(thread.id)
    await analyst.join(thread.id)
    answering = asyncio.create_task(ana_answers(ana, thread.id))
    question = await ana.send(thread.id, "Which region sold the most? The data is in sales.csv.")

    with tempfile.TemporaryDirectory() as work:
        (Path(work) / "sales.csv").write_text(SALES)
        run = await engine.run(
            "analyst",
            scope="demo",
            input=text(question),
            workspace=Workspace(work, unsandboxed=True, timeout=60),
            approve=Approvals(analyst, thread.id),
        )
    await analyst.send(thread.id, run.reply, in_reply_to=question.id)
    answering.cancel()

    for event in resolve(await ana.history(thread.id)):
        if isinstance(event, Message):
            print(f"{event.author.name}: {text(event)}")
    print()
    for step in run.steps:
        target = step.args.get("command") or step.args.get("path") or step.args.get("pattern", "")
        print(f"{step.tool:6} approved={step.approved!s:5} {target}")
    await ana.close()
    await analyst.close()


asyncio.run(main())
