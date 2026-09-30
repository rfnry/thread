import tempfile
from pathlib import Path

from rfnry_engine import Engine, Run, ToolCall, Workspace
from rfnry_thread import Client, Message

from providers import AnthropicProvider

SALES = "region,amount\nnorth,120\nsouth,80\nnorth,40\neast,200\nsouth,30\neast,15\n"
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


def build_engine() -> Engine:
    return Engine("agents", generation=AnthropicProvider())


async def analyze(engine: Engine, analyst: Client, thread_id: str, question: Message) -> Run:
    with tempfile.TemporaryDirectory() as work:
        (Path(work) / "sales.csv").write_text(SALES)
        run = await engine.run(
            "analyst",
            scope="demo",
            input=text(question),
            workspace=Workspace(work, unsandboxed=True, timeout=60),
            approve=Approvals(analyst, thread_id),
        )
    await analyst.send(thread_id, run.reply, in_reply_to=question.id)
    return run
