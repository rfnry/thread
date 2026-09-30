import asyncio
import json
from pathlib import Path

import rfnry_memory
from rfnry_engine import Engine
from rfnry_memory import Memory, MemoryTools
from rfnry_thread import Identity, Message, Text

from providers import AnthropicProvider, TypeSafeProvider, VoyageProvider

ASSISTANT = Identity(role="assistant", id="floor-assistant", name="Floor assistant")
JUDGEMENTS = ("relevant", "evidence", "contradicts", "injection")


def output(result):
    if isinstance(result, list):
        return result
    if isinstance(result, str):
        try:
            result = json.loads(result)
        except ValueError:
            return [Text(text=result[:4000])]
    if isinstance(result, dict) and "hits" in result:
        lines = []
        for hit in result["hits"]:
            label = (hit.get("title") or hit["text"])[:80]
            judge = hit.get("judge")
            shown = " ".join(f"{k} {judge[k]:.2f}" for k in JUDGEMENTS) if judge else ""
            lines.append(f"{label} | {shown}".rstrip(" |"))
        return [Text(text="\n".join(lines))]
    return [Text(text=json.dumps(result)[:4000])]


class FloorAssistant:
    def __init__(self, engine, memory, client):
        self.engine, self.memory, self.client = engine, memory, client
        self.corpora = {}

    async def join(self, thread_id):
        thread = await self.client.get_thread(thread_id)
        machine = thread.metadata["machine"]
        self.corpora[thread_id] = f"{self.client.scope}:{machine}"
        await self.client.join(thread_id)

    async def answer_next(self):
        async for item in self.client.events():
            if isinstance(item, Message):
                return await self.answer(item)

    async def answer(self, message):
        thread, corpus = message.thread_id, self.corpora[message.thread_id]
        machine = corpus.split(":", 1)[1]
        await self.client.working(thread, label=f"Reading the {machine} manuals")
        run = await self.engine.run(
            "machinist",
            scope=self.client.scope,
            input=message.content,
            context=f"The machine is {machine} at the {self.client.scope} factory.",
            tools=MemoryTools(self.memory, corpus_id=corpus).only(
                "search", "read_source", "ingest"
            ),
        )
        for step in run.steps:
            await self.client.step(
                thread,
                name=step.tool,
                input=step.args,
                output=output(step.result),
                is_error=step.is_error,
                duration_ms=step.duration_ms,
                in_reply_to=message.id,
            )
        await self.client.send(thread, run.reply, in_reply_to=message.id)
        await self.client.idle(thread)


async def build_memory(factories):
    memory = Memory(
        rfnry_memory.InMemoryStorage(),
        embedding=VoyageProvider(),
        generation=AnthropicProvider(),
        decision=TypeSafeProvider(),
        pillars={"text", "vector", "graph"},
        relation_types=["PART_OF", "CONTACT_FOR", "SUPERSEDES"],
    )
    await memory.initialize()
    manuals = [
        (f"{factory}:{machine}", path)
        for factory, machines in factories.items()
        for machine in machines
        for path in sorted(Path("manuals", factory).glob(f"{machine}*.md"))
    ]
    await asyncio.gather(
        *(memory.ingest(corpus, name=path.name, text=path.read_text()) for corpus, path in manuals)
    )
    return memory


def build_engine():
    return Engine("agents", generation=AnthropicProvider(), tool_timeout=180)
