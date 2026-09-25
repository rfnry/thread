import asyncio
import json
from pathlib import Path

import rfnry_memory
from rfnry_engine import Engine
from rfnry_memory import Memory, MemoryTools
from rfnry_thread import (
    Document,
    Identity,
    InMemoryStorage,
    Message,
    NotFoundError,
    Server,
    Step,
    Text,
    resolve,
)

from providers import AnthropicProvider, TypeSafeProvider, VoyageProvider

ASSISTANT = Identity(role="assistant", id="floor-assistant", name="Floor assistant")
MACHINES = {"north": ["press-1", "press-2"], "south": ["furnace-1", "conveyor-1"]}
JUDGEMENTS = ("relevant", "evidence", "contradicts", "injection")


def text(content):
    return " ".join(part.text for part in content if isinstance(part, Text))


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


async def open_thread(operator, assistant, machine):
    factory = operator.scope
    if machine not in MACHINES[factory]:
        raise ValueError(f"{machine} is not on the {factory} factory's picker")
    thread = await operator.create_thread(
        thread_id=f"{machine}-{operator.identity.id}",
        metadata={"factory": factory, "machine": machine},
    )
    await operator.add_member(thread.id, assistant.client.identity)
    await assistant.join(thread.id)
    return thread.id


async def transcript(client, thread_id):
    thread = await client.get_thread(thread_id)
    members = [m.identity.name for m in await client.list_members(thread_id)]
    print(f"=== {client.scope} · {thread_id} · {thread.metadata} · members {members}")
    events = resolve(await client.history(thread_id, limit=200))
    replies = {e.in_reply_to for e in events if isinstance(e, Message) and e.in_reply_to}
    for event in events:
        if isinstance(event, Message) and event.id in replies:
            print(f"\n{event.author.name}: {text(event.content)}")
            for part in event.content:
                if isinstance(part, Document):
                    print(f"  [shared {part.name}, {len(part.data)} bytes]")
            for step in events:
                if isinstance(step, Step) and step.in_reply_to == event.id:
                    args = {k: v for k, v in step.input.items() if k != "text"}
                    print(f"    · {step.name} {json.dumps(args, ensure_ascii=False)}")
                    if step.name == "search":
                        for line in text(step.output).splitlines()[:3]:
                            print(f"        {line}")
            for reply in events:
                if isinstance(reply, Message) and reply.in_reply_to == event.id:
                    print(f"{reply.author.name}: {text(reply.content)}")
    print()


async def main() -> None:
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
        for factory, machines in MACHINES.items()
        for machine in machines
        for path in sorted(Path("manuals", factory).glob(f"{machine}*.md"))
    ]
    await asyncio.gather(
        *(memory.ingest(corpus, name=path.name, text=path.read_text()) for corpus, path in manuals)
    )
    engine = Engine("agents", generation=AnthropicProvider(), tool_timeout=180)

    server = Server(InMemoryStorage())
    ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="north")
    bruno = await server.connect(
        identity=Identity(role="user", id="bruno", name="Bruno"), scope="north"
    )
    diego = await server.connect(
        identity=Identity(role="user", id="diego", name="Diego"), scope="south"
    )
    north = FloorAssistant(engine, memory, await server.connect(identity=ASSISTANT, scope="north"))
    south = FloorAssistant(engine, memory, await server.connect(identity=ASSISTANT, scope="south"))

    press = await open_thread(ana, north, "press-2")
    await ana.send(
        press, "Our die-change card says 380 N·m for the press-2 die clamp bolts. Is that right?"
    )
    await north.answer_next()
    await ana.add_member(press, bruno.identity)
    await bruno.join(press)
    await bruno.send(press, "And after a die change, when do we re-torque them?")
    await north.answer_next()
    sampling = Path("shared", "press-2-oil-sampling.md").read_bytes()
    await bruno.send(
        press,
        [
            Text(text="Hydraulics sent the new oil sampling sheet for this press."),
            Document(media_type="text/markdown", name="press-2-oil-sampling.md", data=sampling),
        ],
    )
    await north.answer_next()
    await ana.send(press, "Where do I draw an oil sample from, and how much goes in the bottle?")
    await north.answer_next()

    furnace = await open_thread(diego, south, "furnace-1")
    await diego.send(furnace, "How often do we calibrate the thermocouples?")
    await south.answer_next()
    await diego.send(furnace, "What grade of gearbox oil does press-1 take?")
    await south.answer_next()

    await transcript(ana, press)
    await transcript(diego, furnace)

    print("=== tenancy")
    print(f"south's picker offers {MACHINES['south']}; its assistant pins {south.corpora}")
    try:
        await diego.get_thread(press)
    except NotFoundError as error:
        print(f"diego opening {press!r} from scope south: {error}")
    for machine in MACHINES["south"]:
        hits = await memory.search(f"south:{machine}", "press-1 gearbox oil", top_k=3)
        found = sorted({hit.text.splitlines()[0] for hit in hits.hits})
        print(f"south:{machine} searched for press-1 gearbox oil finds {found}")

    for client in [ana, bruno, diego, north.client, south.client]:
        await client.close()


asyncio.run(main())
