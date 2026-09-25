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
    Server,
    Step,
    Text,
    resolve,
)

from providers import AnthropicProvider, TypeSafeProvider, VoyageProvider

ASSISTANT = Identity(role="assistant", id="floor-assistant", name="Floor assistant")
MACHINES = {"north": ["press-1", "press-2"], "south": ["furnace-1", "conveyor-1"]}


def text(content):
    return " ".join(part.text for part in content if isinstance(part, Text))


def output(result):
    return result if isinstance(result, list) else [Text(text=json.dumps(result)[:4000])]


class FloorAssistant:
    def __init__(self, engine, memory, client, factory):
        self.engine, self.memory, self.client, self.factory = engine, memory, client, factory

    async def answer_next(self):
        async for item in self.client.events():
            if isinstance(item, Message):
                return await self.answer(item)

    async def answer(self, message):
        thread, question = message.thread_id, text(message.content)
        picked = await self.engine.run(
            f"{self.factory}_machines", scope=self.factory, input=question
        )
        machine = picked.answers["machine"]
        await self.client.step(
            thread,
            name="pick_machine",
            input={"agent": f"{self.factory}_machines"},
            output=[Text(text=f"{machine.choice} (confidence {machine.confidence:.2f})")],
            in_reply_to=message.id,
        )
        if machine.choice not in MACHINES[self.factory]:
            mine = ", ".join(MACHINES[self.factory])
            await self.client.send(
                thread,
                f"That isn't one of the {self.factory} factory's machines ({mine}), "
                "so I have no manual for it.",
                in_reply_to=message.id,
            )
            return
        corpus = f"{self.factory}:{machine.choice}"
        shared = [part for part in message.content if isinstance(part, Document)]
        for document in shared:
            ingested = await self.memory.ingest(
                corpus, name=document.name, text=document.data.decode()
            )
            await self.client.step(
                thread,
                name="ingest",
                input={"corpus": corpus, "name": document.name},
                output=[Text(text=f"{ingested.source.chunks} chunks")],
                in_reply_to=message.id,
            )
        if shared:
            names = ", ".join(d.name for d in shared)
            await self.client.send(
                thread, f"Filed {names} with the {machine.choice} manuals.", in_reply_to=message.id
            )
            return
        await self.client.working(thread, label=f"Reading the {machine.choice} manuals")
        run = await self.engine.run(
            "machinist",
            scope=self.factory,
            input=question,
            context=f"The machine is {machine.choice} at the {self.factory} factory.",
            tools=MemoryTools(self.memory, corpus_id=corpus).only("search", "read_source"),
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


async def transcript(client, thread_id):
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
                    shown = next(iter(step.input.values()), "")
                    detail = text(step.output) if step.name in ("pick_machine", "ingest") else ""
                    print(f"    · {step.name} {shown} {detail}".rstrip())
            for reply in events:
                if isinstance(reply, Message) and reply.in_reply_to == event.id:
                    print(f"{reply.author.name}: {text(reply.content)}")


async def main() -> None:
    memory = Memory(rfnry_memory.InMemoryStorage(), embedding=VoyageProvider())
    await memory.initialize()
    for factory, machines in MACHINES.items():
        for machine in machines:
            manual = Path("manuals", factory, f"{machine}.md")
            await memory.ingest(f"{factory}:{machine}", name=manual.name, text=manual.read_text())
    engine = Engine("agents", generation=AnthropicProvider(), decision=TypeSafeProvider())

    server = Server(InMemoryStorage())
    ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="north")
    bruno = await server.connect(
        identity=Identity(role="user", id="bruno", name="Bruno"), scope="north"
    )
    diego = await server.connect(
        identity=Identity(role="user", id="diego", name="Diego"), scope="south"
    )
    assistants = {}
    for factory, operators in (("north", [ana, bruno]), ("south", [diego])):
        client = await server.connect(identity=ASSISTANT, scope=factory)
        floor = await operators[0].create_thread(thread_id="floor")
        for member in [client, *operators[1:]]:
            await operators[0].add_member(floor.id, member.identity)
        await client.join(floor.id)
        assistants[factory] = FloorAssistant(engine, memory, client, factory)
    north, south = assistants["north"], assistants["south"]

    await ana.send("floor", "Press 1 failed its light curtain test this morning. What now?")
    await north.answer_next()
    die_change = Path("shared", "press-2-die-change.md").read_bytes()
    await bruno.send(
        "floor",
        [
            Text(text="New manual for press-2: the die change procedure, rev B."),
            Document(media_type="text/markdown", name="press-2-die-change.md", data=die_change),
        ],
    )
    await north.answer_next()
    await ana.send("floor", "What torque do the press-2 die clamp bolts take, and when again?")
    await north.answer_next()

    await diego.send("floor", "How often do we calibrate the thermocouples on furnace 1?")
    await south.answer_next()
    await diego.send("floor", "What grade of gearbox oil does press-1 take?")
    await south.answer_next()

    print("=== north · floor")
    await transcript(ana, "floor")
    print("\n=== south · floor")
    await transcript(diego, "floor")

    print("\n=== tenancy")
    for client in (ana, diego):
        members = [m.identity.name for m in await client.list_members("floor")]
        events = await client.history("floor", limit=200)
        print(f"'floor' in scope {client.scope}: {len(events)} events, members {members}")
    for machine in MACHINES["south"]:
        hits = await memory.search(f"south:{machine}", "press-1 gearbox oil", top_k=3)
        found = sorted({hit.text.splitlines()[0] for hit in hits.hits})
        print(f"south:{machine} searched for press-1 gearbox oil finds {found}")

    for client in [ana, bruno, diego, *(a.client for a in assistants.values())]:
        await client.close()


asyncio.run(main())
