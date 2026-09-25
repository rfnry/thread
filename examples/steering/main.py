import asyncio
import json

from pydantic import BaseModel
from rfnry_engine import Engine, Refinery, Review, Stop, Tool, Tools
from rfnry_engine import InMemoryStorage as RunStorage
from rfnry_thread import (
    Cancel,
    Edit,
    Identity,
    InMemoryStorage,
    Message,
    Server,
    Step,
    Text,
    Working,
)

from providers import AnthropicProvider

VENUES = {
    "Lisbon": {"venue": "Palácio Chiado loft", "price_eur": 4200, "sleeps": 20},
    "Porto": {"venue": "Ribeira warehouse", "price_eur": 2600, "sleeps": 16},
    "Faro": {"venue": "Ria Formosa lodge", "price_eur": 2900, "sleeps": 18},
}
CATERING = {
    "Lisbon": ("Cozinha Aberta, €38 a head", 1),
    "Porto": ("Mesa do Douro, €31 a head", 1),
    "Faro": ("Sal & Alga, €35 a head — their booking line is slow", 8),
}
PLANNER = Identity(role="assistant", id="planner", name="Planner")
ANA = Identity(role="user", id="ana", name="Ana")


class City(BaseModel):
    city: str


def text(content):
    return " ".join(part.text for part in content if isinstance(part, Text))


class Desk:
    def __init__(self, me, thread_id, engine, refinery):
        self.me, self.thread_id, self.engine, self.refinery = me, thread_id, engine, refinery
        self.conversation = []
        self.turns = {}
        self.stop, self.inbox, self.work, self.rewinding = Stop(), asyncio.Queue(), None, None
        self.tools = Tools(
            [
                Tool(
                    "look_up_venue",
                    "Find a venue for the offsite in a city",
                    body=self.venue,
                    args=City,
                ),
                Tool("look_up_catering", "Find a caterer in a city", body=self.catering, args=City),
            ]
        )

    async def venue(self, args):
        await self.me.working(self.thread_id, label=f"looking up a venue in {args['city']}")
        await asyncio.sleep(2)
        return {"city": args["city"], **VENUES.get(args["city"], {"venue": "none found"})}

    async def catering(self, args):
        await self.me.working(self.thread_id, label=f"looking up catering in {args['city']}")
        found, seconds = CATERING.get(args["city"], ("none found", 1))
        await asyncio.sleep(seconds)
        return found

    def busy(self):
        return self.work is not None and not self.work.done()

    def start(self, message_id, content, history):
        self.turns[message_id] = (len(history), None)
        self.stop, self.inbox = Stop(), asyncio.Queue()
        self.work = asyncio.create_task(self.answer(message_id, content, history))

    async def answer(self, message_id, content, history):
        run = await self.engine.run(
            "planner",
            scope="demo",
            input=content,
            history=history,
            tools=self.tools,
            stop=self.stop,
            inbox=self.inbox,
        )
        self.conversation = run.conversation
        self.turns[message_id] = (self.turns[message_id][0], run.run_id)
        for step in run.steps:
            await self.me.step(
                self.thread_id,
                name=step.tool,
                input=step.args,
                is_error=step.is_error,
                output=[
                    Text(
                        text=step.result
                        if isinstance(step.result, str)
                        else json.dumps(step.result, ensure_ascii=False)
                    )
                ],
                duration_ms=step.duration_ms,
                in_reply_to=message_id,
            )
        said = run.reply or "(nothing yet)"
        if run.stopped:
            finished = sum(not s.stopped for s in run.steps)
            said = (
                f"Stopped. Kept {finished} finished step(s); the rest was cut. {run.reply}".strip()
            )
        await self.me.send(self.thread_id, said, in_reply_to=message_id)
        await self.me.idle(self.thread_id)
        read = [m for m in run.conversation[len(history) + 1 :] if m.role == "user"]
        print(f"  [engine] stopped={run.stopped} steps={len(run.steps)} read mid-run={len(read)}")
        leftovers = []
        while not self.inbox.empty():
            leftovers.extend(self.inbox.get_nowait())
        if leftovers:
            self.start(message_id, leftovers, self.conversation)

    async def rewind(self, edit):
        if self.busy():
            self.stop.now()
            await self.work
        cut, abandoned = self.turns[edit.target]
        self.start(edit.target, edit.content, self.conversation[:cut])
        await self.work
        retry = self.conversation[-1].content
        if abandoned is not None:
            await self.refinery.review(
                "demo", abandoned, Review(stance="objection", correction=retry)
            )
            print(
                f"  [refinery] abandoned run {abandoned[:8]} reviewed; the retry is its correction"
            )

    async def listen(self):
        async for item in self.me.events():
            if isinstance(item, Message) and item.author.role == "user":
                if self.busy():
                    self.inbox.put_nowait(item.content)
                else:
                    self.start(item.id, item.content, self.conversation)
            elif isinstance(item, Cancel) and item.to.id == self.me.identity.id and self.busy():
                self.stop.now()
            elif isinstance(item, Edit) and item.target in self.turns:
                self.rewinding = asyncio.create_task(self.rewind(item))


async def main() -> None:
    server = Server(InMemoryStorage())
    refinery = Refinery(RunStorage())
    engine = Engine("agents", generation=AnthropicProvider(), refinery=refinery)
    ana = await server.connect(identity=ANA, scope="demo")
    me = await server.connect(identity=PLANNER, scope="demo")
    thread = await ana.create_thread(thread_id="offsite")
    await ana.add_member(thread.id, PLANNER)
    await ana.join(thread.id)
    await me.join(thread.id)
    desk = Desk(me, thread.id, engine, refinery)
    listening = asyncio.create_task(desk.listen())
    seen = asyncio.Queue()

    async def show():
        async for item in ana.events():
            if isinstance(item, Working):
                print(f"  … {item.label}")
            elif isinstance(item, Step):
                print(f"  [step {item.name} {json.dumps(item.input)} → {text(item.output)}]")
            elif isinstance(item, Message):
                print(f"Planner: {text(item.content)}\n")
            seen.put_nowait(item)

    async def until(kind, count=1):
        while count:
            if isinstance(await seen.get(), kind):
                count -= 1

    async def say(content):
        print(f"Ana: {content}")
        return await ana.send(thread.id, content)

    printer = asyncio.create_task(show())

    print("— 1. a follow-up while it works, then a pause —")
    await say(
        "Plan our 3-day offsite for 14 people. Look up a venue in Lisbon, Porto and Faro, "
        "one city at a time, then recommend one."
    )
    await until(Working)
    await say("Also: the venue budget is €3,000 max — drop anything over that.")
    await until(Working)
    await say("Hold on, let's pause here — tell me where things stand.")
    await until(Message)

    print("— 2. a second task, stopped with a cancel signal —")
    catering = await say("Now look up catering in Porto, then in Faro.")
    await until(Working, 2)
    await asyncio.sleep(2)
    print("Ana: [cancel]")
    await ana.cancel(thread.id, to=PLANNER)
    await until(Message)

    print("— 3. rewind: Ana edits her last request and the planner continues from there —")
    edited = "Skip catering. Which venue do you recommend, and why, in one sentence?"
    print(f"Ana: [edits “{text(catering.content)}” → “{edited}”]")
    await ana.edit(thread.id, catering.id, edited)
    await until(Message)
    await desk.rewinding

    runs = await refinery.storage.runs("demo")
    print(f"refinery: {len(runs)} runs recorded, stopped: {[r.body['stopped'] for r in runs]}")
    listening.cancel()
    printer.cancel()
    await ana.close()
    await me.close()


asyncio.run(main())
