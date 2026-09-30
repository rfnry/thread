import asyncio
import json

from pydantic import BaseModel
from rfnry_engine import Engine, Refinery, Review, Stop, Tool, Tools
from rfnry_engine import InMemoryStorage as RunStorage
from rfnry_thread import Cancel, Edit, Identity, Message, Text

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


class City(BaseModel):
    city: str


def build_refinery():
    return Refinery(RunStorage())


def build_engine(refinery):
    return Engine("agents", generation=AnthropicProvider(), refinery=refinery)


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
