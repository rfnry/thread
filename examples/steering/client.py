import asyncio
import json

from rfnry_thread import Identity, Message, Step, Text, Working

ANA = Identity(role="user", id="ana", name="Ana")


def text(content):
    return " ".join(part.text for part in content if isinstance(part, Text))


def terminal(ana, thread_id):
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
        return await ana.send(thread_id, content)

    return show, until, say
