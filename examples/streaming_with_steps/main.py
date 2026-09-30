import asyncio

from rfnry_thread import (
    Identity,
    Message,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Working,
    resolve,
)

from client import helper_answers
from server import build_server


async def main() -> None:
    server = build_server()
    ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="demo")
    helper = await server.connect(
        identity=Identity(role="assistant", id="helper", name="Helper"), scope="demo"
    )
    thread = await ana.create_thread(thread_id="refund")
    await ana.add_member(thread.id, helper.identity)
    await ana.join(thread.id)
    await helper.join(thread.id)

    question = await ana.send(thread.id, "My invoice was charged twice.")
    await helper_answers(helper, thread.id, question)

    async for item in ana.events():
        match item:
            case Working(label=label):
                print(f"[helper is working: {label}]")
            case Step(name=name, duration_ms=ms):
                print(f"[step {name} · {ms} ms]")
            case StreamStart():
                print("helper: ", end="")
            case StreamDelta(text=text):
                print(text, end="", flush=True)
            case Message(author=author) if author.role == "assistant":
                print(f"\n[stored as event {item.seq}, client_id {item.client_id}]")
            case StreamEnd():
                break

    print("\nthe log, resolved:")
    for event in resolve(await ana.history(thread.id)):
        under = " (under the question)" if event.in_reply_to == question.id else ""
        print(f"  {event.seq} {event.kind}{under}")
    await ana.close()
    await helper.close()


asyncio.run(main())
