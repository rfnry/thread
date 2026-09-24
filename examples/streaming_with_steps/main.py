import asyncio

from rfnry_thread import (
    Identity,
    InMemoryStorage,
    Message,
    Server,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Working,
    resolve,
)


async def main() -> None:
    server = Server(InMemoryStorage())
    ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="demo")
    helper = await server.connect(
        identity=Identity(role="assistant", id="helper", name="Helper"), scope="demo"
    )
    thread = await ana.create_thread(thread_id="refund")
    await ana.add_member(thread.id, helper.identity)
    await ana.join(thread.id)
    await helper.join(thread.id)

    question = await ana.send(thread.id, "My invoice was charged twice.")
    await helper.working(thread.id, label="Reading rules/refunds.md")
    await helper.step(
        thread.id,
        name="Read",
        input={"path": "rules/refunds.md"},
        output=[Text(text="A double charge is refunded in full.")],
        duration_ms=12,
        in_reply_to=question.id,
    )
    answer = "The second charge is refunded in full, per the refunds rule."
    async with helper.stream(thread.id, in_reply_to=question.id) as reply:
        for word in answer.split(" "):
            await reply.write(word + " ")
            await asyncio.sleep(0.05)
        await reply.finish(reply.text.strip())
    await helper.idle(thread.id)

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
