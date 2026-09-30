import asyncio

from rfnry_thread import Identity, Message, resolve

from client import ana_answers, text
from harness import analyze, build_engine
from server import build_server


async def main() -> None:
    server = build_server()
    engine = build_engine()
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

    run = await analyze(engine, analyst, thread.id, question)
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
