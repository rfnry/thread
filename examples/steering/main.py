import asyncio

from rfnry_thread import Message, Working

from client import ANA, terminal, text
from harness import PLANNER, Desk, build_engine, build_refinery
from server import build_server


async def main() -> None:
    server = build_server()
    refinery = build_refinery()
    engine = build_engine(refinery)
    ana = await server.connect(identity=ANA, scope="demo")
    me = await server.connect(identity=PLANNER, scope="demo")
    thread = await ana.create_thread(thread_id="offsite")
    await ana.add_member(thread.id, PLANNER)
    await ana.join(thread.id)
    await me.join(thread.id)
    desk = Desk(me, thread.id, engine, refinery)
    listening = asyncio.create_task(desk.listen())
    show, until, say = terminal(ana, thread.id)
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
