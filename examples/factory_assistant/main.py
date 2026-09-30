import asyncio
from pathlib import Path

from rfnry_thread import Document, Identity, NotFoundError, Text

from client import MACHINES, open_thread, transcript
from harness import ASSISTANT, FloorAssistant, build_engine, build_memory
from server import build_server


async def main() -> None:
    memory = await build_memory(MACHINES)
    engine = build_engine()

    server = build_server()
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
