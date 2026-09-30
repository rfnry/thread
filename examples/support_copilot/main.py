import asyncio
import json
from pathlib import Path

from rfnry_engine import Review
from rfnry_thread import Document, Identity, Image, Message, Step, Text, resolve

from client import COMPLAINT, CORRECTION, DISAGREEMENT, INVOICE, invoice_pdf, text
from harness import advise, build_engine, build_refinery
from server import build_server


async def main() -> None:
    refinery = await build_refinery()
    engine = build_engine(refinery)
    server = build_server()
    lena = await server.connect(
        identity=Identity(role="user", id="lena", name="Lena"), scope="harbor"
    )
    marta = await server.connect(
        identity=Identity(role="user", id="marta", name="Marta"), scope="harbor"
    )
    copilot = await server.connect(
        identity=Identity(role="assistant", id="copilot", name="Copilot"), scope="harbor"
    )
    case = await lena.create_thread(thread_id="case-a-20931", metadata={"order": "A-20931"})
    await lena.add_member(case.id, marta.identity)
    await lena.add_member(case.id, copilot.identity)
    await copilot.join(case.id)

    await lena.send(
        case.id,
        [
            Text(text=COMPLAINT),
            Image(media_type="image/png", data=Path("receipt.png").read_bytes()),
            Document(media_type="application/pdf", name="invoice.pdf", data=invoice_pdf(INVOICE)),
        ],
    )
    runs = {}
    async for item in copilot.events():
        if isinstance(item, Message) and item.author.role == "user":
            reply, run = await advise(engine, copilot, item)
            runs[reply.id] = run.run_id
            break

    objection = await marta.send(case.id, DISAGREEMENT, in_reply_to=reply.id)
    await refinery.review(
        "harbor",
        runs[objection.in_reply_to],
        Review(
            stance="objection",
            comment=f"{marta.identity.name} in {case.id}",
            correction=CORRECTION,
        ),
    )

    print(f"=== thread {case.id}")
    events = resolve(await lena.history(case.id, limit=200))
    for event in events:
        if isinstance(event, Message):
            print(f"\n{event.author.name}: {text(event.content)}")
            for part in event.content:
                if isinstance(part, Image | Document):
                    print(f"  [{part.type} {part.media_type}, {len(part.data)} bytes]")
        elif isinstance(event, Step):
            shown = event.input.get("path") or event.input.get("agent") or ""
            print(f"    · {event.name} {shown}")
            if event.name == "Delegate" and not event.is_error:
                print(f"        input: {event.input.get('input')}")
                for question, answer in json.loads(text(event.output)).items():
                    shown = answer.get("choice") or f"{answer.get('probability'):.2f}"
                    print(f"        {question}: {shown} (confidence {answer['confidence']:.2f})")

    print("\n=== refinery")
    for row in await refinery.storage.runs("harbor"):
        parent = f" under {row.parent_run_id[:8]}" if row.parent_run_id else ""
        print(f"run   {row.run_id[:8]} {row.kind:<10} {row.agent}{parent} · {row.duration_ms} ms")
    for row in await refinery.storage.labels("harbor"):
        print(f"label {row.run_id[:8]} {row.source:<10} {json.dumps(row.body['review'])}")

    for client in (lena, marta, copilot):
        await client.close()


asyncio.run(main())
