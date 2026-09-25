import asyncio
import json
from pathlib import Path

import rfnry_engine
from rfnry_engine import Engine, Refinery, Review, ToolCallStarted
from rfnry_thread import (
    Document,
    Identity,
    Image,
    InMemoryStorage,
    Message,
    Server,
    Step,
    Text,
    resolve,
)

from providers import AnthropicProvider, TypeSafeProvider

TODAY = "2026-09-25"
COMPLAINT = (
    "New case from Joana Ribeiro, by email this morning: 'My Aurora espresso machine arrived on "
    "the 12th with a cracked water tank and it leaks all over the counter. I want my money back.' "
    "She attached her receipt and the invoice. What do we do?"
)
CORRECTION = (
    "Offer to ship her a new water tank at no cost first; only if she still wants her money back, "
    "escalate to a team lead, since the total is over $500."
)
DISAGREEMENT = f"I disagree. The tank is a spare part we stock. {CORRECTION}"
INVOICE = [
    "HARBOR HOME APPLIANCES - INVOICE INV-77120",
    "Order A-20931",
    "Ordered 2026-09-08    Delivered 2026-09-12",
    "Bill to Joana Ribeiro, Rua das Flores 18, Porto",
    "",
    "Aurora espresso machine     1 x 549.00",
    "Shipping                        0.00",
    "Tax 8%                         43.92",
    "Total USD                     592.92",
    "",
    "Paid by VISA **** 4417 on 2026-09-08",
]


def invoice_pdf(lines):
    stream = "BT /F1 11 Tf 56 780 Td 16 TL " + "".join(f"({line}) Tj T* " for line in lines) + "ET"
    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
        "/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>",
        f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream",
    ]
    pdf, offsets = b"%PDF-1.4\n", []
    for number, body in enumerate(objects, 1):
        offsets.append(len(pdf))
        pdf += f"{number} 0 obj\n{body}\nendobj\n".encode()
    table = "".join(f"{offset:010} 00000 n \n" for offset in offsets)
    size = len(objects) + 1
    return (
        pdf
        + (
            f"xref\n0 {size}\n0000000000 65535 f \n{table}"
            f"trailer\n<< /Size {size} /Root 1 0 R >>\nstartxref\n{len(pdf)}\n%%EOF\n"
        ).encode()
    )


def text(content):
    return " ".join(part.text for part in content if isinstance(part, Text))


def output(result):
    if isinstance(result, str):
        return [Text(text=result)]
    return result if isinstance(result, list) else [Text(text=json.dumps(result))]


async def advise(engine, copilot, case):
    async def mirror(event):
        if isinstance(event, ToolCallStarted):
            await copilot.working(case.thread_id, label=event.tool)

    run = await engine.run(
        "copilot",
        scope=copilot.scope,
        input=case.content,
        context=f"Today is {TODAY}. The support agent asking is {case.author.name}.",
        on_event=mirror,
    )
    for step in run.steps:
        await copilot.step(
            case.thread_id,
            name=step.tool,
            input=step.args,
            output=output(step.result),
            is_error=step.is_error,
            duration_ms=step.duration_ms,
            in_reply_to=case.id,
        )
    reply = await copilot.send(case.thread_id, run.reply, in_reply_to=case.id)
    await copilot.idle(case.thread_id)
    return reply, run


async def main() -> None:
    storage = rfnry_engine.InMemoryStorage()
    refinery = Refinery(storage)
    await refinery.initialize()
    engine = Engine(
        "agents", generation=AnthropicProvider(), decision=TypeSafeProvider(), refinery=refinery
    )
    server = Server(InMemoryStorage())
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
    for row in await storage.runs("harbor"):
        parent = f" under {row.parent_run_id[:8]}" if row.parent_run_id else ""
        print(f"run   {row.run_id[:8]} {row.kind:<10} {row.agent}{parent} · {row.duration_ms} ms")
    for row in await storage.labels("harbor"):
        print(f"label {row.run_id[:8]} {row.source:<10} {json.dumps(row.body['review'])}")

    for client in (lena, marta, copilot):
        await client.close()


asyncio.run(main())
