import asyncio

from rfnry_engine import Engine
from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server, Text, Thread

from providers import decision, generation
from utils import mask, slug

CLINICS = ("clinic-northside", "clinic-riverside")
CALLBACK = "Triage is not sure who should take this: a nurse will call the patient back."
ASSISTANT = Identity(role="assistant", id="triage", name="Triage assistant")


async def redact(author: Identity, thread: Thread, content: list) -> list:
    return [Text(text=mask(part.text)) if part.type == "text" else part for part in content]


server = Server(InMemoryStorage(), redact=redact)
engine = Engine("agents", generation=generation(), decision=decision())
assistants: dict[str, Client] = {}
working: set[asyncio.Task] = set()


def person(name: str) -> Identity:
    return Identity(role="user", id=name.lower(), name=name)


async def serve(socket, name: str, clinic: str) -> None:
    await server.serve(socket, identity=person(name), scope=clinic)


async def open_thread(clinic: str, name: str, patient: str) -> dict:
    metadata = {"patient": patient, "handled_by": "assistant"}
    thread = await server.create_thread(
        clinic, thread_id=f"patient-{slug(patient)}", metadata=metadata
    )
    await server.add_member(clinic, thread.id, person(name))
    await server.add_member(clinic, thread.id, ASSISTANT)
    return thread.model_dump(mode="json")


async def handoff(clinic: str, thread_id: str) -> dict:
    thread = await assistants[clinic].get_thread(thread_id)
    handed = await server.update_thread(
        clinic, thread_id, thread.metadata | {"handled_by": "people"}
    )
    return handed.model_dump(mode="json")


async def listen() -> None:
    await asyncio.gather(*(assist(clinic) for clinic in CLINICS))


async def assist(clinic: str) -> None:
    me = assistants[clinic] = await server.connect(identity=ASSISTANT, scope=clinic)
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id, since=0)
        elif isinstance(item, Message):
            thread = await me.get_thread(item.thread_id)
            if thread.metadata["handled_by"] == "assistant":
                task = asyncio.create_task(triage(me, item))
                working.add(task)
                task.add_done_callback(working.discard)


async def triage(me: Client, message: Message) -> None:
    await me.working(message.thread_id, label="Triaging")
    said = "\n".join(part.text for part in message.content if part.type == "text")
    run = await engine.run("triage", scope=me.scope, input={"message": said})
    department, urgent = run.answers["department"], run.answers["urgent"]
    routed = run.routed_to
    await me.send(
        message.thread_id,
        f"Triage: {department.choice} ({department.confidence:.2f}), "
        f"urgent {urgent.probability:.2f}.\n\n{routed.reply if routed else CALLBACK}",
        in_reply_to=message.id,
        metadata={"run_id": run.run_id, "routed_to": routed.agent if routed else None},
    )
    await me.idle(message.thread_id)
