import json

from rfnry_thread import Document, Message, Step, Text, resolve

MACHINES = {"north": ["press-1", "press-2"], "south": ["furnace-1", "conveyor-1"]}


def text(content):
    return " ".join(part.text for part in content if isinstance(part, Text))


async def open_thread(operator, assistant, machine):
    factory = operator.scope
    if machine not in MACHINES[factory]:
        raise ValueError(f"{machine} is not on the {factory} factory's picker")
    thread = await operator.create_thread(
        thread_id=f"{machine}-{operator.identity.id}",
        metadata={"factory": factory, "machine": machine},
    )
    await operator.add_member(thread.id, assistant.client.identity)
    await assistant.join(thread.id)
    return thread.id


async def transcript(client, thread_id):
    thread = await client.get_thread(thread_id)
    members = [m.identity.name for m in await client.list_members(thread_id)]
    print(f"=== {client.scope} · {thread_id} · {thread.metadata} · members {members}")
    events = resolve(await client.history(thread_id, limit=200))
    replies = {e.in_reply_to for e in events if isinstance(e, Message) and e.in_reply_to}
    for event in events:
        if isinstance(event, Message) and event.id in replies:
            print(f"\n{event.author.name}: {text(event.content)}")
            for part in event.content:
                if isinstance(part, Document):
                    print(f"  [shared {part.name}, {len(part.data)} bytes]")
            for step in events:
                if isinstance(step, Step) and step.in_reply_to == event.id:
                    args = {k: v for k, v in step.input.items() if k != "text"}
                    print(f"    · {step.name} {json.dumps(args, ensure_ascii=False)}")
                    if step.name == "search":
                        for line in text(step.output).splitlines()[:3]:
                            print(f"        {line}")
            for reply in events:
                if isinstance(reply, Message) and reply.in_reply_to == event.id:
                    print(f"{reply.author.name}: {text(reply.content)}")
    print()
