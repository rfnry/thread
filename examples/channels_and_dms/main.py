import asyncio

from rfnry_thread import ForbiddenError, Message, resolve

from client import ANA, BRUNO, CARLA, HELPBOT, NAMES, text, timeline
from server import build_server


async def main() -> None:
    server = build_server()
    ana, bruno, carla, helpbot = [
        await server.connect(identity=who, scope="acme") for who in (ANA, BRUNO, CARLA, HELPBOT)
    ]

    general = await ana.create_thread(thread_id="general", metadata={"channel": "#general"})
    support = await ana.create_thread(thread_id="support", metadata={"channel": "#support"})
    dm = await ana.create_thread(thread_id="dm-ana-bruno", metadata={"dm": True})
    for member in (BRUNO, CARLA, HELPBOT):
        await ana.add_member(general.id, member)
    for member in (CARLA, HELPBOT):
        await ana.add_member(support.id, member)
    await ana.add_member(dm.id, BRUNO)
    secret = await bruno.create_thread(thread_id="dm-bruno-carla", metadata={"dm": True})
    await bruno.add_member(secret.id, CARLA)

    for thread in (general, support, dm):
        await ana.join(thread.id)
    await bruno.join(general.id)
    await bruno.join(dm.id)
    await carla.join(general.id)
    await carla.join(support.id)
    await helpbot.join(support.id)

    deploy = await bruno.send(general.id, "Deploy is at 3pm today.")
    await bruno.edit(general.id, deploy.id, "Deploy moved to 4pm today.")
    await carla.send(general.id, "Thanks, I'll tell the support rota.")
    ticket = await carla.send(support.id, "Customer 4471 can't log in since this morning.")
    await helpbot.working(support.id, label="Opening a ticket")
    await helpbot.send(
        support.id, "Opened T-1042 for customer 4471, priority high.", in_reply_to=ticket.id
    )
    await helpbot.idle(support.id)
    await ana.send(dm.id, "Can you watch the 4pm deploy? I'm out at 3:30.")
    wrong = await bruno.send(dm.id, "Carla, lunch at 1?")
    await bruno.retract(dm.id, wrong.id)
    await bruno.send(dm.id, "Sure, I'll watch it.")
    await bruno.send(secret.id, "Carla, lunch at 1?")
    latest = (await ana.history(general.id))[-1]
    await ana.read(general.id, up_to_seq=latest.seq)
    await carla.read(support.id, up_to_seq=ticket.seq)
    await carla.leave(support.id)

    print("ana's event stream:")
    await timeline(ana)

    print("\n#general, resolved:")
    for event in resolve(await ana.history(general.id)):
        if isinstance(event, Message):
            print(f"  {event.seq} {event.author.name}: {text(event.content)}")

    print("\nthe dm, resolved (the retracted message is gone):")
    for event in resolve(await bruno.history(dm.id)):
        if isinstance(event, Message):
            print(f"  {event.seq} {event.author.name}: {text(event.content)}")

    print("\nread cursors:")
    for thread in (general, support):
        cursors = {e.author.name: e.up_to_seq for e in await ana.history(thread.id, kinds=["read"])}
        messages = [e for e in resolve(await ana.history(thread.id)) if isinstance(e, Message)]
        for member in await ana.list_members(thread.id):
            seen = cursors.get(member.identity.name, 0)
            unread = sum(1 for m in messages if m.seq > seen and m.author.id != member.identity.id)
            print(f"  {NAMES[thread.id]:<9} {member.identity.name:<8} at {seen:<3} unread {unread}")

    print("\npresent now:")
    for thread in (general, support, dm):
        names = [who.name for who in await ana.present(thread.id)]
        print(f"  {NAMES[thread.id]:<9} {', '.join(names)}")

    print("\nana's threads:", [NAMES[t.id] for t in await ana.list_threads()])
    try:
        await ana.history(secret.id)
    except ForbiddenError as error:
        print(f"ana reads bruno and carla's dm: {error}")

    for client in (ana, bruno, carla, helpbot):
        await client.close()


asyncio.run(main())
