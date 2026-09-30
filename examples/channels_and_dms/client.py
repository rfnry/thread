import asyncio

from rfnry_thread import Edit, Identity, Left, Message, Present, Read, Retract, Working

ANA = Identity(role="user", id="ana", name="Ana")
BRUNO = Identity(role="user", id="bruno", name="Bruno")
CARLA = Identity(role="user", id="carla", name="Carla")
HELPBOT = Identity(role="assistant", id="helpbot", name="Helpbot")
NAMES = {"general": "#general", "support": "#support", "dm-ana-bruno": "@bruno"}


def text(content):
    return " ".join(part.text for part in content if part.type == "text")


def line(item):
    where = NAMES.get(item.thread_id, item.thread_id)
    match item:
        case Message(author=author):
            return f"{where:<9} {author.name}: {text(item.content)}"
        case Edit(author=author):
            return f"{where:<9} {author.name} edited: {text(item.content)}"
        case Retract(author=author):
            return f"{where:<9} {author.name} unsent a message"
        case Read(author=author, up_to_seq=seq):
            return f"{where:<9} {author.name} read up to {seq}"
        case Present(sender=sender):
            return f"{where:<9} {sender.name} is here"
        case Left(sender=sender):
            return f"{where:<9} {sender.name} left"
        case Working(sender=sender, label=label):
            return f"{where:<9} {sender.name} is {label.lower()}"
    return None


async def timeline(client):
    stream = client.events(include_own=True)
    while True:
        try:
            item = await asyncio.wait_for(anext(stream), 0.2)
        except TimeoutError:
            return
        if shown := line(item):
            print(f"  {shown}")
