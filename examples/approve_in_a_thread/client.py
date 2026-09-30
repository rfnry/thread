from rfnry_thread import Client, Message

ANSWERS = ["allow", "always"]


def text(message: Message) -> str:
    return message.content[0].text


async def ana_answers(ana: Client, thread_id: str) -> None:
    async for item in ana.events():
        if isinstance(item, Message) and text(item).endswith("Reply allow, deny or always."):
            await ana.send(thread_id, ANSWERS.pop(0) if ANSWERS else "deny", in_reply_to=item.id)
