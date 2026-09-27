import re
from typing import Any

import pytest

from rfnry_thread.client import Client
from rfnry_thread.errors import ConfigurationError, InputError, ThreadException
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import (
    Custom,
    Edit,
    Identity,
    Image,
    Message,
    Part,
    Step,
    StreamDelta,
    StreamEnd,
    Text,
    Thread,
)
from rfnry_thread.resolve import resolve
from rfnry_thread.server import Server

from .factories import ANA, HELPER
from .test_client import drain, pair

CARD = "4111111111111111"
MASKED = "************1111"


def mask(text: str) -> str:
    return re.sub(r"\d{12,19}", lambda m: "*" * (len(m[0]) - 4) + m[0][-4:], text)


class Redactor:
    def __init__(self) -> None:
        self.calls: list[tuple[Identity, Thread, list[Part]]] = []

    async def __call__(self, author: Identity, thread: Thread, content: list[Part]) -> list[Part]:
        self.calls.append((author, thread, content))
        return [Text(text=mask(p.text)) if isinstance(p, Text) else p for p in content]


async def served(redact: Any) -> tuple[InMemoryStorage, Server, Client, Client]:
    storage = InMemoryStorage()
    server = Server(storage, redact=redact)
    ana, helper = await pair(server)
    return storage, server, ana, helper


async def stored(storage: InMemoryStorage) -> str:
    return str(await storage.list_events("acme", "t1", limit=500))


async def test_what_is_stored_delivered_returned_and_resolved_is_the_redacted_form() -> None:
    redactor = Redactor()
    storage, _, ana, helper = await served(redactor)
    image = Image(media_type="image/png", url="https://files.example.com/card.png")
    sent = await ana.send("t1", [Text(text=f"My card is {CARD}."), image])
    assert sent.content == [Text(text=f"My card is {MASKED}."), image]
    received = [i for i in await drain(helper) if isinstance(i, Message)]
    assert [m.content for m in received] == [sent.content]
    edited = await ana.edit("t1", sent.id, [Text(text=f"Sorry, it is {CARD[::-1]}."), image])
    assert edited.content == [Text(text=f"Sorry, it is {mask(CARD[::-1])}."), image]
    assert [i.content for i in await drain(helper) if isinstance(i, Edit)] == [edited.content]
    view = [e for e in resolve(await helper.history("t1")) if isinstance(e, Message)]
    assert [m.content for m in view] == [edited.content]
    assert CARD not in await stored(storage) and CARD[::-1] not in await stored(storage)
    author, thread, content = redactor.calls[0]
    assert (author, thread.id, content[1]) == (ANA, "t1", image)
    assert len(redactor.calls) == 2


async def test_the_message_a_stream_ends_in_is_redacted_and_its_deltas_are_not() -> None:
    storage, _, ana, helper = await served(Redactor())
    async with helper.stream("t1") as reply:
        await reply.write(f"Your card {CARD} ")
        await reply.write("was refunded.")
    items = await drain(ana)
    live = "".join(i.text for i in items if isinstance(i, StreamDelta))
    assert live == f"Your card {CARD} was refunded."
    message = next(i for i in items if isinstance(i, Message))
    assert message.content == [Text(text=f"Your card {MASKED} was refunded.")]
    assert message.client_id == reply.id and CARD not in await stored(storage)


async def test_metadata_custom_payloads_and_steps_are_the_writers_own_and_not_redacted() -> None:
    redactor = Redactor()
    _, _, ana, helper = await served(redactor)
    sent = await ana.send("t1", "hi", metadata={"card": CARD})
    custom = await ana.custom("t1", "payment", {"card": CARD})
    step = await helper.step("t1", name="Charge", input={"card": CARD}, output=[Text(text=CARD)])
    assert sent.metadata == {"card": CARD}
    assert isinstance(custom, Custom) and custom.payload == {"card": CARD}
    assert isinstance(step, Step) and step.input == {"card": CARD}
    assert step.output == [Text(text=CARD)]
    assert len(redactor.calls) == 1


async def test_a_repeated_client_id_returns_the_stored_event_without_redacting_again() -> None:
    redactor = Redactor()
    _, _, ana, _ = await served(redactor)
    first = await ana.send("t1", f"card {CARD}", client_id="c1")
    again = await ana.send("t1", f"card {CARD}", client_id="c1")
    assert again == first and len(redactor.calls) == 1
    assert [e.id for e in await ana.history("t1") if isinstance(e, Message)] == [first.id]


async def test_raising_refuses_the_write_and_the_sender_gets_the_error_once() -> None:
    calls: list[str] = []

    async def refuse(author: Identity, thread: Thread, content: list[Part]) -> list[Part]:
        calls.append(author.id)
        text = "".join(p.text for p in content if isinstance(p, Text))
        if "ssn" in text:
            raise InputError("redact: a social security number cannot be sent here")
        raise RuntimeError(f"scanner is down while reading {text}")

    storage, _, ana, helper = await served(refuse)
    with pytest.raises(InputError, match="^redact: a social security number cannot be sent here$"):
        await ana.send("t1", "my ssn is 078-05-1120")
    with pytest.raises(ThreadException, match="^server: request failed$") as failed:
        await ana.send("t1", f"card {CARD}")
    assert type(failed.value) is ThreadException
    with pytest.raises(ThreadException, match="^server: request failed$"):
        async with helper.stream("t1") as reply:
            await reply.write(f"card {CARD}")
    assert calls == ["ana", "ana", "helper"]
    items = await drain(ana)
    assert not any(isinstance(i, Message) for i in items)
    assert any(isinstance(i, StreamEnd) for i in items)
    assert "078-05-1120" not in await stored(storage) and CARD not in await stored(storage)


async def test_a_refused_finishing_message_ends_the_stream_with_nothing_stored() -> None:
    async def refuse(author: Identity, thread: Thread, content: list[Part]) -> list[Part]:
        raise InputError("redact: refused")

    _, _, ana, helper = await served(refuse)
    with pytest.raises(InputError, match="^redact: refused$"):
        async with helper.stream("t1") as reply:
            await reply.write("secret")
    items = await drain(ana)
    assert [type(i) for i in items if not isinstance(i, StreamDelta)][-1] is StreamEnd
    assert not any(isinstance(i, Message) for i in items)


async def test_what_redact_returns_is_validated_like_any_content() -> None:
    returned: list[Any] = []

    async def odd(author: Identity, thread: Thread, content: list[Part]) -> Any:
        return returned[0]

    storage = InMemoryStorage()
    server = Server(storage, redact=odd, max_inline_bytes=4)
    ana, _ = await pair(server)
    returned[:] = ["masked"]
    with pytest.raises(
        ConfigurationError, match="^server: redact must return a list of parts: input should be"
    ):
        await ana.send("t1", "hello")
    returned[:] = [[{"type": "video", "url": "https://x/v.mp4"}]]
    with pytest.raises(ConfigurationError, match="^server: redact must return a list of parts"):
        await ana.send("t1", "hello")
    returned[:] = [[]]
    with pytest.raises(InputError, match="^server: nothing is left of the message once redacted$"):
        await ana.send("t1", "hello")
    returned[:] = [[Image(media_type="image/png", data=b"12345678")]]
    with pytest.raises(
        InputError,
        match="^server: inline media is 8 bytes; the cap is 4 — upload it and send a url$",
    ):
        await ana.send("t1", "hello")
    returned[:] = [[{"type": "text", "text": "fine"}]]
    assert (await ana.send("t1", "hello")).content == [Text(text="fine")]
    messages = [e for e in await storage.list_events("acme", "t1") if isinstance(e, Message)]
    assert [m.content for m in messages] == [[Text(text="fine")]]


async def test_every_writer_passes_through_it_and_membership_events_do_not() -> None:
    redactor = Redactor()
    storage, server, _, _ = await served(redactor)
    await server.create_thread("acme", thread_id="t2", metadata={"channel": "support"})
    await server.add_member("acme", "t2", HELPER)
    assistant = await server.connect(identity=HELPER, scope="acme")
    sent = await assistant.send("t2", f"Card {CARD} on file.")
    assert sent.content == [Text(text=f"Card {MASKED} on file.")]
    author, thread, _ = redactor.calls[-1]
    assert (author, thread.id, thread.metadata) == (HELPER, "t2", {"channel": "support"})
    assert len(redactor.calls) == 1
    assert CARD not in str(await storage.list_events("acme", "t2"))
