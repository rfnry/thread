import json

import pytest

from rfnry_thread.errors import ConfigurationError, InputError
from rfnry_thread.models import Image, Message, Text, Working
from rfnry_thread.server import Server
from rfnry_thread.tools import ThreadTools, Tool, Tools

from .factories import ANA, lookalike
from .test_client import drain, pair, take


async def test_four_tools_pinned_to_one_thread(server: Server) -> None:
    ana, helper = await pair(server)
    tools = ThreadTools(helper, thread_id="t1", triggered_by=ANA, in_reply_to="e1")
    assert tools.names == ["send", "read_recent", "working", "idle"]
    assert all(
        "thread_id" not in d["input_schema"].get("properties", {}) for d in tools.definitions()
    )
    sent = await tools.dispatch("send", {"text": "The second charge is refunded."})
    assert isinstance(sent, dict) and sent["seq"] > 0
    message = await take(ana, Message)
    assert (message.in_reply_to, message.triggered_by, message.metadata) == ("e1", ANA, {})
    assert all(
        "metadata" not in d["input_schema"].get("properties", {}) for d in tools.definitions()
    )
    assert await tools.dispatch("working", {"label": "Checking the invoice"}) == "ok"
    assert (await take(ana, Working)).label == "Checking the invoice"
    assert await tools.dispatch("idle", {}) == "ok"


async def test_read_recent_returns_resolved_events_as_parts(server: Server) -> None:
    ana, helper = await pair(server)
    image = Image(media_type="image/png", url="https://files.example.com/receipt.png")
    first = await ana.send("t1", [Text(text="Here is the receipt."), image])
    await ana.edit("t1", first.id, [Text(text="Here is the right receipt."), image])
    await helper.step(
        "t1", name="Read", input={"path": "rules/refunds.md"}, output=[Text(text="Refund in full.")]
    )
    retracted = await ana.send("t1", "never mind")
    await ana.retract("t1", retracted.id)
    await drain(helper)
    recent = await ThreadTools(helper, thread_id="t1").dispatch("read_recent", {"limit": 10})
    assert isinstance(recent, list)
    headers = [json.loads(p.text) for p in recent if isinstance(p, Text) and p.text.startswith("{")]
    assert [(h["kind"], h["author"]["id"]) for h in headers] == [
        ("message", "ana"),
        ("step", "helper"),
    ]
    assert recent[1:3] == [Text(text="Here is the right receipt."), image]
    assert headers[1]["name"] == "Read" and headers[1]["input"] == {"path": "rules/refunds.md"}


async def test_read_recent_reads_the_latest_200_events_so_many_edits_leave_fewer_messages(
    server: Server,
) -> None:
    ana, helper = await pair(server)
    first = await ana.send("t1", "one")
    await ana.send("t1", "two")
    third = await ana.send("t1", "three")
    for n in range(198):
        await ana.edit("t1", third.id, f"three, take {n}")
    recent = await ThreadTools(helper, thread_id="t1").dispatch("read_recent", {"limit": 20})
    assert isinstance(recent, list)
    texts = [p.text for p in recent if isinstance(p, Text) and not p.text.startswith("{")]
    assert texts == ["two", "three, take 197"]
    assert first.id not in json.dumps([p.model_dump() for p in recent])


async def test_arguments_are_validated_and_the_thread_is_required(server: Server) -> None:
    _, helper = await pair(server)
    with pytest.raises(ConfigurationError, match="^tools: a thread_id is required$"):
        ThreadTools(helper, thread_id="")
    tools = ThreadTools(helper, thread_id="t1")
    with pytest.raises(InputError, match="^tools: invalid arguments for read_recent"):
        await tools.dispatch("read_recent", {"limit": 0})


async def test_it_combines_like_any_tools(server: Server) -> None:
    _, helper = await pair(server)

    async def now(args: dict[str, object]) -> str:
        return "2026-09-24T12:00:00Z"

    combined = ThreadTools(helper, thread_id="t1").only("send", "read_recent") + Tools(
        [Tool("now", "The current UTC time", body=now)]
    )
    assert combined.names == ["send", "read_recent", "now"]


async def test_triggered_by_is_read_by_its_fields(server: Server) -> None:
    ana, helper = await pair(server)
    tools = ThreadTools(helper, thread_id="t1", triggered_by=lookalike(ANA))
    await tools.dispatch("send", {"text": "On it."})
    assert (await take(ana, Message)).triggered_by == ANA


async def test_read_recent_pages_back_from_a_seq_with_what_was_edited_since(
    server: Server,
) -> None:
    ana, helper = await pair(server)
    sent = [await ana.send("t1", f"m{n}") for n in range(6)]
    await ana.edit("t1", sent[1].id, "m1, edited")
    await ana.retract("t1", sent[2].id)
    tools = ThreadTools(helper, thread_id="t1")

    async def page(**args: int) -> list[tuple[int, str]]:
        found = await tools.dispatch("read_recent", dict(args))
        assert isinstance(found, list)
        texts = [p.text for p in found if isinstance(p, Text)]
        return [(json.loads(h)["seq"], t) for h, t in zip(texts[::2], texts[1::2], strict=True)]

    latest = await page(limit=2)
    assert [text for _, text in latest] == ["m4", "m5"]
    older = await page(limit=2, before=latest[0][0])
    assert [text for _, text in older] == ["m1, edited", "m3"]
    oldest = await page(limit=2, before=older[0][0])
    assert [text for _, text in oldest] == ["m0"]
    assert await page(before=oldest[0][0]) == []
    with pytest.raises(InputError):
        await tools.dispatch("read_recent", {"before": 0})
    [definition] = [d for d in tools.definitions() if d["name"] == "read_recent"]
    assert "before" in definition["input_schema"]["properties"]
