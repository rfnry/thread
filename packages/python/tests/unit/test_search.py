from typing import Any

import pytest

from rfnry_thread.errors import ForbiddenError, InputError, NotFoundError
from rfnry_thread.models import Identity, Text
from rfnry_thread.server import Server
from rfnry_thread.verbs import QUERY_LIMIT

from .conftest import say
from .factories import ANA, BRUNO
from .peer import Peer

CAROL = Identity(role="user", id="carol", name="Carol")


def ids(reply: dict[str, Any]) -> list[str]:
    return [t["id"] for t in reply["threads"]]


async def test_a_member_finds_its_threads_by_text_or_metadata_and_filters_by_metadata(
    server: Server,
) -> None:
    ana = await Peer.open(server, ANA)
    for thread_id, title, team in (
        ("billing", "Billing question", "money"),
        ("refund", "Refund", "money"),
        ("other", "Hello", "ops"),
    ):
        await ana.call(
            "thread.create", thread_id=thread_id, metadata={"title": title, "team": team}
        )
    await say(ana, "billing", "My invoice was charged twice")
    await say(ana, "refund", "Please refund the extra charge")
    found = await ana.call("thread.list", query="charge", order="activity")
    assert ids(found) == ["refund", "billing"]
    assert found["threads"][0]["match"]["content"] == [
        {"type": "text", "text": "Please refund the extra charge"}
    ]
    [hello] = (await ana.call("thread.list", query="HELLO"))["threads"]
    assert hello["id"] == "other" and "match" not in hello
    assert ids(await ana.call("thread.list", metadata={"team": "money"})) == ["refund", "billing"]
    both = await ana.call("thread.list", metadata={"team": "money"}, query="invoice")
    assert ids(both) == ["billing"]
    bruno = await Peer.open(server, BRUNO)
    assert ids(await bruno.call("thread.list", query="charge")) == []
    assert ids(await bruno.call("thread.list", metadata={"team": "money"})) == []
    every = await server.list_threads("acme", query="charge", order="activity")
    assert [t.id for t in every] == ["refund", "billing"]
    assert await server.list_threads("globex", query="charge") == []


async def test_a_query_needs_a_word_and_fits_the_limit(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    for blank in ("", "  \n "):
        with pytest.raises(InputError, match="^server: a query needs at least one word$"):
            await ana.call("thread.list", query=blank)
        with pytest.raises(InputError, match="^server: a query needs at least one word$"):
            await ana.call("thread.search", thread_id="t1", query=blank)
        with pytest.raises(InputError, match="^server: a query needs at least one word$"):
            await server.list_threads("acme", query=blank)
    over = "x" * (QUERY_LIMIT + 1)
    for method, extra in (("thread.list", {}), ("thread.search", {"thread_id": "t1"})):
        with pytest.raises(
            InputError, match=f"^server: a query is at most {QUERY_LIMIT} characters$"
        ):
            await ana.call(method, query=over, **extra)
    assert ids(await ana.call("thread.list", query="x" * QUERY_LIMIT)) == []
    assert (await ana.call("thread.search", thread_id="t1", query="x" * QUERY_LIMIT)) == {
        "events": []
    }


async def test_search_finds_the_messages_a_member_may_read_in_a_thread(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1", history="since_added")
    await say(ana, "t1", "pump one")
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    two = await say(ana, "t1", "pump two")
    await say(ana, "t1", "valve")

    async def texts(peer: Peer, **options: Any) -> list[str]:
        found = await peer.call("thread.search", thread_id="t1", **options)
        return [e["content"][0]["text"] for e in found["events"]]

    assert await texts(ana, query="PUMP") == ["pump one", "pump two"]
    assert await texts(ana, query="pump", limit=1) == ["pump two"]
    assert await texts(ana, query="pump", before=two["seq"]) == ["pump one"]
    assert await texts(ana, query="pump two") == ["pump two"]
    bruno = await Peer.open(server, BRUNO)
    assert await texts(bruno, query="pump") == ["pump two"]
    carol = await Peer.open(server, CAROL)
    with pytest.raises(ForbiddenError, match="^server: 'carol' is not a member of thread 't1'$"):
        await carol.call("thread.search", thread_id="t1", query="pump")
    with pytest.raises(NotFoundError, match="^server: thread 'nope' was not found$"):
        await ana.call("thread.search", thread_id="nope", query="pump")
    with pytest.raises(InputError, match="^server: limit must be between 1 and 500$"):
        await ana.call("thread.search", thread_id="t1", query="pump", limit=0)


async def test_search_asks_authorize_to_read(storage: Any) -> None:
    async def authorize(identity: Identity, action: str, thread: Any) -> bool:
        return action != "read" or not thread.metadata.get("sealed")

    ana = await Peer.open(Server(storage, authorize=authorize), ANA)
    await ana.call("thread.create", thread_id="open")
    await ana.call("thread.create", thread_id="sealed", metadata={"sealed": True})
    for thread_id in ("open", "sealed"):
        await say(ana, thread_id, "pump")
    assert ids(await ana.call("thread.list", query="pump")) == ["open"]
    with pytest.raises(ForbiddenError, match="^server: 'ana' may not read in thread 'sealed'$"):
        await ana.call("thread.search", thread_id="sealed", query="pump")


async def test_a_client_searches_its_threads_and_inside_one(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1", metadata={"title": "Pumps"})
    await ana.create_thread(thread_id="t2", metadata={"title": "Valves"})
    sent = await ana.send("t2", "the pump leaks")
    await ana.edit("t2", sent.id, "the pump leaks oil")
    listed = await ana.list_threads(query="pump", order="activity")
    assert [t.id for t in listed] == ["t2", "t1"]
    assert listed[0].match is not None and listed[1].match is None
    assert (listed[0].match.id, listed[0].match.content) == (
        sent.id,
        [Text(text="the pump leaks oil")],
    )
    assert [t.id for t in await ana.list_threads(metadata={"title": "Valves"})] == ["t2"]
    found = await ana.search("t2", "LEAKS OIL")
    assert [(m.id, m.content) for m in found] == [(sent.id, [Text(text="the pump leaks oil")])]
    assert await ana.search("t2", "pump", before=sent.seq) == []
    assert [m.id for m in await ana.search("t2", "pump", limit=1)] == [sent.id]
    await ana.close()
