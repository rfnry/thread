import asyncio

import pytest

from rfnry_thread.errors import ForbiddenError, InputError
from rfnry_thread.models import Identity, Thread
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server

from .conftest import say
from .factories import ANA, BRUNO, HELPER
from .peer import Peer


async def joined(server: Server, *identities: Identity) -> list[Peer]:
    owner = await Peer.open(server, ANA)
    await owner.call("thread.create", thread_id="t1")
    for identity in identities:
        if identity != ANA:
            await owner.call(
                "member.add", thread_id="t1", identity=identity.model_dump(mode="json")
            )
    peers = []
    for identity in identities:
        peer = owner if identity == ANA else await Peer.open(server, identity)
        await peer.call("thread.join", thread_id="t1")
        await peer.quiet()
        peers.append(peer)
    for peer in peers:
        await peer.quiet()
    return peers


async def test_a_signal_needs_a_join(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    with pytest.raises(
        ForbiddenError, match="^server: signals need a join; 'ana' has not joined thread 't1'$"
    ):
        await ana.call("signal.send", thread_id="t1", kind="working")


async def test_working_reaches_the_room_with_the_sender_stamped(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.call(
        "signal.send",
        thread_id="t1",
        kind="working",
        label="Reading rules/refunds.md",
        sender=BRUNO.model_dump(mode="json"),
    )
    working = await ana.push("working")
    assert working["sender"]["id"] == "helper" and working["label"] == "Reading rules/refunds.md"
    await helper.call("signal.send", thread_id="t1", kind="idle")
    assert (await ana.push("idle"))["sender"]["id"] == "helper"


async def test_signals_are_checked(server: Server) -> None:
    _, helper = await joined(server, ANA, HELPER)
    with pytest.raises(InputError, match="^server: a working label is at most 1000 characters$"):
        await helper.call("signal.send", thread_id="t1", kind="working", label="x" * 1001)
    with pytest.raises(InputError, match="^server: unknown signal kind 'present'$"):
        await helper.call("signal.send", thread_id="t1", kind="present")
    with pytest.raises(InputError, match="^server: stream 's1' was not started$"):
        await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    with pytest.raises(InputError, match="^server: stream 's1' was already started$"):
        await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")


async def test_a_stream_flows_start_deltas_end_and_bad_notifies_are_dropped(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.notify(
        "signal.send", thread_id="t1", kind="stream.delta", stream_id="ghost", index=0, text="lost"
    )
    await helper.call(
        "signal.send", thread_id="t1", kind="stream.start", stream_id="s1", in_reply_to="e1"
    )
    for index, text in enumerate(["Refunded ", "in full."]):
        await helper.notify(
            "signal.send",
            thread_id="t1",
            kind="stream.delta",
            stream_id="s1",
            index=index,
            text=text,
        )
    await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
    received = [p for p in await ana.quiet() if p["kind"].startswith("stream.")]
    assert received[0]["kind"] == "stream.start" and received[0]["in_reply_to"] == "e1"
    assert (
        "".join(p["text"] for p in received if p["kind"] == "stream.delta") == "Refunded in full."
    )
    assert received[-1]["kind"] == "stream.end"
    assert all(p.get("stream_id") != "ghost" for p in received)


async def test_a_late_joiner_sees_the_half_written_reply_first(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await helper.call("signal.send", thread_id="t1", kind="working", label="Writing")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await helper.notify(
        "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="Half"
    )
    await helper.call("thread.present", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    arrived = [(p["kind"], p.get("text")) for p in ana.pushes]
    assert arrived == [
        ("present", None),
        ("working", None),
        ("stream.start", None),
        ("stream.delta", "Half"),
        ("present", None),
    ]


async def test_present_lists_who_has_the_thread_open(server: Server) -> None:
    ana, _ = await joined(server, ANA, HELPER)
    identities = (await ana.call("thread.present", thread_id="t1"))["identities"]
    assert sorted(i["id"] for i in identities) == ["ana", "helper"]


async def test_two_tabs_are_one_presence_over_the_server(server: Server) -> None:
    helper, tab_one = await joined(server, HELPER, ANA)
    tab_two = await Peer.open(server, ANA)
    await tab_two.call("thread.join", thread_id="t1")
    assert [p["kind"] for p in await helper.quiet()] == []
    await tab_one.close()
    assert [p["kind"] for p in await helper.quiet()] == []
    await tab_two.close()
    assert [p["kind"] for p in await helper.quiet()] == ["left"]


async def test_a_dropped_connection_ends_its_stream_and_work(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.call("signal.send", thread_id="t1", kind="working")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await ana.quiet()
    await helper.close()
    assert [p["kind"] for p in await ana.quiet()] == ["stream.end", "idle", "left"]


async def test_leaving_ends_the_work_and_a_late_joiner_sees_none(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    await ana.quiet()
    await helper.call("thread.leave", thread_id="t1")
    assert [p["kind"] for p in await ana.quiet()] == ["idle", "left"]
    with pytest.raises(ForbiddenError):
        await helper.call("signal.send", thread_id="t1", kind="idle")
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    bruno = await Peer.open(server, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    assert [p["kind"] for p in bruno.pushes] == ["present", "present"]


async def test_a_member_removed_mid_stream_ends_the_stream(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await helper.notify(
        "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="Half"
    )
    await ana.quiet()
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    assert [p["kind"] for p in await ana.quiet()] == ["member_removed", "stream.end", "left"]
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    bruno = await Peer.open(server, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    assert [p["kind"] for p in bruno.pushes] == ["present", "present"]


async def test_a_member_the_server_removes_mid_stream_ends_its_stream_and_work(
    server: Server,
) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Writing")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await ana.quiet()
    await server.remove_member("acme", "t1", HELPER)
    assert [p["kind"] for p in await ana.quiet()] == [
        "member_removed",
        "stream.end",
        "idle",
        "left",
    ]
    await server.add_member("acme", "t1", BRUNO)
    bruno = await Peer.open(server, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    assert [p["kind"] for p in bruno.pushes] == ["present", "present"]


async def test_cancel_names_who_should_stop(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await ana.call("signal.send", thread_id="t1", kind="cancel", to=HELPER.model_dump(mode="json"))
    cancel = await helper.push("cancel")
    assert cancel["to"]["id"] == "helper" and cancel["sender"]["id"] == "ana"


async def test_an_added_member_connected_elsewhere_is_invited(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    bruno = await Peer.open(server, BRUNO)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    invited = await bruno.push("invited")
    assert invited["thread"]["id"] == "t1" and invited["sender"]["id"] == "ana"


async def test_deletion_tells_the_room_and_ends_its_joins(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await ana.call("thread.delete", thread_id="t1")
    assert (await helper.push("thread.deleted"))["sender"]["id"] == "ana"
    with pytest.raises(ForbiddenError):
        await helper.call("signal.send", thread_id="t1", kind="working")


async def test_authorize_send_applies_to_signals_that_speak(storage: PostgresStorage) -> None:
    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        return not (identity.id == "helper" and action == "send")

    server = Server(storage, authorize=authorize)
    ana, helper = await joined(server, ANA, HELPER)
    try:
        with pytest.raises(ForbiddenError, match="^server: 'helper' may not send in thread 't1'$"):
            await helper.call("signal.send", thread_id="t1", kind="working")
    finally:
        await ana.close()
        await helper.close()


async def test_a_signal_that_outlives_its_join_is_refused(storage: PostgresStorage) -> None:
    asked, release = asyncio.Event(), asyncio.Event()

    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        if identity.id == "helper" and action == "send":
            asked.set()
            await release.wait()
        return True

    server = Server(storage, authorize=authorize)
    ana, helper = await joined(server, ANA, HELPER)
    working = asyncio.create_task(helper.call("signal.send", thread_id="t1", kind="working"))
    await asyncio.wait_for(asked.wait(), 1.0)
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    release.set()
    with pytest.raises(ForbiddenError, match="^server: signals need a join"):
        await working
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    bruno = await Peer.open(server, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    assert [p["kind"] for p in bruno.pushes] == ["present", "present"]


async def test_away_ends_presence_while_the_thread_keeps_arriving(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.call("thread.away", thread_id="t1")
    assert [p["kind"] for p in await ana.quiet()] == ["left"]
    identities = (await ana.call("thread.present", thread_id="t1"))["identities"]
    assert [i["id"] for i in identities] == ["ana"]
    await say(ana, "t1", "still there?")
    assert [p["kind"] for p in await helper.quiet()] == ["left", "message"]
    await ana.quiet()
    await helper.call("signal.send", thread_id="t1", kind="working")
    await helper.call("thread.away", thread_id="t1")
    await helper.call("thread.join", thread_id="t1")
    assert [p["kind"] for p in await ana.quiet()] == ["working", "present"]


async def test_away_in_one_tab_keeps_the_member_present_in_the_other(server: Server) -> None:
    helper, tab_one = await joined(server, HELPER, ANA)
    tab_two = await Peer.open(server, ANA)
    await tab_two.call("thread.join", thread_id="t1")
    await tab_one.call("thread.away", thread_id="t1")
    assert [p["kind"] for p in await helper.quiet()] == []
    await tab_two.call("thread.away", thread_id="t1")
    assert [p["kind"] for p in await helper.quiet()] == ["left"]
    await tab_one.close()
    await tab_two.close()
    assert [p["kind"] for p in await helper.quiet()] == []


async def test_away_from_a_thread_not_joined_changes_nothing(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    bruno = await Peer.open(server, BRUNO)
    await bruno.call("thread.away", thread_id="t1")
    await bruno.call("thread.away", thread_id="ghost")
    assert [p["kind"] for p in await ana.quiet()] == []


async def test_a_join_while_away_receives_the_thread_without_presence(server: Server) -> None:
    [ana] = await joined(server, ANA)
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await ana.quiet()
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.join", thread_id="t1", away=True)
    assert [p["kind"] for p in await ana.quiet()] == []
    await say(ana, "t1", "hello")
    assert [p["kind"] for p in await helper.quiet()] == ["present", "message"]
    await ana.quiet()
    await helper.call("thread.join", thread_id="t1")
    assert [p["kind"] for p in await ana.quiet()] == ["present"]
