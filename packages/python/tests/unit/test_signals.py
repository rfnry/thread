import pytest

from rfnry_thread.errors import ForbiddenError, InputError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import Identity, Thread
from rfnry_thread.server import Server

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


async def test_authorize_send_applies_to_signals_that_speak(storage: InMemoryStorage) -> None:
    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        return not (identity.id == "helper" and action == "send")

    server = Server(storage, authorize=authorize)
    _, helper = await joined(server, ANA, HELPER)
    with pytest.raises(ForbiddenError, match="^server: 'helper' may not send in thread 't1'$"):
        await helper.call("signal.send", thread_id="t1", kind="working")
