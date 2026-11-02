import asyncio
import json
import logging
from typing import Any

import pytest
from until import until

from rfnry_thread.errors import ForbiddenError, StorageError
from rfnry_thread.hub import Hub
from rfnry_thread.models import (
    SYSTEM,
    Identity,
    Idle,
    Left,
    MemberRemoved,
    Present,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Thread,
    Working,
)
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.protocol import Request, encode
from rfnry_thread.server import Server
from rfnry_thread.state import EXPIRY

from .conftest import say
from .factories import ANA, AT, BRUNO, HELPER, added, message, removed
from .hubkit import kinds, open_session, pushes
from .legacy import enroll
from .peer import Peer


async def setup(
    storage: PostgresStorage, replay_limit: int = 500, events: int = 0
) -> tuple[Hub, Thread]:
    thread, _ = await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=AT))
    for identity in (ANA, HELPER, BRUNO):
        await enroll(storage, "t1", identity)
    for n in range(1, events + 1):
        await storage.append_event("acme", message(n, f"m{n}"))
    return Hub(storage, replay_limit), thread


async def test_a_live_join_replays_nothing_and_reports_the_head(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage, events=3)
    session, recorder = open_session(HELPER)
    joined = await hub.join(session, thread, None)
    assert (joined.seq, joined.truncated) == (3, False)
    assert kinds(await pushes(session, recorder)) == ["present"]
    assert session.joined == {"t1": 3}


async def test_a_join_since_replays_what_followed_and_truncates_to_the_latest_page(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage, replay_limit=3, events=5)
    first, first_recorder = open_session(HELPER)
    joined = await hub.join(first, thread, 3)
    assert [p.get("seq") for p in await pushes(first, first_recorder)] == [4, 5, None]
    assert joined.truncated is False
    second, second_recorder = open_session(BRUNO)
    joined = await hub.join(second, thread, 0)
    assert [p["seq"] for p in await pushes(second, second_recorder) if p["kind"] == "message"] == [
        3,
        4,
        5,
    ]
    assert joined.truncated is True and second.joined["t1"] == 5


async def test_a_join_since_past_the_head_replays_nothing_and_says_truncated(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage, events=3)
    session, recorder = open_session(HELPER)
    joined = await hub.join(session, thread, 8)
    assert (joined.seq, joined.truncated) == (3, True)
    assert kinds(await pushes(session, recorder)) == ["present"]
    assert session.joined == {"t1": 3}


async def test_delivery_drops_repeats_and_fills_gaps_from_storage(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage, events=1)
    session, recorder = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    for n in (2, 3, 4):
        await storage.append_event("acme", message(n, f"m{n}"))
    fourth = (await storage.list_events("acme", "t1", since=3))[0]
    await hub.deliver("acme", fourth)
    await hub.deliver("acme", fourth)
    assert [p["seq"] for p in await pushes(session, recorder)] == [2, 3, 4]


async def test_a_notice_is_read_only_when_a_session_is_behind(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    reads: list[int] = []
    original = storage.list_events

    async def counted(*args: Any, **kwargs: Any) -> Any:
        reads.append(1)
        return await original(*args, **kwargs)

    session, recorder = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    storage.list_events = counted  # type: ignore[method-assign]
    stored, _ = await storage.append_event("acme", message(1, "hi"))
    await hub.on_event("acme", "t1", stored.seq)
    await hub.on_event("acme", "t1", stored.seq)
    await hub.on_event("acme", "other-thread", 1)
    assert [p["seq"] for p in await pushes(session, recorder)] == [1]
    assert len(reads) == 1


async def test_a_removed_member_is_cut_off_after_the_event(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    bruno, bruno_recorder = open_session(BRUNO)
    helper, helper_recorder = open_session(HELPER)
    await hub.join(bruno, thread, None)
    await hub.join(helper, thread, None)
    await pushes(bruno, bruno_recorder)
    removal, _ = await storage.append_event(
        "acme",
        MemberRemoved(
            id="r", thread_id="t1", author=ANA, created_at=AT, client_id="r", identity=BRUNO
        ),
    )
    await hub.deliver("acme", removal)
    after, _ = await storage.append_event("acme", message(2, "after"))
    await hub.deliver("acme", after)
    assert kinds(await pushes(bruno, bruno_recorder)) == ["member_removed"]
    assert "t1" not in bruno.joined
    assert kinds(await pushes(helper, helper_recorder))[-3:] == [
        "member_removed",
        "left",
        "message",
    ]


async def test_a_joining_session_first_receives_the_current_state(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    helper, _ = open_session(HELPER)
    await hub.join(helper, thread, None)
    await hub.signal(helper, Working(thread_id="t1", sender=HELPER, label="Reading"))
    await hub.signal(helper, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    await hub.signal(
        helper, StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text="Half")
    )
    ana, recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    assert kinds(await pushes(ana, recorder)) == [
        "present",
        "working",
        "stream.start",
        "stream.delta",
        "present",
    ]


async def test_two_connections_of_one_member_are_one_presence(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    helper, helper_recorder = open_session(HELPER)
    await hub.join(helper, thread, None)
    await pushes(helper, helper_recorder)
    tab_one, _ = open_session(ANA)
    tab_two, _ = open_session(ANA)
    await hub.join(tab_one, thread, None)
    await hub.join(tab_two, thread, None)
    assert kinds(await pushes(helper, helper_recorder)) == ["present"]
    await hub.leave(tab_one, "t1")
    assert await pushes(helper, helper_recorder) == []
    await hub.drop(tab_two)
    assert kinds(await pushes(helper, helper_recorder)) == ["left"]
    assert hub.state.present("acme", "t1") == [HELPER]


async def test_a_dropped_connection_ends_its_streams_and_work(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    helper, _ = open_session(HELPER)
    ana, recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    await hub.join(helper, thread, None)
    await hub.signal(helper, Working(thread_id="t1", sender=HELPER))
    await hub.signal(helper, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    await pushes(ana, recorder)
    await hub.drop(helper)
    assert kinds(await pushes(ana, recorder)) == ["stream.end", "idle", "left"]
    assert [s.kind for s in hub.state.snapshot("acme", "t1")] == ["present"]


async def test_leaving_ends_its_streams_and_work_in_that_thread_only(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage)
    other, _ = await storage.create_thread("acme", Thread(id="t2", scope="acme", created_at=AT))
    await enroll(storage, "t2", HELPER)
    helper, _ = open_session(HELPER)
    ana, recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    await hub.join(helper, thread, None)
    await hub.join(helper, other, None)
    for thread_id in ("t1", "t2"):
        await hub.signal(helper, Working(thread_id=thread_id, sender=HELPER))
        await hub.signal(helper, StreamStart(thread_id=thread_id, sender=HELPER, stream_id="s1"))
    await pushes(ana, recorder)
    await hub.leave(helper, "t1")
    assert kinds(await pushes(ana, recorder)) == ["stream.end", "idle", "left"]
    assert [s.kind for s in hub.state.snapshot("acme", "t1")] == ["present"]
    assert [s.kind for s in hub.state.snapshot("acme", "t2")] == [
        "present",
        "working",
        "stream.start",
    ]
    assert (helper.working, helper.streams) == ({"t2"}, {("t2", "s1")})


async def test_an_invitation_reaches_only_connections_not_in_the_thread(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage)
    joined, joined_recorder = open_session(BRUNO)
    elsewhere, elsewhere_recorder = open_session(BRUNO)
    other_scope, other_recorder = open_session(BRUNO, scope="other")
    hub.sessions.update({joined, elsewhere, other_scope})
    await hub.join(joined, thread, None)
    await pushes(joined, joined_recorder)
    await hub.added("acme", thread, ANA, BRUNO, 1)
    assert await pushes(joined, joined_recorder) == []
    invited = await pushes(elsewhere, elsewhere_recorder)
    assert kinds(invited) == ["invited"] and invited[0]["thread"]["id"] == "t1"
    assert await pushes(other_scope, other_recorder) == []


async def test_an_add_is_published_as_an_invitation_with_its_seq(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    published: list[dict[str, Any]] = []

    async def publish(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        published.append(payload)

    storage.publish = publish  # type: ignore[method-assign]
    hub.linked = True
    await hub.added("acme", thread, ANA, BRUNO, 3)
    assert published == [
        {
            "o": hub.origin,
            "kind": "invited",
            "thread_id": "t1",
            "sender": ANA.model_dump(mode="json"),
            "to": "bruno",
            "seq": 3,
        }
    ]


async def test_an_invitation_without_a_seq_invites_and_hands_followers_the_thread(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage, events=2)
    plain, plain_recorder = open_session(BRUNO)
    following, following_recorder = open_session(BRUNO)
    following.following = True
    hub.sessions.update({plain, following})
    notice = {"kind": "invited", "thread_id": "t1", "sender": ANA.model_dump(mode="json")}
    await hub.on_signal("acme", "t1", {"o": "older", **notice, "to": "bruno"})
    assert kinds(await pushes(plain, plain_recorder)) == ["invited"]
    await following.outbound.drain()
    assert [frame["name"] for frame in following_recorder.frames] == ["joined"]
    assert following.joined == {"t1": 2}


async def test_an_invited_notice_for_a_non_member_is_ignored(storage: PostgresStorage) -> None:
    hub, _ = await setup(storage, events=2)
    mallory = Identity(id="mallory", role="user", name="Mallory")
    plain, plain_recorder = open_session(mallory)
    following, following_recorder = open_session(mallory)
    following.following = True
    hub.sessions.update({plain, following})
    notice = {
        "o": "elsewhere",
        "kind": "invited",
        "thread_id": "t1",
        "sender": ANA.model_dump(mode="json"),
        "to": "mallory",
        "seq": 3,
    }
    await hub.on_signal("acme", "t1", dict(notice))
    assert await pushes(plain, plain_recorder) == []
    await following.outbound.drain()
    assert following_recorder.frames == []
    assert "t1" not in plain.joined and "t1" not in following.joined
    await storage.add_member("acme", added("t1", mallory))
    await storage.remove_member("acme", removed("t1", mallory))
    await hub.on_signal("acme", "t1", dict(notice))
    assert await pushes(plain, plain_recorder) == []
    await following.outbound.drain()
    assert following_recorder.frames == []
    assert "t1" not in plain.joined and "t1" not in following.joined


async def test_deletion_tells_the_room_and_empties_it(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    ana, recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    await pushes(ana, recorder)
    await hub.deleted("acme", "t1", ANA)
    assert kinds(await pushes(ana, recorder)) == ["thread.deleted"]
    assert ana.joined == {} and hub.state.snapshot("acme", "t1") == []


async def test_deleting_a_scope_tells_each_joined_connection_and_empties_its_rooms(
    storage: PostgresStorage,
) -> None:
    server = Server(storage)
    ana = await Peer.open(server, ANA)
    bruno = await Peer.open(server, BRUNO)
    for thread_id in ("t1", "t2"):
        await ana.call("thread.create", thread_id=thread_id)
        await ana.call("member.add", thread_id=thread_id, identity=BRUNO.model_dump(mode="json"))
        await ana.call("thread.join", thread_id=thread_id)
    await bruno.call("thread.join", thread_id="t1")
    await bruno.call("signal.send", thread_id="t1", kind="working", label="Reading")
    await bruno.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await say(bruno, "t1", "before")
    elsewhere = await Peer.open(server, ANA, scope="other")
    await elsewhere.call("thread.create", thread_id="t1")
    await elsewhere.call("thread.join", thread_id="t1")
    for peer in (ana, bruno, elsewhere):
        await peer.quiet()
    await server.delete_scope("acme")
    told = sorted((p["kind"], p["thread_id"], p["sender"]["id"]) for p in await ana.quiet())
    assert told == [("thread.deleted", "t1", "system"), ("thread.deleted", "t2", "system")]
    assert [(p["kind"], p["thread_id"]) for p in await bruno.quiet()] == [("thread.deleted", "t1")]
    assert await elsewhere.quiet() == []
    hub = server._hub
    assert [key for key in hub._rooms if key[0] == "acme"] == []
    left = [
        (s.joined, s.floors, s.present, s.working, s.streams)
        for s in hub.sessions
        if s.scope == "acme"
    ]
    assert left == [({}, {}, set(), set(), set())] * 2
    assert hub.state.snapshot("acme", "t1") == [] and hub.state.own().keys() == {("other", "t1")}
    with pytest.raises(ForbiddenError, match="^server: signals need a join"):
        await bruno.call("signal.send", thread_id="t1", kind="idle")
    await say(elsewhere, "t1", "still here")
    assert [p["kind"] for p in await elsewhere.quiet()] == ["message"]
    assert hub.state.present("other", "t1") == [ANA]


async def test_a_thread_made_again_after_its_scope_was_deleted_is_handed_over_as_new(
    storage: PostgresStorage,
) -> None:
    server = Server(storage)
    ana = await Peer.open(server, ANA)
    bruno = await Peer.open(server, BRUNO)
    helper = await Peer.open(server, HELPER)
    await ana.call("thread.create", thread_id="t1")
    for identity in (BRUNO, HELPER):
        await ana.call("member.add", thread_id="t1", identity=identity.model_dump(mode="json"))
    await ana.call("thread.join", thread_id="t1")
    await bruno.call("thread.join", thread_id="t1")
    await helper.call("thread.follow")
    for n in range(5):
        await say(helper, "t1", f"old {n}")
    for peer in (ana, bruno, helper):
        await peer.quiet()
    helper.joined.clear()
    await server.delete_scope("acme")
    for peer in (ana, bruno, helper):
        assert kinds(await peer.quiet()) == ["thread.deleted"]
    await server.create_thread("acme", "t1")
    await server.add_member("acme", "t1", ANA)
    await server.add_member("acme", "t1", HELPER)
    assert (await ana.push("invited", timeout=2.0))["thread"]["id"] == "t1"
    await helper.quiet()
    assert [j["thread"]["id"] for j in helper.joined] == ["t1"]
    await ana.call("thread.join", thread_id="t1")
    for n in range(3):
        await say(helper, "t1", f"new {n}")
    for peer in (ana, helper):
        said = [p["content"][0]["text"] for p in await peer.quiet() if p["kind"] == "message"]
        assert said == ["new 0", "new 1", "new 2"]
    assert await bruno.quiet() == []
    assert (await ana.call("thread.present", thread_id="t1"))["identities"] == [
        ANA.model_dump(mode="json")
    ]


async def test_an_invitation_crossing_the_deletion_of_its_scope_is_handed_over_after_it(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    hub, thread = await setup(storage)
    first, _ = await storage.create_thread("acme", Thread(id="t0", scope="acme", created_at=AT))
    await enroll(storage, "t0", BRUNO)
    ana, ana_recorder = open_session(ANA)
    helper, helper_recorder = open_session(HELPER)
    helper.following = True
    for session in (ana, helper):
        hub.sessions.add(session)
        await hub.join(session, thread, None)
    await pushes(ana, ana_recorder)
    await pushes(helper, helper_recorder)
    head, is_member = storage.head, storage.is_member
    reading: list[str] = []
    checked: list[str] = []
    release = asyncio.Event()

    async def held(scope: str, thread_id: str, identity_id: str) -> tuple[int, int] | None:
        found = await head(scope, thread_id, identity_id)
        if thread_id == "t0":
            reading.append(thread_id)
            await release.wait()
        return found

    async def member(scope: str, thread_id: str, identity_id: str) -> bool:
        found = await is_member(scope, thread_id, identity_id)
        checked.append(identity_id)
        return found

    monkeypatch.setattr(storage, "head", held)
    monkeypatch.setattr(storage, "is_member", member)
    bruno, _ = open_session(BRUNO)
    joining = asyncio.create_task(hub.join(bruno, first, None))
    await until(lambda: reading == ["t0"])
    await storage.delete_scope("acme")
    wipe = {"o": "near", "kind": "scope.deleted", "sender": SYSTEM.model_dump(mode="json")}
    deleting = asyncio.create_task(hub.on_signal("acme", "", wipe))
    await asyncio.sleep(0)
    await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=AT))
    inviting = []
    for identity in (ANA, HELPER):
        _, event = await storage.add_member("acme", added("t1", identity))
        assert event is not None
        invited = {
            "o": "near",
            "kind": "invited",
            "thread_id": "t1",
            "sender": SYSTEM.model_dump(mode="json"),
            "to": identity.id,
            "seq": event.seq,
        }
        inviting.append(asyncio.create_task(hub.on_signal("acme", "t1", invited)))
    await until(lambda: sorted(checked) == ["ana", "helper"])
    assert not deleting.done()
    release.set()
    await asyncio.wait_for(asyncio.gather(joining, deleting, *inviting), 2.0)
    for session in (ana, helper):
        await session.outbound.drain()
    told = [(f["name"], f["payload"].get("kind")) for f in ana_recorder.frames]
    assert told == [("signal", "thread.deleted"), ("signal", "invited")]
    told = [(f["name"], f["payload"].get("kind")) for f in helper_recorder.frames]
    assert told == [("signal", "thread.deleted"), ("event", "member_added"), ("joined", None)]
    assert ana.joined == {} and list(helper.joined) == ["t1"]


async def test_joins_and_a_signal_crossing_the_deletion_of_their_scope_leave_nothing_behind(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    hub, thread = await setup(storage)
    other, _ = await storage.create_thread("acme", Thread(id="t2", scope="acme", created_at=AT))
    await enroll(storage, "t2", HELPER)
    ana, ana_recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    await pushes(ana, ana_recorder)
    head = storage.head
    reading: list[str] = []
    release = asyncio.Event()

    async def held(scope: str, thread_id: str, identity_id: str) -> tuple[int, int] | None:
        found = await head(scope, thread_id, identity_id)
        reading.append(thread_id)
        await release.wait()
        return found

    monkeypatch.setattr(storage, "head", held)
    bruno, bruno_recorder = open_session(BRUNO)
    helper, helper_recorder = open_session(HELPER)
    joining = [
        asyncio.create_task(hub.join(bruno, thread, None)),
        asyncio.create_task(hub.join(helper, other, None)),
    ]
    await until(lambda: len(reading) == 2)
    await storage.delete_scope("acme")
    deleting = asyncio.create_task(hub.scope_deleted("acme", SYSTEM))
    await asyncio.sleep(0)
    working = asyncio.create_task(hub.signal(ana, Working(thread_id="t1", sender=ANA)))
    await asyncio.sleep(0)
    release.set()
    await asyncio.wait_for(asyncio.gather(*joining, deleting, working), 2.0)
    assert hub.state.own() == {} and hub._local == {} and hub._rooms == {}
    assert kinds(await pushes(ana, ana_recorder)) == ["thread.deleted"]
    assert kinds(await pushes(bruno, bruno_recorder)) == ["present", "thread.deleted"]
    assert kinds(await pushes(helper, helper_recorder)) == ["present", "thread.deleted"]
    for session in (ana, bruno, helper):
        assert (session.joined, session.present, session.working) == ({}, set(), set())


async def test_linked_hubs_mirror_signals_and_drop_their_own(storage: PostgresStorage) -> None:
    near, thread = await setup(storage)
    far = Hub(storage, 500)
    stops = []
    for hub in (near, far):
        hub.linked = True
        stops.append(await storage.listen(hub.on_event, hub.on_signal))
    speaker, speaker_recorder = open_session(HELPER)
    listener, listener_recorder = open_session(ANA)
    await near.join(speaker, thread, None)
    await far.join(listener, thread, None)
    await until(
        lambda: len(near.state.present("acme", "t1")) == len(far.state.present("acme", "t1")) == 2
    )
    await pushes(speaker, speaker_recorder)
    await pushes(listener, listener_recorder)
    working = Working(thread_id="t1", sender=HELPER, label="Reading")
    await near.signal(speaker, working)
    await until(lambda: far.state.snapshot("acme", "t1")[-1] == working)
    await asyncio.sleep(0.1)
    assert kinds(await pushes(listener, listener_recorder)) == ["working"]
    assert kinds(await pushes(speaker, speaker_recorder)) == ["working"]
    for stop in stops:
        await stop()


@pytest.mark.parametrize("text", ["x" * 20000, "\U0001f600" * 5000])
async def test_a_long_delta_is_mirrored_in_pieces_under_the_notify_limit(
    storage: PostgresStorage, text: str
) -> None:
    near, thread = await setup(storage)
    far = Hub(storage, 500)
    published: list[int] = []
    original = storage.publish

    async def measured(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        notice = {"s": scope, "t": thread_id, "p": payload}
        published.append(len(json.dumps(notice).encode()))
        await original(scope, thread_id, payload)

    storage.publish = measured  # type: ignore[method-assign]
    stops = []
    for hub in (near, far):
        hub.linked = True
        stops.append(await storage.listen(hub.on_event, hub.on_signal))
    speaker, _ = open_session(HELPER)
    listener, recorder = open_session(ANA)
    await near.join(speaker, thread, None)
    await far.join(listener, thread, None)
    await near.signal(speaker, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    await near.signal(
        speaker,
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text=text),
    )

    def received() -> str:
        return "".join(f["payload"].get("text", "") for f in recorder.frames if f["type"] == "push")

    await until(lambda: received() == text)
    assert max(published) < 7900
    for stop in stops:
        await stop()


async def test_announce_teaches_another_replica_who_is_here(storage: PostgresStorage) -> None:
    near, thread = await setup(storage)
    far = Hub(storage, 500)
    ana, _ = open_session(ANA)
    await near.join(ana, thread, None)
    stops = []
    for hub in (near, far):
        hub.linked = True
        stops.append(await storage.listen(hub.on_event, hub.on_signal))
    assert far.state.present("acme", "t1") == []
    await near.announce()
    await until(lambda: far.state.present("acme", "t1") == [ANA])
    for stop in stops:
        await stop()


async def far(hub: Hub, signal: Signal) -> None:
    await hub.on_signal("acme", "t1", {"o": "far", **signal.model_dump(mode="json")})


async def went_silent(hub: Hub) -> None:
    signals: list[Signal] = [
        Present(thread_id="t1", sender=HELPER),
        Working(thread_id="t1", sender=HELPER),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1", in_reply_to="e1"),
    ]
    for signal in signals:
        await far(hub, signal)
    now = hub.state._clock()
    hub.state._clock = lambda: now + EXPIRY + 1


def senders(payloads: list[dict[str, Any]]) -> list[tuple[str, str]]:
    return [(p["kind"], p["sender"]["id"]) for p in payloads]


async def test_a_joiner_after_a_replica_went_silent_sees_neither_its_state_nor_its_end(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    await hub.join(ana, thread, None)
    await went_silent(hub)
    await pushes(ana, ana_out)
    bruno, bruno_out = open_session(BRUNO)
    await hub.join(bruno, thread, None)
    await hub.sweep()
    assert senders(await pushes(ana, ana_out)) == [
        ("stream.end", "helper"),
        ("idle", "helper"),
        ("left", "helper"),
        ("present", "bruno"),
    ]
    assert senders(await pushes(bruno, bruno_out)) == [("present", "ana"), ("present", "bruno")]


async def test_a_leave_after_a_replica_went_silent_is_one_left(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    helper, _ = open_session(HELPER)
    await hub.join(ana, thread, None)
    await hub.join(helper, thread, None)
    await went_silent(hub)
    await pushes(ana, ana_out)
    await hub.leave(helper, "t1")
    await hub.sweep()
    assert senders(await pushes(ana, ana_out)) == [
        ("stream.end", "helper"),
        ("idle", "helper"),
        ("left", "helper"),
    ]


async def test_a_replica_heard_from_after_its_sweep_is_announced_again_and_ends_once(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    await hub.join(ana, thread, None)
    await went_silent(hub)
    await pushes(ana, ana_out)
    await hub.sweep()
    told = [senders(await pushes(ana, ana_out))]
    late: list[Signal] = [
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=1, text="more"),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        Idle(thread_id="t1", sender=HELPER),
        Left(thread_id="t1", sender=HELPER),
    ]
    starts: list[str | None] = []
    for signal in late:
        await far(hub, signal)
        found = await pushes(ana, ana_out)
        starts.extend(p.get("in_reply_to") for p in found if p["kind"] == "stream.start")
        told.append(senders(found))
    assert starts == ["e1"]
    assert told == [
        [("stream.end", "helper"), ("idle", "helper"), ("left", "helper")],
        [("stream.start", "helper"), ("stream.delta", "helper")],
        [("stream.end", "helper")],
        [],
        [],
    ]


async def test_a_repeat_from_a_silent_replica_ends_its_state_then_announces_it_once(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    await hub.join(ana, thread, None)
    await went_silent(hub)
    await pushes(ana, ana_out)
    entry = {
        "sender": HELPER.model_dump(mode="json"),
        "present": True,
        "working": {"label": None},
        "streams": [{"stream_id": "s1", "in_reply_to": None}],
    }
    await hub.on_signal("acme", "t1", {"o": "far", "kind": "refresh", "entries": [entry]})
    told = [senders(await pushes(ana, ana_out))]
    await far(hub, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    told.append(senders(await pushes(ana, ana_out)))
    assert told == [
        [
            ("stream.end", "helper"),
            ("idle", "helper"),
            ("left", "helper"),
            ("present", "helper"),
            ("working", "helper"),
            ("stream.start", "helper"),
        ],
        [],
    ]


async def test_the_ends_a_silent_replica_sends_late_are_told_once(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    await hub.join(ana, thread, None)
    await went_silent(hub)
    await pushes(ana, ana_out)
    told = []
    for signal in (Idle(thread_id="t1", sender=HELPER), Left(thread_id="t1", sender=HELPER)):
        await far(hub, signal)
        told.append(senders(await pushes(ana, ana_out)))
    await hub.sweep()
    told.append(senders(await pushes(ana, ana_out)))
    assert told == [[("stream.end", "helper"), ("idle", "helper"), ("left", "helper")], [], []]


async def test_unlinking_stops_at_the_first_failed_publish(
    storage: PostgresStorage, caplog: pytest.LogCaptureFixture
) -> None:
    hub, thread = await setup(storage)
    for identity in (ANA, HELPER, BRUNO):
        session, _ = open_session(identity)
        await hub.join(session, thread, None)
    attempts: list[dict[str, Any]] = []

    async def closing(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        attempts.append(payload)
        raise StorageError("postgres: storage rejected publish: pool is closing")

    storage.publish = closing  # type: ignore[method-assign]
    hub.linked = True
    with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
        await hub.unlink()
    assert len(attempts) == 1 and len(caplog.records) == 1 and not hub.linked


async def test_a_leave_sent_right_after_a_join_leaves(storage: PostgresStorage) -> None:
    server = Server(storage)
    await server.create_thread("acme", thread_id="t1")
    await server.add_member("acme", "t1", ANA)
    await server.add_member("acme", "t1", BRUNO)
    join = server._hub.join

    async def slow_join(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(0.3)
        return await join(*args, **kwargs)

    server._hub.join = slow_join  # type: ignore[method-assign]
    ana = await Peer.open(server, ANA)
    bruno = await Peer.open(server, BRUNO)
    await ana.end.send_text(
        encode(Request(id="j", method="thread.join", payload={"thread_id": "t1"}))
    )
    await ana.end.send_text(
        encode(Request(id="l", method="thread.leave", payload={"thread_id": "t1"}))
    )
    answers: list[str] = []
    while len(answers) < 2:
        frame = await ana.frame()
        if frame["type"] in ("response", "error"):
            answers.append(frame["id"])
    assert answers == ["j", "l"]
    await bruno.call(
        "event.send",
        thread_id="t1",
        kind="message",
        client_id="c1",
        content=[{"type": "text", "text": "after leave"}],
    )
    with pytest.raises(TimeoutError):
        while True:
            frame = await ana.frame(0.4)
            assert not (frame["type"] == "push" and frame["payload"].get("kind") == "message")
    present = await bruno.call("thread.present", thread_id="t1")
    assert "ana" not in [i["id"] for i in present["identities"]]
    await ana.end.close(1000, "")
    await bruno.end.close(1000, "")


async def test_a_leave_sent_behind_a_full_window_of_requests_still_follows_its_join(
    storage: PostgresStorage,
) -> None:
    server = Server(storage)
    await server.create_thread("acme", thread_id="t1")
    await server.add_member("acme", "t1", ANA)
    ana = await Peer.open(server, ANA)
    for n in range(63):
        await ana.end.send_text(
            encode(Request(id=f"g{n}", method="thread.get", payload={"thread_id": "t1"}))
        )
    for name in ("join", "leave"):
        await ana.end.send_text(
            encode(Request(id=name, method=f"thread.{name}", payload={"thread_id": "t1"}))
        )
    answers: list[str] = []
    while len(answers) < 65:
        frame = await ana.frame()
        if frame["type"] in ("response", "error") and frame["id"] in ("join", "leave"):
            answers.append(frame["id"])
        elif frame["type"] in ("response", "error"):
            answers.append("get")
    assert [a for a in answers if a != "get"] == ["join", "leave"]
    session = next(iter(server._hub.sessions))
    assert "t1" not in session.joined
    assert "ana" not in [i.id for i in server._hub.state.present("acme", "t1")]
    await ana.end.close(1000, "")


async def test_a_flood_of_ordered_requests_never_has_more_than_the_window_in_flight(
    storage: PostgresStorage,
) -> None:
    server = Server(storage)
    await server.create_thread("acme", thread_id="t1")
    await server.add_member("acme", "t1", ANA)
    join = server._hub.join

    async def slow_join(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(0.005)
        return await join(*args, **kwargs)

    server._hub.join = slow_join  # type: ignore[method-assign]
    answer = server._answer
    running = peak = 0

    async def counted(*args: Any, **kwargs: Any) -> None:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        try:
            await answer(*args, **kwargs)
        finally:
            running -= 1

    server._answer = counted  # type: ignore[method-assign]
    ana = await Peer.open(server, ANA)
    total = 200
    for n in range(total):
        await ana.end.send_text(
            encode(Request(id=f"j{n}", method="thread.join", payload={"thread_id": "t1"}))
        )
    answered = 0
    while answered < total:
        frame = await ana.frame(10)
        if frame["type"] in ("response", "error"):
            answered += 1
    assert 1 < peak <= 64
    await ana.end.close(1000, "")


async def test_a_leave_during_a_gap_fill_stays_left(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage, events=2)
    session, recorder = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    for n in (3, 4):
        await storage.append_event("acme", message(n, f"m{n}"))
    fourth = (await storage.list_events("acme", "t1", since=3))[0]
    reading = asyncio.Event()
    release = asyncio.Event()
    read = storage.list_events

    async def paused(*args: Any, **kwargs: Any) -> Any:
        reading.set()
        await release.wait()
        return await read(*args, **kwargs)

    storage.list_events = paused  # type: ignore[method-assign]
    delivering = asyncio.create_task(hub.deliver("acme", fourth))
    await reading.wait()
    storage.list_events = read  # type: ignore[method-assign]
    leaving = asyncio.create_task(hub.leave(session, "t1"))
    await asyncio.sleep(0)
    assert not leaving.done()
    release.set()
    await asyncio.gather(delivering, leaving)
    assert "t1" not in session.joined
    assert session not in hub._rooms.get(("acme", "t1"), set())
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    await storage.append_event("acme", message(5, "m5", BRUNO))
    await hub.deliver("acme", (await storage.list_events("acme", "t1", since=4))[0])
    assert [p["seq"] for p in await pushes(session, recorder)] == [5]


async def test_a_removed_member_gets_nothing_after_the_removal_even_when_deliveries_queue(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage, events=2)
    session, recorder = open_session(ANA)
    other, _ = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    removal, _ = await storage.append_event(
        "acme",
        MemberRemoved(
            id="r", thread_id="t1", author=BRUNO, created_at=AT, client_id="r", identity=ANA
        ),
    )
    after, _ = await storage.append_event("acme", message(4, "after", BRUNO))
    reading = asyncio.Event()
    release = asyncio.Event()
    head = storage.head

    async def paused(*args: Any, **kwargs: Any) -> Any:
        reading.set()
        await release.wait()
        return await head(*args, **kwargs)

    storage.head = paused  # type: ignore[method-assign]
    joining = asyncio.create_task(hub.join(other, thread, None))
    await reading.wait()
    storage.head = head  # type: ignore[method-assign]
    first = asyncio.create_task(hub.deliver("acme", removal))
    second = asyncio.create_task(hub.deliver("acme", after))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(joining, first, second)
    assert [p["kind"] for p in await pushes(session, recorder)] == ["member_removed"]


async def test_a_member_added_is_stored_and_served_though_one_connection_fails_to_enter(
    storage: PostgresStorage,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = Server(storage)
    ana = await Peer.open(server, ANA)
    first, second = await Peer.open(server, HELPER), await Peer.open(server, HELPER)
    await first.call("thread.follow")
    await second.call("thread.follow")
    await ana.call("thread.create", thread_id="t1")
    head = storage.head
    armed = [True]

    async def flaky(scope: str, thread_id: str, identity_id: str) -> Any:
        if identity_id == HELPER.id and armed:
            armed.pop()
            raise StorageError("postgres: storage rejected head: connection reset")
        return await head(scope, thread_id, identity_id)

    monkeypatch.setattr(storage, "head", flaky)
    with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
        reply = await ana.call(
            "member.add", thread_id="t1", identity=HELPER.model_dump(mode="json")
        )
    assert reply["member"]["identity"]["id"] == HELPER.id
    await first.quiet()
    await second.quiet()
    assert len(first.joined) + len(second.joined) == 1
    (record,) = [r for r in caplog.records if r.name == "rfnry_thread.hub"]
    assert record.getMessage().startswith("hub: ")
    assert any(s.id in record.getMessage() for s in server._hub.sessions)


async def test_an_event_is_stored_and_served_though_one_connection_fails_to_catch_up(
    storage: PostgresStorage,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = Server(storage)
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    first, second = await Peer.open(server, HELPER), await Peer.open(server, HELPER)
    await first.call("thread.join", thread_id="t1")
    await second.call("thread.join", thread_id="t1")
    behind = next(s for s in server._hub.sessions if s.identity.id == HELPER.id)
    behind.joined["t1"] -= 1
    listing = storage.list_events
    armed = [True]

    async def flaky(*args: Any, **kwargs: Any) -> Any:
        if armed and kwargs.get("since") == behind.joined["t1"]:
            armed.pop()
            raise StorageError("postgres: storage rejected list_events: connection reset")
        return await listing(*args, **kwargs)

    monkeypatch.setattr(storage, "list_events", flaky)
    with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
        stored = await say(ana, "t1", "hello")
    assert stored["seq"] > 0 and not armed
    served = [[p["kind"] for p in await peer.quiet()].count("message") for peer in (first, second)]
    assert sorted(served) == [0, 1]
    assert [r for r in caplog.records if r.name == "rfnry_thread.hub"]
