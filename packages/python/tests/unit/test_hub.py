import asyncio
import json
import logging
from typing import Any

import pytest
from until import until

from rfnry_thread.errors import ForbiddenError, NotFoundError, StorageError
from rfnry_thread.hub import Hub
from rfnry_thread.models import (
    SYSTEM,
    Edit,
    Identity,
    Idle,
    Joined,
    Left,
    MemberRemoved,
    Present,
    Retract,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
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
from .writes import admit, append, epoch, expel


async def setup(
    storage: PostgresStorage, replay_limit: int = 500, events: int = 0
) -> tuple[Hub, Thread]:
    thread, _ = await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=AT))
    for identity in (ANA, HELPER, BRUNO):
        await enroll(storage, "t1", identity)
    for n in range(1, events + 1):
        await append(storage, "acme", message(n, f"m{n}"))
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
        await append(storage, "acme", message(n, f"m{n}"))
    fourth = (await storage.list_events("acme", "t1", since=3))[0]
    await hub.deliver("acme", fourth, thread.epoch)
    await hub.deliver("acme", fourth, thread.epoch)
    assert [p["seq"] for p in await pushes(session, recorder)] == [2, 3, 4]


async def test_a_notice_is_read_only_when_a_session_is_behind(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    reads: list[int] = []
    original = storage.tail

    async def counted(*args: Any, **kwargs: Any) -> Any:
        reads.append(1)
        return await original(*args, **kwargs)

    session, recorder = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    other, _ = await storage.create_thread("acme", Thread(id="t2", scope="acme", created_at=AT))
    storage.tail = counted  # type: ignore[method-assign]
    stored, _ = await append(storage, "acme", message(1, "hi"))
    await hub.on_event("acme", "t1", stored.seq, thread.epoch)
    await hub.on_event("acme", "t1", stored.seq, thread.epoch)
    await hub.on_event("acme", "t2", 1, other.epoch)
    assert [p["seq"] for p in await pushes(session, recorder)] == [1]
    assert len(reads) == 1


async def test_a_removed_member_is_cut_off_after_the_event(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    bruno, bruno_recorder = open_session(BRUNO)
    helper, helper_recorder = open_session(HELPER)
    await hub.join(bruno, thread, None)
    await hub.join(helper, thread, None)
    await pushes(bruno, bruno_recorder)
    removal, _ = await append(
        storage,
        "acme",
        MemberRemoved(
            id="r", thread_id="t1", author=ANA, created_at=AT, client_id="r", identity=BRUNO
        ),
    )
    await hub.deliver("acme", removal, thread.epoch)
    after, _ = await append(storage, "acme", message(2, "after"))
    await hub.deliver("acme", after, thread.epoch)
    assert kinds(await pushes(bruno, bruno_recorder)) == ["member_removed"]
    assert "t1" not in bruno.joined
    assert kinds(await pushes(helper, helper_recorder))[-3:] == [
        "member_removed",
        "left",
        "message",
    ]


async def _bruno_in(
    storage: PostgresStorage, history: Any = "since_added", following: bool = True
) -> tuple[Hub, Any, Any]:
    thread, _ = await storage.create_thread(
        "acme", Thread(id="t1", scope="acme", created_at=AT, history=history)
    )
    await admit(storage, "acme", added("t1", ANA, ANA))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    hub = Hub(storage, 500)
    bruno, recorder = open_session(BRUNO)
    bruno.following = following
    hub.sessions.add(bruno)
    await asyncio.wait_for(hub.join(bruno, thread, None), 5)
    await pushes(bruno, recorder)
    return hub, bruno, recorder


async def _epoch(storage: PostgresStorage) -> int:
    thread = await storage.get_thread("acme", "t1")
    assert thread is not None
    return thread.epoch


def _seqs(found: list[dict[str, Any]]) -> list[int]:
    return [p["seq"] for p in found if "kind" in p]


def _kinds(found: list[dict[str, Any]]) -> list[str]:
    return [p.get("kind", "joined") for p in found]


@pytest.mark.parametrize(("history", "floor"), [("all", 0), ("since_added", 5)])
async def test_a_member_removed_and_added_again_inside_a_gap_is_handed_the_thread_again(
    storage: PostgresStorage, history: Any, floor: int
) -> None:
    hub, bruno, recorder = await _bruno_in(storage, history)
    await append(storage, "acme", message(3, "before"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    last, _ = await append(storage, "acme", message(6, "after"))
    await asyncio.wait_for(hub.deliver("acme", last, await _epoch(storage)), 5)
    found = await pushes(bruno, recorder)
    assert _kinds(found) == ["message", "member_removed", "member_added", "message", "joined"]
    assert _seqs(found) == [3, 4, 5, 6]
    assert bruno.joined == {"t1": 6}
    assert bruno.floors.get("t1", 0) == floor
    later, _ = await append(storage, "acme", message(7, "live"))
    await asyncio.wait_for(hub.deliver("acme", later, await _epoch(storage)), 5)
    assert _seqs(await pushes(bruno, recorder)) == [7]


async def test_a_session_not_following_is_invited_after_the_gap_removed_it(
    storage: PostgresStorage,
) -> None:
    hub, bruno, recorder = await _bruno_in(storage, "all", following=False)
    await append(storage, "acme", message(3, "before"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    last, _ = await append(storage, "acme", message(6, "after"))
    await asyncio.wait_for(hub.deliver("acme", last, await _epoch(storage)), 5)
    found = await pushes(bruno, recorder)
    assert _kinds(found) == ["message", "member_removed", "invited"]
    assert "t1" not in bruno.joined


async def test_two_sessions_of_one_identity_cut_by_a_gap_are_each_invited_once(
    storage: PostgresStorage,
) -> None:
    hub, first, first_recorder = await _bruno_in(storage, "all", following=False)
    thread = await storage.get_thread("acme", "t1")
    assert thread is not None
    second, second_recorder = open_session(BRUNO)
    hub.sessions.add(second)
    await asyncio.wait_for(hub.join(second, thread, None), 5)
    await pushes(second, second_recorder)
    await append(storage, "acme", message(3, "before"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    last, _ = await append(storage, "acme", message(6, "after"))
    await asyncio.wait_for(hub.deliver("acme", last, await _epoch(storage)), 5)
    for session, recorder in ((first, first_recorder), (second, second_recorder)):
        assert _kinds(await pushes(session, recorder)).count("invited") == 1


async def test_a_gap_that_ends_with_the_member_removed_is_not_handed_the_thread_again(
    storage: PostgresStorage,
) -> None:
    hub, bruno, recorder = await _bruno_in(storage)
    await append(storage, "acme", message(3, "before"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await append(storage, "acme", message(5, "while out"))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    await append(storage, "acme", message(7, "back"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    last, _ = await append(storage, "acme", message(9, "after"))
    await asyncio.wait_for(hub.deliver("acme", last, await _epoch(storage)), 5)
    assert _seqs(await pushes(bruno, recorder)) == [3, 4]
    assert "t1" not in bruno.joined


async def test_a_re_add_already_followed_by_a_removal_in_storage_is_not_handed_again(
    storage: PostgresStorage,
) -> None:
    hub, bruno, recorder = await _bruno_in(storage)
    await append(storage, "acme", message(3, "before"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await append(storage, "acme", message(5, "while out"))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    live, _ = await append(storage, "acme", message(7, "back"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await asyncio.wait_for(hub.deliver("acme", live, await _epoch(storage)), 5)
    assert _seqs(await pushes(bruno, recorder)) == [3, 4]
    assert "t1" not in bruno.joined


@pytest.mark.parametrize("kind", ["edit", "retract"])
async def test_a_change_to_what_the_new_floor_excludes_never_reaches_a_re_added_member(
    storage: PostgresStorage, kind: str
) -> None:
    hub, bruno, recorder = await _bruno_in(storage)
    old, _ = await append(storage, "acme", message(3, "before"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    if kind == "edit":
        change: Edit | Retract = Edit(
            id="x6",
            thread_id="t1",
            author=ANA,
            created_at=AT,
            client_id="x6",
            target=old.id,
            content=[Text(text="secret")],
        )
    else:
        change = Retract(
            id="x6", thread_id="t1", author=ANA, created_at=AT, client_id="x6", target=old.id
        )
    await append(storage, "acme", change)
    last, _ = await append(storage, "acme", message(7, "back"))
    await asyncio.wait_for(hub.deliver("acme", last, await _epoch(storage)), 5)
    assert _seqs(await pushes(bruno, recorder)) == [3, 4, 5, 7]
    assert bruno.joined == {"t1": 7}


async def test_a_re_add_that_is_the_live_event_hands_the_thread_again(
    storage: PostgresStorage,
) -> None:
    hub, bruno, recorder = await _bruno_in(storage)
    await append(storage, "acme", message(3, "before"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    _, live = await admit(storage, "acme", added("t1", BRUNO, ANA))
    assert live is not None
    await asyncio.wait_for(hub.deliver("acme", live, await _epoch(storage)), 5)
    found = await pushes(bruno, recorder)
    assert _seqs(found) == [3, 4, 5]
    assert _kinds(found)[-1] == "joined"
    assert bruno.joined == {"t1": 5}


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
    assert hub.state.present("acme", "t1", thread.epoch) == [HELPER]


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
    assert [s.kind for s in hub.state.snapshot("acme", "t1", thread.epoch)] == ["present"]


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
    assert [s.kind for s in hub.state.snapshot("acme", "t1", thread.epoch)] == ["present"]
    assert [s.kind for s in hub.state.snapshot("acme", "t2", other.epoch)] == [
        "present",
        "working",
        "stream.start",
    ]
    assert (helper.working, helper.streams) == ({"t2"}, {("t2", "s1")})


async def test_a_member_cut_and_joined_again_before_its_ends_go_out_keeps_what_it_started_since(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    helper, _ = open_session(HELPER)
    await hub.join(ana, thread, None)
    await hub.join(helper, thread, None)
    await hub.signal(helper, Working(thread_id="t1", sender=HELPER, label="Old"))
    await hub.signal(helper, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    await pushes(ana, ana_out)
    await expel(storage, "acme", removed("t1", HELPER))
    _, back = await admit(storage, "acme", added("t1", HELPER))
    assert back is not None
    reached, release = gated(storage, monkeypatch, "tail")
    catching = asyncio.create_task(hub.on_event("acme", "t1", back.seq, thread.epoch))
    await asyncio.wait_for(reached.wait(), 5)
    rejoining = asyncio.create_task(hub.join(helper, thread, None))
    await asyncio.sleep(0)
    release.set()
    await asyncio.wait_for(asyncio.gather(catching, rejoining), 5)
    await hub.signal(helper, Working(thread_id="t1", sender=HELPER, label="New"))
    await hub.signal(helper, StreamStart(thread_id="t1", sender=HELPER, stream_id="s2"))
    assert kinds(await pushes(ana, ana_out)) == [
        "member_removed",
        "member_added",
        "stream.end",
        "idle",
        "left",
        "present",
        "working",
        "stream.start",
    ]
    held = [
        (s.kind, s.sender.id, getattr(s, "label", None) or getattr(s, "stream_id", None))
        for s in hub.state.snapshot("acme", "t1", thread.epoch)
    ]
    assert held == [
        ("present", "ana", None),
        ("present", "helper", None),
        ("working", "helper", "New"),
        ("stream.start", "helper", "s2"),
    ]


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
            "e": thread.epoch,
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
    notice = {
        "kind": "invited",
        "thread_id": "t1",
        "sender": ANA.model_dump(mode="json"),
        "e": thread.epoch,
    }
    await hub.on_signal("acme", "t1", {"o": "older", **notice, "to": "bruno"})
    assert kinds(await pushes(plain, plain_recorder)) == ["invited"]
    await following.outbound.drain()
    assert [frame["name"] for frame in following_recorder.frames] == ["joined"]
    assert following.joined == {"t1": 2}


async def test_an_invited_notice_for_a_non_member_is_ignored(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage, events=2)
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
        "e": thread.epoch,
    }
    await hub.on_signal("acme", "t1", dict(notice))
    assert await pushes(plain, plain_recorder) == []
    await following.outbound.drain()
    assert following_recorder.frames == []
    assert "t1" not in plain.joined and "t1" not in following.joined
    await admit(storage, "acme", added("t1", mallory))
    await expel(storage, "acme", removed("t1", mallory))
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
    await hub.deleted("acme", "t1", thread.epoch, ANA)
    assert kinds(await pushes(ana, recorder)) == ["thread.deleted"]
    assert ana.joined == {} and hub.state.snapshot("acme", "t1", thread.epoch) == []


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
    old, other = await _epoch(storage), await epoch(storage, "other", "t1")
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
    assert hub.state.snapshot("acme", "t1", old) == []
    assert [(scope, thread_id) for scope, thread_id, _, _ in hub.state.own()] == [("other", "t1")]
    with pytest.raises(ForbiddenError, match="^server: signals need a join"):
        await bruno.call("signal.send", thread_id="t1", kind="idle")
    await say(elsewhere, "t1", "still here")
    assert [p["kind"] for p in await elsewhere.quiet()] == ["message"]
    assert hub.state.present("other", "t1", other) == [ANA]


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

    async def held(scope: str, thread_id: str, identity_id: str) -> tuple[int, int, int] | None:
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
    made, _ = await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=AT))
    inviting = []
    for identity in (ANA, HELPER):
        _, event = await admit(storage, "acme", added("t1", identity))
        assert event is not None
        invited = {
            "o": "near",
            "kind": "invited",
            "thread_id": "t1",
            "sender": SYSTEM.model_dump(mode="json"),
            "to": identity.id,
            "seq": event.seq,
            "e": made.epoch,
        }
        inviting.append(asyncio.create_task(hub.on_signal("acme", "t1", invited)))
    await until(lambda: sorted(checked) == ["ana", "helper"])
    assert not deleting.done()
    release.set()
    done = asyncio.gather(joining, deleting, *inviting, return_exceptions=True)
    refused, *rest = await asyncio.wait_for(done, 2.0)
    assert isinstance(refused, NotFoundError) and rest == [None] * 3
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

    async def held(scope: str, thread_id: str, identity_id: str) -> tuple[int, int, int] | None:
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
    done = asyncio.gather(*joining, deleting, working, return_exceptions=True)
    *joins, deleted, signalled = await asyncio.wait_for(done, 2.0)
    assert all(isinstance(j, Joined | NotFoundError) for j in joins)
    assert (deleted, signalled) == (None, None)
    assert hub.state.own() == [] and hub._local == {} and hub._rooms == {}
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
        lambda: (
            len(near.state.present("acme", "t1", thread.epoch))
            == len(far.state.present("acme", "t1", thread.epoch))
            == 2
        )
    )
    await pushes(speaker, speaker_recorder)
    await pushes(listener, listener_recorder)
    working = Working(thread_id="t1", sender=HELPER, label="Reading")
    await near.signal(speaker, working)
    await until(lambda: far.state.snapshot("acme", "t1", thread.epoch)[-1] == working)
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
    assert far.state.present("acme", "t1", thread.epoch) == []
    await near.announce()
    await until(lambda: far.state.present("acme", "t1", thread.epoch) == [ANA])
    for stop in stops:
        await stop()


async def far(hub: Hub, thread: Thread, signal: Signal, n: int) -> None:
    payload = {"o": "far", "n": n, "e": thread.epoch, **signal.model_dump(mode="json")}
    await hub.on_signal("acme", "t1", payload)


async def went_silent(hub: Hub, thread: Thread) -> None:
    signals: list[Signal] = [
        Present(thread_id="t1", sender=HELPER),
        Working(thread_id="t1", sender=HELPER),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1", in_reply_to="e1"),
    ]
    for n, signal in enumerate(signals, 1):
        await far(hub, thread, signal, n)
    now = hub.state._clock()
    hub.state._clock = lambda: now + EXPIRY + 1


def senders(payloads: list[dict[str, Any]]) -> list[tuple[str, str]]:
    return [(p["kind"], p["sender"]["id"]) for p in payloads]


async def test_a_silent_replicas_state_is_ended_once_by_the_tick_and_its_late_ends_tell_nothing(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    await hub.join(ana, thread, None)
    await went_silent(hub, thread)
    await pushes(ana, ana_out)
    bruno, bruno_out = open_session(BRUNO)
    await hub.join(bruno, thread, None)
    assert senders(await pushes(bruno, bruno_out)) == [
        ("present", "ana"),
        ("present", "helper"),
        ("working", "helper"),
        ("stream.start", "helper"),
        ("present", "bruno"),
    ]
    hub.sweep()
    ends = [("stream.end", "helper"), ("idle", "helper"), ("left", "helper")]
    assert senders(await pushes(ana, ana_out)) == [("present", "bruno"), *ends]
    assert senders(await pushes(bruno, bruno_out)) == ends
    late: list[Signal] = [
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=1, text="more"),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        Idle(thread_id="t1", sender=HELPER),
        Left(thread_id="t1", sender=HELPER),
    ]
    for n, signal in enumerate(late, 4):
        await far(hub, thread, signal, n)
    hub.sweep()
    assert await pushes(ana, ana_out) == [] and await pushes(bruno, bruno_out) == []


async def test_a_leave_after_a_replica_went_silent_is_one_left(storage: PostgresStorage) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    helper, _ = open_session(HELPER)
    await hub.join(ana, thread, None)
    await hub.join(helper, thread, None)
    await went_silent(hub, thread)
    await pushes(ana, ana_out)
    await hub.leave(helper, "t1")
    hub.sweep()
    assert senders(await pushes(ana, ana_out)) == [
        ("stream.end", "helper"),
        ("idle", "helper"),
        ("left", "helper"),
    ]


async def test_a_snapshot_from_a_replica_heard_from_again_announces_it_once(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage)
    ana, ana_out = open_session(ANA)
    await hub.join(ana, thread, None)
    await went_silent(hub, thread)
    hub.sweep()
    await pushes(ana, ana_out)
    entry = {
        "sender": HELPER.model_dump(mode="json"),
        "present": True,
        "working": {"label": None},
        "streams": [{"stream_id": "s1", "in_reply_to": "e1"}],
    }
    snapshot = {"o": "far", "kind": "refresh", "n": 4, "e": thread.epoch, "part": 0, "parts": 1}
    await hub.on_signal("acme", "t1", {**snapshot, "entries": [entry]})
    told = await pushes(ana, ana_out)
    assert senders(told) == [
        ("present", "helper"),
        ("working", "helper"),
        ("stream.start", "helper"),
    ]
    assert told[-1]["in_reply_to"] == "e1"
    later: list[Signal] = [
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"),
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=1, text="more"),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
    ]
    for n, signal in enumerate(later, 5):
        await far(hub, thread, signal, n)
    assert senders(await pushes(ana, ana_out)) == [("stream.end", "helper")]


async def test_unlinking_sends_an_empty_snapshot_per_thread_at_once_and_logs_each_that_fails(
    storage: PostgresStorage, caplog: pytest.LogCaptureFixture
) -> None:
    hub, thread = await setup(storage)
    threads = [thread]
    for thread_id in ("t2", "t3"):
        made, _ = await storage.create_thread(
            "acme", Thread(id=thread_id, scope="acme", created_at=AT)
        )
        await enroll(storage, thread_id, ANA)
        threads.append(made)
    session, _ = open_session(ANA)
    for made in threads:
        await hub.join(session, made, None)
    attempts: list[tuple[str, dict[str, Any]]] = []
    closed = asyncio.Event()

    async def closing(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        attempts.append((thread_id, payload))
        await closed.wait()
        raise StorageError("postgres: storage rejected publish: pool is closing")

    storage.publish = closing  # type: ignore[method-assign]
    hub.linked = True
    with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
        hub.unlink()
        assert not hub.linked
        assert [(t, p["kind"], p["e"], p["entries"]) for t, p in attempts] == [
            (made.id, "refresh", made.epoch, []) for made in threads
        ]
        draining = asyncio.ensure_future(hub.drain())
        await asyncio.sleep(0.05)
        assert not draining.done()
        closed.set()
        await asyncio.wait_for(draining, 2.0)
    assert [r.getMessage() for r in caplog.records] == [
        "hub: mirroring to other replicas failed: postgres: storage rejected publish: "
        "pool is closing"
    ] * 3


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
    assert "ana" not in [
        i.id for i in server._hub.state.present("acme", "t1", await _epoch(storage))
    ]
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
        await append(storage, "acme", message(n, f"m{n}"))
    fourth = (await storage.list_events("acme", "t1", since=3))[0]
    reading = asyncio.Event()
    release = asyncio.Event()
    read = storage.tail

    async def paused(*args: Any, **kwargs: Any) -> Any:
        reading.set()
        await release.wait()
        return await read(*args, **kwargs)

    storage.tail = paused  # type: ignore[method-assign]
    delivering = asyncio.create_task(hub.deliver("acme", fourth, thread.epoch))
    await reading.wait()
    storage.tail = read  # type: ignore[method-assign]
    leaving = asyncio.create_task(hub.leave(session, "t1"))
    await asyncio.sleep(0)
    assert not leaving.done()
    release.set()
    await asyncio.gather(delivering, leaving)
    assert "t1" not in session.joined
    room = hub._rooms.get(("acme", "t1"))
    assert room is None or session not in room.sessions
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    await append(storage, "acme", message(5, "m5", BRUNO))
    fifth = (await storage.list_events("acme", "t1", since=4))[0]
    await hub.deliver("acme", fifth, thread.epoch)
    assert [p["seq"] for p in await pushes(session, recorder)] == [5]


async def test_a_removed_member_gets_nothing_after_the_removal_even_when_deliveries_queue(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage, events=2)
    session, recorder = open_session(ANA)
    other, _ = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    removal, _ = await append(
        storage,
        "acme",
        MemberRemoved(
            id="r", thread_id="t1", author=BRUNO, created_at=AT, client_id="r", identity=ANA
        ),
    )
    after, _ = await append(storage, "acme", message(4, "after", BRUNO))
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
    first = asyncio.create_task(hub.deliver("acme", removal, thread.epoch))
    second = asyncio.create_task(hub.deliver("acme", after, thread.epoch))
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
    listing = storage.tail
    armed = [True]

    async def flaky(scope: str, thread_id: str, since: int, floor: int, limit: int) -> Any:
        if armed and since == behind.joined["t1"]:
            armed.pop()
            raise StorageError("postgres: storage rejected tail: connection reset")
        return await listing(scope, thread_id, since, floor, limit)

    monkeypatch.setattr(storage, "tail", flaky)
    with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
        stored = await say(ana, "t1", "hello")
    assert stored["seq"] > 0 and not armed
    served = [[p["kind"] for p in await peer.quiet()].count("message") for peer in (first, second)]
    assert sorted(served) == [0, 1]
    assert [r for r in caplog.records if r.name == "rfnry_thread.hub"]


def gated(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, name: str, after: int = 0
) -> tuple[asyncio.Event, asyncio.Event]:
    reached, release = asyncio.Event(), asyncio.Event()
    real = getattr(storage, name)
    calls = [0]

    async def held(*args: Any, **kwargs: Any) -> Any:
        calls[0] += 1
        if calls[0] == after + 1:
            reached.set()
            await release.wait()
        return await real(*args, **kwargs)

    monkeypatch.setattr(storage, name, held)
    return reached, release


async def test_a_member_cut_by_a_fan_out_that_is_cancelled_is_out_of_the_room_and_hears_nothing_more(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    hub, thread = await setup(storage)
    bruno, bruno_recorder = open_session(BRUNO)
    helper, helper_recorder = open_session(HELPER)
    await hub.join(bruno, thread, None)
    await append(storage, "acme", message(1, "before helper", HELPER))
    await hub.join(helper, thread, None)
    await pushes(bruno, bruno_recorder)
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await append(storage, "acme", message(3, "while out", HELPER))
    live, _ = await append(storage, "acme", message(4, "live", HELPER))
    real = storage.tail
    reached, _ = gated(storage, monkeypatch, "tail", after=1)
    delivering = asyncio.create_task(hub.deliver("acme", live, thread.epoch))
    await asyncio.wait_for(reached.wait(), 5)
    delivering.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivering
    monkeypatch.setattr(storage, "tail", real)
    assert "t1" not in bruno.joined
    await until(
        lambda: [i.id for i in hub.state.present("acme", "t1", thread.epoch)] == [HELPER.id]
    )
    after, _ = await append(storage, "acme", message(5, "after the removal", HELPER))
    await asyncio.wait_for(hub.deliver("acme", after, thread.epoch), 5)
    assert kinds(await pushes(bruno, bruno_recorder)) == ["message", "member_removed"]


async def test_a_member_removed_and_added_again_is_handed_the_thread_though_the_fan_out_is_cancelled(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    hub, bruno, recorder = await _bruno_in(storage, "all")
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    live, _ = await append(storage, "acme", message(5, "after"))
    epoch = await _epoch(storage)
    reached, release = gated(storage, monkeypatch, "is_member")
    delivering = asyncio.create_task(hub.deliver("acme", live, epoch))
    await asyncio.wait_for(reached.wait(), 5)
    delivering.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delivering
    release.set()
    await until(lambda: bruno.joined.get("t1") == 5)
    assert "joined" in _kinds(await pushes(bruno, recorder))


async def test_a_member_removed_and_added_again_is_handed_the_thread_though_the_catch_up_that_found_it_fails_after(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    hub, bruno, recorder = await _bruno_in(storage, "all")
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    thread = await storage.get_thread("acme", "t1")
    assert thread is not None
    ana, _ = open_session(ANA)
    await hub.join(ana, thread, None)
    tail = storage.tail

    async def flaky(scope: str, thread_id: str, since: int, floor: int, limit: int) -> Any:
        if since == ana.joined["t1"]:
            raise StorageError("postgres: storage rejected tail: connection reset")
        return await tail(scope, thread_id, since, floor, limit)

    monkeypatch.setattr(storage, "tail", flaky)
    await asyncio.wait_for(hub.on_event("acme", "t1", 4, thread.epoch), 5)
    await until(lambda: bruno.joined.get("t1") == 4)
    assert _kinds(await pushes(bruno, recorder))[-1] == "joined"


async def test_a_member_cut_and_added_again_is_not_handed_a_thread_made_again_meanwhile(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    hub, bruno, recorder = await _bruno_in(storage, "all")
    writer = Server(storage)
    epoch = await _epoch(storage)
    await writer.remove_member("acme", "t1", BRUNO)
    await writer.add_member("acme", "t1", BRUNO)
    reached, release = gated(storage, monkeypatch, "is_member")
    delivering = asyncio.create_task(hub.on_event("acme", "t1", 4, epoch))
    await asyncio.wait_for(reached.wait(), 5)
    await writer.delete_thread("acme", "t1")
    await writer.create_thread("acme", "t1")
    for identity in (ANA, BRUNO):
        await writer.add_member("acme", "t1", identity)
    for n in range(3):
        await writer.update_thread("acme", "t1", {"n": n})
    release.set()
    await asyncio.wait_for(delivering, 5)
    assert _kinds(await pushes(bruno, recorder)) == ["member_removed"]
    assert "t1" not in bruno.joined


async def test_a_write_reaches_the_others_though_the_writers_socket_drops_while_it_is_delivered(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = Server(storage)
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await helper.quiet()
    behind = next(s for s in server._hub.sessions if s.identity.id == HELPER.id)
    behind.joined["t1"] -= 1
    reached, release = gated(storage, monkeypatch, "tail")
    writer = await Peer.open(server, ANA)
    await writer.end.send_text(
        encode(
            Request(
                id="1",
                method="event.send",
                payload={
                    "thread_id": "t1",
                    "kind": "message",
                    "client_id": "c1",
                    "content": [{"type": "text", "text": "live"}],
                },
            )
        )
    )
    await asyncio.wait_for(reached.wait(), 5)
    await writer.close()
    await until(lambda: sum(s.identity.id == ANA.id for s in server._hub.sessions) == 1)
    release.set()
    assert [p["content"][0]["text"] for p in await helper.quiet(0.5) if p["kind"] == "message"] == [
        "live"
    ]


async def test_an_edit_that_cannot_be_caught_up_after_it_is_stored_is_acknowledged_and_caught_up_later(
    storage: PostgresStorage,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    server = Server(storage)
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1", history="since_added")
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    bruno = await Peer.open(server, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    first = await say(ana, "t1", "first")
    await bruno.quiet()
    tail = storage.tail

    async def failing(scope: str, thread_id: str, since: int, floor: int, limit: int) -> Any:
        raise StorageError("postgres: storage rejected tail: connection reset")

    monkeypatch.setattr(storage, "tail", failing)
    with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
        edit = await ana.call(
            "event.send",
            thread_id="t1",
            kind="edit",
            client_id="c-edit",
            target=first["id"],
            content=[{"type": "text", "text": "first, edited"}],
        )
    assert edit["event"]["kind"] == "edit"
    assert await bruno.quiet() == []
    assert "hub: catching thread 't1' up from storage failed" in caplog.text
    monkeypatch.setattr(storage, "tail", tail)
    await say(ana, "t1", "next")
    assert [p["kind"] for p in await bruno.quiet()] == ["edit", "message"]


@pytest.mark.parametrize("crossing", ["deleted", "removed"])
async def test_a_join_that_crosses_the_threads_deletion_or_its_own_removal_is_refused(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, crossing: str
) -> None:
    hub, thread = await setup(storage, events=2)
    session, recorder = open_session(BRUNO)
    reached, release = gated(storage, monkeypatch, "tail")
    joining = asyncio.create_task(hub.join(session, thread, 0))
    await asyncio.wait_for(reached.wait(), 5)
    if crossing == "deleted":
        await storage.delete_thread("acme", "t1")
        other = asyncio.create_task(hub.deleted("acme", "t1", thread.epoch, ANA))
    else:
        removal = await expel(storage, "acme", removed("t1", BRUNO, ANA))
        assert removal is not None
        other = asyncio.create_task(hub.deliver("acme", removal, thread.epoch))
    await asyncio.sleep(0.05)
    release.set()
    with pytest.raises(NotFoundError, match="^server: thread 't1' was not found$"):
        await asyncio.wait_for(joining, 5)
    await asyncio.wait_for(other, 5)
    assert "t1" not in session.joined and "t1" not in session.present
    assert ("acme", "t1") not in hub._rooms


async def test_a_join_since_past_where_a_connection_already_is_says_truncated(
    storage: PostgresStorage,
) -> None:
    hub, thread = await setup(storage, events=3)
    session, recorder = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    joined = await hub.join(session, thread, 8)
    assert (joined.seq, joined.truncated) == (3, True)
    joined = await hub.join(session, thread, 3)
    assert (joined.seq, joined.truncated) == (3, False)
    assert [p for p in await pushes(session, recorder) if "seq" in p] == []


@pytest.mark.parametrize("crossing", ["deleted", "removed"])
async def test_a_follow_answers_no_join_for_a_thread_deleted_or_left_by_its_removal_while_it_ran(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, crossing: str
) -> None:
    hub, thread = await setup(storage, events=2)
    second, _ = await storage.create_thread("acme", Thread(id="t2", scope="acme", created_at=AT))
    await enroll(storage, "t2", BRUNO)
    await append(storage, "acme", message(1, "m1", BRUNO).model_copy(update={"thread_id": "t2"}))
    session, _ = open_session(BRUNO)
    session.following = True
    reached, release = gated(storage, monkeypatch, "tail")
    following = asyncio.create_task(hub.follow(session, [(thread, None), (second, 0)], set()))
    await asyncio.wait_for(reached.wait(), 5)
    assert "t1" in session.joined
    if crossing == "deleted":
        await storage.delete_thread("acme", "t1")
        await asyncio.wait_for(hub.deleted("acme", "t1", thread.epoch, ANA), 5)
    else:
        removal = await expel(storage, "acme", removed("t1", BRUNO, ANA))
        assert removal is not None
        await asyncio.wait_for(hub.deliver("acme", removal, thread.epoch), 5)
    assert "t1" not in session.joined
    release.set()
    answered = await asyncio.wait_for(following, 5)
    assert [joined.thread.id for joined in answered] == ["t2"]
