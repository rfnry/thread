import asyncio
import json
from typing import Any

import pytest
from until import until

from rfnry_thread.hub import Hub
from rfnry_thread.models import (
    Identity,
    MemberRemoved,
    StreamDelta,
    StreamStart,
    Thread,
    Working,
)
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.protocol import Request, encode
from rfnry_thread.server import Server

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
