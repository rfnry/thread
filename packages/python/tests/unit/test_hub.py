import json
from typing import Any

import pytest
from until import until

from rfnry_thread.hub import Hub
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import (
    SYSTEM,
    Member,
    MemberRemoved,
    StreamDelta,
    StreamStart,
    Thread,
    Working,
)

from .factories import ANA, AT, BRUNO, HELPER, message
from .hubkit import kinds, open_session, pushes


async def setup(replay_limit: int = 500, events: int = 0) -> tuple[InMemoryStorage, Hub, Thread]:
    storage = InMemoryStorage()
    thread, _ = await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=AT))
    for identity in (ANA, HELPER, BRUNO):
        await storage.add_member(
            "acme", Member(thread_id="t1", identity=identity, added_at=AT, added_by=SYSTEM)
        )
    for n in range(1, events + 1):
        await storage.append_event("acme", message(n, f"m{n}"))
    return storage, Hub(storage, replay_limit), thread


async def test_a_live_join_replays_nothing_and_reports_the_head() -> None:
    _, hub, thread = await setup(events=3)
    session, recorder = open_session(HELPER)
    joined = await hub.join(session, thread, None)
    assert (joined.seq, joined.truncated) == (3, False)
    assert kinds(await pushes(session, recorder)) == ["present"]
    assert session.joined == {"t1": 3}


async def test_a_join_since_replays_what_followed_and_truncates_to_the_latest_page() -> None:
    _, hub, thread = await setup(replay_limit=3, events=5)
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


async def test_a_join_since_past_the_head_replays_nothing_and_says_truncated() -> None:
    _, hub, thread = await setup(events=3)
    session, recorder = open_session(HELPER)
    joined = await hub.join(session, thread, 8)
    assert (joined.seq, joined.truncated) == (3, True)
    assert kinds(await pushes(session, recorder)) == ["present"]
    assert session.joined == {"t1": 3}


async def test_delivery_drops_repeats_and_fills_gaps_from_storage() -> None:
    storage, hub, thread = await setup(events=1)
    session, recorder = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    for n in (2, 3, 4):
        await storage.append_event("acme", message(n, f"m{n}"))
    fourth = (await storage.list_events("acme", "t1", since=3))[0]
    await hub.deliver("acme", fourth)
    await hub.deliver("acme", fourth)
    assert [p["seq"] for p in await pushes(session, recorder)] == [2, 3, 4]


async def test_a_notice_is_read_only_when_a_session_is_behind() -> None:
    storage, hub, thread = await setup()
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


async def test_a_removed_member_is_cut_off_after_the_event() -> None:
    storage, hub, thread = await setup()
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


async def test_a_joining_session_first_receives_the_current_state() -> None:
    _, hub, thread = await setup()
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


async def test_two_connections_of_one_member_are_one_presence() -> None:
    _, hub, thread = await setup()
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


async def test_a_dropped_connection_ends_its_streams_and_work() -> None:
    _, hub, thread = await setup()
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


async def test_leaving_ends_its_streams_and_work_in_that_thread_only() -> None:
    storage, hub, thread = await setup()
    other, _ = await storage.create_thread("acme", Thread(id="t2", scope="acme", created_at=AT))
    await storage.add_member(
        "acme", Member(thread_id="t2", identity=HELPER, added_at=AT, added_by=SYSTEM)
    )
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


async def test_an_invitation_reaches_only_connections_not_in_the_thread() -> None:
    _, hub, thread = await setup()
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


async def test_an_add_is_published_as_an_invitation_with_its_seq() -> None:
    storage, hub, thread = await setup()
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


async def test_an_invitation_without_a_seq_invites_and_hands_followers_the_thread() -> None:
    _, hub, thread = await setup(events=2)
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


async def test_deletion_tells_the_room_and_empties_it() -> None:
    _, hub, thread = await setup()
    ana, recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    await pushes(ana, recorder)
    await hub.deleted("acme", "t1", ANA)
    assert kinds(await pushes(ana, recorder)) == ["thread.deleted"]
    assert ana.joined == {} and hub.state.snapshot("acme", "t1") == []


async def test_linked_hubs_mirror_signals_and_drop_their_own() -> None:
    storage, near, thread = await setup()
    far = Hub(storage, 500)
    for hub in (near, far):
        hub.linked = True
        await storage.listen(hub.on_event, hub.on_signal)
    speaker, speaker_recorder = open_session(HELPER)
    listener, listener_recorder = open_session(ANA)
    await near.join(speaker, thread, None)
    await far.join(listener, thread, None)
    await pushes(speaker, speaker_recorder)
    await pushes(listener, listener_recorder)
    await near.signal(speaker, Working(thread_id="t1", sender=HELPER, label="Reading"))
    assert kinds(await pushes(listener, listener_recorder)) == ["working"]
    assert kinds(await pushes(speaker, speaker_recorder)) == ["working"]
    assert far.state.snapshot("acme", "t1")[-1] == Working(
        thread_id="t1", sender=HELPER, label="Reading"
    )


@pytest.mark.parametrize("text", ["x" * 20000, "\U0001f600" * 5000])
async def test_a_long_delta_is_mirrored_in_pieces_under_the_notify_limit(text: str) -> None:
    storage, near, thread = await setup()
    far = Hub(storage, 500)
    published: list[int] = []
    original = storage.publish

    async def measured(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        notice = {"s": scope, "t": thread_id, "p": payload}
        published.append(len(json.dumps(notice).encode()))
        await original(scope, thread_id, payload)

    storage.publish = measured  # type: ignore[method-assign]
    for hub in (near, far):
        hub.linked = True
        await storage.listen(hub.on_event, hub.on_signal)
    speaker, _ = open_session(HELPER)
    listener, recorder = open_session(ANA)
    await near.join(speaker, thread, None)
    await far.join(listener, thread, None)
    await pushes(listener, recorder)
    await near.signal(speaker, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    await near.signal(
        speaker,
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text=text),
    )
    received = await pushes(listener, recorder)
    assert "".join(p.get("text", "") for p in received) == text
    assert max(published) < 7900


async def test_announce_teaches_another_replica_who_is_here() -> None:
    storage, near, thread = await setup()
    far = Hub(storage, 500)
    ana, _ = open_session(ANA)
    await near.join(ana, thread, None)
    for hub in (near, far):
        hub.linked = True
        await storage.listen(hub.on_event, hub.on_signal)
    assert far.state.present("acme", "t1") == []
    await near.announce()
    await until(lambda: far.state.present("acme", "t1") == [ANA])
