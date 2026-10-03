import asyncio
from typing import Any

import pytest

from rfnry_thread.errors import ForbiddenError, InputError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import SYSTEM, Identity, Member, Thread
from rfnry_thread.server import Server

from .conftest import say
from .factories import ANA, AT, BRUNO, HELPER
from .peer import Peer


def who(identity: Identity) -> dict[str, Any]:
    return identity.model_dump(mode="json")


async def threads(server: Server, *thread_ids: str) -> Peer:
    ana = await Peer.open(server, ANA)
    for thread_id in thread_ids:
        await ana.call("thread.create", thread_id=thread_id)
        await ana.call("member.add", thread_id=thread_id, identity=who(HELPER))
    return ana


def texts(pushes: list[dict[str, Any]]) -> list[str]:
    return [p["content"][0]["text"] for p in pushes if p["kind"] == "message"]


async def test_follow_delivers_every_thread_its_member_belongs_to_live(server: Server) -> None:
    ana = await threads(server, "t1", "t2")
    helper = await Peer.open(server, HELPER)
    reply = await helper.call("thread.follow")
    assert sorted(j["thread"]["id"] for j in reply["joined"]) == ["t1", "t2"]
    await say(ana, "t1", "one")
    await say(ana, "t2", "two")
    assert sorted(texts(await helper.quiet())) == ["one", "two"]


async def test_following_is_not_presence_but_a_follower_may_signal(server: Server) -> None:
    ana = await threads(server, "t1")
    await ana.call("thread.join", thread_id="t1")
    await ana.quiet()
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    assert [p["kind"] for p in await ana.quiet()] == []
    assert (await ana.call("thread.present", thread_id="t1"))["identities"] == [who(ANA)]
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working"))["label"] == "Reading"
    await ana.call("signal.send", thread_id="t1", kind="cancel", to=who(HELPER))
    assert (await helper.push("cancel"))["to"]["id"] == "helper"


async def test_a_follower_receives_a_thread_it_is_added_to_from_the_add(server: Server) -> None:
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    ana = await threads(server, "t1")
    await say(ana, "t1", "right after")
    pushes = await helper.quiet()
    assert [j["thread"]["id"] for j in helper.joined] == ["t1"]
    assert [p["kind"] for p in pushes] == ["member_added", "message"]


async def test_a_connection_that_does_not_follow_is_still_invited(server: Server) -> None:
    helper = await Peer.open(server, HELPER)
    await threads(server, "t1")
    assert [p["kind"] for p in await helper.quiet()] == ["invited"]
    assert helper.joined == []


async def test_a_membership_added_on_another_replica_reaches_a_follower(
    storage: InMemoryStorage,
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    try:
        helper = await Peer.open(far, HELPER)
        await helper.call("thread.follow")
        ana = await threads(near, "t1")
        await say(ana, "t1", "across")
        pushes = await helper.quiet(0.3)
        assert [j["thread"]["id"] for j in helper.joined] == ["t1"]
        assert texts(pushes) == ["across"]
    finally:
        await near.stop()
        await far.stop()


async def test_authorize_decides_which_threads_a_follower_receives(
    storage: InMemoryStorage,
) -> None:
    async def authorize(actor: Identity, action: str, thread: Thread) -> bool:
        return not thread.metadata.get("secret")

    server = Server(storage, authorize=authorize)
    for thread_id, metadata in (("open", {}), ("closed", {"secret": True})):
        await server.create_thread("acme", thread_id, metadata)
        await server.add_member("acme", thread_id, HELPER)
    helper = await Peer.open(server, HELPER)
    reply = await helper.call("thread.follow")
    assert [j["thread"]["id"] for j in reply["joined"]] == ["open"]


async def test_leave_stops_one_followed_thread(server: Server) -> None:
    ana = await threads(server, "t1", "t2")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await helper.call("thread.leave", thread_id="t1")
    await say(ana, "t1", "gone")
    await say(ana, "t2", "kept")
    assert texts(await helper.quiet()) == ["kept"]


async def test_following_twice_delivers_each_event_once(server: Server) -> None:
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await helper.call("thread.follow")
    await say(ana, "t1", "once")
    assert texts(await helper.quiet()) == ["once"]


async def test_a_follower_removed_and_added_again_receives_the_thread_from_the_new_add(
    server: Server,
) -> None:
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    await say(ana, "t1", "while out")
    await ana.call("member.add", thread_id="t1", identity=who(HELPER))
    await say(ana, "t1", "back")
    assert texts(await helper.quiet()) == ["back"]
    assert [j["thread"]["id"] for j in helper.joined] == ["t1"]


async def test_a_followed_thread_that_is_deleted_ends_with_one_notice(server: Server) -> None:
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await ana.call("thread.delete", thread_id="t1")
    assert [p["kind"] for p in await helper.quiet()] == ["thread.deleted"]


async def test_follow_refuses_a_negative_since(server: Server) -> None:
    helper = await Peer.open(server, HELPER)
    with pytest.raises(InputError, match="^server: since cannot be negative$"):
        await helper.call("thread.follow", since={"t1": -1})


async def test_a_follower_is_not_handed_a_thread_authorize_refuses_when_added_later(
    storage: InMemoryStorage,
) -> None:
    async def authorize(actor: Identity, action: str, thread: Thread) -> bool:
        return not thread.metadata.get("secret")

    server = Server(storage, authorize=authorize)
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await server.create_thread("acme", "closed", {"secret": True})
    await server.add_member("acme", "closed", HELPER)
    assert await helper.quiet() == []
    assert helper.joined == []


async def test_following_then_joining_the_same_thread_arrives_and_leaves_once(
    server: Server,
) -> None:
    ana = await threads(server, "t1")
    await ana.call("thread.join", thread_id="t1")
    await ana.quiet()
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await say(ana, "t1", "before")
    await helper.call("thread.join", thread_id="t1")
    await say(ana, "t1", "after")
    assert texts(await helper.quiet()) == ["before", "after"]
    assert [p["kind"] for p in await ana.quiet() if p["kind"] in ("present", "left")] == ["present"]
    await helper.call("thread.leave", thread_id="t1")
    assert [p["kind"] for p in await ana.quiet() if p["kind"] in ("present", "left")] == ["left"]


async def test_a_following_connection_receives_a_thread_its_identity_creates_elsewhere(
    server: Server,
) -> None:
    follower = await Peer.open(server, ANA)
    await follower.call("thread.follow")
    other = await Peer.open(server, ANA)
    await other.call("thread.create", thread_id="mine")
    pushes = await follower.quiet()
    assert [j["thread"]["id"] for j in follower.joined] == ["mine"]
    assert [p["kind"] for p in pushes] == ["member_added"]


async def test_an_add_survives_a_follower_whose_membership_is_gone_and_is_still_published(
    storage: InMemoryStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    try:
        following = await Peer.open(near, HELPER)
        await following.call("thread.follow")
        plain = await Peer.open(far, HELPER)
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        real = storage.is_member

        async def is_member(scope: str, thread_id: str, identity_id: str) -> bool:
            return False if identity_id == "helper" else await real(scope, thread_id, identity_id)

        monkeypatch.setattr(storage, "is_member", is_member)
        await ana.call("member.add", thread_id="t1", identity=who(HELPER))
        assert [p["kind"] for p in await plain.quiet(0.3)] == ["invited"]
        assert following.joined == []
    finally:
        await near.stop()
        await far.stop()


async def test_a_session_closed_during_an_add_is_not_left_in_the_room(
    storage: InMemoryStorage,
) -> None:
    holder: dict[str, Peer] = {}

    async def authorize(actor: Identity, action: str, thread: Thread) -> bool:
        if actor.id == "helper" and thread.id == "t1" and "helper" in holder:
            await holder.pop("helper").close()
            await asyncio.sleep(0.05)
        return True

    server = Server(storage, authorize=authorize)
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    session = next(s for s in server._hub.sessions if s.identity.id == "helper")
    holder["helper"] = helper
    await server.create_thread("acme", "t1")
    await server.add_member("acme", "t1", HELPER)
    assert session not in server._hub._rooms.get(("acme", "t1"), set())


async def test_a_session_dropped_inside_the_storage_reads_of_an_enter_is_not_left_in_the_room(
    storage: InMemoryStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = Server(storage)
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    session = next(s for s in server._hub.sessions if s.identity.id == "helper")
    await server.create_thread("acme", "t1")
    real = storage.is_member

    async def is_member(scope: str, thread_id: str, identity_id: str) -> bool:
        if identity_id == "helper":
            await server._hub.drop(session)
        return await real(scope, thread_id, identity_id)

    monkeypatch.setattr(storage, "is_member", is_member)
    await server.add_member("acme", "t1", HELPER)
    assert session not in server._hub._rooms.get(("acme", "t1"), set())
    assert "t1" not in session.joined


async def test_a_member_is_marked_just_before_it_was_added(server: Server) -> None:
    ana = await threads(server, "t1")
    await say(ana, "t1", "hello")
    helper = await Peer.open(server, HELPER)
    [listed] = (await helper.call("thread.list"))["threads"]
    assert (listed["marked"], listed["unread"]) == (1, 1)


async def test_resume_replays_each_gap_after_the_mark(server: Server) -> None:
    ana = await threads(server, "t1", "t2")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow", resume=True)
    seen = await say(ana, "t1", "seen")
    await helper.quiet()
    assert await helper.call("thread.mark", thread_id="t1", seq=seen["seq"]) == {
        "seq": seen["seq"],
        "unread": 0,
    }
    await helper.close()
    await say(ana, "t1", "missed one")
    await say(ana, "t2", "missed two")
    again = await Peer.open(server, HELPER)
    await again.call("thread.follow", resume=True)
    assert sorted(texts(await again.quiet())) == ["missed one", "missed two"]


async def test_a_membership_with_no_mark_resumes_from_now_and_is_given_one(
    storage: InMemoryStorage, server: Server
) -> None:
    await server.create_thread("acme", "t1")
    await server.add_member("acme", "t1", ANA)
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=HELPER, added_at=AT, added_by=SYSTEM)
    )
    ana = await Peer.open(server, ANA)
    await say(ana, "t1", "old")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow", resume=True)
    assert texts(await helper.quiet()) == []
    [listed] = (await helper.call("thread.list"))["threads"]
    assert listed["marked"] == 2


async def test_a_since_map_naming_a_foreign_thread_is_ignored(server: Server) -> None:
    await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    reply = await helper.call("thread.follow", resume=True, since={"elsewhere": 0})
    assert [j["thread"]["id"] for j in reply["joined"]] == ["t1"]


async def test_a_resume_gap_beyond_the_replay_limit_is_the_latest_and_truncated(
    storage: InMemoryStorage,
) -> None:
    server = Server(storage, replay_limit=2)
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow", resume=True)
    await helper.close()
    for n in range(4):
        await say(ana, "t1", f"m{n}")
    again = await Peer.open(server, HELPER)
    [joined] = (await again.call("thread.follow", resume=True))["joined"]
    assert joined["truncated"] is True
    assert texts(await again.quiet()) == ["m2", "m3"]


async def test_a_join_with_resume_replays_what_the_thread_said_after_the_mark(
    server: Server,
) -> None:
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.join", thread_id="t1", resume=True)
    seen = await say(ana, "t1", "seen")
    await helper.quiet()
    await helper.call("thread.mark", thread_id="t1", seq=seen["seq"])
    await helper.close()
    await say(ana, "t1", "while away")
    again = await Peer.open(server, HELPER)
    joined = await again.call("thread.join", thread_id="t1", resume=True)
    assert (joined["seq"], joined["truncated"]) == (4, False)
    assert joined["thread"]["marked"] == seen["seq"]
    assert texts(await again.quiet()) == ["while away"]
    await say(ana, "t1", "live")
    assert texts(await again.quiet()) == ["live"]


async def test_a_join_with_resume_and_no_mark_set_starts_from_the_add(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    await say(ana, "t1", "before the add")
    await ana.call("member.add", thread_id="t1", identity=who(HELPER))
    await say(ana, "t1", "after the add")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.join", thread_id="t1", resume=True)
    pushes = await helper.quiet()
    assert [p["kind"] for p in pushes if "seq" in p] == ["member_added", "message"]
    assert texts(pushes) == ["after the add"]


async def test_a_join_with_resume_into_a_membership_with_no_mark_starts_now_and_is_given_one(
    storage: InMemoryStorage, server: Server
) -> None:
    await server.create_thread("acme", "t1")
    await server.add_member("acme", "t1", ANA)
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=HELPER, added_at=AT, added_by=SYSTEM)
    )
    ana = await Peer.open(server, ANA)
    await say(ana, "t1", "old")
    helper = await Peer.open(server, HELPER)
    joined = await helper.call("thread.join", thread_id="t1", resume=True)
    assert (joined["seq"], joined["truncated"], joined["thread"]["marked"]) == (2, False, 2)
    assert texts(await helper.quiet()) == []
    assert await storage.get_mark("acme", "t1", "helper") == 2
    await helper.close()
    await say(ana, "t1", "while away")
    again = await Peer.open(server, HELPER)
    await again.call("thread.join", thread_id="t1", resume=True)
    assert texts(await again.quiet()) == ["while away"]


async def test_a_join_with_resume_gives_a_mark_to_its_own_thread_alone(
    storage: InMemoryStorage, server: Server
) -> None:
    for thread_id in ("t1", "t2"):
        await server.create_thread("acme", thread_id)
        await storage.add_member(
            "acme", Member(thread_id=thread_id, identity=HELPER, added_at=AT, added_by=SYSTEM)
        )
    helper = await Peer.open(server, HELPER)
    joined = await helper.call("thread.join", thread_id="t1", resume=True)
    assert joined["thread"]["marked"] == await storage.get_mark("acme", "t1", "helper")
    assert await storage.get_mark("acme", "t2", "helper") is None
    listed = {t["id"]: t for t in (await helper.call("thread.list"))["threads"]}
    assert (listed["t2"].get("marked"), listed["t2"].get("unread")) == (None, None)


async def test_a_join_takes_since_or_resume_not_both(server: Server) -> None:
    await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    with pytest.raises(InputError, match="^server: a join takes since or resume, not both$"):
        await helper.call("thread.join", thread_id="t1", since=0, resume=True)
    bruno = await Peer.open(server, BRUNO)
    with pytest.raises(ForbiddenError):
        await bruno.call("thread.join", thread_id="t1", resume=True)


async def test_a_join_with_resume_beyond_the_replay_limit_is_the_latest_and_truncated(
    storage: InMemoryStorage,
) -> None:
    server = Server(storage, replay_limit=2)
    ana = await threads(server, "t1")
    for n in range(4):
        await say(ana, "t1", f"m{n}")
    helper = await Peer.open(server, HELPER)
    joined = await helper.call("thread.join", thread_id="t1", resume=True)
    assert (joined["seq"], joined["truncated"], joined["thread"]["marked"]) == (6, True, 1)
    assert texts(await helper.quiet()) == ["m2", "m3"]
    rest = await helper.call(
        "thread.history", thread_id="t1", after=joined["thread"]["marked"], before=5
    )
    assert texts(rest["events"]) == ["m0", "m1"]


async def test_joining_a_followed_thread_with_resume_replays_from_the_mark_once(
    server: Server,
) -> None:
    ana = await threads(server, "t1")
    for n in range(3):
        await say(ana, "t1", f"m{n}")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await helper.call("thread.mark", thread_id="t1", seq=3)
    joined = await helper.call("thread.join", thread_id="t1", resume=True)
    assert (joined["seq"], joined["truncated"], joined["thread"]["marked"]) == (5, False, 3)
    assert texts(await helper.quiet()) == ["m1", "m2"]
    await say(ana, "t1", "live")
    assert texts(await helper.quiet()) == ["live"]


async def test_a_mark_never_moves_back_and_must_name_a_seq_in_the_thread(server: Server) -> None:
    ana = await threads(server, "t1")
    await say(ana, "t1", "a")
    await say(ana, "t1", "b")
    helper = await Peer.open(server, HELPER)
    assert (await helper.call("thread.mark", thread_id="t1", seq=4))["seq"] == 4
    assert (await helper.call("thread.mark", thread_id="t1", seq=3))["seq"] == 4
    for seq in (-1, 5):
        with pytest.raises(InputError, match="^server: seq must be between 0 and 4$"):
            await helper.call("thread.mark", thread_id="t1", seq=seq)
    bruno = await Peer.open(server, BRUNO)
    with pytest.raises(ForbiddenError):
        await bruno.call("thread.mark", thread_id="t1", seq=0)


def marks(pushes: list[dict[str, Any]]) -> list[tuple[str, int, int]]:
    return [(p["thread_id"], p["seq"], p["unread"]) for p in pushes if p["kind"] == "marked"]


async def test_a_mark_that_moves_reaches_only_the_markers_other_connections_on_every_replica(
    storage: InMemoryStorage,
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    try:
        ana = await threads(near, "t1")
        first = await say(ana, "t1", "hi")
        phone = await Peer.open(near, HELPER)
        tablet = await Peer.open(near, HELPER)
        laptop = await Peer.open(far, HELPER)
        stranger = await Peer.open(far, ANA)
        await ana.quiet()
        reply = await phone.call("thread.mark", thread_id="t1", seq=first["seq"])
        assert reply == {"seq": first["seq"], "unread": 0}
        told = await laptop.push("marked")
        assert (told["thread_id"], told["seq"], told["unread"]) == ("t1", first["seq"], 0)
        assert marks(await laptop.quiet()) == []
        assert marks(await tablet.quiet()) == [("t1", first["seq"], 0)]
        assert await phone.quiet() == [] and await stranger.quiet() == []
        for seq in (first["seq"], first["seq"] - 1):
            stays = await phone.call("thread.mark", thread_id="t1", seq=seq)
            assert stays["seq"] == first["seq"]
        second = await say(ana, "t1", "again")
        await tablet.call("thread.mark", thread_id="t1", seq=second["seq"])
        assert (await laptop.push("marked"))["seq"] == second["seq"]
        heard = [marks(await peer.quiet()) for peer in (phone, tablet, laptop, stranger, ana)]
        assert heard == [[("t1", second["seq"], 0)], [], [], [], []]
    finally:
        await near.stop()
        await far.stop()


async def test_joining_a_followed_thread_since_a_seq_replays_up_to_where_it_is_once(
    server: Server,
) -> None:
    ana = await threads(server, "t1")
    for n in range(3):
        await say(ana, "t1", f"m{n}")
    helper = await Peer.open(server, HELPER)
    [followed] = (await helper.call("thread.follow"))["joined"]
    assert followed["seq"] == 5
    joined = await helper.call("thread.join", thread_id="t1", since=0)
    assert (joined["seq"], joined["truncated"]) == (5, False)
    pushes = await helper.quiet()
    assert [p["seq"] for p in pushes if "seq" in p] == [1, 2, 3, 4, 5]
    assert texts(pushes) == ["m0", "m1", "m2"]
    await say(ana, "t1", "live")
    assert texts(await helper.quiet()) == ["live"]
    again = await helper.call("thread.join", thread_id="t1", since=6)
    assert (again["seq"], again["truncated"]) == (6, False)
    assert await helper.quiet() == []


async def test_joining_a_followed_thread_from_beyond_the_replay_limit_is_the_latest_and_truncated(
    storage: InMemoryStorage,
) -> None:
    server = Server(storage, replay_limit=2)
    ana = await threads(server, "t1")
    for n in range(3):
        await say(ana, "t1", f"m{n}")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    joined = await helper.call("thread.join", thread_id="t1", since=0)
    assert (joined["seq"], joined["truncated"]) == (5, True)
    assert texts(await helper.quiet()) == ["m1", "m2"]
    await say(ana, "t1", "live")
    assert texts(await helper.quiet()) == ["live"]


async def test_history_reads_forward_after_a_seq_and_within_a_range(server: Server) -> None:
    ana = await threads(server, "t1")
    for n in range(4):
        await say(ana, "t1", f"m{n}")
    forward = (await ana.call("thread.history", thread_id="t1", after=3, limit=2))["events"]
    assert [e["seq"] for e in forward] == [4, 5]
    window = (await ana.call("thread.history", thread_id="t1", after=2, before=5))["events"]
    assert [e["seq"] for e in window] == [3, 4]
    with pytest.raises(InputError, match="^server: after cannot be negative$"):
        await ana.call("thread.history", thread_id="t1", after=-1)


async def test_the_inbox_lists_by_activity_with_previews(server: Server) -> None:
    ana = await threads(server, "t1", "t2")
    await say(ana, "t1", "newest")
    listed = (await ana.call("thread.list", order="activity", preview=True))["threads"]
    assert [t["id"] for t in listed] == ["t1", "t2"]
    assert listed[0]["last_message"]["content"][0]["text"] == "newest"
    assert "last_message" not in listed[1]
    with pytest.raises(InputError):
        await ana.call("thread.list", order="loudest")
    system = await server.list_threads("acme", order="activity", preview=True)
    assert [t.id for t in system] == ["t1", "t2"]
    assert system[0].marked is None and system[0].last_message is not None
