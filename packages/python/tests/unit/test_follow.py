from typing import Any

import pytest

from rfnry_thread.errors import InputError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import Identity, Thread
from rfnry_thread.server import Server

from .conftest import say
from .factories import ANA, HELPER
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
