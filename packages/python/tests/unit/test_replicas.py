import asyncio
import inspect
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from until import until

from rfnry_thread import server as server_module
from rfnry_thread.errors import ForbiddenError, NotFoundError, StorageError
from rfnry_thread.models import Identity, Thread
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server
from rfnry_thread.state import EXPIRY
from rfnry_thread.storage import OnEvent, OnResume, OnSignal, Stop

from .conftest import say
from .factories import ANA, BRUNO, HELPER, added
from .notices import Muffled, Notice
from .peer import Peer
from .writes import admit


@pytest.fixture
async def replicas(storage: PostgresStorage) -> AsyncIterator[tuple[Server, Server]]:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    yield near, far
    await near.stop()
    await far.stop()


async def room(near: Server, far: Server) -> tuple[Peer, Peer]:
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await ana.call("thread.join", thread_id="t1")
    helper = await Peer.open(far, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await ana.quiet()
    await helper.quiet()
    return ana, helper


async def test_an_event_on_one_replica_reaches_a_member_on_another_once(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    for n in range(3):
        await say(ana, "t1", f"m{n}")
    messages = [p for p in await helper.quiet() if p["kind"] == "message"]
    assert [p["content"][0]["text"] for p in messages] == ["m0", "m1", "m2"]
    assert [p["kind"] for p in await ana.quiet()].count("message") == 3


async def test_signals_are_mirrored_and_never_echoed_twice(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working"))["label"] == "Reading"
    assert [p["kind"] for p in await helper.quiet()] == ["working"]
    assert [p["kind"] for p in await ana.quiet()] == []


async def test_the_finishing_message_reaches_a_remote_connection_before_stream_end(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await helper.notify(
        "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="Done."
    )
    await say(helper, "t1", "Done.", client_id="s1")
    await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
    order = [p["kind"] for p in await ana.quiet() if p["kind"] in ("message", "stream.end")]
    assert order == ["message", "stream.end"]


async def test_an_invitation_and_a_deletion_cross_replicas(replicas: tuple[Server, Server]) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    bruno = await Peer.open(far, BRUNO)
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    assert (await bruno.push("invited"))["thread"]["id"] == "t1"
    await ana.call("thread.delete", thread_id="t1")
    assert (await helper.push("thread.deleted"))["sender"]["id"] == "ana"


async def test_a_replica_learns_who_is_here_from_the_re_announcement(
    storage: PostgresStorage,
) -> None:
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    far = Server(storage)
    await near.start()
    await far.start()
    await near._hub.announce()
    await until(lambda: far._hub.state.present("acme", "t1") == [ANA])
    now = far._hub.state._clock()
    far._hub.state._clock = lambda: now + EXPIRY + 1
    assert far._hub.state.present("acme", "t1") == []
    await near.stop()
    await far.stop()


def resumes(storage: PostgresStorage) -> list[OnResume | None]:
    captured: list[OnResume | None] = []
    listen = storage.listen

    async def spy(
        on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop:
        captured.append(on_resume)
        return await listen(on_event, on_signal, on_resume)

    storage.listen = spy  # type: ignore[method-assign]
    return captured


async def test_a_resumed_listener_republishes_this_replicas_state(storage: PostgresStorage) -> None:
    resumed = resumes(storage)
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    far = Server(storage)
    await near.start()
    await far.start()
    assert far._hub.state.present("acme", "t1") == []
    resume = resumed[0]
    assert resume is not None
    await resume()
    await until(lambda: far._hub.state.present("acme", "t1") == [ANA])
    await near.stop()
    await far.stop()


async def test_a_resumed_listener_takes_its_connections_out_of_a_thread_deleted_and_made_again_unheard(
    pool: Any,
) -> None:
    deaf = [False]
    storage = Muffled(pool, lambda notice: deaf[0])
    resumed = resumes(storage)
    near, far = Server(storage), Server(storage)
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        for n in range(3):
            await say(ana, "t1", f"old {n}")
        await ana.quiet()
        deaf[0] = True
        await storage.delete_scope("acme")
        await far.create_thread("acme", "t1")
        await far.add_member("acme", "t1", BRUNO)
        bruno = await Peer.open(far, BRUNO)
        await bruno.call("thread.join", thread_id="t1")
        assert await ana.quiet() == []
        deaf[0] = False
        [resume] = resumed
        assert resume is not None
        await resume()
        assert (await ana.push("thread.deleted"))["sender"]["id"] == "system"
        for n in range(6):
            await say(bruno, "t1", f"new {n}")
        assert await ana.quiet(0.3) == []
    finally:
        await near.stop()


async def test_what_a_resumed_listener_hears_before_a_room_is_caught_up_never_reaches_its_old_connections(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    deaf = [False]
    storage = Muffled(pool, lambda notice: deaf[0])
    resumed = resumes(storage)
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        for thread_id in ("a0", "a1", "a2", "t1"):
            await ana.call("thread.create", thread_id=thread_id)
            await ana.call("thread.join", thread_id=thread_id)
        for n in range(3):
            await say(ana, "t1", f"old {n}")
        await ana.quiet()
        deaf[0] = True
        await storage.delete_thread("acme", "t1")
        await far.create_thread("acme", "t1")
        await far.add_member("acme", "t1", BRUNO)
        bruno = await Peer.open(far, BRUNO)
        await bruno.call("thread.join", thread_id="t1")
        deaf[0] = False
        heads, release = storage.heads, asyncio.Event()

        async def slow(keys: Any) -> Any:
            await release.wait()
            return await heads(keys)

        monkeypatch.setattr(storage, "heads", slow)
        resume = resumed[0]
        assert resume is not None
        walking = asyncio.ensure_future(resume())
        await far.start()
        await bruno.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
        await bruno.call(
            "signal.send",
            thread_id="t1",
            kind="stream.delta",
            stream_id="s1",
            index=0,
            text="draft",
        )
        for n in range(6):
            await say(bruno, "t1", f"new {n}")
        heard = await ana.quiet(0.3)
        assert not walking.done()
        assert [p["kind"] for p in heard] == ["thread.deleted"]
        release.set()
        await asyncio.wait_for(walking, 5)
    finally:
        await near.stop()
        await far.stop()


async def test_a_write_to_a_thread_made_again_unheard_takes_the_old_ones_connections_out_first(
    storage: PostgresStorage,
) -> None:
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    for n in range(3):
        await say(ana, "t1", f"old {n}")
    await ana.quiet()
    await storage.delete_thread("acme", "t1")
    await near.create_thread("acme", "t1")
    for n in range(5):
        await near.update_thread("acme", "t1", {"n": n})
    assert [p["kind"] for p in await ana.quiet()] == ["thread.deleted"]


async def test_a_join_to_a_thread_made_again_unheard_takes_the_old_ones_connections_out_first(
    storage: PostgresStorage,
) -> None:
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    await ana.quiet()
    await storage.delete_thread("acme", "t1")
    await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=datetime.now(UTC)))
    await admit(storage, "acme", added("t1", BRUNO))
    bruno = await Peer.open(near, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    for n in range(3):
        await say(bruno, "t1", f"new {n}")
    assert [p["kind"] for p in await ana.quiet()] == ["thread.deleted"]
    told = [p["content"][0]["text"] for p in await bruno.quiet() if p["kind"] == "message"]
    assert told == [f"new {n}" for n in range(3)]


async def test_a_follower_in_a_thread_made_again_unheard_is_handed_the_new_one_when_invited(
    replicas: tuple[Server, Server], storage: PostgresStorage
) -> None:
    near, far = replicas
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.follow")
    await ana.quiet()
    await storage.delete_thread("acme", "t1")
    await far.create_thread("acme", "t1")
    await far.add_member("acme", "t1", ANA)
    assert (await ana.push("thread.deleted"))["sender"]["id"] == "system"
    await ana.quiet()
    assert [j["thread"]["id"] for j in ana.joined] == ["t1"]


async def test_a_resumed_listener_cuts_a_member_removed_unheard_and_catches_the_others_up(
    storage: PostgresStorage,
) -> None:
    near, far = Server(storage), Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await ana.call("thread.join", thread_id="t1")
    helper = await Peer.open(near, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await ana.quiet()
    await helper.quiet()
    elsewhere = await Peer.open(far, ANA)
    await say(elsewhere, "t1", "unheard")
    await far.remove_member("acme", "t1", HELPER)
    assert await helper.quiet() == []
    await near._hub.on_resume()
    assert [p["kind"] for p in await helper.quiet()] == ["message", "member_removed"]
    assert [p["kind"] for p in await ana.quiet()] == ["message", "member_removed", "left"]
    await say(ana, "t1", "after")
    assert await helper.quiet(0.3) == []
    assert [p["content"][0]["text"] for p in await ana.quiet()] == ["after"]


async def test_a_connection_taken_out_of_a_thread_ends_its_presence_when_what_follows_fails(
    pool: Any, caplog: pytest.LogCaptureFixture
) -> None:
    storage = Muffled(pool)
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1", history="since_added")
        await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        bruno = await Peer.open(near, BRUNO)
        await bruno.call("thread.join", thread_id="t1")
        await ana.quiet()
        await bruno.quiet()
        storage.refuse = lambda thread_id, since, floor: floor == 2
        with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
            await far.remove_member("acme", "t1", ANA)
            assert (await ana.push("member_removed"))["identity"]["id"] == "ana"
            assert [(p["kind"], p["sender"]["id"]) for p in await bruno.quiet()] == [
                ("left", "ana")
            ]
        assert "hub: catching thread 't1' up from storage failed" in caplog.text
    finally:
        await near.stop()


async def test_a_room_caught_up_again_while_a_catch_up_reads_hears_none_of_the_thread_made_again(
    storage: PostgresStorage, pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(PostgresStorage(pool))
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    await say(ana, "t1", "old")
    await ana.quiet()
    elsewhere = await Peer.open(far, ANA)
    await say(elsewhere, "t1", "unheard")
    real = storage.tail
    reading, held = asyncio.Event(), asyncio.Event()

    async def slow(scope: str, thread_id: str, since: int, floor: int, limit: int) -> Any:
        found = await real(scope, thread_id, since, floor, limit)
        if not reading.is_set():
            reading.set()
            await asyncio.wait_for(held.wait(), 5.0)
        return found

    monkeypatch.setattr(storage, "tail", slow)
    first = asyncio.ensure_future(near._hub.on_resume())
    await asyncio.wait_for(reading.wait(), 5.0)
    await storage.delete_thread("acme", "t1")
    made_again = await made(far, "t1", BRUNO)
    second = asyncio.ensure_future(near._hub.on_resume())
    held.set()
    await asyncio.wait_for(asyncio.gather(first, second), 5.0)
    told = [p["kind"] for p in await ana.quiet()]
    assert told == ["message", "thread.deleted"]
    bruno = await Peer.open(far, BRUNO)
    for n in range(3):
        sent = await say(bruno, "t1", f"new {n}")
        await near._hub.on_event("acme", "t1", sent["seq"], made_again.epoch)
    assert await ana.quiet() == []


async def test_a_room_whose_catch_up_failed_is_caught_up_on_the_next_tick(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.1)
    storage = Muffled(pool)
    near = Server(storage)
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        await ana.quiet()
        storage.refuse = lambda thread_id, since, floor: True
        await storage.delete_thread("acme", "t1")
        await asyncio.sleep(0.4)
        assert await ana.quiet() == []
        storage.refuse = lambda thread_id, since, floor: False
        assert (await ana.push("thread.deleted", timeout=2.0))["sender"]["id"] == "system"
    finally:
        await near.stop()


async def test_an_unexpected_error_catching_one_room_up_is_logged_and_the_rest_are_caught_up(
    storage: PostgresStorage,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    near = Server(storage)
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        for thread_id in ("t1", "t2"):
            await ana.call("thread.create", thread_id=thread_id)
            await ana.call("thread.join", thread_id=thread_id)
        await ana.quiet()
        await storage.delete_scope("acme")
        publish = storage.publish

        async def broken(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
            if thread_id == "t1":
                raise RuntimeError("storage: publish broke")
            await publish(scope, thread_id, payload)

        monkeypatch.setattr(storage, "publish", broken)
        with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
            await near._hub.on_resume()
        deleted = [(p["kind"], p["thread_id"]) for p in await ana.quiet()]
        assert deleted == [("thread.deleted", "t1"), ("thread.deleted", "t2")]
        assert "hub: catching thread 't1' up from storage failed: storage: publish broke" in (
            caplog.text
        )
    finally:
        await near.stop()


async def test_a_catch_up_never_delivers_what_a_thread_made_again_between_its_reads_wrote(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = Muffled(pool, lambda notice: notice.seq is not None)
    near, far = Server(storage), Server(PostgresStorage(pool))
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    for n in range(3):
        await say(ana, "t1", f"old {n}")
    await ana.quiet()
    elsewhere = await Peer.open(far, ANA)
    await say(elsewhere, "t1", "unheard")
    heads = storage.heads

    async def remade(keys: Any) -> Any:
        found = await heads(keys)
        monkeypatch.setattr(storage, "heads", heads)
        await far.delete_thread("acme", "t1")
        await made(far, "t1", BRUNO)
        bruno = await Peer.open(far, BRUNO)
        for n in range(5):
            await say(bruno, "t1", f"new {n}")
        return found

    monkeypatch.setattr(storage, "heads", remade)
    try:
        await near.start()
        assert [p["kind"] for p in await ana.quiet()] == ["thread.deleted"]
    finally:
        await near.stop()


async def test_stop_stops_listening_and_start_is_idempotent(storage: PostgresStorage) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await near.start()
    await far.start()
    ana, helper = await room(near, far)
    await far.stop()
    await far.stop()
    await say(ana, "t1", "unheard")
    assert [p for p in await helper.quiet() if p["kind"] == "message"] == []
    await near.stop()


async def test_a_thread_another_replica_wrote_between_a_listing_and_its_enter_is_followed_whole(
    replicas: tuple[Server, Server],
    storage: PostgresStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    near, far = replicas
    ana = await Peer.open(far, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await asyncio.sleep(0.3)
    said: list[str] = []
    listing = storage.list_threads

    async def listed(*args: Any, **kwargs: Any) -> Any:
        found = await listing(*args, **kwargs)
        if args[1] == "helper" and not said:
            said.append((await say(ana, "t1", "from far"))["id"])
            await asyncio.sleep(0.3)
        return found

    monkeypatch.setattr(storage, "list_threads", listed)
    helper = await Peer.open(near, HELPER)
    reply = await helper.call("thread.follow", since={"t1": {"seq": 0}})
    assert [j["seq"] for j in reply["joined"]] == [3]
    pushed = [p["content"][0]["text"] for p in await helper.quiet() if p["kind"] == "message"]
    assert pushed == ["from far"]


async def test_a_removal_after_a_follow_listing_cuts_the_follower_when_its_notice_is_late(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(storage)
    heard = near._hub.on_event

    async def late(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        await asyncio.sleep(0.3)
        await heard(scope, thread_id, seq, epoch)

    monkeypatch.setattr(near._hub, "on_event", late)
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(far, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await say(ana, "t1", "one")
        await asyncio.sleep(0.6)
        listing = storage.list_threads
        removed: list[bool] = []

        async def listed(*args: Any, **kwargs: Any) -> Any:
            found = await listing(*args, **kwargs)
            if args[1] == "helper" and not removed:
                removed.append(True)
                await ana.call("member.remove", thread_id="t1", identity_id="helper")
            return found

        monkeypatch.setattr(storage, "list_threads", listed)
        helper = await Peer.open(near, HELPER)
        await helper.call("thread.follow", since={"t1": {"seq": 0}})
        kinds = [p["kind"] for p in await helper.quiet(0.6) if "seq" in p]
        assert kinds[-1] == "member_removed"
        await say(ana, "t1", "said after helper was removed")
        assert await helper.quiet(0.8) == []
    finally:
        await near.stop()
        await far.stop()


async def test_a_notice_landing_while_a_follower_enters_is_delivered_at_once(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(far, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await say(ana, "t1", "one")
        await asyncio.sleep(0.3)
        real = storage.tail
        done: list[bool] = []

        async def replaying(scope: str, thread_id: str, since: int, floor: int, limit: int) -> Any:
            found = await real(scope, thread_id, since, floor, limit)
            if since == 0 and not done:
                done.append(True)
                await say(ana, "t1", "late")
                await asyncio.sleep(0.4)
            return found

        monkeypatch.setattr(storage, "tail", replaying)
        helper = await Peer.open(near, HELPER)
        await helper.call("thread.follow", since={"t1": {"seq": 0}})
        pushed = [
            p["content"][0]["text"] for p in await helper.quiet(0.6) if p["kind"] == "message"
        ]
        assert pushed == ["one", "late"]
    finally:
        await near.stop()
        await far.stop()


async def test_a_replica_started_again_with_connections_open_checks_their_threads(
    storage: PostgresStorage,
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(near, ANA)
        for thread_id in ("t1", "t2"):
            await ana.call("thread.create", thread_id=thread_id)
            await ana.call("thread.join", thread_id=thread_id)
        await ana.call("member.add", thread_id="t2", identity=BRUNO.model_dump(mode="json"))
        bruno = await Peer.open(far, BRUNO)
        await bruno.call("thread.join", thread_id="t2")
        await ana.quiet()
        await bruno.quiet()
        await near.stop()
        await ana.quiet()
        await far.delete_thread("acme", "t1")
        await far.remove_member("acme", "t2", ANA)
        await say(bruno, "t2", "ana must not hear this")
        assert [p["kind"] for p in await ana.quiet()] == []
        await near.start()
        assert [p["kind"] for p in await ana.quiet()] == ["thread.deleted", "member_removed"]
        for thread_id in ("t1", "t2"):
            with pytest.raises(ForbiddenError, match="^server: signals need a join"):
                await ana.call("signal.send", thread_id=thread_id, kind="idle")
        await bruno.call("signal.send", thread_id="t2", kind="working", label="private")
        assert await ana.quiet(0.3) == []
    finally:
        await near.stop()
        await far.stop()


class Outage(PostgresStorage):
    down = False

    def __getattribute__(self, name: str) -> Any:
        found = super().__getattribute__(name)
        if (
            name == "listen"
            or name.startswith("_")
            or not inspect.iscoroutinefunction(found)
            or not super().__getattribute__("down")
        ):
            return found

        async def refused(*args: Any, **kwargs: Any) -> Any:
            raise StorageError(f"postgres: storage rejected {name}: connection reset")

        return refused


async def test_a_replica_started_while_storage_fails_starts_and_catches_its_rooms_up_once_it_answers(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    storage = Outage(pool)
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    for thread_id in ("t1", "t2"):
        await ana.call("thread.create", thread_id=thread_id)
        await ana.call("thread.join", thread_id=thread_id)
    await ana.call("member.add", thread_id="t2", identity=BRUNO.model_dump(mode="json"))
    await ana.quiet()
    bruno = await Peer.open(near, BRUNO)
    await storage.delete_thread("acme", "t1")
    storage.down = True
    try:
        await asyncio.wait_for(near.start(), 2.0)
        assert await ana.quiet(0.6) == []
        storage.down = False
        assert (await ana.push("thread.deleted", timeout=2.0))["thread_id"] == "t1"
        await bruno.call("thread.join", thread_id="t2")
        await say(bruno, "t2", "after storage is back")
        told = [
            (p["kind"], p["thread_id"])
            for p in await ana.quiet()
            if p["kind"] in ("thread.deleted", "message")
        ]
        assert told == [("message", "t2")]
    finally:
        await ana.close()
        await bruno.close()
        await near.stop()


async def made(server: Server, thread_id: str, *members: Identity, updates: int = 0) -> Thread:
    await server.create_thread("acme", thread_id)
    for member in members:
        await server.add_member("acme", thread_id, member)
    for n in range(updates):
        await server.update_thread("acme", thread_id, {"n": n})
    return await server.get_thread("acme", thread_id)


async def test_a_hint_for_an_older_epoch_is_ignored_and_a_newer_one_empties_the_room(
    pool: Any,
) -> None:
    holding = [True]
    storage = Muffled(pool, lambda notice: holding[0])
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    try:
        oldest = await made(far, "t1", ANA, updates=2)
        await far.delete_thread("acme", "t1")
        current = await made(far, "t1", ANA)
        ana = await Peer.open(near, ANA)
        await ana.call("thread.join", thread_id="t1")
        await ana.quiet()
        for n in range(2):
            await far.update_thread("acme", "t1", {"unheard": n})
        await until(lambda: [n.epoch for n in storage.dropped].count(current.epoch) == 3)
        stale = [n for n in storage.dropped if n.epoch == oldest.epoch]
        assert [n.seq for n in stale] == [1, 2, 3]
        for notice in stale:
            await notice.hand()
        assert await ana.quiet() == []
        await far.delete_thread("acme", "t1")
        holding[0] = False
        newer = await made(far, "t1", BRUNO, updates=3)
        assert oldest.epoch < current.epoch < newer.epoch
        assert [p["kind"] for p in await ana.quiet(0.3)] == ["thread.deleted"]
    finally:
        await near.stop()


async def test_a_lost_deletion_notice_never_lets_a_new_threads_events_reach_the_old_room(
    pool: Any,
) -> None:
    storage = Muffled(pool, lambda notice: notice.payload.get("kind") == "thread.deleted")
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        for n in range(3):
            await say(ana, "t1", f"old {n}")
        await ana.quiet()
        await far.delete_thread("acme", "t1")
        await until(lambda: len(storage.dropped) == 1)
        made_again = Thread(id="t1", scope="acme", created_at=datetime.now(UTC))
        await storage.create_thread("acme", made_again, added("t1", BRUNO))
        bruno = await Peer.open(far, BRUNO)
        for n in range(6):
            await say(bruno, "t1", f"new {n}")
        assert [p["kind"] for p in await ana.quiet(0.3)] == ["thread.deleted"]
    finally:
        await near.stop()
        await far.stop()


async def test_a_deletion_notice_for_another_epoch_leaves_the_room(pool: Any) -> None:
    storage = Muffled(pool, lambda notice: notice.payload.get("kind") == "thread.deleted")
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        await made(far, "t1", ANA)
        await far.delete_thread("acme", "t1")
        await made(far, "t1", ANA)
        ana = await Peer.open(near, ANA)
        await ana.call("thread.join", thread_id="t1")
        await ana.quiet()
        await until(lambda: len(storage.dropped) == 1)
        for notice in storage.dropped:
            await notice.hand()
        await far.update_thread("acme", "t1", {"after": True})
        assert [p["kind"] for p in await ana.quiet(0.3)] == ["thread_updated"]
    finally:
        await near.stop()
        await far.stop()


async def test_a_thread_made_again_while_the_listener_was_down_empties_the_old_room_on_the_next_tick(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    down = [False]
    storage = Muffled(pool, lambda notice: down[0])
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        for n in range(3):
            await say(ana, "t1", f"old {n}")
        await ana.quiet()
        down[0] = True
        await far.delete_thread("acme", "t1")
        await made(far, "t1", BRUNO)
        bruno = await Peer.open(far, BRUNO)
        for n in range(5):
            await say(bruno, "t1", f"new {n}")
        assert (await ana.push("thread.deleted", timeout=2.0))["sender"]["id"] == "system"
        down[0] = False
        for n in range(3):
            await say(bruno, "t1", f"newer {n}")
        assert await ana.quiet(0.5) == []
    finally:
        await near.stop()
        await far.stop()


async def test_a_member_removed_unheard_stops_receiving_signals_within_a_tick(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    storage = Muffled(pool, lambda notice: notice.seq is not None)
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(far, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        helper = await Peer.open(near, HELPER)
        await helper.call("thread.join", thread_id="t1")
        await ana.quiet(0.3)
        await helper.quiet(0.3)
        await ana.call("member.remove", thread_id="t1", identity_id="helper")
        assert (await helper.push("member_removed", timeout=0.6))["identity"]["id"] == "helper"
        await ana.call("signal.send", thread_id="t1", kind="working", label="private")
        await ana.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
        await say(ana, "t1", "private")
        assert await helper.quiet(0.5) == []
    finally:
        await near.stop()
        await far.stop()


async def test_a_catch_up_that_fails_leaves_the_room_until_a_trigger_succeeds(
    pool: Any, caplog: pytest.LogCaptureFixture
) -> None:
    storage = Muffled(pool)
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(near, ANA)
        for thread_id in ("t1", "t2"):
            await ana.call("thread.create", thread_id=thread_id)
            await ana.call(
                "member.add", thread_id=thread_id, identity=HELPER.model_dump(mode="json")
            )
            await ana.call("thread.join", thread_id=thread_id)
        helper = await Peer.open(far, HELPER)
        for thread_id in ("t1", "t2"):
            await helper.call("thread.join", thread_id=thread_id)
        await ana.quiet(0.3)
        storage.refuse = lambda thread_id, since, floor: True
        with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
            await say(helper, "t1", "during")
            await storage.delete_thread("acme", "t2")
            made_again = Thread(id="t2", scope="acme", created_at=datetime.now(UTC))
            await storage.create_thread("acme", made_again, added("t2", HELPER))
            for n in range(3):
                await say(helper, "t2", f"new {n}")
            await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
            heard = await ana.quiet(0.3)
        assert sorted((p["kind"], p["thread_id"]) for p in heard) == [
            ("left", "t2"),
            ("working", "t1"),
        ]
        assert "hub: catching thread 't1' up from storage failed" in caplog.text
        assert "hub: catching thread 't2' up from storage failed" in caplog.text
        storage.refuse = lambda thread_id, since, floor: False
        await say(helper, "t1", "after")
        await say(helper, "t2", "newer")
        told = await ana.quiet(0.3)
        said = [p["content"][0]["text"] for p in told if p["kind"] == "message"]
        assert [p["thread_id"] for p in told if p["kind"] == "message"] == ["t1", "t1"]
        assert said == ["during", "after"]
        assert [p["kind"] for p in told if p["thread_id"] == "t2"] == ["thread.deleted"]
    finally:
        await near.stop()
        await far.stop()


async def test_a_join_reads_storage_itself_and_enters_while_the_room_lags(pool: Any) -> None:
    storage = Muffled(pool)
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        helper = await Peer.open(far, HELPER)
        await helper.call("thread.join", thread_id="t1")
        bruno = await Peer.open(near, BRUNO)
        await bruno.call("thread.follow")
        await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
        await helper.notify(
            "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="abc"
        )
        await ana.push("stream.delta")
        await ana.quiet(0.3)
        storage.refuse = lambda thread_id, since, floor: since == 2
        await say(helper, "t1", "unheard")
        await helper.notify(
            "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=1, text="def"
        )
        assert [p["kind"] for p in await ana.quiet(0.3)] == ["stream.delta"]
        await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
        await bruno.quiet(0.3)
        assert [(j["thread"]["id"], j["seq"]) for j in bruno.joined] == [("t1", 4)]
        here = await Peer.open(near, HELPER)
        reply = await here.call("thread.join", thread_id="t1", since=0)
        assert reply["seq"] == 4
        pushed = await here.quiet(0.3)
        assert [p["content"][0]["text"] for p in pushed if p["kind"] == "message"] == ["unheard"]
        assert [p["text"] for p in pushed if p["kind"] == "stream.delta"] == ["abcdef"]
        storage.refuse = lambda thread_id, since, floor: False
        await say(helper, "t1", "after")
        said = [p["content"][0]["text"] for p in await ana.quiet(0.3) if p["kind"] == "message"]
        assert said == ["unheard", "after"]
    finally:
        await near.stop()
        await far.stop()


class Counted(PostgresStorage):
    def __init__(self, pool: Any) -> None:
        super().__init__(pool)
        self.reads: list[str] = []

    def __getattribute__(self, name: str) -> Any:
        found = super().__getattribute__(name)
        if (
            name in ("listen", "publish")
            or name.startswith("_")
            or not inspect.iscoroutinefunction(found)
        ):
            return found
        reads = super().__getattribute__("reads")

        async def counted(*args: Any, **kwargs: Any) -> Any:
            reads.append(name)
            return await found(*args, **kwargs)

        return counted


async def test_a_tick_reads_every_room_in_one_query(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.3)
    storage = Counted(pool)
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    for n in range(5):
        await ana.call("thread.create", thread_id=f"t{n}")
        await ana.call("thread.join", thread_id=f"t{n}")
    await ana.quiet()
    storage.reads.clear()
    try:
        await near.start()
        await asyncio.sleep(0.45)
        assert storage.reads == ["heads"] * len(storage.reads)
        assert 2 <= len(storage.reads) <= 3
    finally:
        await near.stop()


async def test_a_hung_storage_read_does_not_stop_the_tick(
    pool: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    storage = Muffled(pool, lambda notice: notice.seq is not None)
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        await ana.quiet()
        hung = [True]
        storage.stuck = lambda thread_id, since, floor: bool(hung) and hung.pop()
        elsewhere = await Peer.open(far, ANA)
        with caplog.at_level(logging.WARNING):
            await say(elsewhere, "t1", "late")
            pushed = await ana.push("message", timeout=2.0)
        assert pushed["content"][0]["text"] == "late"
        assert hung == []
        assert "a catch-up pass took longer than 0.2 s and was stopped" in caplog.text
    finally:
        await near.stop()


class Remade(Muffled):
    def __init__(self, pool: Any, drop: Callable[[Notice], bool]) -> None:
        super().__init__(pool, drop)
        self.writing: Callable[[], Awaitable[None]] | None = None
        self.reading: Callable[[], Awaitable[None]] | None = None

    async def _wrote(self) -> None:
        hook, self.writing = self.writing, None
        if hook is not None:
            await hook()

    async def _read(self) -> None:
        hook, self.reading = self.reading, None
        if hook is not None:
            await hook()

    async def append_event(self, *args: Any, **kwargs: Any) -> Any:
        await self._wrote()
        return await super().append_event(*args, **kwargs)

    async def add_member(self, *args: Any, **kwargs: Any) -> Any:
        await self._wrote()
        return await super().add_member(*args, **kwargs)

    async def remove_member(self, *args: Any, **kwargs: Any) -> Any:
        await self._wrote()
        return await super().remove_member(*args, **kwargs)

    async def update_thread(self, *args: Any, **kwargs: Any) -> Any:
        await self._wrote()
        return await super().update_thread(*args, **kwargs)

    async def list_events(self, *args: Any, **kwargs: Any) -> Any:
        await self._read()
        return await super().list_events(*args, **kwargs)

    async def tail(self, *args: Any, **kwargs: Any) -> Any:
        await self._read()
        return await super().tail(*args, **kwargs)


async def remake(pool: Any, *members: Identity) -> list[str]:
    other = PostgresStorage(pool)
    await other.delete_thread("acme", "t1")
    made_again = Thread(id="t1", scope="acme", created_at=datetime.now(UTC))
    await other.create_thread("acme", made_again, added("t1", ANA, ANA))
    for member in members:
        await Server(other).add_member("acme", "t1", member)
    return [e.kind for e in await other.list_events("acme", "t1")]


@pytest.mark.parametrize("verb", ["event.send", "member.add", "member.remove", "thread.update"])
async def test_a_write_resolved_before_its_thread_was_made_again_is_refused(
    pool: Any, verb: str
) -> None:
    deaf = [False]
    storage = Remade(pool, lambda notice: deaf[0])
    near = Server(storage)
    await near.start()
    try:
        watcher = await Peer.open(near, ANA)
        await watcher.call("thread.create", thread_id="t1")
        if verb == "member.remove":
            await watcher.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
        await watcher.call("thread.join", thread_id="t1")
        await watcher.quiet()
        writer = await Peer.open(near, ANA)
        made: list[str] = []

        async def again() -> None:
            deaf[0] = True
            made.extend(await remake(pool, *([BRUNO] if verb == "member.remove" else [])))

        storage.writing = again
        payloads: dict[str, dict[str, Any]] = {
            "event.send": {
                "kind": "message",
                "client_id": "c1",
                "content": [{"type": "text", "text": "into the new thread"}],
            },
            "member.add": {"identity": BRUNO.model_dump(mode="json")},
            "member.remove": {"identity_id": "bruno"},
            "thread.update": {"metadata": {"n": 1}},
        }
        with pytest.raises(NotFoundError, match="^postgres: thread 't1' was not found$"):
            await writer.call(verb, thread_id="t1", **payloads[verb])
        assert await watcher.quiet(0.3) == []
        found = await PostgresStorage(pool).list_events("acme", "t1")
        assert made and [e.kind for e in found] == made
    finally:
        await near.stop()


async def test_a_rewind_into_a_thread_made_again_empties_the_old_room(pool: Any) -> None:
    deaf = [False]
    storage = Remade(pool, lambda notice: deaf[0])
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        for n in range(2):
            await say(ana, "t1", f"old {n}")
        await ana.quiet()
        elsewhere = await Peer.open(far, ANA)

        async def again() -> None:
            deaf[0] = True
            await remake(pool)
            for n in range(2):
                await say(elsewhere, "t1", f"new {n}")

        storage.reading = again
        with pytest.raises(NotFoundError, match="^server: thread 't1' was not found$"):
            await ana.call("thread.join", thread_id="t1", since=1)
        assert [p["kind"] for p in await ana.quiet(0.3)] == ["thread.deleted"]
    finally:
        await near.stop()


async def test_a_pass_stopped_on_a_later_page_still_hands_a_cut_member_the_thread_again(
    pool: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    storage = Muffled(pool, lambda notice: notice.seq is not None)
    near, far = Server(storage, replay_limit=2), Server(PostgresStorage(pool))
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        bruno = await Peer.open(near, BRUNO)
        await bruno.call("thread.join", thread_id="t1")
        await ana.quiet()
        await bruno.quiet()
        storage.refuse = lambda thread_id, since, floor: True
        await far.remove_member("acme", "t1", BRUNO)
        await far.add_member("acme", "t1", BRUNO)
        writer = await Peer.open(far, ANA)
        for n in range(3):
            await say(writer, "t1", f"m{n}")
        hung = [True]
        storage.stuck = lambda thread_id, since, floor: since >= 4 and bool(hung) and hung.pop()
        with caplog.at_level(logging.WARNING):
            storage.refuse = lambda thread_id, since, floor: False
            assert (await bruno.push("invited", timeout=3.0))["thread"]["id"] == "t1"
            await until(lambda: "a catch-up pass took longer than 0.2 s" in caplog.text)
        assert hung == []
    finally:
        await near.stop()


async def test_a_tick_leaves_a_room_whose_lock_is_held_and_catches_the_others_up(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    storage = Muffled(pool, lambda notice: notice.thread_id == "t2" and notice.seq is not None)
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        for thread_id in ("t1", "t2"):
            await ana.call("thread.create", thread_id=thread_id)
            await ana.call("thread.join", thread_id=thread_id)
        await ana.quiet()
        storage.stuck = lambda thread_id, since, floor: thread_id == "t1"
        elsewhere = await Peer.open(far, ANA)
        await say(elsewhere, "t1", "held")
        await say(elsewhere, "t2", "unheard")
        pushed = await ana.push("message", timeout=2.0)
        assert (pushed["thread_id"], pushed["content"][0]["text"]) == ("t2", "unheard")
    finally:
        await near.stop()


class Delayed(Muffled):
    def __init__(self, pool: Any, drop: Callable[[Notice], bool]) -> None:
        super().__init__(pool, drop)
        self.answered = asyncio.Event()
        self.answered.set()

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        await self.answered.wait()
        return await super().is_member(scope, thread_id, identity_id)


async def test_a_stuck_aftermath_does_not_hold_the_tick(
    pool: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    storage = Delayed(pool, lambda notice: notice.seq is not None)
    near, far = Server(storage, replay_limit=2), Server(PostgresStorage(pool))
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        for thread_id in ("t1", "t2"):
            await ana.call("thread.create", thread_id=thread_id)
        await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
        for thread_id in ("t1", "t2"):
            await ana.call("thread.join", thread_id=thread_id)
        bruno = await Peer.open(near, BRUNO)
        await bruno.call("thread.join", thread_id="t1")
        await ana.quiet()
        await bruno.quiet()
        storage.refuse = lambda thread_id, since, floor: True
        await far.remove_member("acme", "t1", BRUNO)
        await far.add_member("acme", "t1", BRUNO)
        writer = await Peer.open(far, ANA)
        for n in range(3):
            await say(writer, "t1", f"m{n}")
        hung = [True]
        storage.stuck = lambda thread_id, since, floor: (
            thread_id == "t1" and since >= 4 and bool(hung) and hung.pop()
        )
        storage.answered.clear()
        with caplog.at_level(logging.WARNING):
            storage.refuse = lambda thread_id, since, floor: False
            await until(lambda: "a catch-up pass took longer than 0.2 s" in caplog.text)
        await say(writer, "t2", "after")
        said = [
            (p["thread_id"], p["content"][0]["text"])
            for p in await ana.quiet(0.6)
            if p["kind"] == "message"
        ]
        assert said == [("t1", "m0"), ("t1", "m1"), ("t1", "m2"), ("t2", "after")]
        storage.answered.set()
        assert (await bruno.push("invited", timeout=2.0))["thread"]["id"] == "t1"
    finally:
        storage.answered.set()
        await near.stop()
