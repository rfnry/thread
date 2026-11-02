import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from until import until

from rfnry_thread import server as server_module
from rfnry_thread.errors import StorageError
from rfnry_thread.models import StreamDelta, Thread
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server
from rfnry_thread.state import EXPIRY
from rfnry_thread.storage import OnEvent, OnResume, OnSignal, Stop

from .conftest import say
from .factories import ANA, BRUNO, HELPER, added
from .peer import Peer


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
    storage: PostgresStorage,
) -> None:
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
        await storage.delete_scope("acme")
        await far.create_thread("acme", "t1")
        await far.add_member("acme", "t1", BRUNO)
        bruno = await Peer.open(far, BRUNO)
        await bruno.call("thread.join", thread_id="t1")
        assert await ana.quiet() == []
        [resume] = resumed
        assert resume is not None
        await resume()
        assert (await ana.push("thread.deleted"))["sender"]["id"] == "system"
        for n in range(6):
            await say(bruno, "t1", f"new {n}")
        assert await ana.quiet(0.3) == []
    finally:
        await near.stop()


async def test_what_a_resumed_listener_hears_before_a_rooms_turn_waits_for_that_rooms_check(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    resumed = resumes(storage)
    near, far = Server(storage), Server(storage)
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        for thread_id in ("a0", "a1", "a2", "t1"):
            await ana.call("thread.create", thread_id=thread_id)
            await ana.call("thread.join", thread_id=thread_id)
        for n in range(3):
            await say(ana, "t1", f"old {n}")
        await ana.quiet()
        await storage.delete_thread("acme", "t1")
        await far.create_thread("acme", "t1")
        await far.add_member("acme", "t1", BRUNO)
        bruno = await Peer.open(far, BRUNO)
        await bruno.call("thread.join", thread_id="t1")
        await far.start()
        real = storage.get_thread

        async def slow(scope: str, thread_id: str) -> Any:
            if thread_id.startswith("a"):
                await asyncio.sleep(0.5)
            return await real(scope, thread_id)

        monkeypatch.setattr(storage, "get_thread", slow)
        resume = resumed[0]
        assert resume is not None
        walking = asyncio.ensure_future(resume())
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
    await storage.add_member("acme", added("t1", BRUNO))
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


async def test_a_room_whose_check_fails_tells_no_one_until_a_check_succeeds(
    storage: PostgresStorage,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    near, far = Server(storage), Server(storage)
    ana = await Peer.open(near, ANA)
    for thread_id in ("t1", "t2"):
        await ana.call("thread.create", thread_id=thread_id)
        await ana.call("thread.join", thread_id=thread_id)
    for n in range(3):
        await say(ana, "t1", f"old {n}")
    await ana.quiet()
    await storage.delete_scope("acme")
    await far.create_thread("acme", "t1")
    await far.add_member("acme", "t1", BRUNO)
    bruno = await Peer.open(far, BRUNO)
    for n in range(6):
        await say(bruno, "t1", f"new {n}")
    real = storage.get_thread

    async def failing(scope: str, thread_id: str) -> Any:
        if thread_id == "t1":
            raise StorageError("postgres: storage rejected get_thread: connection reset")
        return await real(scope, thread_id)

    monkeypatch.setattr(storage, "get_thread", failing)
    with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
        await near._hub.on_resume()
        await near._hub.on_event("acme", "t1", 7)
    assert [(p["kind"], p["thread_id"]) for p in await ana.quiet()] == [("thread.deleted", "t2")]
    assert "hub: checking thread 't1' against storage failed" in caplog.text
    monkeypatch.setattr(storage, "get_thread", real)
    await near._hub.on_event("acme", "t1", 7)
    assert [(p["kind"], p["thread_id"]) for p in await ana.quiet()] == [("thread.deleted", "t1")]


async def test_a_connection_taken_out_of_a_thread_ends_its_presence_when_what_follows_fails(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    await ana.call("thread.join", thread_id="t1")
    bruno = await Peer.open(near, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    await ana.quiet()
    await bruno.quiet()
    [held] = [s for s in near._hub.sessions if s.identity.id == "ana"]
    held.created["t1"] -= timedelta(days=1)
    real = storage.list_events

    async def failing(*args: Any, **kwargs: Any) -> Any:
        if kwargs.get("since") is not None:
            raise StorageError("postgres: storage rejected list_events: connection reset")
        return await real(*args, **kwargs)

    monkeypatch.setattr(storage, "list_events", failing)
    walk = near._hub.on_resume()
    await near._hub.on_event("acme", "t1", 3)
    assert (await ana.push("thread.deleted"))["sender"]["id"] == "system"
    assert [(p["kind"], p["sender"]["id"]) for p in await bruno.quiet()] == [("left", "ana")]
    await asyncio.wait_for(walk, 5)


async def test_a_room_marked_again_while_its_check_reads_is_checked_again(
    storage: PostgresStorage, pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(PostgresStorage(pool))
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    await say(ana, "t1", "old")
    await ana.quiet()
    real = storage.get_thread
    reading, held = asyncio.Event(), asyncio.Event()

    async def slow(scope: str, thread_id: str) -> Any:
        found = await real(scope, thread_id)
        if not reading.is_set():
            reading.set()
            await asyncio.wait_for(held.wait(), 5.0)
        return found

    monkeypatch.setattr(storage, "get_thread", slow)
    first = asyncio.ensure_future(near._hub.on_resume())
    await asyncio.wait_for(reading.wait(), 5.0)
    await storage.delete_thread("acme", "t1")
    await far.create_thread("acme", "t1")
    await far.add_member("acme", "t1", BRUNO)
    second = asyncio.ensure_future(near._hub.on_resume())
    held.set()
    await asyncio.wait_for(asyncio.gather(first, second), 5.0)
    assert (await ana.push("thread.deleted"))["sender"]["id"] == "system"
    bruno = await Peer.open(far, BRUNO)
    for n in range(3):
        sent = await say(bruno, "t1", f"new {n}")
        await near._hub.on_event("acme", "t1", sent["seq"])
    assert await ana.quiet() == []


async def test_a_room_whose_check_failed_is_checked_again_on_the_refresh_and_not_repeated(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.1)
    near = Server(storage)
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        await ana.quiet()
        await storage.delete_thread("acme", "t1")
        real, publish = storage.get_thread, storage.publish
        published: list[str] = []

        async def failing(scope: str, thread_id: str) -> Any:
            raise StorageError("postgres: storage rejected get_thread: connection reset")

        async def recorded(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
            published.append(thread_id)
            await publish(scope, thread_id, payload)

        monkeypatch.setattr(storage, "get_thread", failing)
        monkeypatch.setattr(storage, "publish", recorded)
        await near._hub.on_resume()
        await asyncio.sleep(0.4)
        assert "t1" not in published
        assert await ana.quiet() == []
        monkeypatch.setattr(storage, "get_thread", real)
        assert (await ana.push("thread.deleted", timeout=2.0))["sender"]["id"] == "system"
    finally:
        await near.stop()


async def test_what_changed_while_a_rooms_check_failed_is_told_once_it_succeeds(
    storage: PostgresStorage, pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        bruno = await Peer.open(far, BRUNO)
        await bruno.call("thread.join", thread_id="t1")

        async def send(kind: str, **fields: Any) -> None:
            await bruno.call("signal.send", thread_id="t1", kind=kind, **fields)

        def held() -> list[tuple[str, str | None]]:
            found = near._hub.state.snapshot("acme", "t1")
            return [(s.kind, getattr(s, "stream_id", None)) for s in found]

        await send("working", label="Reading")
        await send("stream.start", stream_id="s1")
        await send("stream.delta", stream_id="s1", index=0, text="Hel")
        await send("stream.start", stream_id="s2")
        await until(lambda: len(held()) == 6)
        await ana.quiet()
        real = storage.get_thread

        async def failing(scope: str, thread_id: str) -> Any:
            raise StorageError("postgres: storage rejected get_thread: connection reset")

        monkeypatch.setattr(storage, "get_thread", failing)
        await near._hub.on_resume()
        await send("idle")
        await send("stream.delta", stream_id="s1", index=1, text="lo")
        await send("stream.end", stream_id="s2")
        await send("stream.start", stream_id="s3")
        await send("stream.delta", stream_id="s3", index=0, text="Hi")
        await until(lambda: ("stream.delta", "s3") in held() and ("working", None) not in held())
        assert await ana.quiet() == []
        monkeypatch.setattr(storage, "get_thread", real)
        await near._hub.recheck()
        told = [(p["kind"], p.get("stream_id"), p.get("text")) for p in await ana.quiet()]
        assert told == [
            ("stream.end", "s2", None),
            ("idle", None, None),
            ("stream.delta", "s1", "lo"),
            ("stream.start", "s3", None),
            ("stream.delta", "s3", "Hi"),
        ]
    finally:
        await near.stop()
        await far.stop()


async def test_an_unexpected_error_checking_one_room_is_logged_and_the_rest_are_checked(
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
        assert "hub: checking thread 't1' against storage failed: storage: publish broke" in (
            caplog.text
        )
        assert near._hub._unchecked == set()
    finally:
        await near.stop()


async def test_a_check_never_delivers_what_a_thread_made_again_between_its_reads_wrote(
    storage: PostgresStorage, pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(PostgresStorage(pool))
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    for n in range(3):
        await say(ana, "t1", f"old {n}")
    await ana.quiet()
    real = storage.get_thread

    async def remade(scope: str, thread_id: str) -> Any:
        found = await real(scope, thread_id)
        monkeypatch.setattr(storage, "get_thread", real)
        await far.delete_thread("acme", "t1")
        await far.create_thread("acme", "t1")
        await far.add_member("acme", "t1", BRUNO)
        bruno = await Peer.open(far, BRUNO)
        for n in range(5):
            await say(bruno, "t1", f"new {n}")
        return found

    monkeypatch.setattr(storage, "get_thread", remade)
    await near._hub.on_resume()
    assert [p["kind"] for p in await ana.quiet()] == []


def settling(storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
    real = storage.list_events

    async def failing(*args: Any, **kwargs: Any) -> Any:
        if kwargs == {"limit": 1}:
            raise StorageError("postgres: storage rejected list_events: connection reset")
        return await real(*args, **kwargs)

    monkeypatch.setattr(storage, "list_events", failing)
    return lambda: monkeypatch.setattr(storage, "list_events", real)


async def test_no_connection_enters_a_room_whose_check_is_failing(
    storage: PostgresStorage, pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        for who in (BRUNO, HELPER):
            await ana.call("member.add", thread_id="t1", identity=who.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        helper = await Peer.open(far, HELPER)
        await helper.call("thread.join", thread_id="t1")

        async def stream(index: int, text: str) -> None:
            await helper.call(
                "signal.send",
                thread_id="t1",
                kind="stream.delta",
                stream_id="s1",
                index=index,
                text=text,
            )

        def written() -> list[str]:
            found = near._hub.state.snapshot("acme", "t1")
            return [s.text for s in found if isinstance(s, StreamDelta)]

        await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
        await stream(0, "abc")
        await until(lambda: written() == ["abc"])
        recovered = settling(storage, monkeypatch)
        await near._hub.on_resume()
        await stream(1, "def")
        await until(lambda: written() == ["abcdef"])
        bruno = await Peer.open(near, BRUNO)
        with pytest.raises(StorageError, match="^server: storage failed$"):
            await bruno.call("thread.join", thread_id="t1")
        with pytest.raises(StorageError, match="^server: storage failed$"):
            await bruno.call("thread.follow")
        recovered()
        await near._hub.recheck()
        await bruno.call("thread.join", thread_id="t1")
        assert [p["text"] for p in await bruno.quiet() if p["kind"] == "stream.delta"] == ["abcdef"]
    finally:
        await near.stop()
        await far.stop()


async def test_a_follower_added_to_a_room_whose_check_is_failing_is_not_entered(
    storage: PostgresStorage,
    pool: Any,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    near, far = Server(storage), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        bruno = await Peer.open(near, BRUNO)
        await bruno.call("thread.follow")
        settling(storage, monkeypatch)
        await near._hub.on_resume()
        with caplog.at_level(logging.WARNING, logger="rfnry_thread.hub"):
            await far.add_member("acme", "t1", BRUNO)
            await until(lambda: "hub: entering connection" in caplog.text, timeout=5.0)
        assert await bruno.quiet() == []
        assert bruno.joined == []
    finally:
        await near.stop()
        await far.stop()


async def test_a_replica_that_never_lost_its_listener_reads_no_room_again(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    reads: list[str] = []

    def counted(name: str) -> Any:
        real = getattr(storage, name)

        async def read(*args: Any, **kwargs: Any) -> Any:
            reads.append(name)
            return await real(*args, **kwargs)

        return read

    for name in ("get_thread", "list_events", "list_members", "is_member", "head"):
        monkeypatch.setattr(storage, name, counted(name))
    await near.start()
    try:
        await near._hub.recheck()
        await near._hub.announce()
        await near._hub.sweep()
        await ana.quiet()
        assert reads == []
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

    async def late(scope: str, thread_id: str, seq: int) -> None:
        await asyncio.sleep(0.3)
        await heard(scope, thread_id, seq)

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
        real = storage.list_events
        done: list[bool] = []

        async def replaying(*args: Any, **kwargs: Any) -> Any:
            found = await real(*args, **kwargs)
            if kwargs.get("since") == 0 and not done:
                done.append(True)
                await say(ana, "t1", "late")
                await asyncio.sleep(0.4)
            return found

        monkeypatch.setattr(storage, "list_events", replaying)
        helper = await Peer.open(near, HELPER)
        await helper.call("thread.follow", since={"t1": {"seq": 0}})
        pushed = [
            p["content"][0]["text"] for p in await helper.quiet(0.6) if p["kind"] == "message"
        ]
        assert pushed == ["one", "late"]
    finally:
        await near.stop()
        await far.stop()
