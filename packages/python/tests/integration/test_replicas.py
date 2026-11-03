import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg
import pytest
from database import SCHEMA, URL, open_pool
from unit.conftest import say
from unit.factories import ANA, BRUNO, HELPER, added
from unit.peer import Peer
from unit.writes import admit, epoch
from until import until

from integration.pools import Refusing, Stalling
from rfnry_thread import postgres, server as server_module, state
from rfnry_thread.errors import StorageError
from rfnry_thread.models import Identity, Part, Text, Thread
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server


async def listeners(pool: Any) -> list[int]:
    _, signals = await PostgresStorage(pool)._channels()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT pid FROM pg_stat_activity WHERE datname = current_database() "
            "AND application_name = current_setting('application_name') "
            "AND pid <> pg_backend_pid() AND query = $1",
            f'LISTEN "{signals}"',
        )
    return [row["pid"] for row in rows]


async def terminate(pool: Any, expected: int) -> list[int]:
    killed = await listeners(pool)
    assert len(killed) == expected
    async with pool.acquire() as conn:
        for pid in killed:
            await conn.execute("SELECT pg_terminate_backend($1)", pid)
    return killed


async def relistened(pool: Any, killed: list[int], expected: int, timeout: float = 5.0) -> None:
    async def wait() -> None:
        while len([pid for pid in await listeners(pool) if pid not in killed]) < expected:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(wait(), timeout)


@pytest.fixture
async def replicas(storage: PostgresStorage) -> AsyncIterator[tuple[Server, Server]]:
    far_pool = await open_pool()
    near = Server(storage)
    far = Server(PostgresStorage(far_pool))
    await near.start()
    await far.start()
    yield near, far
    await near.stop()
    await far.stop()
    await far_pool.close()


async def room(near: Server, far: Server) -> tuple[Peer, Peer]:
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await ana.call("thread.join", thread_id="t1")
    helper = await Peer.open(far, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await ana.quiet(0.3)
    await helper.quiet(0.3)
    return ana, helper


async def arrived(peer: Peer, count: int, within: float) -> list[tuple[str, str]]:
    async def wait() -> None:
        while len(peer.pushes) < count:
            frame = await peer.frame(within)
            if frame["type"] == "push":
                peer.pushes.append(frame["payload"])

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(wait(), within)
    found, peer.pushes = peer.pushes, []
    return [(p["kind"], p["sender"]["id"]) for p in found]


async def busy(ana: Peer, helper: Peer) -> None:
    await helper.call("signal.send", thread_id="t1", kind="working", label="Checking")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await helper.notify(
        "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="Half"
    )
    assert await arrived(ana, 3, 2.0) == [
        ("working", "helper"),
        ("stream.start", "helper"),
        ("stream.delta", "helper"),
    ]


async def crash(server: Server) -> None:
    assert server._refresher is not None and server._stop is not None
    server._refresher.cancel()
    server._hub.linked = False
    await server._stop()
    server._stop = None


@contextlib.asynccontextmanager
async def briefly(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[tuple[Server, Server]]:
    monkeypatch.setattr(state, "EXPIRY", 1.0)
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    far_pool = await open_pool()
    near = Server(storage)
    far = Server(PostgresStorage(far_pool))
    await near.start()
    await far.start()
    try:
        yield near, far
    finally:
        await near.stop()
        await far.stop()
        await asyncio.wait_for(far_pool.close(), 5)


async def present(peer: Peer) -> list[str]:
    identities = (await peer.call("thread.present", thread_id="t1"))["identities"]
    return sorted(i["id"] for i in identities)


async def test_a_replica_that_stops_ends_its_presence_work_and_streams_for_the_others(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    await busy(ana, helper)
    await far.stop()
    assert await arrived(ana, 3, 2.0) == [
        ("stream.end", "helper"),
        ("idle", "helper"),
        ("left", "helper"),
    ]
    assert await present(ana) == ["ana"]
    await helper.close()
    assert await ana.quiet(0.5) == []


async def test_a_crashed_replica_ends_its_presence_work_and_streams_for_the_others(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with briefly(storage, monkeypatch) as (near, far):
        ana, helper = await room(near, far)
        await busy(ana, helper)
        await crash(far)
        assert await arrived(ana, 3, 5.0) == [
            ("stream.end", "helper"),
            ("idle", "helper"),
            ("left", "helper"),
        ]
        assert await present(ana) == ["ana"]
        assert await ana.quiet(1.0) == []


async def test_a_crashed_replica_ends_only_what_is_still_open_and_held_nowhere_else(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with briefly(storage, monkeypatch) as (near, far):
        ana, helper = await room(near, far)
        here = await Peer.open(near, HELPER)
        await here.call("thread.join", thread_id="t1")
        await busy(ana, helper)
        await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
        assert await arrived(ana, 1, 2.0) == [("stream.end", "helper")]
        await crash(far)
        assert await arrived(ana, 1, 5.0) == [("idle", "helper")]
        assert await present(ana) == ["ana", "helper"]
        assert await ana.quiet(1.0) == []


async def test_a_replica_heard_from_again_is_announced_again_and_ends_once(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with briefly(storage, monkeypatch) as (near, far):
        ana, helper = await room(near, far)
        await busy(ana, helper)
        assert near._stop is not None
        await near._stop()
        assert await arrived(ana, 3, 5.0) == [
            ("stream.end", "helper"),
            ("idle", "helper"),
            ("left", "helper"),
        ]
        near._stop = await storage.listen(
            near._hub.on_event, near._hub.on_signal, near._hub.on_resume
        )
        assert await arrived(ana, 3, 5.0) == [
            ("present", "helper"),
            ("working", "helper"),
            ("stream.start", "helper"),
        ]
        assert await present(ana) == ["ana", "helper"]
        await helper.notify(
            "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=1, text="!"
        )
        await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
        await helper.call("signal.send", thread_id="t1", kind="idle")
        await helper.call("thread.leave", thread_id="t1")
        assert await arrived(ana, 3, 2.0) == [
            ("stream.end", "helper"),
            ("idle", "helper"),
            ("left", "helper"),
        ]
        assert await ana.quiet(1.0) == []


async def test_a_replica_stopped_and_started_again_with_its_connections_open_is_announced_again_and_ends_once(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with briefly(storage, monkeypatch) as (near, far):
        ana, helper = await room(near, far)
        try:
            await busy(ana, helper)
            await far.stop()
            assert await arrived(ana, 3, 2.0) == [
                ("stream.end", "helper"),
                ("idle", "helper"),
                ("left", "helper"),
            ]
            await far.start()
            assert await arrived(ana, 3, 2.0) == [
                ("present", "helper"),
                ("working", "helper"),
                ("stream.start", "helper"),
            ]
            assert await present(ana) == ["ana", "helper"]
            await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
            await helper.call("signal.send", thread_id="t1", kind="idle")
            await helper.call("thread.leave", thread_id="t1")
            assert await arrived(ana, 3, 2.0) == [
                ("stream.end", "helper"),
                ("idle", "helper"),
                ("left", "helper"),
            ]
            assert await ana.quiet(1.0) == []
        finally:
            await ana.close()
            await helper.close()


async def test_a_stopped_replica_ends_the_others_state_for_its_own_connections(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    await busy(ana, helper)
    await near.stop()
    assert await arrived(ana, 3, 2.0) == [
        ("stream.end", "helper"),
        ("idle", "helper"),
        ("left", "helper"),
    ]
    assert await present(ana) == ["ana"]


async def crossed(far: Server, helper: Peer, **step: Any) -> None:
    storage = far._hub.storage
    publish = storage.publish
    held, release = asyncio.Event(), asyncio.Event()

    async def late(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        if payload.get("kind") == "refresh":
            held.set()
            await release.wait()
        await publish(scope, thread_id, payload)

    storage.publish = late  # type: ignore[method-assign]
    try:
        announcing = asyncio.get_running_loop().create_task(far._hub.announce())
        await asyncio.wait_for(held.wait(), 2.0)
        await helper.call(**step)
        release.set()
        await asyncio.wait_for(announcing, 2.0)
    finally:
        storage.publish = publish  # type: ignore[method-assign]


async def test_a_repeat_taken_before_a_stream_end_does_not_start_the_stream_again(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    assert await arrived(ana, 1, 2.0) == [("stream.start", "helper")]
    await crossed(
        far, helper, method="signal.send", thread_id="t1", kind="stream.end", stream_id="s1"
    )
    assert await arrived(ana, 1, 2.0) == [("stream.end", "helper")]
    assert await ana.quiet(0.5) == []


async def test_a_repeat_taken_before_a_leave_does_not_bring_the_presence_back(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    await crossed(far, helper, method="thread.leave", thread_id="t1")
    assert await arrived(ana, 1, 2.0) == [("left", "helper")]
    assert await ana.quiet(0.5) == []
    await far._hub.announce()
    assert await ana.quiet(0.5) == []
    assert await present(ana) == ["ana"]


async def test_deleting_a_scope_empties_its_rooms_on_the_other_replica(
    replicas: tuple[Server, Server], storage: PostgresStorage
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    old = await epoch(storage, "acme", "t1")
    elsewhere = await Peer.open(far, HELPER, scope="other")
    await elsewhere.call("thread.create", thread_id="t1")
    await elsewhere.call("thread.join", thread_id="t1")
    await helper.call("signal.send", thread_id="t1", kind="working", label="Checking")
    assert await arrived(ana, 1, 2.0) == [("working", "helper")]
    await helper.quiet(0.3)
    await elsewhere.quiet(0.3)
    await near.delete_scope("acme")
    assert await arrived(ana, 1, 2.0) == [("thread.deleted", "system")]
    assert await arrived(helper, 1, 2.0) == [("thread.deleted", "system")]
    for server in (near, far):
        assert server._hub.state.present("acme", "t1", old) == []
        assert [key for key in server._hub._rooms if key[0] == "acme"] == []
    await near.create_thread("acme", "t1")
    await near.add_member("acme", "t1", HELPER)
    assert (await helper.push("invited", timeout=2.0))["thread"]["id"] == "t1"
    await helper.call("thread.join", thread_id="t1")
    assert await present(helper) == ["helper"]
    assert await elsewhere.quiet(0.3) == []
    assert far._hub.state.present("other", "t1", await epoch(storage, "other", "t1")) == [HELPER]


async def first_repeat_held(far: Server, step: Callable[[list[str]], Awaitable[None]]) -> None:
    storage = far._hub.storage
    publish = storage.publish
    held, release = asyncio.Event(), asyncio.Event()
    sent: list[str] = []

    async def late(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        if payload.get("kind") == "refresh" and not held.is_set():
            held.set()
            await release.wait()
        await publish(scope, thread_id, payload)
        sent.append(str(payload.get("kind")))

    storage.publish = late  # type: ignore[method-assign]
    try:
        announcing = asyncio.get_running_loop().create_task(far._hub.announce())
        await asyncio.wait_for(held.wait(), 2.0)
        await step(sent)
        release.set()
        await asyncio.wait_for(announcing, 2.0)
    finally:
        storage.publish = publish  # type: ignore[method-assign]


async def heard_since(near: Server, far: Server) -> None:
    elsewhere = await Peer.open(far, HELPER, scope="other")
    await elsewhere.call("thread.create", thread_id="t1")
    await elsewhere.call("thread.join", thread_id="t1")
    other = await epoch(far._hub.storage, "other", "t1")
    await until(lambda: near._hub.state.present("other", "t1", other) == [HELPER])


async def made_again(near: Server, ana: Peer) -> tuple[list[str], list[str]]:
    await near.create_thread("acme", "t1")
    await near.add_member("acme", "t1", ANA)
    await ana.call("thread.join", thread_id="t1")
    told = [p["sender"]["id"] for p in await ana.quiet() if p["kind"] == "present"]
    return told, await present(ana)


async def test_a_repeat_taken_before_a_scope_was_deleted_does_not_bring_its_state_back(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    old = await epoch(near._hub.storage, "acme", "t1")

    async def wipe(sent: list[str]) -> None:
        await near.delete_scope("acme")
        assert await arrived(helper, 1, 2.0) == [("thread.deleted", "system")]
        await until(lambda: sent == ["refresh"])

    await first_repeat_held(far, wipe)
    await heard_since(near, far)
    assert near._hub.state.present("acme", "t1", old) == []
    assert await made_again(near, ana) == (["ana"], ["ana"])


async def test_a_repeat_taken_before_a_leave_does_not_bring_the_presence_back_after_a_wipe(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)

    async def leave_then_wipe(sent: list[str]) -> None:
        await helper.call("thread.leave", thread_id="t1")
        assert await arrived(ana, 1, 2.0) == [("left", "helper")]
        await near.delete_scope("acme")
        assert await arrived(ana, 1, 2.0) == [("thread.deleted", "system")]

    await first_repeat_held(far, leave_then_wipe)
    await heard_since(near, far)
    assert await made_again(near, ana) == (["ana"], ["ana"])
    bruno = await Peer.open(near, BRUNO)
    await near.add_member("acme", "t1", BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    await bruno.quiet()
    await ana.quiet()
    now = near._hub.state._clock()
    near._hub.state._clock = lambda: now + state.EXPIRY + 1
    near._hub.sweep()
    assert await ana.quiet() == [] and await bruno.quiet() == []
    await bruno.close()


async def test_a_cancelled_stop_still_gives_the_listening_connection_back(
    pool: Any, storage: PostgresStorage
) -> None:
    server = Server(storage)
    await server.start()
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    publishing = asyncio.Event()

    async def stuck(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        publishing.set()
        await asyncio.sleep(30)

    storage.publish = stuck  # type: ignore[method-assign]
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(server.stop(), 0.5)
    assert publishing.is_set()
    assert await listeners(pool) == []
    await ana.close()


async def test_events_cross_databases_replicas_in_order_once(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    for n in range(10):
        await say(ana, "t1", f"m{n}")
    messages = [p for p in await helper.quiet(0.5) if p["kind"] == "message"]
    assert [p["seq"] for p in messages] == list(range(3, 13))


async def test_a_message_reaches_the_other_replica_with_its_metadata(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    sent = await say(helper, "t1", "Refunded.", metadata={"run_id": "r1", "reviewed": False})
    pushed = await ana.push("message", timeout=2.0)
    assert pushed["id"] == sent["id"]
    assert pushed["metadata"] == {"run_id": "r1", "reviewed": False}
    history = (await ana.call("thread.history", thread_id="t1", kinds=["message"]))["events"]
    assert [e["metadata"] for e in history] == [{"run_id": "r1", "reviewed": False}]


async def test_a_second_replica_receives_only_the_redacted_form(storage: PostgresStorage) -> None:
    async def redact(author: Identity, thread: Thread, content: list[Part]) -> list[Part]:
        return [
            Text(text=p.text.replace("4111111111111111", "**** 1111"))
            for p in content
            if isinstance(p, Text)
        ]

    far_pool = await open_pool()
    near = Server(storage, redact=redact)
    far = Server(PostgresStorage(far_pool))
    await near.start()
    await far.start()
    try:
        ana, helper = await room(near, far)
        sent = await say(ana, "t1", "card 4111111111111111", client_id="c1")
        await ana.call(
            "event.send",
            thread_id="t1",
            kind="edit",
            client_id="e1",
            target=sent["id"],
            content=[{"type": "text", "text": "card 4111111111111111, sorry"}],
        )
        pushed = [p for p in await helper.quiet(0.5) if p["kind"] in ("message", "edit")]
        assert [p["content"][0]["text"] for p in pushed] == [
            "card **** 1111",
            "card **** 1111, sorry",
        ]
        history = (await helper.call("thread.history", thread_id="t1"))["events"]
        assert "4111111111111111" not in str(history)
        async with far_pool.acquire() as conn:
            rows = await conn.fetch("SELECT body::text AS body FROM thread_events")
        assert rows and all("4111111111111111" not in row["body"] for row in rows)
    finally:
        await near.stop()
        await far.stop()
        await far_pool.close()


class Unhinted(PostgresStorage):
    def __init__(self, pool: Any) -> None:
        super().__init__(pool)
        self.tried: list[tuple[str, str, int, int]] = []

    async def hint(self, scope: str, thread_id: str, seq: int, epoch: int) -> None:
        self.tried.append((scope, thread_id, seq, epoch))
        raise StorageError("postgres: storage rejected publish: the notification queue is full")


async def test_a_write_succeeds_when_its_hint_cannot_be_sent(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.3)
    near_pool = await open_pool()
    unhinted = Unhinted(near_pool)
    near, far = Server(unhinted), Server(storage)
    await near.start()
    await far.start()
    try:
        ana, helper = await room(near, far)
        unhinted.tried.clear()
        with caplog.at_level(logging.WARNING):
            sent = await say(ana, "t1", "unhinted")
            assert (await ana.push("message", timeout=1.0))["id"] == sent["id"]
            pushed = await helper.push("message", timeout=2.0)
        assert pushed["id"] == sent["id"] and pushed["seq"] == sent["seq"]
        thread = await storage.get_thread("acme", "t1")
        assert thread is not None
        assert unhinted.tried == [("acme", "t1", sent["seq"], thread.epoch)]
        assert (
            "hub: hinting other replicas failed: postgres: storage rejected publish: "
            "the notification queue is full"
        ) in caplog.text
    finally:
        await near.stop()
        await far.stop()
        await asyncio.wait_for(near_pool.close(), 5)


async def test_a_write_is_answered_while_its_hint_cannot_be_sent(
    pool: Any, storage: PostgresStorage
) -> None:
    stalling = Stalling(pool)
    near, far = Server(PostgresStorage(stalling)), Server(storage)
    await near.start()
    await far.start()
    try:
        ana, helper = await room(near, far)
        stalling.stall()
        sent = await asyncio.wait_for(say(ana, "t1", "answered"), 1.0)
        assert (await ana.push("message", timeout=1.0))["id"] == sent["id"]
        await until(lambda: stalling.holding)
        assert await helper.quiet(0.3) == []
        stalling.resume()
        assert (await helper.push("message", timeout=1.0))["id"] == sent["id"]
    finally:
        stalling.resume()
        await near.stop()
        await far.stop()


async def test_no_request_waits_on_a_notice(pool: Any, storage: PostgresStorage) -> None:
    stalling = Stalling(pool)
    near, far = Server(PostgresStorage(stalling)), Server(storage)
    await near.start()
    await far.start()
    try:
        ana, helper = await room(near, far)
        stalling.stall()
        sent = await asyncio.wait_for(say(ana, "t1", "first"), 1.0)
        await until(lambda: stalling.holding)
        for step in (
            {"method": "thread.create", "thread_id": "t2"},
            {"method": "member.add", "thread_id": "t2", "identity": BRUNO.model_dump(mode="json")},
            {"method": "thread.update", "thread_id": "t2", "metadata": {"n": 1}},
            {"method": "member.remove", "thread_id": "t2", "identity_id": "bruno"},
            {"method": "signal.send", "thread_id": "t1", "kind": "working", "label": "Busy"},
            {"method": "thread.mark", "thread_id": "t1", "seq": sent["seq"]},
            {"method": "thread.leave", "thread_id": "t1"},
            {"method": "thread.delete", "thread_id": "t2"},
        ):
            await asyncio.wait_for(ana.call(**step), 1.0)
        assert stalling.holding
        stalling.resume()
        assert (await helper.push("message", timeout=1.0))["id"] == sent["id"]
        assert (await helper.push("left", timeout=1.0))["sender"]["id"] == "ana"
    finally:
        stalling.resume()
        await near.stop()
        await far.stop()


async def test_a_stop_right_after_a_write_still_hints_it(storage: PostgresStorage) -> None:
    near_pool = await open_pool()
    stalling = Stalling(near_pool)
    near, far = Server(PostgresStorage(stalling)), Server(storage)
    await near.start()
    await far.start()
    try:
        helper = await Peer.open(far, HELPER)
        await near.create_thread("acme", "t1")
        await near.add_member("acme", "t1", HELPER)
        await helper.call("thread.join", thread_id="t1")
        await helper.quiet()
        stalling.stall()
        await near.update_thread("acme", "t1", {"last": True})
        stopping = asyncio.ensure_future(near.stop())
        await until(lambda: stalling.holding)
        await asyncio.sleep(0.1)
        assert not stopping.done()
        stalling.resume()
        await asyncio.wait_for(stopping, 2.0)
        await asyncio.wait_for(near_pool.close(), 5)
        assert (await helper.push("thread_updated", timeout=1.0))["metadata"] == {"last": True}
    finally:
        stalling.resume()
        await near.stop()
        await far.stop()
        await asyncio.wait_for(near_pool.close(), 5)


async def test_a_stop_while_writes_go_on_and_groups_stall_returns_within_its_bound(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(postgres, "NOTIFY_TIMEOUT", 0.3)
    stalling = Stalling(pool)
    near = Server(PostgresStorage(stalling))
    await near.start()
    writing = asyncio.Event()
    writing.set()

    async def writer(thread_id: str) -> None:
        while writing.is_set():
            stalling.stall()
            await near.update_thread("acme", thread_id, {"at": thread_id})

    for n in range(4):
        await near.create_thread("acme", f"w{n}")
    writers = [asyncio.ensure_future(writer(f"w{n}")) for n in range(4)]
    await asyncio.sleep(0.2)
    stopping = asyncio.ensure_future(near.stop())
    try:
        await asyncio.wait({stopping}, timeout=20 * 0.3)
        assert stopping.done() and not any(task.done() for task in writers)
    finally:
        writing.clear()
        await asyncio.gather(*writers)
        stalling.resume()
        await stopping
        await near.stop()


async def test_a_server_never_started_still_hints_its_writes(storage: PostgresStorage) -> None:
    writer_pool = await open_pool()
    near, writer = Server(storage), Server(PostgresStorage(writer_pool))
    await near.start()
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        await ana.quiet()
        helper = await Peer.open(writer, HELPER)
        sent = await say(helper, "t1", "from a writer")
        assert (await ana.push("message", timeout=1.0))["id"] == sent["id"]
        await writer.update_thread("acme", "t1", {"written": True})
        assert (await ana.push("thread_updated", timeout=1.0))["metadata"] == {"written": True}
    finally:
        await near.stop()
        await asyncio.wait_for(writer_pool.close(), 5)


async def test_signals_are_mirrored_through_notify(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working", timeout=2.0))["label"] == "Reading"


async def test_the_finishing_message_reaches_a_remote_connection_before_stream_end(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    for attempt in range(20):
        stream_id = f"s{attempt}"
        await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id=stream_id)
        await say(helper, "t1", "Done.", client_id=stream_id)
        await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id=stream_id)
    received = [p for p in await ana.quiet(1.0) if p["kind"] in ("message", "stream.end")]
    for attempt in range(20):
        stream_id = f"s{attempt}"
        message = next(
            n
            for n, p in enumerate(received)
            if p["kind"] == "message" and p["client_id"] == stream_id
        )
        end = next(
            n
            for n, p in enumerate(received)
            if p["kind"] == "stream.end" and p["stream_id"] == stream_id
        )
        assert message < end


async def test_the_servers_own_update_and_removal_reach_the_other_replica(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    await near.update_thread("acme", "t1", {"elevated": True})
    pushed = await helper.push("thread_updated", timeout=2.0)
    assert pushed["metadata"] == {"elevated": True} and pushed["author"]["role"] == "system"
    await near.remove_member("acme", "t1", HELPER)
    removed = await helper.push("member_removed", timeout=2.0)
    assert (removed["identity"]["id"], removed["author"]["role"]) == ("helper", "system")
    await say(ana, "t1", "after")
    assert "message" not in [p["kind"] for p in await helper.quiet(0.5)]


async def test_removal_cuts_off_a_member_on_another_replica(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    await say(ana, "t1", "after")
    kinds = [p["kind"] for p in await helper.quiet(0.5)]
    assert "member_removed" in kinds and "message" not in kinds


async def test_a_dropped_listener_listens_again_and_resumes(
    pool: Any, storage: PostgresStorage
) -> None:
    await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=datetime.now(UTC)))
    await admit(storage, "acme", added("t1", ANA))
    seen: list[int] = []
    heard: list[dict[str, Any]] = []
    resumed = asyncio.Event()

    async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        seen.append(seq)

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        heard.append(payload)

    async def on_resume() -> None:
        resumed.set()

    stop = await storage.listen(on_event, on_signal, on_resume)
    killed = await terminate(pool, 1)
    await relistened(pool, killed, 1)
    await asyncio.wait_for(resumed.wait(), 5.0)
    await storage.publish("acme", "t1", {"kind": "idle"})
    await storage.hint("acme", "t1", 2, await epoch(storage, "acme", "t1"))
    await until(lambda: heard == [{"kind": "idle"}] and seen == [2])
    await stop()
    assert await listeners(pool) == []


async def test_what_a_listener_hears_after_listening_again_is_passed_on(
    pool: Any, storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=datetime.now(UTC)))
    await admit(storage, "acme", added("t1", ANA))
    events_channel, _ = await storage._channels()
    order: list[str] = []
    relistening = asyncio.Event()
    halfway = asyncio.Event()
    attached = asyncio.Event()
    attach = asyncpg.connection.Connection.add_listener

    async def held(conn: Any, channel: str, callback: Any) -> None:
        await attach(conn, channel, callback)
        if relistening.is_set() and channel == events_channel:
            halfway.set()
            await asyncio.wait_for(attached.wait(), 5.0)

    monkeypatch.setattr(asyncpg.connection.Connection, "add_listener", held)
    monkeypatch.setattr(postgres, "RELISTEN", 0.01)

    def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> Awaitable[None]:
        order.append(f"event {seq}")
        return asyncio.sleep(0)

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        return None

    def on_resume() -> Awaitable[None]:
        order.append("resume")
        return asyncio.sleep(0)

    stop = await storage.listen(on_event, on_signal, on_resume)
    try:
        relistening.set()
        await terminate(pool, 1)
        await asyncio.wait_for(halfway.wait(), 5.0)
        await storage.hint("acme", "t1", 2, await epoch(storage, "acme", "t1"))
        await until(lambda: order == ["event 2"])
        attached.set()
        await until(lambda: order == ["event 2", "resume"])
    finally:
        attached.set()
        await stop()


async def probed(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, period: float = 0.2
) -> tuple[list[tuple[Any, str, Any]], list[int], asyncio.Event, Callable[[], Awaitable[None]]]:
    await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=datetime.now(UTC)))
    await admit(storage, "acme", added("t1", ANA))
    monkeypatch.setattr(postgres, "PROBE", period)
    monkeypatch.setattr(postgres, "RELISTEN", 0.01)
    attached: list[tuple[Any, str, Any]] = []
    attach = asyncpg.connection.Connection.add_listener

    async def kept(conn: Any, channel: str, callback: Any) -> None:
        await attach(conn, channel, callback)
        attached.append((conn, channel, callback))

    monkeypatch.setattr(asyncpg.connection.Connection, "add_listener", kept)
    seen: list[int] = []
    resumed = asyncio.Event()

    async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        seen.append(seq)

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        return None

    async def on_resume() -> None:
        resumed.set()

    stop = await storage.listen(on_event, on_signal, on_resume)
    assert len(attached) == 2
    return attached, seen, resumed, stop


async def test_a_listener_that_stops_hearing_its_own_probe_listens_again(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    attached, seen, resumed, stop = await probed(storage, monkeypatch)
    _, signals = await storage._channels()
    probes: list[str] = []
    raw = await asyncpg.connect(URL, timeout=5)
    try:
        with caplog.at_level(logging.WARNING):
            for conn, channel, callback in list(attached):
                await conn.remove_listener(channel, callback)
            await asyncio.wait_for(resumed.wait(), 2.0)
            await raw.add_listener(signals, lambda *notice: probes.append(notice[3]))
            await storage.hint("acme", "t1", 2, await epoch(storage, "acme", "t1"))
            await until(lambda: seen == [2])
            await until(lambda: len(probes) >= 3)
        missed = "postgres: the listener missed its own probe; listening again"
        assert caplog.text.count(missed) == 1
    finally:
        await stop()
        await raw.close()


async def test_a_probe_that_cannot_be_sent_is_not_a_miss(
    pool: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(postgres, "PROBE", 0.2)
    refusing = Refusing(pool)
    storage = PostgresStorage(refusing)
    _, signals = await storage._channels()
    probes: list[str] = []
    resumed = asyncio.Event()

    async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        return None

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        return None

    async def on_resume() -> None:
        resumed.set()

    raw = await asyncpg.connect(URL, timeout=5)
    stop = await storage.listen(on_event, on_signal, on_resume)
    try:
        with caplog.at_level(logging.WARNING):
            refusing.refuse = True
            await raw.add_listener(signals, lambda *notice: probes.append(notice[3]))
            await until(lambda: not refusing.refuse)
            await until(lambda: len(probes) >= 2)
        assert (
            "postgres: the listener's probe was not sent: "
            "postgres: storage rejected publish: the pool is closed"
        ) in caplog.text
        assert "missed its own probe" not in caplog.text and not resumed.is_set()
    finally:
        await stop()
        await raw.close()


async def test_a_listener_whose_connection_stops_answering_is_replaced_without_waiting_on_it(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    attached, seen, resumed, stop = await probed(storage, monkeypatch)
    conn = attached[0][0]
    try:
        conn._transport.pause_reading()
        await asyncio.wait_for(resumed.wait(), 2.0)
        assert conn.is_closed()
        await storage.hint("acme", "t1", 2, await epoch(storage, "acme", "t1"))
        await until(lambda: seen == [2])
    finally:
        if not conn.is_closed():
            conn.terminate()
        await stop()


async def test_a_stop_on_a_half_open_listener_returns_within_its_bound(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(postgres, "NOTIFY_TIMEOUT", 0.3)
    attached, _, _, stop = await probed(storage, monkeypatch, period=30.0)
    conn = attached[0][0]
    try:
        conn._transport.pause_reading()
        await asyncio.wait_for(stop(), 20 * 0.3)
        assert conn.is_closed()
    finally:
        if not conn.is_closed():
            conn.terminate()


async def test_a_stop_cancelled_while_it_gives_the_connection_back_leaves_none_checked_out(
    pool: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    own = await open_pool()
    attached, _, _, stop = await probed(PostgresStorage(own), monkeypatch, period=30.0)
    conn = attached[0][0]
    releasing = asyncio.Event()
    remove = asyncpg.connection.Connection.remove_listener

    async def removing(connection: Any, channel: str, callback: Any) -> None:
        releasing.set()
        await remove(connection, channel, callback)

    monkeypatch.setattr(asyncpg.connection.Connection, "remove_listener", removing)
    try:
        conn._transport.pause_reading()
        stopping = asyncio.ensure_future(stop())
        await asyncio.wait_for(releasing.wait(), 2.0)
        stopping.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stopping
        await asyncio.wait_for(own.close(), 5.0)
        assert conn.is_closed()
    finally:
        if not conn.is_closed():
            conn.terminate()
        own.terminate()


async def test_a_listener_that_cannot_attach_leaves_no_termination_callback_behind(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    added_to: list[Any] = []
    removed_from: list[Any] = []
    add = asyncpg.connection.Connection.add_termination_listener
    remove = asyncpg.connection.Connection.remove_termination_listener

    def adding(conn: Any, callback: Any) -> None:
        added_to.append(conn)
        add(conn, callback)

    def removing(conn: Any, callback: Any) -> None:
        removed_from.append(conn)
        remove(conn, callback)

    async def refused(conn: Any, channel: str, callback: Any) -> None:
        raise OSError("the connection was reset")

    monkeypatch.setattr(asyncpg.connection.Connection, "add_termination_listener", adding)
    monkeypatch.setattr(asyncpg.connection.Connection, "remove_termination_listener", removing)
    monkeypatch.setattr(asyncpg.connection.Connection, "add_listener", refused)

    async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        return None

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        return None

    with pytest.raises(StorageError, match="^postgres: storage rejected listen: the connection"):
        await storage.listen(on_event, on_signal)
    assert added_to and removed_from == added_to


async def test_probes_reach_no_callback(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(postgres, "PROBE", 0.2)
    _, signals = await storage._channels()
    probes: list[str] = []

    def probe(_conn: Any, _pid: int, _channel: str, text: str) -> None:
        if "probe" in json.loads(text):
            probes.append(text)

    heard: dict[str, list[dict[str, Any]]] = {"near": [], "far": []}

    def listener(name: str) -> Any:
        async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
            heard[name].append(payload)

        return on_signal

    async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        return None

    raw = await asyncpg.connect(URL, timeout=5)
    far_pool = await open_pool()
    stops: list[Callable[[], Awaitable[None]]] = []
    try:
        await raw.add_listener(signals, probe)
        with caplog.at_level(logging.WARNING):
            for name, store in (("near", storage), ("far", PostgresStorage(far_pool))):
                stops.append(await store.listen(on_event, listener(name)))
            await until(lambda: len(probes) >= 4)
            await storage.publish("acme", "t1", {"kind": "idle"})
            await until(lambda: bool(heard["near"] and heard["far"]))
        assert heard == {"near": [{"kind": "idle"}], "far": [{"kind": "idle"}]}
        assert [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR] == []
    finally:
        for stop in stops:
            await stop()
        await raw.close()
        await asyncio.wait_for(far_pool.close(), 5)


async def test_signals_and_events_cross_again_after_the_listeners_drop(
    replicas: tuple[Server, Server], pool: Any
) -> None:
    ana, helper = await room(*replicas)
    killed = await terminate(pool, 2)
    await say(ana, "t1", "during")
    await relistened(pool, killed, 2)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working", timeout=2.0))["label"] == "Reading"
    await say(ana, "t1", "after")
    messages = [p for p in await helper.quiet(0.5) if p["kind"] == "message"]
    assert [p["content"][0]["text"] for p in messages] == ["during", "after"]


async def test_replicas_whose_listeners_come_back_empty_the_rooms_of_a_thread_deleted_unheard(
    replicas: tuple[Server, Server], pool: Any, storage: PostgresStorage
) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    assert await storage.delete_thread("acme", "t1")
    killed = await terminate(pool, 2)
    await relistened(pool, killed, 2)
    assert (await ana.push("thread.deleted", timeout=5.0))["sender"]["id"] == "system"
    assert (await helper.push("thread.deleted", timeout=5.0))["sender"]["id"] == "system"
    await far.create_thread("acme", "t1")
    await far.add_member("acme", "t1", BRUNO)
    bruno = await Peer.open(far, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    for n in range(4):
        await say(bruno, "t1", f"new {n}")
    assert await ana.quiet(0.5) == []
    assert await helper.quiet(0.5) == []


async def test_a_membership_added_on_one_replica_reaches_a_follower_on_another(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    helper = await Peer.open(far, HELPER)
    await helper.call("thread.follow")
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await say(ana, "t1", "across")
    pushes = await helper.quiet(0.5)
    assert [j["thread"]["id"] for j in helper.joined] == ["t1"]
    assert [p["content"][0]["text"] for p in pushes if p["kind"] == "message"] == ["across"]


async def test_deployments_in_different_schemas_do_not_hear_each_other(
    pool: Any, storage: PostgresStorage
) -> None:
    other = f"{SCHEMA}_other"
    admin = await asyncpg.connect(URL, timeout=5)
    other_pool = sibling_pool = None
    stops = []
    try:
        await admin.execute(f"CREATE SCHEMA {other}")
        dsn = f"{URL}{'&' if '?' in URL else '?'}search_path={other}"
        other_pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4, timeout=5)
        sibling_pool = await open_pool()
        foreign = PostgresStorage(other_pool)
        sibling = PostgresStorage(sibling_pool)
        assert await foreign._channels() != await storage._channels()
        assert await sibling._channels() == await storage._channels()
        heard: dict[str, list[dict[str, Any]]] = {"near": [], "sibling": [], "foreign": []}
        events: list[int] = []

        def listener(name: str) -> Any:
            async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
                heard[name].append(payload)

            return on_signal

        async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
            events.append(seq)

        for name, store in (("near", storage), ("sibling", sibling), ("foreign", foreign)):
            stops.append(await store.listen(on_event, listener(name)))
        await storage.publish("acme", "t1", {"kind": "idle"})
        await until(lambda: bool(heard["near"] and heard["sibling"]))
        await asyncio.sleep(0.3)
        assert heard["near"] == heard["sibling"] == [{"kind": "idle"}]
        assert heard["foreign"] == []
        await foreign.publish("acme", "t1", {"kind": "working"})
        await asyncio.sleep(0.3)
        assert heard["foreign"] == [{"kind": "working"}]
        assert heard["near"] == [{"kind": "idle"}]
    finally:
        for stop in stops:
            await stop()
        for closing in (other_pool, sibling_pool):
            if closing is not None:
                await asyncio.wait_for(closing.close(), 5)
        await admin.execute(f"DROP SCHEMA IF EXISTS {other} CASCADE")
        await admin.close()


async def test_a_search_path_without_a_schema_is_a_storage_error() -> None:
    dsn = f"{URL}{'&' if '?' in URL else '?'}search_path={SCHEMA}_missing"
    missing = await asyncpg.create_pool(dsn, min_size=1, max_size=2, timeout=5)
    try:
        with pytest.raises(StorageError, match="^postgres: no schema on the search_path exists$"):
            await PostgresStorage(missing)._channels()
    finally:
        await asyncio.wait_for(missing.close(), 5)
