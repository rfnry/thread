import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import asyncpg
import pytest
from database import SCHEMA, URL, open_pool
from unit.conftest import say
from unit.factories import ANA, HELPER
from unit.peer import Peer
from until import until

from rfnry_thread.errors import StorageError
from rfnry_thread.models import Identity, Message, Part, Text, Thread
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
    seen: list[int] = []
    heard: list[dict[str, Any]] = []
    resumed = asyncio.Event()

    async def on_event(scope: str, thread_id: str, seq: int) -> None:
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
    await storage.append_event(
        "acme",
        Message(
            id="m1",
            thread_id="t1",
            author=ANA,
            created_at=datetime.now(UTC),
            client_id="c1",
            content=[Text(text="back")],
        ),
    )
    await until(lambda: heard == [{"kind": "idle"}] and seen == [1])
    await stop()
    assert await listeners(pool) == []


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

        async def on_event(scope: str, thread_id: str, seq: int) -> None:
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
