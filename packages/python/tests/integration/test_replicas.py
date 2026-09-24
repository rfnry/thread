import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
from unit.conftest import say
from unit.factories import ANA, HELPER
from unit.peer import Peer
from until import until

from rfnry_thread.models import Message, Text, Thread
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server

from .conftest import fresh, postgres_pool

pytestmark = pytest.mark.postgres


async def listeners(pool: Any) -> list[int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT pid FROM pg_stat_activity WHERE datname = current_database() "
            "AND pid <> pg_backend_pid() AND query = 'LISTEN \"thread_signals\"'"
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
async def replicas() -> AsyncIterator[tuple[Server, Server]]:
    near_pool, far_pool = await postgres_pool(), await postgres_pool()
    near = Server(await fresh(near_pool))
    far = Server(PostgresStorage(far_pool))
    await near.start()
    await far.start()
    yield near, far
    await near.stop()
    await far.stop()
    await near_pool.close()
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


async def test_removal_cuts_off_a_member_on_another_replica(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    await say(ana, "t1", "after")
    kinds = [p["kind"] for p in await helper.quiet(0.5)]
    assert "member_removed" in kinds and "message" not in kinds


async def test_a_dropped_listener_listens_again_and_resumes() -> None:
    pool = await postgres_pool()
    storage = await fresh(pool)
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
    await pool.close()


async def test_signals_and_events_cross_again_after_the_listeners_drop(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    pool = await postgres_pool()
    killed = await terminate(pool, 2)
    await say(ana, "t1", "during")
    await relistened(pool, killed, 2)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working", timeout=2.0))["label"] == "Reading"
    await say(ana, "t1", "after")
    messages = [p for p in await helper.quiet(0.5) if p["kind"] == "message"]
    assert [p["content"][0]["text"] for p in messages] == ["during", "after"]
    await pool.close()
