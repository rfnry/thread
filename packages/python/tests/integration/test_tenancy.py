from datetime import UTC, datetime

from unit.conftest import say
from unit.factories import ANA
from unit.peer import Peer

from rfnry_thread.models import Thread
from rfnry_thread.server import Server
from rfnry_thread.storage import Storage


async def test_nothing_of_a_is_reachable_from_b(storage: Storage) -> None:
    server = Server(storage)
    a = await Peer.open(server, ANA, scope="tenant-a")
    b = await Peer.open(server, ANA, scope="tenant-b")
    for peer in (a, b):
        await peer.call("thread.create", thread_id="t1")
        await peer.call("thread.join", thread_id="t1")
    await say(a, "t1", "MARKER-A")
    assert all("MARKER-A" not in str(p) for p in await b.quiet())
    assert all(
        "MARKER-A" not in str(e) for e in (await b.call("thread.history", thread_id="t1"))["events"]
    )
    assert all("MARKER-A" not in str(t) for t in (await b.call("thread.list"))["threads"])


async def test_erasure_is_provable_and_falsifiable(storage: Storage) -> None:
    server = Server(storage)
    await server.create_thread("tenant-a", thread_id="t1")
    await server.add_member("tenant-a", "t1", ANA)
    await server.create_thread("tenant-b", thread_id="t1")
    assert not await server.verify_absent("tenant-a")
    assert await server.delete_scope("tenant-a") == 3
    assert await server.verify_absent("tenant-a")
    assert not await server.verify_absent("tenant-b")
    await storage.create_thread(
        "tenant-a", Thread(id="orphan", scope="tenant-a", created_at=datetime.now(UTC))
    )
    assert not await server.verify_absent("tenant-a")


async def test_the_server_lists_only_the_scope_it_is_asked_for(storage: Storage) -> None:
    server = Server(storage)
    await server.create_thread("tenant-a", thread_id="t1", metadata={"elevated": True})
    await server.create_thread(
        "tenant-b", thread_id="t1", metadata={"elevated": True, "marker": "B"}
    )
    await server.create_thread("tenant-b", thread_id="t2", metadata={"elevated": True})
    listed = await server.list_threads("tenant-a", metadata={"elevated": True})
    assert [(t.scope, t.id, t.metadata) for t in listed] == [("tenant-a", "t1", {"elevated": True})]
    assert await server.list_threads("tenant-a", metadata={"marker": "B"}) == []
