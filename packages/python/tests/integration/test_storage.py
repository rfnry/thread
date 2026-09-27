import asyncio
from datetime import timedelta
from typing import Any

import pytest
from unit.factories import ANA, AT, BRUNO, HELPER
from until import until

from rfnry_thread.errors import NotFoundError
from rfnry_thread.models import SYSTEM, Event, Member, Message, Read, Text, Thread
from rfnry_thread.storage import Storage


def thread(thread_id: str, scope: str = "acme", minutes: int = 0) -> Thread:
    return Thread(
        id=thread_id,
        scope=scope,
        metadata={"n": minutes},
        created_at=AT + timedelta(minutes=minutes),
    )


def seqs(events: list[Event]) -> list[int]:
    return [event.seq for event in events]


def say(thread_id: str, client_id: str, text: str = "hi", author: Any = ANA) -> Message:
    return Message(
        id=f"id-{client_id}-{text}",
        thread_id=thread_id,
        author=author,
        created_at=AT,
        client_id=client_id,
        content=[Text(text=text)],
    )


async def room(
    storage: Storage, thread_id: str = "t1", scope: str = "acme", *members: Any
) -> Thread:
    stored, _ = await storage.create_thread(scope, thread(thread_id, scope))
    for identity in members or (ANA,):
        await storage.add_member(
            scope, Member(thread_id=thread_id, identity=identity, added_at=AT, added_by=SYSTEM)
        )
    return stored


async def test_create_is_get_or_create(storage: Storage) -> None:
    first, created = await storage.create_thread("acme", thread("t1"))
    again, created_again = await storage.create_thread("acme", thread("t1", minutes=5))
    assert (created, created_again) == (True, False)
    assert again == first
    assert await storage.get_thread("acme", "t1") == first
    assert await storage.get_thread("acme", "ghost") is None


async def test_threads_are_listed_for_a_member_newest_first_in_pages(storage: Storage) -> None:
    for minutes, thread_id in enumerate(["a", "b", "c", "d"]):
        await storage.create_thread("acme", thread(thread_id, minutes=minutes))
        if thread_id != "c":
            await storage.add_member(
                "acme", Member(thread_id=thread_id, identity=ANA, added_at=AT, added_by=SYSTEM)
            )
    assert [t.id for t in await storage.list_threads("acme", "ana")] == ["d", "b", "a"]
    assert [t.id for t in await storage.list_threads("acme", "ana", limit=2)] == ["d", "b"]
    assert [t.id for t in await storage.list_threads("acme", "ana", before="b")] == ["a"]
    assert await storage.list_threads("acme", "bruno") == []


async def test_update_and_delete_take_members_and_events_along(storage: Storage) -> None:
    await room(storage)
    await storage.append_event("acme", say("t1", "c1"))
    updated = await storage.update_thread("acme", "t1", {"topic": "refunds"})
    assert updated is not None and updated.metadata == {"topic": "refunds"}
    assert await storage.update_thread("acme", "ghost", {}) is None
    assert await storage.delete_thread("acme", "t1") is True
    assert await storage.delete_thread("acme", "t1") is False
    assert await storage.list_members("acme", "t1") == []
    assert await storage.list_events("acme", "t1") == []
    assert await storage.verify_absent("acme")


async def test_members_are_added_once_and_removed(storage: Storage) -> None:
    await room(storage)
    member, created = await storage.add_member(
        "acme", Member(thread_id="t1", identity=BRUNO, added_at=AT, added_by=ANA)
    )
    again, created_again = await storage.add_member(
        "acme", Member(thread_id="t1", identity=BRUNO, added_at=AT + timedelta(1), added_by=HELPER)
    )
    assert (created, created_again) == (True, False) and again == member
    assert [m.identity.id for m in await storage.list_members("acme", "t1")] == ["ana", "bruno"]
    assert await storage.is_member("acme", "t1", "bruno")
    assert await storage.remove_member("acme", "t1", "bruno") is True
    assert await storage.remove_member("acme", "t1", "bruno") is False
    assert not await storage.is_member("acme", "t1", "bruno")
    with pytest.raises(NotFoundError, match=r"^\w+: thread 'ghost' was not found$"):
        await storage.add_member(
            "acme", Member(thread_id="ghost", identity=BRUNO, added_at=AT, added_by=ANA)
        )


async def test_seq_is_thread_local_gapless_and_dedup_answers_with_the_stored_event(
    storage: Storage,
) -> None:
    await room(storage, "t1")
    await room(storage, "t2")
    first, created = await storage.append_event("acme", say("t1", "c1", "hello"))
    repeat, created_again = await storage.append_event("acme", say("t1", "c1", "hello, again"))
    other, _ = await storage.append_event("acme", say("t2", "c1"))
    second, _ = await storage.append_event("acme", say("t1", "c2"))
    assert (created, created_again) == (True, False)
    assert (
        repeat == first and isinstance(repeat, Message) and repeat.content == [Text(text="hello")]
    )
    assert (first.seq, second.seq, other.seq) == (1, 2, 1)
    bruno, _ = await storage.append_event("acme", say("t1", "c1", author=BRUNO))
    assert bruno.seq == 3
    with pytest.raises(NotFoundError):
        await storage.append_event("acme", say("ghost", "c1"))


async def test_a_message_keeps_its_metadata(storage: Storage) -> None:
    await room(storage)
    metadata = {"run_id": "r1", "attempt": 2, "reviewed": False, "tags": ["refund"]}
    sent = say("t1", "c1").model_copy(update={"metadata": metadata})
    stored, _ = await storage.append_event("acme", sent)
    assert isinstance(stored, Message) and stored.metadata == metadata
    listed = (await storage.list_events("acme", "t1"))[0]
    fetched = await storage.get_event("acme", "t1", stored.id)
    assert isinstance(listed, Message) and listed.metadata == metadata
    assert isinstance(fetched, Message) and fetched.metadata == metadata


async def test_concurrent_duplicates_store_one_event_and_seq_stays_gapless(
    storage: Storage,
) -> None:
    await room(storage)
    same = [storage.append_event("acme", say("t1", "dup")) for _ in range(20)]
    distinct = [storage.append_event("acme", say("t1", f"c{n}")) for n in range(20)]
    results = await asyncio.gather(*same, *distinct)
    assert sum(created for _, created in results[:20]) == 1
    assert len({event.id for event, _ in results[:20]}) == 1
    assert [e.seq for e in await storage.list_events("acme", "t1", limit=100)] == list(range(1, 22))


async def test_list_events_pages_both_ways_and_filters_kinds(storage: Storage) -> None:
    await room(storage)
    for n in range(1, 11):
        await storage.append_event("acme", say("t1", f"c{n}"))
    await storage.append_event(
        "acme", Read(id="r", thread_id="t1", author=ANA, created_at=AT, client_id="r", up_to_seq=10)
    )
    assert seqs(await storage.list_events("acme", "t1", limit=3)) == [9, 10, 11]
    assert seqs(await storage.list_events("acme", "t1", since=2, limit=3)) == [3, 4, 5]
    assert seqs(await storage.list_events("acme", "t1", before=5, limit=3)) == [2, 3, 4]
    assert seqs(await storage.list_events("acme", "t1", since=2, before=5)) == [3, 4]
    assert seqs(await storage.list_events("acme", "t1", kinds=["read"])) == [11]
    assert (await storage.get_event("acme", "t1", "r")) is not None
    assert await storage.get_event("acme", "t1", "ghost") is None


async def test_the_same_thread_id_in_two_scopes_is_two_threads(storage: Storage) -> None:
    await room(storage, "t1", "tenant-a")
    await room(storage, "t1", "tenant-b")
    await storage.append_event("tenant-a", say("t1", "c1", "MARKER-A"))
    assert await storage.list_events("tenant-b", "t1") == []
    assert await storage.get_event("tenant-b", "t1", "id-c1-MARKER-A") is None
    assert await storage.list_threads("tenant-b", "ana") == [
        await storage.get_thread("tenant-b", "t1")
    ]
    assert await storage.delete_scope("tenant-a") == 3
    assert await storage.verify_absent("tenant-a")
    assert not await storage.verify_absent("tenant-b")


async def test_listeners_hear_new_events_and_published_signals(storage: Storage) -> None:
    await room(storage)
    events: list[tuple[str, str, int]] = []
    signals: list[tuple[str, str, dict[str, Any]]] = []

    async def on_event(scope: str, thread_id: str, seq: int) -> None:
        events.append((scope, thread_id, seq))

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        signals.append((scope, thread_id, payload))

    stop = await storage.listen(on_event, on_signal)
    await storage.append_event("acme", say("t1", "c1"))
    await storage.append_event("acme", say("t1", "c1"))
    await storage.publish("acme", "t1", {"o": "r1", "kind": "idle"})
    await until(lambda: len(events) == 1 and len(signals) == 1)
    assert events == [("acme", "t1", 1)]
    assert signals == [("acme", "t1", {"o": "r1", "kind": "idle"})]
    await stop()
    await storage.append_event("acme", say("t1", "c2"))
    await asyncio.sleep(0.1)
    assert len(events) == 1


def test_it_is_a_storage(storage: Storage) -> None:
    assert isinstance(storage, Storage)
