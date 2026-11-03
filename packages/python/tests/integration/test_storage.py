import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import pytest
from database import Schemas
from unit.factories import ANA, AT, BRUNO, HELPER, added, removed, updated
from unit.writes import admit, append, expel, revise
from until import until

from integration.pools import Counting, Refusing, Stalling
from rfnry_thread import migrations, postgres
from rfnry_thread.errors import ForbiddenError, NotFoundError, StorageError
from rfnry_thread.models import (
    SYSTEM,
    Edit,
    Event,
    Identity,
    Message,
    Read,
    Retract,
    Text,
    Thread,
)
from rfnry_thread.postgres import PostgresStorage
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


async def epoch_of(storage: Storage, scope: str = "acme", thread_id: str = "t1") -> int:
    found = await storage.get_thread(scope, thread_id)
    assert found is not None
    return found.epoch


async def room(
    storage: Storage, thread_id: str = "t1", scope: str = "acme", *members: Any
) -> Thread:
    stored, _ = await storage.create_thread(scope, thread(thread_id, scope))
    for identity in members or (ANA,):
        await admit(storage, scope, added(thread_id, identity))
    return stored


async def test_create_is_get_or_create(storage: Storage) -> None:
    first, created = await storage.create_thread("acme", thread("t1"))
    again, created_again = await storage.create_thread("acme", thread("t1", minutes=5))
    assert (created, created_again) == (True, False)
    assert again == first
    assert await storage.get_thread("acme", "t1") == first
    assert await storage.get_thread("acme", "ghost") is None


async def test_concurrent_creates_of_one_thread_make_it_once_with_one_creator(
    storage: Storage,
) -> None:
    people = [Identity(role="user", id=f"p{n}") for n in range(8)]
    ids = [f"t{n}" for n in range(5)]
    results = await asyncio.gather(
        *(storage.create_thread("acme", thread(t), added(t, who)) for t in ids for who in people)
    )
    for n, thread_id in enumerate(ids):
        mine = results[n * len(people) : (n + 1) * len(people)]
        [winner] = [who for (_, created), who in zip(mine, people, strict=True) if created]
        stored = await storage.get_thread("acme", thread_id)
        assert all(found == stored for found, _ in mine)
        members = await storage.list_members("acme", thread_id)
        assert [m.identity.id for m in members] == [winner.id]
        assert seqs(await storage.list_events("acme", thread_id)) == [1]


async def test_threads_are_listed_for_a_member_newest_first_in_pages(storage: Storage) -> None:
    for minutes, thread_id in enumerate(["a", "b", "c", "d"]):
        await storage.create_thread("acme", thread(thread_id, minutes=minutes))
        if thread_id != "c":
            await admit(storage, "acme", added(thread_id, ANA))
    assert [t.id for t in await storage.list_threads("acme", "ana")] == ["d", "b", "a"]
    assert [t.id for t in await storage.list_threads("acme", "ana", limit=2)] == ["d", "b"]
    assert [t.id for t in await storage.list_threads("acme", "ana", before="b")] == ["a"]
    assert await storage.list_threads("acme", "bruno") == []


async def test_a_member_cannot_anchor_on_a_thread_it_is_not_in(storage: Storage) -> None:
    await storage.create_thread("acme", thread("ana-only", minutes=0))
    await admit(storage, "acme", added("ana-only", ANA))
    await storage.create_thread("acme", thread("bruno-only", minutes=1))
    await admit(storage, "acme", added("bruno-only", BRUNO))
    with pytest.raises(NotFoundError, match="postgres: thread 'bruno-only' was not found"):
        await storage.list_threads("acme", "ana", before="bruno-only")
    assert [t.id for t in await storage.list_threads("acme", None, before="bruno-only")] == [
        "ana-only"
    ]


async def test_threads_are_listed_by_metadata_for_anyone_or_for_a_member(
    storage: Storage,
) -> None:
    catalog: list[tuple[str, dict[str, Any]]] = [
        ("a", {"elevated": True, "location": "reno", "machine": {"id": "press-4", "line": 2}}),
        ("b", {"elevated": "true", "location": "reno"}),
        ("c", {"elevated": True, "location": "austin", "tags": ["leak", "urgent"]}),
        ("d", {"elevated": False, "count": 1, "ratio": 1.0}),
        ("e", {}),
    ]
    for minutes, (thread_id, metadata) in enumerate(catalog):
        await storage.create_thread(
            "acme",
            Thread(
                id=thread_id,
                scope="acme",
                metadata=metadata,
                created_at=AT + timedelta(minutes=minutes),
            ),
        )
    await storage.create_thread(
        "globex", Thread(id="g", scope="globex", metadata={"elevated": True}, created_at=AT)
    )
    for thread_id in ("a", "b"):
        await admit(storage, "acme", added(thread_id, ANA))

    async def listed(member_id: str | None = None, **query: Any) -> list[str]:
        return [t.id for t in await storage.list_threads("acme", member_id, **query)]

    assert await listed() == ["e", "d", "c", "b", "a"]
    assert await listed(metadata={}) == ["e", "d", "c", "b", "a"]
    assert await listed(metadata={"elevated": True}) == ["c", "a"]
    assert await listed(metadata={"elevated": "true"}) == ["b"]
    assert await listed(metadata={"location": "reno", "machine": {"line": 2}}) == ["a"]
    assert await listed(metadata={"machine": {"id": "press-4", "line": 3}}) == []
    assert await listed(metadata={"tags": ["urgent"]}) == ["c"]
    assert await listed(metadata={"tags": "urgent"}) == []
    assert await listed(metadata={"count": True}) == []
    assert await listed(metadata={"count": 1.0, "ratio": 1}) == ["d"]
    assert await listed(metadata={"elevated": True}, limit=1) == ["c"]
    assert await listed(metadata={"elevated": True}, before="c") == ["a"]
    with pytest.raises(NotFoundError, match="^postgres: thread 'ghost' was not found$"):
        await listed(metadata={"elevated": True}, before="ghost")
    assert await listed("ana") == ["b", "a"]
    assert await listed("ana", metadata={"elevated": True}) == ["a"]
    assert [t.id for t in await storage.list_threads("globex", None)] == ["g"]


async def test_update_and_delete_take_members_and_events_along(storage: Storage) -> None:
    await room(storage)
    await append(storage, "acme", say("t1", "c1"))
    result = await revise(storage, "acme", updated("t1", {"topic": "refunds"}))
    assert result is not None and result[0].metadata == {"topic": "refunds"}
    assert result[1].metadata == {"topic": "refunds"} and result[1].seq > 0
    assert await revise(storage, "acme", updated("ghost", {})) is None
    epoch = await epoch_of(storage)
    assert await storage.delete_thread("acme", "t1") == epoch
    assert await storage.delete_thread("acme", "t1") is None
    assert await storage.list_members("acme", "t1") == []
    assert await storage.list_events("acme", "t1") == []
    assert await storage.verify_absent("acme")


async def test_members_are_added_once_and_removed(storage: Storage) -> None:
    await room(storage)
    member, event = await admit(storage, "acme", added("t1", BRUNO, ANA))
    again, event_again = await admit(storage, "acme", added("t1", BRUNO, HELPER))
    assert event is not None and event_again is None and again == member
    assert [m.identity.id for m in await storage.list_members("acme", "t1")] == ["ana", "bruno"]
    assert await storage.is_member("acme", "t1", "bruno")
    assert await expel(storage, "acme", removed("t1", BRUNO)) is not None
    assert await expel(storage, "acme", removed("t1", BRUNO)) is None
    assert not await storage.is_member("acme", "t1", "bruno")
    with pytest.raises(NotFoundError, match=r"^\w+: thread 'ghost' was not found$"):
        await admit(storage, "acme", added("ghost", BRUNO, ANA))


async def test_seq_is_thread_local_gapless_and_dedup_answers_with_the_stored_event(
    storage: Storage,
) -> None:
    await room(storage, "t1")
    await room(storage, "t2")
    first, created = await append(storage, "acme", say("t1", "c1", "hello"))
    repeat, created_again = await append(storage, "acme", say("t1", "c1", "hello, again"))
    other, _ = await append(storage, "acme", say("t2", "c1"))
    second, _ = await append(storage, "acme", say("t1", "c2"))
    assert (created, created_again) == (True, False)
    assert (
        repeat == first and isinstance(repeat, Message) and repeat.content == [Text(text="hello")]
    )
    assert (first.seq, second.seq, other.seq) == (2, 3, 2)
    await admit(storage, "acme", added("t1", BRUNO))
    bruno, _ = await append(storage, "acme", say("t1", "c1", author=BRUNO))
    assert bruno.seq == 5
    with pytest.raises(NotFoundError):
        await append(storage, "acme", say("ghost", "c1"))


async def test_the_log_refuses_an_author_who_is_not_a_member(storage: Storage) -> None:
    await room(storage)
    with pytest.raises(ForbiddenError, match=r"^postgres: 'bruno' is not a member of thread 't1'$"):
        await append(storage, "acme", say("t1", "c1", author=BRUNO))
    stored, created = await append(storage, "acme", say("t1", "c2", author=SYSTEM))
    assert (created, stored.author.id) == (True, SYSTEM.id)
    assert seqs(await storage.list_events("acme", "t1")) == [1, 2]


async def test_a_message_keeps_its_metadata(storage: Storage) -> None:
    await room(storage)
    metadata = {"run_id": "r1", "attempt": 2, "reviewed": False, "tags": ["refund"]}
    sent = say("t1", "c1").model_copy(update={"metadata": metadata})
    stored, _ = await append(storage, "acme", sent)
    assert isinstance(stored, Message) and stored.metadata == metadata
    listed = (await storage.list_events("acme", "t1", kinds=["message"]))[0]
    fetched = await storage.get_event("acme", "t1", stored.id)
    assert isinstance(listed, Message) and listed.metadata == metadata
    assert isinstance(fetched, Message) and fetched.metadata == metadata


async def test_an_event_is_found_by_its_author_and_client_id(storage: Storage) -> None:
    await room(storage, "t1")
    await room(storage, "t1", "tenant-b")
    sent, _ = await append(storage, "acme", say("t1", "c1"))
    assert await storage.find_event("acme", "t1", "ana", "c1") == sent
    assert await storage.find_event("acme", "t1", "bruno", "c1") is None
    assert await storage.find_event("acme", "t1", "ana", "c2") is None
    assert await storage.find_event("tenant-b", "t1", "ana", "c1") is None


async def test_concurrent_duplicates_store_one_event_and_seq_stays_gapless(
    storage: Storage,
) -> None:
    await room(storage)
    same = [append(storage, "acme", say("t1", "dup")) for _ in range(20)]
    distinct = [append(storage, "acme", say("t1", f"c{n}")) for n in range(20)]
    results = await asyncio.gather(*same, *distinct)
    assert sum(created for _, created in results[:20]) == 1
    assert len({event.id for event, _ in results[:20]}) == 1
    assert [e.seq for e in await storage.list_events("acme", "t1", limit=100)] == list(range(1, 23))


async def test_list_events_pages_both_ways_and_filters_kinds(storage: Storage) -> None:
    await room(storage)
    for n in range(1, 11):
        await append(storage, "acme", say("t1", f"c{n}"))
    await append(
        storage,
        "acme",
        Read(id="r", thread_id="t1", author=ANA, created_at=AT, client_id="r", up_to_seq=10),
    )
    assert seqs(await storage.list_events("acme", "t1", limit=3)) == [10, 11, 12]
    assert seqs(await storage.list_events("acme", "t1", since=2, limit=3)) == [3, 4, 5]
    assert seqs(await storage.list_events("acme", "t1", before=5, limit=3)) == [2, 3, 4]
    assert seqs(await storage.list_events("acme", "t1", since=2, before=5)) == [3, 4]
    assert seqs(await storage.list_events("acme", "t1", kinds=["read"])) == [12]
    assert (await storage.get_event("acme", "t1", "r")) is not None
    assert await storage.get_event("acme", "t1", "ghost") is None


async def test_a_head_is_the_last_seq_for_a_member_and_none_for_anyone_else(
    storage: Storage,
) -> None:
    await room(storage, "t1")
    await room(storage, "t1", "globex")
    for client_id in ("c1", "c2"):
        await append(storage, "acme", say("t1", client_id))
    assert await storage.head("acme", "t1", "ana") == (3, 0, await epoch_of(storage))
    assert await storage.head("globex", "t1", "ana") == (1, 0, await epoch_of(storage, "globex"))
    assert await storage.head("acme", "t1", "bruno") is None
    assert await storage.head("acme", "ghost", "ana") is None


async def test_the_same_thread_id_in_two_scopes_is_two_threads(storage: Storage) -> None:
    await room(storage, "t1", "tenant-a")
    await room(storage, "t1", "tenant-b")
    await append(storage, "tenant-a", say("t1", "c1", "MARKER-A"))
    assert [e.kind for e in await storage.list_events("tenant-b", "t1")] == ["member_added"]
    assert await storage.get_event("tenant-b", "t1", "id-c1-MARKER-A") is None
    await revise(storage, "tenant-a", updated("t1", {"tenant": "a"}))
    await revise(storage, "tenant-b", updated("t1", {"tenant": "b"}))
    [listed] = await storage.list_threads("tenant-b", "ana")
    assert (listed.scope, listed.id, listed.metadata, listed.created_at, listed.last_seq) == (
        "tenant-b",
        "t1",
        {"tenant": "b"},
        AT,
        2,
    )
    assert await storage.list_threads("tenant-b", "ana", query="marker") == []
    assert await storage.list_threads("tenant-b", None, query="marker") == []
    assert await storage.search("tenant-b", "t1", "marker") == []
    assert [m.id for m in await storage.search("tenant-a", "t1", "marker")] == ["id-c1-MARKER-A"]
    assert await storage.delete_scope("tenant-a") == 8
    assert await storage.verify_absent("tenant-a")
    assert not await storage.verify_absent("tenant-b")


async def test_listeners_hear_hints_and_published_signals(storage: Storage) -> None:
    events: list[tuple[str, str, int, int]] = []
    signals: list[tuple[str, str, dict[str, Any]]] = []

    async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        events.append((scope, thread_id, seq, epoch))

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        signals.append((scope, thread_id, payload))

    stop = await storage.listen(on_event, on_signal)
    await storage.hint("acme", "t1", 2, 7)
    await storage.publish("acme", "t1", {"o": "r1", "kind": "idle"})
    await until(lambda: len(events) == 1 and len(signals) == 1)
    assert events == [("acme", "t1", 2, 7)]
    assert signals == [("acme", "t1", {"o": "r1", "kind": "idle"})]
    await stop()
    await storage.hint("acme", "t1", 3, 7)
    await storage.publish("acme", "t1", {"o": "r1", "kind": "working"})
    await asyncio.sleep(0.1)
    assert (len(events), len(signals)) == (1, 1)


async def hearing(storage: Storage) -> tuple[list[Any], Callable[[], Awaitable[None]]]:
    heard: list[Any] = []

    async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        heard.append((thread_id, seq, epoch))

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        heard.append(payload)

    return heard, await storage.listen(on_event, on_signal)


async def test_a_write_commits_no_notice(storage: Storage) -> None:
    heard, stop = await hearing(storage)
    try:
        await storage.create_thread("acme", thread("t1"), added("t1", ANA))
        await admit(storage, "acme", added("t1", BRUNO))
        await append(storage, "acme", say("t1", "c1"))
        await revise(storage, "acme", updated("t1", {"topic": "refunds"}))
        await expel(storage, "acme", removed("t1", BRUNO))
        assert seqs(await storage.list_events("acme", "t1", since=0)) == [1, 2, 3, 4, 5]
        await asyncio.sleep(0.3)
        assert heard == []
        epoch = await epoch_of(storage)
        await storage.hint("acme", "t1", 5, epoch)
        await until(lambda: heard == [("t1", 5, epoch)])
    finally:
        await stop()


async def waves(sender: Storage, count: int, size: int = 10) -> None:
    sent: list[asyncio.Future[None]] = []
    for n in range(count):
        if n and n % size == 0:
            await asyncio.sleep(0)
        sent.append(asyncio.ensure_future(sender.publish("acme", "t1", {"n": n})))
    await asyncio.wait_for(asyncio.gather(*sent), 5.0)


async def warmed(pool: Any, storage: Storage) -> tuple[Counting, PostgresStorage, list[Any], Any]:
    heard, stop = await hearing(storage)
    counting = Counting(pool)
    sender = PostgresStorage(counting)
    await sender.publish("acme", "t1", {"n": -1})
    await until(lambda: heard == [{"n": -1}])
    heard.clear()
    counting.acquired = 0
    return counting, sender, heard, stop


async def test_concurrent_notices_share_transactions(pool: Any, storage: Storage) -> None:
    counting, sender, heard, stop = await warmed(pool, storage)
    try:
        await waves(sender, 50)
        assert 1 < counting.acquired < 10
        await until(lambda: len(heard) == 50)
        assert sorted(p["n"] for p in heard) == list(range(50))
    finally:
        await stop()


async def test_one_replicas_notices_arrive_in_the_order_sent(pool: Any, storage: Storage) -> None:
    counting, sender, heard, stop = await warmed(pool, storage)
    try:
        await waves(sender, 100)
        assert counting.acquired > 1
        await until(lambda: len(heard) == 100)
        assert [p["n"] for p in heard] == list(range(100))
    finally:
        await stop()


async def test_the_first_notices_of_a_storage_arrive_in_the_order_sent(
    pool: Any, storage: Storage
) -> None:
    heard: list[int] = []

    async def on_event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
        heard.append(seq)

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        heard.append(payload["n"])

    stop = await storage.listen(on_event, on_signal)
    counting = Counting(pool)
    sender = PostgresStorage(counting)
    try:
        sent = [
            asyncio.ensure_future(
                sender.hint("acme", "t1", n, 1) if n % 2 else sender.publish("acme", "t1", {"n": n})
            )
            for n in range(40)
        ]
        await asyncio.wait_for(asyncio.gather(*sent), 5.0)
        await until(lambda: len(heard) == 40)
        assert heard == list(range(40))
        assert counting.acquired < 10
    finally:
        await stop()


async def test_a_stuck_group_fails_and_the_next_one_is_sent(
    pool: Any, storage: Storage, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(postgres, "NOTIFY_TIMEOUT", 0.2)
    heard, stop = await hearing(storage)
    stalling = Stalling(pool)
    sender = PostgresStorage(stalling)
    try:
        await sender.publish("acme", "t1", {"n": -1})
        await until(lambda: heard == [{"n": -1}])
        stalling.stall()
        first = asyncio.ensure_future(sender.publish("acme", "t1", {"n": 0}))
        await until(lambda: stalling.holding)
        stalling.stall()
        pair = [asyncio.ensure_future(sender.publish("acme", "t1", {"n": n})) for n in (1, 2)]
        failed = await asyncio.wait_for(asyncio.gather(first, *pair, return_exceptions=True), 2.0)
        assert [type(error) for error in failed] == [StorageError] * 3
        assert {str(error) for error in failed} == {
            "postgres: storage rejected publish: no answer within 0.2 s"
        }
        await asyncio.wait_for(sender.publish("acme", "t1", {"n": 3}), 2.0)
        await until(lambda: len(heard) == 2)
        assert heard == [{"n": -1}, {"n": 3}]
    finally:
        stalling.resume()
        await stop()


async def test_a_cancelled_flush_leaves_no_notice_waiting(pool: Any, storage: Storage) -> None:
    heard, stop = await hearing(storage)
    stalling = Stalling(pool)
    sender = PostgresStorage(stalling)
    try:
        await sender.publish("acme", "t1", {"n": -1})
        await until(lambda: heard == [{"n": -1}])
        stalling.stall()
        taken = asyncio.ensure_future(sender.publish("acme", "t1", {"n": 0}))
        await until(lambda: stalling.holding)
        queued = asyncio.ensure_future(sender.publish("acme", "t1", {"n": 1}))
        await asyncio.sleep(0)
        assert sender._flushing is not None
        sender._flushing.cancel()
        ended = await asyncio.wait_for(asyncio.gather(taken, queued, return_exceptions=True), 2.0)
        assert [type(error) for error in ended] == [asyncio.CancelledError] * 2
        await sender.publish("acme", "t1", {"n": 2})
        await until(lambda: len(heard) == 2)
        assert heard == [{"n": -1}, {"n": 2}]
    finally:
        stalling.resume()
        await stop()


async def test_a_flush_cancelled_before_its_first_step_leaves_no_notice_waiting(
    pool: Any, storage: Storage
) -> None:
    heard, stop = await hearing(storage)
    sender = PostgresStorage(pool)
    try:
        await sender.publish("acme", "t1", {"n": -1})
        await until(lambda: heard == [{"n": -1}])
        caller = asyncio.ensure_future(sender.publish("acme", "t1", {"n": 0}))
        await asyncio.sleep(0)
        assert sender._flushing is not None
        sender._flushing.cancel()
        ended = await asyncio.wait_for(asyncio.gather(caller, return_exceptions=True), 1.0)
        assert [type(error) for error in ended] == [asyncio.CancelledError]
        await sender.publish("acme", "t1", {"n": 1})
        await until(lambda: len(heard) == 2)
        assert heard == [{"n": -1}, {"n": 1}]
    finally:
        await stop()


async def test_a_notice_sent_while_the_channel_names_are_read_keeps_its_place(
    pool: Any, storage: Storage
) -> None:
    heard, stop = await hearing(storage)
    sender = PostgresStorage(pool)

    async def latecomer() -> None:
        while sender._names is None:
            await asyncio.sleep(0)
        await sender.publish("acme", "t1", {"n": 3})

    try:
        first = [asyncio.ensure_future(sender.publish("acme", "t1", {"n": n})) for n in range(3)]
        await asyncio.sleep(0)
        await asyncio.wait_for(asyncio.gather(*first, latecomer()), 5.0)
        await until(lambda: len(heard) == 4)
        assert [p["n"] for p in heard] == [0, 1, 2, 3]
    finally:
        await stop()


async def test_a_send_refused_at_once_on_an_eager_loop_leaves_the_next_one_free(
    pool: Any, storage: Storage
) -> None:
    heard, stop = await hearing(storage)
    refusing = Refusing(pool)
    sender = PostgresStorage(refusing)
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)
    try:
        await sender.publish("acme", "t1", {"n": -1})
        await until(lambda: heard == [{"n": -1}])
        refusing.refuse = True
        with pytest.raises(
            StorageError, match="^postgres: storage rejected publish: the pool is closed$"
        ):
            await sender.publish("acme", "t1", {"n": 0})
        await asyncio.wait_for(sender.publish("acme", "t1", {"n": 1}), 2.0)
        await until(lambda: len(heard) == 2)
        assert heard == [{"n": -1}, {"n": 1}]
    finally:
        loop.set_task_factory(None)
        await stop()


def test_it_is_a_storage(storage: Storage) -> None:
    assert isinstance(storage, Storage)


async def test_a_mark_moves_forward_only_counts_what_others_said_after_it_and_says_if_it_moved(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    for n in range(3):
        await append(storage, "acme", say("t1", f"b{n}", author=BRUNO))
    await append(storage, "acme", say("t1", "a0", author=ANA))
    assert await storage.mark("acme", "t1", "ana", 3) == (3, 2, True)
    assert await storage.mark("acme", "t1", "ana", 0) == (3, 2, False)
    assert await storage.mark("acme", "t1", "ana", 3) == (3, 2, False)
    assert await storage.mark("acme", "t1", "ana", 6) == (6, 0, True)
    assert await storage.mark("acme", "t1", "bruno", 2) == (2, 1, True)


async def test_concurrent_marks_report_one_move(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    for n in range(5):
        await append(storage, "acme", say("t1", f"c{n}", author=BRUNO))
    for seq in range(1, 6):
        results = await asyncio.gather(*(storage.mark("acme", "t1", "ana", seq) for _ in range(5)))
        assert [moved for _, _, moved in results].count(True) == 1
    assert await storage.get_mark("acme", "t1", "ana") == 5


async def test_a_mark_moves_back_only_when_asked(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    for n in range(3):
        await append(storage, "acme", say("t1", f"b{n}", author=BRUNO))
    await append(storage, "acme", say("t1", "a0", author=ANA))
    assert await storage.mark("acme", "t1", "ana", 6) == (6, 0, True)
    assert await storage.mark("acme", "t1", "ana", 3) == (6, 0, False)
    assert await storage.mark("acme", "t1", "ana", 3, back=True) == (3, 2, True)
    assert await storage.mark("acme", "t1", "ana", 3, back=True) == (3, 2, False)
    assert await storage.mark("acme", "t1", "ana", 5, back=True) == (5, 0, True)
    assert await storage.mark("acme", "t1", "bruno", 4, back=True) == (4, 1, True)
    assert await storage.get_mark("acme", "t1", "ana") == 5


async def test_a_mark_needs_a_membership_and_leaves_with_it(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA)
    with pytest.raises(NotFoundError, match="'bruno' is not a member of thread 't1'$"):
        await storage.mark("acme", "t1", "bruno", 0)
    await admit(storage, "acme", added("t1", BRUNO))
    await append(storage, "acme", say("t1", "b1", author=BRUNO))
    assert await storage.mark("acme", "t1", "ana", 3) == (3, 0, True)
    await expel(storage, "acme", removed("t1", ANA))
    await admit(storage, "acme", added("t1", ANA))
    assert await storage.get_mark("acme", "t1", "ana") == 4
    assert await storage.mark("acme", "t1", "ana", 0, back=True) == (0, 1, True)


async def test_baseline_marks_set_the_last_seq_only_where_there_is_no_mark(
    storage: Storage, pool: Any
) -> None:
    await room(storage, "t1", "acme", ANA)
    await room(storage, "t2", "acme", ANA)
    await room(storage, "t3", "acme", BRUNO)
    for client_id in ("x", "y"):
        await append(storage, "acme", say("t1", client_id))
    await append(storage, "acme", say("t2", "z"))
    await storage.mark("acme", "t2", "ana", 0)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM thread_marks WHERE thread_id = 't1'")
    assert await storage.baseline_marks("acme", "ana") == 1
    assert await storage.baseline_marks("acme", "ana") == 0
    assert (await storage.mark("acme", "t1", "ana", 0))[0] == 3
    assert (await storage.mark("acme", "t2", "ana", 0))[0] == 0


async def test_baseline_marks_wait_out_a_removal_instead_of_failing_on_it(
    storage: Storage, pool: Any
) -> None:
    await room(storage, "t1", "acme", ANA)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM thread_marks WHERE thread_id = 't1'")
    async with pool.acquire() as holder:
        transaction = holder.transaction()
        await transaction.start()
        await holder.execute(
            "SELECT 1 FROM thread_threads WHERE scope = 'acme' AND id = 't1' FOR UPDATE"
        )
        await holder.execute("DELETE FROM thread_members WHERE thread_id = 't1'")
        baseline = asyncio.create_task(storage.baseline_marks("acme", "ana"))
        await asyncio.sleep(0.3)
        assert not baseline.done()
        await transaction.commit()
    assert await asyncio.wait_for(baseline, 5.0) == 0


async def test_an_append_waits_out_a_removal_and_then_refuses_the_removed_author(
    storage: Storage, pool: Any
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    async with pool.acquire() as holder:
        transaction = holder.transaction()
        await transaction.start()
        await holder.execute(
            "SELECT 1 FROM thread_threads WHERE scope = 'acme' AND id = 't1' FOR UPDATE"
        )
        await holder.execute(
            "DELETE FROM thread_members WHERE thread_id = 't1' AND identity_id = 'bruno'"
        )
        await holder.execute("UPDATE thread_threads SET last_seq = last_seq + 1 WHERE id = 't1'")
        appending = asyncio.create_task(append(storage, "acme", say("t1", "b1", author=BRUNO)))
        await asyncio.sleep(0.3)
        assert not appending.done()
        await transaction.commit()
    with pytest.raises(ForbiddenError, match="is not a member of thread"):
        await asyncio.wait_for(appending, 5.0)


async def test_a_creator_for_another_thread_is_refused_and_writes_nothing(
    storage: Storage,
) -> None:
    with pytest.raises(StorageError, match="^postgres: the creator belongs to another thread$"):
        await storage.create_thread("acme", thread("t1"), added("t2", ANA, ANA))
    assert await storage.get_thread("acme", "t1") is None
    assert await storage.list_events("acme", "t2") == []


async def test_a_mark_is_read_for_one_membership_and_is_none_without_one(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await room(storage, "t1", "other", ANA)
    await append(storage, "acme", say("t1", "x"))
    assert await storage.get_mark("acme", "t1", "ana") == 0
    await storage.mark("acme", "t1", "ana", 3)
    assert await storage.get_mark("acme", "t1", "ana") == 3
    assert await storage.get_mark("acme", "t1", "bruno") == 1
    assert await storage.get_mark("other", "t1", "ana") == 0
    assert await storage.get_mark("acme", "missing", "ana") is None
    await expel(storage, "acme", removed("t1", ANA))
    assert await storage.get_mark("acme", "t1", "ana") is None


async def test_a_deleted_scope_takes_its_marks_along(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA)
    await storage.mark("acme", "t1", "ana", 0)
    assert await storage.delete_scope("acme") == 5
    assert await storage.verify_absent("acme")


async def test_a_member_listing_carries_its_mark_and_what_others_said_after_it(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await append(storage, "acme", say("t1", "b1", author=BRUNO))
    await append(storage, "acme", say("t1", "a1", author=ANA))
    await append(storage, "acme", say("t1", "b2", author=BRUNO))
    [before] = await storage.list_threads("acme", "ana")
    assert (before.last_seq, before.marked, before.unread) == (5, 0, 2)
    await storage.mark("acme", "t1", "ana", 3)
    [after] = await storage.list_threads("acme", "ana")
    assert (after.marked, after.unread) == (3, 1)
    [anyone] = await storage.list_threads("acme", None)
    assert (anyone.last_seq, anyone.marked, anyone.unread) == (5, None, None)


async def test_unread_is_counted_up_to_100(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await storage.mark("acme", "t1", "ana", 0)
    for n in range(105):
        await append(storage, "acme", say("t1", f"b{n}", author=BRUNO))
    [listed] = await storage.list_threads("acme", "ana")
    assert listed.unread == 100


async def test_activity_order_follows_the_latest_message_and_pages(storage: Storage) -> None:
    for minutes, thread_id in enumerate(["a", "b", "c"]):
        await storage.create_thread("acme", thread(thread_id, minutes=minutes))
        await admit(storage, "acme", added(thread_id, ANA))
    late = say("a", "late").model_copy(update={"created_at": AT + timedelta(hours=1)})
    await append(storage, "acme", late)
    read = Read(
        id="r1",
        thread_id="b",
        author=ANA,
        created_at=AT + timedelta(hours=2),
        client_id="r1",
        up_to_seq=1,
    )
    await append(storage, "acme", read)

    async def listed(**query: Any) -> list[str]:
        threads = await storage.list_threads("acme", "ana", order="activity", **query)
        return [t.id for t in threads]

    assert await listed() == ["a", "c", "b"]
    assert await listed(before="a") == ["c", "b"]
    assert await listed(before="c", limit=1) == ["b"]
    assert [t.id for t in await storage.list_threads("acme", "ana")] == ["c", "b", "a"]
    [first] = await storage.list_threads("acme", "ana", order="activity", limit=1)
    assert first.last_message_at == AT + timedelta(hours=1)
    with pytest.raises(NotFoundError, match="^postgres: thread 'ghost' was not found$"):
        await listed(before="ghost")


@pytest.mark.parametrize("order", ["created", "activity"])
async def test_a_listed_thread_places_the_next_page_after_it_is_deleted_or_moves(
    storage: Storage, order: Any
) -> None:
    for minutes, thread_id in enumerate(["a", "b", "c", "d"]):
        await storage.create_thread("acme", thread(thread_id, minutes=minutes))
        await admit(storage, "acme", added(thread_id, ANA))
        later = AT + timedelta(hours=1, minutes=minutes)
        await append(
            storage, "acme", say(thread_id, thread_id).model_copy(update={"created_at": later})
        )

    async def listed(**query: Any) -> list[Thread]:
        return await storage.list_threads("acme", "ana", order=order, **query)

    first = await listed(limit=2)
    assert [t.id for t in first] == ["d", "c"]
    await storage.delete_thread("acme", "c")
    assert [t.id for t in await listed(before=first[-1])] == ["b", "a"]
    [anchor] = await listed(before=first[-1], limit=1)
    moved = say("b", "again").model_copy(update={"created_at": AT + timedelta(hours=2)})
    await append(storage, "acme", moved)
    assert [t.id for t in await listed(before=anchor)] == ["a"]


async def test_a_preview_shows_the_latest_message_with_its_edit_and_skips_a_retracted_one(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA)
    first, _ = await append(storage, "acme", say("t1", "c1", "first"))
    second, _ = await append(storage, "acme", say("t1", "c2", "second"))
    edit = Edit(
        id="ed",
        thread_id="t1",
        author=ANA,
        created_at=AT,
        client_id="c3",
        target=first.id,
        content=[Text(text="first, edited")],
    )
    await append(storage, "acme", edit)
    [plain] = await storage.list_threads("acme", "ana")
    assert plain.last_message is None
    assert "last_message" not in plain.model_dump(mode="json")
    [shown] = await storage.list_threads("acme", "ana", preview=True)
    assert shown.last_message is not None
    assert shown.last_message.content == [Text(text="second")]
    retract = Retract(
        id="rt", thread_id="t1", author=ANA, created_at=AT, client_id="c4", target=second.id
    )
    await append(storage, "acme", retract)
    [after] = await storage.list_threads("acme", "ana", preview=True)
    assert after.last_message is not None
    assert (after.last_message.id, after.last_message.content) == (
        first.id,
        [Text(text="first, edited")],
    )


async def test_a_preview_reads_the_last_five_messages_with_the_edits_and_retractions_after_them(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA)
    old, _ = await append(storage, "acme", say("t1", "c0", "old"))
    recent = [(await append(storage, "acme", say("t1", f"c{n}", f"m{n}")))[0] for n in range(1, 6)]

    def change(kind: Any, client_id: str, target: str, **fields: Any) -> Any:
        return kind(
            id=client_id,
            thread_id="t1",
            author=ANA,
            created_at=AT,
            client_id=client_id,
            target=target,
            **fields,
        )

    async def shown() -> Message | None:
        [thread] = await storage.list_threads("acme", "ana", preview=True)
        return thread.last_message

    await append(storage, "acme", change(Edit, "e0", old.id, content=[Text(text="old, edited")]))
    assert (await shown()) == recent[-1]
    for n, message in enumerate(recent[1:]):
        await append(storage, "acme", change(Retract, f"r{n}", message.id))
    edited = await shown()
    assert edited is not None and (edited.id, edited.content) == (recent[0].id, [Text(text="m1")])
    await append(storage, "acme", change(Retract, "r-last", recent[0].id))
    assert await shown() is None


async def test_a_kind_filter_reads_only_kinds_a_log_can_hold(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA)
    await append(storage, "acme", say("t1", "c1"))
    assert seqs(await storage.list_events("acme", "t1", kinds=["it's", "message"])) == [2]
    assert await storage.list_events("acme", "t1", kinds=["it's"]) == []
    assert await storage.list_events("acme", "t1", since=0, kinds=[]) == []


def changed(kind: Any, client_id: str, target: str, **fields: Any) -> Any:
    return kind(
        id=client_id,
        thread_id="t1",
        author=ANA,
        created_at=AT,
        client_id=client_id,
        target=target,
        **fields,
    )


async def test_a_thread_since_added_keeps_what_came_before_a_member_from_them(
    storage: Storage,
) -> None:
    made = thread("t1").model_copy(update={"history": "since_added"})
    stored, _ = await storage.create_thread("acme", made)
    assert stored.history == "since_added"
    got = await storage.get_thread("acme", "t1")
    assert got is not None and got.history == "since_added"
    await admit(storage, "acme", added("t1", ANA))
    before, _ = await append(storage, "acme", say("t1", "c1", "before"))
    await admit(storage, "acme", added("t1", BRUNO))
    previews = {
        who: (await storage.list_threads("acme", who, preview=True))[0] for who in ("ana", "bruno")
    }
    assert previews["ana"].last_message == before and previews["bruno"].last_message is None
    assert previews["bruno"].history == "since_added"
    after, _ = await append(storage, "acme", say("t1", "c2", "after"))
    await append(storage, "acme", changed(Edit, "e1", before.id, content=[Text(text="x")]))
    await append(storage, "acme", changed(Edit, "e2", after.id, content=[Text(text="y")]))
    await append(storage, "acme", changed(Retract, "r1", before.id))
    assert await storage.head("acme", "t1", "ana") == (7, 1, await epoch_of(storage))
    assert await storage.head("acme", "t1", "bruno") == (7, 3, await epoch_of(storage))
    assert seqs(await storage.list_events("acme", "t1")) == [1, 2, 3, 4, 5, 6, 7]
    assert seqs(await storage.list_events("acme", "t1", floor=3)) == [3, 4, 6]
    assert seqs(await storage.list_events("acme", "t1", since=0, floor=3)) == [3, 4, 6]
    assert seqs(await storage.list_events("acme", "t1", before=4, floor=3)) == [3]
    for hidden in (before.id, "e1", "r1"):
        assert await storage.get_event("acme", "t1", hidden, floor=3) is None
        assert await storage.get_event("acme", "t1", hidden) is not None
    assert await storage.get_event("acme", "t1", "e2", floor=3) is not None
    found = await storage.tail("acme", "t1", 0, 3, 50)
    assert found is not None and found[:2] == (7, await epoch_of(storage))
    assert seqs(found[2]) == [3, 4, 6]
    [seen] = await storage.list_threads("acme", "bruno", preview=True)
    assert seen.last_message is not None and seen.last_message.content == [Text(text="y")]


async def test_a_thread_with_its_whole_history_floors_no_member(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA)
    await append(storage, "acme", say("t1", "c1"))
    await admit(storage, "acme", added("t1", BRUNO))
    got = await storage.get_thread("acme", "t1")
    assert got is not None and got.history == "all"
    assert await storage.head("acme", "t1", "bruno") == (3, 0, await epoch_of(storage))
    assert await storage.head("acme", "t1", "zed") is None


async def test_a_member_listing_by_activity_filters_metadata_and_pages_together(
    storage: Storage,
) -> None:
    for minutes, (thread_id, metadata) in enumerate(
        [
            ("a", {"open": True}),
            ("b", {"open": True}),
            ("c", {"open": False}),
            ("d", {"open": True}),
        ]
    ):
        await storage.create_thread(
            "acme",
            Thread(
                id=thread_id,
                scope="acme",
                metadata=metadata,
                created_at=AT + timedelta(minutes=minutes),
            ),
        )
        if thread_id != "b":
            await admit(storage, "acme", added(thread_id, ANA))
        if thread_id == "a":
            await admit(storage, "acme", added(thread_id, BRUNO))
    late = say("a", "late", author=BRUNO).model_copy(update={"created_at": AT + timedelta(hours=1)})
    await append(storage, "acme", late)
    await storage.mark("acme", "a", "ana", 0)

    async def listed(**query: Any) -> list[tuple[str, int | None]]:
        threads = await storage.list_threads(
            "acme", "ana", metadata={"open": True}, order="activity", **query
        )
        return [(t.id, t.unread) for t in threads]

    assert await listed() == [("a", 1), ("d", 0)]
    assert await listed(limit=1) == [("a", 1)]
    assert await listed(before="a") == [("d", 0)]
    assert await listed(before="d") == []


async def test_a_member_added_while_an_append_is_in_flight_is_floored_after_it(
    pool: Any, storage: Storage
) -> None:
    made = thread("t1").model_copy(update={"history": "since_added"})
    await storage.create_thread("acme", made)
    await room(storage, "t1", "acme", ANA)
    async with pool.acquire() as writer, writer.transaction():
        await writer.execute(
            "SELECT last_seq FROM thread_threads WHERE scope = 'acme' AND id = 't1' FOR UPDATE"
        )
        secret = say("t1", "c1", "before the add").model_copy(update={"seq": 2})
        await writer.execute(
            "INSERT INTO thread_events (scope, thread_id, seq, id, kind, author_id, client_id, "
            "body) VALUES ('acme', 't1', 2, $1, 'message', 'ana', 'c1', $2::jsonb)",
            secret.id,
            secret.model_dump_json(),
        )
        await writer.execute(
            "UPDATE thread_threads SET last_seq = 2 WHERE scope = 'acme' AND id = 't1'"
        )
        adding = asyncio.ensure_future(admit(storage, "acme", added("t1", BRUNO)))
        await asyncio.sleep(0.2)
        assert not adding.done()
    await adding
    assert await storage.head("acme", "t1", "bruno") == (3, 3, await epoch_of(storage))


async def failing(schemas: Schemas) -> tuple[PostgresStorage, Any]:
    pool = await schemas.pool(await schemas.create())
    async with pool.acquire() as conn:
        await migrations.apply(conn, migrations.plan_create)
    return PostgresStorage(pool), pool


async def fail_on(pool: Any, kind: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE FUNCTION refuse_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
            f"IF NEW.kind = '{kind}' THEN RAISE EXCEPTION 'refused'; END IF; RETURN NEW; END $$"
        )
        await conn.execute(
            "CREATE TRIGGER refuse BEFORE INSERT ON thread_events "
            "FOR EACH ROW EXECUTE FUNCTION refuse_event()"
        )


async def unfail(pool: Any) -> None:
    async with pool.acquire() as conn:
        await conn.execute("DROP FUNCTION refuse_event() CASCADE")


async def test_a_failed_membership_event_leaves_the_membership_as_it_was(
    schemas: Schemas,
) -> None:
    storage, pool = await failing(schemas)
    await room(storage)
    before = await storage.list_events("acme", "t1")
    await fail_on(pool, "member_added")
    with pytest.raises(StorageError):
        await admit(storage, "acme", added("t1", BRUNO))
    assert not await storage.is_member("acme", "t1", "bruno")
    assert await storage.list_events("acme", "t1") == before
    await unfail(pool)
    await admit(storage, "acme", added("t1", BRUNO))
    await fail_on(pool, "member_removed")
    with pytest.raises(StorageError):
        await expel(storage, "acme", removed("t1", BRUNO))
    assert await storage.is_member("acme", "t1", "bruno")


async def test_a_failed_update_event_leaves_the_metadata_as_it_was(schemas: Schemas) -> None:
    storage, pool = await failing(schemas)
    await room(storage)
    old = (await storage.get_thread("acme", "t1")).metadata  # type: ignore[union-attr]
    await fail_on(pool, "thread_updated")
    with pytest.raises(StorageError):
        await revise(storage, "acme", updated("t1", {"topic": "new"}))
    got = await storage.get_thread("acme", "t1")
    assert got is not None and got.metadata == old


async def test_a_failed_creator_event_leaves_no_thread_and_a_retry_makes_it(
    schemas: Schemas,
) -> None:
    storage, pool = await failing(schemas)
    await fail_on(pool, "member_added")
    with pytest.raises(StorageError):
        await storage.create_thread("acme", thread("t1"), added("t1", ANA, ANA))
    assert await storage.get_thread("acme", "t1") is None
    await unfail(pool)
    made, created = await storage.create_thread("acme", thread("t1"), added("t1", ANA, ANA))
    assert created and made.id == "t1"
    assert await storage.is_member("acme", "t1", "ana")
    events = await storage.list_events("acme", "t1")
    assert [(e.kind, e.seq) for e in events] == [("member_added", 1)]
    assert await storage.get_mark("acme", "t1", "ana") == 0


async def test_membership_reads_from_its_member_added_event(storage: Storage) -> None:
    await storage.create_thread("acme", thread("t1").model_copy(update={"history": "since_added"}))
    await admit(storage, "acme", added("t1", ANA))
    await append(storage, "acme", say("t1", "c1", "one"))
    await append(storage, "acme", say("t1", "c2", "two"))
    _, event = await admit(storage, "acme", added("t1", BRUNO))
    assert event is not None
    head = await storage.head("acme", "t1", "bruno")
    assert head is not None and head[1] == event.seq
    assert await storage.get_mark("acme", "t1", "bruno") == event.seq - 1
    assert await expel(storage, "acme", removed("t1", BRUNO)) is not None
    _, again = await admit(storage, "acme", added("t1", BRUNO))
    assert again is not None and again.seq > event.seq
    head = await storage.head("acme", "t1", "bruno")
    assert head is not None and head[1] == again.seq


async def test_removing_a_non_member_writes_nothing(storage: Storage) -> None:
    await room(storage)
    before = await storage.list_events("acme", "t1")
    assert await expel(storage, "acme", removed("t1", BRUNO)) is None
    assert await storage.list_events("acme", "t1") == before


async def refuse(pool: Any, table: str, when: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE FUNCTION refuse_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
            "RAISE EXCEPTION 'refused'; END $$"
        )
        await conn.execute(
            f"CREATE TRIGGER refuse_row {when} ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION refuse_row()"
        )


async def test_a_failed_mark_after_the_event_leaves_no_member_and_no_event(
    schemas: Schemas,
) -> None:
    storage, pool = await failing(schemas)
    await room(storage)
    before = await storage.list_events("acme", "t1")
    await refuse(pool, "thread_marks", "BEFORE INSERT")
    with pytest.raises(StorageError):
        await admit(storage, "acme", added("t1", BRUNO))
    assert not await storage.is_member("acme", "t1", "bruno")
    assert await storage.list_events("acme", "t1") == before


async def test_a_failed_metadata_write_after_the_event_leaves_no_event(schemas: Schemas) -> None:
    storage, pool = await failing(schemas)
    await room(storage)
    before = await storage.list_events("acme", "t1")
    old = (await storage.get_thread("acme", "t1")).metadata  # type: ignore[union-attr]
    await refuse(pool, "thread_threads", "BEFORE UPDATE OF metadata")
    with pytest.raises(StorageError):
        await revise(storage, "acme", updated("t1", {"topic": "new"}))
    got = await storage.get_thread("acme", "t1")
    assert got is not None and got.metadata == old
    assert await storage.list_events("acme", "t1") == before


async def test_deleting_a_scope_does_not_deadlock_with_an_append(
    storage: Storage, pool: Any
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    async with pool.acquire() as holder:
        tx = holder.transaction()
        await tx.start()
        await holder.execute(
            "SELECT 1 FROM thread_threads WHERE scope = 'acme' AND id = 't1' FOR UPDATE"
        )
        appending = asyncio.create_task(append(storage, "acme", say("t1", "c1", author=ANA)))
        await asyncio.sleep(0.3)
        wiping = asyncio.create_task(storage.delete_scope("acme"))
        await asyncio.sleep(0.3)
        await tx.commit()
    done, pending = await asyncio.wait({appending, wiping}, timeout=10)
    assert not pending
    assert appending.exception() is None
    assert wiping.exception() is None
    assert await storage.verify_absent("acme")


async def test_a_mark_in_flight_does_not_hold_back_appends(storage: Storage, pool: Any) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await storage.mark("acme", "t1", "bruno", 0)
    async with pool.acquire() as holder:
        tx = holder.transaction()
        await tx.start()
        await holder.execute(
            "UPDATE thread_marks SET seq = seq WHERE scope = 'acme' AND thread_id = 't1' "
            "AND identity_id = 'bruno'"
        )
        marking = asyncio.create_task(storage.mark("acme", "t1", "bruno", 1))
        await asyncio.sleep(0.3)
        assert not marking.done()
        bruno = asyncio.create_task(append(storage, "acme", say("t1", "c1", author=BRUNO)))
        ana = asyncio.create_task(append(storage, "acme", say("t1", "c2", author=ANA)))
        done, pending = await asyncio.wait({bruno, ana}, timeout=3)
        await tx.commit()
    await asyncio.gather(marking, bruno, ana)
    assert not pending


async def test_deleting_a_scope_does_not_deadlock_with_a_mark(storage: Storage, pool: Any) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await storage.mark("acme", "t1", "bruno", 0)
    async with pool.acquire() as holder:
        tx = holder.transaction()
        await tx.start()
        await holder.execute(
            "SELECT 1 FROM thread_members WHERE scope = 'acme' AND thread_id = 't1' "
            "AND identity_id = 'bruno' FOR NO KEY UPDATE"
        )
        marking = asyncio.create_task(storage.mark("acme", "t1", "bruno", 1))
        await asyncio.sleep(0.3)
        wiping = asyncio.create_task(storage.delete_scope("acme"))
        await asyncio.sleep(0.3)
        await tx.commit()
    done, pending = await asyncio.wait({marking, wiping}, timeout=10)
    assert not pending
    assert wiping.exception() is None
    assert isinstance(marking.exception(), (type(None), NotFoundError))
    assert await storage.verify_absent("acme")


async def test_deleting_a_scope_does_not_deadlock_with_baseline_marks(
    storage: Storage, pool: Any
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await room(storage, "t2", "acme", ANA, BRUNO)
    await storage.mark("acme", "t1", "ana", 0)
    await storage.mark("acme", "t2", "ana", 0)
    async with pool.acquire() as holder:
        tx = holder.transaction()
        await tx.start()
        await holder.execute(
            "SELECT 1 FROM thread_members WHERE scope = 'acme' AND thread_id = 't2' "
            "AND identity_id = 'ana' FOR UPDATE"
        )
        baselining = asyncio.create_task(storage.baseline_marks("acme", "ana"))
        await asyncio.sleep(0.3)
        wiping = asyncio.create_task(storage.delete_scope("acme"))
        await asyncio.sleep(0.3)
        await tx.commit()
    done, pending = await asyncio.wait({baselining, wiping}, timeout=10)
    assert not pending
    assert wiping.exception() is None
    assert baselining.exception() is None
    assert await storage.verify_absent("acme")


async def test_a_thread_without_listing_fields_anchors_a_page_like_its_id(
    storage: Storage,
) -> None:
    for minutes, thread_id in enumerate(["a", "b", "c"]):
        await storage.create_thread("acme", thread(thread_id, minutes=minutes))
        await admit(storage, "acme", added(thread_id, ANA))
    late = say("a", "c1").model_copy(update={"created_at": AT + timedelta(hours=1)})
    await append(storage, "acme", late)
    plain = await storage.get_thread("acme", "a")
    assert plain is not None and plain.last_seq is None
    by_id = await storage.list_threads("acme", "ana", before="a", order="activity")
    by_thread = await storage.list_threads("acme", "ana", before=plain, order="activity")
    assert [t.id for t in by_id] == ["c", "b"]
    assert [t.id for t in by_thread] == ["c", "b"]


async def test_a_thread_without_listing_fields_still_needs_a_membership_to_anchor(
    storage: Storage,
) -> None:
    await storage.create_thread("acme", thread("ana-only", minutes=0))
    await admit(storage, "acme", added("ana-only", ANA))
    await storage.create_thread("acme", thread("bruno-only", minutes=1))
    await admit(storage, "acme", added("bruno-only", BRUNO))
    stranger = await storage.get_thread("acme", "bruno-only")
    assert stranger is not None
    with pytest.raises(NotFoundError, match="postgres: thread 'bruno-only' was not found"):
        await storage.list_threads("acme", "ana", before=stranger)


async def test_a_retracted_message_is_not_unread_in_a_listing_or_a_mark(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    first, _ = await append(storage, "acme", say("t1", "c1", author=ANA))
    assert await storage.mark("acme", "t1", "bruno", first.seq) == (first.seq, 0, True)
    oops, _ = await append(storage, "acme", say("t1", "c2", "wrong thread", author=ANA))
    kept, _ = await append(storage, "acme", say("t1", "c3", "kept", author=ANA))
    assert (await storage.list_threads("acme", "bruno"))[0].unread == 2
    retract = Retract(
        id="rt", thread_id="t1", author=ANA, created_at=AT, client_id="c4", target=oops.id
    )
    await append(storage, "acme", retract)
    assert (await storage.list_threads("acme", "bruno"))[0].unread == 1
    assert await storage.mark("acme", "t1", "bruno", first.seq, back=True) == (first.seq, 1, False)
    await append(
        storage,
        "acme",
        Retract(
            id="rt2", thread_id="t1", author=ANA, created_at=AT, client_id="c5", target=kept.id
        ),
    )
    assert (await storage.list_threads("acme", "bruno"))[0].unread == 0
    assert await storage.mark("acme", "t1", "bruno", first.seq, back=True) == (first.seq, 0, False)


async def test_head_returns_the_epoch(storage: Storage) -> None:
    await room(storage, "t1")
    await room(storage, "t2")
    one, two = await epoch_of(storage), await epoch_of(storage, thread_id="t2")
    assert one != two and one > 0 and two > 0
    assert await storage.head("acme", "t1", "ana") == (1, 0, one)
    assert await storage.head("acme", "t2", "ana") == (1, 0, two)


async def test_the_event_hint_carries_the_epoch(storage: Storage) -> None:
    await room(storage)
    stored, _ = await append(storage, "acme", say("t1", "c1"))
    epoch = await epoch_of(storage)
    heard, stop = await hearing(storage)
    try:
        await storage.hint("acme", "t1", stored.seq, epoch)
        await until(lambda: len(heard) == 1)
        assert heard == [("t1", stored.seq, epoch)]
    finally:
        await stop()


async def test_a_thread_made_again_has_a_larger_epoch(storage: Storage) -> None:
    await room(storage)
    old = await epoch_of(storage)
    assert await storage.delete_thread("acme", "t1")
    await room(storage)
    assert await epoch_of(storage) > old


async def test_creating_a_thread_that_exists_keeps_its_epoch(storage: Storage) -> None:
    await room(storage)
    old = await epoch_of(storage)
    again, created = await storage.create_thread(
        "acme", Thread(id="t1", scope="acme", created_at=AT)
    )
    assert not created and again.epoch == old


async def test_tail_reads_the_head_the_epoch_and_the_events_after_since_together(
    storage: Storage,
) -> None:
    await room(storage)
    for n in range(4):
        await append(storage, "acme", say("t1", f"c{n}"))
    epoch = await epoch_of(storage)
    assert await storage.tail("acme", "t1", 0, 0, 50) == (
        5,
        epoch,
        await storage.list_events("acme", "t1", since=0),
    )
    found = await storage.tail("acme", "t1", 1, 0, 2)
    assert found is not None and found[:2] == (5, epoch) and seqs(found[2]) == [2, 3]
    assert await storage.tail("acme", "t1", 5, 0, 50) == (5, epoch, [])
    assert await storage.tail("other", "t1", 0, 0, 50) is None
    assert await storage.tail("acme", "ghost", 0, 0, 50) is None


async def test_heads_reads_each_thread_that_exists_in_one_query(storage: Storage) -> None:
    await room(storage, "t1")
    await room(storage, "t2", "acme", ANA, BRUNO)
    await room(storage, "t1", "other")
    found = await storage.heads(
        [("acme", "t1"), ("acme", "t2"), ("acme", "ghost"), ("other", "t1")]
    )
    assert found == {
        ("acme", "t1"): (1, await epoch_of(storage)),
        ("acme", "t2"): (2, await epoch_of(storage, thread_id="t2")),
        ("other", "t1"): (1, await epoch_of(storage, "other")),
    }
    assert await storage.heads([]) == {}


async def test_a_write_into_another_epoch_is_refused(storage: Storage) -> None:
    first = await room(storage)
    await storage.delete_thread("acme", "t1")
    again = await room(storage, "t1", "acme", ANA, BRUNO)
    assert again.epoch > first.epoch
    writes: list[Callable[[], Awaitable[Any]]] = [
        lambda: storage.append_event("acme", say("t1", "c1"), first.epoch),
        lambda: storage.add_member("acme", added("t1", HELPER), first.epoch),
        lambda: storage.remove_member("acme", removed("t1", BRUNO), first.epoch),
        lambda: storage.update_thread("acme", updated("t1", {"n": 1}), first.epoch),
    ]
    for write in writes:
        with pytest.raises(NotFoundError, match="^postgres: thread 't1' was not found$"):
            await write()
    assert seqs(await storage.list_events("acme", "t1")) == [1, 2]
    assert [m.identity.id for m in await storage.list_members("acme", "t1")] == ["ana", "bruno"]
    stored, created = await storage.append_event("acme", say("t1", "c1"), again.epoch)
    assert created and stored.seq == 3
