import asyncio
from datetime import timedelta
from typing import Any

import pytest
from unit.factories import ANA, AT, BRUNO, HELPER
from until import until

from rfnry_thread.errors import NotFoundError
from rfnry_thread.models import (
    SYSTEM,
    Edit,
    Event,
    Member,
    Message,
    Read,
    Retract,
    Text,
    Thread,
)
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
        await storage.add_member(
            "acme", Member(thread_id=thread_id, identity=ANA, added_at=AT, added_by=SYSTEM)
        )

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


async def test_an_event_is_found_by_its_author_and_client_id(storage: Storage) -> None:
    await room(storage, "t1")
    await room(storage, "t1", "tenant-b")
    sent, _ = await storage.append_event("acme", say("t1", "c1"))
    assert await storage.find_event("acme", "t1", "ana", "c1") == sent
    assert await storage.find_event("acme", "t1", "bruno", "c1") is None
    assert await storage.find_event("acme", "t1", "ana", "c2") is None
    assert await storage.find_event("tenant-b", "t1", "ana", "c1") is None


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


async def test_a_head_is_the_last_seq_for_a_member_and_none_for_anyone_else(
    storage: Storage,
) -> None:
    await room(storage, "t1")
    await room(storage, "t1", "globex")
    for client_id in ("c1", "c2"):
        await storage.append_event("acme", say("t1", client_id))
    assert await storage.head("acme", "t1", "ana") == 2
    assert await storage.head("globex", "t1", "ana") == 0
    assert await storage.head("acme", "t1", "bruno") is None
    assert await storage.head("acme", "ghost", "ana") is None


async def test_the_same_thread_id_in_two_scopes_is_two_threads(storage: Storage) -> None:
    await room(storage, "t1", "tenant-a")
    await room(storage, "t1", "tenant-b")
    await storage.append_event("tenant-a", say("t1", "c1", "MARKER-A"))
    assert await storage.list_events("tenant-b", "t1") == []
    assert await storage.get_event("tenant-b", "t1", "id-c1-MARKER-A") is None
    await storage.update_thread("tenant-a", "t1", {"tenant": "a"})
    await storage.update_thread("tenant-b", "t1", {"tenant": "b"})
    [listed] = await storage.list_threads("tenant-b", "ana")
    assert (listed.scope, listed.id, listed.metadata, listed.created_at, listed.last_seq) == (
        "tenant-b",
        "t1",
        {"tenant": "b"},
        AT,
        0,
    )
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


async def test_a_mark_moves_forward_only_counts_what_others_said_after_it_and_says_if_it_moved(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    for n in range(3):
        await storage.append_event("acme", say("t1", f"b{n}", author=BRUNO))
    await storage.append_event("acme", say("t1", "a0", author=ANA))
    assert await storage.mark("acme", "t1", "ana", 1) == (1, 2, True)
    assert await storage.mark("acme", "t1", "ana", 0) == (1, 2, False)
    assert await storage.mark("acme", "t1", "ana", 1) == (1, 2, False)
    assert await storage.mark("acme", "t1", "ana", 4) == (4, 0, True)
    assert await storage.mark("acme", "t1", "bruno", 0) == (0, 1, True)


async def test_a_mark_moves_back_only_when_asked(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    for n in range(3):
        await storage.append_event("acme", say("t1", f"b{n}", author=BRUNO))
    await storage.append_event("acme", say("t1", "a0", author=ANA))
    assert await storage.mark("acme", "t1", "ana", 4) == (4, 0, True)
    assert await storage.mark("acme", "t1", "ana", 1) == (4, 0, False)
    assert await storage.mark("acme", "t1", "ana", 1, back=True) == (1, 2, True)
    assert await storage.mark("acme", "t1", "ana", 1, back=True) == (1, 2, False)
    assert await storage.mark("acme", "t1", "ana", 3, back=True) == (3, 0, True)
    assert await storage.mark("acme", "t1", "bruno", 2, back=True) == (2, 1, True)
    assert await storage.get_mark("acme", "t1", "ana") == 3


async def test_a_mark_needs_a_membership_and_leaves_with_it(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA)
    with pytest.raises(NotFoundError, match="'bruno' is not a member of thread 't1'$"):
        await storage.mark("acme", "t1", "bruno", 0)
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=BRUNO, added_at=AT, added_by=SYSTEM)
    )
    await storage.append_event("acme", say("t1", "b1", author=BRUNO))
    assert await storage.mark("acme", "t1", "ana", 1) == (1, 0, True)
    await storage.remove_member("acme", "t1", "ana")
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=ANA, added_at=AT, added_by=SYSTEM)
    )
    assert await storage.mark("acme", "t1", "ana", 0) == (0, 1, True)


async def test_baseline_marks_set_the_last_seq_only_where_there_is_no_mark(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA)
    await room(storage, "t2", "acme", ANA)
    await room(storage, "t3", "acme", BRUNO)
    for client_id in ("x", "y"):
        await storage.append_event("acme", say("t1", client_id))
    await storage.append_event("acme", say("t2", "z"))
    await storage.mark("acme", "t2", "ana", 0)
    assert await storage.baseline_marks("acme", "ana") == 1
    assert await storage.baseline_marks("acme", "ana") == 0
    assert (await storage.mark("acme", "t1", "ana", 0))[0] == 2
    assert (await storage.mark("acme", "t2", "ana", 0))[0] == 0


async def test_a_mark_is_read_for_one_membership_and_is_none_without_one(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await room(storage, "t1", "other", ANA)
    await storage.append_event("acme", say("t1", "x"))
    assert await storage.get_mark("acme", "t1", "ana") is None
    await storage.mark("acme", "t1", "ana", 1)
    assert await storage.get_mark("acme", "t1", "ana") == 1
    assert await storage.get_mark("acme", "t1", "bruno") is None
    assert await storage.get_mark("other", "t1", "ana") is None
    assert await storage.get_mark("acme", "missing", "ana") is None
    await storage.remove_member("acme", "t1", "ana")
    assert await storage.get_mark("acme", "t1", "ana") is None


async def test_a_deleted_scope_takes_its_marks_along(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA)
    await storage.mark("acme", "t1", "ana", 0)
    assert await storage.delete_scope("acme") == 3
    assert await storage.verify_absent("acme")


async def test_a_member_listing_carries_its_mark_and_what_others_said_after_it(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await storage.append_event("acme", say("t1", "b1", author=BRUNO))
    await storage.append_event("acme", say("t1", "a1", author=ANA))
    await storage.append_event("acme", say("t1", "b2", author=BRUNO))
    [before] = await storage.list_threads("acme", "ana")
    assert (before.last_seq, before.marked, before.unread) == (3, None, None)
    await storage.mark("acme", "t1", "ana", 1)
    [after] = await storage.list_threads("acme", "ana")
    assert (after.marked, after.unread) == (1, 1)
    [anyone] = await storage.list_threads("acme", None)
    assert (anyone.last_seq, anyone.marked, anyone.unread) == (3, None, None)


async def test_unread_is_counted_up_to_100(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await storage.mark("acme", "t1", "ana", 0)
    for n in range(105):
        await storage.append_event("acme", say("t1", f"b{n}", author=BRUNO))
    [listed] = await storage.list_threads("acme", "ana")
    assert listed.unread == 100


async def test_activity_order_follows_the_latest_message_and_pages(storage: Storage) -> None:
    for minutes, thread_id in enumerate(["a", "b", "c"]):
        await storage.create_thread("acme", thread(thread_id, minutes=minutes))
        await storage.add_member(
            "acme", Member(thread_id=thread_id, identity=ANA, added_at=AT, added_by=SYSTEM)
        )
    late = say("a", "late").model_copy(update={"created_at": AT + timedelta(hours=1)})
    await storage.append_event("acme", late)
    read = Read(
        id="r1",
        thread_id="b",
        author=ANA,
        created_at=AT + timedelta(hours=2),
        client_id="r1",
        up_to_seq=1,
    )
    await storage.append_event("acme", read)

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
        await storage.add_member(
            "acme", Member(thread_id=thread_id, identity=ANA, added_at=AT, added_by=SYSTEM)
        )
        later = AT + timedelta(hours=1, minutes=minutes)
        await storage.append_event(
            "acme", say(thread_id, thread_id).model_copy(update={"created_at": later})
        )

    async def listed(**query: Any) -> list[Thread]:
        return await storage.list_threads("acme", "ana", order=order, **query)

    first = await listed(limit=2)
    assert [t.id for t in first] == ["d", "c"]
    await storage.delete_thread("acme", "c")
    assert [t.id for t in await listed(before=first[-1])] == ["b", "a"]
    [anchor] = await listed(before=first[-1], limit=1)
    moved = say("b", "again").model_copy(update={"created_at": AT + timedelta(hours=2)})
    await storage.append_event("acme", moved)
    assert [t.id for t in await listed(before=anchor)] == ["a"]


async def test_a_preview_shows_the_latest_message_with_its_edit_and_skips_a_retracted_one(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA)
    first, _ = await storage.append_event("acme", say("t1", "c1", "first"))
    second, _ = await storage.append_event("acme", say("t1", "c2", "second"))
    edit = Edit(
        id="ed",
        thread_id="t1",
        author=ANA,
        created_at=AT,
        client_id="c3",
        target=first.id,
        content=[Text(text="first, edited")],
    )
    await storage.append_event("acme", edit)
    [plain] = await storage.list_threads("acme", "ana")
    assert plain.last_message is None
    assert "last_message" not in plain.model_dump(mode="json")
    [shown] = await storage.list_threads("acme", "ana", preview=True)
    assert shown.last_message is not None
    assert shown.last_message.content == [Text(text="second")]
    retract = Retract(
        id="rt", thread_id="t1", author=ANA, created_at=AT, client_id="c4", target=second.id
    )
    await storage.append_event("acme", retract)
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
    old, _ = await storage.append_event("acme", say("t1", "c0", "old"))
    recent = [
        (await storage.append_event("acme", say("t1", f"c{n}", f"m{n}")))[0] for n in range(1, 6)
    ]

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

    await storage.append_event(
        "acme", change(Edit, "e0", old.id, content=[Text(text="old, edited")])
    )
    assert (await shown()) == recent[-1]
    for n, message in enumerate(recent[1:]):
        await storage.append_event("acme", change(Retract, f"r{n}", message.id))
    edited = await shown()
    assert edited is not None and (edited.id, edited.content) == (recent[0].id, [Text(text="m1")])
    await storage.append_event("acme", change(Retract, "r-last", recent[0].id))
    assert await shown() is None


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
            await storage.add_member(
                "acme", Member(thread_id=thread_id, identity=ANA, added_at=AT, added_by=SYSTEM)
            )
    late = say("a", "late", author=BRUNO).model_copy(update={"created_at": AT + timedelta(hours=1)})
    await storage.append_event("acme", late)
    await storage.mark("acme", "a", "ana", 0)

    async def listed(**query: Any) -> list[tuple[str, int | None]]:
        threads = await storage.list_threads(
            "acme", "ana", metadata={"open": True}, order="activity", **query
        )
        return [(t.id, t.unread) for t in threads]

    assert await listed() == [("a", 1), ("d", None)]
    assert await listed(limit=1) == [("a", 1)]
    assert await listed(before="a") == [("d", None)]
    assert await listed(before="d") == []
