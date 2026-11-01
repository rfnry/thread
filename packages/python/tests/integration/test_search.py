from datetime import timedelta
from functools import partial
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest
from database import URL, Schemas, admin, quoted
from unit.factories import ANA, AT, BRUNO

from rfnry_thread import migrations
from rfnry_thread.errors import ConfigurationError
from rfnry_thread.models import SYSTEM, Edit, Member, Message, Retract, Text, Thread
from rfnry_thread.postgres import PostgresStorage


async def made(schemas: Schemas, search: bool = True) -> tuple[str, PostgresStorage, Any]:
    name = await schemas.create()
    pool = await schemas.pool(name)
    async with pool.acquire() as conn:
        await migrations.apply(conn, migrations.plan_create)
        if search:
            await migrations.apply(conn, partial(migrations.plan_search, on=True))
    return name, PostgresStorage(pool), pool


async def room(
    storage: PostgresStorage,
    thread_id: str,
    metadata: dict[str, Any] | None = None,
    minutes: int = 0,
    history: str = "all",
    members: tuple[Any, ...] = (ANA,),
) -> None:
    await storage.create_thread(
        "acme",
        Thread(
            id=thread_id,
            scope="acme",
            metadata=metadata or {},
            created_at=AT + timedelta(minutes=minutes),
            history=history,  # type: ignore[arg-type]
        ),
    )
    for identity in members:
        await storage.add_member(
            "acme", Member(thread_id=thread_id, identity=identity, added_at=AT, added_by=SYSTEM)
        )


async def say(
    storage: PostgresStorage, thread_id: str, text: str, minutes: int = 0, author: Any = ANA
) -> Message:
    stored, _ = await storage.append_event(
        "acme",
        Message(
            id=f"{thread_id}-{text}",
            thread_id=thread_id,
            author=author,
            client_id=f"{thread_id}-{text}",
            created_at=AT + timedelta(minutes=minutes),
            content=[Text(text=text)],
        ),
    )
    assert isinstance(stored, Message)
    return stored


async def change(storage: PostgresStorage, kind: Any, target: Message, text: str = "") -> None:
    fields: dict[str, Any] = {"content": [Text(text=text)]} if kind is Edit else {}
    await storage.append_event(
        "acme",
        kind(
            id=f"{kind.__name__}-{target.id}-{text}",
            thread_id=target.thread_id,
            author=target.author,
            client_id=f"{kind.__name__}-{target.id}-{text}",
            created_at=AT,
            target=target.id,
            **fields,
        ),
    )


async def texts(pool: Any) -> list[tuple[str, int, str]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT thread_id, seq, text FROM thread_texts WHERE seq > 0 ORDER BY 1, 2"
        )
    return [(r["thread_id"], r["seq"], r["text"]) for r in rows]


async def test_set_search_fills_the_index_from_the_log_as_it_reads_now(schemas: Schemas) -> None:
    _, storage, pool = await made(schemas, search=False)
    await room(storage, "t1", {"title": "Billing"})
    first = await say(storage, "t1", "Refund the second charge")
    second = await say(storage, "t1", "hello")
    secret = await say(storage, "t1", "secret plan")
    await change(storage, Edit, second, "hello, invoice")
    await change(storage, Edit, second, "hello, invoice attached")
    await change(storage, Retract, secret)
    async with pool.acquire() as conn:
        shown = await migrations.apply(conn, partial(migrations.plan_search, on=True), run=False)
        assert shown.steps[-1] == (
            "search: thread_texts filled from the log and the threads' metadata, kept current by "
            "triggers, with a trigram index"
        )
        await migrations.apply(conn, partial(migrations.plan_search, on=True))
        again = await migrations.apply(conn, partial(migrations.plan_search, on=True))
    assert again.steps == []
    assert await texts(pool) == [
        ("t1", first.seq, "Refund the second charge"),
        ("t1", second.seq, "hello, invoice attached"),
    ]
    async with pool.acquire() as conn:
        analyzed = await conn.fetch(
            "SELECT tablename FROM pg_stats WHERE schemaname = current_schema() "
            "AND tablename = 'thread_texts' AND attname = 'text'"
        )
    assert len(analyzed) == 1
    assert await listed(storage, "billing") == ["t1"]
    await storage.check()


async def test_the_index_follows_every_message_edit_and_retraction(schemas: Schemas) -> None:
    _, storage, pool = await made(schemas)
    await room(storage, "t1")
    kept = await say(storage, "t1", "first draft")
    gone = await say(storage, "t1", "never mind")
    await change(storage, Edit, kept, "final text")
    await change(storage, Retract, gone)
    await change(storage, Edit, gone, "edited after its retraction")
    assert await texts(pool) == [("t1", kept.seq, "final text")]


async def test_the_index_follows_a_threads_metadata_as_it_is_made_and_changed(
    schemas: Schemas,
) -> None:
    _, storage, pool = await made(schemas)
    await room(storage, "t1", {"title": "Pump order", "tags": ["urgent"]})
    assert await listed(storage, "pump") == ["t1"]
    await storage.update_thread("acme", "t1", {"title": "Valve order"})
    assert await listed(storage, "pump") == []
    assert await listed(storage, "urgent") == []
    assert await listed(storage, "valve") == ["t1"]
    await say(storage, "t1", "and a pump")
    [found] = await storage.list_threads("acme", "ana", query="valve")
    assert found.match is None


async def test_deleting_a_thread_or_a_scope_takes_its_texts_along(schemas: Schemas) -> None:
    _, storage, pool = await made(schemas)
    for thread_id in ("t1", "t2"):
        await room(storage, thread_id)
        await say(storage, thread_id, f"words in {thread_id}")
    assert await storage.delete_thread("acme", "t1")
    assert [t for t, _, _ in await texts(pool)] == ["t2"]
    removed = await storage.delete_scope("acme")
    assert removed == 5
    assert await texts(pool) == []
    assert await storage.verify_absent("acme")


@pytest.mark.parametrize(
    "drop",
    [
        "DROP TRIGGER thread_texts_follow ON thread_events",
        "DROP TRIGGER thread_texts_name ON thread_threads",
        "DROP INDEX ix_thread_texts_trgm",
    ],
)
async def test_check_names_a_half_set_up_search_and_no_search_removes_it(
    schemas: Schemas, drop: str
) -> None:
    name, storage, pool = await made(schemas)
    async with pool.acquire() as conn:
        await conn.execute(drop)
    with pytest.raises(
        ConfigurationError,
        match=f"^postgres: search is half set up in {name} — run rfnry-thread set --search --schema {name}$",
    ):
        await storage.check()
    async with pool.acquire() as conn:
        await migrations.apply(conn, partial(migrations.plan_search, on=True))
    await storage.check()
    async with pool.acquire() as conn:
        dropped = await migrations.apply(conn, partial(migrations.plan_search, on=False))
        assert dropped.steps == ["no search: thread_texts, its triggers and index dropped"]
        assert await conn.fetchval("SELECT to_regclass('thread_texts')") is None
    await storage.check()
    with pytest.raises(
        ConfigurationError,
        match=f"^postgres: search is not set up in {name} — run rfnry-thread set --search --schema {name}$",
    ):
        await storage.list_threads("acme", "ana", query="anything")


async def listed(storage: PostgresStorage, query: str, **options: Any) -> list[str]:
    found = await storage.list_threads("acme", "ana", order="activity", query=query, **options)
    return [t.id for t in found]


async def test_a_query_finds_every_thread_whose_title_or_text_holds_every_word(
    schemas: Schemas,
) -> None:
    _, storage, _ = await made(schemas)
    await room(storage, "billing", {"title": "Billing question"}, minutes=1)
    await say(storage, "billing", "My invoice was charged twice", minutes=1)
    await room(storage, "refund", {"title": "Second charge"}, minutes=2)
    await say(storage, "refund", "Please REFUND the extra charge", minutes=5)
    await room(storage, "other", {"title": "Hello"}, minutes=3)
    await say(storage, "other", "Nothing to see", minutes=3)
    await room(storage, "foreign", {"title": "Invoice for bruno"}, members=(BRUNO,), minutes=9)
    await room(storage, "odd", {"title": "100% sure_thing"}, minutes=4)
    assert await listed(storage, "charge") == ["refund", "billing"]
    assert await listed(storage, "refund charge") == ["refund"]
    assert await listed(storage, "voic") == ["billing"]
    assert await listed(storage, "billing") == ["billing"]
    assert await listed(storage, "nothing here") == []
    assert await listed(storage, "100%") == ["odd"]
    assert await listed(storage, "%") == ["odd"]
    assert await listed(storage, "e_t") == ["odd"]
    assert await listed(storage, "charge", limit=1) == ["refund"]
    [page] = await storage.list_threads("acme", "ana", order="activity", query="charge", limit=1)
    assert await listed(storage, "charge", before=page) == ["billing"]
    assert await listed(storage, "charge", metadata={"title": "Second charge"}) == ["refund"]
    every = await storage.list_threads("acme", None, query="invoice")
    assert sorted(t.id for t in every) == ["billing", "foreign"]


async def test_every_string_of_the_metadata_is_searched_and_any_script_matches(
    schemas: Schemas,
) -> None:
    _, storage, _ = await made(schemas)
    await room(storage, "nested", {"customer": {"name": "Zelia"}, "tags": ["vip", 3], "n": 7})
    await room(storage, "minutes", minutes=1)
    await say(storage, "minutes", "会議の議事録を送ります")
    assert await listed(storage, "zelia") == ["nested"]
    assert await listed(storage, "VIP") == ["nested"]
    assert await listed(storage, "customer") == []
    assert await listed(storage, "7") == []
    assert await listed(storage, "議事") == ["minutes"]
    assert await listed(storage, "議事 送ります") == ["minutes"]


async def test_a_match_is_the_latest_message_as_it_reads_now_or_none_for_a_title(
    schemas: Schemas,
) -> None:
    _, storage, _ = await made(schemas)
    await room(storage, "t1", {"title": "Charge dispute"})
    old = await say(storage, "t1", "first charge")
    await say(storage, "t1", "an early charge")
    edited = await say(storage, "t1", "second charge")
    gone = await say(storage, "t1", "third charge")
    await change(storage, Edit, edited, "second charge, corrected")
    await change(storage, Retract, gone)
    await change(storage, Edit, old, "first, no longer")
    [found] = await storage.list_threads("acme", "ana", query="charge", preview=True)
    assert found.match is not None
    assert (found.match.id, found.match.content) == (
        edited.id,
        [Text(text="second charge, corrected")],
    )
    assert found.last_message is not None
    [titled] = await storage.list_threads("acme", "ana", query="dispute")
    assert titled.match is None and titled.id == "t1"
    [first] = await storage.list_threads("acme", "ana", query="first")
    assert first.match is not None and first.match.id == old.id
    assert await storage.list_threads("acme", "ana", query="first charge") == []
    assert await storage.list_threads("acme", "ana", query="third") == []
    plain = await storage.list_threads("acme", "ana")
    assert plain[0].match is None and "match" not in plain[0].model_dump(mode="json")


async def test_a_since_added_member_finds_only_what_it_may_read(schemas: Schemas) -> None:
    _, storage, _ = await made(schemas)
    await room(storage, "t1", {"title": "Hiring plans"}, history="since_added")
    await say(storage, "t1", "before the add, secret")
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=BRUNO, added_at=AT, added_by=SYSTEM)
    )
    after = await say(storage, "t1", "after the add, open")
    for word, ana, bruno in (
        ("secret", ["t1"], []),
        ("open", ["t1"], ["t1"]),
        ("hiring", ["t1"], ["t1"]),
    ):
        assert [t.id for t in await storage.list_threads("acme", "ana", query=word)] == ana
        assert [t.id for t in await storage.list_threads("acme", "bruno", query=word)] == bruno
    _, floor = await storage.head("acme", "t1", "bruno") or (0, 0)
    found = await storage.search("acme", "t1", "the add", floor=floor)
    assert [m.id for m in found] == [after.id]


async def test_search_in_a_thread_pages_back_through_its_matches(schemas: Schemas) -> None:
    _, storage, _ = await made(schemas)
    await room(storage, "t1")
    await room(storage, "t2")
    sent = [await say(storage, "t1", f"note {n} about pumps") for n in range(5)]
    await say(storage, "t1", "unrelated")
    await say(storage, "t2", "pumps elsewhere")
    await change(storage, Edit, sent[4], "note 4 about valves")
    latest = await storage.search("acme", "t1", "pumps", limit=2)
    assert [m.id for m in latest] == [sent[2].id, sent[3].id]
    older = await storage.search("acme", "t1", "pumps", before=latest[0].seq, limit=10)
    assert [m.id for m in older] == [sent[0].id, sent[1].id]
    [valves] = await storage.search("acme", "t1", "VALVES")
    assert valves.content == [Text(text="note 4 about valves")]


async def test_check_names_the_grants_a_role_lacks_on_thread_texts(schemas: Schemas) -> None:
    name, _, _ = await made(schemas)
    role = f"thread_probe_{uuid4().hex[:8]}"
    await admin(f"CREATE ROLE {role} LOGIN PASSWORD 'probe'")
    try:
        await admin(f"GRANT USAGE ON SCHEMA {quoted(name)} TO {role}")
        await admin(
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON {quoted(name)}.thread_threads, "
            f"{quoted(name)}.thread_members, {quoted(name)}.thread_events, "
            f"{quoted(name)}.thread_marks TO {role}"
        )
        await admin(f"GRANT SELECT ON {quoted(name)}.thread_migrations TO {role}")
        parts = urlsplit(URL)
        url = urlunsplit(parts._replace(netloc=f"{role}:probe@{parts.hostname}:{parts.port}"))
        pool = await asyncpg.create_pool(
            url, min_size=1, max_size=1, server_settings={"search_path": quoted(name)}
        )
        try:
            with pytest.raises(ConfigurationError) as raised:
                await PostgresStorage(pool).check()
            assert str(raised.value) == (
                f"postgres: {role} lacks SELECT, INSERT, UPDATE, DELETE on thread_texts — grant it "
                "SELECT, INSERT, UPDATE, DELETE on them and SELECT on thread_migrations"
            )
            await admin(
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON {quoted(name)}.thread_texts TO {role}"
            )
            await PostgresStorage(pool).check()
        finally:
            await pool.close()
    finally:
        await admin(f"DROP OWNED BY {role}")
        await admin(f"DROP ROLE {role}")


async def test_an_extension_the_role_cannot_create_names_a_privileged_role() -> None:
    class Refusing:
        async def execute(self, statement: str) -> None:
            raise RuntimeError("permission denied to create extension")

    install = "CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public"
    with pytest.raises(ConfigurationError) as raised:
        await migrations._execute(Refusing(), install)
    assert str(raised.value) == (
        f"postgres: {install} failed: permission denied to create extension — run it as a "
        "privileged role, then run this command again"
    )


async def test_every_search_is_planned_for_its_own_words(schemas: Schemas) -> None:
    name = await schemas.create()
    pool = await asyncpg.create_pool(
        URL, min_size=1, max_size=1, server_settings={"search_path": quoted(name)}
    )
    try:
        async with pool.acquire() as conn:
            await migrations.apply(conn, migrations.plan_create)
            await migrations.apply(conn, partial(migrations.plan_search, on=True))
        storage = PostgresStorage(pool)
        await room(storage, "t1", {"title": "pumps"})
        await say(storage, "t1", "the pump leaks")
        for _ in range(8):
            await storage.list_threads("acme", "ana", query="pump")
            await storage.list_threads("acme", None, query="pump")
            await storage.search("acme", "t1", "pump")
        async with pool.acquire() as conn:
            plans = await conn.fetch(
                "SELECT generic_plans, custom_plans FROM pg_prepared_statements "
                "WHERE statement LIKE '%thread_texts x%' "
                "AND statement NOT LIKE '%pg_prepared_statements%'"
            )
        assert len(plans) == 3
        assert all(p["generic_plans"] == 0 and p["custom_plans"] == 8 for p in plans)
    finally:
        await pool.close()
