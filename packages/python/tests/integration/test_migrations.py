from datetime import timedelta
from functools import partial
from typing import Any

import pytest
from database import Schemas, quoted
from unit.factories import ANA, AT

from rfnry_thread import migrations
from rfnry_thread.errors import ConfigurationError, InputError, StorageError
from rfnry_thread.migrations import Migration, Plan
from rfnry_thread.models import SYSTEM, Member, Message, Text, Thread
from rfnry_thread.postgres import PostgresStorage

INITIALIZED = [
    """
    CREATE TABLE IF NOT EXISTS thread_threads (
        scope TEXT NOT NULL, id TEXT NOT NULL, metadata JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL, last_seq BIGINT NOT NULL DEFAULT 0,
        last_message_at TIMESTAMPTZ,
        PRIMARY KEY (scope, id))
    """,
    "ALTER TABLE thread_threads ADD COLUMN IF NOT EXISTS last_message_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS ix_thread_threads_created ON thread_threads (scope, created_at DESC, id DESC)",
    "CREATE INDEX IF NOT EXISTS ix_thread_threads_metadata ON thread_threads USING GIN (metadata)",
    "CREATE INDEX IF NOT EXISTS ix_thread_threads_activity ON thread_threads "
    "(scope, (COALESCE(last_message_at, created_at)) DESC, id DESC)",
    """
    CREATE TABLE IF NOT EXISTS thread_members (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, identity_id TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL, body JSONB NOT NULL,
        PRIMARY KEY (scope, thread_id, identity_id),
        FOREIGN KEY (scope, thread_id) REFERENCES thread_threads (scope, id) ON DELETE CASCADE)
    """,
    "CREATE INDEX IF NOT EXISTS ix_thread_members_identity ON thread_members (scope, identity_id)",
    """
    CREATE TABLE IF NOT EXISTS thread_events (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, seq BIGINT NOT NULL, id TEXT NOT NULL,
        kind TEXT NOT NULL, author_id TEXT NOT NULL, client_id TEXT NOT NULL, body JSONB NOT NULL,
        PRIMARY KEY (scope, thread_id, seq),
        UNIQUE (scope, thread_id, id),
        UNIQUE (scope, thread_id, author_id, client_id),
        FOREIGN KEY (scope, thread_id) REFERENCES thread_threads (scope, id) ON DELETE CASCADE)
    """,
    """
    CREATE TABLE IF NOT EXISTS thread_marks (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, identity_id TEXT NOT NULL,
        seq BIGINT NOT NULL, at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (scope, thread_id, identity_id),
        FOREIGN KEY (scope, thread_id, identity_id)
            REFERENCES thread_members (scope, thread_id, identity_id) ON DELETE CASCADE)
    """,
]
TABLES = ["thread_events", "thread_marks", "thread_members", "thread_threads"]
NOW = migrations.LATEST
NEXT = NOW + 1
PROBE = Migration(NEXT, "probe", ("CREATE TABLE thread_probe (n INTEGER)",))


async def created(schemas: Schemas) -> tuple[str, Any]:
    name = await schemas.create()
    pool = await schemas.pool(name)
    async with pool.acquire() as conn:
        await migrations.apply(conn, migrations.plan_create)
    return name, pool


async def initialized(schemas: Schemas) -> tuple[str, Any]:
    name = await schemas.create()
    pool = await schemas.pool(name)
    async with pool.acquire() as conn:
        for statement in INITIALIZED:
            await conn.execute(statement)
    return name, pool


async def written(storage: PostgresStorage) -> None:
    await storage.create_thread("acme", Thread(id="t1", scope="acme", metadata={}, created_at=AT))
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=ANA, added_at=AT, added_by=SYSTEM)
    )
    for n, minutes in enumerate((5, 0)):
        await storage.append_event(
            "acme",
            Message(
                id=f"m{n}",
                thread_id="t1",
                author=ANA,
                client_id=f"c{n}",
                created_at=AT + timedelta(minutes=minutes),
                content=[Text(text=f"hello {n}")],
            ),
        )


async def written_before(pool: Any) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_threads (scope, id, metadata, created_at, last_seq, "
            "last_message_at) VALUES ('acme', 't1', '{}', $1, 2, $1)",
            AT,
        )
        await conn.execute(
            "INSERT INTO thread_members (scope, thread_id, identity_id, added_at, body) "
            "VALUES ('acme', 't1', 'ana', $1, $2::jsonb)",
            AT,
            Member(thread_id="t1", identity=ANA, added_at=AT, added_by=SYSTEM).model_dump_json(),
        )
        for n, minutes in enumerate((5, 0)):
            message = Message(
                id=f"m{n}",
                thread_id="t1",
                seq=n + 1,
                author=ANA,
                client_id=f"c{n}",
                created_at=AT + timedelta(minutes=minutes),
                content=[Text(text=f"hello {n}")],
            )
            await conn.execute(
                "INSERT INTO thread_events (scope, thread_id, seq, id, kind, author_id, "
                "client_id, body) VALUES ('acme', 't1', $1, $2, 'message', 'ana', $3, $4::jsonb)",
                message.seq,
                message.id,
                message.client_id,
                message.model_dump_json(),
            )


async def tables(pool: Any, schema: str) -> list[str]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY 1", schema
        )
    return [row["tablename"] for row in rows]


def probing(monkeypatch: pytest.MonkeyPatch, *steps: Migration) -> None:
    monkeypatch.setattr(migrations, "MIGRATIONS", [*migrations.MIGRATIONS, *steps])
    monkeypatch.setattr(migrations, "LATEST", steps[-1].version)


async def test_create_makes_every_table_and_records_every_version(schemas: Schemas) -> None:
    name, pool = await created(schemas)
    assert await tables(pool, name) == [*TABLES[:3], "thread_migrations", TABLES[3]]
    async with pool.acquire() as conn:
        versions = await conn.fetch("SELECT version, name FROM thread_migrations ORDER BY 1")
    assert [tuple(row) for row in versions] == [(m.version, m.name) for m in migrations.MIGRATIONS]
    storage = PostgresStorage(pool)
    await storage.check()
    await written(storage)
    [listed] = await storage.list_threads("acme", None)
    assert listed.last_message_at == AT


async def test_create_indexes_the_log_by_kind_for_reads_of_rare_kinds(schemas: Schemas) -> None:
    _, pool = await created(schemas)
    async with pool.acquire() as conn:
        definition = await conn.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() "
            "AND indexname = 'ix_thread_events_kind'"
        )
    assert definition is not None and "(scope, thread_id, kind, seq)" in definition


async def test_create_refuses_a_schema_that_already_holds_thread_tables(
    schemas: Schemas,
) -> None:
    name, pool = await created(schemas)
    async with pool.acquire() as conn:
        with pytest.raises(
            ConfigurationError,
            match=f"^postgres: {name} already holds thread tables — run rfnry-thread upgrade, "
            "or create into another schema$",
        ):
            await migrations.apply(conn, migrations.plan_create)


async def test_create_refuses_a_shared_schema_and_runs_nothing_there(schemas: Schemas) -> None:
    pool = await schemas.pool("public")
    async with pool.acquire() as conn:
        with pytest.raises(
            InputError,
            match="^postgres: thread is not created in 'public' — name a schema of its own$",
        ):
            await migrations.apply(conn, migrations.plan_create)
    assert await tables(pool, "public") == []


async def test_a_dry_run_shows_the_plan_and_runs_nothing(schemas: Schemas) -> None:
    name = await schemas.create()
    pool = await schemas.pool(name)
    async with pool.acquire() as conn:
        shown = await migrations.apply(conn, migrations.plan_create, run=False)
    assert shown.steps == [f"{m.version:04d} {m.name}" for m in migrations.MIGRATIONS]
    assert await tables(pool, name) == []


async def test_upgrade_with_nothing_pending_does_nothing(schemas: Schemas) -> None:
    _, pool = await created(schemas)
    async with pool.acquire() as conn:
        assert await migrations.apply(conn, migrations.plan_upgrade) == Plan([], [])


async def test_upgrade_runs_a_new_migration_once_and_records_it(
    schemas: Schemas, monkeypatch: pytest.MonkeyPatch
) -> None:
    name, pool = await created(schemas)
    probing(monkeypatch, PROBE)
    async with pool.acquire() as conn:
        ran = await migrations.apply(conn, migrations.plan_upgrade)
        assert ran.steps == [f"{NEXT:04d} probe"]
        assert await migrations.apply(conn, migrations.plan_upgrade) == Plan([], [])
        assert await conn.fetchval("SELECT max(version) FROM thread_migrations") == NEXT
    assert "thread_probe" in await tables(pool, name)
    await PostgresStorage(pool).check()


async def test_a_failing_step_leaves_the_schema_as_it_was(
    schemas: Schemas, monkeypatch: pytest.MonkeyPatch
) -> None:
    name, pool = await created(schemas)
    broken = Migration(NEXT, "broken", ("CREATE TABLE thread_probe (n INTEGER)", "SELECT 1/0"))
    probing(monkeypatch, broken)
    async with pool.acquire() as conn:
        with pytest.raises(StorageError, match="^postgres: SELECT 1/0 failed: "):
            await migrations.apply(conn, migrations.plan_upgrade)
        assert await conn.fetchval("SELECT max(version) FROM thread_migrations") == NOW
    assert "thread_probe" not in await tables(pool, name)


async def test_a_plan_the_schema_has_moved_past_is_refused(
    schemas: Schemas, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, pool = await created(schemas)
    probing(monkeypatch, PROBE)
    async with pool.acquire() as conn:
        shown = await migrations.apply(conn, migrations.plan_upgrade, run=False)
        await migrations.apply(conn, migrations.plan_upgrade)
        with pytest.raises(
            ConfigurationError,
            match="^postgres: the schema changed since the plan was shown — run the command again$",
        ):
            await migrations.apply(conn, migrations.plan_upgrade, expected=shown)


async def test_a_schema_ahead_of_this_version_is_refused(schemas: Schemas) -> None:
    name, pool = await created(schemas)
    async with pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO thread_migrations (version, name) VALUES ({NEXT}, 'later')"
        )
        ahead = f"{name} is at version {NEXT}, this rfnry-thread knows {NOW} — upgrade rfnry-thread"
        with pytest.raises(ConfigurationError, match=f"^postgres: {ahead}$"):
            await migrations.apply(conn, migrations.plan_upgrade)
    with pytest.raises(ConfigurationError, match=f"^postgres: {ahead}$"):
        await PostgresStorage(pool).check()


async def test_upgrade_adopts_the_tables_initialize_made_and_keeps_every_row(
    schemas: Schemas,
) -> None:
    name, pool = await initialized(schemas)
    storage = PostgresStorage(pool)
    await written_before(pool)
    with pytest.raises(
        ConfigurationError,
        match=f"^postgres: the thread tables in {name} were made by initialize\\(\\) — "
        f"run rfnry-thread upgrade --schema {name}$",
    ):
        await storage.check()
    async with pool.acquire() as conn:
        ran = await migrations.apply(conn, migrations.plan_upgrade)
    assert ran.steps == [
        "0001 create, over the tables initialize() made",
        *(f"{m.version:04d} {m.name}" for m in migrations.MIGRATIONS[1:]),
    ]
    await storage.check()
    assert [e.seq for e in await storage.list_events("acme", "t1")] == [1, 2]
    async with pool.acquire() as conn:
        assert await migrations.apply(conn, migrations.plan_upgrade) == Plan([], [])


async def test_adopting_fills_in_what_an_older_initialize_did_not_make(schemas: Schemas) -> None:
    _, pool = await initialized(schemas)
    storage = PostgresStorage(pool)
    await written_before(pool)
    async with pool.acquire() as conn:
        await conn.execute("DROP INDEX ix_thread_threads_metadata")
        await conn.execute("DROP INDEX ix_thread_threads_activity")
        await conn.execute("ALTER TABLE thread_threads DROP COLUMN last_message_at")
        await migrations.apply(conn, migrations.plan_upgrade)
        definition = await conn.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() "
            "AND indexname = 'ix_thread_threads_metadata'"
        )
    assert definition is not None and "USING gin (metadata)" in definition
    [listed] = await storage.list_threads("acme", None, order="activity")
    assert listed.last_message_at == AT


async def test_upgrade_from_another_schema_moves_the_tables_and_adopts_them(
    schemas: Schemas,
) -> None:
    source, held = await initialized(schemas)
    await written_before(held)
    target = schemas.name()
    pool = await schemas.pool(target)
    async with pool.acquire() as conn:
        ran = await migrations.apply(
            conn,
            partial(migrations.plan_upgrade, source=source),
            before=[f"CREATE SCHEMA IF NOT EXISTS {quoted(target)}"],
        )
    assert ran.steps[:2] == [
        f"move {', '.join(sorted(TABLES))} from {source}",
        "0001 create, over the tables initialize() made",
    ]
    assert await tables(pool, source) == []
    storage = PostgresStorage(pool)
    await storage.check()
    assert [e.seq for e in await storage.list_events("acme", "t1")] == [1, 2]


async def test_upgrade_from_refuses_a_source_without_tables_or_a_target_that_holds_them(
    schemas: Schemas,
) -> None:
    empty = await schemas.create()
    target, pool = await created(schemas)
    source, _ = await initialized(schemas)
    async with pool.acquire() as conn:
        with pytest.raises(
            ConfigurationError,
            match=f"^postgres: {empty} holds no thread tables initialize\\(\\) made$",
        ):
            await migrations.apply(conn, partial(migrations.plan_upgrade, source=empty))
        with pytest.raises(
            ConfigurationError,
            match=f"^postgres: {target} already holds thread tables — "
            "move them into a schema of their own$",
        ):
            await migrations.apply(conn, partial(migrations.plan_upgrade, source=source))
    assert [*TABLES] == sorted(set(await tables(pool, source)))


async def test_upgrade_from_refuses_a_source_the_command_made(schemas: Schemas) -> None:
    source, _ = await created(schemas)
    target = await schemas.create()
    pool = await schemas.pool(target)
    async with pool.acquire() as conn:
        with pytest.raises(
            ConfigurationError,
            match=f"^postgres: {source} is a thread schema of its own — point the pool at it$",
        ):
            await migrations.apply(conn, partial(migrations.plan_upgrade, source=source))


async def test_check_names_the_command_for_each_way_a_schema_does_not_fit(
    schemas: Schemas, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def problem(pool: Any) -> str:
        with pytest.raises(ConfigurationError) as raised:
            await PostgresStorage(pool).check()
        return str(raised.value)

    missing = schemas.name()
    assert await problem(await schemas.pool(missing)) == (
        f"postgres: there is no schema {missing} on search_path '{quoted(missing)}' — "
        f"run rfnry-thread create --schema {missing}"
    )
    empty = await schemas.create()
    assert await problem(await schemas.pool(empty)) == (
        f"postgres: no thread schema in {empty} — run rfnry-thread create --schema {empty}"
    )
    assert await problem(await schemas.pool("public")) == (
        "postgres: no thread schema on search_path '\"public\"' — run rfnry-thread create "
        "and put its schema first on the pool's search_path"
    )
    old, held = await initialized(schemas)
    monkeypatch.setattr(migrations, "_shared", lambda schema: schema == old)
    assert await problem(held) == (
        f"postgres: the thread tables in {old} were made by initialize() — "
        f"run rfnry-thread upgrade --from {old} and put the thread schema first on the pool's search_path"
    )
    name, pool = await created(schemas)
    probing(monkeypatch, PROBE)
    assert await problem(pool) == (
        f"postgres: {name} is at version {NOW}, this rfnry-thread needs {NEXT} — "
        f"run rfnry-thread upgrade --schema {name}"
    )


async def test_upgrading_to_history_keeps_every_thread_whole_for_its_members(
    schemas: Schemas, monkeypatch: pytest.MonkeyPatch
) -> None:
    every = migrations.MIGRATIONS
    monkeypatch.setattr(migrations, "MIGRATIONS", every[:1])
    monkeypatch.setattr(migrations, "LATEST", 1)
    _, pool = await created(schemas)
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_threads (scope, id, metadata, created_at, last_seq) "
            "VALUES ('acme', 't1', '{}', now(), 7)"
        )
        await conn.execute(
            "INSERT INTO thread_members (scope, thread_id, identity_id, added_at, body) "
            "VALUES ('acme', 't1', 'ana', now(), $1::jsonb)",
            Member(thread_id="t1", identity=ANA, added_at=AT, added_by=SYSTEM).model_dump_json(),
        )
    monkeypatch.setattr(migrations, "MIGRATIONS", every)
    monkeypatch.setattr(migrations, "LATEST", every[-1].version)
    async with pool.acquire() as conn:
        ran = await migrations.apply(conn, migrations.plan_upgrade)
    assert ran.steps == [f"{m.version:04d} {m.name}" for m in every[1:]]
    storage = PostgresStorage(pool)
    await storage.check()
    thread = await storage.get_thread("acme", "t1")
    assert thread is not None and thread.history == "all"
    assert await storage.head("acme", "t1", "ana") == (7, 0)


def test_the_first_schema_of_a_search_path_is_read_as_postgres_reads_it() -> None:
    assert migrations._first('"$user", public', "thread", None) == "public"
    assert migrations._first('"$user", public', "thread", "thread") == "thread"
    assert migrations._first('"Odd ""x""", public', "thread", None) == 'Odd "x"'
    assert migrations._first("Thread, public", "thread", None) == "thread"
    assert migrations._first("", "thread", None) is None
