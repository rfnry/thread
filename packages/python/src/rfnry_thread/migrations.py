import re
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, NamedTuple

from rfnry_thread.errors import ConfigurationError, InputError, StorageError


class Migration(NamedTuple):
    version: int
    name: str
    statements: Sequence[str]


class Plan(NamedTuple):
    steps: list[str]
    statements: list[str]


Planner = Callable[[Any], Awaitable[Plan]]


_CREATE = (
    """CREATE TABLE IF NOT EXISTS thread_migrations (
        version INTEGER NOT NULL PRIMARY KEY, name TEXT NOT NULL,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now())""",
    """CREATE TABLE IF NOT EXISTS thread_threads (
        scope TEXT NOT NULL, id TEXT NOT NULL, metadata JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL, last_seq BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (scope, id))""",
    "ALTER TABLE thread_threads ADD COLUMN IF NOT EXISTS last_message_at TIMESTAMPTZ",
    "CREATE INDEX IF NOT EXISTS ix_thread_threads_created "
    "ON thread_threads (scope, created_at DESC, id DESC)",
    "CREATE INDEX IF NOT EXISTS ix_thread_threads_metadata ON thread_threads USING GIN (metadata)",
    "CREATE INDEX IF NOT EXISTS ix_thread_threads_activity "
    "ON thread_threads (scope, (COALESCE(last_message_at, created_at)) DESC, id DESC)",
    """CREATE TABLE IF NOT EXISTS thread_members (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, identity_id TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL, body JSONB NOT NULL,
        PRIMARY KEY (scope, thread_id, identity_id),
        FOREIGN KEY (scope, thread_id) REFERENCES thread_threads (scope, id) ON DELETE CASCADE)""",
    "CREATE INDEX IF NOT EXISTS ix_thread_members_identity ON thread_members (scope, identity_id)",
    """CREATE TABLE IF NOT EXISTS thread_events (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, seq BIGINT NOT NULL, id TEXT NOT NULL,
        kind TEXT NOT NULL, author_id TEXT NOT NULL, client_id TEXT NOT NULL, body JSONB NOT NULL,
        PRIMARY KEY (scope, thread_id, seq),
        UNIQUE (scope, thread_id, id),
        UNIQUE (scope, thread_id, author_id, client_id),
        FOREIGN KEY (scope, thread_id) REFERENCES thread_threads (scope, id) ON DELETE CASCADE)""",
    """CREATE TABLE IF NOT EXISTS thread_marks (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, identity_id TEXT NOT NULL,
        seq BIGINT NOT NULL, at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (scope, thread_id, identity_id),
        FOREIGN KEY (scope, thread_id, identity_id)
            REFERENCES thread_members (scope, thread_id, identity_id) ON DELETE CASCADE)""",
    """UPDATE thread_threads t SET last_message_at = m.at
        FROM (SELECT s.scope, s.id, (SELECT (e.body->>'created_at')::timestamptz FROM thread_events e
              WHERE e.scope = s.scope AND e.thread_id = s.id AND e.kind = 'message'
              ORDER BY e.seq DESC LIMIT 1) AS at
              FROM thread_threads s WHERE s.last_message_at IS NULL) m
        WHERE t.scope = m.scope AND t.id = m.id AND m.at IS NOT NULL""",
)


_HISTORY = (
    "ALTER TABLE thread_threads ADD COLUMN history TEXT NOT NULL DEFAULT 'all'",
    "ALTER TABLE thread_members ADD COLUMN added_seq BIGINT NOT NULL DEFAULT 0",
)


_KINDS = ("CREATE INDEX ix_thread_events_kind ON thread_events (scope, thread_id, kind, seq)",)


MIGRATIONS = [
    Migration(1, "create", _CREATE),
    Migration(2, "history", _HISTORY),
    Migration(3, "kinds", _KINDS),
]


LATEST = MIGRATIONS[-1].version


LOCK = "SELECT pg_advisory_xact_lock(hashtextextended('thread.migrate:' || current_schema(), 0))"


DEFAULT = "thread"


_INITIALIZED = ("thread_events", "thread_marks", "thread_members", "thread_threads")


_WRITES = ("SELECT", "INSERT", "UPDATE", "DELETE")


_STATE = (
    "SELECT current_schema() AS schema, current_setting('search_path') AS path, "
    "current_user AS role, "
    "to_regclass(quote_ident(current_schema()) || '.thread_migrations') IS NOT NULL AS migrated, "
    "to_regclass(quote_ident(current_schema()) || '.thread_threads') IS NOT NULL AS tables"
)


_NAMES = re.compile(r'"((?:[^"]|"")*)"|([^,\s]+)')


_ELSEWHERE = (
    "SELECT n.nspname AS name FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relname = 'thread_threads' AND c.relkind IN ('r', 'p') AND n.nspname <> $1 "
    "AND NOT EXISTS (SELECT 1 FROM pg_class m WHERE m.relnamespace = n.oid "
    "AND m.relname = 'thread_migrations') ORDER BY 1"
)


_OCCUPIED = (
    "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = $1 AND c.relkind IN ('r', 'p'))"
)


_MIGRATED = "SELECT to_regclass(quote_ident($1) || '.thread_migrations') IS NOT NULL"


_LACKING = (
    "SELECT t.name AS table_name, p.name AS privilege "
    "FROM unnest($1::text[]) WITH ORDINALITY AS t(name, n) "
    "CROSS JOIN unnest($2::text[]) WITH ORDINALITY AS p(name, m) "
    "WHERE NOT has_table_privilege(quote_ident(current_schema()) || '.' || t.name, p.name) "
    "ORDER BY p.m, t.name"
)


def _names(path: str, role: str, schema: str | None) -> list[str]:
    found: list[str] = []
    for quoted, bare in _NAMES.findall(path):
        name: str = quoted.replace('""', '"') if quoted else bare.lower()
        if name == "$user":
            if role == schema:
                found.append(role)
            continue
        if name:
            found.append(name)
    return found


def _first(path: str, role: str, schema: str | None) -> str | None:
    names = _names(path, role, schema)
    return names[0] if names else None


def _listed(names: Sequence[str]) -> str:
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} and {names[-1]}"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _recorded(migration: Migration) -> str:
    return (
        "INSERT INTO thread_migrations (version, name) "
        f"VALUES ({migration.version}, {_literal(migration.name)})"
    )


def _steps(migrations: Sequence[Migration]) -> Plan:
    return Plan(
        [f"{m.version:04d} {m.name}" for m in migrations],
        [s for m in migrations for s in [*m.statements, _recorded(m)]],
    )


def _shared(schema: str) -> bool:
    return schema in ("public", "information_schema") or schema.startswith("pg_")


async def remedy(conn: Any, schema: str, path: Sequence[str] = ()) -> str:
    moved = _shared(schema) or bool(await conn.fetchval(_OCCUPIED, schema))
    target = DEFAULT if moved else schema
    tail = " and put it first on the pool's search_path" if moved else ""
    if moved and await conn.fetchval(_MIGRATED, target):
        return f"thread's tables are in {target} — put it first on the pool's search_path"
    found = [row["name"] for row in await conn.fetch(_ELSEWHERE, target)]
    near = [name for name in found if _shared(name) or name in path]
    if near:
        return (
            f"the thread tables initialize() made are in {near[0]} — "
            f"run rfnry-thread upgrade --schema {target} --from {near[0]}{tail}"
        )
    if found:
        return (
            f"run rfnry-thread create --schema {target}{tail}; if the tables initialize() made "
            f"in {', '.join(found)} are this app's, run rfnry-thread upgrade --schema {target} "
            "--from that schema instead"
        )
    return f"run rfnry-thread create --schema {target}{tail}"


async def _lacking(conn: Any, role: str) -> list[str]:
    rows = [
        *await conn.fetch(_LACKING, list(_INITIALIZED), list(_WRITES)),
        *await conn.fetch(_LACKING, ["thread_migrations"], ["SELECT"]),
    ]
    if not rows:
        return []
    privileges = list(dict.fromkeys(row["privilege"] for row in rows))
    tables = sorted({row["table_name"] for row in rows})
    return [
        f"{role} lacks {', '.join(privileges)} on {_listed(tables)} — grant it "
        f"{', '.join(_WRITES)} on them and SELECT on thread_migrations"
    ]


async def problems(conn: Any) -> list[str]:
    state = await conn.fetchrow(_STATE)
    schema, path, role = state["schema"], state["path"], state["role"]
    names = _names(path, role, schema)
    first = names[0] if names else None
    if first is not None and first != schema:
        held = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", first
        )
        if held:
            return [
                f"{role} cannot use schema {first} — grant it USAGE on the schema and SELECT, "
                "INSERT, UPDATE, DELETE on its tables"
            ]
        return [
            f"there is no schema {first} on search_path {path!r} — "
            f"{await remedy(conn, first, names)}"
        ]
    if schema is None:
        return [f"no schema on search_path {path!r} exists — {await remedy(conn, DEFAULT, names)}"]
    if not state["migrated"]:
        if state["tables"] and _shared(schema):
            return [
                f"the thread tables in {schema} were made by initialize() — run rfnry-thread "
                f"upgrade --from {schema} and put the thread schema first on the pool's search_path"
            ]
        if state["tables"]:
            return [
                f"the thread tables in {schema} were made by initialize() — "
                f"run rfnry-thread upgrade --schema {schema}"
            ]
        if _shared(schema):
            return [
                f"no thread schema on search_path {path!r} — {await remedy(conn, schema, names)}"
            ]
        return [f"no thread schema in {schema} — {await remedy(conn, schema, names)}"]
    lacking = await _lacking(conn, role)
    if lacking:
        return lacking
    version = int(await conn.fetchval("SELECT max(version) FROM thread_migrations") or 0)
    if version < LATEST:
        return [
            f"{schema} is at version {version}, this rfnry-thread needs {LATEST} — "
            f"run rfnry-thread upgrade --schema {schema}"
        ]
    if version > LATEST:
        return [_ahead(schema, version)]
    return []


def _ahead(schema: str, version: int) -> str:
    return (
        f"{schema} is at version {version}, this rfnry-thread knows {LATEST} — upgrade rfnry-thread"
    )


async def plan_create(conn: Any, *, fresh: bool = False) -> Plan:
    state = await conn.fetchrow(_STATE)
    schema = state["schema"]
    if schema is None or _shared(schema):
        raise InputError(
            f"postgres: thread is not created in {schema!r} — name a schema of its own"
        )
    if state["tables"] or state["migrated"]:
        raise ConfigurationError(
            f"postgres: {schema} already holds thread tables — run rfnry-thread upgrade, "
            "or create into another schema"
        )
    found = [row["name"] for row in await conn.fetch(_ELSEWHERE, schema)]
    shared = [name for name in found if _shared(name)]
    if shared and not fresh:
        raise ConfigurationError(
            f"postgres: the thread tables initialize() made are in {shared[0]} — run "
            f"rfnry-thread upgrade --schema {schema} --from {shared[0]} to keep their threads, or "
            "create with --fresh to start without them"
        )
    return _steps(MIGRATIONS)


async def plan_upgrade(conn: Any, *, source: str | None = None) -> Plan:
    state = await conn.fetchrow(_STATE)
    schema = state["schema"]
    moves = await _moves(conn, state, source) if source is not None else Plan([], [])
    if not state["migrated"] and not (state["tables"] or moves.steps):
        raise ConfigurationError(
            f"postgres: no thread schema in {schema} — {await remedy(conn, schema)}"
        )
    if not state["migrated"]:
        rest = _steps(MIGRATIONS[1:])
        return Plan(
            [*moves.steps, "0001 create, over the tables initialize() made", *rest.steps],
            [
                *moves.statements,
                *MIGRATIONS[0].statements,
                _recorded(MIGRATIONS[0]),
                *rest.statements,
            ],
        )
    rows = await conn.fetch("SELECT version FROM thread_migrations ORDER BY version")
    applied = [int(row["version"]) for row in rows]
    newest = max(applied, default=0)
    if newest > LATEST:
        raise ConfigurationError(f"postgres: {_ahead(schema, newest)}")
    pending = [m for m in MIGRATIONS if m.version not in applied]
    return _steps(pending) if pending else Plan([], [])


async def _moves(conn: Any, state: Any, source: str) -> Plan:
    held = await conn.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename = ANY($2::text[]) "
        "ORDER BY tablename",
        source,
        list(_INITIALIZED),
    )
    names = [row["tablename"] for row in held]
    if "thread_threads" not in names and state["migrated"]:
        return Plan([], [])
    if "thread_threads" not in names:
        raise ConfigurationError(f"postgres: {source} holds no thread tables initialize() made")
    migrated = await conn.fetchval(
        "SELECT to_regclass(quote_ident($1) || '.thread_migrations') IS NOT NULL", source
    )
    if migrated:
        raise ConfigurationError(
            f"postgres: {source} is a thread schema of its own — point the pool at it"
        )
    if state["tables"] or state["migrated"]:
        raise ConfigurationError(
            f"postgres: {state['schema']} already holds thread tables — "
            "move them into a schema of their own"
        )
    target = await conn.fetchval("SELECT quote_ident(current_schema())")
    origin = await conn.fetchval("SELECT quote_ident($1)", source)
    return Plan(
        [f"move {', '.join(names)} from {source}"],
        [f"ALTER TABLE {origin}.{name} SET SCHEMA {target}" for name in names],
    )


async def apply(
    conn: Any,
    planner: Planner,
    *,
    run: bool = True,
    expected: Plan | None = None,
    before: Sequence[str] = (),
) -> Plan:
    transaction = conn.transaction(isolation="read_committed")
    await transaction.start()
    try:
        for made in before:
            await conn.execute(made)
        await conn.execute(LOCK)
        planned = await planner(conn)
        plan = Plan(planned.steps, [*before, *planned.statements])
        if expected is not None and plan != expected:
            raise ConfigurationError(
                "postgres: the schema changed since the plan was shown — run the command again"
            )
        if run:
            for statement in planned.statements:
                await _execute(conn, statement)
    except BaseException:
        await transaction.rollback()
        raise
    if run:
        await transaction.commit()
    else:
        await transaction.rollback()
    return plan


async def _execute(conn: Any, statement: str) -> None:
    try:
        await conn.execute(statement)
    except Exception as exc:
        raise StorageError(f"postgres: {statement.split(chr(10))[0]} failed: {exc}") from exc
