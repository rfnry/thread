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


_TEXTS = "SELECT to_regclass(quote_ident(current_schema()) || '.thread_texts') IS NOT NULL"


_LACKING = (
    "SELECT t.name AS table_name, p.name AS privilege "
    "FROM unnest($1::text[]) WITH ORDINALITY AS t(name, n) "
    "CROSS JOIN unnest($2::text[]) WITH ORDINALITY AS p(name, m) "
    "WHERE NOT has_table_privilege(quote_ident(current_schema()) || '.' || t.name, p.name) "
    "ORDER BY p.m, t.name"
)


_SEARCH_STATE = (
    "SELECT (SELECT quote_ident(n.nspname) FROM pg_extension e JOIN pg_namespace n "
    "ON n.oid = e.extnamespace WHERE e.extname = 'pg_trgm') AS trigram, "
    "ARRAY[to_regprocedure(quote_ident(current_schema()) || '.thread_text(jsonb)') IS NOT NULL, "
    "to_regprocedure(quote_ident(current_schema()) || '.thread_strings(jsonb)') IS NOT NULL, "
    "to_regprocedure(quote_ident(current_schema()) || '.thread_texts_follow()') IS NOT NULL, "
    "to_regprocedure(quote_ident(current_schema()) || '.thread_texts_name()') IS NOT NULL, "
    "to_regclass(quote_ident(current_schema()) || '.thread_texts') IS NOT NULL, "
    "to_regclass(quote_ident(current_schema()) || '.ix_thread_texts_trgm') IS NOT NULL, "
    "EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid "
    "WHERE t.tgname = 'thread_texts_follow' AND t.tgenabled <> 'D' "
    "AND t.tgrelid = to_regclass(quote_ident(current_schema()) || '.thread_events') "
    "AND p.proconfig = ARRAY['search_path=' || quote_ident(current_schema())]), "
    "EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid "
    "WHERE t.tgname = 'thread_texts_name' AND t.tgenabled <> 'D' "
    "AND t.tgrelid = to_regclass(quote_ident(current_schema()) || '.thread_threads') "
    "AND p.proconfig = ARRAY['search_path=' || quote_ident(current_schema())])] AS made"
)


SEARCH_ON = (
    "search: thread_texts filled from the log and the threads' metadata, kept current by "
    "triggers, with a trigram index"
)


SEARCH_OFF = "no search: thread_texts, its triggers and index dropped"


_GRANTS = (
    "SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(a.grantee)) "
    "END AS grantee, string_agg(a.privilege_type, ', ' ORDER BY a.privilege_type) AS privileges "
    "FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a "
    "WHERE c.oid = to_regclass(quote_ident(current_schema()) || '.thread_events') "
    "AND a.grantee <> c.relowner AND a.privilege_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE') "
    "GROUP BY 1 ORDER BY 1"
)


_OWNER = (
    "SELECT quote_ident(pg_get_userbyid(relowner)) FROM pg_class "
    "WHERE oid = to_regclass(quote_ident(current_schema()) || '.thread_events') "
    "AND pg_get_userbyid(relowner) <> current_user"
)


_SEALED = "LOCK TABLE thread_threads, thread_events IN ACCESS EXCLUSIVE MODE"


_HELD = (
    "LOCK TABLE thread_threads IN EXCLUSIVE MODE",
    "LOCK TABLE thread_events IN SHARE ROW EXCLUSIVE MODE",
)


_UNSEARCHED = (
    "DROP TRIGGER IF EXISTS thread_texts_follow ON thread_events",
    "DROP TRIGGER IF EXISTS thread_texts_name ON thread_threads",
    "DROP TABLE IF EXISTS thread_texts",
    "DROP FUNCTION IF EXISTS thread_texts_follow()",
    "DROP FUNCTION IF EXISTS thread_texts_name()",
    "DROP FUNCTION IF EXISTS thread_strings(jsonb)",
    "DROP FUNCTION IF EXISTS thread_text(jsonb)",
)


def _searched(schema: str, trigram: str) -> list[str]:
    return [
        "CREATE FUNCTION thread_text(body jsonb) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE "
        "BEGIN ATOMIC SELECT coalesce(string_agg(p.part->>'text', E'\\n' ORDER BY p.n), '') "
        "FROM jsonb_array_elements(CASE WHEN jsonb_typeof(body->'content') = 'array' "
        "THEN body->'content' ELSE '[]'::jsonb END) WITH ORDINALITY AS p(part, n) "
        "WHERE p.part->>'type' = 'text'; END",
        "CREATE FUNCTION thread_strings(metadata jsonb) RETURNS text LANGUAGE sql IMMUTABLE "
        "PARALLEL SAFE BEGIN ATOMIC SELECT coalesce(string_agg(v.value #>> '{}', E'\\n'), '') "
        "FROM jsonb_path_query(metadata, 'strict $.** ? (@.type() == \"string\")') AS v(value); "
        "END",
        """CREATE TABLE thread_texts (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, seq BIGINT NOT NULL, text TEXT NOT NULL,
        PRIMARY KEY (scope, thread_id, seq))""",
        f"""CREATE FUNCTION thread_texts_follow() RETURNS trigger LANGUAGE plpgsql
        SET search_path = {schema} AS $follow$
        BEGIN
            IF NEW.kind = 'message' THEN
                INSERT INTO thread_texts (scope, thread_id, seq, text)
                VALUES (NEW.scope, NEW.thread_id, NEW.seq, thread_text(NEW.body));
            ELSIF NEW.kind = 'edit' THEN
                UPDATE thread_texts x SET text = thread_text(NEW.body) FROM thread_events e
                WHERE e.scope = NEW.scope AND e.thread_id = NEW.thread_id
                AND e.id = NEW.body->>'target' AND x.scope = e.scope
                AND x.thread_id = e.thread_id AND x.seq = e.seq;
            ELSE
                DELETE FROM thread_texts x USING thread_events e
                WHERE e.scope = NEW.scope AND e.thread_id = NEW.thread_id
                AND e.id = NEW.body->>'target' AND x.scope = e.scope
                AND x.thread_id = e.thread_id AND x.seq = e.seq;
            END IF;
            RETURN NULL;
        END
        $follow$""",
        "CREATE TRIGGER thread_texts_follow AFTER INSERT ON thread_events FOR EACH ROW "
        "WHEN (NEW.kind IN ('message', 'edit', 'retract')) EXECUTE FUNCTION thread_texts_follow()",
        f"""CREATE FUNCTION thread_texts_name() RETURNS trigger LANGUAGE plpgsql
        SET search_path = {schema} AS $name$
        BEGIN
            INSERT INTO thread_texts (scope, thread_id, seq, text)
            VALUES (NEW.scope, NEW.id, 0, thread_strings(NEW.metadata))
            ON CONFLICT (scope, thread_id, seq) DO UPDATE SET text = EXCLUDED.text;
            RETURN NULL;
        END
        $name$""",
        "CREATE TRIGGER thread_texts_name AFTER INSERT OR UPDATE OF metadata ON thread_threads "
        "FOR EACH ROW EXECUTE FUNCTION thread_texts_name()",
        "INSERT INTO thread_texts (scope, thread_id, seq, text) "
        "SELECT scope, id, 0, thread_strings(metadata) FROM thread_threads",
        """INSERT INTO thread_texts (scope, thread_id, seq, text)
        SELECT m.scope, m.thread_id, m.seq, thread_text(coalesce(d.body, m.body))
        FROM thread_events m LEFT JOIN (
            SELECT DISTINCT ON (scope, thread_id, body->>'target')
                scope, thread_id, body->>'target' AS target, body
            FROM thread_events WHERE kind = 'edit'
            ORDER BY scope, thread_id, body->>'target', seq DESC) d
        ON d.scope = m.scope AND d.thread_id = m.thread_id AND d.target = m.id
        WHERE m.kind = 'message' AND NOT EXISTS (SELECT 1 FROM thread_events r
            WHERE r.scope = m.scope AND r.thread_id = m.thread_id AND r.kind = 'retract'
            AND r.body->>'target' = m.id)""",
        "ALTER TABLE thread_texts ADD FOREIGN KEY (scope, thread_id) "
        "REFERENCES thread_threads (scope, id) ON DELETE CASCADE",
        f"CREATE INDEX ix_thread_texts_trgm ON thread_texts USING gin (text {trigram}.gin_trgm_ops)",
        "ANALYZE thread_texts",
    ]


async def search_state(conn: Any) -> tuple[bool | None, str | None]:
    row = await conn.fetchrow(_SEARCH_STATE)
    made = list(row["made"])
    return (True if all(made) else False if not any(made) else None), row["trigram"]


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
    texts = await conn.fetchval(_TEXTS)
    tables = [*_INITIALIZED, *(["thread_texts"] if texts else [])]
    rows = [
        *await conn.fetch(_LACKING, tables, list(_WRITES)),
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
    behind = await _behind(conn, schema)
    if behind is not None:
        return [behind]
    if (await search_state(conn))[0] is None:
        return [
            f"search is half set up in {schema} — run rfnry-thread set --search --schema {schema}"
        ]
    return []


async def _behind(conn: Any, schema: str) -> str | None:
    version = int(await conn.fetchval("SELECT max(version) FROM thread_migrations") or 0)
    if version < LATEST:
        return (
            f"{schema} is at version {version}, this rfnry-thread needs {LATEST} — "
            f"run rfnry-thread upgrade --schema {schema}"
        )
    if version > LATEST:
        return _ahead(schema, version)
    return None


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


async def plan_search(conn: Any, *, on: bool) -> Plan:
    state = await conn.fetchrow(_STATE)
    schema = state["schema"]
    if not state["migrated"]:
        raise ConfigurationError(
            f"postgres: no thread schema in {schema} — {await remedy(conn, schema)}"
        )
    behind = await _behind(conn, schema)
    if behind is not None:
        raise ConfigurationError(f"postgres: {behind}")
    made, trigram = await search_state(conn)
    if made is on:
        return Plan([], [])
    if not on:
        return Plan([SEARCH_OFF], [_SEALED, *_UNSEARCHED])
    target = await conn.fetchval("SELECT quote_ident(current_schema())")
    owner = await conn.fetchval(_OWNER)
    return Plan(
        [*([] if trigram else ["extension pg_trgm in public"]), SEARCH_ON],
        [
            *([] if trigram else ["CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public"]),
            *([_SEALED, *_UNSEARCHED] if made is None else _HELD),
            *_searched(target, trigram or "public"),
            *[
                f"GRANT {r['privileges']} ON thread_texts TO {r['grantee']}"
                for r in await conn.fetch(_GRANTS)
            ],
            *([] if owner is None else [f"ALTER TABLE thread_texts OWNER TO {owner}"]),
        ],
    )


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
        if statement.startswith("CREATE EXTENSION"):
            raise ConfigurationError(
                f"postgres: {statement} failed: {exc} — run it as a privileged role, then run "
                "this command again"
            ) from exc
        raise StorageError(f"postgres: {statement.split(chr(10))[0]} failed: {exc}") from exc
