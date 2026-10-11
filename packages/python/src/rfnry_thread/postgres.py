from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import random
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from rfnry_thread.errors import NotFoundError, StorageError, ThreadException
from rfnry_thread.models import EVENTS, Event, Member, Order, Thread
from rfnry_thread.resolve import last_message
from rfnry_thread.storage import PREVIEW_DEPTH, UNREAD_CAP, OnEvent, OnResume, OnSignal, Stop

EVENTS_PREFIX = "thread_events_"
SIGNALS_PREFIX = "thread_signals_"
NOTIFY_LIMIT = 7900
RELISTEN = 0.5
RELISTEN_CAP = 30.0
_LOCK = 0x7468726561640001

_DDL = [
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

_BACKFILL = (
    "UPDATE thread_threads t SET last_message_at = m.at FROM (SELECT scope, thread_id, "
    "max((body->>'created_at')::timestamptz) AS at FROM thread_events WHERE kind = 'message' "
    "GROUP BY scope, thread_id) m WHERE t.scope = m.scope AND t.id = m.thread_id"
)

_THREAD = "scope, id, metadata, created_at"


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _thread(row: Any) -> Thread:
    return Thread(
        id=row["id"],
        scope=row["scope"],
        metadata=_json(row["metadata"]),
        created_at=row["created_at"],
    )


def _listed(row: Any) -> Thread:
    return _thread(row).model_copy(
        update={
            "last_seq": int(row["last_seq"]),
            "last_message_at": row["last_message_at"],
            "marked": None if row["marked"] is None else int(row["marked"]),
            "unread": None if row["unread"] is None else int(row["unread"]),
        }
    )


def _event(body: Any) -> Event:
    return EVENTS.validate_python(_json(body))


def _count(status: str) -> int:
    return int(status.rsplit(" ", 1)[-1])


class PostgresStorage:
    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self._names: tuple[str, str] | None = None

    async def _channels(self) -> tuple[str, str]:
        if self._names is None:
            async with self._guard("channels"), self._pool.acquire() as conn:
                schema = await conn.fetchval("SELECT current_schema()")
            if schema is None:
                raise StorageError("postgres: no schema on the search_path exists")
            tag = hashlib.sha256(schema.encode()).hexdigest()[:12]
            self._names = (f"{EVENTS_PREFIX}{tag}", f"{SIGNALS_PREFIX}{tag}")
        return self._names

    @asynccontextmanager
    async def _guard(self, operation: str) -> AsyncIterator[None]:
        try:
            yield
        except ThreadException:
            raise
        except Exception as exc:
            raise StorageError(f"postgres: storage rejected {operation}: {exc}") from exc

    async def initialize(self) -> None:
        async with self._guard("initialize"), self._pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", _LOCK)
            existed = await conn.fetchval("SELECT to_regclass('thread_threads') IS NOT NULL")
            had = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE "
                "table_schema = current_schema() AND table_name = 'thread_threads' "
                "AND column_name = 'last_message_at')"
            )
            for ddl in _DDL:
                await conn.execute(ddl)
            if existed and not had:
                await conn.execute(_BACKFILL)

    async def create_thread(self, scope: str, thread: Thread) -> tuple[Thread, bool]:
        if thread.scope != scope:
            raise StorageError("postgres: row scope does not match the call scope")
        async with self._guard("create_thread"), self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"INSERT INTO thread_threads ({_THREAD}) VALUES ($1, $2, $3::jsonb, $4) "
                f"ON CONFLICT (scope, id) DO NOTHING RETURNING {_THREAD}",
                scope,
                thread.id,
                json.dumps(thread.metadata),
                thread.created_at,
            )
            if row is not None:
                return _thread(row), True
            row = await conn.fetchrow(
                f"SELECT {_THREAD} FROM thread_threads WHERE scope = $1 AND id = $2",
                scope,
                thread.id,
            )
            return _thread(row), False

    async def get_thread(self, scope: str, thread_id: str) -> Thread | None:
        async with self._guard("get_thread"), self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT {_THREAD} FROM thread_threads WHERE scope = $1 AND id = $2",
                scope,
                thread_id,
            )
        return None if row is None else _thread(row)

    async def list_threads(
        self,
        scope: str,
        member_id: str | None,
        before: str | None = None,
        limit: int = 50,
        metadata: dict[str, Any] | None = None,
        order: Order = "created",
        preview: bool = False,
    ) -> list[Thread]:
        args: list[Any] = [scope]
        join = ""
        marks = "NULL::bigint AS marked, NULL::bigint AS unread"
        if member_id is not None:
            args.append(member_id)
            who = f"${len(args)}"
            args.append(UNREAD_CAP)
            join = (
                "JOIN thread_members m ON m.scope = t.scope AND m.thread_id = t.id "
                f"AND m.identity_id = {who} LEFT JOIN thread_marks k ON k.scope = m.scope "
                "AND k.thread_id = m.thread_id AND k.identity_id = m.identity_id "
            )
            marks = (
                "k.seq AS marked, CASE WHEN k.seq IS NULL THEN NULL ELSE (SELECT count(*) FROM "
                "(SELECT 1 FROM thread_events e WHERE e.scope = t.scope AND e.thread_id = t.id "
                f"AND e.seq > k.seq AND e.kind = 'message' AND e.author_id <> {who} "
                f"LIMIT ${len(args)}) u) END AS unread"
            )
        where = ["t.scope = $1"]
        if metadata:
            args.append(json.dumps(metadata))
            where.append(f"t.metadata @> ${len(args)}::jsonb")
        key = "t.created_at" if order == "created" else "COALESCE(t.last_message_at, t.created_at)"
        anchor = (
            "a.created_at" if order == "created" else "COALESCE(a.last_message_at, a.created_at)"
        )
        if before is not None:
            args.append(before)
            where.append(
                f"({key}, t.id) < (SELECT {anchor}, a.id FROM thread_threads a "
                f"WHERE a.scope = $1 AND a.id = ${len(args)})"
            )
        args.append(max(limit, 0))
        async with self._guard("list_threads"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT t.scope, t.id, t.metadata, t.created_at, t.last_seq, t.last_message_at, "
                f"{marks} FROM thread_threads t {join}WHERE {' AND '.join(where)} "
                f"ORDER BY {key} DESC, t.id DESC LIMIT ${len(args)}",
                *args,
            )
            threads = [_listed(row) for row in rows]
            if preview and threads:
                threads = await self._previewed(conn, scope, threads)
        return threads

    async def _previewed(self, conn: Any, scope: str, threads: list[Thread]) -> list[Thread]:
        rows = await conn.fetch(
            "SELECT p.id AS thread_id, e.body FROM unnest($2::text[]) AS p(id) CROSS JOIN LATERAL "
            "(SELECT x.body FROM thread_events x WHERE x.scope = $1 AND x.thread_id = p.id "
            "AND x.kind IN ('message', 'edit', 'retract') AND x.seq >= COALESCE((SELECT min(y.seq) "
            "FROM (SELECT seq FROM thread_events WHERE scope = $1 AND thread_id = p.id "
            "AND kind = 'message' ORDER BY seq DESC LIMIT $3) y), 0)) e",
            scope,
            [thread.id for thread in threads],
            PREVIEW_DEPTH,
        )
        found: dict[str, list[Event]] = {}
        for row in rows:
            found.setdefault(row["thread_id"], []).append(_event(row["body"]))
        return [
            t.model_copy(update={"last_message": last_message(found.get(t.id, []))})
            for t in threads
        ]

    async def update_thread(
        self, scope: str, thread_id: str, metadata: dict[str, Any]
    ) -> Thread | None:
        async with self._guard("update_thread"), self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE thread_threads SET metadata = $3::jsonb WHERE scope = $1 AND id = $2 RETURNING {_THREAD}",
                scope,
                thread_id,
                json.dumps(metadata),
            )
        return None if row is None else _thread(row)

    async def delete_thread(self, scope: str, thread_id: str) -> bool:
        async with self._guard("delete_thread"), self._pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM thread_threads WHERE scope = $1 AND id = $2", scope, thread_id
            )
        return _count(status) > 0

    async def add_member(self, scope: str, member: Member) -> tuple[Member, bool]:
        async with self._guard("add_member"), self._pool.acquire() as conn:
            body = await conn.fetchval(
                "INSERT INTO thread_members (scope, thread_id, identity_id, added_at, body) "
                "SELECT $1, $2, $3, $4, $5::jsonb WHERE EXISTS "
                "(SELECT 1 FROM thread_threads WHERE scope = $1 AND id = $2) "
                "ON CONFLICT (scope, thread_id, identity_id) DO NOTHING RETURNING body",
                scope,
                member.thread_id,
                member.identity.id,
                member.added_at,
                member.model_dump_json(),
            )
            if body is not None:
                return Member.model_validate(_json(body)), True
            existing = await conn.fetchval(
                "SELECT body FROM thread_members WHERE scope = $1 AND thread_id = $2 AND identity_id = $3",
                scope,
                member.thread_id,
                member.identity.id,
            )
        if existing is None:
            raise NotFoundError(f"postgres: thread {member.thread_id!r} was not found")
        return Member.model_validate(_json(existing)), False

    async def remove_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        async with self._guard("remove_member"), self._pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM thread_members WHERE scope = $1 AND thread_id = $2 AND identity_id = $3",
                scope,
                thread_id,
                identity_id,
            )
        return _count(status) > 0

    async def list_members(self, scope: str, thread_id: str) -> list[Member]:
        async with self._guard("list_members"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT body FROM thread_members WHERE scope = $1 AND thread_id = $2 ORDER BY added_at, identity_id",
                scope,
                thread_id,
            )
        return [Member.model_validate(_json(row["body"])) for row in rows]

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        async with self._guard("is_member"), self._pool.acquire() as conn:
            return bool(
                await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM thread_members WHERE scope = $1 AND thread_id = $2 AND identity_id = $3)",
                    scope,
                    thread_id,
                    identity_id,
                )
            )

    async def append_event(self, scope: str, event: Event) -> tuple[Event, bool]:
        events_channel, _ = await self._channels()
        async with self._guard("append_event"), self._pool.acquire() as conn, conn.transaction():
            last = await conn.fetchval(
                "SELECT last_seq FROM thread_threads WHERE scope = $1 AND id = $2 FOR UPDATE",
                scope,
                event.thread_id,
            )
            if last is None:
                raise NotFoundError(f"postgres: thread {event.thread_id!r} was not found")
            existing = await conn.fetchval(
                "SELECT body FROM thread_events WHERE scope = $1 AND thread_id = $2 AND author_id = $3 AND client_id = $4",
                scope,
                event.thread_id,
                event.author.id,
                event.client_id,
            )
            if existing is not None:
                return _event(existing), False
            stored = event.model_copy(update={"seq": int(last) + 1})
            await conn.execute(
                "INSERT INTO thread_events (scope, thread_id, seq, id, kind, author_id, client_id, body) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)",
                scope,
                stored.thread_id,
                stored.seq,
                stored.id,
                stored.kind,
                stored.author.id,
                stored.client_id,
                stored.model_dump_json(),
            )
            if stored.kind == "message":
                await conn.execute(
                    "UPDATE thread_threads SET last_seq = $3, last_message_at = $4 "
                    "WHERE scope = $1 AND id = $2",
                    scope,
                    stored.thread_id,
                    stored.seq,
                    stored.created_at,
                )
            else:
                await conn.execute(
                    "UPDATE thread_threads SET last_seq = $3 WHERE scope = $1 AND id = $2",
                    scope,
                    stored.thread_id,
                    stored.seq,
                )
            await conn.execute(
                "SELECT pg_notify($1, $2)",
                events_channel,
                json.dumps({"s": scope, "t": stored.thread_id, "q": stored.seq}),
            )
        return stored, True

    async def list_events(
        self,
        scope: str,
        thread_id: str,
        since: int | None = None,
        before: int | None = None,
        limit: int = 50,
        kinds: Sequence[str] | None = None,
    ) -> list[Event]:
        if limit < 1:
            return []
        where = ["scope = $1", "thread_id = $2"]
        args: list[Any] = [scope, thread_id]
        if since is not None:
            args.append(since)
            where.append(f"seq > ${len(args)}")
        if before is not None:
            args.append(before)
            where.append(f"seq < ${len(args)}")
        if kinds is not None:
            args.append(list(kinds))
            where.append(f"kind = ANY(${len(args)}::text[])")
        args.append(limit)
        order = "ASC" if since is not None else "DESC"
        async with self._guard("list_events"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT body FROM thread_events WHERE {' AND '.join(where)} ORDER BY seq {order} LIMIT ${len(args)}",
                *args,
            )
        events = [_event(row["body"]) for row in rows]
        return events if since is not None else events[::-1]

    async def get_event(self, scope: str, thread_id: str, event_id: str) -> Event | None:
        async with self._guard("get_event"), self._pool.acquire() as conn:
            body = await conn.fetchval(
                "SELECT body FROM thread_events WHERE scope = $1 AND thread_id = $2 AND id = $3",
                scope,
                thread_id,
                event_id,
            )
        return None if body is None else _event(body)

    async def find_event(
        self, scope: str, thread_id: str, author_id: str, client_id: str
    ) -> Event | None:
        async with self._guard("find_event"), self._pool.acquire() as conn:
            body = await conn.fetchval(
                "SELECT body FROM thread_events WHERE scope = $1 AND thread_id = $2 AND author_id = $3 AND client_id = $4",
                scope,
                thread_id,
                author_id,
                client_id,
            )
        return None if body is None else _event(body)

    async def mark(
        self, scope: str, thread_id: str, identity_id: str, seq: int
    ) -> tuple[int, int, bool]:
        async with self._guard("mark"), self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "WITH old AS (SELECT seq FROM thread_marks "
                "WHERE scope = $1 AND thread_id = $2 AND identity_id = $3), "
                "stored AS (INSERT INTO thread_marks (scope, thread_id, identity_id, seq, at) "
                "SELECT $1, $2, $3, $4, now() WHERE EXISTS (SELECT 1 FROM thread_members "
                "WHERE scope = $1 AND thread_id = $2 AND identity_id = $3) "
                "ON CONFLICT (scope, thread_id, identity_id) DO UPDATE "
                "SET seq = GREATEST(thread_marks.seq, EXCLUDED.seq), at = EXCLUDED.at RETURNING seq) "
                "SELECT s.seq, (SELECT count(*) FROM (SELECT 1 FROM thread_events e "
                "WHERE e.scope = $1 AND e.thread_id = $2 AND e.seq > s.seq AND e.kind = 'message' "
                "AND e.author_id <> $3 LIMIT $5) u) AS unread, "
                "s.seq IS DISTINCT FROM (SELECT seq FROM old) AS moved FROM stored s",
                scope,
                thread_id,
                identity_id,
                seq,
                UNREAD_CAP,
            )
        if row is None:
            raise NotFoundError(
                f"postgres: {identity_id!r} is not a member of thread {thread_id!r}"
            )
        return int(row["seq"]), int(row["unread"]), bool(row["moved"])

    async def get_mark(self, scope: str, thread_id: str, identity_id: str) -> int | None:
        async with self._guard("get_mark"), self._pool.acquire() as conn:
            seq = await conn.fetchval(
                "SELECT seq FROM thread_marks WHERE scope = $1 AND thread_id = $2 AND identity_id = $3",
                scope,
                thread_id,
                identity_id,
            )
        return None if seq is None else int(seq)

    async def baseline_marks(self, scope: str, identity_id: str) -> int:
        async with self._guard("baseline_marks"), self._pool.acquire() as conn:
            status = await conn.execute(
                "INSERT INTO thread_marks (scope, thread_id, identity_id, seq, at) "
                "SELECT m.scope, m.thread_id, m.identity_id, t.last_seq, now() "
                "FROM thread_members m JOIN thread_threads t ON t.scope = m.scope AND t.id = m.thread_id "
                "WHERE m.scope = $1 AND m.identity_id = $2 "
                "ON CONFLICT (scope, thread_id, identity_id) DO NOTHING",
                scope,
                identity_id,
            )
        return _count(status)

    async def delete_scope(self, scope: str) -> int:
        async with self._guard("delete_scope"), self._pool.acquire() as conn, conn.transaction():
            removed = 0
            for table in ("thread_marks", "thread_events", "thread_members", "thread_threads"):
                removed += _count(
                    await conn.execute(f"DELETE FROM {table} WHERE scope = $1", scope)
                )
        return removed

    async def verify_absent(self, scope: str) -> bool:
        async with self._guard("verify_absent"), self._pool.acquire() as conn:
            counts = [
                await conn.fetchval(f"SELECT count(*) FROM {table} WHERE scope = $1", scope)
                for table in ("thread_threads", "thread_members", "thread_events", "thread_marks")
            ]
        return all(count == 0 for count in counts)

    async def listen(
        self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop:
        events_channel, signals_channel = await self._channels()
        tasks: set[asyncio.Future[None]] = set()
        lost = asyncio.Event()
        held: list[Any] = [None]

        def receive(_connection: Any, _pid: int, channel: str, text: str) -> None:
            data = json.loads(text)
            if channel == events_channel:
                future = asyncio.ensure_future(on_event(data["s"], data["t"], int(data["q"])))
            else:
                future = asyncio.ensure_future(on_signal(data["s"], data["t"], data["p"]))
            tasks.add(future)
            future.add_done_callback(tasks.discard)

        def terminated(_connection: Any) -> None:
            lost.set()

        async def attach() -> None:
            conn = await self._pool.acquire()
            try:
                conn.add_termination_listener(terminated)
                await conn.add_listener(events_channel, receive)
                await conn.add_listener(signals_channel, receive)
            except BaseException:
                with contextlib.suppress(Exception):
                    await self._pool.release(conn)
                raise
            held[0] = conn

        async def release() -> None:
            conn, held[0] = held[0], None
            if conn is None:
                return
            with contextlib.suppress(Exception):
                conn.remove_termination_listener(terminated)
                await conn.remove_listener(events_channel, receive)
                await conn.remove_listener(signals_channel, receive)
            with contextlib.suppress(Exception):
                await self._pool.release(conn)

        async def relisten() -> None:
            while True:
                await lost.wait()
                lost.clear()
                await release()
                attempt = 0
                while held[0] is None:
                    await asyncio.sleep(random.random() * min(RELISTEN_CAP, RELISTEN * 2**attempt))
                    attempt += 1
                    with contextlib.suppress(Exception):
                        await attach()
                if on_resume is not None:
                    with contextlib.suppress(Exception):
                        await on_resume()

        async with self._guard("listen"):
            await attach()
        watcher = asyncio.get_running_loop().create_task(relisten())

        async def stop() -> None:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher
            await release()
            for future in list(tasks):
                future.cancel()

        return stop

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        text = json.dumps({"s": scope, "t": thread_id, "p": payload})
        size = len(text.encode())
        if size > NOTIFY_LIMIT:
            raise StorageError(
                f"postgres: a notification is at most {NOTIFY_LIMIT} bytes; this one is {size}"
            )
        _, signals_channel = await self._channels()
        async with self._guard("publish"), self._pool.acquire() as conn:
            await conn.execute("SELECT pg_notify($1, $2)", signals_channel, text)
