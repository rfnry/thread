import asyncio
import contextlib
import hashlib
import json
import random
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Literal, cast

from rfnry_thread import migrations
from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    NotFoundError,
    StorageError,
    ThreadException,
)
from rfnry_thread.models import (
    EVENT_KINDS,
    EVENTS,
    Edit,
    Event,
    Member,
    MemberAdded,
    MemberRemoved,
    Message,
    Order,
    Thread,
    ThreadUpdated,
)
from rfnry_thread.resolve import last_message
from rfnry_thread.storage import PREVIEW_DEPTH, UNREAD_CAP, OnEvent, OnResume, OnSignal, Stop

EVENTS_PREFIX = "thread_events_"
SIGNALS_PREFIX = "thread_signals_"
NOTIFY_LIMIT = 7900
NOTIFY_TIMEOUT = 10.0
RELISTEN = 0.5
RELISTEN_CAP = 30.0
_THREAD = "scope, id, metadata, created_at, history, epoch"
_MAKE = "scope, id, metadata, created_at, history"
_Channel = Literal["events", "signals"]
_Notice = tuple[_Channel, str, asyncio.Future[None]]


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _thread(row: Any) -> Thread:
    return Thread(
        id=row["id"],
        scope=row["scope"],
        metadata=_json(row["metadata"]),
        created_at=row["created_at"],
        history=row["history"],
        epoch=int(row["epoch"]),
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


def _visible(floor: int) -> str:
    return (
        f"e.seq >= ${floor} AND NOT (e.kind IN ('edit', 'retract') AND EXISTS (SELECT 1 "
        "FROM thread_events x WHERE x.scope = e.scope AND x.thread_id = e.thread_id "
        f"AND x.id = e.body->>'target' AND x.seq < ${floor}))"
    )


def _unchanged(thread_id: str, locked: tuple[int, int], epoch: int) -> None:
    if locked[1] != epoch:
        raise NotFoundError(f"postgres: thread {thread_id!r} was not found")


def _count(status: str) -> int:
    return int(status.rsplit(" ", 1)[-1])


def _terms(query: str) -> list[str]:
    if "\x00" in query:
        raise InputError("postgres: a query cannot hold a NUL character")
    seen: dict[str, str] = {}
    for word in query.split():
        seen.setdefault(word.lower(), word)
    words = list(seen.values())
    if not words:
        raise InputError("postgres: a query needs at least one word")
    return [
        "%" + word.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        for word in words
    ]


def _holds(column: str, first: int, count: int) -> str:
    return " AND ".join(f"{column} ILIKE ${n}" for n in range(first, first + count))


_MISSING = ("42P01", "42883")
_TEXTS_TABLE = (
    "SELECT quote_ident(current_schema()) || '.thread_texts' AS name, "
    "to_regclass(quote_ident(current_schema()) || '.thread_texts') IS NOT NULL AS held"
)


class PostgresStorage:
    def __init__(self, pool: Any) -> None:
        self._pool = pool
        self._names: tuple[str, str] | None = None
        self._texts: str | None = None
        self._outgoing: list[_Notice] = []
        self._flushing: asyncio.Task[None] | None = None

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

    async def check(self) -> None:
        async with self._guard("check"), self._pool.acquire() as conn:
            found = await migrations.problems(conn)
        if found:
            raise ConfigurationError(f"postgres: {'; '.join(found)}")

    async def create_thread(
        self, scope: str, thread: Thread, creator: MemberAdded | None = None
    ) -> tuple[Thread, bool]:
        if thread.scope != scope:
            raise StorageError("postgres: row scope does not match the call scope")
        if creator is not None and creator.thread_id != thread.id:
            raise StorageError("postgres: the creator belongs to another thread")
        async with self._guard("create_thread"), self._pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                f"INSERT INTO thread_threads ({_MAKE}) VALUES ($1, $2, $3::jsonb, $4, $5) "
                "ON CONFLICT (scope, id) DO UPDATE SET scope = EXCLUDED.scope "
                f"RETURNING {_THREAD}, last_seq, (xmax = 0) AS created",
                scope,
                thread.id,
                json.dumps(thread.metadata),
                thread.created_at,
                thread.history,
            )
            if row["created"] and creator is not None:
                stored = await self._append(conn, scope, creator, int(row["last_seq"]))
                await self._admitted(conn, scope, creator.created_at, stored)
        return _thread(row), bool(row["created"])

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
        before: str | Thread | None = None,
        limit: int = 50,
        metadata: dict[str, Any] | None = None,
        order: Order = "created",
        preview: bool = False,
        query: str | None = None,
    ) -> list[Thread]:
        terms = None if query is None else _terms(query)
        args: list[Any] = [scope]
        member = ""
        join = ""
        floor = "0"
        marks = "NULL::bigint AS marked, NULL::bigint AS unread, 0::bigint AS floor"
        if member_id is not None:
            args.append(member_id)
            who = f"${len(args)}"
            args.append(UNREAD_CAP)
            member = (
                "JOIN thread_members m ON m.scope = t.scope AND m.thread_id = t.id "
                f"AND m.identity_id = {who} "
            )
            join = (
                f"{member}LEFT JOIN thread_marks k ON k.scope = m.scope "
                "AND k.thread_id = m.thread_id AND k.identity_id = m.identity_id "
            )
            floor = "CASE WHEN t.history = 'since_added' THEN m.added_seq ELSE 0 END"
            marks = (
                "k.seq AS marked, CASE WHEN k.seq IS NULL THEN NULL ELSE (SELECT count(*) FROM "
                "(SELECT 1 FROM thread_events e WHERE e.scope = t.scope AND e.thread_id = t.id "
                f"AND e.seq > k.seq AND e.kind = 'message' AND e.author_id <> {who} "
                "AND NOT EXISTS (SELECT 1 FROM thread_events r WHERE r.scope = e.scope "
                "AND r.thread_id = e.thread_id AND r.kind = 'retract' "
                "AND r.body->>'target' = e.id) "
                f"LIMIT ${len(args)}) u) END AS unread, "
                f"{floor} AS floor"
            )
        where = ["t.scope = $1"]
        if metadata:
            args.append(json.dumps(metadata))
            where.append(f"t.metadata @> ${len(args)}::jsonb")
        key = "t.created_at" if order == "created" else "COALESCE(t.last_message_at, t.created_at)"
        if isinstance(before, Thread) and before.last_seq is None:
            before = before.id
        async with self._guard("list_threads"), self._pool.acquire() as conn:
            if isinstance(before, str):
                row = await conn.fetchrow(
                    "SELECT t.id, t.created_at, t.last_message_at FROM thread_threads t "
                    "WHERE t.scope = $1 AND t.id = $2 AND ($3::text IS NULL OR EXISTS "
                    "(SELECT 1 FROM thread_members m WHERE m.scope = t.scope "
                    "AND m.thread_id = t.id AND m.identity_id = $3))",
                    scope,
                    before,
                    member_id,
                )
                if row is None:
                    raise NotFoundError(f"postgres: thread {before!r} was not found")
                before = Thread(
                    id=row["id"],
                    scope=scope,
                    created_at=row["created_at"],
                    last_message_at=row["last_message_at"],
                )
            if before is not None:
                latest = before.last_message_at if order == "activity" else None
                args.extend([latest or before.created_at, before.id])
                where.append(f"({key}, t.id) < (${len(args) - 1}::timestamptz, ${len(args)}::text)")
            args.append(max(limit, 0))
            size = f"${len(args)}"
            columns = (
                "t.scope, t.id, t.metadata, t.created_at, t.history, t.epoch, t.last_seq, "
                f"t.last_message_at, {marks}"
            )
            ordered = f"ORDER BY {key} DESC, t.id DESC LIMIT {size}"
            if terms is None:
                rows = await conn.fetch(
                    f"SELECT {columns} FROM thread_threads t {join}"
                    f"WHERE {' AND '.join(where)} {ordered}",
                    *args,
                )
                listed = [(_listed(row), int(row["floor"])) for row in rows]
            else:
                listed = await self._found_threads(
                    conn, scope, terms, args, where, member, join, columns, floor, ordered
                )
            threads = [thread for thread, _ in listed]
            if preview and threads:
                floors = [floor for _, floor in listed]
                threads = await self._previewed(conn, scope, threads, floors)
        return threads

    async def _found_threads(
        self,
        conn: Any,
        scope: str,
        terms: list[str],
        args: list[Any],
        where: list[str],
        member: str,
        join: str,
        columns: str,
        floor: str,
        ordered: str,
    ) -> list[Any]:
        first = len(args) + 1
        args.extend(terms)
        hit = (
            f"SELECT x.seq FROM {await self._held(conn)} x WHERE x.scope = t.scope "
            f"AND x.thread_id = t.id AND {_holds('x.text', first, len(terms))}"
        )
        rows, found = await self._matched(
            conn,
            scope,
            f"WITH picked AS MATERIALIZED (SELECT t.id FROM thread_threads t {member}"
            f"WHERE {' AND '.join(where)} AND EXISTS ({hit} "
            f"AND (x.seq = 0 OR x.seq >= {floor})) {ordered}) "
            f"SELECT {columns}, h.seq AS hit FROM thread_threads t {join}"
            f"LEFT JOIN LATERAL ({hit} AND x.seq >= GREATEST({floor}, 1) "
            "ORDER BY x.seq DESC LIMIT 1) h ON true "
            f"WHERE t.scope = $1 AND t.id IN (SELECT id FROM picked) {ordered}",
            args,
            lambda listed: [
                (row["id"], int(row["hit"])) for row in listed if row["hit"] is not None
            ],
        )
        return [
            (
                _listed(row).model_copy(update={"match": found.get((row["id"], row["hit"]))}),
                int(row["floor"]),
            )
            for row in rows
        ]

    async def _previewed(
        self, conn: Any, scope: str, threads: list[Thread], floors: list[int]
    ) -> list[Thread]:
        rows = await conn.fetch(
            "SELECT p.id AS thread_id, e.body FROM unnest($2::text[], $4::bigint[]) AS p(id, floor) "
            "CROSS JOIN LATERAL (SELECT x.body FROM thread_events x WHERE x.scope = $1 "
            "AND x.thread_id = p.id AND x.kind IN ('message', 'edit', 'retract') "
            "AND x.seq >= GREATEST(p.floor, COALESCE((SELECT min(y.seq) "
            "FROM (SELECT seq FROM thread_events WHERE scope = $1 AND thread_id = p.id "
            "AND kind = 'message' AND seq >= p.floor ORDER BY seq DESC LIMIT $3) y), 0))) e",
            scope,
            [thread.id for thread in threads],
            PREVIEW_DEPTH,
            floors,
        )
        found: dict[str, list[Event]] = {}
        for row in rows:
            found.setdefault(row["thread_id"], []).append(_event(row["body"]))
        return [
            t.model_copy(update={"last_message": last_message(found.get(t.id, []))})
            for t in threads
        ]

    async def _held(self, conn: Any) -> str:
        if self._texts is None:
            row = await conn.fetchrow(_TEXTS_TABLE)
            if row["name"] is None:
                raise StorageError("postgres: no schema on the search_path exists")
            self._texts = str(row["name"])
        return self._texts

    async def _matched(
        self,
        conn: Any,
        scope: str,
        sql: str,
        args: Sequence[Any],
        hits: Callable[[list[Any]], list[tuple[str, int]]],
    ) -> tuple[list[Any], dict[tuple[str, int], Message]]:
        try:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                await conn.execute("SET LOCAL plan_cache_mode = force_custom_plan")
                rows = list(await conn.fetch(sql, *args))
                return rows, await self._current(conn, scope, hits(rows))
        except Exception as exc:
            if getattr(exc, "sqlstate", None) not in _MISSING:
                raise
            schema = await conn.fetchval("SELECT current_schema()")
            made, _ = await migrations.search_state(conn)
            if made:
                raise
            state = "half set up" if made is None else "not set up"
            raise ConfigurationError(
                f"postgres: search is {state} in {schema} — "
                f"run rfnry-thread set --search --schema {schema}"
            ) from exc

    async def _current(
        self, conn: Any, scope: str, hits: list[tuple[str, int]]
    ) -> dict[tuple[str, int], Message]:
        if not hits:
            return {}
        rows = await conn.fetch(
            "SELECT e.thread_id, e.seq, e.body, (SELECT d.body FROM thread_events d "
            "WHERE d.scope = e.scope AND d.thread_id = e.thread_id AND d.kind = 'edit' "
            "AND d.body->>'target' = e.id ORDER BY d.seq DESC LIMIT 1) AS edit "
            "FROM unnest($2::text[], $3::bigint[]) AS p(id, seq) JOIN thread_events e "
            "ON e.scope = $1 AND e.thread_id = p.id AND e.seq = p.seq",
            scope,
            [thread_id for thread_id, _ in hits],
            [seq for _, seq in hits],
        )
        found: dict[tuple[str, int], Message] = {}
        for row in rows:
            message = _event(row["body"])
            edit = None if row["edit"] is None else _event(row["edit"])
            if isinstance(edit, Edit):
                message = message.model_copy(update={"content": edit.content})
            if isinstance(message, Message):
                found[(row["thread_id"], int(row["seq"]))] = message
        return found

    async def search(
        self,
        scope: str,
        thread_id: str,
        query: str,
        before: int | None = None,
        limit: int = 50,
        floor: int = 0,
    ) -> list[Message]:
        terms = _terms(query)
        if limit < 1:
            return []
        args: list[Any] = [scope, thread_id, floor]
        where = ["x.scope = $1", "x.thread_id = $2", "x.seq >= GREATEST($3::bigint, 1)"]
        if before is not None:
            args.append(before)
            where.append(f"x.seq < ${len(args)}")
        args.append(limit)
        size = len(args)
        where.append(_holds("x.text", size + 1, len(terms)))
        async with self._guard("search"), self._pool.acquire() as conn:
            _, found = await self._matched(
                conn,
                scope,
                f"SELECT x.seq FROM {await self._held(conn)} x WHERE {' AND '.join(where)} "
                f"ORDER BY x.seq DESC LIMIT ${size}",
                [*args, *terms],
                lambda rows: [(thread_id, int(row["seq"])) for row in rows],
            )
        return [found[key] for key in sorted(found)]

    async def update_thread(
        self, scope: str, updated: ThreadUpdated, epoch: int
    ) -> tuple[Thread, ThreadUpdated] | None:
        async with self._guard("update_thread"), self._pool.acquire() as conn, conn.transaction():
            locked = await self._locked(conn, scope, updated.thread_id)
            if locked is None:
                return None
            _unchanged(updated.thread_id, locked, epoch)
            stored = await self._append(conn, scope, updated, locked[0])
            row = await conn.fetchrow(
                "UPDATE thread_threads SET metadata = $3::jsonb WHERE scope = $1 AND id = $2 "
                f"RETURNING {_THREAD}",
                scope,
                updated.thread_id,
                json.dumps(updated.metadata),
            )
        return _thread(row), stored

    async def delete_thread(self, scope: str, thread_id: str) -> int | None:
        async with self._guard("delete_thread"), self._pool.acquire() as conn:
            epoch = await conn.fetchval(
                "DELETE FROM thread_threads WHERE scope = $1 AND id = $2 RETURNING epoch",
                scope,
                thread_id,
            )
        return None if epoch is None else int(epoch)

    async def add_member(
        self, scope: str, added: MemberAdded, epoch: int
    ) -> tuple[Member, MemberAdded | None]:
        async with self._guard("add_member"), self._pool.acquire() as conn, conn.transaction():
            locked = await self._locked(conn, scope, added.thread_id)
            if locked is None:
                raise NotFoundError(f"postgres: thread {added.thread_id!r} was not found")
            _unchanged(added.thread_id, locked, epoch)
            existing = await conn.fetchval(
                "SELECT body FROM thread_members "
                "WHERE scope = $1 AND thread_id = $2 AND identity_id = $3",
                scope,
                added.thread_id,
                added.identity.id,
            )
            if existing is not None:
                return Member.model_validate(_json(existing)), None
            stored = await self._append(conn, scope, added, locked[0])
            member = await self._admitted(conn, scope, added.created_at, stored)
        return member, stored

    async def remove_member(
        self, scope: str, removed: MemberRemoved, epoch: int
    ) -> MemberRemoved | None:
        async with self._guard("remove_member"), self._pool.acquire() as conn, conn.transaction():
            locked = await self._locked(conn, scope, removed.thread_id)
            if locked is None:
                raise NotFoundError(f"postgres: thread {removed.thread_id!r} was not found")
            _unchanged(removed.thread_id, locked, epoch)
            body = await conn.fetchval(
                "DELETE FROM thread_members WHERE scope = $1 AND thread_id = $2 "
                "AND identity_id = $3 RETURNING body",
                scope,
                removed.thread_id,
                removed.identity.id,
            )
            if body is None:
                return None
            member = Member.model_validate(_json(body))
            stamped = removed.model_copy(update={"identity": member.identity})
            stored = await self._append(conn, scope, stamped, locked[0])
        return stored

    async def _locked(self, conn: Any, scope: str, thread_id: str) -> tuple[int, int] | None:
        row = await conn.fetchrow(
            "SELECT last_seq, epoch FROM thread_threads WHERE scope = $1 AND id = $2 FOR UPDATE",
            scope,
            thread_id,
        )
        return None if row is None else (int(row["last_seq"]), int(row["epoch"]))

    async def _admitted(self, conn: Any, scope: str, at: datetime, added: MemberAdded) -> Member:
        member = Member(
            thread_id=added.thread_id,
            identity=added.identity,
            added_at=at,
            added_by=added.author,
        )
        await conn.execute(
            "INSERT INTO thread_members (scope, thread_id, identity_id, added_at, body, added_seq) "
            "VALUES ($1, $2, $3, $4, $5::jsonb, $6)",
            scope,
            member.thread_id,
            member.identity.id,
            member.added_at,
            member.model_dump_json(),
            added.seq,
        )
        await conn.execute(
            "INSERT INTO thread_marks (scope, thread_id, identity_id, seq, at) "
            "VALUES ($1, $2, $3, $4, now())",
            scope,
            member.thread_id,
            member.identity.id,
            added.seq - 1,
        )
        return member

    async def list_members(self, scope: str, thread_id: str) -> list[Member]:
        async with self._guard("list_members"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT body FROM thread_members WHERE scope = $1 AND thread_id = $2 ORDER BY added_at, identity_id",
                scope,
                thread_id,
            )
        return [Member.model_validate(_json(row["body"])) for row in rows]

    async def head(
        self, scope: str, thread_id: str, identity_id: str
    ) -> tuple[int, int, int] | None:
        async with self._guard("head"), self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT t.last_seq, t.epoch, CASE WHEN t.history = 'since_added' THEN m.added_seq "
                "ELSE 0 END AS floor FROM thread_threads t JOIN thread_members m "
                "ON m.scope = t.scope AND m.thread_id = t.id "
                "WHERE t.scope = $1 AND t.id = $2 AND m.identity_id = $3",
                scope,
                thread_id,
                identity_id,
            )
        return None if row is None else (int(row["last_seq"]), int(row["floor"]), int(row["epoch"]))

    async def heads(
        self, keys: Sequence[tuple[str, str]]
    ) -> dict[tuple[str, str], tuple[int, int]]:
        async with self._guard("heads"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT t.scope, t.id, t.last_seq, t.epoch "
                "FROM unnest($1::text[], $2::text[]) AS k(scope, id) "
                "JOIN thread_threads t ON t.scope = k.scope AND t.id = k.id",
                [scope for scope, _ in keys],
                [thread_id for _, thread_id in keys],
            )
        return {
            (row["scope"], row["id"]): (int(row["last_seq"]), int(row["epoch"])) for row in rows
        }

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

    async def append_event(self, scope: str, event: Event, epoch: int) -> tuple[Event, bool]:
        async with self._guard("append_event"), self._pool.acquire() as conn, conn.transaction():
            locked = None
            if event.author.role != "system":
                row = await conn.fetchrow(
                    "SELECT t.last_seq, t.epoch FROM thread_threads t JOIN thread_members m "
                    "ON m.scope = t.scope AND m.thread_id = t.id "
                    "WHERE t.scope = $1 AND t.id = $2 AND m.identity_id = $3 "
                    "FOR UPDATE OF t FOR KEY SHARE OF m",
                    scope,
                    event.thread_id,
                    event.author.id,
                )
                locked = None if row is None else (int(row["last_seq"]), int(row["epoch"]))
            if locked is None:
                locked = await self._locked(conn, scope, event.thread_id)
                if locked is None:
                    raise NotFoundError(f"postgres: thread {event.thread_id!r} was not found")
                if locked[1] == epoch and event.author.role != "system":
                    raise ForbiddenError(
                        f"postgres: {event.author.id!r} is not a member of thread "
                        f"{event.thread_id!r}"
                    )
            _unchanged(event.thread_id, locked, epoch)
            existing = await conn.fetchval(
                "SELECT body FROM thread_events WHERE scope = $1 AND thread_id = $2 AND author_id = $3 AND client_id = $4",
                scope,
                event.thread_id,
                event.author.id,
                event.client_id,
            )
            if existing is not None:
                return _event(existing), False
            return await self._append(conn, scope, event, locked[0]), True

    async def _append[E: Event](self, conn: Any, scope: str, event: E, last: int) -> E:
        stored = cast(E, event.model_copy(update={"seq": last + 1}))
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
        return stored

    async def list_events(
        self,
        scope: str,
        thread_id: str,
        since: int | None = None,
        before: int | None = None,
        limit: int = 50,
        kinds: Sequence[str] | None = None,
        floor: int = 0,
    ) -> list[Event]:
        if limit < 1:
            return []
        where = ["e.scope = $1", "e.thread_id = $2"]
        args: list[Any] = [scope, thread_id]
        if floor > 0:
            args.append(floor)
            where.append(_visible(len(args)))
        if since is not None:
            args.append(since)
            where.append(f"e.seq > ${len(args)}")
        if before is not None:
            args.append(before)
            where.append(f"e.seq < ${len(args)}")
        if kinds is not None:
            held = sorted(EVENT_KINDS.intersection(kinds))
            if not held:
                return []
            where.append(f"e.kind IN ({', '.join(repr(kind) for kind in held)})")
        args.append(limit)
        order = "ASC" if since is not None else "DESC"
        async with self._guard("list_events"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT e.body FROM thread_events e WHERE {' AND '.join(where)} "
                f"ORDER BY e.seq {order} LIMIT ${len(args)}",
                *args,
            )
        events = [_event(row["body"]) for row in rows]
        return events if since is not None else events[::-1]

    async def tail(
        self, scope: str, thread_id: str, since: int, floor: int, limit: int
    ) -> tuple[int, int, list[Event]] | None:
        async with self._guard("tail"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT t.last_seq, t.epoch, e.body FROM thread_threads t "
                "LEFT JOIN LATERAL (SELECT e.seq, e.body FROM thread_events e "
                "WHERE e.scope = t.scope AND e.thread_id = t.id AND e.seq > $3 "
                f"AND e.seq <= t.last_seq AND {_visible(4)} ORDER BY e.seq LIMIT $5) e ON true "
                "WHERE t.scope = $1 AND t.id = $2 ORDER BY e.seq",
                scope,
                thread_id,
                since,
                floor,
                max(limit, 0),
            )
        if not rows:
            return None
        events = [_event(row["body"]) for row in rows if row["body"] is not None]
        return int(rows[0]["last_seq"]), int(rows[0]["epoch"]), events

    async def get_event(
        self, scope: str, thread_id: str, event_id: str, floor: int = 0
    ) -> Event | None:
        async with self._guard("get_event"), self._pool.acquire() as conn:
            body = await conn.fetchval(
                "SELECT e.body FROM thread_events e WHERE e.scope = $1 AND e.thread_id = $2 "
                f"AND e.id = $3 AND {_visible(4)}",
                scope,
                thread_id,
                event_id,
                floor,
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
        self, scope: str, thread_id: str, identity_id: str, seq: int, back: bool = False
    ) -> tuple[int, int, bool]:
        async with self._guard("mark"), self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT 1 FROM thread_members "
                "WHERE scope = $1 AND thread_id = $2 AND identity_id = $3 FOR NO KEY UPDATE",
                scope,
                thread_id,
                identity_id,
            )
            row = await conn.fetchrow(
                "WITH old AS (SELECT seq FROM thread_marks "
                "WHERE scope = $1 AND thread_id = $2 AND identity_id = $3), "
                "stored AS (INSERT INTO thread_marks (scope, thread_id, identity_id, seq, at) "
                "SELECT $1, $2, $3, $4, now() WHERE EXISTS (SELECT 1 FROM thread_members "
                "WHERE scope = $1 AND thread_id = $2 AND identity_id = $3) "
                "ON CONFLICT (scope, thread_id, identity_id) DO UPDATE "
                "SET seq = CASE WHEN $6 THEN EXCLUDED.seq ELSE GREATEST(thread_marks.seq, EXCLUDED.seq) "
                "END, at = EXCLUDED.at RETURNING seq) "
                "SELECT s.seq, (SELECT count(*) FROM (SELECT 1 FROM thread_events e "
                "WHERE e.scope = $1 AND e.thread_id = $2 AND e.seq > s.seq AND e.kind = 'message' "
                "AND e.author_id <> $3 AND NOT EXISTS (SELECT 1 FROM thread_events r "
                "WHERE r.scope = e.scope AND r.thread_id = e.thread_id AND r.kind = 'retract' "
                "AND r.body->>'target' = e.id) LIMIT $5) u) AS unread, "
                "s.seq IS DISTINCT FROM (SELECT seq FROM old) AS moved FROM stored s",
                scope,
                thread_id,
                identity_id,
                seq,
                UNREAD_CAP,
                back,
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
                "WHERE m.scope = $1 AND m.identity_id = $2 ORDER BY m.thread_id FOR KEY SHARE OF m "
                "ON CONFLICT (scope, thread_id, identity_id) DO NOTHING",
                scope,
                identity_id,
            )
        return _count(status)

    async def delete_scope(self, scope: str) -> int:
        async with self._guard("delete_scope"), self._pool.acquire() as conn, conn.transaction():
            removed = 0
            await conn.execute(
                "SELECT 1 FROM thread_threads WHERE scope = $1 ORDER BY id FOR UPDATE", scope
            )
            await conn.execute(
                "SELECT 1 FROM thread_members WHERE scope = $1 "
                "ORDER BY thread_id, identity_id FOR UPDATE",
                scope,
            )
            texts = await conn.fetchrow(_TEXTS_TABLE)
            for table in (
                *([texts["name"]] if texts["held"] else []),
                "thread_marks",
                "thread_events",
                "thread_members",
                "thread_threads",
            ):
                removed += _count(
                    await conn.execute(f"DELETE FROM {table} WHERE scope = $1", scope)
                )
        return removed

    async def verify_absent(self, scope: str) -> bool:
        async with self._guard("verify_absent"), self._pool.acquire() as conn:
            texts = await conn.fetchrow(_TEXTS_TABLE)
            counts = [
                await conn.fetchval(f"SELECT count(*) FROM {table} WHERE scope = $1", scope)
                for table in (
                    "thread_threads",
                    "thread_members",
                    "thread_events",
                    "thread_marks",
                    *([texts["name"]] if texts["held"] else []),
                )
            ]
        return all(count == 0 for count in counts)

    async def listen(
        self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop:
        events_channel, signals_channel = await self._channels()
        tasks: set[asyncio.Future[None]] = set()
        lost = asyncio.Event()
        held: list[Any] = [None]
        waiting: list[list[tuple[str, str]] | None] = [None]

        def track(work: Awaitable[None]) -> None:
            future = asyncio.ensure_future(work)
            tasks.add(future)
            future.add_done_callback(tasks.discard)

        def dispatch(channel: str, text: str) -> None:
            data = json.loads(text)
            if channel == events_channel:
                track(on_event(data["s"], data["t"], int(data["q"]), int(data["e"])))
            else:
                track(on_signal(data["s"], data["t"], data["p"]))

        def receive(_connection: Any, _pid: int, channel: str, text: str) -> None:
            heard = waiting[0]
            if heard is None:
                dispatch(channel, text)
            else:
                heard.append((channel, text))

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
                waiting[0] = []
                attempt = 0
                while held[0] is None:
                    await asyncio.sleep(random.random() * min(RELISTEN_CAP, RELISTEN * 2**attempt))
                    attempt += 1
                    with contextlib.suppress(Exception):
                        await attach()
                resumed = None
                if on_resume is not None:
                    with contextlib.suppress(Exception):
                        resumed = on_resume()
                heard, waiting[0] = waiting[0] or [], None
                for channel, text in heard:
                    dispatch(channel, text)
                if resumed is not None:
                    track(resumed)

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
        await self._send("signals", text)

    async def hint(self, scope: str, thread_id: str, seq: int, epoch: int) -> None:
        await self._send("events", json.dumps({"s": scope, "t": thread_id, "q": seq, "e": epoch}))

    async def _send(self, channel: _Channel, text: str) -> None:
        loop = asyncio.get_running_loop()
        sent = loop.create_future()
        self._outgoing.append((channel, text, sent))
        if self._flushing is None or self._flushing.done():
            self._flushing = asyncio.Task(self._flush(), loop=loop, eager_start=True)
        await sent

    async def _flush(self) -> None:
        batch: list[_Notice] = []
        try:
            while self._outgoing:
                batch, self._outgoing = self._outgoing, []
                failed = await self._group(batch)
                for _, _, sent in batch:
                    if sent.done():
                        continue
                    if failed is None:
                        sent.set_result(None)
                    else:
                        sent.set_exception(failed)
        except BaseException:
            for _, _, sent in [*batch, *self._outgoing]:
                sent.cancel()
            self._outgoing = []
            raise

    async def _group(self, batch: list[_Notice]) -> Exception | None:
        try:
            async with asyncio.timeout(NOTIFY_TIMEOUT):
                events, signals = await self._channels()
                names = {"events": events, "signals": signals}
                async with self._guard("publish"), self._pool.acquire() as conn, conn.transaction():
                    await conn.executemany(
                        "SELECT pg_notify($1, $2)",
                        [(names[channel], text) for channel, text, _ in batch],
                    )
        except TimeoutError:
            return StorageError(
                f"postgres: storage rejected publish: no answer within {NOTIFY_TIMEOUT:g} s"
            )
        except Exception as error:
            return error
        return None
