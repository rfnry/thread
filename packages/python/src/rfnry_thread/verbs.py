import contextlib
import json
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from pydantic import TypeAdapter, ValidationError

from rfnry_thread.errors import ConfigurationError, ForbiddenError, InputError, NotFoundError
from rfnry_thread.hub import Hub, Session
from rfnry_thread.models import (
    CLIENT_EVENTS,
    CLIENT_SIGNALS,
    EVENTS,
    SIGNALS,
    Cancel,
    Custom,
    Edit,
    Event,
    History,
    Identity,
    Joined,
    Marked,
    Member,
    MemberAdded,
    MemberRemoved,
    Message,
    Order,
    Part,
    Read,
    Retract,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Thread,
    ThreadUpdated,
    Working,
)
from rfnry_thread.storage import Storage

Authorize = Callable[[Identity, str, Thread], Awaitable[bool]]
Redact = Callable[[Identity, Thread, list[Part]], Awaitable[list[Part]]]
PARTS: TypeAdapter[list[Part]] = TypeAdapter(list[Part])

CUSTOM_LIMIT = 64 * 1024
METADATA_LIMIT = 8 * 1024
LABEL_LIMIT = 1000
QUERY_LIMIT = 1000
PAGE_LIMIT = 500
ID_LIMIT = 256
POSITION_MAX = 2**63 - 1


def now() -> datetime:
    return datetime.now(UTC)


def _nul(value: Any) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_nul(k) or _nul(v) for k, v in value.items())
    if isinstance(value, list | tuple):
        return any(_nul(item) for item in value)
    return False


def _clean(value: Any) -> None:
    if _nul(value):
        raise InputError("server: text cannot hold a NUL character")


def _sized(value: str, what: str) -> str:
    _clean(value)
    if len(value.encode()) > ID_LIMIT:
        article = "an" if what[0] in "aeiou" else "a"
        raise InputError(f"server: {article} {what} is at most {ID_LIMIT} bytes")
    return value


def _bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False).encode())


def _thread_metadata(metadata: dict[str, Any]) -> None:
    _clean(metadata)
    if _bytes(metadata) > METADATA_LIMIT:
        raise InputError(f"server: thread metadata is at most {METADATA_LIMIT} bytes")


def _limit(limit: int) -> int:
    if not 1 <= limit <= PAGE_LIMIT:
        raise InputError(f"server: limit must be between 1 and {PAGE_LIMIT}")
    return limit


def _remade(made: datetime | None, thread: Thread) -> bool:
    return made is not None and made != thread.created_at


def _query(query: str) -> str:
    if not query.split():
        raise InputError("server: a query needs at least one word")
    _clean(query)
    if len(query) > QUERY_LIMIT:
        raise InputError(f"server: a query is at most {QUERY_LIMIT} characters")
    return query


class Verbs:
    def __init__(
        self,
        storage: Storage,
        hub: Hub,
        authorize: Authorize | None,
        max_inline_bytes: int,
        redact: Redact | None = None,
    ) -> None:
        self._storage = storage
        self._hub = hub
        self._authorize = authorize
        self._max_inline = max_inline_bytes
        self._redact = redact

    async def _allow(self, actor: Identity, action: str, thread: Thread) -> None:
        if actor.role == "system" or self._authorize is None:
            return
        if not await self._authorize(actor, action, thread):
            raise ForbiddenError(f"server: {actor.id!r} may not {action} in thread {thread.id!r}")

    async def thread(self, actor: Identity, scope: str, thread_id: str, action: str) -> Thread:
        _sized(thread_id, "thread id")
        thread = await self._storage.get_thread(scope, thread_id)
        if thread is None:
            raise NotFoundError(f"server: thread {thread_id!r} was not found")
        if actor.role != "system" and not await self._storage.is_member(scope, thread_id, actor.id):
            raise ForbiddenError(f"server: {actor.id!r} is not a member of thread {thread_id!r}")
        await self._allow(actor, action, thread)
        return thread

    async def create_thread(
        self,
        actor: Identity,
        scope: str,
        thread_id: str | None,
        metadata: dict[str, Any],
        history: History = "all",
    ) -> Thread:
        if thread_id is not None and not thread_id.strip():
            raise InputError("server: a thread id cannot be empty")
        if thread_id is not None:
            _sized(thread_id, "thread id")
        _thread_metadata(metadata)
        if thread_id is not None and await self._storage.get_thread(scope, thread_id) is not None:
            return await self.thread(actor, scope, thread_id, "read")
        candidate = Thread(
            id=thread_id or uuid.uuid4().hex,
            scope=scope,
            metadata=metadata,
            created_at=now(),
            history=history,
        )
        await self._allow(actor, "create", candidate)
        creator = None if actor.role == "system" else self._added(candidate.id, actor, actor)
        thread, created = await self._storage.create_thread(scope, candidate, creator)
        if not created:
            return await self.thread(actor, scope, thread.id, "read")
        if creator is not None:
            await self._admitted(scope, thread, actor, creator.model_copy(update={"seq": 1}))
        return thread

    def _added(self, thread_id: str, identity: Identity, by: Identity) -> MemberAdded:
        return MemberAdded(
            id=uuid.uuid4().hex,
            thread_id=thread_id,
            author=by,
            created_at=now(),
            client_id=uuid.uuid4().hex,
            identity=identity,
        )

    async def _admitted(self, scope: str, thread: Thread, by: Identity, added: MemberAdded) -> None:
        await self._hub.deliver(scope, added, thread.created_at)
        await self._hub.added(scope, thread, by, added.identity, added.seq)

    async def _admit(self, scope: str, thread: Thread, identity: Identity, by: Identity) -> Member:
        member, added = await self._storage.add_member(scope, self._added(thread.id, identity, by))
        if added is not None:
            await self._admitted(scope, thread, by, added)
        return member

    async def get_thread(self, actor: Identity, scope: str, thread_id: str) -> Thread:
        return await self.thread(actor, scope, thread_id, "read")

    async def list_threads(
        self,
        actor: Identity,
        scope: str,
        before: str | Thread | None,
        limit: int,
        metadata: dict[str, Any] | None = None,
        order: Order = "created",
        preview: bool = False,
        query: str | None = None,
    ) -> list[Thread]:
        limit = _limit(limit)
        if before is not None:
            _sized(before if isinstance(before, str) else before.id, "thread id")
        _clean(metadata)
        if query is not None:
            _query(query)

        async def page(member_id: str | None, after: str | Thread | None) -> list[Thread]:
            return await self._storage.list_threads(
                scope, member_id, after, limit, metadata, order, preview, query
            )

        if actor.role == "system":
            return await page(None, before)
        if self._authorize is None:
            return await page(actor.id, before)
        readable: list[Thread] = []
        anchor: str | Thread | None = before
        while True:
            found = await page(actor.id, anchor)
            for thread in found:
                if await self._authorize(actor, "read", thread):
                    readable.append(thread)
                    if len(readable) == limit:
                        return readable
            if len(found) < limit:
                return readable
            anchor = found[-1]

    async def update_thread(
        self, actor: Identity, scope: str, thread_id: str, metadata: dict[str, Any]
    ) -> Thread:
        _thread_metadata(metadata)
        await self.thread(actor, scope, thread_id, "manage")
        event = ThreadUpdated(
            id=uuid.uuid4().hex,
            thread_id=thread_id,
            author=actor,
            created_at=now(),
            client_id=uuid.uuid4().hex,
            metadata=metadata,
        )
        result = await self._storage.update_thread(scope, event)
        if result is None:
            raise NotFoundError(f"server: thread {thread_id!r} was not found")
        thread, stored = result
        await self._hub.deliver(scope, stored, thread.created_at)
        return thread

    async def delete_thread(self, actor: Identity, scope: str, thread_id: str) -> None:
        await self.thread(actor, scope, thread_id, "delete")
        await self._storage.delete_thread(scope, thread_id)
        await self._hub.deleted(scope, thread_id, actor)

    async def add_member(
        self, actor: Identity, scope: str, thread_id: str, identity: Identity
    ) -> Member:
        if identity.role == "system":
            raise InputError("server: the system identity cannot be a member")
        _sized(identity.id, "identity id")
        _clean(identity.model_dump())
        thread = await self.thread(actor, scope, thread_id, "manage")
        return await self._admit(scope, thread, identity, actor)

    async def remove_member(
        self, actor: Identity, scope: str, thread_id: str, identity_id: str
    ) -> None:
        _sized(identity_id, "identity id")
        thread = await self.thread(actor, scope, thread_id, "manage")
        event = MemberRemoved(
            id=uuid.uuid4().hex,
            thread_id=thread_id,
            author=actor,
            created_at=now(),
            client_id=uuid.uuid4().hex,
            identity=Identity(role="user", id=identity_id),
        )
        stored = await self._storage.remove_member(scope, event)
        if stored is None:
            raise NotFoundError(f"server: {identity_id!r} is not a member of thread {thread_id!r}")
        await self._hub.deliver(scope, stored, thread.created_at)

    async def list_members(self, actor: Identity, scope: str, thread_id: str) -> list[Member]:
        await self.thread(actor, scope, thread_id, "read")
        return await self._storage.list_members(scope, thread_id)

    async def history(
        self,
        actor: Identity,
        scope: str,
        thread_id: str,
        before: int | None,
        limit: int,
        kinds: Sequence[str] | None,
        after: int | None = None,
    ) -> list[Event]:
        if after is not None and after < 0:
            raise InputError("server: after cannot be negative")
        thread = await self.thread(actor, scope, thread_id, "read")
        return await self._storage.list_events(
            scope,
            thread_id,
            since=after,
            before=before,
            limit=_limit(limit),
            kinds=kinds,
            floor=await self._floor(actor, scope, thread),
        )

    async def search(
        self,
        actor: Identity,
        scope: str,
        thread_id: str,
        query: str,
        before: int | None,
        limit: int,
    ) -> list[Message]:
        _query(query)
        thread = await self.thread(actor, scope, thread_id, "read")
        return await self._storage.search(
            scope,
            thread_id,
            query,
            before=before,
            limit=_limit(limit),
            floor=await self._floor(actor, scope, thread),
        )

    async def _floor(self, actor: Identity, scope: str, thread: Thread) -> int:
        if actor.role == "system" or thread.history == "all":
            return 0
        found = await self._storage.head(scope, thread.id, actor.id)
        if found is None:
            raise ForbiddenError(f"server: {actor.id!r} is not a member of thread {thread.id!r}")
        return found[1]

    async def get_event(self, actor: Identity, scope: str, thread_id: str, event_id: str) -> Event:
        _clean(event_id)
        thread = await self.thread(actor, scope, thread_id, "read")
        floor = await self._floor(actor, scope, thread)
        event = await self._storage.get_event(scope, thread_id, event_id, floor)
        if event is None:
            raise NotFoundError(f"server: event {event_id!r} was not found")
        return event

    async def mark(
        self, session: Session, thread_id: str, seq: int, back: bool = False
    ) -> tuple[int, int]:
        actor, scope = session.identity, session.scope
        await self.thread(actor, scope, thread_id, "read")
        found = await self._storage.head(scope, thread_id, actor.id)
        if found is None:
            raise NotFoundError(f"server: {actor.id!r} is not a member of thread {thread_id!r}")
        head = found[0]
        low = 0 if actor.role == "system" else max(found[1] - 1, 0)
        if not low <= seq <= head:
            raise InputError(f"server: seq must be between {low} and {head}")
        stored, unread, moved = await self._storage.mark(scope, thread_id, actor.id, seq, back)
        if moved:
            await self._hub.marked(
                session, Marked(thread_id=thread_id, sender=actor, seq=stored, unread=unread)
            )
        return stored, unread

    async def marked(self, actor: Identity, scope: str, thread_id: str) -> int | None:
        await self.thread(actor, scope, thread_id, "read")
        return await self._storage.get_mark(scope, thread_id, actor.id)

    async def join(
        self,
        session: Session,
        thread_id: str,
        since: int | None,
        resume: bool = False,
        away: bool = False,
        made: datetime | None = None,
    ) -> Joined:
        if since is not None and resume:
            raise InputError("server: a join takes since or resume, not both")
        if since is not None and since < 0:
            raise InputError("server: since cannot be negative")
        actor, scope = session.identity, session.scope
        thread = await self.thread(actor, scope, thread_id, "read")
        if since is not None and _remade(made, thread):
            joined = await self._hub.join(session, thread, 0, away)
            return joined.model_copy(update={"truncated": True})
        if resume:
            since = await self._storage.get_mark(scope, thread_id, actor.id)
            if since is None:
                found = await self._storage.head(scope, thread_id, actor.id)
                head = found[0] if found else 0
                with contextlib.suppress(NotFoundError):
                    since, _, _ = await self._storage.mark(scope, thread_id, actor.id, head)
            thread = thread.model_copy(update={"marked": since})
        return await self._hub.join(session, thread, since, away)

    async def follow(
        self,
        session: Session,
        resume: bool,
        since: dict[str, tuple[int, datetime | None]] | None,
        left: dict[str, tuple[int, datetime | None]] | None,
    ) -> list[Joined]:
        if since is not None and any(seq < 0 for seq, _ in since.values()):
            raise InputError("server: since cannot be negative")
        if left is not None and any(seq < 0 for seq, _ in left.values()):
            raise InputError("server: left cannot be negative")
        actor, scope = session.identity, session.scope
        previous, session.following = session.following, True
        try:
            if resume and since is None:
                await self._storage.baseline_marks(scope, actor.id)
            start, gone = since or {}, left or {}

            def position(thread: Thread) -> int | None:
                if thread.id in remade:
                    return 0
                if thread.id in start:
                    return start[thread.id][0]
                return thread.marked if resume else None

            with self._hub.watching() as touched:
                threads = [
                    t
                    for t in await self._memberships(actor, scope)
                    if t.id not in gone or await self._back(scope, actor.id, t, *gone[t.id])
                ]
                remade = {t.id for t in threads if t.id in start and _remade(start[t.id][1], t)}
                found = await self._hub.follow(
                    session, [(t, position(t)) for t in threads], touched
                )
            return [
                j.model_copy(update={"truncated": True}) if j.thread.id in remade else j
                for j in found
            ]
        except BaseException:
            session.following = previous
            raise

    async def _back(
        self, scope: str, identity_id: str, thread: Thread, seq: int, made: datetime | None
    ) -> bool:
        return (
            _remade(made, thread)
            or (thread.last_seq or 0) < seq
            or await self._added_since(scope, thread.id, identity_id, seq)
        )

    async def _added_since(self, scope: str, thread_id: str, identity_id: str, seq: int) -> bool:
        while True:
            added = await self._storage.list_events(
                scope, thread_id, since=seq, limit=PAGE_LIMIT, kinds=["member_added"]
            )
            if any(isinstance(e, MemberAdded) and e.identity.id == identity_id for e in added):
                return True
            if len(added) < PAGE_LIMIT:
                return False
            seq = added[-1].seq

    async def _memberships(self, actor: Identity, scope: str) -> list[Thread]:
        found: list[Thread] = []
        before: Thread | None = None
        while True:
            page = await self._storage.list_threads(scope, actor.id, before, PAGE_LIMIT)
            for thread in page:
                if self._authorize is None or await self._authorize(actor, "read", thread):
                    found.append(thread)
            if len(page) < PAGE_LIMIT:
                return found
            before = page[-1]

    async def leave(self, session: Session, thread_id: str) -> None:
        await self._hub.leave(session, thread_id)

    async def away(self, session: Session, thread_id: str) -> None:
        await self._hub.away(session, thread_id)

    async def send(self, actor: Identity, scope: str, payload: dict[str, Any]) -> Event:
        kind = payload.get("kind")
        if kind not in CLIENT_EVENTS:
            raise InputError(f"server: unknown event kind {kind!r}")
        thread_id = payload.get("thread_id")
        if not isinstance(thread_id, str):
            raise InputError("server: event.send needs a thread_id")
        client_id = payload.get("client_id")
        if not client_id:
            raise InputError("server: event.send needs a client_id")
        if isinstance(client_id, str):
            _sized(client_id, "client id")
        thread = await self.thread(actor, scope, thread_id, "send")
        fields = {
            k: v for k, v in payload.items() if k not in ("id", "seq", "author", "created_at")
        }
        try:
            event = EVENTS.validate_python(
                {**fields, "id": uuid.uuid4().hex, "created_at": now(), "author": actor}
            )
        except ValidationError as exc:
            raise InputError(
                f"server: invalid {kind} event: {exc.errors(include_url=False)[0]['msg'].lower()}"
            ) from exc
        await self._check(scope, actor, event, await self._floor(actor, scope, thread))
        if self._redact is not None and isinstance(event, Message | Edit):
            sent = await self._storage.find_event(scope, thread_id, actor.id, event.client_id)
            if sent is not None:
                return sent
            event = await self._redacted(self._redact, actor, thread, event)
        stored, created = await self._storage.append_event(scope, event)
        if created:
            await self._hub.deliver(scope, stored, thread.created_at)
        return stored

    async def _redacted(
        self, redact: Redact, actor: Identity, thread: Thread, event: Message | Edit
    ) -> Message | Edit:
        try:
            content = PARTS.validate_python(await redact(actor, thread, list(event.content)))
        except ValidationError as exc:
            raise ConfigurationError(
                f"server: redact must return a list of parts: {exc.errors(include_url=False)[0]['msg'].lower()}"
            ) from exc
        if not content:
            raise InputError(f"server: nothing is left of the {event.kind} once redacted")
        self._inline(content)
        return event.model_copy(update={"content": content})

    def _inline(self, content: list[Part]) -> None:
        size = sum(
            len(part.data)
            for part in content
            if not isinstance(part, Text) and part.data is not None
        )
        if size > self._max_inline:
            raise InputError(
                f"server: inline media is {size} bytes; the cap is {self._max_inline} — upload it and send a url"
            )

    async def _check(self, scope: str, actor: Identity, event: Event, floor: int) -> None:
        _clean(event.model_dump())
        if isinstance(event, Message | Edit) and not event.content:
            raise InputError(
                f"server: {'a' if isinstance(event, Message) else 'an'} {event.kind} needs content"
            )
        if isinstance(event, Read) and not 0 <= event.up_to_seq <= POSITION_MAX:
            raise InputError("server: a position is from 0 to 2**63 - 1")
        if isinstance(event, Message | Edit | Step):
            self._inline(event.output if isinstance(event, Step) else event.content)
        if isinstance(event, Custom) and _bytes(event.payload) > CUSTOM_LIMIT:
            raise InputError(f"server: a custom payload is at most {CUSTOM_LIMIT} bytes")
        if isinstance(event, Message) and _bytes(event.metadata) > METADATA_LIMIT:
            raise InputError(f"server: message metadata is at most {METADATA_LIMIT} bytes")
        if isinstance(event, Edit | Retract):
            verb = "edit" if isinstance(event, Edit) else "retract"
            target = await self._storage.get_event(scope, event.thread_id, event.target, floor)
            if target is None:
                raise NotFoundError(f"server: event {event.target!r} was not found")
            if target.author.id != actor.id:
                raise ForbiddenError(f"server: only its author can {verb} event {event.target!r}")
            if isinstance(event, Edit) and not isinstance(target, Message):
                raise InputError(
                    f"server: only a message can be edited; {event.target!r} is a {target.kind}"
                )
            if isinstance(event, Retract) and not isinstance(target, Message | Step):
                raise InputError(
                    f"server: only a message or a step can be retracted; {event.target!r} is a {target.kind}"
                )

    async def signal(self, session: Session, payload: dict[str, Any]) -> None:
        kind = payload.get("kind")
        if kind not in CLIENT_SIGNALS:
            raise InputError(f"server: unknown signal kind {kind!r}")
        thread_id = payload.get("thread_id")
        if not isinstance(thread_id, str) or thread_id not in session.joined:
            raise ForbiddenError(
                f"server: signals need a join; {session.identity.id!r} has not joined thread {thread_id!r}"
            )
        try:
            signal = SIGNALS.validate_python({**payload, "sender": session.identity})
        except ValidationError as exc:
            raise InputError(
                f"server: invalid {kind} signal: {exc.errors(include_url=False)[0]['msg'].lower()}"
            ) from exc
        if (
            isinstance(signal, Working)
            and signal.label is not None
            and len(signal.label) > LABEL_LIMIT
        ):
            raise InputError(f"server: a working label is at most {LABEL_LIMIT} characters")
        if isinstance(signal, StreamStart) and (thread_id, signal.stream_id) in session.streams:
            raise InputError(f"server: stream {signal.stream_id!r} was already started")
        if (
            isinstance(signal, StreamDelta | StreamEnd)
            and (thread_id, signal.stream_id) not in session.streams
        ):
            raise InputError(f"server: stream {signal.stream_id!r} was not started")
        if self._authorize is not None and isinstance(signal, Working | StreamStart | Cancel):
            await self.thread(session.identity, session.scope, thread_id, "send")
            if thread_id not in session.joined:
                raise ForbiddenError(
                    f"server: signals need a join; {session.identity.id!r} has not joined thread {thread_id!r}"
                )
        await self._hub.signal(session, signal)

    async def present(self, actor: Identity, scope: str, thread_id: str) -> list[Identity]:
        await self.thread(actor, scope, thread_id, "read")
        return self._hub.state.present(scope, thread_id)
