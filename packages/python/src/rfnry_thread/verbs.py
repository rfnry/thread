from __future__ import annotations

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
    Identity,
    Joined,
    Member,
    Message,
    Part,
    Retract,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Thread,
    Working,
)
from rfnry_thread.storage import Storage

Authorize = Callable[[Identity, str, Thread], Awaitable[bool]]
Redact = Callable[[Identity, Thread, list[Part]], Awaitable[list[Part]]]
PARTS: TypeAdapter[list[Part]] = TypeAdapter(list[Part])

CUSTOM_LIMIT = 64 * 1024
METADATA_LIMIT = 8 * 1024
LABEL_LIMIT = 1000
PAGE_LIMIT = 500


def now() -> datetime:
    return datetime.now(UTC)


def _limit(limit: int) -> int:
    if not 1 <= limit <= PAGE_LIMIT:
        raise InputError(f"server: limit must be between 1 and {PAGE_LIMIT}")
    return limit


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
        thread = await self._storage.get_thread(scope, thread_id)
        if thread is None:
            raise NotFoundError(f"server: thread {thread_id!r} was not found")
        if actor.role != "system" and not await self._storage.is_member(scope, thread_id, actor.id):
            raise ForbiddenError(f"server: {actor.id!r} is not a member of thread {thread_id!r}")
        await self._allow(actor, action, thread)
        return thread

    async def _append(self, scope: str, fields: dict[str, Any]) -> Event:
        event = EVENTS.validate_python({"id": uuid.uuid4().hex, "created_at": now(), **fields})
        stored, created = await self._storage.append_event(scope, event)
        if created:
            await self._hub.deliver(scope, stored)
        return stored

    async def create_thread(
        self, actor: Identity, scope: str, thread_id: str | None, metadata: dict[str, Any]
    ) -> Thread:
        if thread_id is not None and not thread_id.strip():
            raise InputError("server: a thread id cannot be empty")
        if thread_id is not None and await self._storage.get_thread(scope, thread_id) is not None:
            return await self.thread(actor, scope, thread_id, "read")
        candidate = Thread(
            id=thread_id or uuid.uuid4().hex, scope=scope, metadata=metadata, created_at=now()
        )
        await self._allow(actor, "create", candidate)
        thread, created = await self._storage.create_thread(scope, candidate)
        if not created:
            return await self.thread(actor, scope, thread.id, "read")
        if actor.role != "system":
            await self._admit(scope, thread, actor, actor)
        return thread

    async def _admit(self, scope: str, thread: Thread, identity: Identity, by: Identity) -> Member:
        member, created = await self._storage.add_member(
            scope, Member(thread_id=thread.id, identity=identity, added_at=now(), added_by=by)
        )
        if created:
            await self._append(
                scope,
                {
                    "kind": "member_added",
                    "thread_id": thread.id,
                    "author": by,
                    "client_id": uuid.uuid4().hex,
                    "identity": identity,
                },
            )
            if identity.id != by.id:
                await self._hub.invite(scope, thread, by, identity)
        return member

    async def get_thread(self, actor: Identity, scope: str, thread_id: str) -> Thread:
        return await self.thread(actor, scope, thread_id, "read")

    async def list_threads(
        self,
        actor: Identity,
        scope: str,
        before: str | None,
        limit: int,
        metadata: dict[str, Any] | None = None,
    ) -> list[Thread]:
        limit = _limit(limit)
        if actor.role == "system":
            return await self._storage.list_threads(scope, None, before, limit, metadata)
        if self._authorize is None:
            return await self._storage.list_threads(scope, actor.id, before, limit, metadata)
        readable: list[Thread] = []
        while True:
            page = await self._storage.list_threads(scope, actor.id, before, limit, metadata)
            for thread in page:
                if await self._authorize(actor, "read", thread):
                    readable.append(thread)
                    if len(readable) == limit:
                        return readable
            if len(page) < limit:
                return readable
            before = page[-1].id

    async def update_thread(
        self, actor: Identity, scope: str, thread_id: str, metadata: dict[str, Any]
    ) -> Thread:
        await self.thread(actor, scope, thread_id, "manage")
        updated = await self._storage.update_thread(scope, thread_id, metadata)
        if updated is None:
            raise NotFoundError(f"server: thread {thread_id!r} was not found")
        await self._append(
            scope,
            {
                "kind": "thread_updated",
                "thread_id": thread_id,
                "author": actor,
                "client_id": uuid.uuid4().hex,
                "metadata": metadata,
            },
        )
        return updated

    async def delete_thread(self, actor: Identity, scope: str, thread_id: str) -> None:
        await self.thread(actor, scope, thread_id, "delete")
        await self._storage.delete_thread(scope, thread_id)
        await self._hub.deleted(scope, thread_id, actor)

    async def add_member(
        self, actor: Identity, scope: str, thread_id: str, identity: Identity
    ) -> Member:
        if identity.role == "system":
            raise InputError("server: the system identity cannot be a member")
        thread = await self.thread(actor, scope, thread_id, "manage")
        return await self._admit(scope, thread, identity, actor)

    async def remove_member(
        self, actor: Identity, scope: str, thread_id: str, identity_id: str
    ) -> None:
        await self.thread(actor, scope, thread_id, "manage")
        member = next(
            (
                m
                for m in await self._storage.list_members(scope, thread_id)
                if m.identity.id == identity_id
            ),
            None,
        )
        if member is None:
            raise NotFoundError(f"server: {identity_id!r} is not a member of thread {thread_id!r}")
        await self._storage.remove_member(scope, thread_id, identity_id)
        await self._append(
            scope,
            {
                "kind": "member_removed",
                "thread_id": thread_id,
                "author": actor,
                "client_id": uuid.uuid4().hex,
                "identity": member.identity,
            },
        )

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
    ) -> list[Event]:
        await self.thread(actor, scope, thread_id, "read")
        return await self._storage.list_events(
            scope, thread_id, before=before, limit=_limit(limit), kinds=kinds
        )

    async def join(self, session: Session, thread_id: str, since: int | None) -> Joined:
        if since is not None and since < 0:
            raise InputError("server: since cannot be negative")
        thread = await self.thread(session.identity, session.scope, thread_id, "read")
        return await self._hub.join(session, thread, since)

    async def leave(self, session: Session, thread_id: str) -> None:
        await self._hub.leave(session, thread_id)

    async def send(self, actor: Identity, scope: str, payload: dict[str, Any]) -> Event:
        kind = payload.get("kind")
        if kind not in CLIENT_EVENTS:
            raise InputError(f"server: unknown event kind {kind!r}")
        thread_id = payload.get("thread_id")
        if not isinstance(thread_id, str):
            raise InputError("server: event.send needs a thread_id")
        if not payload.get("client_id"):
            raise InputError("server: event.send needs a client_id")
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
        await self._check(scope, actor, event)
        if self._redact is not None and isinstance(event, Message | Edit):
            sent = await self._storage.find_event(scope, thread_id, actor.id, event.client_id)
            if sent is not None:
                return sent
            event = await self._redacted(self._redact, actor, thread, event)
        stored, created = await self._storage.append_event(scope, event)
        if created:
            await self._hub.deliver(scope, stored)
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

    async def _check(self, scope: str, actor: Identity, event: Event) -> None:
        if isinstance(event, Message | Edit | Step):
            self._inline(event.output if isinstance(event, Step) else event.content)
        if isinstance(event, Custom) and len(json.dumps(event.payload).encode()) > CUSTOM_LIMIT:
            raise InputError(f"server: a custom payload is at most {CUSTOM_LIMIT} bytes")
        if isinstance(event, Message) and len(json.dumps(event.metadata).encode()) > METADATA_LIMIT:
            raise InputError(f"server: message metadata is at most {METADATA_LIMIT} bytes")
        if isinstance(event, Edit | Retract):
            verb = "edit" if isinstance(event, Edit) else "retract"
            target = await self._storage.get_event(scope, event.thread_id, event.target)
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
