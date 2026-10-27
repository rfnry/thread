import asyncio
import json
import logging
import uuid
import weakref
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from rfnry_thread.errors import ForbiddenError, NotFoundError, ThreadException
from rfnry_thread.models import (
    SIGNALS,
    Event,
    Identity,
    Idle,
    Invited,
    Joined,
    Left,
    Marked,
    MemberRemoved,
    Present,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Thread,
    ThreadDeleted,
    Working,
)
from rfnry_thread.outbound import Outbound
from rfnry_thread.protocol import Push
from rfnry_thread.state import LatestState
from rfnry_thread.storage import Storage

logger = logging.getLogger(__name__)

NOTIFY_LIMIT = 7000
PIECE = 500


@dataclass(eq=False)
class Session:
    identity: Identity
    scope: str
    outbound: Outbound
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    joined: dict[str, int] = field(default_factory=dict)
    streams: set[tuple[str, str]] = field(default_factory=set)
    working: set[str] = field(default_factory=set)
    present: set[str] = field(default_factory=set)
    following: bool = False
    dropped: bool = False

    def push_event(self, event: Event, payload: dict[str, Any]) -> None:
        self.joined[event.thread_id] = event.seq
        self.outbound.put(Push(name="event", payload=payload))

    def push_signal(self, payload: dict[str, Any]) -> None:
        self.outbound.put(Push(name="signal", payload=payload))

    def push_joined(self, joined: Joined) -> None:
        self.outbound.put(Push(name="joined", payload=joined.model_dump(mode="json")))


def _size(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload).encode())


class Hub:
    def __init__(
        self,
        storage: Storage,
        replay_limit: int,
        authorize: Callable[[Identity, str, Thread], Awaitable[bool]] | None = None,
    ) -> None:
        self.storage = storage
        self.replay_limit = replay_limit
        self.authorize = authorize
        self.origin = uuid.uuid4().hex
        self.state = LatestState(self.origin)
        self.sessions: set[Session] = set()
        self.linked = False
        self._rooms: dict[tuple[str, str], set[Session]] = {}
        self._locks: weakref.WeakValueDictionary[tuple[str, str], asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )
        self._local: dict[tuple[str, str, str], int] = {}

    def _lock(self, scope: str, thread_id: str) -> asyncio.Lock:
        lock = self._locks.get((scope, thread_id))
        if lock is None:
            lock = asyncio.Lock()
            self._locks[(scope, thread_id)] = lock
        return lock

    def _room(self, scope: str, thread_id: str) -> list[Session]:
        return list(self._rooms.get((scope, thread_id), ()))

    async def join(self, session: Session, thread: Thread, since: int | None) -> Joined:
        joined, _ = await self._enter(session, thread, since)
        if thread.id not in session.present and not session.dropped:
            session.present.add(thread.id)
            await self._arrive(session, thread.id)
        return joined

    async def _enter(
        self, session: Session, thread: Thread, since: int | None, rewind: bool = True
    ) -> tuple[Joined, bool]:
        scope, thread_id = session.scope, thread.id
        async with self._lock(scope, thread_id):
            if session.dropped:
                return Joined(thread=thread, seq=session.joined.get(thread_id, 0)), False
            if thread_id in session.joined:
                return await self._rewind(session, thread, since if rewind else None), False
            head = await self.storage.head(scope, thread_id, session.identity.id)
            if head is None:
                raise ForbiddenError(
                    f"server: {session.identity.id!r} is not a member of thread {thread_id!r}"
                )
            replay: list[Event] = []
            truncated = since is not None and since > head
            if since is not None and since < head:
                if head - since > self.replay_limit:
                    replay = await self.storage.list_events(
                        scope, thread_id, before=head + 1, limit=self.replay_limit
                    )
                    truncated = True
                else:
                    replay = await self.storage.list_events(
                        scope, thread_id, since=since, limit=self.replay_limit
                    )
            if session.dropped:
                return Joined(thread=thread, seq=head), False
            session.joined[thread_id] = replay[0].seq - 1 if replay else head
            self._rooms.setdefault((scope, thread_id), set()).add(session)
            for event in replay:
                session.push_event(event, event.model_dump(mode="json"))
            for signal in self.state.snapshot(scope, thread_id):
                session.push_signal(signal.model_dump(mode="json"))
        return Joined(thread=thread, seq=head, truncated=truncated), True

    async def _rewind(self, session: Session, thread: Thread, since: int | None) -> Joined:
        cursor = session.joined[thread.id]
        if since is None or since >= cursor:
            return Joined(thread=thread, seq=cursor)
        truncated = cursor - since > self.replay_limit
        replay = await self.storage.list_events(
            session.scope,
            thread.id,
            since=None if truncated else since,
            before=cursor + 1,
            limit=self.replay_limit,
        )
        if session.dropped or session.joined.get(thread.id) != cursor:
            return Joined(thread=thread, seq=cursor)
        for event in replay:
            session.outbound.put(Push(name="event", payload=event.model_dump(mode="json")))
        return Joined(thread=thread, seq=cursor, truncated=truncated)

    async def follow(
        self, session: Session, threads: list[tuple[Thread, int | None]]
    ) -> list[Joined]:
        found: list[Joined] = []
        for thread, since in threads:
            try:
                joined, _ = await self._enter(session, thread, since, rewind=False)
            except ForbiddenError, NotFoundError:
                continue
            found.append(joined)
        return found

    async def leave(self, session: Session, thread_id: str) -> None:
        if session.joined.pop(thread_id, None) is None:
            return
        room = self._rooms.get((session.scope, thread_id))
        if room is not None:
            room.discard(session)
            if not room:
                del self._rooms[(session.scope, thread_id)]
        for stream_id in sorted(s for t, s in session.streams if t == thread_id):
            await self.signal(
                session,
                StreamEnd(thread_id=thread_id, sender=session.identity, stream_id=stream_id),
            )
        if thread_id in session.working:
            await self.signal(session, Idle(thread_id=thread_id, sender=session.identity))
        if thread_id in session.present:
            session.present.discard(thread_id)
            await self._depart(session, thread_id)

    async def deliver(self, scope: str, event: Event) -> None:
        async with self._lock(scope, event.thread_id):
            cut = await self._deliver(scope, event)
        for session in cut:
            await self.leave(session, event.thread_id)

    async def _deliver(self, scope: str, event: Event) -> list[Session]:
        payload = event.model_dump(mode="json")
        cut: list[Session] = []
        for session in self._room(scope, event.thread_id):
            cursor = session.joined.get(event.thread_id)
            if cursor is None or event.seq <= cursor:
                continue
            if event.seq > cursor + 1:
                missing = await self.storage.list_events(
                    scope,
                    event.thread_id,
                    since=cursor,
                    before=event.seq,
                    limit=event.seq - cursor - 1,
                )
                for earlier in missing:
                    session.push_event(earlier, earlier.model_dump(mode="json"))
            session.push_event(event, payload)
            if isinstance(event, MemberRemoved) and event.identity.id == session.identity.id:
                cut.append(session)
        return cut

    async def on_event(self, scope: str, thread_id: str, seq: int) -> None:
        if not any(s.joined.get(thread_id, seq) < seq for s in self._room(scope, thread_id)):
            return
        try:
            async with self._lock(scope, thread_id):
                found = await self.storage.list_events(scope, thread_id, since=seq - 1, limit=1)
                cut = await self._deliver(scope, found[0]) if found and found[0].seq == seq else []
            for session in cut:
                await self.leave(session, thread_id)
        except ThreadException as error:
            logger.warning("hub: a notice for thread %r failed: %s", thread_id, error)

    async def signal(self, session: Session, signal: Signal) -> None:
        key = (signal.thread_id, getattr(signal, "stream_id", ""))
        if isinstance(signal, StreamStart):
            session.streams.add(key)
        elif isinstance(signal, StreamEnd):
            session.streams.discard(key)
        elif isinstance(signal, Working):
            session.working.add(signal.thread_id)
        elif isinstance(signal, Idle):
            session.working.discard(signal.thread_id)
        await self._fanout(session.scope, signal, self.origin)
        await self._mirror(session.scope, signal)

    async def _fanout(self, scope: str, signal: Signal, origin: str) -> None:
        async with self._lock(scope, signal.thread_id):
            if self._visible(scope, signal, origin):
                payload = signal.model_dump(mode="json")
                for session in self._room(scope, signal.thread_id):
                    session.push_signal(payload)

    def _visible(self, scope: str, signal: Signal, origin: str) -> bool:
        if isinstance(signal, Present):
            before = self.state.is_present(scope, signal.thread_id, signal.sender.id)
            self.state.apply(scope, signal, origin)
            return not before
        self.state.apply(scope, signal, origin)
        if isinstance(signal, Left):
            return not self.state.is_present(scope, signal.thread_id, signal.sender.id)
        return True

    async def _arrive(self, session: Session, thread_id: str) -> None:
        key = (session.scope, thread_id, session.identity.id)
        self._local[key] = self._local.get(key, 0) + 1
        if self._local[key] == 1:
            await self.signal(session, Present(thread_id=thread_id, sender=session.identity))

    async def _depart(self, session: Session, thread_id: str) -> None:
        key = (session.scope, thread_id, session.identity.id)
        count = self._local.get(key, 0) - 1
        if count > 0:
            self._local[key] = count
            return
        self._local.pop(key, None)
        await self.signal(session, Left(thread_id=thread_id, sender=session.identity))

    async def added(
        self, scope: str, thread: Thread, sender: Identity, identity: Identity, seq: int
    ) -> None:
        await self._publish(
            scope,
            thread.id,
            {
                "kind": "invited",
                "thread_id": thread.id,
                "sender": sender.model_dump(mode="json"),
                "to": identity.id,
                "seq": seq,
            },
        )
        await self._added_here(scope, thread, sender, identity.id, seq)

    async def _added_here(
        self, scope: str, thread: Thread, sender: Identity, identity_id: str, seq: int | None
    ) -> None:
        for session in list(self.sessions):
            if (
                session.scope != scope
                or session.identity.id != identity_id
                or thread.id in session.joined
            ):
                continue
            if session.following:
                try:
                    if not await self._readable(session, thread):
                        continue
                    joined, entered = await self._enter(
                        session, thread, None if seq is None else seq - 1
                    )
                except ForbiddenError, NotFoundError:
                    continue
                if entered:
                    session.push_joined(joined)
            elif identity_id != sender.id:
                session.push_signal(
                    Invited(thread_id=thread.id, sender=sender, thread=thread).model_dump(
                        mode="json"
                    )
                )

    async def _readable(self, session: Session, thread: Thread) -> bool:
        if self.authorize is None or session.identity.role == "system":
            return True
        try:
            return await self.authorize(session.identity, "read", thread)
        except ThreadException:
            raise
        except Exception as error:
            logger.warning(
                "hub: authorize failed for %r in thread %r: %s",
                session.identity.id,
                thread.id,
                error,
            )
            return False

    async def deleted(self, scope: str, thread_id: str, sender: Identity) -> None:
        signal = ThreadDeleted(thread_id=thread_id, sender=sender)
        await self._deleted_here(scope, signal)
        await self._mirror(scope, signal)

    async def _deleted_here(self, scope: str, signal: ThreadDeleted) -> None:
        thread_id = signal.thread_id
        async with self._lock(scope, thread_id):
            self.state.apply(scope, signal, self.origin)
            payload = signal.model_dump(mode="json")
            for session in self._room(scope, thread_id):
                session.push_signal(payload)
                session.joined.pop(thread_id, None)
                session.present.discard(thread_id)
                session.working.discard(thread_id)
                session.streams = {s for s in session.streams if s[0] != thread_id}
            self._rooms.pop((scope, thread_id), None)
            for key in [k for k in self._local if k[:2] == (scope, thread_id)]:
                del self._local[key]

    async def drop(self, session: Session) -> None:
        session.dropped = True
        for thread_id in list(session.joined):
            await self.leave(session, thread_id)
        self.sessions.discard(session)

    async def announce(self) -> None:
        for (scope, thread_id), items in self.state.own().items():
            batch: list[dict[str, Any]] = []
            for item in items:
                if _size({"entries": [*batch, item]}) > NOTIFY_LIMIT and batch:
                    await self._publish(scope, thread_id, {"kind": "refresh", "entries": batch})
                    batch = []
                batch.append(item)
            if batch:
                await self._publish(scope, thread_id, {"kind": "refresh", "entries": batch})

    async def _mirror(self, scope: str, signal: Signal) -> None:
        if not self.linked:
            return
        payload = signal.model_dump(mode="json")
        if _size(payload) <= NOTIFY_LIMIT:
            await self._publish(scope, signal.thread_id, payload)
            return
        if not isinstance(signal, StreamDelta):
            logger.warning(
                "hub: a %s signal of %d bytes is too large to mirror", signal.kind, _size(payload)
            )
            return
        for start in range(0, len(signal.text), PIECE):
            piece = signal.model_copy(update={"text": signal.text[start : start + PIECE]})
            await self._publish(scope, signal.thread_id, piece.model_dump(mode="json"))

    async def marked(self, origin: Session, signal: Marked) -> None:
        self._marked_here(origin.scope, signal, origin)
        await self._publish(origin.scope, signal.thread_id, signal.model_dump(mode="json"))

    def _marked_here(self, scope: str, signal: Marked, origin: Session | None = None) -> None:
        payload = signal.model_dump(mode="json")
        for session in list(self.sessions):
            if (
                session is not origin
                and session.scope == scope
                and session.identity.id == signal.sender.id
            ):
                session.push_signal(payload)

    async def _publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        if not self.linked:
            return
        try:
            await self.storage.publish(scope, thread_id, {"o": self.origin, **payload})
        except ThreadException as error:
            logger.warning("hub: mirroring to other replicas failed: %s", error)

    async def on_signal(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        origin = payload.pop("o", None)
        if origin is None or origin == self.origin:
            return
        try:
            kind = payload.get("kind")
            if kind == "refresh":
                async with self._lock(scope, thread_id):
                    self.state.refresh(scope, thread_id, str(origin), payload["entries"])
                return
            if kind in ("added", "invited"):
                thread = await self.storage.get_thread(scope, thread_id)
                if thread is not None:
                    seq = payload.get("seq")
                    await self._added_here(
                        scope,
                        thread,
                        Identity.model_validate(payload["sender"]),
                        str(payload["to"]),
                        None if seq is None else int(seq),
                    )
                return
            if kind == "marked":
                self._marked_here(scope, Marked.model_validate(payload))
                return
            signal = SIGNALS.validate_python(payload)
            if isinstance(signal, ThreadDeleted):
                await self._deleted_here(scope, signal)
                return
            await self._fanout(scope, signal, str(origin))
        except Exception as error:
            logger.warning("hub: a notice from replica %s failed: %s", origin, error)
