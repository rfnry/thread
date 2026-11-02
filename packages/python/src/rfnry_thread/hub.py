import asyncio
import itertools
import json
import logging
import uuid
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from rfnry_thread.errors import ForbiddenError, NotFoundError, StorageError, ThreadException
from rfnry_thread.models import (
    SIGNALS,
    SYSTEM,
    Edit,
    Event,
    Identity,
    Idle,
    Invited,
    Joined,
    Left,
    Marked,
    MemberAdded,
    MemberRemoved,
    Present,
    Retract,
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
from rfnry_thread.state import LatestState, changes
from rfnry_thread.storage import Storage

logger = logging.getLogger(__name__)

MIRROR_BUDGET = 7000
PIECE = 500


@dataclass(eq=False)
class Session:
    identity: Identity
    scope: str
    outbound: Outbound
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    joined: dict[str, int] = field(default_factory=dict)
    floors: dict[str, int] = field(default_factory=dict)
    created: dict[str, datetime] = field(default_factory=dict)
    streams: set[tuple[str, str]] = field(default_factory=set)
    working: set[str] = field(default_factory=set)
    present: set[str] = field(default_factory=set)
    following: bool = False
    dropped: bool = False
    last_ordered: asyncio.Task[None] | None = None

    def push_event(self, event: Event, payload: dict[str, Any]) -> None:
        self.joined[event.thread_id] = event.seq
        self.outbound.put(Push(name="event", payload=payload))

    def push_signal(self, payload: dict[str, Any]) -> None:
        self.outbound.put(Push(name="signal", payload=payload))

    def push_joined(self, joined: Joined) -> None:
        self.outbound.put(Push(name="joined", payload=joined.model_dump(mode="json")))


@dataclass
class _Aftermath:
    parted: list[Session] = field(default_factory=list)
    again: list[tuple[Session, MemberAdded]] = field(default_factory=list)
    emptied: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


def _removes(event: Event, session: Session) -> bool:
    return isinstance(event, MemberRemoved) and event.identity.id == session.identity.id


def _adds(event: Event, session: Session) -> bool:
    return isinstance(event, MemberAdded) and event.identity.id == session.identity.id


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
        self._watching: list[set[tuple[str, str | None]]] = []
        self._wiping: dict[str, list[asyncio.Event]] = {}
        self._unchecked: set[tuple[str, str]] = set()
        self._told: dict[tuple[str, str], list[Signal]] = {}
        self._sent = itertools.count(1)
        self._shielded: set[asyncio.Future[None]] = set()

    async def shielded(self, work: Coroutine[Any, Any, None]) -> None:
        task = asyncio.ensure_future(work)
        self._shielded.add(task)
        task.add_done_callback(self._shielded.discard)
        await asyncio.shield(task)

    def _lock(self, scope: str, thread_id: str) -> asyncio.Lock:
        lock = self._locks.get((scope, thread_id))
        if lock is None:
            lock = asyncio.Lock()
            self._locks[(scope, thread_id)] = lock
        return lock

    def _room(self, scope: str, thread_id: str) -> list[Session]:
        return list(self._rooms.get((scope, thread_id), ()))

    def _audience(self, scope: str, thread_id: str) -> list[Session]:
        return [] if (scope, thread_id) in self._unchecked else self._room(scope, thread_id)

    def _doubted(self, scope: str, thread_id: str, created: datetime | None) -> bool:
        return (scope, thread_id) in self._unchecked or (
            created is not None
            and any(s.created.get(thread_id) != created for s in self._room(scope, thread_id))
        )

    @asynccontextmanager
    async def _locked(
        self, scope: str, thread_id: str, created: datetime | None = None
    ) -> AsyncIterator[_Aftermath]:
        after = _Aftermath()
        try:
            async with self._lock(scope, thread_id):
                if self._doubted(scope, thread_id, created):
                    await self._settle(scope, thread_id, after)
                yield after
        finally:
            if after.parted or after.emptied:
                await self.shielded(self._aftermath(scope, thread_id, after))

    async def _aftermath(self, scope: str, thread_id: str, after: _Aftermath) -> None:
        for session in after.parted:
            await self._parted(session, thread_id)
        await self._reentered(scope, thread_id, [a for a in after.again if a[0] in after.parted])
        if after.emptied:
            await self._emptied(scope, after.emptied)

    async def _settle(self, scope: str, thread_id: str, after: _Aftermath) -> None:
        key = (scope, thread_id)
        room = self._room(scope, thread_id)
        self._unchecked.discard(key)
        if not room:
            return
        self._touch(scope, thread_id)
        try:
            found = await self.storage.list_events(scope, thread_id, limit=1)
            thread = await self.storage.get_thread(scope, thread_id)
        except Exception as error:
            self._unchecked.add(key)
            self._told.setdefault(key, self.state.snapshot(scope, thread_id, expired=True))
            logger.warning("hub: checking thread %r against storage failed: %s", thread_id, error)
            return
        stale = [s for s in room if thread is None or s.created.get(thread_id) != thread.created_at]
        gone = ThreadDeleted(thread_id=thread_id, sender=SYSTEM)
        if len(stale) == len(room):
            after.emptied.update(self._wipe(scope, gone))
            return
        payload = gone.model_dump(mode="json")
        for session in stale:
            session.push_signal(payload)
            self._cut(session, thread_id, after)
        if found:
            try:
                await self._deliver(scope, found[0], after)
            except Exception as error:
                logger.warning(
                    "hub: checking thread %r against storage failed: %s", thread_id, error
                )
        told = None if key in self._unchecked else self._told.pop(key, None)
        if told is not None:
            now = self.state.snapshot(scope, thread_id, expired=True)
            self._tell(scope, thread_id, changes(thread_id, told, now))

    @contextmanager
    def watching(self) -> Iterator[set[tuple[str, str | None]]]:
        touched: set[tuple[str, str | None]] = set()
        self._watching.append(touched)
        try:
            yield touched
        finally:
            self._watching = [watch for watch in self._watching if watch is not touched]

    def _touch(self, scope: str, thread_id: str | None) -> None:
        for touched in self._watching:
            touched.add((scope, thread_id))

    async def join(
        self, session: Session, thread: Thread, since: int | None, away: bool = False
    ) -> Joined:
        joined, _ = await self._enter(session, thread, since)
        if not away and not session.dropped:
            await self._arrive(session, thread.id)
        if thread.id not in session.joined and not session.dropped:
            raise NotFoundError(f"server: thread {thread.id!r} was not found")
        return joined

    async def _enter(
        self,
        session: Session,
        thread: Thread,
        since: int | None,
        rewind: bool = True,
        touched: set[tuple[str, str | None]] | None = None,
    ) -> tuple[Joined, bool]:
        scope, thread_id = session.scope, thread.id
        async with self._locked(scope, thread_id, thread.created_at):
            if session.dropped:
                return Joined(thread=thread, seq=session.joined.get(thread_id, 0)), False
            if (scope, thread_id) in self._unchecked:
                raise StorageError(
                    f"server: thread {thread_id!r} could not be checked against storage"
                )
            if thread_id in session.joined:
                return await self._rewind(session, thread, since if rewind else None), False
            listed = (
                touched is not None
                and (scope, thread_id) not in touched
                and (scope, None) not in touched
                and thread.history == "all"
                and thread.last_seq is not None
            )
            found = (
                (thread.last_seq or 0, 0)
                if listed
                else await self.storage.head(scope, thread_id, session.identity.id)
            )
            if found is None:
                raise ForbiddenError(
                    f"server: {session.identity.id!r} is not a member of thread {thread_id!r}"
                )
            head, floor = found
            start = None if since is None else max(since, floor - 1)
            replay: list[Event] = []
            truncated = since is not None and since > head
            if start is not None and start < head:
                if head - start > self.replay_limit:
                    replay = await self.storage.list_events(
                        scope, thread_id, before=head + 1, limit=self.replay_limit, floor=floor
                    )
                    truncated = True
                else:
                    replay = await self.storage.list_events(
                        scope,
                        thread_id,
                        since=start,
                        before=head + 1,
                        limit=self.replay_limit,
                        floor=floor,
                    )
            if session.dropped:
                return Joined(thread=thread, seq=head), False
            session.joined[thread_id] = replay[0].seq - 1 if replay else head
            session.floors[thread_id] = floor
            session.created[thread_id] = thread.created_at
            self._expire(scope, thread_id)
            self._rooms.setdefault((scope, thread_id), set()).add(session)
            for event in replay:
                session.push_event(event, event.model_dump(mode="json"))
            for signal in self.state.snapshot(scope, thread_id):
                session.push_signal(signal.model_dump(mode="json"))
        return Joined(thread=thread, seq=head, truncated=truncated), True

    async def _rewind(self, session: Session, thread: Thread, since: int | None) -> Joined:
        cursor = session.joined[thread.id]
        floor = session.floors.get(thread.id, 0)
        if since is not None:
            since = max(since, floor - 1)
        if since is None or since >= cursor:
            return Joined(thread=thread, seq=cursor)
        truncated = cursor - since > self.replay_limit
        replay = await self.storage.list_events(
            session.scope,
            thread.id,
            since=None if truncated else since,
            before=cursor + 1,
            limit=self.replay_limit,
            floor=floor,
        )
        if session.dropped:
            return Joined(thread=thread, seq=cursor)
        for event in replay:
            session.outbound.put(Push(name="event", payload=event.model_dump(mode="json")))
        return Joined(thread=thread, seq=cursor, truncated=truncated)

    async def follow(
        self,
        session: Session,
        threads: list[tuple[Thread, int | None]],
        touched: set[tuple[str, str | None]],
    ) -> list[Joined]:
        found: list[Joined] = []
        for thread, since in threads:
            try:
                joined, _ = await self._enter(session, thread, since, False, touched)
            except ForbiddenError, NotFoundError:
                continue
            found.append(joined)
        return found

    def _cut(self, session: Session, thread_id: str, after: _Aftermath) -> None:
        if self._part(session, thread_id):
            after.parted.append(session)

    def _part(self, session: Session, thread_id: str) -> bool:
        session.floors.pop(thread_id, None)
        session.created.pop(thread_id, None)
        if session.joined.pop(thread_id, None) is None:
            return False
        room = self._rooms.get((session.scope, thread_id))
        if room is not None:
            room.discard(session)
            if not room:
                del self._rooms[(session.scope, thread_id)]
                self._unchecked.discard((session.scope, thread_id))
                self._told.pop((session.scope, thread_id), None)
        return True

    async def leave(self, session: Session, thread_id: str) -> None:
        async with self._lock(session.scope, thread_id):
            parted = self._part(session, thread_id)
        if parted:
            await self.shielded(self._parted(session, thread_id))

    async def _parted(self, session: Session, thread_id: str) -> None:
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

    async def away(self, session: Session, thread_id: str) -> None:
        if thread_id in session.present:
            session.present.discard(thread_id)
            await self._depart(session, thread_id)

    async def deliver(self, scope: str, event: Event, created: datetime | None = None) -> None:
        self._touch(scope, event.thread_id)
        async with self._locked(scope, event.thread_id, created) as after:
            await self._deliver(scope, event, after)

    async def _deliver(self, scope: str, event: Event, after: _Aftermath) -> None:
        payload = event.model_dump(mode="json")
        room = self._audience(scope, event.thread_id)
        target, unread = event.seq, None
        try:
            target = await self._target(scope, event, room)
        except Exception as error:
            unread = error
        for session in room:
            cursor = session.joined.get(event.thread_id)
            if cursor is None or event.seq <= cursor:
                continue
            floor = session.floors.get(event.thread_id, 0)
            if unread is not None and floor > 0:
                self._behind(session, event.thread_id, unread)
                continue
            if event.seq > cursor + 1:
                try:
                    missing = await self.storage.list_events(
                        scope,
                        event.thread_id,
                        since=cursor,
                        before=event.seq,
                        limit=event.seq - cursor - 1,
                        floor=floor,
                    )
                except Exception as error:
                    self._behind(session, event.thread_id, error)
                    continue
                removed = next((e for e in missing if _removes(e, session)), None)
                if removed is not None:
                    last = [
                        e for e in (*missing, event) if _removes(e, session) or _adds(e, session)
                    ][-1]
                    if isinstance(last, MemberAdded):
                        after.again.append((session, last))
                for earlier in missing:
                    session.push_event(earlier, earlier.model_dump(mode="json"))
                    if earlier is removed:
                        break
                if removed is not None:
                    self._cut(session, event.thread_id, after)
                    continue
            if target < floor:
                session.joined[event.thread_id] = event.seq
                continue
            session.push_event(event, payload)
            if _removes(event, session):
                self._cut(session, event.thread_id, after)

    def _behind(self, session: Session, thread_id: str, error: Exception) -> None:
        logger.warning(
            "hub: catching connection %s of %r up in thread %r failed: %s",
            session.id,
            session.identity.id,
            thread_id,
            error,
        )

    async def _reentered(
        self, scope: str, thread_id: str, again: list[tuple[Session, MemberAdded]]
    ) -> None:
        latest: dict[str, tuple[Session, MemberAdded]] = {}
        for session, add in again:
            held = latest.get(session.identity.id)
            if held is None or held[1].seq < add.seq:
                latest[session.identity.id] = (session, add)
        for session, add in latest.values():
            try:
                if not await self.storage.is_member(scope, thread_id, session.identity.id):
                    continue
                thread = await self.storage.get_thread(scope, thread_id)
            except Exception as error:
                logger.warning(
                    "hub: handing thread %r to %r again failed: %s",
                    thread_id,
                    session.identity.id,
                    error,
                )
                continue
            if thread is not None:
                await self._added_here(scope, thread, add.author, session.identity.id, add.seq)

    async def _target(self, scope: str, event: Event, room: list[Session]) -> int:
        if not isinstance(event, Edit | Retract):
            return event.seq
        if not any(s.floors.get(event.thread_id, 0) > 0 for s in room):
            return event.seq
        found = await self.storage.get_event(scope, event.thread_id, event.target)
        return event.seq if found is None else found.seq

    async def on_event(self, scope: str, thread_id: str, seq: int) -> None:
        self._touch(scope, thread_id)
        lock = self._locks.get((scope, thread_id))
        entering = lock is not None and lock.locked()
        if (
            not entering
            and (scope, thread_id) not in self._unchecked
            and not any(s.joined.get(thread_id, seq) < seq for s in self._room(scope, thread_id))
        ):
            return
        try:
            async with self._locked(scope, thread_id) as after:
                found = await self.storage.list_events(scope, thread_id, since=seq - 1, limit=1)
                if found and found[0].seq == seq:
                    await self._deliver(scope, found[0], after)
        except ThreadException as error:
            logger.warning("hub: a notice for thread %r failed: %s", thread_id, error)

    async def signal(self, session: Session, signal: Signal) -> None:
        async with self._locked(session.scope, signal.thread_id):
            if (
                isinstance(signal, Working | StreamStart | StreamDelta)
                and signal.thread_id not in session.joined
            ):
                return
            n = self._said(session, signal)
        await self._mirror(session.scope, signal, n)

    def _said(self, session: Session, signal: Signal) -> int:
        key = (signal.thread_id, getattr(signal, "stream_id", ""))
        if isinstance(signal, StreamStart):
            session.streams.add(key)
        elif isinstance(signal, StreamEnd):
            session.streams.discard(key)
        elif isinstance(signal, Working):
            session.working.add(signal.thread_id)
        elif isinstance(signal, Idle):
            session.working.discard(signal.thread_id)
        self._tell(
            session.scope, signal.thread_id, self._visible(session.scope, signal, self.origin)
        )
        return next(self._sent)

    async def _fanout(self, scope: str, signal: Signal, origin: str, n: int) -> None:
        async with self._locked(scope, signal.thread_id):
            self._tell(scope, signal.thread_id, self._visible(scope, signal, origin, n))

    def _tell(self, scope: str, thread_id: str, signals: list[Signal]) -> None:
        for signal in signals:
            payload = signal.model_dump(mode="json")
            for session in self._audience(scope, thread_id):
                session.push_signal(payload)

    def _visible(self, scope: str, signal: Signal, origin: str, n: int = 0) -> list[Signal]:
        thread_id, sender = signal.thread_id, signal.sender
        self._expire(scope, thread_id)
        if isinstance(signal, Present):
            before = self.state.is_present(scope, thread_id, sender.id)
            self.state.apply(scope, signal, origin, n)
            return [] if before else [signal]
        held = self.state.holds(scope, signal, origin)
        self.state.apply(scope, signal, origin, n)
        if origin != self.origin:
            if isinstance(signal, Idle | StreamEnd | Left) and not held:
                return []
            if isinstance(signal, StreamStart) and held:
                return []
            if isinstance(signal, StreamDelta) and not held:
                return [self.state.started(scope, signal, origin), signal]
        if isinstance(signal, Left) and self.state.is_present(scope, thread_id, sender.id):
            return []
        return [signal]

    def _expire(self, scope: str, thread_id: str, forget: bool = False) -> None:
        self._tell(scope, thread_id, self.state.expire(scope, thread_id, forget))

    async def sweep(self, forget: bool = False) -> None:
        for scope, thread_id in self.state.expiring(forget):
            async with self._locked(scope, thread_id):
                self._expire(scope, thread_id, forget)

    async def _arrive(self, session: Session, thread_id: str) -> None:
        present = Present(thread_id=thread_id, sender=session.identity)
        async with self._locked(session.scope, thread_id):
            if thread_id not in session.joined or thread_id in session.present:
                return
            session.present.add(thread_id)
            key = (session.scope, thread_id, session.identity.id)
            self._local[key] = self._local.get(key, 0) + 1
            if self._local[key] > 1:
                return
            n = self._said(session, present)
        await self._mirror(session.scope, present, n)

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
        for wiping in list(self._wiping.get(scope, ())):
            await wiping.wait()
        await self._check(scope, thread.id, thread.created_at)
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
                except Exception as error:
                    logger.warning(
                        "hub: entering connection %s of %r into thread %r failed: %s",
                        session.id,
                        identity_id,
                        thread.id,
                        error,
                    )
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
        except ForbiddenError, NotFoundError:
            return False
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
        emptied = await self._deleted_here(scope, signal)
        await self._mirror(scope, signal, next(self._sent))
        await self._emptied(scope, emptied)

    async def scope_deleted(self, scope: str, sender: Identity) -> None:
        emptied = await self._scope_deleted_here(scope, sender)
        await self._publish(
            scope, "", {"kind": "scope.deleted", "sender": sender.model_dump(mode="json")}
        )
        await self._emptied(scope, emptied)

    async def _scope_deleted_here(
        self, scope: str, sender: Identity
    ) -> dict[str, list[dict[str, Any]]]:
        done = asyncio.Event()
        self._wiping.setdefault(scope, []).append(done)
        try:
            self._touch(scope, None)
            held = {t for s, t in [*self._rooms, *self._locks.keys()] if s == scope}
            emptied: dict[str, list[dict[str, Any]]] = {}
            for thread_id in sorted(held.union(self.state.threads(scope))):
                emptied.update(
                    await self._deleted_here(
                        scope, ThreadDeleted(thread_id=thread_id, sender=sender)
                    )
                )
            return emptied
        finally:
            done.set()
            self._wiping[scope].remove(done)
            if not self._wiping[scope]:
                del self._wiping[scope]

    async def _deleted_here(
        self, scope: str, signal: ThreadDeleted
    ) -> dict[str, list[dict[str, Any]]]:
        self._touch(scope, signal.thread_id)
        async with self._lock(scope, signal.thread_id):
            return self._wipe(scope, signal)

    def _wipe(self, scope: str, signal: ThreadDeleted) -> dict[str, list[dict[str, Any]]]:
        thread_id = signal.thread_id
        items = self.state.delete(scope, thread_id)
        payload = signal.model_dump(mode="json")
        for session in self._room(scope, thread_id):
            session.push_signal(payload)
            session.joined.pop(thread_id, None)
            session.floors.pop(thread_id, None)
            session.created.pop(thread_id, None)
            session.present.discard(thread_id)
            session.working.discard(thread_id)
            session.streams = {s for s in session.streams if s[0] != thread_id}
        self._rooms.pop((scope, thread_id), None)
        self._unchecked.discard((scope, thread_id))
        self._told.pop((scope, thread_id), None)
        for key in [k for k in self._local if k[:2] == (scope, thread_id)]:
            del self._local[key]
        return {thread_id: items} if items else {}

    def on_resume(self) -> Awaitable[None]:
        self._unchecked.update(self._rooms)
        return self._resumed()

    async def _resumed(self) -> None:
        await self.recheck()
        await self.announce()

    async def recheck(self) -> None:
        for scope, thread_id in sorted(self._unchecked):
            try:
                await self._check(scope, thread_id)
            except Exception as error:
                logger.warning(
                    "hub: checking thread %r against storage failed: %s", thread_id, error
                )

    async def _check(self, scope: str, thread_id: str, created: datetime | None = None) -> None:
        async with self._locked(scope, thread_id, created):
            pass

    async def _emptied(self, scope: str, emptied: dict[str, list[dict[str, Any]]]) -> None:
        n = next(self._sent)
        for thread_id, items in emptied.items():
            await self._repeat(scope, thread_id, n, items)

    async def drop(self, session: Session) -> None:
        session.dropped = True
        for thread_id in list(session.joined):
            await self.leave(session, thread_id)
        self.sessions.discard(session)

    async def announce(self) -> None:
        n = next(self._sent)
        for (scope, thread_id), items in self.state.own().items():
            if (scope, thread_id) not in self._unchecked:
                await self._repeat(scope, thread_id, n, items)

    async def _repeat(
        self, scope: str, thread_id: str, n: int, items: list[dict[str, Any]]
    ) -> None:
        batch: list[dict[str, Any]] = []
        for item in items:
            if _size({"entries": [*batch, item]}) > MIRROR_BUDGET and batch:
                await self._publish(scope, thread_id, {"kind": "refresh", "n": n, "entries": batch})
                batch = []
            batch.append(item)
        if batch:
            await self._publish(scope, thread_id, {"kind": "refresh", "n": n, "entries": batch})

    async def _mirror(self, scope: str, signal: Signal, n: int) -> None:
        if not self.linked:
            return
        payload = {"n": n, **signal.model_dump(mode="json")}
        if _size(payload) <= MIRROR_BUDGET:
            await self._publish(scope, signal.thread_id, payload)
            return
        if not isinstance(signal, StreamDelta):
            logger.warning(
                "hub: a %s signal of %d bytes is too large to mirror", signal.kind, _size(payload)
            )
            return
        for start in range(0, len(signal.text), PIECE):
            piece = signal.model_copy(update={"text": signal.text[start : start + PIECE]})
            await self._publish(scope, signal.thread_id, {"n": n, **piece.model_dump(mode="json")})

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

    async def unlink(self) -> None:
        if not self.linked:
            return
        self.linked = False
        n = next(self._sent)
        for scope, signal in self.state.own_ends():
            payload = {"n": n, **signal.model_dump(mode="json")}
            if _size(payload) <= MIRROR_BUDGET and not await self._notify(
                scope, signal.thread_id, payload
            ):
                return

    async def _publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        if self.linked:
            await self._notify(scope, thread_id, payload)

    async def _notify(self, scope: str, thread_id: str, payload: dict[str, Any]) -> bool:
        try:
            await self.storage.publish(scope, thread_id, {"o": self.origin, **payload})
        except ThreadException as error:
            logger.warning("hub: mirroring to other replicas failed: %s", error)
            return False
        return True

    async def on_signal(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        origin = payload.pop("o", None)
        if origin is None or origin == self.origin:
            return
        try:
            n = int(payload.pop("n", 0))
            kind = payload.get("kind")
            if kind == "refresh":
                async with self._locked(scope, thread_id):
                    self._expire(scope, thread_id)
                    self._tell(
                        scope,
                        thread_id,
                        self.state.refresh(scope, thread_id, str(origin), payload["entries"], n),
                    )
                return
            if kind == "invited":
                thread = await self.storage.get_thread(scope, thread_id)
                if thread is not None and await self.storage.is_member(
                    scope, thread_id, str(payload["to"])
                ):
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
            if kind == "scope.deleted":
                sender = Identity.model_validate(payload["sender"])
                await self._emptied(scope, await self._scope_deleted_here(scope, sender))
                return
            signal = SIGNALS.validate_python(payload)
            if isinstance(signal, ThreadDeleted):
                await self._emptied(scope, await self._deleted_here(scope, signal))
                return
            await self._fanout(scope, signal, str(origin), n)
        except Exception as error:
            logger.warning("hub: a notice from replica %s failed: %s", origin, error)
