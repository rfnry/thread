import asyncio
import itertools
import json
import logging
import uuid
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from typing import Any

from rfnry_thread.errors import ForbiddenError, NotFoundError
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
from rfnry_thread.state import LatestState
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


@dataclass(eq=False)
class _Room:
    epoch: int
    sessions: set[Session] = field(default_factory=set)


@dataclass
class _Aftermath:
    ends: list[tuple[int, Signal]] = field(default_factory=list)
    again: list[tuple[Session, MemberAdded, int]] = field(default_factory=list)


def _removes(event: Event, session: Session) -> bool:
    return isinstance(event, MemberRemoved) and event.identity.id == session.identity.id


def _adds(event: Event, session: Session) -> bool:
    return isinstance(event, MemberAdded) and event.identity.id == session.identity.id


def _level(room: _Room, thread_id: str, seq: int, epoch: int) -> bool:
    return epoch == room.epoch and all(s.joined[thread_id] >= seq for s in room.sessions)


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
        self._rooms: dict[tuple[str, str], _Room] = {}
        self._locks: weakref.WeakValueDictionary[tuple[str, str], asyncio.Lock] = (
            weakref.WeakValueDictionary()
        )
        self._local: dict[tuple[str, str, str], int] = {}
        self._watching: list[set[tuple[str, str | None]]] = []
        self._sent = itertools.count(1)
        self._shielded: set[asyncio.Future[None]] = set()
        self._sending: set[asyncio.Task[None]] = set()

    async def shielded(self, work: Coroutine[Any, Any, None]) -> None:
        await asyncio.shield(self._started(work))

    def _started(self, work: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.Task(work, loop=asyncio.get_running_loop(), eager_start=True)
        self._shielded.add(task)
        task.add_done_callback(self._shielded.discard)
        return task

    def _queued(self, send: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.Task(send, loop=asyncio.get_running_loop(), eager_start=True)
        self._sending.add(task)
        task.add_done_callback(self._sending.discard)
        return task

    async def drain(self) -> None:
        if self._sending:
            await asyncio.wait(list(self._sending))

    def _lock(self, scope: str, thread_id: str) -> asyncio.Lock:
        lock = self._locks.get((scope, thread_id))
        if lock is None:
            lock = asyncio.Lock()
            self._locks[(scope, thread_id)] = lock
        return lock

    def _busy(self, scope: str, thread_id: str) -> bool:
        lock = self._locks.get((scope, thread_id))
        return lock is not None and lock.locked()

    def _quiet(self, scope: str, thread_id: str) -> bool:
        return (scope, thread_id) not in self._rooms and not self._busy(scope, thread_id)

    @asynccontextmanager
    async def _locked(self, scope: str, thread_id: str) -> AsyncIterator[_Aftermath]:
        after = _Aftermath()
        try:
            async with self._lock(scope, thread_id):
                try:
                    yield after
                finally:
                    for epoch, signal in after.ends:
                        self._say(scope, epoch, signal)
        finally:
            if after.again:
                aftermath = self._started(self._reentered(scope, thread_id, after.again))
                current = asyncio.current_task()
                if current is None or not current.cancelling():
                    await asyncio.shield(aftermath)

    async def catch_up(self) -> None:
        held = sorted(self._rooms)
        if not held:
            return
        try:
            heads = await self.storage.heads(held)
        except Exception as error:
            logger.warning("hub: reading the heads of %d threads failed: %s", len(held), error)
            return
        for scope, thread_id in held:
            room = self._rooms.get((scope, thread_id))
            head = heads.get((scope, thread_id))
            if (
                room is None
                or self._busy(scope, thread_id)
                or (head is not None and _level(room, thread_id, *head))
            ):
                continue
            await self._catch_up(scope, thread_id)

    async def _catch_up(self, scope: str, thread_id: str, gone_by: Identity = SYSTEM) -> None:
        async with self._locked(scope, thread_id) as after:
            room = self._rooms.get((scope, thread_id))
            if room is not None:
                await self._caught_up(scope, thread_id, room, after, gone_by)

    async def _caught_up(
        self,
        scope: str,
        thread_id: str,
        room: _Room,
        after: _Aftermath,
        gone_by: Identity = SYSTEM,
    ) -> None:
        groups: dict[tuple[int, int], list[Session]] = {}
        for session in room.sessions:
            at = (session.joined[thread_id], session.floors.get(thread_id, 0))
            groups.setdefault(at, []).append(session)
        for (since, floor), sessions in sorted(groups.items(), key=lambda group: group[0]):
            out: dict[Session, Event] = {}
            try:
                while True:
                    try:
                        found = await self.storage.tail(
                            scope, thread_id, since, floor, self.replay_limit
                        )
                    except Exception as error:
                        logger.warning(
                            "hub: catching thread %r up from storage failed: %s", thread_id, error
                        )
                        break
                    if found is None or found[1] != room.epoch:
                        self._wipe(scope, ThreadDeleted(thread_id=thread_id, sender=gone_by))
                        return
                    last, _, events = found
                    for event in events:
                        self._push(thread_id, event, sessions, out, after)
                    if len(events) < self.replay_limit:
                        for session in sessions:
                            if session not in out:
                                session.joined[thread_id] = max(session.joined[thread_id], last)
                        break
                    since = events[-1].seq
            finally:
                after.again.extend(
                    (s, e, room.epoch) for s, e in out.items() if isinstance(e, MemberAdded)
                )

    def _push(
        self,
        thread_id: str,
        event: Event,
        sessions: list[Session],
        out: dict[Session, Event],
        after: _Aftermath,
    ) -> None:
        payload = event.model_dump(mode="json")
        for session in sessions:
            if session in out:
                if _removes(event, session) or _adds(event, session):
                    out[session] = event
            elif event.seq > session.joined[thread_id]:
                session.push_event(event, payload)
                if _removes(event, session):
                    out[session] = event
                    self._part(session, thread_id, after.ends)

    def _outdated(self, scope: str, thread: Thread) -> _Room | None:
        room = self._rooms.get((scope, thread.id))
        if room is not None and room.epoch < thread.epoch:
            self._wipe(scope, ThreadDeleted(thread_id=thread.id, sender=SYSTEM))
            return None
        return room

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
        async with self._lock(scope, thread_id):
            if session.dropped:
                return Joined(thread=thread, seq=session.joined.get(thread_id, 0)), False
            room = self._outdated(scope, thread)
            if thread_id in session.joined:
                return await self._rewind(session, thread, since if rewind else None), False
            listed = (
                (room is None or room.epoch == thread.epoch)
                and touched is not None
                and (scope, thread_id) not in touched
                and (scope, None) not in touched
                and thread.history == "all"
                and thread.last_seq is not None
            )
            found = (
                (thread.last_seq or 0, 0, thread.epoch)
                if listed
                else await self.storage.head(scope, thread_id, session.identity.id)
            )
            if found is None:
                raise ForbiddenError(
                    f"server: {session.identity.id!r} is not a member of thread {thread_id!r}"
                )
            head, floor, epoch = found
            if epoch != thread.epoch:
                raise NotFoundError(f"server: thread {thread_id!r} was not found")
            start = None if since is None else max(since, floor - 1)
            replay: list[Event] = []
            truncated = since is not None and since > head
            if start is not None and start < head:
                truncated = head - start > self.replay_limit
                read = await self.storage.tail(
                    scope,
                    thread_id,
                    max(start, head - self.replay_limit),
                    floor,
                    self.replay_limit,
                )
                if read is None or read[1] != thread.epoch:
                    raise NotFoundError(f"server: thread {thread_id!r} was not found")
                replay = [event for event in read[2] if event.seq <= head]
            if session.dropped:
                return Joined(thread=thread, seq=head), False
            session.joined[thread_id] = replay[0].seq - 1 if replay else head
            session.floors[thread_id] = floor
            room = self._rooms.setdefault((scope, thread_id), _Room(thread.epoch))
            room.sessions.add(session)
            for event in replay:
                session.push_event(event, event.model_dump(mode="json"))
            for signal in self.state.snapshot(scope, thread_id, room.epoch):
                session.push_signal(signal.model_dump(mode="json"))
        return Joined(thread=thread, seq=head, truncated=truncated), True

    async def _rewind(self, session: Session, thread: Thread, since: int | None) -> Joined:
        scope, cursor = session.scope, session.joined[thread.id]
        floor = session.floors.get(thread.id, 0)
        if since is not None:
            since = max(since, floor - 1)
        if since is None or since >= cursor:
            return Joined(thread=thread, seq=cursor, truncated=since is not None and since > cursor)
        room = self._rooms[(scope, thread.id)]
        read = await self.storage.tail(
            scope, thread.id, max(since, cursor - self.replay_limit), floor, self.replay_limit
        )
        if read is None or read[1] != room.epoch:
            self._wipe(scope, ThreadDeleted(thread_id=thread.id, sender=SYSTEM))
            raise NotFoundError(f"server: thread {thread.id!r} was not found")
        if session.dropped:
            return Joined(thread=thread, seq=cursor)
        for event in read[2]:
            if event.seq <= cursor:
                session.outbound.put(Push(name="event", payload=event.model_dump(mode="json")))
        return Joined(thread=thread, seq=cursor, truncated=cursor - since > self.replay_limit)

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
        return [joined for joined in found if joined.thread.id in session.joined]

    def _part(self, session: Session, thread_id: str, ends: list[tuple[int, Signal]]) -> None:
        session.floors.pop(thread_id, None)
        room = self._rooms.get((session.scope, thread_id))
        if session.joined.pop(thread_id, None) is None or room is None:
            return
        room.sessions.discard(session)
        if not room.sessions:
            del self._rooms[(session.scope, thread_id)]
        sender = session.identity
        for stream_id in sorted(s for t, s in session.streams if t == thread_id):
            ends.append(
                (room.epoch, StreamEnd(thread_id=thread_id, sender=sender, stream_id=stream_id))
            )
        session.streams = {s for s in session.streams if s[0] != thread_id}
        if thread_id in session.working:
            session.working.discard(thread_id)
            ends.append((room.epoch, Idle(thread_id=thread_id, sender=sender)))
        if thread_id in session.present:
            session.present.discard(thread_id)
            if self._departed(session, thread_id):
                ends.append((room.epoch, Left(thread_id=thread_id, sender=sender)))

    async def leave(self, session: Session, thread_id: str) -> None:
        async with self._locked(session.scope, thread_id) as after:
            self._part(session, thread_id, after.ends)

    async def away(self, session: Session, thread_id: str) -> None:
        async with self._lock(session.scope, thread_id):
            room = self._rooms.get((session.scope, thread_id))
            if room is None or thread_id not in session.present:
                return
            session.present.discard(thread_id)
            if self._departed(session, thread_id):
                self._say(
                    session.scope, room.epoch, Left(thread_id=thread_id, sender=session.identity)
                )

    async def deliver(self, scope: str, event: Event, epoch: int) -> None:
        try:
            await self._heard(scope, event.thread_id, event.seq, epoch, event)
        finally:
            self._queued(self._hint(scope, event, epoch))

    async def _hint(self, scope: str, event: Event, epoch: int) -> None:
        try:
            await self.storage.hint(scope, event.thread_id, event.seq, epoch)
        except Exception as error:
            logger.warning("hub: hinting other replicas failed: %s", error)

    async def on_event(self, scope: str, thread_id: str, seq: int, epoch: int) -> None:
        try:
            await self._heard(scope, thread_id, seq, epoch)
        except Exception as error:
            logger.warning("hub: a notice for thread %r failed: %s", thread_id, error)

    async def _heard(
        self, scope: str, thread_id: str, seq: int, epoch: int, event: Event | None = None
    ) -> None:
        self._touch(scope, thread_id)
        if self._quiet(scope, thread_id):
            return
        async with self._locked(scope, thread_id) as after:
            room = self._rooms.get((scope, thread_id))
            if room is None or epoch < room.epoch or _level(room, thread_id, seq, epoch):
                return
            sessions = list(room.sessions)
            if (
                event is not None
                and epoch == room.epoch
                and all(s.joined[thread_id] == seq - 1 for s in sessions)
                and not (
                    isinstance(event, Edit | Retract)
                    and any(s.floors.get(thread_id, 0) > 0 for s in sessions)
                )
            ):
                self._push(thread_id, event, sessions, {}, after)
                return
            await self._caught_up(scope, thread_id, room, after)

    async def _reentered(
        self, scope: str, thread_id: str, again: list[tuple[Session, MemberAdded, int]]
    ) -> None:
        latest: dict[str, tuple[Session, MemberAdded, int]] = {}
        for session, add, epoch in again:
            held = latest.get(session.identity.id)
            if held is None or held[1].seq < add.seq:
                latest[session.identity.id] = (session, add, epoch)
        for session, add, epoch in latest.values():
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
            if thread is not None and thread.epoch == epoch:
                await self._added_here(scope, thread, add.author, session.identity.id, add.seq)

    async def signal(self, session: Session, signal: Signal) -> None:
        scope, thread_id = session.scope, signal.thread_id
        async with self._lock(scope, thread_id):
            room = self._rooms.get((scope, thread_id))
            if room is None or thread_id not in session.joined:
                return
            key = (thread_id, getattr(signal, "stream_id", ""))
            if isinstance(signal, StreamStart):
                session.streams.add(key)
            elif isinstance(signal, StreamEnd):
                session.streams.discard(key)
            elif isinstance(signal, Working):
                session.working.add(thread_id)
            elif isinstance(signal, Idle):
                session.working.discard(thread_id)
            self._say(scope, room.epoch, signal)

    def _say(self, scope: str, epoch: int, signal: Signal) -> None:
        n = next(self._sent)
        told, emptied = self.state.apply(scope, epoch, signal, self.origin, n)
        self._tell(scope, signal.thread_id, epoch, told)
        self._mirror(scope, epoch, signal, n)
        if emptied:
            self._snapshot(scope, signal.thread_id, epoch, [])

    async def _fanout(
        self, scope: str, signal: Signal, origin: str, n: int, epoch: int, d: int | None = None
    ) -> None:
        async with self._locked(scope, signal.thread_id) as after:
            if await self._current(scope, signal.thread_id, epoch, after):
                told, _ = self.state.apply(scope, epoch, signal, origin, n, d)
                self._tell(scope, signal.thread_id, epoch, told)

    async def _refreshed(
        self, scope: str, thread_id: str, origin: str, n: int, epoch: int, payload: dict[str, Any]
    ) -> None:
        async with self._locked(scope, thread_id) as after:
            if await self._current(scope, thread_id, epoch, after):
                part, parts = int(payload["part"]), int(payload["parts"])
                told = self.state.part(
                    scope, thread_id, epoch, origin, n, part, parts, payload["entries"]
                )
                self._tell(scope, thread_id, epoch, told)

    async def _current(self, scope: str, thread_id: str, epoch: int, after: _Aftermath) -> bool:
        room = self._rooms.get((scope, thread_id))
        if room is None or epoch == room.epoch:
            return True
        if epoch > room.epoch:
            await self._caught_up(scope, thread_id, room, after)
        return (scope, thread_id) not in self._rooms

    def _tell(self, scope: str, thread_id: str, epoch: int, signals: list[Signal]) -> None:
        room = self._rooms.get((scope, thread_id))
        if room is None or room.epoch != epoch:
            return
        for signal in signals:
            payload = signal.model_dump(mode="json")
            for session in room.sessions:
                session.push_signal(payload)

    def sweep(self, forget: bool = False) -> None:
        for scope, thread_id, epoch, ends in self.state.expired(forget):
            self._tell(scope, thread_id, epoch, ends)

    async def _arrive(self, session: Session, thread_id: str) -> None:
        async with self._lock(session.scope, thread_id):
            room = self._rooms.get((session.scope, thread_id))
            if room is None or thread_id not in session.joined or thread_id in session.present:
                return
            session.present.add(thread_id)
            key = (session.scope, thread_id, session.identity.id)
            self._local[key] = self._local.get(key, 0) + 1
            if self._local[key] == 1:
                present = Present(thread_id=thread_id, sender=session.identity)
                self._say(session.scope, room.epoch, present)

    def _departed(self, session: Session, thread_id: str) -> bool:
        key = (session.scope, thread_id, session.identity.id)
        count = self._local.get(key, 0) - 1
        if count > 0:
            self._local[key] = count
            return False
        self._local.pop(key, None)
        return True

    async def added(
        self, scope: str, thread: Thread, sender: Identity, identity: Identity, seq: int
    ) -> None:
        self._publish(
            scope,
            thread.id,
            {
                "kind": "invited",
                "thread_id": thread.id,
                "sender": sender.model_dump(mode="json"),
                "to": identity.id,
                "seq": seq,
                "e": thread.epoch,
            },
        )
        await self._added_here(scope, thread, sender, identity.id, seq)

    async def _added_here(
        self, scope: str, thread: Thread, sender: Identity, identity_id: str, seq: int | None
    ) -> None:
        async with self._lock(scope, thread.id):
            self._outdated(scope, thread)
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

    async def deleted(self, scope: str, thread_id: str, epoch: int, sender: Identity) -> None:
        signal = ThreadDeleted(thread_id=thread_id, sender=sender)
        self._publish(
            scope, thread_id, {"n": next(self._sent), "e": epoch, **signal.model_dump(mode="json")}
        )
        await self._gone(scope, signal, epoch)

    async def _gone(self, scope: str, signal: ThreadDeleted, epoch: int) -> None:
        self._touch(scope, signal.thread_id)
        async with self._lock(scope, signal.thread_id):
            room = self._rooms.get((scope, signal.thread_id))
            if room is not None and room.epoch == epoch:
                self._wipe(scope, signal)

    async def scope_deleted(self, scope: str, sender: Identity) -> None:
        self._publish(
            scope, "", {"kind": "scope.deleted", "sender": sender.model_dump(mode="json")}
        )
        await self._scope_gone(scope, sender)

    async def _scope_gone(self, scope: str, sender: Identity) -> None:
        self._touch(scope, None)
        for held, thread_id in sorted({*self._rooms, *list(self._locks.keys())}):
            if held == scope:
                await self._catch_up(scope, thread_id, sender)

    def _wipe(self, scope: str, signal: ThreadDeleted) -> None:
        thread_id = signal.thread_id
        payload = signal.model_dump(mode="json")
        room = self._rooms.pop((scope, thread_id), None)
        for session in [] if room is None else room.sessions:
            session.push_signal(payload)
            session.joined.pop(thread_id, None)
            session.floors.pop(thread_id, None)
            session.present.discard(thread_id)
            session.working.discard(thread_id)
            session.streams = {s for s in session.streams if s[0] != thread_id}
        for key in [k for k in self._local if k[:2] == (scope, thread_id)]:
            del self._local[key]
        epoch = self.state.drop(scope, thread_id)
        if epoch is not None:
            self._snapshot(scope, thread_id, epoch, [])

    async def on_resume(self) -> None:
        await self.catch_up()
        await self.announce()
        self.request_announce()

    async def drop(self, session: Session) -> None:
        session.dropped = True
        for thread_id in list(session.joined):
            await self.leave(session, thread_id)
        self.sessions.discard(session)

    async def announce(self) -> None:
        for scope, thread_id, epoch, entries in self.state.own():
            self._snapshot(scope, thread_id, epoch, entries)

    def request_announce(self) -> None:
        self._publish("", "", {"kind": "announce", "n": next(self._sent)})

    def _snapshot(
        self, scope: str, thread_id: str, epoch: int, entries: list[dict[str, Any]]
    ) -> None:
        parts: list[list[dict[str, Any]]] = [[]]
        for entry in entries:
            if _size({"entries": [entry]}) > MIRROR_BUDGET:
                logger.warning(
                    "hub: a state entry of %d bytes is too large to mirror", _size(entry)
                )
                continue
            if parts[-1] and _size({"entries": [*parts[-1], entry]}) > MIRROR_BUDGET:
                parts.append([])
            parts[-1].append(entry)
        head = {"kind": "refresh", "n": next(self._sent), "e": epoch, "parts": len(parts)}
        for index, part in enumerate(parts):
            self._publish(scope, thread_id, {**head, "part": index, "entries": part})

    def _mirror(self, scope: str, epoch: int, signal: Signal, n: int) -> None:
        if not self.linked:
            return
        payload = {"n": n, "e": epoch, **signal.model_dump(mode="json")}
        whole = _size(payload) <= MIRROR_BUDGET
        if isinstance(signal, StreamDelta):
            pieces = 1 if whole else -(-len(signal.text) // PIECE)
            payload["d"] = self.state.counted(scope, signal, pieces)
        if whole:
            self._publish(scope, signal.thread_id, payload)
            return
        if not isinstance(signal, StreamDelta):
            logger.warning(
                "hub: a %s signal of %d bytes is too large to mirror", signal.kind, _size(payload)
            )
            return
        for start in range(0, len(signal.text), PIECE):
            piece = {
                **payload,
                "n": next(self._sent),
                "d": payload["d"] + start // PIECE,
                "text": signal.text[start : start + PIECE],
            }
            self._publish(scope, signal.thread_id, piece)

    async def marked(self, origin: Session, signal: Marked) -> None:
        self._marked_here(origin.scope, signal, origin)
        self._publish(origin.scope, signal.thread_id, signal.model_dump(mode="json"))

    def _marked_here(self, scope: str, signal: Marked, origin: Session | None = None) -> None:
        payload = signal.model_dump(mode="json")
        for session in list(self.sessions):
            if (
                session is not origin
                and session.scope == scope
                and session.identity.id == signal.sender.id
            ):
                session.push_signal(payload)

    def unlink(self) -> None:
        if not self.linked:
            return
        for scope, thread_id, epoch, _ in self.state.own():
            self._snapshot(scope, thread_id, epoch, [])
        self.linked = False

    def _publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        if self.linked:
            self._queued(self._notify(scope, thread_id, payload))

    async def _notify(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        try:
            await self.storage.publish(scope, thread_id, {"o": self.origin, **payload})
        except Exception as error:
            logger.warning("hub: mirroring to other replicas failed: %s", error)

    async def on_signal(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        origin = payload.pop("o", None)
        if origin is None or origin == self.origin:
            return
        try:
            n = int(payload.pop("n", 0))
            epoch = int(payload.pop("e", 0))
            kind = payload.get("kind")
            if kind == "announce":
                await self.announce()
                return
            if kind == "refresh":
                await self._refreshed(scope, thread_id, str(origin), n, epoch, payload)
                return
            if kind == "invited":
                thread = await self.storage.get_thread(scope, thread_id)
                if (
                    thread is not None
                    and thread.epoch == epoch
                    and await self.storage.is_member(scope, thread_id, str(payload["to"]))
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
                await self._scope_gone(scope, Identity.model_validate(payload["sender"]))
                return
            d = payload.pop("d", None)
            signal = SIGNALS.validate_python(payload)
            if isinstance(signal, ThreadDeleted):
                await self._gone(scope, signal, epoch)
                return
            await self._fanout(scope, signal, str(origin), n, epoch, None if d is None else int(d))
        except Exception as error:
            logger.warning("hub: a notice from replica %s failed: %s", origin, error)
