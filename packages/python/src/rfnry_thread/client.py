import asyncio
import contextlib
import itertools
import json
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any

from pydantic import ValidationError

from rfnry_thread.connection import Connection
from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    NotFoundError,
    ThreadException,
    TransportError,
    raised,
)
from rfnry_thread.models import (
    EVENTS,
    SIGNALS,
    Content,
    Custom,
    Edit,
    Event,
    History,
    Identity,
    IdentityLike,
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
    Signal,
    Status,
    StatusChanged,
    Step,
    Thread,
    ThreadDeleted,
    as_identity,
    parts,
)
from rfnry_thread.protocol import (
    HELLO_ID,
    PROTOCOL,
    ErrorFrame,
    Hello,
    Notify,
    Push,
    Request,
    Response,
    decode,
    encode,
)
from rfnry_thread.websocket import open_socket

Dial = Callable[[], Awaitable[Connection]]
HeadersSource = dict[str, str] | Callable[[], Awaitable[dict[str, str]]]
ProtocolsSource = str | list[str] | Callable[[], Awaitable[str | list[str]]]
Item = Event | Signal | Joined | StatusChanged

HANDSHAKE = 10.0
DEADLINE = 60.0
BACKOFF = 0.5
BACKOFF_CAP = 8.0
RECONNECT = 0.5
RECONNECT_CAP = 30.0
HEALTHY = 30.0


def unreadable_push(text: str) -> bool:
    try:
        frame = json.loads(text)
    except ValueError:
        return False
    return isinstance(frame, dict) and frame.get("type") == "push"


def _wire(content: Content) -> list[dict[str, Any]]:
    return [part.model_dump(mode="json") for part in parts(content)]


def _who(identity: IdentityLike | None) -> dict[str, Any] | None:
    return None if identity is None else as_identity(identity).model_dump(mode="json")


def _expect[E](event: Event, kind: type[E]) -> E:
    if not isinstance(event, kind):
        raise ThreadException(f"client: the server answered with a {event.kind}")
    return event


async def _supplied[T](source: T | Callable[[], Awaitable[T]] | None, name: str) -> T | None:
    if source is None or not callable(source):
        return source
    try:
        return await source()
    except ThreadException:
        raise
    except Exception as exc:
        raise TransportError(f"client: the {name} function raised: {exc}") from exc


def _own(item: Item, identity: Identity) -> bool:
    if isinstance(item, Joined | Marked):
        return False
    who = getattr(item, "sender", None) or getattr(item, "author", None)
    return isinstance(who, Identity) and who.id == identity.id


class Stream:
    def __init__(
        self,
        client: Client,
        thread_id: str,
        in_reply_to: str | None,
        triggered_by: Identity | None,
        metadata: dict[str, Any] | None,
    ) -> None:
        self.id = uuid.uuid4().hex
        self.text = ""
        self._client = client
        self._thread_id = thread_id
        self._in_reply_to = in_reply_to
        self._triggered_by = triggered_by
        self._metadata = metadata
        self._index = 0
        self._ended = False

    async def __aenter__(self) -> Stream:
        await self._client._request(
            "signal.send",
            {
                "thread_id": self._thread_id,
                "kind": "stream.start",
                "stream_id": self.id,
                "in_reply_to": self._in_reply_to,
            },
        )
        return self

    async def write(self, text: str) -> None:
        self.text += text
        await self._client._notify(
            "signal.send",
            {
                "thread_id": self._thread_id,
                "kind": "stream.delta",
                "stream_id": self.id,
                "index": self._index,
                "text": text,
            },
        )
        self._index += 1

    async def finish(
        self, content: Content | None = None, *, metadata: dict[str, Any] | None = None
    ) -> Message:
        if self._ended:
            raise InputError(f"client: stream {self.id!r} has already ended")
        merged = self._metadata if metadata is None else {**(self._metadata or {}), **metadata}
        self._ended = True
        try:
            return await self._client._message(
                self._thread_id,
                self.text if content is None else content,
                self._in_reply_to,
                self._triggered_by,
                self.id,
                merged,
            )
        finally:
            await self._end()

    async def _end(self) -> None:
        with contextlib.suppress(ThreadException):
            await self._client._request(
                "signal.send",
                {"thread_id": self._thread_id, "kind": "stream.end", "stream_id": self.id},
            )

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> None:
        if self._ended:
            return
        if kind is None:
            await self.finish()
            return
        self._ended = True
        await self._end()


@dataclass
class _Hold:
    events: list[Event] = field(default_factory=list)
    done: asyncio.Event = field(default_factory=asyncio.Event)


class Client:
    identity: Identity
    scope: str

    def __init__(self, dial: Dial) -> None:
        self._dial = dial
        self._connection: Connection | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._ids = itertools.count(1)
        self._inbox: asyncio.Queue[Item | None] = asyncio.Queue()
        self._cursors: dict[str, int] = {}
        self._holds: dict[str, _Hold] = {}
        self._following = False
        self._present: set[str] = set()
        self._left: dict[str, int] = {}
        self._epochs: dict[str, int] = {}
        self._settled: dict[str, Connection | None] = {}
        self._outbox: dict[str, asyncio.Lock] = {}
        self._connected = asyncio.Event()
        self._reader: asyncio.Task[None] | None = None
        self._reconnector: asyncio.Task[None] | None = None
        self._retrier: asyncio.Task[None] | None = None
        self._refollow = False
        self._unjoined: set[str] = set()
        self._attempt = 0
        self._since = 0.0
        self._closed = False
        self._status: Status = "open"
        self._failure: ThreadException | None = None

    @property
    def status(self) -> Status:
        return self._status

    @classmethod
    async def connect(
        cls,
        url: str,
        *,
        headers: HeadersSource | None = None,
        protocols: ProtocolsSource | None = None,
    ) -> Client:
        async def dial() -> Connection:
            return await open_socket(
                url, await _supplied(headers, "headers"), await _supplied(protocols, "protocols")
            )

        return await cls._open(dial)

    @classmethod
    async def _open(cls, dial: Dial) -> Client:
        client = cls(dial)
        await client._establish()
        return client

    async def _establish(self) -> None:
        try:
            connection = await self._dial()
        except ThreadException:
            raise
        except Exception as exc:
            raise TransportError(f"client: could not connect: {exc}") from exc
        adopted = False
        try:
            try:
                await connection.send_text(encode(Hello(protocol=PROTOCOL)))
                reply = decode(await asyncio.wait_for(connection.receive_text(), HANDSHAKE))
            except Exception as exc:
                if isinstance(exc, ThreadException) and not isinstance(exc, InputError):
                    raise
                raise TransportError(f"client: the hello was not answered: {exc}") from exc
            if isinstance(reply, ErrorFrame):
                raise raised(reply.error.code, reply.error.message)
            if not isinstance(reply, Response) or reply.id != HELLO_ID:
                raise TransportError("client: the server did not answer the hello")
            if self._closed:
                raise TransportError("client: the client is closed")
            try:
                identity = Identity.model_validate(reply.payload["identity"])
                scope = str(reply.payload["scope"])
            except (KeyError, ValidationError) as exc:
                raise TransportError(f"client: the hello reply is malformed: {exc}") from exc
            self.identity, self.scope = identity, scope
            self._connection = connection
            self._since = asyncio.get_running_loop().time()
            self._reader = asyncio.get_running_loop().create_task(self._read(connection))
            self._connected.set()
            adopted = True
        finally:
            if not adopted:
                with contextlib.suppress(Exception):
                    await connection.close(
                        1000, "client: closed" if self._closed else "client: hello failed"
                    )

    async def _read(self, connection: Connection) -> None:
        try:
            while True:
                text = await connection.receive_text()
                try:
                    frame = decode(text)
                except InputError:
                    if not unreadable_push(text):
                        raise
                    continue
                if isinstance(frame, Response | ErrorFrame):
                    future = self._pending.get(frame.id)
                    if future is None or future.done():
                        continue
                    if isinstance(frame, ErrorFrame):
                        future.set_exception(raised(frame.error.code, frame.error.message))
                    else:
                        future.set_result(frame.payload)
                elif isinstance(frame, Push):
                    with contextlib.suppress(ValidationError, InputError):
                        self._receive(frame, connection)
        except asyncio.CancelledError:
            raise
        except Exception:
            with contextlib.suppress(Exception):
                await connection.close(1000, "client: connection lost")
        self._lost(connection)

    def _receive(self, push: Push, connection: Connection) -> None:
        if push.name == "joined":
            joined = Joined.model_validate(push.payload)
            thread_id, epoch = joined.thread.id, joined.thread.epoch
            held = self._epochs.get(thread_id, epoch)
            if epoch < held:
                return
            if epoch > held:
                self._cursors[thread_id] = 0
            self._flush(thread_id)
            self._left.pop(thread_id, None)
            self._epochs[thread_id] = epoch
            self._cursors[thread_id] = max(self._cursors.get(thread_id, 0), joined.seq)
            self._settled[thread_id] = connection
            self._inbox.put_nowait(joined)
            return
        if push.name == "event":
            event = EVENTS.validate_python(push.payload)
            hold = self._holds.get(event.thread_id)
            if hold is not None:
                hold.events.append(event)
                return
            cursor = self._cursors.get(event.thread_id)
            if cursor is not None:
                if event.seq <= cursor:
                    return
                self._cursors[event.thread_id] = event.seq
            self._admit(event)
            return
        signal = SIGNALS.validate_python(push.payload)
        if isinstance(signal, ThreadDeleted):
            self._flush(signal.thread_id)
            self._cursors.pop(signal.thread_id, None)
            self._present.discard(signal.thread_id)
        self._inbox.put_nowait(signal)

    def _mine(self, event: Event, kind: type[MemberAdded | MemberRemoved]) -> bool:
        return isinstance(event, kind) and event.identity.id == self.identity.id

    def _admit(self, event: Event, last: bool = True) -> bool:
        removed = last and self._mine(event, MemberRemoved)
        if removed:
            self._cursors.pop(event.thread_id, None)
            self._present.discard(event.thread_id)
        self._inbox.put_nowait(event)
        return removed

    @contextlib.asynccontextmanager
    async def _holding(self, thread_ids: Iterable[str]) -> AsyncIterator[dict[str, _Hold]]:
        ids = list(thread_ids)
        while (
            pending := next((self._holds[t] for t in ids if t in self._holds), None)
        ) is not None:
            await pending.done.wait()
        holds = {thread_id: _Hold() for thread_id in ids}
        self._holds.update(holds)
        try:
            yield holds
        finally:
            for thread_id, hold in holds.items():
                del self._holds[thread_id]
                hold.done.set()
            for thread_id, hold in holds.items():
                if thread_id in self._cursors and self._settled.get(thread_id) is self._connection:
                    self._drain(thread_id, hold.events, self._cursors[thread_id])

    def _flush(self, thread_id: str) -> None:
        if (hold := self._holds.get(thread_id)) is not None:
            self._drain(thread_id, hold.events, self._cursors.get(thread_id, 0))
            hold.events.clear()

    def _drain(self, thread_id: str, events: list[Event], floor: int) -> None:
        readded = max((e.seq for e in events if self._mine(e, MemberAdded)), default=0)
        for event in sorted(events, key=lambda held: held.seq):
            if event.seq <= floor:
                continue
            floor = event.seq
            if thread_id in self._cursors:
                self._cursors[thread_id] = max(self._cursors[thread_id], event.seq)
            if self._admit(event, event.seq > readded):
                return

    def _settle(
        self,
        joined: Joined,
        events: list[Event],
        held: int | None,
        since: int | None,
        announce: bool,
    ) -> None:
        thread_id, epoch = joined.thread.id, joined.thread.epoch
        cursor, now = self._cursors.get(thread_id), self._epochs.get(thread_id)
        if now is not None and epoch < now:
            self._drain(thread_id, events, cursor or 0)
            return
        again = now is not None and epoch > now
        start = since if held == now or not announce else None
        floor = 0 if again or cursor is None else cursor if start is None else min(start, cursor)
        self._epochs[thread_id] = epoch
        self._cursors[thread_id] = max(joined.seq, floor)
        if announce and again:
            self._inbox.put_nowait(joined)
        self._drain(thread_id, events, floor)
        if announce and not again:
            self._inbox.put_nowait(joined)

    def _change(self, status: Status) -> None:
        if self._status in (status, "closed"):
            return
        self._status = status
        self._inbox.put_nowait(StatusChanged(status=status))

    def _lost(self, connection: Connection) -> None:
        if self._connection is not connection:
            return
        self._connection = None
        self._connected.clear()
        if self._retrier is not None and self._retrier is not asyncio.current_task():
            self._retrier.cancel()
        if asyncio.get_running_loop().time() - self._since >= HEALTHY:
            self._attempt = 0
        for future in self._pending.values():
            if not future.done():
                future.set_exception(TransportError("client: connection lost"))
        if self._closed:
            return
        self._change("reconnecting")
        if self._reconnector is None:
            self._reconnector = asyncio.get_running_loop().create_task(self._reconnect())

    async def _reconnect(self) -> None:
        try:
            while not self._closed:
                attempt = self._attempt
                ceiling = min(RECONNECT_CAP, RECONNECT * 2 ** (attempt - 1)) if attempt else 0.0
                self._attempt += 1
                await asyncio.sleep(random.random() * ceiling)
                try:
                    await self._establish()
                except (ForbiddenError, ConfigurationError, InputError) as error:
                    self._fail(error)
                    return
                except ThreadException:
                    continue
                await self._rejoin()
                if self._connection is not None:
                    self._change("open")
                    return
        finally:
            self._reconnector = None

    async def _rejoin(self) -> None:
        connection = self._connection
        self._refollow, self._unjoined = False, set()
        followed = self._following and await self._refollowed()
        if self._connection is not connection:
            return
        for thread_id in list(self._cursors):
            if followed and thread_id not in self._present:
                continue
            if not await self._rejoin_one(thread_id):
                return
        if self._refollow or self._unjoined:
            self._retrier = asyncio.get_running_loop().create_task(self._retry(connection))

    async def _refollowed(self) -> bool:
        try:
            await self._follow(True, dict(self._cursors), dict(self._left))
        except ForbiddenError, NotFoundError:
            self._following = self._refollow = False
            return False
        except ThreadException as error:
            self._refollow = await self._tolerated(error)
            return False
        self._refollow = False
        return True

    async def _rejoin_one(self, thread_id: str) -> bool:
        async with self._holding([thread_id]) as holds:
            cursor = self._cursors.get(thread_id)
            if cursor is not None:
                try:
                    await self._join(
                        holds[thread_id].events,
                        thread_id,
                        cursor,
                        away=thread_id not in self._present,
                        announce=True,
                        epoch=self._epochs.get(thread_id),
                    )
                except ForbiddenError, NotFoundError:
                    self._cursors.pop(thread_id, None)
                    self._present.discard(thread_id)
                except ThreadException as error:
                    if not await self._tolerated(error):
                        return False
                    self._unjoined.add(thread_id)
                    return True
            self._unjoined.discard(thread_id)
            return True

    async def _tolerated(self, error: ThreadException) -> bool:
        if isinstance(error, InputError | ConfigurationError):
            await self._end(error)
            return False
        if isinstance(error, TransportError):
            await self._abandon()
            return False
        return True

    async def _retry(self, connection: Connection | None) -> None:
        attempt = 0
        while self._refollow or self._unjoined:
            await asyncio.sleep(random.random() * min(RECONNECT_CAP, RECONNECT * 2**attempt))
            attempt += 1
            if self._connection is not connection:
                return
            if self._refollow and await self._refollowed():
                self._unjoined &= self._present
            if self._connection is not connection:
                return
            for thread_id in sorted(self._unjoined):
                if not await self._rejoin_one(thread_id):
                    return

    async def _end(self, error: ThreadException) -> None:
        self._fail(error)
        await self._abandon()

    async def _abandon(self) -> None:
        connection = self._connection
        if connection is None:
            return
        with contextlib.suppress(Exception):
            await connection.close(1000, "client: resume failed")
        self._lost(connection)

    def _fail(self, error: ThreadException) -> None:
        self._failure = error
        self._closed = True
        self._change("closed")
        self._inbox.put_nowait(None)

    async def _request(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self._closed:
            raise self._failure or TransportError("client: the client is closed")
        connection = self._connection
        if connection is None:
            raise TransportError("client: not connected")
        request_id = str(next(self._ids))
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await connection.send_text(
                encode(Request(id=request_id, method=method, payload=payload))
            )
            return await future
        except ThreadException:
            raise
        except Exception as exc:
            raise TransportError(f"client: {method} failed: {exc}") from exc
        finally:
            self._pending.pop(request_id, None)

    async def _notify(self, method: str, payload: dict[str, Any]) -> None:
        connection = self._connection
        if connection is None:
            return
        with contextlib.suppress(Exception):
            await connection.send_text(encode(Notify(method=method, payload=payload)))

    async def _send(
        self, thread_id: str, fields: dict[str, Any], client_id: str | None = None
    ) -> Event:
        payload = {"thread_id": thread_id, "client_id": client_id or uuid.uuid4().hex, **fields}
        lock = self._outbox.setdefault(thread_id, asyncio.Lock())
        async with lock:
            reply = await self._deliver(payload)
        return EVENTS.validate_python(reply["event"])

    async def _deliver(self, payload: dict[str, Any]) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        attempt = 0
        while True:
            try:
                return await self._request("event.send", payload)
            except TransportError:
                if self._closed:
                    raise
                elapsed = loop.time() - started
                if elapsed >= DEADLINE:
                    raise TransportError(
                        f"client: event {payload['client_id']!r} for thread {payload['thread_id']!r} "
                        f"was not acknowledged within {DEADLINE:g}s"
                    ) from None
                delay = min(BACKOFF * 2**attempt, BACKOFF_CAP, DEADLINE - elapsed)
                attempt += 1
                if self._connected.is_set():
                    await asyncio.sleep(delay)
                else:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self._connected.wait(), delay)

    async def _message(
        self,
        thread_id: str,
        content: Content,
        in_reply_to: str | None,
        triggered_by: IdentityLike | None,
        client_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Message:
        event = await self._send(
            thread_id,
            {
                "kind": "message",
                "content": _wire(content),
                "in_reply_to": in_reply_to,
                "triggered_by": _who(triggered_by),
                "metadata": metadata or {},
            },
            client_id,
        )
        return _expect(event, Message)

    async def _join(
        self,
        events: list[Event],
        thread_id: str,
        since: int | None,
        resume: bool = False,
        away: bool = False,
        announce: bool = False,
        epoch: int | None = None,
    ) -> Joined:
        payload: dict[str, Any] = {"thread_id": thread_id, "since": since}
        if resume:
            payload["resume"] = True
        if away:
            payload["away"] = True
        if epoch is not None:
            payload["epoch"] = epoch
        held, connection = self._epochs.get(thread_id), self._connection
        joined = Joined.model_validate(await self._request("thread.join", payload))
        self._settle(joined, events, held, 0 if resume else since, announce)
        self._settled[thread_id] = connection
        return joined

    async def create_thread(
        self,
        thread_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        history: History = "all",
    ) -> Thread:
        payload: dict[str, Any] = {"thread_id": thread_id, "metadata": metadata or {}}
        if history != "all":
            payload["history"] = history
        reply = await self._request("thread.create", payload)
        return Thread.model_validate(reply["thread"])

    async def get_thread(self, thread_id: str) -> Thread:
        return Thread.model_validate(
            (await self._request("thread.get", {"thread_id": thread_id}))["thread"]
        )

    async def list_threads(
        self,
        before: str | Thread | None = None,
        limit: int = 50,
        order: Order = "created",
        preview: bool = False,
        metadata: dict[str, Any] | None = None,
        query: str | None = None,
    ) -> list[Thread]:
        payload: dict[str, Any] = {"before": before, "limit": limit}
        if isinstance(before, Thread):
            if before.last_seq is None:
                payload["before"] = before.id
            else:
                stamps = before.model_dump(
                    mode="json", include={"created_at", "last_message_at", "last_seq"}
                )
                payload["before"] = {"id": before.id, "last_message_at": None, **stamps}
        if order != "created":
            payload["order"] = order
        if preview:
            payload["preview"] = True
        if metadata is not None:
            payload["metadata"] = metadata
        if query is not None:
            payload["query"] = query
        reply = await self._request("thread.list", payload)
        return [Thread.model_validate(t) for t in reply["threads"]]

    async def update_thread(self, thread_id: str, metadata: dict[str, Any]) -> Thread:
        reply = await self._request("thread.update", {"thread_id": thread_id, "metadata": metadata})
        return Thread.model_validate(reply["thread"])

    async def delete_thread(self, thread_id: str) -> None:
        await self._request("thread.delete", {"thread_id": thread_id})
        self._cursors.pop(thread_id, None)

    async def join(self, thread_id: str, since: int | None = None, resume: bool = False) -> Joined:
        self._left.pop(thread_id, None)
        async with self._holding([thread_id]) as holds:
            joined = await self._join(holds[thread_id].events, thread_id, since, resume)
        self._present.add(thread_id)
        self._unjoined.discard(thread_id)
        return joined

    async def leave(self, thread_id: str) -> None:
        while (pending := self._holds.get(thread_id)) is not None:
            await pending.done.wait()
        seq = self._cursors.pop(thread_id, None)
        self._present.discard(thread_id)
        if self._following and (seq is not None or thread_id not in self._left):
            self._left[thread_id] = seq or 0
        await self._request("thread.leave", {"thread_id": thread_id})

    async def away(self, thread_id: str) -> None:
        while (pending := self._holds.get(thread_id)) is not None:
            await pending.done.wait()
        self._present.discard(thread_id)
        await self._request("thread.away", {"thread_id": thread_id})

    async def follow(self, resume: bool = False) -> list[Joined]:
        self._left.clear()
        found = await self._follow(resume, None, None)
        self._refollow = False
        return found

    async def _follow(
        self, resume: bool, since: dict[str, int] | None, left: dict[str, int] | None
    ) -> list[Joined]:
        async with self._holding(since or {}) as holds:
            payload: dict[str, Any] = {
                "resume": resume,
                "since": None if since is None else self._positions(since),
            }
            if left:
                payload["left"] = self._positions(left)
            held = {thread_id: self._epochs.get(thread_id) for thread_id in holds}
            connection = self._connection
            reply = await self._request("thread.follow", payload)
            self._following = True
            found: list[Joined] = []
            raced: list[str] = []
            replied = [Joined.model_validate(joined) for joined in reply["joined"]]
            for thread_id in (
                holds.keys() - {joined.thread.id for joined in replied} - self._present
            ):
                self._cursors.pop(thread_id, None)
            for joined in replied:
                thread_id = joined.thread.id
                if thread_id in self._left and thread_id not in (left or {}):
                    raced.append(thread_id)
                    continue
                self._left.pop(thread_id, None)
                found.append(joined)
                hold = holds.get(thread_id)
                self._settle(
                    joined,
                    [] if hold is None else hold.events,
                    held.get(thread_id, self._epochs.get(thread_id)),
                    (since or {}).get(thread_id),
                    True,
                )
                self._settled[thread_id] = connection
        for thread_id in raced:
            await self._request("thread.leave", {"thread_id": thread_id})
        return found

    def _positions(self, seqs: dict[str, int]) -> dict[str, dict[str, Any]]:
        return {
            thread_id: {"seq": seq, "epoch": self._epochs[thread_id]}
            if thread_id in self._epochs
            else {"seq": seq}
            for thread_id, seq in seqs.items()
        }

    async def mark(self, thread_id: str, seq: int, back: bool = False) -> int:
        payload: dict[str, Any] = {"thread_id": thread_id, "seq": seq}
        if back:
            payload["back"] = True
        reply = await self._request("thread.mark", payload)
        return int(reply["seq"])

    async def marked(self, thread_id: str) -> int | None:
        reply = await self._request("thread.marked", {"thread_id": thread_id})
        seq = reply["seq"]
        return None if seq is None else int(seq)

    async def history(
        self,
        thread_id: str,
        before: int | None = None,
        limit: int = 50,
        kinds: list[str] | None = None,
        after: int | None = None,
    ) -> list[Event]:
        payload: dict[str, Any] = {
            "thread_id": thread_id,
            "before": before,
            "limit": limit,
            "kinds": kinds,
        }
        if after is not None:
            payload["after"] = after
        reply = await self._request("thread.history", payload)
        return [EVENTS.validate_python(e) for e in reply["events"]]

    async def search(
        self, thread_id: str, query: str, before: int | None = None, limit: int = 50
    ) -> list[Message]:
        reply = await self._request(
            "thread.search",
            {"thread_id": thread_id, "query": query, "before": before, "limit": limit},
        )
        return [Message.model_validate(m) for m in reply["events"]]

    async def get_event(self, thread_id: str, event_id: str) -> Event:
        reply = await self._request("event.get", {"thread_id": thread_id, "event_id": event_id})
        return EVENTS.validate_python(reply["event"])

    async def present(self, thread_id: str) -> list[Identity]:
        reply = await self._request("thread.present", {"thread_id": thread_id})
        return [Identity.model_validate(i) for i in reply["identities"]]

    async def add_member(self, thread_id: str, identity: IdentityLike) -> Member:
        reply = await self._request(
            "member.add", {"thread_id": thread_id, "identity": _who(identity)}
        )
        return Member.model_validate(reply["member"])

    async def remove_member(self, thread_id: str, identity: IdentityLike) -> None:
        await self._request(
            "member.remove", {"thread_id": thread_id, "identity_id": as_identity(identity).id}
        )

    async def list_members(self, thread_id: str) -> list[Member]:
        reply = await self._request("member.list", {"thread_id": thread_id})
        return [Member.model_validate(m) for m in reply["members"]]

    async def send(
        self,
        thread_id: str,
        content: Content,
        in_reply_to: str | None = None,
        triggered_by: IdentityLike | None = None,
        client_id: str | None = None,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> Message:
        return await self._message(
            thread_id, content, in_reply_to, triggered_by, client_id, metadata
        )

    def stream(
        self,
        thread_id: str,
        in_reply_to: str | None = None,
        triggered_by: IdentityLike | None = None,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> Stream:
        return Stream(
            self,
            thread_id,
            in_reply_to,
            None if triggered_by is None else as_identity(triggered_by),
            metadata,
        )

    async def step(
        self,
        thread_id: str,
        name: str,
        input: dict[str, Any] | None = None,
        output: list[Part] | None = None,
        is_error: bool = False,
        duration_ms: int | None = None,
        in_reply_to: str | None = None,
        triggered_by: IdentityLike | None = None,
        client_id: str | None = None,
    ) -> Step:
        event = await self._send(
            thread_id,
            {
                "kind": "step",
                "name": name,
                "input": input or {},
                "output": _wire(output or []),
                "is_error": is_error,
                "duration_ms": duration_ms,
                "in_reply_to": in_reply_to,
                "triggered_by": _who(triggered_by),
            },
            client_id,
        )
        return _expect(event, Step)

    async def edit(
        self, thread_id: str, target: str, content: Content, client_id: str | None = None
    ) -> Edit:
        event = await self._send(
            thread_id, {"kind": "edit", "target": target, "content": _wire(content)}, client_id
        )
        return _expect(event, Edit)

    async def retract(self, thread_id: str, target: str, client_id: str | None = None) -> Retract:
        return _expect(
            await self._send(thread_id, {"kind": "retract", "target": target}, client_id), Retract
        )

    async def read(self, thread_id: str, up_to_seq: int) -> Read:
        return _expect(await self._send(thread_id, {"kind": "read", "up_to_seq": up_to_seq}), Read)

    async def custom(
        self, thread_id: str, name: str, payload: dict[str, Any], client_id: str | None = None
    ) -> Custom:
        event = await self._send(
            thread_id, {"kind": "custom", "name": name, "payload": payload}, client_id
        )
        return _expect(event, Custom)

    async def working(self, thread_id: str, label: str | None = None) -> None:
        await self._request(
            "signal.send", {"thread_id": thread_id, "kind": "working", "label": label}
        )

    async def idle(self, thread_id: str) -> None:
        await self._request("signal.send", {"thread_id": thread_id, "kind": "idle"})

    async def cancel(self, thread_id: str, to: IdentityLike) -> None:
        await self._request(
            "signal.send", {"thread_id": thread_id, "kind": "cancel", "to": _who(to)}
        )

    async def events(self, include_own: bool = False) -> AsyncIterator[Item]:
        while True:
            item = await self._inbox.get()
            if item is None:
                self._inbox.put_nowait(None)
                if self._failure is not None:
                    raise self._failure
                return
            if include_own or not _own(item, self.identity):
                yield item

    async def close(self) -> None:
        if self._closed and self._connection is None and self._reader is None:
            return
        self._closed = True
        for task in (self._reconnector, self._retrier, self._reader):
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._reconnector = self._retrier = self._reader = None
        connection, self._connection = self._connection, None
        self._connected.clear()
        if connection is not None:
            with contextlib.suppress(Exception):
                await connection.close(1000, "client: closed")
        for future in self._pending.values():
            if not future.done():
                future.set_exception(TransportError("client: the client is closed"))
        self._change("closed")
        self._inbox.put_nowait(None)
