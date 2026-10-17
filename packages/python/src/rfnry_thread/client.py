import asyncio
import contextlib
import itertools
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import Any

from rfnry_thread.connection import Connection
from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
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
    Identity,
    IdentityLike,
    Joined,
    Marked,
    Member,
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
    HELLO,
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
Item = Event | Signal | Joined | StatusChanged

HANDSHAKE = 10.0
DEADLINE = 60.0
BACKOFF = 0.5
BACKOFF_CAP = 8.0
RECONNECT = 0.5
RECONNECT_CAP = 30.0


def _wire(content: Content) -> list[dict[str, Any]]:
    return [part.model_dump(mode="json") for part in parts(content)]


def _who(identity: IdentityLike | None) -> dict[str, Any] | None:
    return None if identity is None else as_identity(identity).model_dump(mode="json")


def _expect[E](event: Event, kind: type[E]) -> E:
    if not isinstance(event, kind):
        raise ThreadException(f"client: the server answered with a {event.kind}")
    return event


async def _headers(source: HeadersSource | None) -> dict[str, str] | None:
    if source is None or isinstance(source, dict):
        return source
    try:
        return await source()
    except ThreadException:
        raise
    except Exception as exc:
        raise TransportError(f"client: the headers function raised: {exc}") from exc


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

    async def finish(self, content: Content | None = None) -> Message:
        if self._ended:
            raise InputError(f"client: stream {self.id!r} has already ended")
        self._ended = True
        try:
            return await self._client._message(
                self._thread_id,
                self.text if content is None else content,
                self._in_reply_to,
                self._triggered_by,
                self.id,
                self._metadata,
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
        self._rewinds: list[tuple[str, range, set[int]]] = []
        self._following = False
        self._present: set[str] = set()
        self._left: set[str] = set()
        self._leaving: set[str] = set()
        self._outbox: dict[str, asyncio.Lock] = {}
        self._connected = asyncio.Event()
        self._reader: asyncio.Task[None] | None = None
        self._reconnector: asyncio.Task[None] | None = None
        self._closed = False
        self._status: Status = "open"
        self._failure: ThreadException | None = None

    @property
    def status(self) -> Status:
        return self._status

    @classmethod
    async def connect(cls, url: str, *, headers: HeadersSource | None = None) -> Client:
        async def dial() -> Connection:
            return await open_socket(url, await _headers(headers))

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
        try:
            await connection.send_text(encode(Hello(protocol=PROTOCOL)))
            reply = decode(await asyncio.wait_for(connection.receive_text(), HANDSHAKE))
        except Exception as exc:
            with contextlib.suppress(Exception):
                await connection.close(1000, "client: hello failed")
            if isinstance(exc, ThreadException) and not isinstance(exc, InputError):
                raise
            raise TransportError(f"client: the hello was not answered: {exc}") from exc
        if isinstance(reply, ErrorFrame):
            raise raised(reply.error.code, reply.error.message)
        if not isinstance(reply, Response) or reply.id != HELLO:
            raise TransportError("client: the server did not answer the hello")
        self.identity = Identity.model_validate(reply.payload["identity"])
        self.scope = str(reply.payload["scope"])
        self._connection = connection
        self._reader = asyncio.get_running_loop().create_task(self._read(connection))
        self._connected.set()

    async def _read(self, connection: Connection) -> None:
        try:
            while True:
                frame = decode(await connection.receive_text())
                if isinstance(frame, Response | ErrorFrame):
                    future = self._pending.get(frame.id)
                    if future is None or future.done():
                        continue
                    if isinstance(frame, ErrorFrame):
                        future.set_exception(raised(frame.error.code, frame.error.message))
                    else:
                        future.set_result(frame.payload)
                elif isinstance(frame, Push):
                    self._receive(frame)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        self._lost(connection)

    def _receive(self, push: Push) -> None:
        if push.name == "joined":
            joined = Joined.model_validate(push.payload)
            thread_id = joined.thread.id
            self._left.discard(thread_id)
            self._leaving.discard(thread_id)
            self._cursors[thread_id] = max(self._cursors.get(thread_id, 0), joined.seq)
            self._inbox.put_nowait(joined)
            return
        if push.name == "event":
            event = EVENTS.validate_python(push.payload)
            if event.thread_id in self._leaving:
                return
            for rewound, window, taken in self._rewinds:
                if rewound == event.thread_id and event.seq in window and event.seq not in taken:
                    taken.add(event.seq)
                    self._inbox.put_nowait(event)
                    return
            cursor = self._cursors.get(event.thread_id)
            if cursor is not None:
                if event.seq <= cursor:
                    return
                self._cursors[event.thread_id] = event.seq
            if isinstance(event, MemberRemoved) and event.identity.id == self.identity.id:
                self._cursors.pop(event.thread_id, None)
            self._inbox.put_nowait(event)
            return
        signal = SIGNALS.validate_python(push.payload)
        if signal.thread_id in self._leaving and not isinstance(signal, Marked):
            return
        if isinstance(signal, ThreadDeleted):
            self._cursors.pop(signal.thread_id, None)
        self._inbox.put_nowait(signal)

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
        for future in self._pending.values():
            if not future.done():
                future.set_exception(TransportError("client: connection lost"))
        if self._closed:
            return
        self._change("reconnecting")
        if self._reconnector is None:
            self._reconnector = asyncio.get_running_loop().create_task(self._reconnect())

    async def _reconnect(self) -> None:
        attempt = 0
        try:
            while not self._closed:
                ceiling = min(RECONNECT_CAP, RECONNECT * 2 ** (attempt - 1)) if attempt else 0.0
                attempt += 1
                await asyncio.sleep(random.random() * ceiling)
                try:
                    await self._establish()
                except (ForbiddenError, ConfigurationError) as error:
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
        followed = False
        if self._following:
            self._leaving = set(self._left)
            try:
                await self._follow(True, dict(self._cursors))
                followed = True
            except TransportError:
                return
            except ThreadException:
                pass
            for thread_id in sorted(self._leaving):
                if thread_id not in self._leaving:
                    continue
                try:
                    await self._request("thread.leave", {"thread_id": thread_id})
                except TransportError:
                    return
                except ThreadException:
                    pass
            self._leaving = set()
        for thread_id in list(self._cursors):
            cursor = self._cursors.get(thread_id)
            if cursor is None or (followed and thread_id not in self._present):
                continue
            try:
                joined = await self._join(thread_id, cursor)
            except TransportError:
                return
            except ThreadException:
                self._cursors.pop(thread_id, None)
                self._present.discard(thread_id)
                continue
            self._inbox.put_nowait(joined)

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

    async def _join(self, thread_id: str, since: int | None, resume: bool = False) -> Joined:
        held = self._cursors.get(thread_id)
        start = 0 if resume else since
        rewind: tuple[str, range, set[int]] | None = None
        if held is None:
            self._cursors[thread_id] = since or 0
        elif start is not None and start < held:
            rewind = (thread_id, range(start + 1, held + 1), set())
            self._rewinds.append(rewind)
        payload: dict[str, Any] = {"thread_id": thread_id, "since": since}
        if resume:
            payload["resume"] = True
        try:
            reply = await self._request("thread.join", payload)
        finally:
            self._rewinds = [other for other in self._rewinds if other is not rewind]
        joined = Joined.model_validate(reply)
        if joined.seq < (since or 0):
            self._cursors[thread_id] = joined.seq
        else:
            self._cursors[thread_id] = max(self._cursors.get(thread_id, 0), joined.seq)
        return joined

    async def create_thread(
        self, thread_id: str | None = None, metadata: dict[str, Any] | None = None
    ) -> Thread:
        reply = await self._request(
            "thread.create", {"thread_id": thread_id, "metadata": metadata or {}}
        )
        return Thread.model_validate(reply["thread"])

    async def get_thread(self, thread_id: str) -> Thread:
        return Thread.model_validate(
            (await self._request("thread.get", {"thread_id": thread_id}))["thread"]
        )

    async def list_threads(
        self,
        before: str | None = None,
        limit: int = 50,
        order: Order = "created",
        preview: bool = False,
    ) -> list[Thread]:
        payload: dict[str, Any] = {"before": before, "limit": limit}
        if order != "created":
            payload["order"] = order
        if preview:
            payload["preview"] = True
        reply = await self._request("thread.list", payload)
        return [Thread.model_validate(t) for t in reply["threads"]]

    async def update_thread(self, thread_id: str, metadata: dict[str, Any]) -> Thread:
        reply = await self._request("thread.update", {"thread_id": thread_id, "metadata": metadata})
        return Thread.model_validate(reply["thread"])

    async def delete_thread(self, thread_id: str) -> None:
        await self._request("thread.delete", {"thread_id": thread_id})
        self._cursors.pop(thread_id, None)

    async def join(self, thread_id: str, since: int | None = None, resume: bool = False) -> Joined:
        self._left.discard(thread_id)
        self._leaving.discard(thread_id)
        known = thread_id in self._cursors
        try:
            joined = await self._join(thread_id, since, resume)
        except ThreadException:
            if not known:
                self._cursors.pop(thread_id, None)
            raise
        self._present.add(thread_id)
        return joined

    async def leave(self, thread_id: str) -> None:
        self._cursors.pop(thread_id, None)
        self._present.discard(thread_id)
        if self._following:
            self._left.add(thread_id)
        await self._request("thread.leave", {"thread_id": thread_id})

    async def follow(self, resume: bool = False) -> list[Joined]:
        self._left.clear()
        self._leaving.clear()
        return await self._follow(resume, None)

    async def _follow(self, resume: bool, since: dict[str, int] | None) -> list[Joined]:
        reply = await self._request("thread.follow", {"resume": resume, "since": since})
        self._following = True
        found = [
            joined
            for joined in map(Joined.model_validate, reply["joined"])
            if joined.thread.id not in self._leaving
        ]
        for joined in found:
            thread_id = joined.thread.id
            sent = (since or {}).get(thread_id)
            if sent is not None and joined.seq < sent:
                self._cursors[thread_id] = joined.seq
            else:
                self._cursors[thread_id] = max(self._cursors.get(thread_id, 0), joined.seq)
            self._inbox.put_nowait(joined)
        return found

    async def mark(self, thread_id: str, seq: int) -> int:
        reply = await self._request("thread.mark", {"thread_id": thread_id, "seq": seq})
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
        for task in (self._reconnector, self._reader):
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._reconnector = self._reader = None
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
