import asyncio
import contextlib
import logging
from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from rfnry_thread.client import Client
from rfnry_thread.connection import Connection
from rfnry_thread.errors import (
    ConfigurationError,
    InputError,
    StorageError,
    ThreadException,
    TransportError,
)
from rfnry_thread.hub import Hub, Session
from rfnry_thread.models import (
    SYSTEM,
    Event,
    History,
    Identity,
    IdentityLike,
    Member,
    Order,
    Thread,
    as_identity,
)
from rfnry_thread.outbound import Outbound
from rfnry_thread.pipe import End, pipe
from rfnry_thread.protocol import (
    CLOSE_PROTOCOL,
    CLOSE_SLOW,
    HELLO_ID,
    PROTOCOL,
    Hello,
    Notify,
    Request,
    Response,
    decode,
    encode,
    failure,
)
from rfnry_thread.state import REFRESH
from rfnry_thread.storage import Stop, Storage
from rfnry_thread.verbs import POSITION_MAX, Authorize, Redact, Verbs, _clean, _sized

logger = logging.getLogger(__name__)

IN_FLIGHT = 64
HELLO = 10.0
ORDERED = frozenset({"thread.join", "thread.leave", "thread.away", "thread.follow"})


Seq = Annotated[int, Field(ge=0, le=POSITION_MAX)]


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Create(_Args):
    thread_id: str | None = None
    metadata: dict[str, Any] = {}
    history: History = "all"


class _Anchor(_Args):
    id: str
    created_at: datetime
    last_message_at: datetime | None = None
    last_seq: Seq


class _List(_Args):
    before: _Anchor | str | None = None
    limit: int = 50
    order: Order = "created"
    preview: bool = False
    metadata: dict[str, Any] | None = None
    query: str | None = None


class _Ref(_Args):
    thread_id: str


class _Update(_Ref):
    metadata: dict[str, Any]


class _Join(_Ref):
    since: Seq | None = None
    created_at: datetime | None = None
    resume: bool = False
    away: bool = False


class _Position(_Args):
    seq: Seq
    created_at: datetime | None = None


class _Follow(_Args):
    resume: bool = False
    since: dict[str, _Position] | None = None
    left: dict[str, _Position] | None = None


def _positions(found: dict[str, _Position] | None) -> dict[str, tuple[int, datetime | None]] | None:
    return None if found is None else {t: (p.seq, p.created_at) for t, p in found.items()}


class _History(_Ref):
    before: Seq | None = None
    after: Seq | None = None
    limit: int = 50
    kinds: list[str] | None = None


class _Search(_Ref):
    query: str
    before: Seq | None = None
    limit: int = 50


class _Mark(_Ref):
    seq: Seq
    back: bool = False


class _Add(_Ref):
    identity: Identity


class _Remove(_Ref):
    identity_id: str


class _EventRef(_Ref):
    event_id: str


def _args[T: _Args](model: type[T], method: str, payload: dict[str, Any]) -> T:
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        first = exc.errors(include_url=False)[0]
        reason = str(first["msg"]).lower()
        if first["type"] == "missing":
            reason = f"{reason}: {first['loc'][-1]}"
        raise InputError(f"server: invalid {method} payload: {reason}") from exc


def _dump(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


def _scope(scope: str) -> str:
    if not isinstance(scope, str) or not scope.strip():
        raise InputError("server: a scope is required")
    return _sized(scope, "scope")


def _identity(value: IdentityLike) -> Identity:
    identity = as_identity(value)
    if identity.role == "system":
        raise ConfigurationError(
            "server: 'system' is the library's role; connect as a user or an assistant"
        )
    _sized(identity.id, "identity id")
    _clean(identity.model_dump())
    return identity


async def _close(connection: Connection, code: int, reason: str) -> None:
    with contextlib.suppress(Exception):
        await connection.close(code, reason)


async def _refuse(connection: Connection, error: ThreadException) -> None:
    with contextlib.suppress(Exception):
        await connection.send_text(encode(failure(HELLO_ID, error)))
    await _close(connection, CLOSE_PROTOCOL, str(error))


class Server:
    def __init__(
        self,
        storage: Storage,
        *,
        authorize: Authorize | None = None,
        redact: Redact | None = None,
        max_inline_bytes: int = 256 * 1024,
        replay_limit: int = 500,
    ) -> None:
        if max_inline_bytes < 1:
            raise ConfigurationError("server: max_inline_bytes must be positive")
        if replay_limit < 1:
            raise ConfigurationError("server: replay_limit must be positive")
        self._storage = storage
        self._hub = Hub(storage, replay_limit, authorize)
        self._verbs = Verbs(storage, self._hub, authorize, max_inline_bytes, redact)
        self._tasks: set[asyncio.Task[None]] = set()
        self._stop: Stop | None = None
        self._refresher: asyncio.Task[None] | None = None

    async def serve(self, connection: Connection, *, identity: IdentityLike, scope: str) -> None:
        who = _identity(identity)
        _scope(scope)
        try:
            text = await asyncio.wait_for(connection.receive_text(), HELLO)
        except TimeoutError:
            await _refuse(connection, TransportError(f"server: no hello within {HELLO:g} seconds"))
            return
        except Exception:
            return
        try:
            hello = decode(text)
            if not isinstance(hello, Hello) or hello.protocol != PROTOCOL:
                raise InputError(f"server: the first frame must be a hello for protocol {PROTOCOL}")
        except InputError as error:
            await _refuse(connection, error)
            return
        await self._run(connection, who, scope)

    def _attach(self, identity: Identity, scope: str) -> End:
        ours, theirs = pipe()
        task = asyncio.get_running_loop().create_task(
            self.serve(theirs, identity=identity, scope=scope)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return ours

    async def _run(self, connection: Connection, identity: Identity, scope: str) -> None:
        async def too_slow() -> None:
            await _close(connection, CLOSE_SLOW, "server: connection too slow")

        session = Session(identity=identity, scope=scope, outbound=Outbound(connection, too_slow))
        self._hub.sessions.add(session)
        session.outbound.put(
            Response(id=HELLO_ID, payload={"identity": _dump(identity), "scope": scope})
        )
        tasks: set[asyncio.Task[None]] = set()
        try:
            while True:
                try:
                    text = await connection.receive_text()
                except Exception:
                    break
                try:
                    frame = decode(text)
                except InputError:
                    await session.outbound.drain()
                    await _close(connection, CLOSE_PROTOCOL, "server: malformed frame")
                    break
                if isinstance(frame, Request):
                    ordered = frame.method in ORDERED
                    if ordered:
                        while len(tasks) >= IN_FLIGHT:
                            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    elif frame.method == "signal.send" or len(tasks) >= IN_FLIGHT:
                        await self._answer(session, frame, None)
                        continue
                    after = session.last_ordered if ordered else None
                    task = asyncio.get_running_loop().create_task(
                        self._answer(session, frame, after)
                    )
                    if ordered:
                        session.last_ordered = task
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
                elif isinstance(frame, Notify):
                    if frame.method == "signal.send":
                        with contextlib.suppress(ThreadException):
                            await self._verbs.signal(session, frame.payload)
                else:
                    await _close(
                        connection,
                        CLOSE_PROTOCOL,
                        f"server: clients may not send {frame.type} frames",
                    )
                    break
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._hub.drop(session)
            await session.outbound.drain()
            await session.outbound.close()

    async def _answer(
        self, session: Session, frame: Request, after: asyncio.Task[None] | None
    ) -> None:
        try:
            if after is not None:
                await asyncio.wait({after})
            payload = await self._call(session, frame.method, frame.payload)
        except StorageError as error:
            logger.warning("server: %s failed: %s", frame.method, error)
            session.outbound.put(failure(frame.id, StorageError("server: storage failed")))
        except ThreadException as error:
            session.outbound.put(failure(frame.id, error))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("server: %s failed: %s", frame.method, exc)
            session.outbound.put(failure(frame.id, ThreadException("server: request failed")))
        else:
            session.outbound.put(Response(id=frame.id, payload=payload))

    async def _call(self, session: Session, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        actor, scope, verbs = session.identity, session.scope, self._verbs
        if method == "thread.create":
            create = _args(_Create, method, payload)
            return {
                "thread": _dump(
                    await verbs.create_thread(
                        actor, scope, create.thread_id, create.metadata, create.history
                    )
                )
            }
        if method == "thread.get":
            return {
                "thread": _dump(
                    await verbs.get_thread(actor, scope, _args(_Ref, method, payload).thread_id)
                )
            }
        if method == "thread.list":
            listing = _args(_List, method, payload)
            return {
                "threads": [
                    _dump(t)
                    for t in await verbs.list_threads(
                        actor,
                        scope,
                        listing.before
                        if not isinstance(listing.before, _Anchor)
                        else Thread(
                            id=listing.before.id,
                            scope=scope,
                            created_at=listing.before.created_at,
                            last_message_at=listing.before.last_message_at,
                            last_seq=listing.before.last_seq,
                        ),
                        listing.limit,
                        listing.metadata,
                        listing.order,
                        listing.preview,
                        listing.query,
                    )
                ]
            }
        if method == "thread.update":
            update = _args(_Update, method, payload)
            return {
                "thread": _dump(
                    await verbs.update_thread(actor, scope, update.thread_id, update.metadata)
                )
            }
        if method == "thread.delete":
            await verbs.delete_thread(actor, scope, _args(_Ref, method, payload).thread_id)
            return {}
        if method == "thread.join":
            join = _args(_Join, method, payload)
            return _dump(
                await verbs.join(
                    session, join.thread_id, join.since, join.resume, join.away, join.created_at
                )
            )
        if method == "thread.follow":
            follow = _args(_Follow, method, payload)
            joined = await verbs.follow(
                session, follow.resume, _positions(follow.since), _positions(follow.left)
            )
            return {"joined": [_dump(j) for j in joined]}
        if method == "thread.leave":
            await verbs.leave(session, _args(_Ref, method, payload).thread_id)
            return {}
        if method == "thread.away":
            await verbs.away(session, _args(_Ref, method, payload).thread_id)
            return {}
        if method == "thread.history":
            history = _args(_History, method, payload)
            events = await verbs.history(
                actor,
                scope,
                history.thread_id,
                history.before,
                history.limit,
                history.kinds,
                history.after,
            )
            return {"events": [_dump(e) for e in events]}
        if method == "thread.search":
            search = _args(_Search, method, payload)
            found = await verbs.search(
                actor, scope, search.thread_id, search.query, search.before, search.limit
            )
            return {"events": [_dump(m) for m in found]}
        if method == "thread.mark":
            mark = _args(_Mark, method, payload)
            seq, unread = await verbs.mark(session, mark.thread_id, mark.seq, mark.back)
            return {"seq": seq, "unread": unread}
        if method == "thread.marked":
            ref = _args(_Ref, method, payload)
            return {"seq": await verbs.marked(actor, scope, ref.thread_id)}
        if method == "member.add":
            add = _args(_Add, method, payload)
            return {
                "member": _dump(await verbs.add_member(actor, scope, add.thread_id, add.identity))
            }
        if method == "member.remove":
            remove = _args(_Remove, method, payload)
            await verbs.remove_member(actor, scope, remove.thread_id, remove.identity_id)
            return {}
        if method == "member.list":
            members = await verbs.list_members(actor, scope, _args(_Ref, method, payload).thread_id)
            return {"members": [_dump(m) for m in members]}
        if method == "event.send":
            return {"event": _dump(await verbs.send(actor, scope, payload))}
        if method == "event.get":
            ref = _args(_EventRef, method, payload)
            return {
                "event": _dump(await verbs.get_event(actor, scope, ref.thread_id, ref.event_id))
            }
        if method == "thread.present":
            present = await verbs.present(actor, scope, _args(_Ref, method, payload).thread_id)
            return {"identities": [_dump(identity) for identity in present]}
        if method == "signal.send":
            await verbs.signal(session, payload)
            return {}
        raise InputError(f"server: unknown method {method!r}")

    async def connect(self, *, identity: IdentityLike, scope: str) -> Client:
        who = _identity(identity)
        _scope(scope)

        async def dial() -> Connection:
            return self._attach(who, scope)

        return await Client._open(dial)

    async def start(self) -> None:
        if self._stop is not None:
            return
        doubted = self._hub.doubt()
        self._stop = await self._storage.listen(
            self._hub.on_event, self._hub.on_signal, self._hub.on_resume
        )
        self._hub.linked = True
        self._refresher = asyncio.get_running_loop().create_task(self._refresh())
        if doubted:
            await self._hub.resync()

    async def stop(self) -> None:
        if self._refresher is not None:
            self._refresher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refresher
            self._refresher = None
        if self._stop is not None:
            try:
                await self._hub.unlink()
            finally:
                await self._stop()
                self._stop = None
                await self._hub.sweep(forget=True)

    async def _refresh(self) -> None:
        while True:
            await asyncio.sleep(REFRESH)
            await self._hub.recheck()
            await self._hub.announce()
            await self._hub.sweep()

    async def create_thread(
        self,
        scope: str,
        thread_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        history: History = "all",
    ) -> Thread:
        return await self._verbs.create_thread(
            SYSTEM, _scope(scope), thread_id, metadata or {}, history
        )

    async def get_thread(self, scope: str, thread_id: str) -> Thread:
        return await self._verbs.get_thread(SYSTEM, _scope(scope), thread_id)

    async def list_threads(
        self,
        scope: str,
        *,
        metadata: dict[str, Any] | None = None,
        before: str | Thread | None = None,
        limit: int = 50,
        order: Order = "created",
        preview: bool = False,
        query: str | None = None,
    ) -> list[Thread]:
        return await self._verbs.list_threads(
            SYSTEM, _scope(scope), before, limit, metadata, order, preview, query
        )

    async def update_thread(self, scope: str, thread_id: str, metadata: dict[str, Any]) -> Thread:
        return await self._verbs.update_thread(SYSTEM, _scope(scope), thread_id, metadata)

    async def add_member(self, scope: str, thread_id: str, identity: IdentityLike) -> Member:
        return await self._verbs.add_member(SYSTEM, _scope(scope), thread_id, as_identity(identity))

    async def remove_member(self, scope: str, thread_id: str, identity: IdentityLike) -> None:
        await self._verbs.remove_member(SYSTEM, _scope(scope), thread_id, as_identity(identity).id)

    async def list_members(self, scope: str, thread_id: str) -> list[Member]:
        return await self._verbs.list_members(SYSTEM, _scope(scope), thread_id)

    async def get_event(self, scope: str, thread_id: str, event_id: str) -> Event:
        return await self._verbs.get_event(SYSTEM, _scope(scope), thread_id, event_id)

    async def delete_thread(self, scope: str, thread_id: str) -> None:
        await self._verbs.delete_thread(SYSTEM, _scope(scope), thread_id)

    async def delete_scope(self, scope: str) -> int:
        scope = _scope(scope)
        removed = await self._storage.delete_scope(scope)
        await self._hub.scope_deleted(scope, SYSTEM)
        return removed

    async def verify_absent(self, scope: str) -> bool:
        return await self._storage.verify_absent(_scope(scope))
