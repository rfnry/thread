from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

from rfnry_thread.connection import Connection
from rfnry_thread.errors import ConfigurationError, InputError, ThreadException
from rfnry_thread.hub import Hub, Session
from rfnry_thread.models import SYSTEM, Identity, Member, Thread
from rfnry_thread.outbound import Outbound
from rfnry_thread.pipe import End, pipe
from rfnry_thread.protocol import (
    CLOSE_PROTOCOL,
    CLOSE_SLOW,
    HELLO,
    PROTOCOL,
    Hello,
    Notify,
    Request,
    Response,
    decode,
    encode,
    failure,
)
from rfnry_thread.storage import Stop, Storage
from rfnry_thread.verbs import Authorize, Verbs

log = logging.getLogger("rfnry_thread")

IN_FLIGHT = 64


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Create(_Args):
    thread_id: str | None = None
    metadata: dict[str, Any] = {}


class _List(_Args):
    before: str | None = None
    limit: int = 50


class _Ref(_Args):
    thread_id: str


class _Update(_Ref):
    metadata: dict[str, Any]


class _Join(_Ref):
    since: int | None = None


class _History(_Ref):
    before: int | None = None
    limit: int = 50
    kinds: list[str] | None = None


class _Add(_Ref):
    identity: Identity


class _Remove(_Ref):
    identity_id: str


def _args[T: _Args](model: type[T], method: str, payload: dict[str, Any]) -> T:
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        raise InputError(
            f"server: invalid {method} payload: {exc.errors(include_url=False)[0]['msg'].lower()}"
        ) from exc


def _dump(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


def _scope(scope: str) -> str:
    if not isinstance(scope, str) or not scope.strip():
        raise InputError("server: a scope is required")
    return scope


def _identity(identity: Identity) -> Identity:
    if identity.role == "system":
        raise ConfigurationError(
            "server: 'system' is the library's role; connect as a user or an assistant"
        )
    return identity


async def _close(connection: Connection, code: int, reason: str) -> None:
    with contextlib.suppress(Exception):
        await connection.close(code, reason)


async def _refuse(connection: Connection, error: ThreadException) -> None:
    with contextlib.suppress(Exception):
        await connection.send_text(encode(failure(HELLO, error)))
    await _close(connection, CLOSE_PROTOCOL, str(error))


class Server:
    def __init__(
        self,
        storage: Storage,
        *,
        authorize: Authorize | None = None,
        max_inline_bytes: int = 256 * 1024,
        replay_limit: int = 500,
    ) -> None:
        if max_inline_bytes < 1:
            raise ConfigurationError("server: max_inline_bytes must be positive")
        if replay_limit < 1:
            raise ConfigurationError("server: replay_limit must be positive")
        self._storage = storage
        self._hub = Hub(storage, replay_limit)
        self._verbs = Verbs(storage, self._hub, authorize, max_inline_bytes)
        self._tasks: set[asyncio.Task[None]] = set()
        self._stop: Stop | None = None
        self._refresher: asyncio.Task[None] | None = None

    async def serve(self, connection: Connection, identity: Identity, scope: str) -> None:
        _identity(identity)
        _scope(scope)
        try:
            text = await connection.receive_text()
        except Exception:
            return
        try:
            hello = decode(text)
            if not isinstance(hello, Hello) or hello.protocol != PROTOCOL:
                raise InputError(f"server: the first frame must be a hello for protocol {PROTOCOL}")
        except InputError as error:
            await _refuse(connection, error)
            return
        await self._run(connection, identity, scope)

    def _attach(self, identity: Identity, scope: str) -> End:
        _identity(identity)
        _scope(scope)
        ours, theirs = pipe()
        task = asyncio.get_running_loop().create_task(self.serve(theirs, identity, scope))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return ours

    async def _run(self, connection: Connection, identity: Identity, scope: str) -> None:
        async def too_slow() -> None:
            await _close(connection, CLOSE_SLOW, "server: connection too slow")

        session = Session(identity=identity, scope=scope, outbound=Outbound(connection, too_slow))
        self._hub.sessions.add(session)
        session.outbound.put(
            Response(id=HELLO, payload={"identity": _dump(identity), "scope": scope})
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
                    if frame.method == "signal.send" or len(tasks) >= IN_FLIGHT:
                        await self._answer(session, frame)
                        continue
                    task = asyncio.get_running_loop().create_task(self._answer(session, frame))
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

    async def _answer(self, session: Session, frame: Request) -> None:
        try:
            payload = await self._call(session, frame.method, frame.payload)
        except ThreadException as error:
            session.outbound.put(failure(frame.id, error))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("server: %s failed: %s", frame.method, exc)
            session.outbound.put(failure(frame.id, ThreadException("server: request failed")))
        else:
            session.outbound.put(Response(id=frame.id, payload=payload))

    async def _call(self, session: Session, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        actor, scope, verbs = session.identity, session.scope, self._verbs
        if method == "thread.create":
            create = _args(_Create, method, payload)
            return {
                "thread": _dump(
                    await verbs.create_thread(actor, scope, create.thread_id, create.metadata)
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
                    for t in await verbs.list_threads(actor, scope, listing.before, listing.limit)
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
            return _dump(await verbs.join(session, join.thread_id, join.since))
        if method == "thread.leave":
            await verbs.leave(session, _args(_Ref, method, payload).thread_id)
            return {}
        if method == "thread.history":
            history = _args(_History, method, payload)
            events = await verbs.history(
                actor, scope, history.thread_id, history.before, history.limit, history.kinds
            )
            return {"events": [_dump(e) for e in events]}
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
        if method == "thread.present":
            present = await verbs.present(actor, scope, _args(_Ref, method, payload).thread_id)
            return {"identities": [_dump(identity) for identity in present]}
        if method == "signal.send":
            await verbs.signal(session, payload)
            return {}
        raise InputError(f"server: unknown method {method!r}")

    async def create_thread(
        self, scope: str, thread_id: str | None = None, metadata: dict[str, Any] | None = None
    ) -> Thread:
        return await self._verbs.create_thread(SYSTEM, _scope(scope), thread_id, metadata or {})

    async def add_member(self, scope: str, thread_id: str, identity: Identity) -> Member:
        return await self._verbs.add_member(SYSTEM, _scope(scope), thread_id, identity)

    async def delete_thread(self, scope: str, thread_id: str) -> None:
        await self._verbs.delete_thread(SYSTEM, _scope(scope), thread_id)

    async def delete_scope(self, scope: str) -> int:
        return await self._storage.delete_scope(_scope(scope))

    async def verify_absent(self, scope: str) -> bool:
        return await self._storage.verify_absent(_scope(scope))
