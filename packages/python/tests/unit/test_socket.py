import asyncio
import contextlib
from collections.abc import AsyncIterator, Sequence
from http import HTTPStatus
from typing import Any

import pytest
from until import until
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request, Response
from websockets.typing import Subprotocol

from rfnry_thread import client as client_module, server as server_module
from rfnry_thread.client import Client, Item
from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    ThreadException,
    TransportError,
)
from rfnry_thread.models import Identity, Joined, Message, StatusChanged, Text
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server

from .factories import BRUNO
from .test_client import drain

ANA_HEADERS = {"Authorization": "Bearer ana"}


def who(request: Request | None) -> Identity | None:
    token = (
        ""
        if request is None
        else request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    )
    return Identity(role="user", id=token, name=token.title()) if token else None


class Live:
    def __init__(self, server: Server) -> None:
        self.server = server
        self.open: list[Served] = []
        self.paused = False
        self.refusing = False
        self.drop_ack = False
        self.drop_join = False
        self.url = ""

    async def drop(self) -> None:
        for served in self.open:
            await served.socket.close()
        self.open.clear()


class Served:
    def __init__(self, socket: ServerConnection, live: Live) -> None:
        self.socket = socket
        self.live = live

    async def receive_text(self) -> str:
        data = await self.socket.recv()
        return data if isinstance(data, str) else data.decode()

    async def send_text(self, data: str) -> None:
        if self.live.drop_ack and data.startswith('{"type":"response"') and '"event":' in data:
            self.live.drop_ack = False
            await self.socket.close()
            return
        if self.live.drop_join and data.startswith('{"type":"response"') and '"truncated":' in data:
            self.live.drop_join = False
            await self.socket.close()
            return
        await self.socket.send(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        await self.socket.close(code, reason or "")


@contextlib.asynccontextmanager
async def running(server: Server) -> AsyncIterator[Live]:
    live = Live(server)

    def upgrade(socket: ServerConnection, request: Request) -> Response | None:
        if live.paused:
            return socket.respond(HTTPStatus.SERVICE_UNAVAILABLE, "paused\n")
        if who(request) is None:
            return socket.respond(HTTPStatus.UNAUTHORIZED, "who are you?\n")
        return None

    async def handler(socket: ServerConnection) -> None:
        if live.refusing:
            await socket.close(1008, "session expired")
            return
        identity = who(socket.request)
        assert identity is not None
        served = Served(socket, live)
        live.open.append(served)
        await server.serve(served, identity=identity, scope="acme")

    async with serve(handler, "127.0.0.1", 0, process_request=upgrade) as listening:
        live.url = f"ws://127.0.0.1:{next(iter(listening.sockets)).getsockname()[1]}"
        yield live


@pytest.fixture(autouse=True)
def fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    monkeypatch.setattr(client_module, "BACKOFF", 0.01)


@pytest.fixture
async def live(server: Server) -> AsyncIterator[Live]:
    async with running(server) as live:
        yield live


def first_text(message: Message) -> str:
    part = message.content[0]
    assert isinstance(part, Text)
    return part.text


async def until_joined(client: Client, timeout: float = 3.0) -> list[Item]:
    items: list[Item] = []

    async def collect() -> None:
        async for item in client.events():
            items.append(item)
            if isinstance(item, Joined):
                return

    await asyncio.wait_for(collect(), timeout)
    return items


async def room(live: Live) -> tuple[Client, Client]:
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", BRUNO)
    await ana.join("t1")
    await drain(ana)
    return ana, bruno


async def read_to_the_end(client: Client, seen: list[Item]) -> None:
    async for item in client.events():
        seen.append(item)


async def test_a_socket_client_is_the_same_client(live: Live) -> None:
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    assert (ana.identity.id, ana.scope) == ("ana", "acme")
    await ana.create_thread(thread_id="t1")
    await ana.send("t1", "hello")
    assert [e.kind for e in await ana.history("t1")] == ["member_added", "message"]
    await ana.close()


async def test_a_refused_upgrade_and_an_unreachable_server_raise(live: Live) -> None:
    with pytest.raises(
        ForbiddenError, match="^websocket: the server refused the connection with http 401$"
    ):
        await Client.connect(live.url)
    with pytest.raises(TransportError, match="^client: could not connect"):
        await Client.connect("ws://127.0.0.1:1", headers=ANA_HEADERS)


async def test_a_headers_function_that_refuses_and_one_that_fails_raise_apart(live: Live) -> None:
    async def refusing() -> dict[str, str]:
        raise ForbiddenError("app: session expired")

    async def failing() -> dict[str, str]:
        raise OSError("network is unreachable")

    with pytest.raises(ForbiddenError, match="^app: session expired$"):
        await Client.connect(live.url, headers=refusing)
    with pytest.raises(
        TransportError, match="^client: the headers function raised: network is unreachable$"
    ):
        await Client.connect(live.url, headers=failing)


@contextlib.asynccontextmanager
async def by_subprotocol(server: Server) -> AsyncIterator[tuple[Live, list[list[str]]]]:
    live, offered = Live(server), []

    def pick(socket: ServerConnection, protocols: Sequence[Subprotocol]) -> Subprotocol | None:
        offered.append([str(p) for p in protocols])
        return protocols[0] if protocols and protocols[0] == "bearer" else None

    async def handler(socket: ServerConnection) -> None:
        assert socket.request is not None
        sent = socket.request.headers.get("Sec-WebSocket-Protocol", "").split(",")
        token = sent[1].strip() if len(sent) > 1 else "anonymous"
        served = Served(socket, live)
        live.open.append(served)
        await server.serve(served, identity=Identity(role="user", id=token), scope="acme")

    async with serve(handler, "127.0.0.1", 0, select_subprotocol=pick) as listening:
        live.url = f"ws://127.0.0.1:{next(iter(listening.sockets)).getsockname()[1]}"
        yield live, offered


async def test_subprotocols_go_with_every_attempt(server: Server) -> None:
    calls: list[int] = []

    async def protocols() -> list[str]:
        calls.append(1)
        return ["bearer", f"ana-{len(calls)}"]

    async with by_subprotocol(server) as (live, offered):
        fixed = await Client.connect(live.url, protocols=["bearer", "bruno"])
        ana = await Client.connect(live.url, protocols=protocols)
        first = ana.identity.id
        await live.drop()
        await until(lambda: len(calls) >= 2 and ana.status == "open", timeout=3.0)
        again = ana.identity.id
        await ana.close()
        await fixed.close()
    assert fixed.identity.id == "bruno" and offered[0] == ["bearer", "bruno"]
    assert (first, again) == ("ana-1", "ana-2")


async def test_one_subprotocol_can_be_a_string(server: Server) -> None:
    async with by_subprotocol(server) as (live, offered):
        alone = await Client.connect(live.url, protocols="bearer")
        await alone.close()
    assert offered == [["bearer"]] and alone.identity.id == "anonymous"


async def test_a_protocols_function_that_refuses_ends_for_good(server: Server) -> None:
    async def refusing() -> list[str]:
        raise ForbiddenError("app: session expired")

    async def failing() -> list[str]:
        raise OSError("network is unreachable")

    async with by_subprotocol(server) as (live, _):
        with pytest.raises(ForbiddenError, match="^app: session expired$"):
            await Client.connect(live.url, protocols=refusing)
        with pytest.raises(
            TransportError, match="^client: the protocols function raised: network is unreachable$"
        ):
            await Client.connect(live.url, protocols=failing)


async def test_a_protocols_function_that_refuses_on_reconnect_closes_the_client(
    server: Server,
) -> None:
    calls: list[int] = []

    async def protocols() -> list[str]:
        calls.append(1)
        if len(calls) > 1:
            raise ForbiddenError("app: session expired")
        return ["bearer", "ana"]

    async with by_subprotocol(server) as (live, _):
        ana = await Client.connect(live.url, protocols=protocols)
        await live.drop()
        with pytest.raises(ForbiddenError, match="^app: session expired$"):
            await asyncio.wait_for(read_to_the_end(ana, []), 3.0)
    assert ana.status == "closed"


async def test_a_headers_function_that_fails_on_a_reconnect_is_called_again(live: Live) -> None:
    calls: list[int] = []

    async def headers() -> dict[str, str]:
        calls.append(1)
        if len(calls) in (2, 3):
            raise OSError("network is unreachable")
        return ANA_HEADERS

    ana = await Client.connect(live.url, headers=headers)
    await live.drop()
    await until(lambda: len(calls) >= 4 and ana.status == "open", timeout=3.0)
    await ana.close()


async def test_a_dropped_socket_resumes_every_joined_thread_in_order_once(live: Live) -> None:
    calls: list[int] = []

    async def headers() -> dict[str, str]:
        calls.append(1)
        return ANA_HEADERS

    ana = await Client.connect(live.url, headers=headers)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", BRUNO)
    await ana.join("t1")
    await drain(ana)
    live.paused = True
    await live.drop()
    for n in range(5):
        await bruno.send("t1", f"m{n}")
    live.paused = False
    items = await until_joined(ana)
    assert [first_text(i) for i in items if isinstance(i, Message)] == [f"m{n}" for n in range(5)]
    assert isinstance(items[-1], Joined) and items[-1].truncated is False
    assert len(calls) >= 2
    await bruno.send("t1", "live")
    after = [i for i in await drain(ana) if isinstance(i, Message)]
    assert [first_text(m) for m in after] == ["live"]


async def test_a_socket_dropped_during_the_rejoin_reconnects_again(live: Live) -> None:
    ana, bruno = await room(live)
    live.drop_join = True
    live.paused = True
    await live.drop()
    await bruno.send("t1", "while away")
    live.paused = False
    items = await until_joined(ana)
    assert [first_text(i) for i in items if isinstance(i, Message)] == ["while away"]
    await bruno.send("t1", "live")
    after = [i for i in await drain(ana) if isinstance(i, Message)]
    assert [first_text(m) for m in after] == ["live"]


async def test_a_dropped_socket_is_reconnecting_until_every_thread_is_resumed_then_open(
    live: Live,
) -> None:
    ana, bruno = await room(live)
    opened = ana.status
    live.paused = True
    await live.drop()
    await until(lambda: ana.status == "reconnecting")
    await bruno.send("t1", "while away")
    await asyncio.sleep(0.1)
    assert (opened, ana.status) == ("open", "reconnecting")
    live.paused = False
    await until(lambda: ana.status == "open", timeout=3.0)
    told = [
        item.status if isinstance(item, StatusChanged) else type(item).__name__
        for item in await drain(ana, include_own=False)
    ]
    assert told == ["reconnecting", "Message", "Joined", "open"]


async def test_a_resume_beyond_the_replay_limit_says_truncated(storage: PostgresStorage) -> None:
    async with running(Server(storage, replay_limit=3)) as live:
        ana, bruno = await room(live)
        live.paused = True
        await live.drop()
        for n in range(5):
            await bruno.send("t1", f"m{n}")
        live.paused = False
        items = await until_joined(ana)
        assert [first_text(i) for i in items if isinstance(i, Message)] == ["m2", "m3", "m4"]
        assert isinstance(items[-1], Joined) and items[-1].truncated is True


async def test_a_resume_into_a_thread_made_again_says_truncated_and_follows_it(
    live: Live,
) -> None:
    ana, bruno = await room(live)
    for n in range(5):
        await bruno.send("t1", f"old {n}")
    await drain(ana)
    live.paused = True
    await live.drop()
    await live.server.delete_thread("acme", "t1")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana.identity)
    live.paused = False
    items = await until_joined(ana)
    assert isinstance(items[-1], Joined) and (items[-1].seq, items[-1].truncated) == (2, True)
    await bruno.send("t1", "new")
    assert [first_text(m) for m in await drain(ana) if isinstance(m, Message)] == ["new"]


async def test_a_write_whose_ack_was_lost_is_stored_exactly_once(live: Live) -> None:
    ana, _ = await room(live)
    live.drop_ack = True
    message = await asyncio.wait_for(ana.send("t1", "exactly once"), 5.0)
    stored = [e for e in await ana.history("t1") if isinstance(e, Message)]
    assert [e.id for e in stored] == [message.id]


async def test_a_write_made_while_disconnected_is_delivered_after_reconnect(live: Live) -> None:
    ana, bruno = await room(live)
    live.paused = True
    await live.drop()
    pending = asyncio.create_task(ana.send("t1", "sent offline"))
    await asyncio.sleep(0.1)
    assert not pending.done()
    live.paused = False
    message = await asyncio.wait_for(pending, 5.0)
    assert [e.id for e in await bruno.history("t1") if isinstance(e, Message)] == [message.id]


async def test_a_write_not_acknowledged_within_the_deadline_fails(
    live: Live, monkeypatch: pytest.MonkeyPatch
) -> None:
    ana, _ = await room(live)
    monkeypatch.setattr(client_module, "DEADLINE", 0.3)
    live.paused = True
    await live.drop()
    with pytest.raises(
        TransportError,
        match=r"^client: event '\w+' for thread 't1' was not acknowledged within 0.3s$",
    ):
        await ana.send("t1", "lost")


async def test_an_upgrade_refused_on_reconnect_ends_the_stream_with_the_error(live: Live) -> None:
    answers = iter([ANA_HEADERS, {}])

    async def headers() -> dict[str, str]:
        return next(answers, {})

    ana = await Client.connect(live.url, headers=headers)
    await live.drop()
    seen: list[Item] = []
    with pytest.raises(
        ForbiddenError, match="^websocket: the server refused the connection with http 401$"
    ):
        await asyncio.wait_for(read_to_the_end(ana, seen), 3.0)
    assert ana.status == "closed"
    assert seen == [StatusChanged(status="reconnecting"), StatusChanged(status="closed")]


async def test_a_connection_closed_as_a_policy_violation_before_its_hello_is_refused(
    live: Live,
) -> None:
    live.refusing = True
    with pytest.raises(
        ForbiddenError,
        match="^websocket: the server refused the connection with code 1008: session expired$",
    ):
        await Client.connect(live.url, headers=ANA_HEADERS)


async def test_a_reconnect_closed_as_a_policy_violation_ends_the_stream_with_the_error(
    live: Live,
) -> None:
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    live.refusing = True
    await live.drop()
    seen: list[Item] = []
    with pytest.raises(
        ForbiddenError,
        match="^websocket: the server refused the connection with code 1008: session expired$",
    ):
        await asyncio.wait_for(read_to_the_end(ana, seen), 3.0)
    assert ana.status == "closed"
    assert seen == [StatusChanged(status="reconnecting"), StatusChanged(status="closed")]


async def test_a_socket_closed_as_a_policy_violation_after_its_hello_reconnects(
    live: Live,
) -> None:
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    for served in live.open:
        await served.socket.close(1008, "token rotated")
    live.open.clear()
    seen: list[Item] = []

    async def reopened() -> None:
        async for item in ana.events():
            seen.append(item)
            if item == StatusChanged(status="open"):
                return

    await asyncio.wait_for(reopened(), 3.0)
    assert seen == [StatusChanged(status="reconnecting"), StatusChanged(status="open")]
    await ana.close()


async def test_a_headers_function_that_refuses_on_reconnect_ends_the_stream_with_the_error(
    live: Live,
) -> None:
    calls: list[int] = []

    async def headers() -> dict[str, str]:
        calls.append(1)
        if len(calls) > 1:
            raise ForbiddenError("app: session expired")
        return ANA_HEADERS

    ana = await Client.connect(live.url, headers=headers)
    await live.drop()
    seen: list[Item] = []
    with pytest.raises(ForbiddenError, match="^app: session expired$"):
        await asyncio.wait_for(read_to_the_end(ana, seen), 3.0)
    assert ana.status == "closed"
    assert seen == [StatusChanged(status="reconnecting"), StatusChanged(status="closed")]


async def test_a_library_error_from_the_headers_function_is_raised_as_it_is_and_stops_a_reconnect(
    live: Live,
) -> None:
    async def misconfigured() -> dict[str, str]:
        raise ConfigurationError("app: no token endpoint")

    with pytest.raises(ConfigurationError, match="^app: no token endpoint$"):
        await Client.connect(live.url, headers=misconfigured)
    calls: list[int] = []

    async def headers() -> dict[str, str]:
        calls.append(1)
        if len(calls) > 1:
            raise ConfigurationError("app: no token endpoint")
        return ANA_HEADERS

    ana = await Client.connect(live.url, headers=headers)
    await live.drop()
    seen: list[Item] = []
    with pytest.raises(ConfigurationError, match="^app: no token endpoint$"):
        await asyncio.wait_for(read_to_the_end(ana, seen), 3.0)
    assert (ana.status, len(calls)) == ("closed", 2)
    assert seen == [StatusChanged(status="reconnecting"), StatusChanged(status="closed")]


async def test_a_follower_refollows_once_after_a_drop_and_misses_nothing(live: Live) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ana_identity)
    await ana.follow()
    await drain(ana)
    sent: list[tuple[str, dict[str, Any]]] = []
    request = ana._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append((method, payload))
        return await request(method, payload)

    ana._request = record  # type: ignore[method-assign]
    live.paused = True
    await live.drop()
    await until(lambda: ana.status == "reconnecting")
    await bruno.send("t1", "away one")
    await bruno.create_thread(thread_id="t3")
    await bruno.add_member("t3", ana_identity)
    await bruno.send("t3", "away three")
    live.paused = False
    await until(lambda: ana.status == "open", timeout=3.0)
    said = [first_text(item) for item in await drain(ana) if isinstance(item, Message)]
    assert sorted(said) == ["away one", "away three"]
    follows = [payload for method, payload in sent if method == "thread.follow"]
    assert len(follows) == 1
    assert follows[0]["resume"] is True
    assert set(follows[0]["since"]) == {"t1", "t2"}
    assert [m for m, _ in sent if m == "thread.join"] == []


async def test_a_followed_client_rejoins_only_the_threads_it_joined_explicitly(
    live: Live,
) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ana_identity)
    await ana.follow()
    await ana.join("t2")
    await drain(ana)
    sent: list[tuple[str, dict[str, Any]]] = []
    request = ana._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append((method, payload))
        return await request(method, payload)

    ana._request = record  # type: ignore[method-assign]
    live.paused = True
    await live.drop()
    await until(lambda: ana.status == "reconnecting")
    live.paused = False
    await until(lambda: ana.status == "open", timeout=3.0)
    joins = [payload["thread_id"] for method, payload in sent if method == "thread.join"]
    assert joins == ["t2"]


async def test_a_rejoin_reads_the_cursor_a_live_event_moved_during_the_rejoin_before_it(
    live: Live,
) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ana_identity)
    await ana.follow()
    await ana.join("t1")
    await ana.join("t2")
    await drain(ana)
    request = ana._request
    landed: list[str] = []

    async def land(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join" and not landed:
            other = "t2" if payload["thread_id"] == "t1" else "t1"
            landed.append(other)
            sent = await bruno.send(other, "live during the re-joins")
            await until(lambda: ana._cursors.get(other) == sent.seq)
        return await request(method, payload)

    ana._request = land  # type: ignore[method-assign]
    await drop_and_reopen(live, ana)
    said = [
        (item.thread_id, first_text(item)) for item in await drain(ana) if isinstance(item, Message)
    ]
    assert said == [(landed[0], "live during the re-joins")]


async def test_a_follow_that_fails_leaves_the_client_not_following(live: Live) -> None:
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    with pytest.raises(InputError):
        await ana._follow(False, {"t1": -1}, None)
    assert ana._following is False


async def test_a_failed_refollow_falls_back_to_joining_every_thread(live: Live) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana_identity)
    await ana.follow()
    await drain(ana)
    sent: list[str] = []
    request = ana._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append(method)
        return await request(method, payload)

    async def refuse(
        resume: bool, since: dict[str, int] | None, left: dict[str, int] | None
    ) -> list[Joined]:
        raise ThreadException("client: refused")

    ana._request = record  # type: ignore[method-assign]
    ana._follow = refuse  # type: ignore[method-assign]
    live.paused = True
    await live.drop()
    await until(lambda: ana.status == "reconnecting")
    await bruno.send("t1", "away one")
    live.paused = False
    await until(lambda: ana.status == "open", timeout=3.0)
    said = [first_text(item) for item in await drain(ana) if isinstance(item, Message)]
    assert said == ["away one"]
    assert "thread.join" in sent


async def test_a_refollow_into_a_thread_made_again_follows_it_from_the_new_head(
    live: Live,
) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana_identity)
    await ana.follow()
    for n in range(5):
        await bruno.send("t1", f"old {n}")
    await drain(ana)
    live.paused = True
    await live.drop()
    await until(lambda: ana.status == "reconnecting")
    await live.server.delete_thread("acme", "t1")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana_identity)
    live.paused = False
    await until(lambda: ana.status == "open", timeout=3.0)
    assert ana._cursors["t1"] == 2
    await bruno.send("t1", "new")
    assert [first_text(m) for m in await drain(ana) if isinstance(m, Message)] == ["new"]


async def drop_and_reopen(live: Live, client: Client) -> None:
    live.paused = True
    await live.drop()
    await until(lambda: client.status == "reconnecting")
    live.paused = False
    await until(lambda: client.status == "open", timeout=3.0)


async def test_a_followed_thread_left_stays_left_after_a_drop_until_joined_or_followed(
    live: Live,
) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ana_identity)
    await ana.follow(resume=True)
    await drain(ana)
    await ana.leave("t1")
    await bruno.send("t1", "while away")
    sent: list[tuple[str, dict[str, Any]]] = []
    request = ana._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append((method, payload))
        return await request(method, payload)

    ana._request = record  # type: ignore[method-assign]
    await drop_and_reopen(live, ana)
    assert [m for m, _ in sent] == ["thread.follow"]
    assert sent[0][1]["left"] == {"t1": 2}
    await bruno.send("t1", "after the drop")
    await bruno.send("t2", "elsewhere")
    items = await drain(ana)
    assert [first_text(m) for m in items if isinstance(m, Message)] == ["elsewhere"]
    assert [j.thread.id for j in items if isinstance(j, Joined)] == ["t2"]
    await ana.join("t1")
    await bruno.send("t1", "joined again")
    assert [first_text(m) for m in await drain(ana) if isinstance(m, Message)] == ["joined again"]
    await ana.leave("t2")
    await ana.follow()
    await drop_and_reopen(live, ana)
    await bruno.send("t2", "followed again")
    said = [first_text(m) for m in await drain(ana) if isinstance(m, Message)]
    assert said == ["followed again"]


async def test_a_left_thread_the_follower_is_added_to_again_is_delivered_after_a_drop(
    live: Live,
) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana_identity)
    await ana.follow()
    await drain(ana)
    await ana.leave("t1")
    await bruno.remove_member("t1", ana_identity)
    await bruno.add_member("t1", ana_identity)
    assert [j.thread.id for j in await drain(ana) if isinstance(j, Joined)] == ["t1"]
    await drop_and_reopen(live, ana)
    await bruno.send("t1", "after the drop")
    assert [first_text(m) for m in await drain(ana) if isinstance(m, Message)] == ["after the drop"]


async def test_a_left_thread_the_follower_is_added_to_again_while_away_is_delivered_after_the_drop(
    live: Live,
) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana_identity)
    await ana.follow()
    await drain(ana)
    await ana.leave("t1")
    live.paused = True
    await live.drop()
    await until(lambda: ana.status == "reconnecting")
    await bruno.remove_member("t1", ana_identity)
    await bruno.add_member("t1", ana_identity)
    away = await bruno.send("t1", "while away")
    elsewhere = await live.server.connect(identity=ana_identity, scope="acme")
    await elsewhere.mark("t1", away.seq)
    live.paused = False
    await until(lambda: ana.status == "open", timeout=3.0)
    await bruno.send("t1", "after the drop")
    items = await drain(ana)
    assert [j.thread.id for j in items if isinstance(j, Joined)] == ["t1"]
    assert [first_text(m) for m in items if isinstance(m, Message)] == ["after the drop"]


@pytest.mark.parametrize("gone", ["removed", "deleted"])
async def test_a_thread_opened_before_it_was_removed_or_deleted_is_not_reopened_after_a_drop(
    live: Live, gone: str
) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana_identity)
    await ana.follow()
    await ana.join("t1")
    await drain(ana)
    if gone == "removed":
        await bruno.remove_member("t1", ana_identity)
    else:
        await bruno.delete_thread("t1")
        await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana_identity)
    await until(lambda: "t1" in ana._cursors)
    await drain(ana)
    sent: list[str] = []
    request = ana._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append(method)
        return await request(method, payload)

    ana._request = record  # type: ignore[method-assign]
    await drop_and_reopen(live, ana)
    assert sent == ["thread.follow"]
    assert await bruno.present("t1") == []


async def followed_and_left(live: Live) -> tuple[Client, Client, Identity]:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ana_identity)
    await ana.follow()
    await drain(ana)
    return ana, bruno, ana_identity


async def test_leaving_a_thread_twice_keeps_it_left_after_a_drop(live: Live) -> None:
    ana, bruno, _ = await followed_and_left(live)
    await ana.leave("t1")
    await ana.leave("t1")
    await drop_and_reopen(live, ana)
    await bruno.send("t1", "left")
    await bruno.send("t2", "followed")
    items = await drain(ana)
    assert [first_text(m) for m in items if isinstance(m, Message)] == ["followed"]
    assert [j.thread.id for j in items if isinstance(j, Joined)] == ["t2"]


async def test_a_leave_made_while_the_refollow_is_in_flight_holds(live: Live) -> None:
    ana, bruno, _ = await followed_and_left(live)
    request = ana._request

    async def leave_meanwhile(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.follow":
            await ana.leave("t1")
        return await request(method, payload)

    ana._request = leave_meanwhile  # type: ignore[method-assign]
    await drop_and_reopen(live, ana)
    ana._request = request  # type: ignore[method-assign]
    await bruno.send("t1", "left")
    await bruno.send("t2", "followed")
    items = await drain(ana)
    assert [first_text(m) for m in items if isinstance(m, Message)] == ["followed"]
    assert [j.thread.id for j in items if isinstance(j, Joined)] == ["t2"]
    await drop_and_reopen(live, ana)
    await bruno.send("t1", "still left")
    assert [first_text(m) for m in await drain(ana) if isinstance(m, Message)] == []


async def test_a_server_from_before_left_still_has_the_client_follow_after_a_drop(
    live: Live, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Before(server_module._Args):
        resume: bool = False
        since: dict[str, int] | None = None
        left: None = None

    monkeypatch.setattr(server_module, "_Follow", Before)
    ana, bruno, ana_identity = await followed_and_left(live)
    await ana.leave("t1")
    await drop_and_reopen(live, ana)
    await bruno.create_thread(thread_id="t3")
    await bruno.add_member("t3", ana_identity)
    await bruno.send("t1", "left")
    await bruno.send("t3", "added later")
    items = await drain(ana)
    assert [first_text(m) for m in items if isinstance(m, Message)] == ["added later"]
    assert sorted(j.thread.id for j in items if isinstance(j, Joined)) == ["t2", "t3"]


async def test_a_left_thread_made_again_while_away_and_the_follower_added_is_delivered(
    live: Live,
) -> None:
    ana, bruno, ana_identity = await followed_and_left(live)
    for n in range(3):
        await bruno.send("t1", f"old {n}")
    await drain(ana)
    await ana.leave("t1")
    live.paused = True
    await live.drop()
    await until(lambda: ana.status == "reconnecting")
    await bruno.delete_thread("t1")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ana_identity)
    live.paused = False
    await until(lambda: ana.status == "open", timeout=3.0)
    await bruno.send("t1", "new")
    items = await drain(ana)
    assert sorted(j.thread.id for j in items if isinstance(j, Joined)) == ["t1", "t2"]
    assert [first_text(m) for m in items if isinstance(m, Message)] == ["new"]
