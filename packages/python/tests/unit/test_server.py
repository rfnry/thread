import asyncio
import json

import pytest

from rfnry_thread import server as server_module
from rfnry_thread.errors import ConfigurationError, InputError, StorageError
from rfnry_thread.models import SYSTEM
from rfnry_thread.pipe import pipe
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.protocol import CLOSE_PROTOCOL, Hello, encode
from rfnry_thread.server import Server

from .factories import ANA, lookalike
from .peer import Peer


def test_limits_are_positive(storage: PostgresStorage) -> None:
    with pytest.raises(ConfigurationError, match="^server: max_inline_bytes must be positive$"):
        Server(storage, max_inline_bytes=0)
    with pytest.raises(ConfigurationError, match="^server: replay_limit must be positive$"):
        Server(storage, replay_limit=0)


async def test_a_hello_is_answered_with_who_and_where_the_consumer_said(server: Server) -> None:
    ours, theirs = pipe()
    task = asyncio.create_task(server.serve(theirs, identity=ANA, scope="acme"))
    await ours.send_text(encode(Hello(protocol=1)))
    reply = json.loads(await asyncio.wait_for(ours.receive_text(), 1.0))
    assert reply == {
        "type": "response",
        "id": "hello",
        "payload": {
            "identity": {"role": "user", "id": "ana", "name": "Ana", "metadata": {}},
            "scope": "acme",
        },
    }
    await ours.close()
    await asyncio.wait_for(task, 1.0)


async def test_serve_refuses_the_system_role_and_an_empty_scope_before_reading(
    server: Server,
) -> None:
    _, theirs = pipe()
    with pytest.raises(ConfigurationError, match="^server: 'system' is the library's role"):
        await server.serve(theirs, identity=SYSTEM, scope="acme")
    with pytest.raises(InputError, match="^server: a scope is required$"):
        await server.serve(theirs, identity=ANA, scope="")
    with pytest.raises(ConfigurationError, match="^server: 'system' is the library's role"):
        await server.connect(identity=SYSTEM, scope="acme")
    with pytest.raises(InputError, match="^server: a scope is required$"):
        await server.connect(identity=ANA, scope=" ")


async def test_attach_trusts_the_identity_connect_already_checked(
    server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    checked: list[str] = []
    original = server_module._identity

    def counting(value: object) -> object:
        checked.append("identity")
        return original(value)  # type: ignore[arg-type]

    monkeypatch.setattr(server_module, "_identity", counting)
    client = await server.connect(identity=ANA, scope="acme")
    try:
        assert checked == ["identity", "identity"]
    finally:
        await client.close()


@pytest.mark.parametrize(
    "first",
    [
        "garbage",
        '{"type": "hello", "protocol": 2}',
        '{"type": "request", "id": "1", "method": "thread.get"}',
    ],
)
async def test_a_bad_first_frame_closes_as_a_protocol_error(server: Server, first: str) -> None:
    ours, theirs = pipe()
    task = asyncio.create_task(server.serve(theirs, identity=ANA, scope="acme"))
    await ours.send_text(first)
    await asyncio.wait_for(task, 1.0)
    assert ours.code == 4000


async def test_a_wrong_protocol_is_told_which_one_the_server_speaks(server: Server) -> None:
    ours, theirs = pipe()
    task = asyncio.create_task(server.serve(theirs, identity=ANA, scope="acme"))
    await ours.send_text('{"type": "hello", "protocol": 2}')
    reply = json.loads(await asyncio.wait_for(ours.receive_text(), 1.0))
    assert reply == {
        "type": "error",
        "id": "hello",
        "error": {
            "code": "input",
            "message": "server: the first frame must be a hello for protocol 1",
        },
    }
    await asyncio.wait_for(task, 1.0)


async def test_a_malformed_frame_after_hello_closes(server: Server) -> None:
    peer = await Peer.open(server, ANA)
    await peer.end.send_text("{")
    await asyncio.sleep(0.05)
    assert peer.end.code == 4000


async def test_an_unknown_method_is_an_input_error(server: Server) -> None:
    peer = await Peer.open(server, ANA)
    with pytest.raises(InputError, match="^server: unknown method 'thread.shout'$"):
        await peer.call("thread.shout")


async def test_serve_reads_the_identity_by_its_fields(server: Server) -> None:
    ours, theirs = pipe()
    task = asyncio.create_task(server.serve(theirs, identity=lookalike(ANA), scope="acme"))
    await ours.send_text(encode(Hello(protocol=1)))
    reply = json.loads(await asyncio.wait_for(ours.receive_text(), 1.0))
    assert reply["payload"]["identity"] == ANA.model_dump(mode="json")
    await ours.close()
    await asyncio.wait_for(task, 1.0)


async def test_a_connection_without_a_hello_is_refused_after_the_deadline(
    server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(server_module, "HELLO", 0.05)
    ours, theirs = pipe()
    task = asyncio.create_task(server.serve(theirs, identity=ANA, scope="acme"))
    reply = json.loads(await asyncio.wait_for(ours.receive_text(), 1.0))
    assert reply["type"] == "error"
    assert reply["error"]["code"] == "transport"
    assert reply["error"]["message"] == "server: no hello within 0.05 seconds"
    await asyncio.wait_for(task, 1.0)
    assert ours.code == CLOSE_PROTOCOL


async def test_a_storage_failure_reaches_the_client_without_the_databases_text(
    server: Server,
    storage: PostgresStorage,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def broken(*args: object, **kwargs: object) -> None:
        raise StorageError("postgres: storage rejected list_threads: secret DETAIL")

    monkeypatch.setattr(storage, "list_threads", broken)
    client = await server.connect(identity=ANA, scope="acme")
    with pytest.raises(StorageError, match="^server: storage failed$"):
        await client.list_threads()
    assert "secret DETAIL" in caplog.text
