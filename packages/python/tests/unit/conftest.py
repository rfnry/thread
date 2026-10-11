import uuid
from typing import Any

import pytest

from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server

from .peer import Peer


@pytest.fixture
def server(storage: PostgresStorage) -> Server:
    return Server(storage)


async def say(
    peer: Peer, thread_id: str, text: str, client_id: str | None = None, **extra: Any
) -> dict[str, Any]:
    reply = await peer.call(
        "event.send",
        thread_id=thread_id,
        kind="message",
        client_id=client_id or uuid.uuid4().hex,
        content=[{"type": "text", "text": text}],
        **extra,
    )
    return dict(reply["event"])
