from __future__ import annotations

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import InvalidStatus

from rfnry_thread.connection import Connection
from rfnry_thread.errors import ForbiddenError

MAX_FRAME = 16 * 1024 * 1024
REFUSED = frozenset({401, 403})


class _Socket:
    def __init__(self, socket: ClientConnection) -> None:
        self._socket = socket

    async def receive_text(self) -> str:
        data = await self._socket.recv()
        return data if isinstance(data, str) else data.decode()

    async def send_text(self, data: str) -> None:
        await self._socket.send(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        await self._socket.close(code, reason or "")


async def open_socket(url: str, headers: dict[str, str] | None = None) -> Connection:
    try:
        socket = await connect(url, additional_headers=headers, max_size=MAX_FRAME, open_timeout=10)
    except InvalidStatus as exc:
        status = exc.response.status_code
        if status in REFUSED:
            raise ForbiddenError(
                f"websocket: the server refused the connection with http {status}"
            ) from exc
        raise
    return _Socket(socket)
