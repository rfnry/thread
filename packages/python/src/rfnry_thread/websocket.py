from __future__ import annotations

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from rfnry_thread.connection import Connection
from rfnry_thread.errors import ForbiddenError
from rfnry_thread.protocol import CLOSE_REFUSED

MAX_FRAME = 16 * 1024 * 1024
REFUSED = frozenset({401, 403})


class _Socket:
    def __init__(self, socket: ClientConnection) -> None:
        self._socket = socket
        self._answered = False

    async def receive_text(self) -> str:
        try:
            data = await self._socket.recv()
        except ConnectionClosed as closed:
            self._refused(closed)
            raise
        self._answered = True
        return data if isinstance(data, str) else data.decode()

    async def send_text(self, data: str) -> None:
        try:
            await self._socket.send(data)
        except ConnectionClosed as closed:
            self._refused(closed)
            raise

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        await self._socket.close(code, reason or "")

    def _refused(self, closed: ConnectionClosed) -> None:
        if self._answered or closed.rcvd is None or closed.rcvd.code != CLOSE_REFUSED:
            return
        reason = f": {closed.rcvd.reason}" if closed.rcvd.reason else ""
        raise ForbiddenError(
            f"websocket: the server refused the connection with code {CLOSE_REFUSED}{reason}"
        ) from closed


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
