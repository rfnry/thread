import asyncio

from rfnry_thread.errors import TransportError


class End:
    def __init__(self, inbox: asyncio.Queue[str | None], outbox: asyncio.Queue[str | None]) -> None:
        self._inbox = inbox
        self._outbox = outbox
        self.peer: End | None = None
        self.code: int | None = None
        self.closed = False

    async def receive_text(self) -> str:
        text = await self._inbox.get()
        if text is None:
            self._inbox.put_nowait(None)
            raise TransportError("pipe: the other end closed")
        return text

    async def send_text(self, data: str) -> None:
        if self.closed:
            raise TransportError("pipe: this end is closed")
        self._outbox.put_nowait(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        if self.closed:
            return
        self.closed = True
        self.code = code
        if self.peer is not None:
            self.peer.code = code
            self.peer.closed = True
        self._outbox.put_nowait(None)
        self._inbox.put_nowait(None)


def pipe() -> tuple[End, End]:
    one: asyncio.Queue[str | None] = asyncio.Queue()
    two: asyncio.Queue[str | None] = asyncio.Queue()
    first, second = End(one, two), End(two, one)
    first.peer, second.peer = second, first
    return first, second
