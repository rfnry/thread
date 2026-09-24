from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Protocol

from rfnry_thread.protocol import ErrorFrame, Push, Response, encode

BOUND = 1000
STALL = 10.0

Outgoing = Push | Response | ErrorFrame


class Sink(Protocol):
    async def send_text(self, data: str) -> None: ...


def _delta_key(frame: Outgoing) -> tuple[str, str] | None:
    if (
        isinstance(frame, Push)
        and frame.name == "signal"
        and frame.payload.get("kind") == "stream.delta"
    ):
        return (str(frame.payload["thread_id"]), str(frame.payload["stream_id"]))
    return None


class Outbound:
    def __init__(
        self,
        sink: Sink,
        on_stall: Callable[[], Awaitable[None]],
        *,
        bound: int = BOUND,
        stall: float = STALL,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sink = sink
        self._on_stall = on_stall
        self._bound = bound
        self._stall = stall
        self._clock = clock
        self._frames: deque[Outgoing] = deque()
        self._deltas: dict[tuple[str, str], Push] = {}
        self._ready = asyncio.Event()
        self._progress = clock()
        self._stalled: asyncio.Future[None] | None = None
        self.closed = False
        self._writer = asyncio.create_task(self._run())

    def put(self, frame: Outgoing) -> None:
        if self.closed:
            return
        if not self._frames:
            self._progress = self._clock()
        key = _delta_key(frame)
        if key is not None and isinstance(frame, Push):
            pending = self._deltas.get(key)
            if pending is not None:
                pending.payload["text"] = str(pending.payload["text"]) + str(frame.payload["text"])
                pending.payload["index"] = frame.payload["index"]
                return
            frame = Push(name="signal", payload=dict(frame.payload))
            self._deltas[key] = frame
        self._frames.append(frame)
        self._ready.set()
        if len(self._frames) > self._bound and self._clock() - self._progress > self._stall:
            self.closed = True
            self._stalled = asyncio.ensure_future(self._on_stall())

    async def _run(self) -> None:
        try:
            while True:
                await self._ready.wait()
                while self._frames:
                    frame = self._frames.popleft()
                    key = _delta_key(frame)
                    if key is not None and self._deltas.get(key) is frame:
                        del self._deltas[key]
                    await self._sink.send_text(encode(frame))
                    self._progress = self._clock()
                self._ready.clear()
        except Exception:
            self.closed = True

    async def drain(self, timeout: float = 2.0) -> None:
        async def empty() -> None:
            while self._frames and not self._writer.done():
                await asyncio.sleep(0.005)
            await asyncio.sleep(0)

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(empty(), timeout)

    async def close(self) -> None:
        self.closed = True
        self._writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._writer
