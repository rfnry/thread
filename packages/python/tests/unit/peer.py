import asyncio
import itertools
import json
from typing import Any

from rfnry_thread.errors import raised
from rfnry_thread.models import Identity
from rfnry_thread.pipe import End
from rfnry_thread.protocol import Hello, Notify, Request, encode
from rfnry_thread.server import Server


class Peer:
    def __init__(self, end: End) -> None:
        self.end = end
        self.pushes: list[dict[str, Any]] = []
        self.joined: list[dict[str, Any]] = []
        self._ids = itertools.count(1)

    @classmethod
    async def open(cls, server: Server, identity: Identity, scope: str = "acme") -> Peer:
        peer = cls(server._attach(identity, scope))
        await peer.end.send_text(encode(Hello(protocol=1)))
        reply = await peer.frame()
        assert reply["type"] == "response" and reply["id"] == "hello", reply
        return peer

    async def frame(self, timeout: float = 1.0) -> dict[str, Any]:
        return dict(json.loads(await asyncio.wait_for(self.end.receive_text(), timeout)))

    async def call(self, method: str, **payload: Any) -> dict[str, Any]:
        request_id = str(next(self._ids))
        await self.end.send_text(encode(Request(id=request_id, method=method, payload=payload)))
        while True:
            frame = await self.frame()
            if frame["type"] == "push":
                (self.joined if frame["name"] == "joined" else self.pushes).append(frame["payload"])
            elif frame.get("id") == request_id and frame["type"] == "error":
                raise raised(frame["error"]["code"], frame["error"]["message"])
            elif frame.get("id") == request_id:
                return dict(frame["payload"])

    async def notify(self, method: str, **payload: Any) -> None:
        await self.end.send_text(encode(Notify(method=method, payload=payload)))

    async def push(self, kind: str, timeout: float = 1.0) -> dict[str, Any]:
        async def find() -> dict[str, Any]:
            while True:
                for index, payload in enumerate(self.pushes):
                    if payload["kind"] == kind:
                        return self.pushes.pop(index)
                frame = await self.frame(timeout)
                if frame["type"] == "push":
                    (self.joined if frame["name"] == "joined" else self.pushes).append(
                        frame["payload"]
                    )

        return await asyncio.wait_for(find(), timeout)

    async def quiet(self, wait: float = 0.15) -> list[dict[str, Any]]:
        try:
            while True:
                frame = await self.frame(wait)
                if frame["type"] == "push":
                    (self.joined if frame["name"] == "joined" else self.pushes).append(
                        frame["payload"]
                    )
        except Exception:
            found, self.pushes = self.pushes, []
            return found

    async def close(self) -> None:
        await self.end.close()
