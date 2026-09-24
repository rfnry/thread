import json
from typing import Any

from rfnry_thread.hub import Session
from rfnry_thread.models import Identity
from rfnry_thread.outbound import Outbound


class Recorder:
    def __init__(self) -> None:
        self.frames: list[dict[str, Any]] = []

    async def send_text(self, data: str) -> None:
        self.frames.append(json.loads(data))


async def nothing() -> None:
    return None


def open_session(identity: Identity, scope: str = "acme") -> tuple[Session, Recorder]:
    recorder = Recorder()
    return Session(identity=identity, scope=scope, outbound=Outbound(recorder, nothing)), recorder


async def pushes(session: Session, recorder: Recorder) -> list[dict[str, Any]]:
    await session.outbound.drain()
    found = [frame["payload"] for frame in recorder.frames if frame["type"] == "push"]
    recorder.frames.clear()
    return found


def kinds(payloads: list[dict[str, Any]]) -> list[str]:
    return [payload["kind"] for payload in payloads]
