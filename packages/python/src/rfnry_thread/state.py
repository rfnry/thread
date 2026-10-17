import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from rfnry_thread.models import (
    Identity,
    Idle,
    Left,
    Present,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
    ThreadDeleted,
    Working,
)

REFRESH = 10.0
EXPIRY = 30.0


@dataclass
class _Stream:
    start: StreamStart
    index: int = 0
    text: str = ""


@dataclass
class _Entry:
    sender: Identity
    seen: float
    present: bool = False
    working: Working | None = None
    streams: dict[str, _Stream] = field(default_factory=dict)

    def empty(self) -> bool:
        return not self.present and self.working is None and not self.streams


class LatestState:
    def __init__(self, origin: str, clock: Callable[[], float] = time.monotonic) -> None:
        self.origin = origin
        self._clock = clock
        self._threads: dict[tuple[str, str], dict[tuple[str, str], _Entry]] = {}

    def apply(self, scope: str, signal: Signal, origin: str) -> None:
        key = (scope, signal.thread_id)
        if isinstance(signal, ThreadDeleted):
            self._threads.pop(key, None)
            return
        if not isinstance(
            signal, Present | Left | Working | Idle | StreamStart | StreamDelta | StreamEnd
        ):
            return
        entries = self._threads.setdefault(key, {})
        slot = (origin, signal.sender.id)
        entry = entries.get(slot) or _Entry(sender=signal.sender, seen=self._clock())
        entry.seen = self._clock()
        if isinstance(signal, Present):
            entry.present = True
        elif isinstance(signal, Left):
            entry.present = False
        elif isinstance(signal, Working):
            entry.working = signal
        elif isinstance(signal, Idle):
            entry.working = None
        elif isinstance(signal, StreamStart):
            entry.streams[signal.stream_id] = _Stream(signal)
        elif isinstance(signal, StreamDelta):
            stream = entry.streams.setdefault(
                signal.stream_id,
                _Stream(
                    StreamStart(
                        thread_id=signal.thread_id, sender=signal.sender, stream_id=signal.stream_id
                    )
                ),
            )
            stream.index = signal.index
            stream.text += signal.text
        else:
            entry.streams.pop(signal.stream_id, None)
        self._store(key, slot, entry)

    def _store(self, key: tuple[str, str], slot: tuple[str, str], entry: _Entry) -> None:
        entries = self._threads.setdefault(key, {})
        if entry.empty():
            entries.pop(slot, None)
        else:
            entries[slot] = entry
        if not entries:
            del self._threads[key]

    def _live(self, scope: str, thread_id: str) -> list[_Entry]:
        entries = self._threads.get((scope, thread_id))
        if entries is None:
            return []
        now = self._clock()
        for slot in [
            s for s, e in entries.items() if s[0] != self.origin and now - e.seen > EXPIRY
        ]:
            del entries[slot]
        if not entries:
            del self._threads[(scope, thread_id)]
        return list(entries.values())

    def snapshot(self, scope: str, thread_id: str) -> list[Signal]:
        present: dict[str, Signal] = {}
        rest: list[Signal] = []
        for entry in self._live(scope, thread_id):
            if entry.present:
                present.setdefault(
                    entry.sender.id, Present(thread_id=thread_id, sender=entry.sender)
                )
            if entry.working is not None:
                rest.append(entry.working)
            for stream in entry.streams.values():
                rest.append(stream.start)
                if stream.text:
                    rest.append(
                        StreamDelta(
                            thread_id=thread_id,
                            sender=entry.sender,
                            stream_id=stream.start.stream_id,
                            index=stream.index,
                            text=stream.text,
                        )
                    )
        return [*present.values(), *rest]

    def present(self, scope: str, thread_id: str) -> list[Identity]:
        found: dict[str, Identity] = {}
        for entry in self._live(scope, thread_id):
            if entry.present:
                found.setdefault(entry.sender.id, entry.sender)
        return list(found.values())

    def is_present(self, scope: str, thread_id: str, identity_id: str) -> bool:
        return any(e.present and e.sender.id == identity_id for e in self._live(scope, thread_id))

    def own(self) -> dict[tuple[str, str], list[dict[str, Any]]]:
        items: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for key, entries in self._threads.items():
            for (origin, _), entry in entries.items():
                if origin != self.origin:
                    continue
                items.setdefault(key, []).append(
                    {
                        "sender": entry.sender.model_dump(mode="json"),
                        "present": entry.present,
                        "working": None
                        if entry.working is None
                        else {"label": entry.working.label},
                        "streams": [
                            {"stream_id": s.start.stream_id, "in_reply_to": s.start.in_reply_to}
                            for s in entry.streams.values()
                        ],
                    }
                )
        return items

    def refresh(self, scope: str, thread_id: str, origin: str, items: list[dict[str, Any]]) -> None:
        key = (scope, thread_id)
        for item in items:
            sender = Identity.model_validate(item["sender"])
            slot = (origin, sender.id)
            entry = self._threads.get(key, {}).get(slot) or _Entry(
                sender=sender, seen=self._clock()
            )
            entry.seen = self._clock()
            entry.present = bool(item["present"])
            working = item.get("working")
            entry.working = (
                None
                if working is None
                else Working(thread_id=thread_id, sender=sender, label=working.get("label"))
            )
            entry.streams = {
                s["stream_id"]: entry.streams.get(s["stream_id"])
                or _Stream(
                    StreamStart(
                        thread_id=thread_id,
                        sender=sender,
                        stream_id=s["stream_id"],
                        in_reply_to=s.get("in_reply_to"),
                    )
                )
                for s in item.get("streams", [])
            }
            self._store(key, slot, entry)
