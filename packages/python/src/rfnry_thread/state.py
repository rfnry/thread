import math
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
    n: int = 0
    ended: dict[str, StreamStart] = field(default_factory=dict)

    def empty(self) -> bool:
        return not self.present and self.working is None and not self.streams

    def ends(self, thread_id: str) -> list[Signal]:
        ended: list[Signal] = [
            StreamEnd(thread_id=thread_id, sender=self.sender, stream_id=stream_id)
            for stream_id in sorted(self.streams)
        ]
        if self.working is not None:
            ended.append(Idle(thread_id=thread_id, sender=self.sender))
        return ended


class LatestState:
    def __init__(self, origin: str, clock: Callable[[], float] = time.monotonic) -> None:
        self.origin = origin
        self._clock = clock
        self._threads: dict[tuple[str, str], dict[tuple[str, str], _Entry]] = {}

    def apply(self, scope: str, signal: Signal, origin: str, n: int = 0) -> None:
        key = (scope, signal.thread_id)
        if not isinstance(
            signal, Present | Left | Working | Idle | StreamStart | StreamDelta | StreamEnd
        ):
            return
        entries = self._threads.setdefault(key, {})
        slot = (origin, signal.sender.id)
        entry = entries.get(slot) or _Entry(sender=signal.sender, seen=self._clock())
        entry.seen = self._clock()
        entry.n = max(entry.n, n)
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
                    entry.ended.get(signal.stream_id)
                    or StreamStart(
                        thread_id=signal.thread_id, sender=signal.sender, stream_id=signal.stream_id
                    )
                ),
            )
            stream.index = signal.index
            stream.text += signal.text
        else:
            entry.streams.pop(signal.stream_id, None)
        self._store(key, slot, entry)

    def delete(self, scope: str, thread_id: str) -> list[dict[str, Any]]:
        entries = self._threads.pop((scope, thread_id), {})
        now = self._clock()
        kept = {
            slot: _Entry(sender=entry.sender, seen=now, n=entry.n)
            for slot, entry in entries.items()
            if slot[0] != self.origin
        }
        if kept:
            self._threads[(scope, thread_id)] = kept
        return [
            {
                "sender": entry.sender.model_dump(mode="json"),
                "present": False,
                "working": None,
                "streams": [],
            }
            for slot, entry in entries.items()
            if slot[0] == self.origin
        ]

    def _store(self, key: tuple[str, str], slot: tuple[str, str], entry: _Entry) -> None:
        entries = self._threads.setdefault(key, {})
        if entry.empty() and slot[0] == self.origin:
            entries.pop(slot, None)
        else:
            entries[slot] = entry
        if not entries:
            del self._threads[key]

    def _expired(self, origin: str, entry: _Entry, now: float) -> bool:
        return origin != self.origin and now - entry.seen > EXPIRY

    def _live(self, scope: str, thread_id: str) -> list[_Entry]:
        now = self._clock()
        return [
            entry
            for (origin, _), entry in self._threads.get((scope, thread_id), {}).items()
            if not self._expired(origin, entry, now)
        ]

    def holds(self, scope: str, signal: Signal, origin: str) -> bool:
        entry = self._threads.get((scope, signal.thread_id), {}).get((origin, signal.sender.id))
        if entry is None:
            return False
        if isinstance(signal, Left):
            return entry.present
        if isinstance(signal, Idle):
            return entry.working is not None
        if isinstance(signal, StreamStart | StreamDelta | StreamEnd):
            return signal.stream_id in entry.streams
        return True

    def started(self, scope: str, signal: StreamDelta, origin: str) -> StreamStart:
        entry = self._threads[(scope, signal.thread_id)][(origin, signal.sender.id)]
        return entry.streams[signal.stream_id].start

    def expiring(self, forget: bool = False) -> list[tuple[str, str]]:
        now = math.inf if forget else self._clock()
        return [
            key
            for key, entries in self._threads.items()
            if any(self._expired(origin, entry, now) for (origin, _), entry in entries.items())
        ]

    def expire(self, scope: str, thread_id: str, forget: bool = False) -> list[Signal]:
        entries = self._threads.get((scope, thread_id))
        if entries is None:
            return []
        now = self._clock()
        until = math.inf if forget else now
        gone = [
            (slot, entries.pop(slot))
            for slot in [s for s, e in entries.items() if self._expired(s[0], e, until)]
        ]
        gone = [(slot, entry) for slot, entry in gone if not entry.empty()]
        if not forget:
            for slot, entry in gone:
                entries[slot] = _Entry(
                    sender=entry.sender,
                    seen=now,
                    n=entry.n,
                    ended={i: stream.start for i, stream in entry.streams.items()},
                )
        if not entries:
            del self._threads[(scope, thread_id)]
        ended = [signal for _, entry in gone for signal in entry.ends(thread_id)]
        senders = {entry.sender.id: entry.sender for _, entry in gone if entry.present}
        ended.extend(
            Left(thread_id=thread_id, sender=sender)
            for sender in senders.values()
            if not self.is_present(scope, thread_id, sender.id)
        )
        return ended

    def own_ends(self) -> list[tuple[str, Signal]]:
        ended: list[tuple[str, Signal]] = []
        for (scope, thread_id), entries in self._threads.items():
            for (origin, _), entry in entries.items():
                if origin != self.origin:
                    continue
                ended.extend((scope, signal) for signal in entry.ends(thread_id))
                if entry.present:
                    ended.append((scope, Left(thread_id=thread_id, sender=entry.sender)))
        return ended

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

    def refresh(
        self, scope: str, thread_id: str, origin: str, items: list[dict[str, Any]], n: int = 0
    ) -> list[Signal]:
        key = (scope, thread_id)
        told: list[Signal] = []
        for item in items:
            sender = Identity.model_validate(item["sender"])
            slot = (origin, sender.id)
            found = self._threads.get(key, {}).get(slot)
            if found is not None and found.n > n:
                continue
            was = found or _Entry(sender=sender, seen=self._clock())
            present = self.is_present(scope, thread_id, sender.id)
            working = item.get("working")
            entry = _Entry(
                sender=sender,
                seen=self._clock(),
                n=n,
                present=bool(item["present"]),
                working=None
                if working is None
                else Working(thread_id=thread_id, sender=sender, label=working.get("label")),
                streams={
                    s["stream_id"]: was.streams.get(s["stream_id"])
                    or _Stream(
                        StreamStart(
                            thread_id=thread_id,
                            sender=sender,
                            stream_id=s["stream_id"],
                            in_reply_to=s.get("in_reply_to"),
                        )
                    )
                    for s in item.get("streams", [])
                },
            )
            self._store(key, slot, entry)
            if entry.present and not present:
                told.append(Present(thread_id=thread_id, sender=sender))
            if entry.working is not None and (
                was.working is None or was.working.label != entry.working.label
            ):
                told.append(entry.working)
            told.extend(s.start for i, s in entry.streams.items() if i not in was.streams)
            told.extend(
                StreamEnd(thread_id=thread_id, sender=sender, stream_id=i)
                for i in sorted(was.streams)
                if i not in entry.streams
            )
            if was.working is not None and entry.working is None:
                told.append(Idle(thread_id=thread_id, sender=sender))
            if was.present and not self.is_present(scope, thread_id, sender.id):
                told.append(Left(thread_id=thread_id, sender=sender))
        return told
