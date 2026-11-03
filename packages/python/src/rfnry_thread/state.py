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

_Key = tuple[str, str]


@dataclass
class _Stream:
    start: StreamStart
    text: str = ""
    index: int = 0


@dataclass
class _Held:
    sender: Identity
    present: bool = False
    working: Working | None = None
    streams: dict[str, _Stream] = field(default_factory=dict)

    def empty(self) -> bool:
        return not self.present and self.working is None and not self.streams

    def entry(self) -> dict[str, Any]:
        return {
            "sender": self.sender.model_dump(mode="json"),
            "present": self.present,
            "working": None if self.working is None else {"label": self.working.label},
            "streams": [
                {"stream_id": i, "in_reply_to": s.start.in_reply_to}
                for i, s in self.streams.items()
            ],
        }


@dataclass
class _Slice:
    epoch: int
    n: int
    seen: float
    held: dict[str, _Held] = field(default_factory=dict)


@dataclass
class _Parts:
    n: int
    seen: float
    entries: dict[int, list[dict[str, Any]]] = field(default_factory=dict)


def _told(
    thread_id: str,
    sender: Identity,
    old: _Held | None,
    new: _Held | None,
    before: set[str],
    after: set[str],
) -> list[Signal]:
    told: list[Signal] = []
    if sender.id in after and sender.id not in before:
        told.append(Present(thread_id=thread_id, sender=sender))
    if new is not None:
        if new.working is not None and (old is None or old.working != new.working):
            told.append(new.working)
        told.extend(s.start for i, s in new.streams.items() if old is None or i not in old.streams)
    if old is not None:
        told.extend(
            StreamEnd(thread_id=thread_id, sender=sender, stream_id=i)
            for i in sorted(old.streams)
            if new is None or i not in new.streams
        )
        if old.working is not None and (new is None or new.working is None):
            told.append(Idle(thread_id=thread_id, sender=sender))
    if sender.id in before and sender.id not in after:
        told.append(Left(thread_id=thread_id, sender=sender))
    return told


def _held(thread_id: str, item: dict[str, Any], was: _Held | None) -> _Held:
    sender = Identity.model_validate(item["sender"])
    working = item.get("working")
    kept = {} if was is None else was.streams
    return _Held(
        sender=sender,
        present=bool(item.get("present")),
        working=None
        if working is None
        else Working(thread_id=thread_id, sender=sender, label=working.get("label")),
        streams={
            s["stream_id"]: kept.get(s["stream_id"])
            or _Stream(StreamStart(thread_id=thread_id, sender=sender, **s))
            for s in item.get("streams", [])
        },
    )


class LatestState:
    def __init__(self, origin: str, clock: Callable[[], float] = time.monotonic) -> None:
        self.origin = origin
        self._clock = clock
        self._slices: dict[_Key, dict[str, _Slice]] = {}
        self._parts: dict[tuple[str, _Key], _Parts] = {}

    def _at(self, key: _Key, epoch: int) -> list[_Held]:
        found = self._slices.get(key, {}).values()
        return [held for s in found if s.epoch == epoch for held in s.held.values()]

    def _present(self, key: _Key, epoch: int) -> set[str]:
        return {held.sender.id for held in self._at(key, epoch) if held.present}

    def _taken(self, key: _Key, epoch: int, origin: str, n: int) -> _Slice | None:
        found = self._slices.get(key, {}).get(origin)
        if found is not None and (epoch, n) <= (found.epoch, found.n):
            return None
        if found is None or epoch > found.epoch:
            found = self._slices.setdefault(key, {})[origin] = _Slice(epoch, n, 0.0)
        found.n, found.seen = n, self._clock()
        return found

    def _settle(self, key: _Key, origin: str, found: _Slice) -> None:
        found.held = {i: held for i, held in found.held.items() if not held.empty()}
        if origin == self.origin and not found.held:
            del self._slices[key][origin]
        if not self._slices[key]:
            del self._slices[key]

    def apply(
        self, scope: str, epoch: int, signal: Signal, origin: str, n: int
    ) -> tuple[list[Signal], bool]:
        kinds = Present | Left | Working | Idle | StreamStart | StreamDelta | StreamEnd
        if not isinstance(signal, kinds):
            return [signal], False
        key, sender = (scope, signal.thread_id), signal.sender
        found = self._taken(key, epoch, origin, n)
        if found is None:
            return [], False
        had = bool(found.held)
        held = found.held.setdefault(sender.id, _Held(sender))
        told = True
        if isinstance(signal, Present):
            told = sender.id not in self._present(key, epoch)
            held.present = True
        elif isinstance(signal, Left):
            held.present, told = False, held.present
        elif isinstance(signal, Working):
            held.working = signal
        elif isinstance(signal, Idle):
            held.working, told = None, held.working is not None
        elif isinstance(signal, StreamStart):
            told = signal.stream_id not in held.streams
            held.streams.setdefault(signal.stream_id, _Stream(signal))
        elif isinstance(signal, StreamDelta):
            stream = held.streams.get(signal.stream_id)
            told = stream is not None
            if stream is not None:
                stream.text, stream.index = stream.text + signal.text, signal.index
        else:
            told = held.streams.pop(signal.stream_id, None) is not None
        self._settle(key, origin, found)
        if told and isinstance(signal, Left) and sender.id in self._present(key, epoch):
            told = False
        return [signal] if told else [], had and not found.held

    def replace(
        self,
        scope: str,
        thread_id: str,
        epoch: int,
        origin: str,
        n: int,
        entries: list[dict[str, Any]],
    ) -> list[Signal]:
        key = (scope, thread_id)
        old = self._slices.get(key, {}).get(origin)
        was = {} if old is None or old.epoch != epoch else old.held
        now = [_held(thread_id, item, was.get(str(item["sender"]["id"]))) for item in entries]
        before = self._present(key, epoch)
        found = self._taken(key, epoch, origin, n)
        if found is None:
            return []
        found.held = {held.sender.id: held for held in now}
        self._settle(key, origin, found)
        after = self._present(key, epoch)
        senders = {held.sender.id: held.sender for held in now}
        senders.update((i, held.sender) for i, held in was.items() if i not in senders)
        return [
            signal
            for i, sender in senders.items()
            for signal in _told(thread_id, sender, was.get(i), found.held.get(i), before, after)
        ]

    def part(
        self,
        scope: str,
        thread_id: str,
        epoch: int,
        origin: str,
        n: int,
        index: int,
        count: int,
        entries: list[dict[str, Any]],
    ) -> list[Signal]:
        key = (origin, (scope, thread_id))
        buffer = self._parts.get(key)
        if buffer is not None and buffer.n > n:
            return []
        if buffer is None or buffer.n < n:
            buffer = self._parts[key] = _Parts(n, self._clock())
        buffer.entries[index] = entries
        if len(buffer.entries) < count:
            return []
        del self._parts[key]
        whole = [item for i in sorted(buffer.entries) for item in buffer.entries[i]]
        return self.replace(scope, thread_id, epoch, origin, n, whole)

    def expired(self, forget: bool = False) -> list[tuple[str, str, int, list[Signal]]]:
        now = self._clock()
        for stale in [k for k, b in self._parts.items() if forget or now - b.seen > EXPIRY]:
            del self._parts[stale]
        gone: list[tuple[str, str, int, list[Signal]]] = []
        for key, slices in list(self._slices.items()):
            for origin, found in list(slices.items()):
                if origin == self.origin or not (forget or now - found.seen > EXPIRY):
                    continue
                before = self._present(key, found.epoch)
                del slices[origin]
                after = self._present(key, found.epoch)
                ends = [
                    signal
                    for held in found.held.values()
                    for signal in _told(key[1], held.sender, held, None, before, after)
                ]
                if ends:
                    gone.append((*key, found.epoch, ends))
            if not slices:
                del self._slices[key]
        return gone

    def snapshot(self, scope: str, thread_id: str, epoch: int) -> list[Signal]:
        present: dict[str, Signal] = {}
        rest: list[Signal] = []
        for held in self._at((scope, thread_id), epoch):
            if held.present:
                present.setdefault(held.sender.id, Present(thread_id=thread_id, sender=held.sender))
            if held.working is not None:
                rest.append(held.working)
            for i, stream in held.streams.items():
                rest.append(stream.start)
                if stream.text:
                    rest.append(
                        StreamDelta(
                            thread_id=thread_id,
                            sender=held.sender,
                            stream_id=i,
                            index=stream.index,
                            text=stream.text,
                        )
                    )
        return [*present.values(), *rest]

    def present(self, scope: str, thread_id: str, epoch: int) -> list[Identity]:
        held = self._at((scope, thread_id), epoch)
        return list({h.sender.id: h.sender for h in held if h.present}.values())

    def own(self) -> list[tuple[str, str, int, list[dict[str, Any]]]]:
        return [
            (scope, thread_id, found.epoch, [held.entry() for held in found.held.values()])
            for (scope, thread_id), slices in self._slices.items()
            if (found := slices.get(self.origin)) is not None
        ]

    def drop(self, scope: str, thread_id: str) -> int | None:
        found = self._slices.pop((scope, thread_id), {}).get(self.origin)
        return None if found is None else found.epoch
