from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from rfnry_thread.errors import NotFoundError, StorageError
from rfnry_thread.models import Event, Member, Order, Thread
from rfnry_thread.resolve import last_message
from rfnry_thread.storage import PREVIEW_DEPTH, UNREAD_CAP, OnEvent, OnResume, OnSignal, Stop

Key = tuple[str, str]


def _contains(have: Any, want: Any) -> bool:
    if isinstance(want, dict):
        return isinstance(have, dict) and all(
            key in have and _contains(have[key], value) for key, value in want.items()
        )
    if isinstance(want, list):
        return isinstance(have, list) and all(any(_contains(h, w) for h in have) for w in want)
    if isinstance(want, bool) or isinstance(have, bool):
        return have is want
    return bool(have == want)


class InMemoryStorage:
    def __init__(self) -> None:
        self._threads: dict[Key, Thread] = {}
        self._members: dict[Key, dict[str, Member]] = {}
        self._events: dict[Key, list[Event]] = {}
        self._keys: dict[tuple[str, str, str, str], Event] = {}
        self._marks: dict[tuple[str, str, str], int] = {}
        self._last_message_at: dict[Key, datetime] = {}
        self._listeners: list[tuple[OnEvent, OnSignal]] = []

    async def initialize(self) -> None:
        return None

    async def create_thread(self, scope: str, thread: Thread) -> tuple[Thread, bool]:
        if thread.scope != scope:
            raise StorageError("inmemory: row scope does not match the call scope")
        key = (scope, thread.id)
        if key in self._threads:
            return self._threads[key], False
        self._threads[key] = thread
        self._members[key] = {}
        self._events[key] = []
        return thread, True

    async def get_thread(self, scope: str, thread_id: str) -> Thread | None:
        return self._threads.get((scope, thread_id))

    async def list_threads(
        self,
        scope: str,
        member_id: str | None,
        before: str | None = None,
        limit: int = 50,
        metadata: dict[str, Any] | None = None,
        order: Order = "created",
        preview: bool = False,
    ) -> list[Thread]:
        def key(thread: Thread) -> tuple[datetime, str]:
            moment = self._last_message_at.get((scope, thread.id)) if order == "activity" else None
            return (moment or thread.created_at, thread.id)

        threads = [
            thread
            for (owner, thread_id), thread in self._threads.items()
            if owner == scope
            and (member_id is None or member_id in self._members[(owner, thread_id)])
            and _contains(thread.metadata, metadata or {})
        ]
        threads.sort(key=key, reverse=True)
        if before is not None:
            anchor = self._threads.get((scope, before))
            if anchor is None:
                return []
            threads = [t for t in threads if key(t) < key(anchor)]
        return [self._listed(scope, t, member_id, preview) for t in threads[: max(limit, 0)]]

    def _listed(self, scope: str, thread: Thread, member_id: str | None, preview: bool) -> Thread:
        log = self._events[(scope, thread.id)]
        marked = None if member_id is None else self._marks.get((scope, thread.id, member_id))
        unread = (
            None
            if member_id is None or marked is None
            else self._unread(scope, thread.id, member_id, marked)
        )
        return thread.model_copy(
            update={
                "last_seq": len(log),
                "last_message_at": self._last_message_at.get((scope, thread.id)),
                "marked": marked,
                "unread": unread,
                "last_message": last_message(self._window(log)) if preview else None,
            }
        )

    def _window(self, log: list[Event]) -> list[Event]:
        seqs = [event.seq for event in log if event.kind == "message"][-PREVIEW_DEPTH:]
        start = seqs[0] if seqs else 0
        return [e for e in log if e.seq >= start and e.kind in ("message", "edit", "retract")]

    async def update_thread(
        self, scope: str, thread_id: str, metadata: dict[str, Any]
    ) -> Thread | None:
        thread = self._threads.get((scope, thread_id))
        if thread is None:
            return None
        updated = thread.model_copy(update={"metadata": dict(metadata)})
        self._threads[(scope, thread_id)] = updated
        return updated

    async def delete_thread(self, scope: str, thread_id: str) -> bool:
        key = (scope, thread_id)
        if self._threads.pop(key, None) is None:
            return False
        self._members.pop(key, None)
        self._events.pop(key, None)
        self._last_message_at.pop(key, None)
        self._marks = {k: v for k, v in self._marks.items() if k[:2] != key}
        self._keys = {k: v for k, v in self._keys.items() if k[:2] != key}
        return True

    async def add_member(self, scope: str, member: Member) -> tuple[Member, bool]:
        members = self._members.get((scope, member.thread_id))
        if members is None:
            raise NotFoundError(f"inmemory: thread {member.thread_id!r} was not found")
        existing = members.get(member.identity.id)
        if existing is not None:
            return existing, False
        members[member.identity.id] = member
        return member, True

    async def remove_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        self._marks.pop((scope, thread_id, identity_id), None)
        return self._members.get((scope, thread_id), {}).pop(identity_id, None) is not None

    async def list_members(self, scope: str, thread_id: str) -> list[Member]:
        return sorted(
            self._members.get((scope, thread_id), {}).values(),
            key=lambda m: (m.added_at, m.identity.id),
        )

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        return identity_id in self._members.get((scope, thread_id), {})

    async def mark(self, scope: str, thread_id: str, identity_id: str, seq: int) -> tuple[int, int]:
        if identity_id not in self._members.get((scope, thread_id), {}):
            raise NotFoundError(
                f"inmemory: {identity_id!r} is not a member of thread {thread_id!r}"
            )
        key = (scope, thread_id, identity_id)
        stored = max(self._marks.get(key, seq), seq)
        self._marks[key] = stored
        return stored, self._unread(scope, thread_id, identity_id, stored)

    def _unread(self, scope: str, thread_id: str, identity_id: str, after: int) -> int:
        count = 0
        for event in self._events.get((scope, thread_id), []):
            if event.seq > after and event.kind == "message" and event.author.id != identity_id:
                count += 1
                if count == UNREAD_CAP:
                    break
        return count

    async def baseline_marks(self, scope: str, identity_id: str) -> int:
        stored = 0
        for (owner, thread_id), members in self._members.items():
            key = (owner, thread_id, identity_id)
            if owner == scope and identity_id in members and key not in self._marks:
                self._marks[key] = len(self._events[(owner, thread_id)])
                stored += 1
        return stored

    async def append_event(self, scope: str, event: Event) -> tuple[Event, bool]:
        log = self._events.get((scope, event.thread_id))
        if log is None:
            raise NotFoundError(f"inmemory: thread {event.thread_id!r} was not found")
        dedup = (scope, event.thread_id, event.author.id, event.client_id)
        existing = self._keys.get(dedup)
        if existing is not None:
            return existing, False
        stored = event.model_copy(update={"seq": len(log) + 1})
        log.append(stored)
        if stored.kind == "message":
            self._last_message_at[(scope, stored.thread_id)] = stored.created_at
        self._keys[dedup] = stored
        for on_event, _ in list(self._listeners):
            await on_event(scope, stored.thread_id, stored.seq)
        return stored, True

    async def list_events(
        self,
        scope: str,
        thread_id: str,
        since: int | None = None,
        before: int | None = None,
        limit: int = 50,
        kinds: Sequence[str] | None = None,
    ) -> list[Event]:
        if limit < 1:
            return []
        chosen = [
            event
            for event in self._events.get((scope, thread_id), [])
            if (since is None or event.seq > since)
            and (before is None or event.seq < before)
            and (kinds is None or event.kind in kinds)
        ]
        return chosen[:limit] if since is not None else chosen[-limit:]

    async def get_event(self, scope: str, thread_id: str, event_id: str) -> Event | None:
        return next((e for e in self._events.get((scope, thread_id), []) if e.id == event_id), None)

    async def find_event(
        self, scope: str, thread_id: str, author_id: str, client_id: str
    ) -> Event | None:
        return self._keys.get((scope, thread_id, author_id, client_id))

    async def delete_scope(self, scope: str) -> int:
        keys = [key for key in self._threads if key[0] == scope]
        removed = 0
        for key in keys:
            marks = sum(1 for mark in self._marks if mark[:2] == key)
            removed += 1 + len(self._members.get(key, {})) + len(self._events.get(key, [])) + marks
            await self.delete_thread(*key)
        return removed

    async def verify_absent(self, scope: str) -> bool:
        return not any(
            key[0] == scope for key in [*self._threads, *self._members, *self._events, *self._marks]
        )

    async def listen(
        self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop:
        entry = (on_event, on_signal)
        self._listeners.append(entry)

        async def stop() -> None:
            if entry in self._listeners:
                self._listeners.remove(entry)

        return stop

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        text = json.dumps(payload)
        for _, on_signal in list(self._listeners):
            await on_signal(scope, thread_id, json.loads(text))
