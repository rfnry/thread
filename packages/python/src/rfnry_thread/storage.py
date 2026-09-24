from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol, runtime_checkable

from rfnry_thread.models import Event, Member, Thread

OnEvent = Callable[[str, str, int], Awaitable[None]]
OnSignal = Callable[[str, str, dict[str, Any]], Awaitable[None]]
OnResume = Callable[[], Awaitable[None]]
Stop = Callable[[], Awaitable[None]]


@runtime_checkable
class Storage(Protocol):
    async def initialize(self) -> None: ...

    async def create_thread(self, scope: str, thread: Thread) -> tuple[Thread, bool]: ...

    async def get_thread(self, scope: str, thread_id: str) -> Thread | None: ...

    async def list_threads(
        self, scope: str, member_id: str, before: str | None = None, limit: int = 50
    ) -> list[Thread]: ...

    async def update_thread(
        self, scope: str, thread_id: str, metadata: dict[str, Any]
    ) -> Thread | None: ...

    async def delete_thread(self, scope: str, thread_id: str) -> bool: ...

    async def add_member(self, scope: str, member: Member) -> tuple[Member, bool]: ...

    async def remove_member(self, scope: str, thread_id: str, identity_id: str) -> bool: ...

    async def list_members(self, scope: str, thread_id: str) -> list[Member]: ...

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool: ...

    async def append_event(self, scope: str, event: Event) -> tuple[Event, bool]: ...

    async def list_events(
        self,
        scope: str,
        thread_id: str,
        since: int | None = None,
        before: int | None = None,
        limit: int = 50,
        kinds: Sequence[str] | None = None,
    ) -> list[Event]: ...

    async def get_event(self, scope: str, thread_id: str, event_id: str) -> Event | None: ...

    async def delete_scope(self, scope: str) -> int: ...

    async def verify_absent(self, scope: str) -> bool: ...

    async def listen(
        self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop: ...

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None: ...
