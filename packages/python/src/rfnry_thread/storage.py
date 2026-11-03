from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol, runtime_checkable

from rfnry_thread.models import (
    Event,
    Member,
    MemberAdded,
    MemberRemoved,
    Message,
    Order,
    Thread,
    ThreadUpdated,
)

OnEvent = Callable[[str, str, int, int], Awaitable[None]]
OnSignal = Callable[[str, str, dict[str, Any]], Awaitable[None]]
OnResume = Callable[[], Awaitable[None]]
Stop = Callable[[], Awaitable[None]]

UNREAD_CAP = 100
PREVIEW_DEPTH = 5


@runtime_checkable
class Storage(Protocol):
    async def check(self) -> None: ...

    async def create_thread(
        self, scope: str, thread: Thread, creator: MemberAdded | None = None
    ) -> tuple[Thread, bool]: ...

    async def get_thread(self, scope: str, thread_id: str) -> Thread | None: ...

    async def list_threads(
        self,
        scope: str,
        member_id: str | None,
        before: str | Thread | None = None,
        limit: int = 50,
        metadata: dict[str, Any] | None = None,
        order: Order = "created",
        preview: bool = False,
        query: str | None = None,
    ) -> list[Thread]: ...

    async def search(
        self,
        scope: str,
        thread_id: str,
        query: str,
        before: int | None = None,
        limit: int = 50,
        floor: int = 0,
    ) -> list[Message]: ...

    async def update_thread(
        self, scope: str, updated: ThreadUpdated
    ) -> tuple[Thread, ThreadUpdated] | None: ...

    async def delete_thread(self, scope: str, thread_id: str) -> bool: ...

    async def add_member(
        self, scope: str, added: MemberAdded
    ) -> tuple[Member, MemberAdded | None]: ...

    async def remove_member(self, scope: str, removed: MemberRemoved) -> MemberRemoved | None: ...

    async def list_members(self, scope: str, thread_id: str) -> list[Member]: ...

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool: ...

    async def head(
        self, scope: str, thread_id: str, identity_id: str
    ) -> tuple[int, int, int] | None: ...

    async def mark(
        self, scope: str, thread_id: str, identity_id: str, seq: int, back: bool = False
    ) -> tuple[int, int, bool]: ...

    async def get_mark(self, scope: str, thread_id: str, identity_id: str) -> int | None: ...

    async def baseline_marks(self, scope: str, identity_id: str) -> int: ...

    async def append_event(self, scope: str, event: Event) -> tuple[Event, bool]: ...

    async def list_events(
        self,
        scope: str,
        thread_id: str,
        since: int | None = None,
        before: int | None = None,
        limit: int = 50,
        kinds: Sequence[str] | None = None,
        floor: int = 0,
    ) -> list[Event]: ...

    async def get_event(
        self, scope: str, thread_id: str, event_id: str, floor: int = 0
    ) -> Event | None: ...

    async def find_event(
        self, scope: str, thread_id: str, author_id: str, client_id: str
    ) -> Event | None: ...

    async def delete_scope(self, scope: str) -> int: ...

    async def verify_absent(self, scope: str) -> bool: ...

    async def listen(
        self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop: ...

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None: ...
