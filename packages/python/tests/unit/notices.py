import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from rfnry_thread.errors import StorageError
from rfnry_thread.models import Event
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.storage import OnEvent, OnResume, OnSignal, Stop


@dataclass
class Notice:
    scope: str
    thread_id: str
    hand: Callable[[], Awaitable[None]]
    seq: int | None = None
    epoch: int | None = None
    payload: dict[str, Any] = field(default_factory=dict)


class Muffled(PostgresStorage):
    def __init__(self, pool: Any, drop: Callable[[Notice], bool] = lambda notice: False) -> None:
        super().__init__(pool)
        self.drop = drop
        self.dropped: list[Notice] = []
        self.refuse: Callable[[str, int, int], bool] = lambda thread_id, since, floor: False
        self.stuck: Callable[[str, int, int], bool] = lambda thread_id, since, floor: False

    async def listen(
        self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop:
        async def event(scope: str, thread_id: str, seq: int, epoch: int) -> None:
            await self._heard(
                Notice(scope, thread_id, lambda: on_event(scope, thread_id, seq, epoch), seq, epoch)
            )

        async def signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
            await self._heard(
                Notice(
                    scope,
                    thread_id,
                    lambda: on_signal(scope, thread_id, payload),
                    payload=dict(payload),
                )
            )

        return await super().listen(event, signal, on_resume)

    async def _heard(self, notice: Notice) -> None:
        if self.drop(notice):
            self.dropped.append(notice)
        else:
            await notice.hand()

    async def tail(
        self, scope: str, thread_id: str, since: int, floor: int, limit: int
    ) -> tuple[int, int, list[Event]] | None:
        if self.stuck(thread_id, since, floor):
            await asyncio.Event().wait()
        if self.refuse(thread_id, since, floor):
            raise StorageError("postgres: storage rejected tail: connection reset")
        return await super().tail(scope, thread_id, since, floor, limit)
