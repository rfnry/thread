from rfnry_thread.models import Event, Member, MemberAdded, MemberRemoved, Thread, ThreadUpdated
from rfnry_thread.storage import Storage


async def epoch(storage: Storage, scope: str, thread_id: str) -> int:
    thread = await storage.get_thread(scope, thread_id)
    return 0 if thread is None else thread.epoch


async def append(storage: Storage, scope: str, event: Event) -> tuple[Event, bool]:
    return await storage.append_event(scope, event, await epoch(storage, scope, event.thread_id))


async def admit(
    storage: Storage, scope: str, added: MemberAdded
) -> tuple[Member, MemberAdded | None]:
    return await storage.add_member(scope, added, await epoch(storage, scope, added.thread_id))


async def expel(storage: Storage, scope: str, removed: MemberRemoved) -> MemberRemoved | None:
    return await storage.remove_member(
        scope, removed, await epoch(storage, scope, removed.thread_id)
    )


async def revise(
    storage: Storage, scope: str, updated: ThreadUpdated
) -> tuple[Thread, ThreadUpdated] | None:
    return await storage.update_thread(
        scope, updated, await epoch(storage, scope, updated.thread_id)
    )
