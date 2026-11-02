from typing import Any

from rfnry_thread.models import Identity, Member
from rfnry_thread.postgres import PostgresStorage

from .factories import AT


async def enroll(storage: PostgresStorage, thread_id: str, identity: Identity) -> None:
    pool: Any = storage._pool
    member = Member(thread_id=thread_id, identity=identity, added_at=AT, added_by=identity)
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO thread_members (scope, thread_id, identity_id, added_at, body, added_seq) "
            "SELECT 'acme', $1, $2, $3, $4::jsonb, t.last_seq + 1 FROM thread_threads t "
            "WHERE t.scope = 'acme' AND t.id = $1",
            thread_id,
            identity.id,
            AT,
            member.model_dump_json(),
        )
