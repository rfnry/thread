import os
import uuid
from typing import Any

import asyncpg

URL = os.environ.get("RFNRY_TEST_POSTGRES_URL", "")
SCHEMA = f"thread_test_{uuid.uuid4().hex[:12]}"
DSN = f"{URL}{'&' if '?' in URL else '?'}search_path={SCHEMA}&application_name={SCHEMA}"
TABLES = "thread_marks, thread_events, thread_members, thread_threads"


async def open_pool() -> Any:
    return await asyncpg.create_pool(DSN, min_size=1, max_size=10, timeout=5)
