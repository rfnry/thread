# Follow, marks and the inbox: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give rfnry-thread the following, along with consumer adoption:
- `follow`: receive every thread you belong to, and new ones as you are added;
- resume after a restart from a private read mark;
- an inbox listing: activity order, `marked`, `unread`, an opt-in preview;
- forward history paging;
- adoption by the `customer_assistant` example and by AIHelpdesk.

**Architecture:**
- **Storage:** gains a `thread_marks` table (a private position per member, moving forward only)
  and `last_message_at` on threads. Its listing fills the new optional `Thread` fields.
- **The hub:** splits a connection's delivered threads from its present ones. `follow` puts a
  connection in every room it belongs to, reusing the join path. A new membership reaches the
  member's following connections on every replica as a `joined` push, from the add.
- **Marks:** a mark reaches the marker's own connections as a `Marked` signal.
- **Clients:** both re-follow once after a dropped socket, sending their cursor map.

**Tech Stack:**
- **Python:** 3.12, pydantic 2, asyncpg; pytest with pytest-asyncio, ruff, mypy strict, uv,
  poethepoet.
- **TypeScript:** 7 strict, vitest, Biome.

**Spec:** `docs/plans/2026-10-03-follow-and-marks-design.md`. The contract is `docs/thread.md`; it
wins over this plan where they differ, and any difference is reported to the human.

## Global Constraints

- **Python gate** for every commit that touches Python, from `packages/python`:
  `uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`.
- **Postgres tier** for every task that touches storage:
  `RFNRY_TEST_POSTGRES_URL=postgresql://thread:thread@127.0.0.1:55436/thread uv run poe test:integration`.
  The database is the `rfnry-thread-pg` container; the tests truncate it freely.
- **TypeScript gate** for every commit that touches TypeScript, from `packages/typescript`:
  `npm run check && npm run typecheck && npm test && npm run build`.
- **Golden fixtures:**
  - written by `uv run poe fixtures` (from `packages/python`) and shared by both suites;
  - any task that changes a frame regenerates them and updates the TypeScript golden test in the
    same commit, so both gates stay green.
- **Code style (CLAUDE.md engineering rules):**
  - no comments and no docstrings in library code;
  - every error message starts with its module's file stem (`server: …`, `postgres: …`,
    `inmemory: …`, `client: …`);
  - `scope` is the first argument of every storage method that reads or writes a row.
- **Docs travel with the surface.** `docs/api.md` and `docs/thread.md` change in the same commit as
  any public name or signature. `tests/unit/test_docs.py` and `tests/unit/test_api.py` enforce it.
- **Commits:**
  - messages in the repo's style (`Storage: …`, `Server: …`, `Clients: …`, `Docs: …`,
    `Examples: …`), with **no attribution or session line of any kind**;
  - the check `git grep -n -e sk-ant -e sk-proj -e AQ.Ab8 -e pa-3W -e apikey_` finds nothing
    before each commit;
  - push to `origin main` when the task's gates are green.
- **Constants (from the spec):**
  - `UNREAD_CAP = 100`;
  - `PREVIEW_DEPTH = 5`;
  - a resume gap per thread is capped by the server's existing `replay_limit` (500), the same rule
    a join's replay uses: past the cap, the latest `replay_limit` events with `truncated=True`.
- **The resume gap's older part.** It is paged with `history(thread_id, after=mark, before=<first
  seq delivered>)`. This makes precise the spec's "paged with `history(after=)`" and matches how
  a join already truncates.
- **Two refinements of the spec, made deliberately:**
  - **`Storage.mark` returns `(seq, unread)`, not just the seq.** One round trip serves the reply
    and the `Marked` signal. The client verb `Client.mark` still returns the stored seq, as the
    spec says.
  - **No baseline write on a resuming follow that carries a cursor map** (`since`), only on a
    first resume. A reconnecting UI then never writes marks it didn't ask for.
- **AIHelpdesk** (Task 9) is the owner's friend's repository:
  - local branch `rfnry-backend` only; never push, never add a remote, never set an upstream;
  - `backend/`, `frontend/` and the root `README.md` stay identical to `dev`;
  - documents there say "the original", never "he" or "his";
  - never open or print `/home/frndvrgs/software/rfnry/.env.examples`; pass it with `--env-file`
    only;
  - never `pkill` a pattern; stop only processes you started, by PID.

## Review Focus

1. **Following twice on one connection** must not replay or deliver anything twice. The test is in
   Task 3.
2. **A follower removed and then added again** receives the thread from the new add, not from its
   old position: a removal deletes the mark. The test is in Task 3.
3. **A `since` map naming a thread the caller doesn't belong to** is ignored. A negative value is an
   `InputError`. The tests are in Task 3 (negative) and Task 4 (a foreign thread id).
4. **A mark beyond the thread's last `seq`, or below 0,** is an `InputError`. A mark from a
   non-member is a `ForbiddenError`. The test is in Task 4.
5. **A followed thread that is deleted** ends with exactly one `ThreadDeleted` and no further
   delivery. The test is in Task 3.

---

### Task 1: Marks in storage

**Files:**
- Modify: `packages/python/src/rfnry_thread/storage.py`
- Modify: `packages/python/src/rfnry_thread/inmemory.py`
- Modify: `packages/python/src/rfnry_thread/postgres.py`
- Modify: `packages/python/tests/integration/conftest.py:10` (`TABLES`)
- Test: `packages/python/tests/integration/test_storage.py`
- Docs: `docs/api.md`, the `### \`Storage\`` block

**Interfaces:**
- Produces:
  - `UNREAD_CAP: int = 100` in `rfnry_thread.storage`;
  - `Storage.mark(scope, thread_id, identity_id, seq) -> tuple[int, int]`, returning the stored seq
    and the count of `message` events by others after it, at most `UNREAD_CAP`;
  - `Storage.baseline_marks(scope, identity_id) -> int`, returning how many marks it stored.

- [ ] **Step 1: Write the failing tests** at the end of `tests/integration/test_storage.py`.
  `NotFoundError`, `Member`, `SYSTEM`, `AT`, `ANA`, `BRUNO`, `room` and `say` are already
  imported or defined there.

```python
async def test_a_mark_moves_forward_only_and_counts_what_others_said_after_it(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    for n in range(3):
        await storage.append_event("acme", say("t1", f"b{n}", author=BRUNO))
    await storage.append_event("acme", say("t1", "a0", author=ANA))
    assert await storage.mark("acme", "t1", "ana", 1) == (1, 2)
    assert await storage.mark("acme", "t1", "ana", 0) == (1, 2)
    assert await storage.mark("acme", "t1", "ana", 4) == (4, 0)


async def test_a_mark_needs_a_membership_and_leaves_with_it(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA)
    with pytest.raises(NotFoundError, match="'bruno' is not a member of thread 't1'$"):
        await storage.mark("acme", "t1", "bruno", 0)
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=BRUNO, added_at=AT, added_by=SYSTEM)
    )
    await storage.append_event("acme", say("t1", "b1", author=BRUNO))
    assert await storage.mark("acme", "t1", "ana", 1) == (1, 0)
    await storage.remove_member("acme", "t1", "ana")
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=ANA, added_at=AT, added_by=SYSTEM)
    )
    assert await storage.mark("acme", "t1", "ana", 0) == (0, 1)


async def test_baseline_marks_set_the_last_seq_only_where_there_is_no_mark(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA)
    await room(storage, "t2", "acme", ANA)
    await room(storage, "t3", "acme", BRUNO)
    for client_id in ("x", "y"):
        await storage.append_event("acme", say("t1", client_id))
    await storage.append_event("acme", say("t2", "z"))
    await storage.mark("acme", "t2", "ana", 0)
    assert await storage.baseline_marks("acme", "ana") == 1
    assert await storage.baseline_marks("acme", "ana") == 0
    assert (await storage.mark("acme", "t1", "ana", 0))[0] == 2
    assert (await storage.mark("acme", "t2", "ana", 0))[0] == 0


async def test_a_deleted_scope_takes_its_marks_along(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA)
    await storage.mark("acme", "t1", "ana", 0)
    assert await storage.delete_scope("acme") == 3
    assert await storage.verify_absent("acme")
```

- [ ] **Step 2: Run them to see them fail.**

Run, from `packages/python`: `uv run pytest -q tests/integration/test_storage.py -k "mark"`.
Expected: FAIL with `AttributeError: 'InMemoryStorage' object has no attribute 'mark'`.

- [ ] **Step 3: Add the protocol methods** to `storage.py`. Add the constant after the type
  aliases:

```python
UNREAD_CAP = 100
```

Add these to `class Storage(Protocol)`, after `is_member`:

```python
    async def mark(
        self, scope: str, thread_id: str, identity_id: str, seq: int
    ) -> tuple[int, int]: ...

    async def baseline_marks(self, scope: str, identity_id: str) -> int: ...
```

- [ ] **Step 4: Implement them in `inmemory.py`.**
  - **Import:** `from rfnry_thread.storage import UNREAD_CAP, OnEvent, OnResume, OnSignal, Stop`.
  - **`__init__`:** add `self._marks: dict[tuple[str, str, str], int] = {}`.
  - **New methods:**

```python
    async def mark(
        self, scope: str, thread_id: str, identity_id: str, seq: int
    ) -> tuple[int, int]:
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
```

  - **`remove_member`:** drop the mark with the membership:

```python
    async def remove_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        self._marks.pop((scope, thread_id, identity_id), None)
        return self._members.get((scope, thread_id), {}).pop(identity_id, None) is not None
```

  - **`delete_thread`,** after `self._events.pop(key, None)`:

```python
        self._marks = {k: v for k, v in self._marks.items() if k[:2] != key}
```

  - **`delete_scope`:** count marks into `removed` before deleting:

```python
        for key in keys:
            marks = sum(1 for mark in self._marks if mark[:2] == key)
            removed += 1 + len(self._members.get(key, {})) + len(self._events.get(key, [])) + marks
            await self.delete_thread(*key)
```

  - **`verify_absent`:** add `*self._marks` to the keys it scans:

```python
        return not any(
            key[0] == scope for key in [*self._threads, *self._members, *self._events, *self._marks]
        )
```

- [ ] **Step 5: Implement them in `postgres.py`.**
  - **Imports:** `UNREAD_CAP` from `rfnry_thread.storage`, and `NotFoundError` if it isn't there.
  - **`_DDL`:** append, last:

```python
    """
    CREATE TABLE IF NOT EXISTS thread_marks (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, identity_id TEXT NOT NULL,
        seq BIGINT NOT NULL, at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (scope, thread_id, identity_id),
        FOREIGN KEY (scope, thread_id, identity_id)
            REFERENCES thread_members (scope, thread_id, identity_id) ON DELETE CASCADE)
    """,
```

  - **New methods:**

```python
    async def mark(
        self, scope: str, thread_id: str, identity_id: str, seq: int
    ) -> tuple[int, int]:
        async with self._guard("mark"), self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "WITH stored AS (INSERT INTO thread_marks (scope, thread_id, identity_id, seq, at) "
                "SELECT $1, $2, $3, $4, now() WHERE EXISTS (SELECT 1 FROM thread_members "
                "WHERE scope = $1 AND thread_id = $2 AND identity_id = $3) "
                "ON CONFLICT (scope, thread_id, identity_id) DO UPDATE "
                "SET seq = GREATEST(thread_marks.seq, EXCLUDED.seq), at = EXCLUDED.at RETURNING seq) "
                "SELECT s.seq, (SELECT count(*) FROM (SELECT 1 FROM thread_events e "
                "WHERE e.scope = $1 AND e.thread_id = $2 AND e.seq > s.seq AND e.kind = 'message' "
                "AND e.author_id <> $3 LIMIT $5) u) AS unread FROM stored s",
                scope,
                thread_id,
                identity_id,
                seq,
                UNREAD_CAP,
            )
        if row is None:
            raise NotFoundError(
                f"postgres: {identity_id!r} is not a member of thread {thread_id!r}"
            )
        return int(row["seq"]), int(row["unread"])

    async def baseline_marks(self, scope: str, identity_id: str) -> int:
        async with self._guard("baseline_marks"), self._pool.acquire() as conn:
            status = await conn.execute(
                "INSERT INTO thread_marks (scope, thread_id, identity_id, seq, at) "
                "SELECT m.scope, m.thread_id, m.identity_id, t.last_seq, now() "
                "FROM thread_members m JOIN thread_threads t ON t.scope = m.scope AND t.id = m.thread_id "
                "WHERE m.scope = $1 AND m.identity_id = $2 "
                "ON CONFLICT (scope, thread_id, identity_id) DO NOTHING",
                scope,
                identity_id,
            )
        return _count(status)
```

  - **`delete_scope`:** the table tuple becomes
    `("thread_marks", "thread_events", "thread_members", "thread_threads")`.
  - **`verify_absent`:** its tuple gains `"thread_marks"`.
  - **`tests/integration/conftest.py`:** `TABLES = "thread_marks, thread_events, thread_members, thread_threads"`.

- [ ] **Step 6: Run the tests and see them pass.**

Run: `uv run pytest -q tests/integration/test_storage.py -k "mark or scope"`. Expected: PASS
(in-memory).

Then run the Postgres tier (Global Constraints). Expected: PASS.

- [ ] **Step 7: Update `docs/api.md`.** In the `### \`Storage\`` code block, add the two
  signatures after `is_member`, exactly as in Step 3. Add these sentences to the end of the
  paragraph under the block:

```markdown
`mark` stores the identity's private read position in the thread, never lowering it, and returns
it with the count of `message` events by others after it, at most 100; `baseline_marks` stores each
thread's last `seq` as the mark of every membership of `identity_id` that has none, and returns how
many it stored. `mark` raises `NotFoundError` when the identity is not a member of the thread.
```

- [ ] **Step 8: Run the full Python gate and the Postgres tier.** Both must pass.

- [ ] **Step 9: Commit and push.**

```bash
git add packages/python docs/api.md
git commit -m "Storage: a private mark per member, moving forward only, with the unread count after it"
git push origin main
```

---

### Task 2: The listing — activity order, marks and unread, the preview

**Files:**
- Modify: `packages/python/src/rfnry_thread/models.py`: the `Thread` fields and serializer, and `Order`
- Modify: `packages/python/src/rfnry_thread/resolve.py`: `last_message`
- Modify: `packages/python/src/rfnry_thread/storage.py`: the `list_threads` signature and `PREVIEW_DEPTH`
- Modify: `packages/python/src/rfnry_thread/inmemory.py`
- Modify: `packages/python/src/rfnry_thread/postgres.py`
- Modify: `packages/python/tests/unit/test_verbs.py:271-279`, where the `counted` wrapper must accept the new parameters
- Test: `packages/python/tests/integration/test_storage.py`
- Docs: `docs/api.md` (`Thread`, `Storage`)

**Interfaces:**
- Consumes: `Storage.mark`, `UNREAD_CAP` (Task 1).
- Produces:
  - `Order = Literal["created", "activity"]` in `rfnry_thread.models`, not exported;
  - `Thread.last_seq: int | None`;
  - `Thread.last_message_at: datetime | None`;
  - `Thread.marked: int | None`;
  - `Thread.unread: int | None`;
  - `Thread.last_message: Message | None`, all left out of a dump when `None`;
  - `Storage.list_threads(scope, member_id, before=None, limit=50, metadata=None, order="created", preview=False)`;
  - `last_message(events) -> Message | None` in `rfnry_thread.resolve`;
  - `PREVIEW_DEPTH = 5` in `rfnry_thread.storage`.

- [ ] **Step 1: Write the failing tests** in `tests/integration/test_storage.py`. Add `Edit` and
  `Retract` to the `rfnry_thread.models` import.

```python
async def test_a_member_listing_carries_its_mark_and_what_others_said_after_it(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await storage.append_event("acme", say("t1", "b1", author=BRUNO))
    await storage.append_event("acme", say("t1", "a1", author=ANA))
    await storage.append_event("acme", say("t1", "b2", author=BRUNO))
    [before] = await storage.list_threads("acme", "ana")
    assert (before.last_seq, before.marked, before.unread) == (3, None, None)
    await storage.mark("acme", "t1", "ana", 1)
    [after] = await storage.list_threads("acme", "ana")
    assert (after.marked, after.unread) == (1, 1)
    [anyone] = await storage.list_threads("acme", None)
    assert (anyone.last_seq, anyone.marked, anyone.unread) == (3, None, None)


async def test_unread_is_counted_up_to_100(storage: Storage) -> None:
    await room(storage, "t1", "acme", ANA, BRUNO)
    await storage.mark("acme", "t1", "ana", 0)
    for n in range(105):
        await storage.append_event("acme", say("t1", f"b{n}", author=BRUNO))
    [listed] = await storage.list_threads("acme", "ana")
    assert listed.unread == 100


async def test_activity_order_follows_the_latest_message_and_pages(storage: Storage) -> None:
    for minutes, thread_id in enumerate(["a", "b", "c"]):
        await storage.create_thread("acme", thread(thread_id, minutes=minutes))
        await storage.add_member(
            "acme", Member(thread_id=thread_id, identity=ANA, added_at=AT, added_by=SYSTEM)
        )
    late = say("a", "late").model_copy(update={"created_at": AT + timedelta(hours=1)})
    await storage.append_event("acme", late)
    read = Read(
        id="r1", thread_id="b", author=ANA, created_at=AT + timedelta(hours=2),
        client_id="r1", up_to_seq=1,
    )
    await storage.append_event("acme", read)

    async def listed(**query: Any) -> list[str]:
        threads = await storage.list_threads("acme", "ana", order="activity", **query)
        return [t.id for t in threads]

    assert await listed() == ["a", "c", "b"]
    assert await listed(before="a") == ["c", "b"]
    assert await listed(before="c", limit=1) == ["b"]
    assert [t.id for t in await storage.list_threads("acme", "ana")] == ["c", "b", "a"]
    [first] = await storage.list_threads("acme", "ana", order="activity", limit=1)
    assert first.last_message_at == AT + timedelta(hours=1)


async def test_a_preview_shows_the_latest_message_with_its_edit_and_skips_a_retracted_one(
    storage: Storage,
) -> None:
    await room(storage, "t1", "acme", ANA)
    first, _ = await storage.append_event("acme", say("t1", "c1", "first"))
    second, _ = await storage.append_event("acme", say("t1", "c2", "second"))
    edit = Edit(
        id="ed", thread_id="t1", author=ANA, created_at=AT, client_id="c3",
        target=first.id, content=[Text(text="first, edited")],
    )
    await storage.append_event("acme", edit)
    [plain] = await storage.list_threads("acme", "ana")
    assert plain.last_message is None
    assert "last_message" not in plain.model_dump(mode="json")
    [shown] = await storage.list_threads("acme", "ana", preview=True)
    assert shown.last_message is not None
    assert shown.last_message.content == [Text(text="second")]
    retract = Retract(
        id="rt", thread_id="t1", author=ANA, created_at=AT, client_id="c4", target=second.id
    )
    await storage.append_event("acme", retract)
    [after] = await storage.list_threads("acme", "ana", preview=True)
    assert after.last_message is not None
    assert (after.last_message.id, after.last_message.content) == (
        first.id,
        [Text(text="first, edited")],
    )
```

Add the Postgres-only backfill test next to
`test_initialize_adds_the_metadata_index_to_an_existing_database`, following its structure:

```python
@pytest.mark.postgres
async def test_initialize_backfills_the_latest_message_time_on_an_existing_database() -> None:
    pool = await postgres_pool()
    try:
        storage = await fresh(pool)
        await room(storage, "t1")
        await storage.append_event("acme", say("t1", "c1"))
        async with pool.acquire() as conn:
            await conn.execute("DROP INDEX IF EXISTS ix_thread_threads_activity")
            await conn.execute("ALTER TABLE thread_threads DROP COLUMN last_message_at")
        await storage.initialize()
        [listed] = await storage.list_threads("acme", None)
        assert listed.last_message_at == AT
    finally:
        await pool.close()
```

- [ ] **Step 2: Run them to see them fail.**

Run: `uv run pytest -q tests/integration/test_storage.py -k "listing or unread or activity or preview"`.
Expected: FAIL (`Thread` has no `last_seq`; `list_threads` has no `order`).

- [ ] **Step 3: Change the models** in `models.py`.
  - **Import:** add `SerializerFunctionWrapHandler` and `model_serializer` to the pydantic import.
  - **Above `class Thread`:**

```python
LISTED = frozenset({"last_seq", "last_message_at", "marked", "unread", "last_message"})
Order = Literal["created", "activity"]
```

  - **`class Thread` becomes:**

```python
class Thread(BaseModel):
    id: str
    scope: str
    metadata: dict[str, Any] = {}
    created_at: datetime
    last_seq: int | None = None
    last_message_at: datetime | None = None
    marked: int | None = None
    unread: int | None = None
    last_message: Message | None = None

    @model_serializer(mode="wrap")
    def _listed(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        return {key: value for key, value in data.items() if key not in LISTED or value is not None}
```

  - **After the `Event`/`EVENTS` definitions** (once `Message` exists), add `Thread.model_rebuild()`.

- [ ] **Step 4: Add `last_message`** to `resolve.py`:

```python
def last_message(events: Sequence[Event]) -> Message | None:
    return next((event for event in reversed(resolve(events)) if isinstance(event, Message)), None)
```

- [ ] **Step 5: Change the storage protocol** in `storage.py`.
  - **Import:** `Order` from `rfnry_thread.models`.
  - **Constant:** add `PREVIEW_DEPTH = 5` beside `UNREAD_CAP`.
  - **`list_threads` becomes:**

```python
    async def list_threads(
        self,
        scope: str,
        member_id: str | None,
        before: str | None = None,
        limit: int = 50,
        metadata: dict[str, Any] | None = None,
        order: Order = "created",
        preview: bool = False,
    ) -> list[Thread]: ...
```

- [ ] **Step 6: Implement it in `inmemory.py`.**
  - **Imports:** `from datetime import datetime`, `Order` from models, `last_message` from resolve,
    and `PREVIEW_DEPTH`.
  - **`__init__`:** add `self._last_message_at: dict[Key, datetime] = {}`.
  - **`append_event`,** after `log.append(stored)`:

```python
        if stored.kind == "message":
            self._last_message_at[(scope, stored.thread_id)] = stored.created_at
```

  - **`delete_thread`:** add `self._last_message_at.pop(key, None)`.
  - **`list_threads` is replaced by:**

```python
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
```

- [ ] **Step 7: Implement it in `postgres.py`.**
  - **Imports:** `Order` from models; `last_message` from resolve; `PREVIEW_DEPTH` and
    `UNREAD_CAP` from storage.
  - **`_DDL`:** the `thread_threads` create gains the column for fresh databases:

```python
    """
    CREATE TABLE IF NOT EXISTS thread_threads (
        scope TEXT NOT NULL, id TEXT NOT NULL, metadata JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL, last_seq BIGINT NOT NULL DEFAULT 0,
        last_message_at TIMESTAMPTZ,
        PRIMARY KEY (scope, id))
    """,
    "ALTER TABLE thread_threads ADD COLUMN IF NOT EXISTS last_message_at TIMESTAMPTZ",
```

    The two existing `thread_threads` indexes follow, and then:

```python
    "CREATE INDEX IF NOT EXISTS ix_thread_threads_activity ON thread_threads "
    "(scope, (COALESCE(last_message_at, created_at)) DESC, id DESC)",
```

  - **Below `_DDL`:**

```python
_BACKFILL = (
    "UPDATE thread_threads t SET last_message_at = m.at FROM (SELECT scope, thread_id, "
    "max((body->>'created_at')::timestamptz) AS at FROM thread_events WHERE kind = 'message' "
    "GROUP BY scope, thread_id) m WHERE t.scope = m.scope AND t.id = m.thread_id"
)
```

  - **`initialize`** backfills once, when it adds the column to an existing table:

```python
    async def initialize(self) -> None:
        async with self._guard("initialize"), self._pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", _LOCK)
            existed = await conn.fetchval("SELECT to_regclass('thread_threads') IS NOT NULL")
            had = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE "
                "table_schema = current_schema() AND table_name = 'thread_threads' "
                "AND column_name = 'last_message_at')"
            )
            for ddl in _DDL:
                await conn.execute(ddl)
            if existed and not had:
                await conn.execute(_BACKFILL)
```

  - **`append_event`:** replace the `UPDATE thread_threads SET last_seq` statement with:

```python
            if stored.kind == "message":
                await conn.execute(
                    "UPDATE thread_threads SET last_seq = $3, last_message_at = $4 "
                    "WHERE scope = $1 AND id = $2",
                    scope,
                    stored.thread_id,
                    stored.seq,
                    stored.created_at,
                )
            else:
                await conn.execute(
                    "UPDATE thread_threads SET last_seq = $3 WHERE scope = $1 AND id = $2",
                    scope,
                    stored.thread_id,
                    stored.seq,
                )
```

  - **`list_threads` is replaced by:**

```python
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
        args: list[Any] = [scope]
        join = ""
        marks = "NULL::bigint AS marked, NULL::bigint AS unread"
        if member_id is not None:
            args.append(member_id)
            who = f"${len(args)}"
            args.append(UNREAD_CAP)
            join = (
                "JOIN thread_members m ON m.scope = t.scope AND m.thread_id = t.id "
                f"AND m.identity_id = {who} LEFT JOIN thread_marks k ON k.scope = m.scope "
                "AND k.thread_id = m.thread_id AND k.identity_id = m.identity_id "
            )
            marks = (
                "k.seq AS marked, CASE WHEN k.seq IS NULL THEN NULL ELSE (SELECT count(*) FROM "
                "(SELECT 1 FROM thread_events e WHERE e.scope = t.scope AND e.thread_id = t.id "
                f"AND e.seq > k.seq AND e.kind = 'message' AND e.author_id <> {who} "
                f"LIMIT ${len(args)}) u) END AS unread"
            )
        where = ["t.scope = $1"]
        if metadata:
            args.append(json.dumps(metadata))
            where.append(f"t.metadata @> ${len(args)}::jsonb")
        key = "t.created_at" if order == "created" else "COALESCE(t.last_message_at, t.created_at)"
        anchor = (
            "a.created_at" if order == "created" else "COALESCE(a.last_message_at, a.created_at)"
        )
        if before is not None:
            args.append(before)
            where.append(
                f"({key}, t.id) < (SELECT {anchor}, a.id FROM thread_threads a "
                f"WHERE a.scope = $1 AND a.id = ${len(args)})"
            )
        args.append(max(limit, 0))
        async with self._guard("list_threads"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT t.scope, t.id, t.metadata, t.created_at, t.last_seq, t.last_message_at, "
                f"{marks} FROM thread_threads t {join}WHERE {' AND '.join(where)} "
                f"ORDER BY {key} DESC, t.id DESC LIMIT ${len(args)}",
                *args,
            )
            threads = [_listed(row) for row in rows]
            if preview and threads:
                threads = await self._previewed(conn, scope, threads)
        return threads

    async def _previewed(self, conn: Any, scope: str, threads: list[Thread]) -> list[Thread]:
        rows = await conn.fetch(
            "SELECT p.id AS thread_id, e.body FROM unnest($2::text[]) AS p(id) CROSS JOIN LATERAL "
            "(SELECT x.body FROM thread_events x WHERE x.scope = $1 AND x.thread_id = p.id "
            "AND x.kind IN ('message', 'edit', 'retract') AND x.seq >= COALESCE((SELECT min(y.seq) "
            "FROM (SELECT seq FROM thread_events WHERE scope = $1 AND thread_id = p.id "
            "AND kind = 'message' ORDER BY seq DESC LIMIT $3) y), 0)) e",
            scope,
            [thread.id for thread in threads],
            PREVIEW_DEPTH,
        )
        found: dict[str, list[Event]] = {}
        for row in rows:
            found.setdefault(row["thread_id"], []).append(_event(row["body"]))
        return [
            t.model_copy(update={"last_message": last_message(found.get(t.id, []))})
            for t in threads
        ]
```

  - **Beside `_thread`:**

```python
def _listed(row: Any) -> Thread:
    return _thread(row).model_copy(
        update={
            "last_seq": int(row["last_seq"]),
            "last_message_at": row["last_message_at"],
            "marked": None if row["marked"] is None else int(row["marked"]),
            "unread": None if row["unread"] is None else int(row["unread"]),
        }
    )
```

- [ ] **Step 8: Keep the existing wrapper in `tests/unit/test_verbs.py` working.** Its `counted`
  function lists the parameters. Give it the same two new ones and pass them on:

```python
    async def counted(
        scope: str,
        member_id: str | None,
        before: str | None = None,
        limit: int = 50,
        metadata: dict[str, Any] | None = None,
        order: Any = "created",
        preview: bool = False,
    ) -> list[Thread]:
        reads.append(before)
        return await original(scope, member_id, before, limit, metadata, order, preview)
```

- [ ] **Step 9: Run the tests.**

Run: `uv run pytest -q tests/integration/test_storage.py`. Expected: PASS.

Then the Postgres tier. Expected: PASS, including the backfill test.

- [ ] **Step 10: Update `docs/api.md`.**
  - **Both `### \`Thread\`` entries:**
    - Python: the five optional fields in the code block, exactly as in Step 3, without the
      serializer;
    - TypeScript: `last_seq?: number`, `last_message_at?: string`, `marked?: number`,
      `unread?: number`, `last_message?: Message`.
  - **The prose under both:**

```markdown
A conversation: `id` yours or the server's, `scope` its tenant, `metadata` yours. A listed thread
also carries `last_seq` and `last_message_at` (when its latest message was written); listed for a
member, `marked` (the member's private read position, `None` without one) and `unread` (messages by
others after it, at most 100); with `preview`, `last_message` (the latest message not retracted,
with its edits). Fields without a value are left out on the wire.
```

  - **The `Storage` code block:** the new `list_threads` signature from Step 5.
  - **The prose under it:** add this sentence:

```markdown
`order="activity"` lists by the latest message instead, a thread without messages by its creation;
`preview=True` fills each thread's `last_message`.
```

- [ ] **Step 11: Run the full Python gate.** It must pass, including `test_docs.py` and
  `test_api.py`.

- [ ] **Step 12: Commit and push.**

```bash
git add packages/python docs/api.md
git commit -m "Storage: the listing orders by activity and carries marks, unread and an opt-in preview"
git push origin main
```

---

### Task 3: Follow — the server delivers every thread a connection belongs to

**Files:**
- Modify: `packages/python/src/rfnry_thread/protocol.py`: `METHODS` and `Push.name`
- Modify: `packages/python/src/rfnry_thread/hub.py`
- Modify: `packages/python/src/rfnry_thread/verbs.py`
- Modify: `packages/python/src/rfnry_thread/server.py`
- Modify: `packages/python/tests/unit/peer.py`, which keeps `joined` pushes apart
- Modify: `packages/python/tests/unit/test_hub.py:213`, where `hub.invite` becomes `hub.added`
- Modify: `packages/python/tests/unit/test_golden.py`: three fixtures, and the round-trip for `joined`
- Modify: `packages/typescript/src/protocol.ts` (`Push.name`) and `packages/typescript/tests/golden.test.ts` (the count)
- Test: `packages/python/tests/unit/test_follow.py` (new)

**Interfaces:**
- Consumes:
  - `Storage.list_threads(..., order, preview)` and `Thread.marked` (Task 2);
  - `Storage.mark` (Task 1).
- Produces:
  - **The request:** `"thread.follow"`, payload `{resume: bool = False, since: dict[str, int] | None = None}` → `{"joined": [Joined…]}`;
  - **The push:** a server push `name="joined"` whose payload is a `Joined`;
  - **Session fields:** `Session.present: set[str]` and `Session.following: bool`;
  - **Hub methods:** `Hub.follow(session, threads: list[tuple[Thread, int | None]]) -> list[Joined]` and `Hub.added(scope, thread, sender, identity, seq)`;
  - **Verb:** `Verbs.follow(session, resume, since) -> list[Joined]`.

- [ ] **Step 1: Keep `joined` pushes apart in `tests/unit/peer.py`.**
  - **`__init__`:** add `self.joined: list[dict[str, Any]] = []`.
  - **`call`, `push` and `quiet`:** wherever a push frame is collected, collect it this way:

```python
                if frame["type"] == "push":
                    (self.joined if frame["name"] == "joined" else self.pushes).append(
                        frame["payload"]
                    )
```

- [ ] **Step 2: Write the failing tests** in a new file, `tests/unit/test_follow.py`:

```python
from typing import Any

import pytest

from rfnry_thread.errors import InputError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import Identity, Thread
from rfnry_thread.server import Server

from .conftest import say
from .factories import ANA, HELPER
from .peer import Peer


def who(identity: Identity) -> dict[str, Any]:
    return identity.model_dump(mode="json")


async def threads(server: Server, *thread_ids: str) -> Peer:
    ana = await Peer.open(server, ANA)
    for thread_id in thread_ids:
        await ana.call("thread.create", thread_id=thread_id)
        await ana.call("member.add", thread_id=thread_id, identity=who(HELPER))
    return ana


def texts(pushes: list[dict[str, Any]]) -> list[str]:
    return [p["content"][0]["text"] for p in pushes if p["kind"] == "message"]


async def test_follow_delivers_every_thread_its_member_belongs_to_live(server: Server) -> None:
    ana = await threads(server, "t1", "t2")
    helper = await Peer.open(server, HELPER)
    reply = await helper.call("thread.follow")
    assert sorted(j["thread"]["id"] for j in reply["joined"]) == ["t1", "t2"]
    await say(ana, "t1", "one")
    await say(ana, "t2", "two")
    assert sorted(texts(await helper.quiet())) == ["one", "two"]


async def test_following_is_not_presence_but_a_follower_may_signal(server: Server) -> None:
    ana = await threads(server, "t1")
    await ana.call("thread.join", thread_id="t1")
    await ana.quiet()
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    assert [p["kind"] for p in await ana.quiet()] == []
    assert (await ana.call("thread.present", thread_id="t1"))["identities"] == [who(ANA)]
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working"))["label"] == "Reading"
    await ana.call("signal.send", thread_id="t1", kind="cancel", to=who(HELPER))
    assert (await helper.push("cancel"))["to"]["id"] == "helper"


async def test_a_follower_receives_a_thread_it_is_added_to_from_the_add(server: Server) -> None:
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    ana = await threads(server, "t1")
    await say(ana, "t1", "right after")
    pushes = await helper.quiet()
    assert [j["thread"]["id"] for j in helper.joined] == ["t1"]
    assert [p["kind"] for p in pushes] == ["member_added", "message"]


async def test_a_connection_that_does_not_follow_is_still_invited(server: Server) -> None:
    helper = await Peer.open(server, HELPER)
    await threads(server, "t1")
    assert [p["kind"] for p in await helper.quiet()] == ["invited"]
    assert helper.joined == []


async def test_a_membership_added_on_another_replica_reaches_a_follower(
    storage: InMemoryStorage,
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    try:
        helper = await Peer.open(far, HELPER)
        await helper.call("thread.follow")
        ana = await threads(near, "t1")
        await say(ana, "t1", "across")
        pushes = await helper.quiet(0.3)
        assert [j["thread"]["id"] for j in helper.joined] == ["t1"]
        assert texts(pushes) == ["across"]
    finally:
        await near.stop()
        await far.stop()


async def test_authorize_decides_which_threads_a_follower_receives(
    storage: InMemoryStorage,
) -> None:
    async def authorize(actor: Identity, action: str, thread: Thread) -> bool:
        return not thread.metadata.get("secret")

    server = Server(storage, authorize=authorize)
    for thread_id, metadata in (("open", {}), ("closed", {"secret": True})):
        await server.create_thread("acme", thread_id, metadata)
        await server.add_member("acme", thread_id, HELPER)
    helper = await Peer.open(server, HELPER)
    reply = await helper.call("thread.follow")
    assert [j["thread"]["id"] for j in reply["joined"]] == ["open"]


async def test_leave_stops_one_followed_thread(server: Server) -> None:
    ana = await threads(server, "t1", "t2")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await helper.call("thread.leave", thread_id="t1")
    await say(ana, "t1", "gone")
    await say(ana, "t2", "kept")
    assert texts(await helper.quiet()) == ["kept"]


async def test_following_twice_delivers_each_event_once(server: Server) -> None:
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await helper.call("thread.follow")
    await say(ana, "t1", "once")
    assert texts(await helper.quiet()) == ["once"]


async def test_a_follower_removed_and_added_again_receives_the_thread_from_the_new_add(
    server: Server,
) -> None:
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    await say(ana, "t1", "while out")
    await ana.call("member.add", thread_id="t1", identity=who(HELPER))
    await say(ana, "t1", "back")
    assert texts(await helper.quiet()) == ["back"]
    assert [j["thread"]["id"] for j in helper.joined] == ["t1"]


async def test_a_followed_thread_that_is_deleted_ends_with_one_notice(server: Server) -> None:
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow")
    await ana.call("thread.delete", thread_id="t1")
    assert [p["kind"] for p in await helper.quiet()] == ["thread.deleted"]


async def test_follow_refuses_a_negative_since(server: Server) -> None:
    helper = await Peer.open(server, HELPER)
    with pytest.raises(InputError, match="^server: since cannot be negative$"):
        await helper.call("thread.follow", since={"t1": -1})
```

- [ ] **Step 3: Run them to see them fail.**

Run: `uv run pytest -q tests/unit/test_follow.py`. Expected: FAIL with
`InputError: server: unknown method 'thread.follow'`.

- [ ] **Step 4: Change the protocol** in `protocol.py`.
  - Add `"thread.follow"` to `METHODS`.
  - `Push.name` becomes `Literal["event", "signal", "joined"]`.

- [ ] **Step 5: Change the hub** in `hub.py`.
  - **Import:** `Invited` stays imported; add `Joined` if it isn't there.
  - **`Session`:** add fields and a method:

```python
    present: set[str] = field(default_factory=set)
    following: bool = False

    def push_joined(self, joined: Joined) -> None:
        self.outbound.put(Push(name="joined", payload=joined.model_dump(mode="json")))
```

  - **`join`:** split into a presence-free `_enter` and a `join` that adds presence:

```python
    async def join(self, session: Session, thread: Thread, since: int | None) -> Joined:
        present = thread.id in session.present
        joined = await self._enter(session, thread, since)
        session.present.add(thread.id)
        if not present:
            await self._arrive(session, thread.id)
        return joined

    async def _enter(self, session: Session, thread: Thread, since: int | None) -> Joined:
        scope, thread_id = session.scope, thread.id
        async with self._lock(scope, thread_id):
            latest = await self.storage.list_events(scope, thread_id, limit=1)
            head = latest[-1].seq if latest else 0
            if not await self.storage.is_member(scope, thread_id, session.identity.id):
                raise ForbiddenError(
                    f"server: {session.identity.id!r} is not a member of thread {thread_id!r}"
                )
            replay: list[Event] = []
            truncated = since is not None and since > head
            if since is not None and since < head:
                if head - since > self.replay_limit:
                    replay = await self.storage.list_events(
                        scope, thread_id, before=head + 1, limit=self.replay_limit
                    )
                    truncated = True
                else:
                    replay = await self.storage.list_events(
                        scope, thread_id, since=since, limit=self.replay_limit
                    )
            session.joined[thread_id] = replay[0].seq - 1 if replay else head
            self._rooms.setdefault((scope, thread_id), set()).add(session)
            for event in replay:
                session.push_event(event, event.model_dump(mode="json"))
            for signal in self.state.snapshot(scope, thread_id):
                session.push_signal(signal.model_dump(mode="json"))
        return Joined(thread=thread, seq=head, truncated=truncated)

    async def follow(
        self, session: Session, threads: list[tuple[Thread, int | None]]
    ) -> list[Joined]:
        found: list[Joined] = []
        for thread, since in threads:
            if thread.id in session.joined:
                found.append(Joined(thread=thread, seq=session.joined[thread.id]))
                continue
            found.append(await self._enter(session, thread, since))
        return found
```

  - **`leave`:** after the streams and working cleanup, only a present connection departs:

```python
        if thread_id in session.present:
            session.present.discard(thread_id)
            await self._depart(session, thread_id)
```

  - **`_deleted_here`:** inside the loop over the room's sessions, add
    `session.present.discard(thread_id)`.
  - **Replace `invite` and `_invite_here`** with `added` and `_added_here`:

```python
    async def added(
        self, scope: str, thread: Thread, sender: Identity, identity: Identity, seq: int
    ) -> None:
        await self._added_here(scope, thread, sender, identity.id, seq)
        await self._publish(
            scope,
            thread.id,
            {
                "kind": "added",
                "thread_id": thread.id,
                "sender": sender.model_dump(mode="json"),
                "to": identity.id,
                "seq": seq,
            },
        )

    async def _added_here(
        self, scope: str, thread: Thread, sender: Identity, identity_id: str, seq: int | None
    ) -> None:
        for session in list(self.sessions):
            if (
                session.scope != scope
                or session.identity.id != identity_id
                or thread.id in session.joined
            ):
                continue
            if session.following:
                since = None if seq is None else seq - 1
                session.push_joined(await self._enter(session, thread, since))
            elif identity_id != sender.id:
                session.push_signal(
                    Invited(thread_id=thread.id, sender=sender, thread=thread).model_dump(
                        mode="json"
                    )
                )
```

  - **`on_signal`:** replace the `kind == "invited"` branch. `"invited"` is still read, so a
    replica on the previous version can still be heard during a rollout:

```python
            if kind in ("added", "invited"):
                thread = await self.storage.get_thread(scope, thread_id)
                if thread is not None:
                    seq = payload.get("seq")
                    await self._added_here(
                        scope,
                        thread,
                        Identity.model_validate(payload["sender"]),
                        str(payload["to"]),
                        None if seq is None else int(seq),
                    )
                return
```

  - **`tests/unit/test_hub.py`,** in `test_an_invitation_reaches_only_connections_not_in_the_thread`:
    the call becomes `await hub.added("acme", thread, ANA, BRUNO, 1)`. The assertions stay as
    they are, because none of those sessions follows.

- [ ] **Step 6: Change the verbs** in `verbs.py`.
  - **`_admit`:** every new member is now announced, self-adds included:

```python
    async def _admit(self, scope: str, thread: Thread, identity: Identity, by: Identity) -> Member:
        member, created = await self._storage.add_member(
            scope, Member(thread_id=thread.id, identity=identity, added_at=now(), added_by=by)
        )
        if created:
            added = await self._append(
                scope,
                {
                    "kind": "member_added",
                    "thread_id": thread.id,
                    "author": by,
                    "client_id": uuid.uuid4().hex,
                    "identity": identity,
                },
            )
            await self._hub.added(scope, thread, by, identity, added.seq)
        return member
```

  - **New methods:**

```python
    async def follow(
        self, session: Session, resume: bool, since: dict[str, int] | None
    ) -> list[Joined]:
        if since is not None and any(seq < 0 for seq in since.values()):
            raise InputError("server: since cannot be negative")
        actor, scope = session.identity, session.scope
        session.following = True
        if resume and since is None:
            await self._storage.baseline_marks(scope, actor.id)
        start = since or {}
        threads = await self._memberships(actor, scope)
        return await self._hub.follow(
            session,
            [(t, start.get(t.id, t.marked if resume else None)) for t in threads],
        )

    async def _memberships(self, actor: Identity, scope: str) -> list[Thread]:
        found: list[Thread] = []
        before: str | None = None
        while True:
            page = await self._storage.list_threads(scope, actor.id, before, PAGE_LIMIT)
            for thread in page:
                if self._authorize is None or await self._authorize(actor, "read", thread):
                    found.append(thread)
            if len(page) < PAGE_LIMIT:
                return found
            before = page[-1].id
```

- [ ] **Step 7: Change the server** in `server.py`.
  - **A new arguments model:**

```python
class _Follow(_Args):
    resume: bool = False
    since: dict[str, int] | None = None
```

  - **`_call`:** a new branch, beside `thread.join`:

```python
        if method == "thread.follow":
            follow = _args(_Follow, method, payload)
            joined = await verbs.follow(session, follow.resume, follow.since)
            return {"joined": [_dump(j) for j in joined]}
```

- [ ] **Step 8: Add the golden fixtures** in `tests/unit/test_golden.py`, in `frames`:

```python
        "request-thread-follow": Request(
            id="4", method="thread.follow", payload={"resume": True, "since": {"t1": 9}}
        ),
        "response-thread-follow": Response(
            id="4",
            payload={"joined": [Joined(thread=thread, seq=12).model_dump(mode="json")]},
        ),
        "push-joined": Push(
            name="joined", payload=Joined(thread=thread, seq=12).model_dump(mode="json")
        ),
```

  In `test_every_fixture_round_trips`, validate a `joined` push as a `Joined`:

```python
    if isinstance(frame, Push):
        if frame.name == "joined":
            assert Joined.model_validate(frame.payload).model_dump(mode="json") == frame.payload
        else:
            adapter = EVENTS if frame.name == "event" else SIGNALS
            assert adapter.validate_python(frame.payload).model_dump(mode="json") == frame.payload
```

  Run `uv run poe fixtures`, then `uv run pytest -q tests/unit/test_golden.py`. Expected: PASS.

- [ ] **Step 9: Keep the TypeScript gate green.**
  - **`packages/typescript/src/protocol.ts`:**
    `export type Push = { type: 'push'; name: 'event' | 'signal' | 'joined'; payload: Payload }`.
  - **`packages/typescript/tests/golden.test.ts`:** `expect(files.length).toBe(31)`.
  - Run the TypeScript gate. Expected: PASS.

- [ ] **Step 10: Add the two-replica test on a real database** to
  `tests/integration/test_replicas.py`, using its `replicas` fixture:

```python
async def test_a_membership_added_on_one_replica_reaches_a_follower_on_another(
    replicas: tuple[Server, Server],
) -> None:
    near, far = replicas
    helper = await Peer.open(far, HELPER)
    await helper.call("thread.follow")
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await say(ana, "t1", "across")
    pushes = await helper.quiet(0.5)
    assert [j["thread"]["id"] for j in helper.joined] == ["t1"]
    assert [p["content"][0]["text"] for p in pushes if p["kind"] == "message"] == ["across"]
```

  Then run the Postgres tier. Expected: PASS.

- [ ] **Step 11: Run the tests.**

Run: `uv run pytest -q tests/unit/test_follow.py tests/unit/test_replicas.py tests/unit/test_server.py`.
Expected: PASS.

Then the full Python gate.

- [ ] **Step 12: Commit and push.**

```bash
git add packages/python packages/typescript
git commit -m "Server: follow delivers every thread a connection belongs to, and each new one from its add"
git push origin main
```

---

### Task 4: Marks, resume, forward history and the inbox options on the server

**Files:**
- Modify: `packages/python/src/rfnry_thread/models.py` (`Marked`)
- Modify: `packages/python/src/rfnry_thread/__init__.py` (export `Marked`)
- Modify: `packages/python/src/rfnry_thread/protocol.py` (`METHODS`)
- Modify: `packages/python/src/rfnry_thread/hub.py` (`marked`)
- Modify: `packages/python/src/rfnry_thread/verbs.py`
- Modify: `packages/python/src/rfnry_thread/server.py`
- Modify: `packages/python/tests/unit/test_golden.py`: the `Marked` signal, and the mark request and response
- Modify: `packages/python/tests/unit/test_client.py:315-320`: `Server.list_threads` keywords
- Modify: `packages/typescript/src/types.ts` (`Marked` in `Signal`) and `packages/typescript/tests/golden.test.ts` (`SIGNAL_KINDS` and the count)
- Test: `packages/python/tests/unit/test_follow.py`
- Docs: `docs/api.md` (`Marked` in both halves, `Server.list_threads`), and `docs/thread.md` (the signals table)

**Interfaces:**
- Consumes:
  - `Storage.mark` and `Storage.baseline_marks` (Task 1);
  - `Storage.list_threads(order, preview)` (Task 2);
  - `Verbs.follow` (Task 3).
- Produces:
  - **The signal:** `class Marked(_Signal)`, with `kind="marked"`, `seq: int` and `unread: int`;
  - **The request:** `"thread.mark"`, payload `{thread_id, seq}` → `{"seq": int, "unread": int}`;
  - **History:** `"thread.history"` accepts `after: int | None`;
  - **Listing:** `"thread.list"` accepts `order` and `preview`;
  - **The server verb:** `Server.list_threads(scope, *, metadata=None, before=None, limit=50, order="created", preview=False)`;
  - **The first mark at an add:** in `_admit`.

- [ ] **Step 1: Write the failing tests**, appended to `tests/unit/test_follow.py`. Add
  `ForbiddenError` to the errors import; `Member`, `SYSTEM` and `AT`/`BRUNO` to the imports from
  models and factories:

```python
async def test_a_member_is_marked_just_before_it_was_added(server: Server) -> None:
    ana = await threads(server, "t1")
    await say(ana, "t1", "hello")
    helper = await Peer.open(server, HELPER)
    [listed] = (await helper.call("thread.list"))["threads"]
    assert (listed["marked"], listed["unread"]) == (1, 1)


async def test_resume_replays_each_gap_after_the_mark(server: Server) -> None:
    ana = await threads(server, "t1", "t2")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow", resume=True)
    seen = await say(ana, "t1", "seen")
    await helper.quiet()
    assert await helper.call("thread.mark", thread_id="t1", seq=seen["seq"]) == {
        "seq": seen["seq"],
        "unread": 0,
    }
    await helper.close()
    await say(ana, "t1", "missed one")
    await say(ana, "t2", "missed two")
    again = await Peer.open(server, HELPER)
    await again.call("thread.follow", resume=True)
    assert sorted(texts(await again.quiet())) == ["missed one", "missed two"]


async def test_a_membership_with_no_mark_resumes_from_now_and_is_given_one(
    storage: InMemoryStorage, server: Server
) -> None:
    await server.create_thread("acme", "t1")
    await server.add_member("acme", "t1", ANA)
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=HELPER, added_at=AT, added_by=SYSTEM)
    )
    ana = await Peer.open(server, ANA)
    await say(ana, "t1", "old")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow", resume=True)
    assert texts(await helper.quiet()) == []
    [listed] = (await helper.call("thread.list"))["threads"]
    assert listed["marked"] == 2


async def test_a_since_map_naming_a_foreign_thread_is_ignored(server: Server) -> None:
    await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    reply = await helper.call("thread.follow", resume=True, since={"elsewhere": 0})
    assert [j["thread"]["id"] for j in reply["joined"]] == ["t1"]


async def test_a_resume_gap_beyond_the_replay_limit_is_the_latest_and_truncated(
    storage: InMemoryStorage,
) -> None:
    server = Server(storage, replay_limit=2)
    ana = await threads(server, "t1")
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.follow", resume=True)
    await helper.close()
    for n in range(4):
        await say(ana, "t1", f"m{n}")
    again = await Peer.open(server, HELPER)
    [joined] = (await again.call("thread.follow", resume=True))["joined"]
    assert joined["truncated"] is True
    assert texts(await again.quiet()) == ["m2", "m3"]


async def test_a_mark_never_moves_back_and_must_name_a_seq_in_the_thread(server: Server) -> None:
    ana = await threads(server, "t1")
    await say(ana, "t1", "a")
    await say(ana, "t1", "b")
    helper = await Peer.open(server, HELPER)
    assert (await helper.call("thread.mark", thread_id="t1", seq=4))["seq"] == 4
    assert (await helper.call("thread.mark", thread_id="t1", seq=3))["seq"] == 4
    for seq in (-1, 5):
        with pytest.raises(InputError, match="^server: seq must be between 0 and 4$"):
            await helper.call("thread.mark", thread_id="t1", seq=seq)
    bruno = await Peer.open(server, BRUNO)
    with pytest.raises(ForbiddenError):
        await bruno.call("thread.mark", thread_id="t1", seq=0)


async def test_a_mark_reaches_the_markers_own_connections_on_every_replica(
    storage: InMemoryStorage,
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    try:
        ana = await threads(near, "t1")
        message = await say(ana, "t1", "hi")
        phone = await Peer.open(near, HELPER)
        laptop = await Peer.open(far, HELPER)
        await ana.quiet()
        reply = await phone.call("thread.mark", thread_id="t1", seq=message["seq"])
        assert reply == {"seq": message["seq"], "unread": 0}
        told = await laptop.push("marked")
        assert (told["thread_id"], told["seq"], told["unread"]) == ("t1", message["seq"], 0)
        assert [p["kind"] for p in await ana.quiet()] == []
    finally:
        await near.stop()
        await far.stop()


async def test_history_reads_forward_after_a_seq_and_within_a_range(server: Server) -> None:
    ana = await threads(server, "t1")
    for n in range(4):
        await say(ana, "t1", f"m{n}")
    forward = (await ana.call("thread.history", thread_id="t1", after=3, limit=2))["events"]
    assert [e["seq"] for e in forward] == [4, 5]
    window = (await ana.call("thread.history", thread_id="t1", after=2, before=5))["events"]
    assert [e["seq"] for e in window] == [3, 4]
    with pytest.raises(InputError, match="^server: after cannot be negative$"):
        await ana.call("thread.history", thread_id="t1", after=-1)


async def test_the_inbox_lists_by_activity_with_previews(server: Server) -> None:
    ana = await threads(server, "t1", "t2")
    await say(ana, "t1", "newest")
    listed = (await ana.call("thread.list", order="activity", preview=True))["threads"]
    assert [t["id"] for t in listed] == ["t1", "t2"]
    assert listed[0]["last_message"]["content"][0]["text"] == "newest"
    assert "last_message" not in listed[1]
    with pytest.raises(InputError):
        await ana.call("thread.list", order="loudest")
    system = await server.list_threads("acme", order="activity", preview=True)
    assert [t.id for t in system] == ["t1", "t2"]
    assert system[0].marked is None and system[0].last_message is not None
```

- [ ] **Step 2: Run them to see them fail.**

Run: `uv run pytest -q tests/unit/test_follow.py`. Expected: the new tests FAIL. For example,
`thread.list` has no `marked`, and `thread.mark` is an unknown method.

- [ ] **Step 3: Add the signal** in `models.py`, after `ThreadDeleted`:

```python
class Marked(_Signal):
    kind: Literal["marked"] = "marked"
    seq: int
    unread: int
```

  - **The `Signal` union:** add `| Marked`.
  - **`__init__.py`:** import and export `Marked`. Add it to `__all__` after `"ThreadDeleted"`.

- [ ] **Step 4: Change the protocol and the hub.**
  - **`protocol.py`:** add `"thread.mark"` to `METHODS`.
  - **`hub.py`:** import `Marked` and add:

```python
    async def marked(self, scope: str, signal: Marked) -> None:
        self._marked_here(scope, signal)
        await self._publish(scope, signal.thread_id, signal.model_dump(mode="json"))

    def _marked_here(self, scope: str, signal: Marked) -> None:
        payload = signal.model_dump(mode="json")
        for session in list(self.sessions):
            if session.scope == scope and session.identity.id == signal.sender.id:
                session.push_signal(payload)
```

  - **`on_signal`:** before `signal = SIGNALS.validate_python(payload)`, add:

```python
            if kind == "marked":
                self._marked_here(scope, Marked.model_validate(payload))
                return
```

- [ ] **Step 5: Change the verbs.**
  - **Imports:** `Marked`, and `Order` from models.
  - **`_admit`:** after `added = await self._append(...)` and before `self._hub.added(...)`, write
    the first mark:

```python
            await self._storage.mark(scope, thread.id, identity.id, added.seq - 1)
```

  - **A new method:**

```python
    async def mark(self, session: Session, thread_id: str, seq: int) -> tuple[int, int]:
        actor, scope = session.identity, session.scope
        await self.thread(actor, scope, thread_id, "read")
        latest = await self._storage.list_events(scope, thread_id, limit=1)
        head = latest[-1].seq if latest else 0
        if not 0 <= seq <= head:
            raise InputError(f"server: seq must be between 0 and {head}")
        stored, unread = await self._storage.mark(scope, thread_id, actor.id, seq)
        await self._hub.marked(
            scope, Marked(thread_id=thread_id, sender=actor, seq=stored, unread=unread)
        )
        return stored, unread
```

  - **`history`** gains `after`:

```python
    async def history(
        self,
        actor: Identity,
        scope: str,
        thread_id: str,
        before: int | None,
        limit: int,
        kinds: Sequence[str] | None,
        after: int | None = None,
    ) -> list[Event]:
        if after is not None and after < 0:
            raise InputError("server: after cannot be negative")
        await self.thread(actor, scope, thread_id, "read")
        return await self._storage.list_events(
            scope, thread_id, since=after, before=before, limit=_limit(limit), kinds=kinds
        )
```

  - **`list_threads`** becomes:

```python
    async def list_threads(
        self,
        actor: Identity,
        scope: str,
        before: str | None,
        limit: int,
        metadata: dict[str, Any] | None = None,
        order: Order = "created",
        preview: bool = False,
    ) -> list[Thread]:
        limit = _limit(limit)

        async def page(member_id: str | None, after: str | None) -> list[Thread]:
            return await self._storage.list_threads(
                scope, member_id, after, limit, metadata, order, preview
            )

        if actor.role == "system":
            return await page(None, before)
        if self._authorize is None:
            return await page(actor.id, before)
        readable: list[Thread] = []
        while True:
            found = await page(actor.id, before)
            for thread in found:
                if await self._authorize(actor, "read", thread):
                    readable.append(thread)
                    if len(readable) == limit:
                        return readable
            if len(found) < limit:
                return readable
            before = found[-1].id
```

- [ ] **Step 6: Change the server.**
  - **The arguments models:**

```python
class _List(_Args):
    before: str | None = None
    limit: int = 50
    order: Order = "created"
    preview: bool = False


class _History(_Ref):
    before: int | None = None
    after: int | None = None
    limit: int = 50
    kinds: list[str] | None = None


class _Mark(_Ref):
    seq: int
```

  - **`_call`, the `thread.list` branch:**
    `verbs.list_threads(actor, scope, listing.before, listing.limit, None, listing.order, listing.preview)`.
  - **`_call`, the `thread.history` branch:** passes `history.after` as the last argument.
  - **`_call`, a new branch:**

```python
        if method == "thread.mark":
            mark = _args(_Mark, method, payload)
            seq, unread = await verbs.mark(session, mark.thread_id, mark.seq)
            return {"seq": seq, "unread": unread}
```

  - **`Server.list_threads`:**

```python
    async def list_threads(
        self,
        scope: str,
        *,
        metadata: dict[str, Any] | None = None,
        before: str | None = None,
        limit: int = 50,
        order: Order = "created",
        preview: bool = False,
    ) -> list[Thread]:
        return await self._verbs.list_threads(
            SYSTEM, _scope(scope), before, limit, metadata, order, preview
        )
```

  - **`tests/unit/test_client.py`:** its keyword-only assertion for `Server.list_threads`
    becomes `["metadata", "before", "limit", "order", "preview"]`.

- [ ] **Step 7: Add the fixtures, and keep the TypeScript gate green.**
  - **`test_golden.py`:**
    - add `Marked(thread_id="t1", sender=HELPER, seq=7, unread=2)` to `signals`, and `Marked`
      to the import;
    - add to `frames`:

```python
        "request-thread-mark": Request(
            id="5", method="thread.mark", payload={"thread_id": "t1", "seq": 7}
        ),
        "response-thread-mark": Response(id="5", payload={"seq": 7, "unread": 2}),
```

  - Run `uv run poe fixtures`.
  - **`packages/typescript/src/types.ts`:** add
    `export type Marked = Sent & { kind: 'marked'; seq: number; unread: number }` and `| Marked`
    to `Signal`.
  - **`packages/typescript/tests/golden.test.ts`:** add `marked: true` to `SIGNAL_KINDS` and
    expect `34` files.

- [ ] **Step 8: Update the docs.**
  - **`docs/thread.md`, the signals table:** after the `Invited` row:

```markdown
| `Marked` | `seq`, `unread` | your read position in the thread moved; only your own connections hear it |
```

  - **`docs/api.md`:** a `### \`Marked\`` entry in both halves, beside `Invited`:
    - Python:

```python
class Marked(BaseModel):
    kind: Literal["marked"] = "marked"
    thread_id: str
    sender: Identity
    seq: int
    unread: int
```

    - TypeScript: `type Marked = { kind: 'marked'; thread_id: string; sender: Identity; seq: number; unread: number }`.
    - Prose:

```markdown
Your private read position in the thread moved to `seq`, with `unread` messages by others still
after it (at most 100); sent to every connection of the identity that marked, never to anyone
else.
```

  - **`docs/api.md`, `### \`Server.list_threads\`:** the new signature, and this sentence:

```markdown
`order="activity"` lists by the latest message, and `preview=True` fills `last_message`; a system
listing has no `marked` or `unread`.
```

- [ ] **Step 9: Run the full Python gate and the TypeScript gate.** Both must pass.

- [ ] **Step 10: Commit and push.**

```bash
git add packages docs
git commit -m "Server: marks, resume from them, forward history, and the inbox's order and preview"
git push origin main
```

---

### Task 5: The Python client follows, marks, re-follows and reads forward

**Files:**
- Modify: `packages/python/src/rfnry_thread/client.py`
- Modify: `packages/python/tests/unit/test_public_api.py` (`CLIENT` gains `"follow"` and `"mark"`)
- Test: `packages/python/tests/unit/test_client.py` and `packages/python/tests/unit/test_socket.py`
- Docs: `docs/api.md` (`Client.follow`, `Client.mark`, `Client.history`, `Client.list_threads`); `docs/thread.md` (one sentence each for `.follow(` and `.mark(`, so the docs test finds them; the narrative is Task 7)

**Interfaces:**
- Consumes:
  - `"thread.follow"`, `"thread.mark"`, and `"thread.history"` with `after` (Tasks 3 and 4);
  - `"thread.list"` with `order` and `preview` (Task 4);
  - the `joined` push.
- Produces:
  - `Client.follow(resume: bool = False) -> list[Joined]`;
  - `Client.mark(thread_id: str, seq: int) -> int`;
  - `Client.history(thread_id, before=None, limit=50, kinds=None, after=None)`;
  - `Client.list_threads(before=None, limit=50, order="created", preview=False)`.

- [ ] **Step 1: Write the failing tests.**
  - **In `tests/unit/test_client.py`:** import `Marked`, `Text` and `Joined` from
    `rfnry_thread.models`.

```python
async def test_a_client_follows_its_threads_and_is_handed_new_ones(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    helper = await server.connect(identity=HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    [joined] = await helper.follow()
    assert joined.thread.id == "t1"
    await ana.create_thread(thread_id="t2")
    await ana.add_member("t2", HELPER)
    await ana.send("t2", "hello")
    kinds = [type(item).__name__ for item in await drain(helper, include_own=False)]
    assert kinds == ["Joined", "MemberAdded", "Joined", "Message"]


async def test_mark_returns_the_stored_position_and_tells_the_markers_other_clients(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    phone = await server.connect(identity=HELPER, scope="acme")
    laptop = await server.connect(identity=HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    message = await ana.send("t1", "hi")
    assert await phone.mark("t1", message.seq) == message.seq
    assert await phone.mark("t1", 0) == message.seq
    told = [item for item in await drain(laptop, include_own=False) if isinstance(item, Marked)]
    assert {(m.thread_id, m.seq, m.unread) for m in told} == {("t1", message.seq, 0)}


async def test_history_reads_forward_and_the_inbox_lists_by_activity(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    for thread_id in ("t1", "t2"):
        await ana.create_thread(thread_id=thread_id)
    first = await ana.send("t1", "one")
    await ana.send("t1", "two")
    assert [e.seq for e in await ana.history("t1", after=first.seq)] == [first.seq + 1]
    listed = await ana.list_threads(order="activity", preview=True)
    assert [t.id for t in listed] == ["t1", "t2"]
    assert listed[0].last_message is not None
    assert listed[0].last_message.content == [Text(text="two")]
```

  - **In `tests/unit/test_socket.py`:** import `Message` (already imported) and `Identity`.

```python
async def test_a_follower_refollows_once_after_a_drop_and_misses_nothing(live: Live) -> None:
    ana_identity = Identity(role="user", id="ana", name="Ana")
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ana_identity)
    await ana.follow()
    await drain(ana)
    live.paused = True
    await live.drop()
    await until(lambda: ana.status == "reconnecting")
    await bruno.send("t1", "away one")
    await bruno.create_thread(thread_id="t3")
    await bruno.add_member("t3", ana_identity)
    await bruno.send("t3", "away three")
    live.paused = False
    await until(lambda: ana.status == "open", timeout=3.0)
    said = [first_text(item) for item in await drain(ana) if isinstance(item, Message)]
    assert sorted(said) == ["away one", "away three"]
```

- [ ] **Step 2: Run them to see them fail.**

Run: `uv run pytest -q tests/unit/test_client.py tests/unit/test_socket.py -k "follow or mark or forward"`.
Expected: FAIL with `AttributeError: 'Client' object has no attribute 'follow'`.

- [ ] **Step 3: Implement the client** in `client.py`.
  - **Imports:** `Marked` and `Order` from models.
  - **`__init__`:** add `self._following = False` and `self._present: set[str] = set()`.
  - **`_own`:** returns `False` for `Joined` and `Marked`:
    `if isinstance(item, Joined | Marked): return False`.
  - **`_receive`:** handle the `joined` push first:

```python
        if push.name == "joined":
            joined = Joined.model_validate(push.payload)
            thread_id = joined.thread.id
            self._cursors[thread_id] = max(self._cursors.get(thread_id, 0), joined.seq)
            self._inbox.put_nowait(joined)
            return
```

  - **`_rejoin`** becomes:

```python
    async def _rejoin(self) -> None:
        if self._following:
            try:
                await self._follow(True, dict(self._cursors))
            except TransportError:
                return
            except ThreadException:
                pass
        for thread_id, cursor in list(self._cursors.items()):
            if self._following and thread_id not in self._present:
                continue
            try:
                joined = await self._join(thread_id, cursor)
            except TransportError:
                return
            except ThreadException:
                self._cursors.pop(thread_id, None)
                self._present.discard(thread_id)
                continue
            self._inbox.put_nowait(joined)
```

  - **`join` and `leave`:**

```python
    async def join(self, thread_id: str, since: int | None = None) -> Joined:
        try:
            joined = await self._join(thread_id, since)
        except ThreadException:
            self._cursors.pop(thread_id, None)
            raise
        self._present.add(thread_id)
        return joined

    async def leave(self, thread_id: str) -> None:
        self._cursors.pop(thread_id, None)
        self._present.discard(thread_id)
        await self._request("thread.leave", {"thread_id": thread_id})
```

  - **New verbs:**

```python
    async def follow(self, resume: bool = False) -> list[Joined]:
        return await self._follow(resume, None)

    async def _follow(self, resume: bool, since: dict[str, int] | None) -> list[Joined]:
        self._following = True
        reply = await self._request("thread.follow", {"resume": resume, "since": since})
        found = [Joined.model_validate(joined) for joined in reply["joined"]]
        for joined in found:
            thread_id = joined.thread.id
            self._cursors[thread_id] = max(self._cursors.get(thread_id, 0), joined.seq)
            self._inbox.put_nowait(joined)
        return found

    async def mark(self, thread_id: str, seq: int) -> int:
        reply = await self._request("thread.mark", {"thread_id": thread_id, "seq": seq})
        return int(reply["seq"])
```

  - **`history`** gains `after: int | None = None` as its last parameter and sends `"after": after`.
  - **`list_threads`** becomes:

```python
    async def list_threads(
        self,
        before: str | None = None,
        limit: int = 50,
        order: Order = "created",
        preview: bool = False,
    ) -> list[Thread]:
        reply = await self._request(
            "thread.list",
            {"before": before, "limit": limit, "order": order, "preview": preview},
        )
        return [Thread.model_validate(t) for t in reply["threads"]]
```

  - **`tests/unit/test_public_api.py`:** add `"follow"` and `"mark"` to `CLIENT`.

- [ ] **Step 4: Run the tests.**

Run: `uv run pytest -q tests/unit/test_client.py tests/unit/test_socket.py`. Expected: PASS.

- [ ] **Step 5: Update the docs.**
  - **`docs/api.md`, the Python client:** new entries `### \`Client.follow\`` and
    `### \`Client.mark\``, placed after `Client.leave`:

```python
async def follow(self, resume: bool = False) -> list[Joined]: ...
```

```markdown
Receives every thread the caller is a member of, from now on, and each thread it is added to later
from the moment it is added, as a `Joined` on `events()`; returns the `Joined` of each thread it
receives now. With `resume=True`, each thread's events after the caller's mark come first (from
where it was added when it has no mark yet; a membership older than marks starts now), at most the
server's `replay_limit`, beyond which the latest and `truncated=True`. Following is not presence:
no `Present` is announced, and the caller may still signal and receive signals in every thread.
After a dropped socket the client follows again from what it has seen. Threads `authorize` refuses
to `read` are left out. Raises `InputError` for a negative position.
```

```python
async def mark(self, thread_id: str, seq: int) -> int: ...
```

```markdown
Stores the caller's private read position in the thread, never lowering it, and returns the stored
`seq`; the caller's other connections receive `Marked`. Raises `InputError` when `seq` is not
between 0 and the thread's last `seq`, `NotFoundError` when the scope holds no such thread,
`ForbiddenError` when the caller is not a member or `authorize` refuses `read`.
```

  - **`Client.history`:** the signature with `after: int | None = None` last. Add to its prose:
    "With `after` (a `seq`), the first `limit` events after it, in `seq` order; `before` and
    `after` together are a range. Raises `InputError` when `after` is negative".
  - **`Client.list_threads`:** the new signature. Replace its prose with:

```markdown
Returns the threads the caller is a member of, newest first, up to `limit`; `before` is a thread
id, for the next page. `order="activity"` orders them by their latest message. Each carries
`last_seq`, `last_message_at`, `marked` and `unread`, and with `preview=True` its `last_message`.
With an `authorize`, the threads it refuses to `read` are skipped and a page still holds up to
`limit`. Raises `InputError` when `limit` is not between 1 and 500 or `order` is neither.
```

  - **`docs/thread.md`, "Threads and members":** after the join paragraph, add one sentence. Task
    7 replaces it with the full section:

```markdown
`await helper.follow()` receives every thread you belong to at once, and `await helper.mark(thread.id, seq)`
stores your read position; [Following and catching up](#following-and-catching-up) has the rest.
```

- [ ] **Step 6: Run the full Python gate.** It must pass, including `test_docs.py`,
  `test_api.py` and `test_public_api.py`.

- [ ] **Step 7: Commit and push.**

```bash
git add packages/python docs
git commit -m "Clients: the Python client follows, marks, re-follows after a drop and reads forward"
git push origin main
```

---

### Task 6: The TypeScript client, with parity and the live tier

**Files:**
- Modify: `packages/typescript/src/types.ts` (`Thread` fields)
- Modify: `packages/typescript/src/client.ts`
- Modify: `packages/typescript/tests/fakes.ts` (`push` takes `'joined'`)
- Test: `packages/typescript/tests/client.test.ts` and `packages/typescript/tests/e2e/live.test.ts`
- Docs: `docs/api.md`, the TypeScript half (`Client.follow`, `Client.mark`, `Client.history`, `Client.listThreads`)

**Interfaces:**
- Consumes: the wire from Tasks 3 and 4, and the `Marked` type (Task 4).
- Produces:
  - `follow(options: { resume?: boolean } = {}): Promise<Joined[]>`;
  - `mark(threadId: string, seq: number): Promise<number>`;
  - `history(threadId, options: { before?; after?; limit?; kinds? })`;
  - `listThreads(options: { before?; limit?; order?: 'created' | 'activity'; preview?: boolean })`.

- [ ] **Step 1: Write the failing tests.**
  - **In `tests/fakes.ts`:** the `push` method becomes
    `push(name: 'event' | 'signal' | 'joined', payload: Frame): void`.
  - **In `tests/client.test.ts`:**

```ts
  it('follows, and after a drop follows again once from the cursors it holds', async () => {
    server.handlers.set('thread.follow', () => ({
      joined: [
        {
          thread: { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
          seq: 4,
          truncated: false,
        },
      ],
    }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item), { includeOwn: true })
    const [joined] = await client.follow({ resume: true })
    expect(joined.thread.id).toBe('t1')
    server.push('event', message(5, 'live'))
    await vi.waitFor(() => expect(items.some((item) => 'seq' in item && item.seq === 5)).toBe(true))
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.follow')).toHaveLength(2))
    expect(server.sent('thread.follow').map((frame) => frame.payload)).toEqual([
      { resume: true, since: null },
      { resume: true, since: { t1: 5 } },
    ])
    expect(server.sent('thread.join')).toEqual([])
  })

  it('takes a thread the server starts delivering and keeps its cursor', async () => {
    server.handlers.set('thread.follow', () => ({ joined: [] }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    server.push('joined', {
      thread: { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
      seq: 6,
      truncated: false,
    })
    await vi.waitFor(() => expect(items.some((item) => 'truncated' in item)).toBe(true))
    server.push('event', message(6, 'old'))
    server.push('event', message(7, 'new'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 7 }))
    expect(items.filter((item) => 'seq' in item && item.seq === 6)).toEqual([])
  })

  it('marks, and hears its own marks from its other connections', async () => {
    server.handlers.set('thread.mark', (payload) => ({ seq: payload.seq, unread: 0 }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    expect(await client.mark('t1', 3)).toBe(3)
    server.push('signal', {
      kind: 'marked',
      thread_id: 't1',
      sender: server.identity,
      seq: 3,
      unread: 0,
    })
    await vi.waitFor(() => expect(items).toContainEqual(expect.objectContaining({ kind: 'marked' })))
  })

  it('reads forward and lists an inbox by activity with previews', async () => {
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    await client.history('t1', { after: 3 })
    await client.listThreads({ order: 'activity', preview: true })
    expect(server.sent('thread.history').at(-1)?.payload).toMatchObject({ after: 3 })
    expect(server.sent('thread.list').at(-1)?.payload).toMatchObject({
      order: 'activity',
      preview: true,
    })
  })
```

  - **In `tests/e2e/live.test.ts`,** inside `describe.skipIf(!url)`:

```ts
  it('follows its threads, resumes after a drop without repeats, and marks', async () => {
    const client = await connect({ url, protocols: async () => ['thread', 'ana'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item), { includeOwn: true })
    const thread = await client.createThread({ threadId: 'e2e-follow' })
    await client.addMember(thread.id, helper)
    const joined = await client.follow({ resume: true })
    expect(joined.map((one) => one.thread.id)).toContain('e2e-follow')
    await client.custom(thread.id, 'drop', {})
    await until(() => client.status === 'open' && items.some((item) => 'status' in item))
    const sent = await client.send(thread.id, 'after the drop')
    expect(await client.mark(thread.id, sent.seq)).toBe(sent.seq)
    const listed = (await client.listThreads({ order: 'activity' })).find((t) => t.id === thread.id)
    expect(listed?.marked).toBe(sent.seq)
    const seqs = items.flatMap((item) => ('kind' in item && 'seq' in item ? [item.seq] : []))
    expect(new Set(seqs).size).toBe(seqs.length)
    await client.close()
  })
```

- [ ] **Step 2: Run them to see them fail.**

Run, from `packages/typescript`: `npm test`. Expected: FAIL with
`client.follow is not a function`.

- [ ] **Step 3: Implement the client.**
  - **`types.ts`, `Thread`:** add `last_seq?: number`, `last_message_at?: string`,
    `marked?: number`, `unread?: number`, `last_message?: Message`.
  - **`client.ts`, fields:** `private following = false` and
    `private readonly present = new Set<string>()`.
  - **`receive`:** before the `frame.name === 'event'` branch:

```ts
    if (frame.name === 'joined') {
      const joined = frame.payload as unknown as Joined
      this.cursors.set(
        joined.thread.id,
        Math.max(this.cursors.get(joined.thread.id) ?? 0, joined.seq)
      )
      this.emit(joined)
      return
    }
```

  - **`emit`:** a `Marked` is never "own":

```ts
    const own =
      who !== null &&
      who.id === this.identity.id &&
      !('kind' in item && item.kind === 'marked')
```

  - **`rejoin`** becomes:

```ts
  private async rejoin(): Promise<void> {
    if (this.following) {
      try {
        await this.followFrom(true, Object.fromEntries(this.cursors))
      } catch (error) {
        if (error instanceof TransportError) return
      }
    }
    for (const [threadId, cursor] of [...this.cursors]) {
      if (this.following && !this.present.has(threadId)) continue
      try {
        this.emit(await this.joinFrom(threadId, cursor))
      } catch (error) {
        if (error instanceof TransportError) return
        this.cursors.delete(threadId)
        this.present.delete(threadId)
      }
    }
  }
```

  - **`join` and `leave`:**

```ts
  async join(threadId: string, options: { since?: number } = {}): Promise<Joined> {
    try {
      const joined = await this.joinFrom(threadId, options.since ?? null)
      this.present.add(threadId)
      return joined
    } catch (error) {
      this.cursors.delete(threadId)
      throw error
    }
  }

  async leave(threadId: string): Promise<void> {
    this.cursors.delete(threadId)
    this.present.delete(threadId)
    await this.request('thread.leave', { thread_id: threadId })
  }
```
  - **New verbs:**

```ts
  async follow(options: { resume?: boolean } = {}): Promise<Joined[]> {
    return this.followFrom(options.resume ?? false, null)
  }

  private async followFrom(
    resume: boolean,
    since: Record<string, number> | null
  ): Promise<Joined[]> {
    this.following = true
    const reply = await this.request('thread.follow', { resume, since })
    const found = reply.joined as Joined[]
    for (const joined of found) {
      this.cursors.set(
        joined.thread.id,
        Math.max(this.cursors.get(joined.thread.id) ?? 0, joined.seq)
      )
      this.emit(joined)
    }
    return found
  }

  async mark(threadId: string, seq: number): Promise<number> {
    const reply = await this.request('thread.mark', { thread_id: threadId, seq })
    return reply.seq as number
  }
```

  - **`history`'s options:** gain `after?: number`, sent as `after: options.after ?? null`.
  - **`listThreads`** becomes:

```ts
  async listThreads(
    options: {
      before?: string
      limit?: number
      order?: 'created' | 'activity'
      preview?: boolean
    } = {}
  ): Promise<Thread[]> {
    const reply = await this.request('thread.list', {
      before: options.before ?? null,
      limit: options.limit ?? 50,
      order: options.order ?? 'created',
      preview: options.preview ?? false,
    })
    return reply.threads as Thread[]
  }
```

- [ ] **Step 4: Run the TypeScript gate.** Expected: PASS.

- [ ] **Step 5: Run the live tier.** From `packages/python`:
  `uv run poe test:e2e`. It builds and runs the TypeScript live tests against a Python server.
  Expected: PASS.

- [ ] **Step 6: Update `docs/api.md`, the TypeScript half,** mirroring Task 5's prose:
  - `### \`Client.follow\``: `async follow(options: { resume?: boolean } = {}): Promise<Joined[]>`;
  - `### \`Client.mark\``: `async mark(threadId: string, seq: number): Promise<number>`;
  - the new `history` and `listThreads` signatures and their sentences.

  Then run the Python gate (`test_api.py` reads this half too).

- [ ] **Step 7: Commit and push.**

```bash
git add packages/typescript docs/api.md
git commit -m "Clients: the TypeScript client follows, marks, re-follows after a drop and reads forward"
git push origin main
```

---

### Task 7: The contract's narrative — following, catching up, one handler per thread

**Files:**
- Modify: `docs/thread.md`

**Interfaces:**
- Consumes: every verb from Tasks 1–6.
- Produces: the sections other docs link to: `#following-and-catching-up` and
  `#one-handler-per-thread`.

- [ ] **Step 1: Write the section.** Replace the one sentence added in Task 5 with a new section,
  `### Following and catching up`, placed after "Delivery and resume":

````markdown
### Following and catching up

A participant that belongs to many threads follows them instead of joining each:

```python
await helper.follow(resume=True)
async for item in helper.events():
    if isinstance(item, Message):
        await handle(item)
        await helper.mark(item.thread_id, item.seq)
```

`follow()` receives every thread you are a member of from now on, and a thread you are added to
later from the moment you were added, as a `Joined` on the stream: there is no `Invited` to answer.
Following is not presence; a page that opens a thread still joins it. `follow(resume=True)` first
hands you what each thread said after your **mark**, your private read position: `mark(thread_id,
seq)` stores it, never lowers it, and tells your other connections with `Marked`. A thread you were
added to starts from the add; a gap longer than the replay limit arrives as its latest events with
`truncated=True`, and `history(thread_id, after=mark, before=…)` reads the rest. Resuming is at
least once: an event handled just before a crash comes again, so handle idempotently, and when you
handle events concurrently, mark only up to the lowest one not yet handled.

An inbox reads the same marks:

```python
inbox = await ana.list_threads(order="activity", preview=True)
inbox[0].unread, inbox[0].last_message
first_unread = await ana.history(inbox[0].id, after=inbox[0].marked)
older = await ana.history(inbox[0].id, before=first_unread[0].seq)
```

`order="activity"` puts the thread with the latest message first; `unread` counts messages by
others after your mark, up to 100; `preview=True` adds each thread's latest message, edits applied,
retractions skipped.
````

- [ ] **Step 2: Add the pattern** to "Many replicas":

````markdown
#### One handler per thread

Every replica's assistant follows the same threads, so every one of them hears each message. When
exactly one should answer, hold a Postgres advisory lock for the thread while a run works on it,
on the same database, and let the others leave the message alone:

```python
async with pool.acquire() as conn:
    if await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", item.thread_id):
        try:
            await answer(item)
            await assistant.mark(item.thread_id, item.seq)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", item.thread_id)
```

A replica that dies releases its lock with its connection. Effects stay idempotent all the same: a
resume can hand the next holder an event the last one handled just before it died.
````

- [ ] **Step 3: Add a sentence to "Threads and members".** After the paragraph on joining on
  `Invited`, add: "A participant that follows never needs this: a thread it is added to arrives on
  its own ([Following and catching up](#following-and-catching-up))."

- [ ] **Step 4: Check that the examples in the docs run.** Run
  `uv run pytest -q tests/unit/test_docs.py tests/unit/test_docs_examples.py`. Expected: PASS. If
  `test_docs_examples.py` executes fenced Python blocks, keep the new blocks syntactically valid
  within its conventions; `handle` and `answer` are names the reader brings.

- [ ] **Step 5: Run the full Python gate, then commit and push.**

```bash
git add docs/thread.md
git commit -m "Docs: following and catching up, and one handler per thread as a pattern"
git push origin main
```

---

### Task 8: The example — `customer_assistant` follows

**Files:**
- Modify: `examples/customer_assistant/src/server.py`
- Modify: `examples/customer_assistant/README.md` (the Description only)

**Interfaces:**
- Consumes: `Client.follow` (Task 5).

- [ ] **Step 1: Replace the invitation dance.** In `listen()`:

```python
async def listen() -> None:
    me = await server.connect(identity=ASSISTANT, scope=SCOPE)
    await me.follow()
    async for item in me.events():
        if isinstance(item, Message):
            task = asyncio.create_task(answer(me, item))
            running.add(task)
            task.add_done_callback(running.discard)
        elif isinstance(item, Cancel) and item.to.id == ASSISTANT.id and item.thread_id in stops:
            stops[item.thread_id].now()
```

Remove `Invited` from the `rfnry_thread` import if nothing else uses it. In the README's
Description, replace the mention of joining on invitation with "the assistant follows every thread
it belongs to (`me.follow()`)".

- [ ] **Step 2: Run it live on a private port.**
  - **Start it:** from `examples/customer_assistant`:
    `uv run --env-file /home/frndvrgs/software/rfnry/.env.examples uvicorn --app-dir src main:app --port 8302`.
  - **Drive it:** with a scratch Python client that is never committed, sending `Cookie: name=Dana`
    and `Origin: http://localhost:8000` (the route checks both):
    1. `POST /threads`;
    2. connect to `/ws` and join the thread;
    3. send "Where is my order A-1001?";
    4. see `stream.start`, the steps, and the assistant's message.
  - **Stop it:** stop the server by its PID.
  - **Expected:** the assistant answers a thread created after it started.

- [ ] **Step 3: Gates.** From `examples/customer_assistant`: `uv run ruff format src` and
  `uv run ruff check src`. From `packages/python`:
  `uv run pytest -q tests/unit/test_docs_examples.py`.

- [ ] **Step 4: Commit and push.**

```bash
git add examples/customer_assistant
git commit -m "Examples: customer_assistant's assistant follows its threads instead of joining on invitations"
git push origin main
```

---

### Task 9: AIHelpdesk adopts follow and marks (local branch, never pushed)

**Files:**
- Modify: `/home/frndvrgs/software/rfnry/AIHelpdesk/backend_rfnry/app/assistant/helpdesk.py`
- Modify: `/home/frndvrgs/software/rfnry/AIHelpdesk/backend_rfnry/tests/test_assistant.py`
- Modify: `/home/frndvrgs/software/rfnry/AIHelpdesk/backend_rfnry/README.md` and `/home/frndvrgs/software/rfnry/AIHelpdesk/comparison.md` (only the sentences about start-up joins)
- Modify: `/home/frndvrgs/software/rfnry/pilots/airfilterbuy-cs.md` and `/home/frndvrgs/software/rfnry/pilots/ops-maintenance-assistant.md` (local, untracked files)

**Interfaces:**
- Consumes: `Client.follow(resume=True)` and `Client.mark` (Task 5).
- Produces: none for later tasks.

- [ ] **Step 1: Write the failing test** in `backend_rfnry/tests/test_assistant.py`. A sent reply
  heard while no assistant was listening is labeled on the next start:

```python
def test_a_reply_sent_while_the_assistant_was_down_is_labeled_on_the_next_start(
    db_session, customer, agent
):
    models = [Scripted()]
    ticket = TICKET | {"opened_by_id": customer.id}

    async def scenario():
        storage = rfnry_thread.InMemoryStorage()
        refinery = Refinery(rfnry_engine.InMemoryStorage())
        memory = Memory(rfnry_memory.InMemoryStorage(), embedding=HashEmbedding())
        await memory.initialize()
        shared = _shared(db_session)

        def helpdesk(server):
            engine = Engine(
                AGENTS, generation=models[0], tools=catalog_tools(shared), refinery=refinery
            )
            return Helpdesk(server, engine, memory, refinery, Drafts(shared))

        first = Server(storage)
        async with helpdesk(first) as one:
            await one.opened(ticket, customer, "Which PSU?")
            await one.admit(1, agent)
            await one.draft(1, "PSU size", agent)

            async def drafted():
                events = await one.me.history(drafts_thread(1), limit=500)
                return [e for e in events if e.kind == "message"]

            [draft] = await _eventually(drafted)
        staff = await first.connect(identity=identity(agent), scope=SCOPE)
        await staff.send(ticket_thread(1), "Happy to help.", metadata={"run_id": draft.metadata["run_id"]})
        await staff.close()
        async with helpdesk(Server(storage)):

            async def labeled():
                return await refinery.storage.labels(SCOPE, run_ids=[draft.metadata["run_id"]])

            [label] = await _eventually(labeled)
            return label.body

    body = asyncio.run(scenario())
    assert body["review"]["stance"] == "approval"
```

- [ ] **Step 2: Run it to see it fail.** From `backend_rfnry`:
  `uv run pytest -q tests/test_assistant.py -k down_is_labeled`. Expected: FAIL, timed out
  waiting, because a fresh assistant joins live and never sees the reply.

- [ ] **Step 3: Adopt follow** in `helpdesk.py`.
  - **`__aenter__`:** replace the loop over `self.drafts.unresolved()` with
    `await self.me.follow(resume=True)`.
  - **Remove:**
    - `Drafts.unresolved`;
    - `_follow`, with its calls in `changed` and `draft`;
    - the `UNRESOLVED` constant if nothing else uses it;
    - the `Invited` branch of `_listen`;
    - the now-unused imports (`TicketRepository`, `TicketStatus`).
  - **`_listen`:** after handling an item that has a `seq`, mark it:

```python
    async def _listen(self) -> None:
        async for item in self.me.events():
            try:
                if isinstance(item, Message) and "run_id" in item.metadata:
                    await self._label(item)
                elif isinstance(item, Cancel) and item.to.id == ASSISTANT.id:
                    if item.thread_id in self.stops:
                        self.stops[item.thread_id].now()
                thread_id, seq = getattr(item, "thread_id", None), getattr(item, "seq", None)
                if thread_id is not None and seq:
                    await self.me.mark(thread_id, seq)
            except Exception:
                logger.exception("the assistant could not handle a %s", type(item).__name__)
```

  - **The module docstring:** replace its paragraph beginning "Every backend process runs this
    assistant" with:

```text
Every backend process runs this assistant on the same database. It follows every thread it belongs
to and marks each event once handled, so after a restart it resumes from its marks and a reply
stored while it was down is still labeled. Every process hears the same replies, and each writes
the reply's label under the reply's own id, so the refinery stores it once. A draft runs only in the
process asked for it; ``assistant_drafts`` tracks it until it ends, so one cut off by a crash is
reported as interrupted instead of waiting forever.
```

- [ ] **Step 4: Run the backend suite.** From `backend_rfnry`: `uv run pytest -q`,
  `uv run ruff format app tests` and `uv run ruff check app tests`. Expected: every test passes
  (73 before this task, plus the new one).

- [ ] **Step 5: Update the docs** (neutral wording; "the original").
  - **`backend_rfnry/README.md`:** the "One process" bullet says the assistant follows its threads
    and resumes from its marks, instead of joining open tickets at start-up.
  - **`comparison.md`:** in "What it improves", the exactly-once bullet gains one sentence: "A
    reply stored while every assistant was down is labeled when one comes back: the assistant
    resumes each thread from its own read mark." Change no numbers.
  - **Check:** `git diff dev -- backend frontend README.md` is empty.

- [ ] **Step 6: Update the pilot specs** (local files, outside any repo):
  - **`airfilterbuy-cs.md`:** the recorder uses `follow(resume=True)` and `mark` instead of its
    own cursor table.
  - **`ops-maintenance-assistant.md`:** the per-thread advisory lock is the documented
    "One handler per thread" pattern of `docs/thread.md`.

- [ ] **Step 7: Commit locally.** Never push.

```bash
cd /home/frndvrgs/software/rfnry/AIHelpdesk
git add backend_rfnry comparison.md
git commit -m "feat(rfnry): the assistant follows its threads and resumes from its marks"
```
