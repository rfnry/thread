# rfnry-thread — design

Written 2026-09-24 from the brainstorm of the communication cycle (approved decision by decision in
conversation). A from-scratch rewrite of rfnry-thread (old repo `../rfnry-thread`, ~17.9k lines of
source across three packages). This document says what is kept; everything not named here is
stripped. Depends on the family content parts (engine `docs/plans/2026-09-23-content-parts-design.md`,
landed in rfnry-engine and rfnry-memory).

## 1. What the library is

rfnry-thread is realtime conversation where people and AI assistants are equal participants with the
same API. Anyone in a thread — a person in a browser, an assistant on another machine, an assistant
in the server's own process — sends, streams, signals and reads through one set of verbs, and may
act first when it has a reason to. Four directions are first-class because nothing distinguishes
them: user↔user, user↔assistant, assistant↔assistant, and an assistant starting a conversation.

The library is a WebSocket protocol, a Python server, a Python client and a TypeScript client. It
stores the conversation durably, fans it out across replicas, and delivers it at least once and in
order. It imports no web framework, ships no model, and knows nothing about what an assistant is
thinking.

### Not in scope (stripped from the old library)

REST endpoints and the FastAPI dependency; server-side handler decorators and the handler registry;
runs, the run watchdog and failure codes; the tool-call, tool-result and tool-manifest events;
delivery receipts; mention parsing; LLM transcript helpers; read-model folds beyond `resolve()`;
auth caching, the insecure default authenticator and the origin allowlist; multi-key namespaces and
wildcard matching; the consumer-backed pub/sub and presence-store protocols; telemetry; the content
codec; bulk clearing of a thread; the React package (hooks, provider, stores, error boundary,
upload helper); the benchmark.

## 2. Family shapes (copied, never imported)

Byte-identical blocks shared with the other family libraries, pinned by a `test_family.py` that
compares against sibling checkouts when present:

- **Content parts**: `Text`, `Image`, `Audio`, `Document`, `Part`, `Content` (from engine/memory).
- **`Tool` / `Tools`**: the family tool block, for `ThreadTools`.
- **`Identity`** (new, shared with rfnry-senses):

```python
class Identity(BaseModel):
    role: Literal["user", "assistant", "system"]
    id: str
    name: str | None = None
    metadata: dict[str, Any] = {}
```

One class; `role` distinguishes. `"system"` is reserved for events the library itself authors.

Errors: root `ThreadException`, every child ends in `Error`: `NotFoundError`, `InputError`,
`ConfigurationError`, `ForbiddenError`, `StorageError`, `TransportError` (the names avoid Python's
builtin `PermissionError` and `ConnectionError`). Messages follow rule 9: lowercase, prefixed with
the module's file stem.

## 3. Tenancy

`scope: str` — the same word and shape as the engine: one opaque string, required, first where a
verb takes it. A connection belongs to one scope (returned by `authenticate`); every thread belongs
to one scope; nothing reads across scopes. `delete_scope(scope)` and `verify_absent(scope)` exist
for erasure, as in the engine.

## 4. The data model

```python
class Thread(BaseModel):
    id: str
    scope: str
    metadata: dict[str, Any] = {}
    created_at: datetime

class Member(BaseModel):
    thread_id: str
    identity: Identity
    added_at: datetime
    added_by: Identity
```

**Events** — the durable, append-only log of a thread. Every event carries the envelope:

```
id, thread_id, seq (room-local, gapless, assigned by the server), author: Identity,
created_at, client_id (the author's idempotency key), in_reply_to?: str, triggered_by?: Identity
```

The server stamps `id`, `seq`, `author` (from the connection's identity — never trusted from the
client) and `created_at`. Kinds:

| kind | fields | meaning |
|---|---|---|
| `message` | `content: list[Part]` | the conversation itself |
| `step` | `name, input: dict, output: list[Part], is_error, duration_ms` | one thing an assistant did — a tool, a skill, a memory lookup, a delegation, a command. Shaped like the engine's `Step` |
| `edit` | `target, content: list[Part]` | a correction of the author's own message |
| `retract` | `target` | "unsend": a tombstone; the log keeps its `seq` |
| `read` | `up_to_seq` | a member's read cursor |
| `member_added` / `member_removed` | `identity` | written by `manage` |
| `thread_updated` | `metadata` | written by `manage` |
| `custom` | `kind, payload` | the escape hatch; payload capped (64 KiB) |

`step` events and the reply that follows share `in_reply_to` (the message that started the turn),
so a UI folds them under the reply. What an assistant posts as steps is its product's choice.

**Media.** Parts with inline `data` are allowed under a cap (default 256 KiB per event,
`Server(max_inline_bytes=)`); larger media is an `InputError` telling the sender to upload and send a
`url`. Uploads are the consumer's (their storage, their endpoint); the library carries URLs and never
fetches them.

**`resolve(events) -> list[Event]`** — one public function in each client: applies edits and
retractions so every UI and every assistant sees the same current view.

## 5. Signals

A **signal** is ephemeral: fanned out to a thread's connected members, never stored.

| signal | fields | notes |
|---|---|---|
| `working` | `label?: str` | an assistant (or person: "typing") is active; `label` carries the live step ("Reading rules/refunds.md") |
| `idle` | — | ends `working` |
| `stream.start` / `stream.delta` / `stream.end` | `stream_id, index, text` | text streaming; the durable `message` that finishes a stream uses `client_id = stream_id`, so clients replace the streamed bubble; an abandoned stream leaves nothing stored |
| `cancel` | `to: Identity` | asks an assistant to stop; its own loop decides what stopping means |
| `present` / `left` | `identity` | emitted automatically when a member's connection joins or leaves a thread |

**Latest-state cache.** Every replica holds, per thread and sender, the latest `working` state, each
open stream's accumulated text, and presence. A connection joining a thread receives that state
first, then live signals. Entries clear when the sender's connection drops; a crashed replica's
entries expire after a short TTL checked on read — no background sweep.

Only text streams; media arrives in the final durable event.

## 6. Delivery

- **Client → server: at least once.** Both clients keep an outbox: per-thread FIFO, retries with
  backoff until acknowledged or a deadline; the server deduplicates on `(thread_id, author.id,
  client_id)` and answers a duplicate with the stored event.
- **Server → client:** ordered per connection; no acks on pushes. Completeness comes from `seq`.
- **Resume, one policy.** A client resumes from the last `seq` it saw: the server replays up to a cap
  (default 500); beyond it the client receives the latest page with `truncated: true` and backfills
  with `history(thread_id, before=seq)` if it wants.
- **Backpressure.** A connection's outbound queue that stays over its bound with no completed write
  for a stall window is closed; the client reconnects and resumes from its cursor. Stream deltas
  queued for a slow connection are merged, never dropped.

## 7. Distribution across replicas

Built in, no infrastructure beyond the Postgres the consumer already provides:

- `PostgresStorage.append_event` commits the event and `NOTIFY thread_events, '<scope>|<thread_id>|<seq>'`
  in the same transaction. Every replica `LISTEN`s on one dedicated pool connection, reads the event
  by `seq`, and pushes it to its own connected members.
- Signals are delivered to the sender's replica first and mirrored to others as `NOTIFY` payloads
  (under Postgres's 8 KB limit; a delta is small). Each replica drops its own mirrored frames by an
  origin id.
- Ordering and completeness come from storage and `seq`, never from the bus; a missed notification
  is recovered by the client's resume.
- `InMemoryStorage` fans out in-process: correct for one replica and for tests.
- The docs state the cost: `LISTEN` holds one connection per replica.

## 8. Storage

The family shape (engine, memory):

- A `Storage` protocol; `scope` is the first argument of every method.
- `InMemoryStorage` and `PostgresStorage(pool)` — an asyncpg-compatible pool is passed in; the
  library imports no driver. Extra: `rfnry-thread[postgres]` pulls asyncpg for convenience.
- Tables prefixed `thread_` (`thread_threads`, `thread_members`, `thread_events`), so one database and
  one pool serve `engine_*`, `memory_*` and `thread_*`.
- One storage test suite, parametrized over both backends; the two-tenant canary.

Verbs: `create_thread` (get-or-create by caller-supplied id — idempotent creation), `get_thread`,
`list_threads`, `update_thread`, `delete_thread`, `add_member`, `remove_member`, `list_members`,
`is_member`, `append_event` (dedup, room-local `seq`), `list_events(since, before, limit, kinds)`,
`get_event`, `delete_scope`, `verify_absent`.

## 9. The protocol

One WebSocket, JSON text frames, five frame types: `request{id, method, payload}`,
`response{id, payload}`, `error{id, error{code, message}}`, `push{name, payload}`,
`notify{method, payload}` (fire-and-forget, e.g. `stream.delta`). A `hello{protocol, auth}` frame
opens the connection within a deadline. Protocol version 1, pinned by golden fixtures shared by the
Python and TypeScript test suites.

Methods: `thread.create`, `thread.get`, `thread.list`, `thread.update`, `thread.delete`,
`thread.join` (with `since`), `thread.leave`, `thread.history`, `member.add`, `member.remove`,
`member.list`, `event.send` (message, step, edit, retract, read, custom), `signal.send`.
Pushes: `event`, `signal`, `state` (the latest-state snapshot on join), `thread.updated`,
`thread.deleted`, `member.added`, `member.removed`, `invited` (a member added to a thread you are not
joined to — lets a proactive assistant's counterpart discover the thread).

## 10. The server

```python
server = Server(storage, authenticate=authenticate, authorize=None, max_inline_bytes=256 * 1024)
await server.serve(connection)          # connection: receive_text(), send_text(), close()
client = server.connect(identity, scope)   # an in-process client, same class interface
await server.create_thread(scope, thread_id=None, metadata=None)
await server.add_member(scope, thread_id, identity)
await server.delete_thread(scope, thread_id)
await server.delete_scope(scope); await server.verify_absent(scope)
await server.start(); await server.stop()   # LISTEN on Postgres; nothing on in-memory
```

- **`authenticate(hello) -> (Identity, scope) | None`** — required, no default. The library never sees
  how credentials work.
- **Membership** is enforced by construction on every verb: only members read, write, signal or see
  presence.
- **`authorize(identity, action, thread) -> bool`** — optional; default "members may do everything".
  Five actions: `create`, `read`, `send`, `manage` (members and thread metadata), `delete`.
- `serve` works with any framework: a FastAPI/Starlette/`websockets` socket adapts in a few lines
  (shown in the docs and examples).

## 11. The clients

**One client interface, two ways to reach it.** The Python `Client` speaks the protocol over a
WebSocket; `server.connect(identity, scope)` returns an in-process object with the same interface.
An assistant written once runs in-process or remote, unchanged.

```python
client = await Client.connect(url, hello={"token": "..."})
thread = await client.create_thread(thread_id=None, metadata={})
await client.join(thread_id, since=None)
await client.send(thread_id, [Text("hello")], in_reply_to=None, triggered_by=None)
async with client.stream(thread_id, in_reply_to=...) as s:
    await s.write("partial ")
await client.step(thread_id, name="search", input={...}, output=[Text("...")], in_reply_to=...)
await client.working(thread_id, label="Reading rules/refunds.md"); await client.idle(thread_id)
await client.cancel(thread_id, to=identity)
await client.read(thread_id, up_to_seq=812)
await client.edit(thread_id, target, [Text("fixed")]); await client.retract(thread_id, target)
await client.add_member(thread_id, identity); await client.remove_member(thread_id, identity)
await client.history(thread_id, before=seq, limit=50)
await client.present(thread_id)
async for item in client.events():       # events and signals of joined threads, typed
    match item: ...
```

- An assistant does not receive its own events by default (`events(include_own=False)`); loop
  prevention is the consumer's logic, not a depth guard.
- Reconnect: jittered backoff; every attempt re-sends `hello` (tokens can refresh); resume per §6.

**TypeScript `@rfnry/thread`** — the same verbs, framework-free: `connect({url, hello})`,
`createThread`, `join`, `send`, `stream(threadId)` → `{write, finish}`, `step`, `working`, `idle`,
`cancel`, `read`, `edit`, `retract`, `history`, `present`, `subscribe(listener) -> unsubscribe`,
`resolve(events)`. Outbox and resume as in Python. No React, no stores, no hooks.

## 12. ThreadTools

`ThreadTools(client, thread_id=, triggered_by=)` — a family `Tools` pinned to one thread and one
cause, mounted on an engine run like `MemoryTools`:

| tool | does |
|---|---|
| `send(text)` / `send(parts)` | post a message to the thread |
| `read_recent(limit)` | the latest resolved messages and steps, as parts |
| `working(label)` / `idle()` | show what the assistant is doing |

Posting the engine's steps into the thread is consumer glue (a few lines over `on_event`), shown in
an example — the engine and thread never import each other.

## 13. Package and layout

`packages/python` (`rfnry-thread`: server + client + storage + protocol) and `packages/typescript`
(`@rfnry/thread`). `docs/thread.md` is the contract, with the family heading skeleton (Philosophy ·
Models · Getting started · Core features · Tools · Storage · Tenancy · At a glance · Errors); a docs-
examples test runs every Python block. Examples: standalone projects like the engine's (a person and
an assistant; two assistants; a proactive assistant opening a thread; streaming with steps; two
replicas over Postgres; an engine agent in a thread via ThreadTools).

## 14. Tests

Three tiers as in the family: unit (in-memory storage, both client kinds against an in-process
server, every verb and signal, resume, outbox dedup, authorization, membership, inline-media cap),
integration (Postgres: the storage suite on both backends, two servers sharing one database proving
cross-replica fan-out of events and signals, the two-tenant canary), e2e (the TypeScript client
against a live Python server; golden protocol fixtures shared by both suites).

## 15. Decisions

| decision | reason |
|---|---|
| WebSocket only; no REST | one protocol, one client transport per language, no web framework in a library; the consumer's REST endpoints call the server's verbs |
| `scope: str` | the engine's word and shape; composing a tenant key is consumer concern |
| One `Identity` with `role` | three subclasses carried no different fields |
| Caller-supplied thread ids | idempotent creation without a lookup path |
| No bulk clear | deleting inside an append-only log breaks resume and dedup; `delete_thread`, `delete_scope` and new threads cover the needs |
| Postgres `LISTEN/NOTIFY` for fan-out | multi-replica out of the box with the database the consumer already has; storage and `seq` carry correctness |
| Signals replace runs, watchdog, typing and stream frames | one ephemeral mechanism; no table, no background loop; the engine records the agent's work |
| Presence is two automatic signals | falls out of the signal cache; serves proactive assistants |
| `authenticate` required; membership by construction; optional `authorize` with five actions | who someone is and product policy are the consumer's; authorship, tenancy and membership are guarantees |
| Uploads by URL; inline media under a cap | large frames block the socket and inflate by a third |
| One client interface, socket or in-process | "the same API for both sides" held literally |
| `events()` stream / `subscribe()` | no registry, no dispatcher lanes, no depth guard |
| `step` is a first-class event | showing what an assistant did is core to human–AI conversation; one generic shape covers tools, skills, memory, delegation |
| `retract`, not delete, for events | keeps `seq` intact; `delete` names erasure verbs across the family |
| One Python and one TypeScript package | one library, one version, like engine and memory |
