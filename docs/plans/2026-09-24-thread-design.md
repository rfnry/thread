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
| `custom` | `name, payload` | the escape hatch; payload capped (64 KiB). `name`, not `kind`: `kind` is every event's discriminator |

Only an event's author edits or retracts it; only a `message` is edited; a `message` or `step` is
retracted.

`step` events and the reply that follows share `in_reply_to` (the message that started the turn),
so a UI folds them under the reply. What an assistant posts as steps is its product's choice.

**Media.** Parts with inline `data` are allowed under a cap (default 256 KiB per event,
`Server(max_inline_bytes=)`); larger media is an `InputError` telling the sender to upload and send a
`url`. Uploads are the consumer's (their storage, their endpoint); the library carries URLs and never
fetches them.

**`resolve(events) -> list[Event]`** — one public function in each client: applies edits and
retractions so every UI and every assistant sees the same current view: an edited message carries
its latest content, a retracted event and the `edit`/`retract` events themselves are left out.

Parts are pydantic models and take keywords: `Text(text="hello")`. Every verb that takes content
takes `Content` (`str | list[Part]`); a string is stored as one `Text`.

## 5. Signals

A **signal** is ephemeral: fanned out to the connections that joined a thread, never stored. Every
signal carries `thread_id` and `sender` (stamped by the server). Only a connection that joined a
thread may signal in it — joining checked membership, and removal ends the join — so the hot path
(`stream.delta`) needs no storage read.

| signal | fields | notes |
|---|---|---|
| `working` | `label?: str` | an assistant (or person: "typing") is active; `label` carries the live step ("Reading rules/refunds.md") |
| `idle` | — | ends `working` |
| `stream.start` / `stream.delta` / `stream.end` | `stream_id, in_reply_to?` / `stream_id, index, text` / `stream_id` | text streaming; the durable `message` that finishes a stream uses `client_id = stream_id`, so clients replace the streamed bubble; an abandoned stream leaves nothing stored. The finishing message always reaches a connection before `stream.end`, so a `stream.end` with no message before it means abandoned |
| `cancel` | `to: Identity` | asks an assistant to stop; its own loop decides what stopping means |
| `present` / `left` | — (the `sender` is who) | emitted automatically when a member's first connection joins a thread and its last one leaves or drops |
| `invited` | `thread: Thread` | server-authored, to the added identity's connections that have not joined the thread (`sender` = who added) |
| `thread.deleted` | — | server-authored, to the connections that joined the deleted thread |

**Latest-state cache.** Every replica holds, per thread and sender, the latest `working` state, each
open stream's accumulated text, and presence. A connection joining a thread receives that state
first — as signals (`present`, `working`, `stream.start` plus one `stream.delta` carrying the text so
far), then live signals. Entries clear when the sender's connection drops. A crashed replica's
entries expire on read: a TTL alone would also expire a person present for an hour, so every
started replica re-announces its own entries every 10 s and a mirrored entry not re-announced for
30 s is dropped when read — no sweep. The re-announcement also teaches a replica that started late
who is present and working.

`present(thread_id) -> list[Identity]` reads the cache: who has the thread open now.

Only text streams; media arrives in the final durable event.

## 6. Delivery

- **Client → server: at least once.** Both clients keep an outbox: per-thread FIFO, retries with
  backoff until acknowledged or a deadline; the server deduplicates on `(thread_id, author.id,
  client_id)` and answers a duplicate with the stored event.
- **Server → client:** ordered and complete per connection; no acks on pushes. The server keeps,
  per connection and joined thread, the last `seq` it pushed: a repeat is dropped, a gap is filled
  from storage before the event is pushed. Fan-out for one thread is serialized on each replica
  (one lock per thread), so a notification's order is kept.
- **Resume, one policy.** `join(thread_id, since=None)` is live from now; `since=n` replays the
  events after `n` first (`since=0`: the whole log) up to a cap (`Server(replay_limit=500)`); beyond
  it the connection receives the latest page and `Joined.truncated` is true, and the client
  backfills with `history(thread_id, before=seq)` if it wants. `join` returns
  `Joined(thread, seq, truncated)`. A reconnecting client re-joins every thread from its cursor and
  yields each re-join's `Joined` in `events()`, so a UI redraws after a truncated resume.
- **Backpressure.** A connection's outbound queue that stays over its bound with no completed write
  for a stall window is closed; the client reconnects and resumes from its cursor. Stream deltas
  queued for a slow connection are merged, never dropped.

## 7. Distribution across replicas

Built in, no infrastructure beyond the Postgres the consumer already provides:

- `PostgresStorage.append_event` commits the event and `NOTIFY thread_events, '{"s": scope, "t":
  thread_id, "q": seq}'` (JSON: ids may contain any character) in the same transaction. Every
  replica `LISTEN`s on one dedicated pool connection and, when it has a connection joined to that
  thread whose cursor is behind `seq`, reads the event and pushes it.
- Signals are delivered to the sender's replica first and mirrored to others as `NOTIFY` payloads
  (under Postgres's 8 KB limit; a delta is small). Each replica drops its own mirrored frames by an
  origin id (one per `Server`).
- The bus lives on the storage: `listen(on_event, on_signal) -> stop` and `publish(scope, thread_id,
  payload)`. `server.start()` listens and starts the re-announcement; one server alone needs
  neither. Servers sharing one `InMemoryStorage` in one process link the same way, so the unit tier
  proves cross-replica fan-out without a database.
- Ordering and completeness come from storage and `seq`, never from the bus; a missed notification
  is recovered by the client's resume.
- `InMemoryStorage` fans out in-process: correct for one process, and for tests of several servers.
- The docs state the cost: `LISTEN` holds one connection per replica.

## 8. Storage

The family shape (engine, memory):

- A `Storage` protocol; `scope` is the first argument of every method.
- `InMemoryStorage` and `PostgresStorage(pool)` — an asyncpg-compatible pool is passed in; the
  library imports no driver. Extra: `rfnry-thread[postgres]` pulls asyncpg for convenience.
- Tables prefixed `thread_` (`thread_threads`, `thread_members`, `thread_events`), so one database and
  one pool serve `engine_*`, `memory_*` and `thread_*`.
- One storage test suite, parametrized over both backends; the two-tenant canary.

Verbs: `initialize`, `create_thread` (get-or-create by caller-supplied id — idempotent creation),
`get_thread`, `list_threads(scope, member_id, before, limit)`, `update_thread`, `delete_thread`,
`add_member`, `remove_member`, `list_members`, `is_member`, `append_event` (dedup, room-local `seq`;
returns the stored event and whether it is new), `list_events(since, before, limit, kinds)`,
`get_event`, `delete_scope -> int` (rows removed), `verify_absent -> bool`; and the bus, `listen`
and `publish` (§7). `initialize` and `listen` are the only methods without `scope`: neither reads a
row. The room-local, gapless `seq` is a `last_seq` column on `thread_threads`, bumped under the
thread row's lock in the event's transaction.

## 9. The protocol

One WebSocket, JSON text frames, five frame types: `request{id, method, payload}`,
`response{id, payload}`, `error{id, error{code, message}}`, `push{name, payload}`,
`notify{method, payload}` (fire-and-forget, e.g. `stream.delta`). A `hello{protocol, auth}` frame
opens the connection within 10 s; it is answered by `response{id: "hello", payload: {identity,
scope}}` or `error{id: "hello"}` and a close. Error codes map one to one onto the error classes
(`not_found`, `input`, `forbidden`, `configuration`, `storage`, `transport`, and `internal` for the
root). Close codes: 4000 protocol error, 4001 refused hello, 4002 hello deadline, 4003 too slow.
Protocol version 1, pinned by golden fixtures shared by the Python and TypeScript test suites.

Methods: `thread.create`, `thread.get`, `thread.list`, `thread.update`, `thread.delete`,
`thread.join` (with `since`), `thread.leave`, `thread.history`, `member.add`, `member.remove`,
`member.list`, `thread.present`, `event.send` (message, step, edit, retract, read, custom),
`signal.send` (as a request for `working`, `idle`, `cancel`, `stream.start`, `stream.end`; as a
notify for `stream.delta`). Pushes: `event` and `signal` — nothing else. The latest-state snapshot
arrives as signals; `invited` (a member added to a thread you are not joined to — lets a proactive
assistant's counterpart discover the thread) and `thread.deleted` are signal kinds; metadata and
member changes are events already.

## 10. The server

```python
server = Server(storage, authenticate=authenticate, authorize=None, max_inline_bytes=256 * 1024,
                replay_limit=500)
await server.serve(connection)          # Connection: receive_text(), send_text(), close()
client = await server.connect(identity, scope)   # an in-process Client — the same class
await server.create_thread(scope, thread_id=None, metadata=None)
await server.add_member(scope, thread_id, identity)
await server.delete_thread(scope, thread_id)
await server.delete_scope(scope); await server.verify_absent(scope)
await server.start(); await server.stop()   # LISTEN on Postgres; nothing on in-memory
```

- **`authenticate(hello) -> (Identity, scope) | None`** — async, required, no default; `hello` is the
  dict the client sent. The library never sees how credentials work. A `system` identity from it (or
  given to `server.connect`) is a `ConfigurationError`: that role is the library's.
- **Membership** is enforced by construction on every verb: only members read, write, signal or see
  presence.
- **`authorize(identity, action, thread) -> bool`** — optional; default "members may do everything".
  Five actions: `create`, `read`, `send`, `manage` (members and thread metadata), `delete`.
- `serve` works with any framework: a FastAPI/Starlette/`websockets` socket adapts in a few lines
  (shown in the docs and examples).

## 11. The clients

**One client, two ways to reach it.** `Client.connect(url, hello=)` speaks the protocol over a
WebSocket (the `websockets` package, a dependency of the Python package); `await
server.connect(identity, scope)` returns the same `Client` class over an in-process pair of queues
that runs the very same session code, frames and all, with authentication skipped. An assistant
written once runs in-process or remote, unchanged, and every guarantee is tested once.
`hello` is a dict or an async function returning one, called on every attempt so tokens refresh.

```python
client = await Client.connect(url, hello={"token": "..."})
thread = await client.create_thread(thread_id=None, metadata={})
await client.get_thread(thread_id); await client.list_threads(before=None, limit=50)
await client.update_thread(thread_id, metadata); await client.delete_thread(thread_id)
joined = await client.join(thread_id, since=None); await client.leave(thread_id)
await client.send(thread_id, "hello", in_reply_to=None, triggered_by=None)
async with client.stream(thread_id, in_reply_to=...) as s:
    await s.write("partial ")           # s.finish(content) stores other content; an exception abandons
await client.step(thread_id, name="search", input={...}, output=[Text("...")], in_reply_to=...)
await client.working(thread_id, label="Reading rules/refunds.md"); await client.idle(thread_id)
await client.cancel(thread_id, to=identity)
await client.read(thread_id, up_to_seq=812)
await client.edit(thread_id, target, [Text("fixed")]); await client.retract(thread_id, target)
await client.custom(thread_id, name, payload)
await client.add_member(thread_id, identity); await client.remove_member(thread_id, identity)
await client.list_members(thread_id)
await client.history(thread_id, before=seq, limit=50, kinds=None)
await client.present(thread_id)          # -> list[Identity]: who has the thread open
async for item in client.events():       # events and signals of joined threads, typed
    match item: ...
```

- An assistant does not receive its own events by default (`events(include_own=False)`); loop
  prevention is the consumer's logic, not a depth guard.
- Reconnect: jittered backoff; every attempt re-sends `hello` (tokens can refresh); resume per §6.

**TypeScript `@rfnry/thread`** — the same verbs, framework-free: `connect({url, hello})`,
`createThread`, `getThread`, `listThreads`, `updateThread`, `deleteThread`, `join`, `leave`,
`send`, `stream(threadId)` → `{write, finish, abort}`, `step`, `working`, `idle`, `cancel`, `read`,
`edit`, `retract`, `custom`, `history`, `present`, `addMember`, `removeMember`, `listMembers`,
`subscribe(listener, {includeOwn}) -> unsubscribe`, `close`, `resolve(events)`. Events and signals
are the wire shapes (snake_case fields); options are camelCase. Outbox and resume as in Python. No
React, no stores, no hooks, no runtime dependencies (the platform `WebSocket`).

## 12. ThreadTools

`ThreadTools(client, thread_id=, triggered_by=None, in_reply_to=None)` — a family `Tools` pinned to
one thread and one cause, mounted on an engine run like `MemoryTools`. `in_reply_to` files what the
model sends under the turn that started it:

| tool | does |
|---|---|
| `send(text)` | post a message to the thread (text only: a model cannot author bytes, and a url it has is text) |
| `read_recent(limit)` | the latest resolved messages and steps, as parts: per event a `Text` of its `seq`, author and kind as JSON, then its content as sent — no transcript formatting |
| `working(label)` / `idle()` | show what the assistant is doing |

Posting the engine's steps into the thread is consumer glue (a few lines over `on_event`), shown in
an example — the engine and thread never import each other.

## 13. Package and layout

`packages/python` (`rfnry-thread`: server + client + storage + protocol; dependencies `pydantic`
and `websockets`, extra `postgres` = asyncpg) and `packages/typescript` (`@rfnry/thread`: TypeScript
strict, `tsc` build, vitest, Biome; no runtime dependencies). `docs/thread.md` is the contract, with the family heading skeleton (Philosophy ·
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
| `server.connect` returns the same `Client` over an in-process pipe | one class, one session path; every guarantee tested once |
| Two push names, `event` and `signal` | the join snapshot is signals, invitations and deletion are signals, member and metadata changes are events |
| Completeness enforced by the server per connection | a gap is filled before a push; the client never has to detect one |
| Replicas re-announce their live state | a TTL without refresh would expire a long presence; the refresh also heals a missed `idle` and teaches a late replica |

## 16. Revisions from writing the docs (2026-09-24)

Writing `docs/thread.md` against this design exposed these gaps; the text above is fixed:

1. `server.connect` is async and returns the same `Client` class (in-process pipe through the same
   session code), not a look-alike.
2. `authenticate(hello)` receives the client's `hello` dict; `hello` may be an async function so a
   token refreshes on reconnect.
3. Pushes are `event` and `signal` only; the `state` snapshot is delivered as signals; `invited` and
   `thread.deleted` are signal kinds; the `thread.updated`/`member.*` pushes duplicated events and
   are gone.
4. Every signal carries `thread_id` and `sender`; `present`/`left` use `sender` instead of an
   `identity` field.
5. The `custom` event's own field is `name` (`kind` is the discriminator).
6. `join` returns `Joined(thread, seq, truncated)`; `since=None` is live-only, `since=0` the whole
   log; automatic re-joins yield their `Joined` in `events()`.
7. Server → client completeness is the server's job (per-connection cursor, gap fill, per-thread
   serial fan-out); this yields the guarantee that a stream's message precedes its `stream.end`.
8. Latest-state expiry needs a re-announcement (10 s, expiry 30 s), or presence would expire.
9. `NOTIFY` payloads are JSON; the bus (`listen`, `publish`) is on the storage.
10. Signals require the connection to have joined the thread.
11. `present(thread_id)` is a read (`thread.present`), not a send.
12. The client gains the verbs its protocol already had: `get_thread`, `list_threads`,
    `update_thread`, `delete_thread`, `leave`, `list_members`, `custom`.
13. Parts take keywords (`Text(text=...)`); content verbs take `Content`, a string included.
14. `ThreadTools.send` takes text only; `ThreadTools(in_reply_to=)` added; `read_recent`'s shape is
    defined.
15. Storage gains `initialize`; `delete_scope -> int`, `verify_absent -> bool` as in the engine.
16. The Python socket client depends on `websockets`.
17. Python `Stream.finish(content)`; TypeScript `abort()`.
18. Edit/retract rules: author only; edit messages; retract messages and steps.
19. Error codes and close codes are named (§9).
