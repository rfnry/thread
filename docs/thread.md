# rfnry-thread

rfnry-thread is realtime conversation where people and AI assistants are equal participants with
the same API.

A **thread** is a conversation among its **members** — people in browsers, assistants on other
machines, assistants in the server's own process. Everything said in it is an **event** in a
durable, ordered log: messages, the steps an assistant took, edits, retractions, read cursors.
Everything that is only true right now is a **signal**, fanned out to whoever is connected and
never stored: someone is working, a reply is streaming, a member arrived or left. Every
participant sends, streams, signals and reads through one set of verbs, and any of them may speak
first. One WebSocket protocol, a Python server and client, a TypeScript client.

## Philosophy

**Everyone is a participant.** Nothing in the protocol distinguishes a person from an assistant:
the same client, the same verbs, the same events. A person can write to a person, a person to an
assistant, an assistant to an assistant, and an assistant can open a conversation on its own. An
assistant written once runs in the server's process or on another machine, unchanged.

**The library carries; it does not take over.** You bring authentication, the storage connection,
the web framework, uploads and any HTTP endpoints of your own. There is no handler registry, no
dispatcher, no configuration language. The server serves a socket you accepted; the client hands
you a stream of what happened.

**The log is the truth.** Ordering and completeness come from storage: every event has a
thread-local `seq`, gapless, assigned by the server. Delivery is at least once from clients and in
order to them, and a client that was away resumes from the last `seq` it saw. Nothing the network
drops is lost.

**What an assistant does is part of the conversation.** A step — a tool call, a skill, a memory
lookup, a delegation — is an event like a message, filed under the reply it led to. What an
assistant shows is its product's choice; the thread carries it.

**Tenancy by construction.** Every connection and every thread names its scope. Every row carries
it. There is no default scope and nothing reads across scopes. Authorship is stamped by the server
from the authenticated connection, never trusted from a client.

## Models

The shapes a consumer meets are an identity, parts, events and signals.

An **identity** is anyone who takes part. One class; `role` tells them apart, and `"system"` is
reserved for what the library itself authors:

```python
Identity(role="user", id="ana", name="Ana")
Identity(role="assistant", id="helper", name="Helper", metadata={"agent": "support"})
```

**Parts** are the content of a message — the family's one vocabulary, shared byte for byte with
rfnry-engine and rfnry-memory, so a message's parts reach a model without translation:

```python
Text(text="Is this invoice charged twice?")
Image(media_type="image/png", url="https://files.example.com/receipt.png")
Document(media_type="application/pdf", name="invoice.pdf", url="https://files.example.com/invoice.pdf")
Audio(media_type="audio/wav", data=wav)
```

`Part` is any of the four and `Content` is `str | list[Part]`; a string is sent as one `Text`. A
media part carries exactly one of `data` (bytes, base64 on the wire) or `url`. Inline bytes are
capped per event (`Server(max_inline_bytes=256 * 1024)`); larger media is uploaded by your own
endpoint to your own storage and sent as a `url`. The library carries a url and never fetches it.

An **event** is a row in a thread's log. Every event carries `id`, `thread_id`, `seq`, `author`,
`created_at`, `client_id` (the sender's idempotency key), and optionally `in_reply_to` (an event
id) and `triggered_by` (the identity that caused it, as the sender declares). The server stamps
`id`, `seq`, `author` and `created_at`. The kinds:

| event | fields | meaning |
| --- | --- | --- |
| `Message` | `content` | the conversation itself |
| `Step` | `name`, `input`, `output`, `is_error`, `duration_ms` | one thing an assistant did |
| `Edit` | `target`, `content` | a correction of the author's own message |
| `Retract` | `target` | "unsend": the log keeps its `seq` |
| `Read` | `up_to_seq` | a member's read cursor |
| `MemberAdded` / `MemberRemoved` | `identity` | who joined or left the membership |
| `ThreadUpdated` | `metadata` | the thread's new metadata |
| `Custom` | `name`, `payload` | yours; the payload is capped at 64 KiB |

`Event` is any of them. `resolve(events)` applies edits and retractions and returns the current
view — the same view in every UI and every assistant:

```python
events = await ana.history(thread.id)
view = resolve(events)
```

A **signal** is ephemeral. Every signal carries `thread_id` and `sender`, stamped by the server:

| signal | fields | meaning |
| --- | --- | --- |
| `Working` | `label` | the sender is busy — an assistant's live step, a person typing |
| `Idle` | — | ends `Working` |
| `StreamStart` / `StreamDelta` / `StreamEnd` | `stream_id`, `in_reply_to` / `index`, `text` / — | a reply being written |
| `Cancel` | `to` | asks an assistant to stop; what stopping means is its own |
| `Present` / `Left` | — | the sender opened or closed the thread |
| `Invited` | `thread` | you were added to a thread you have not joined |
| `ThreadDeleted` | — | the thread is gone |

`Signal` is any of them. `Thread` (`id`, `scope`, `metadata`, `created_at`) and `Member`
(`thread_id`, `identity`, `added_at`, `added_by`) complete the shapes.

## Getting started

```python
server = Server(InMemoryStorage(), authenticate=authenticate)

ana = await server.connect(Identity(role="user", id="ana", name="Ana"), scope="acme")
helper = await server.connect(Identity(role="assistant", id="helper", name="Helper"), scope="acme")

thread = await ana.create_thread(thread_id="support-ana")
await ana.add_member(thread.id, helper.identity)
await helper.join(thread.id)
await ana.send(thread.id, "My invoice was charged twice.")

async for item in helper.events():
    if isinstance(item, Message):
        await helper.send(thread.id, "Looking into it.", in_reply_to=item.id)
        break
```

A server over a storage, and two participants connected to it in the same process. A browser or an
assistant elsewhere reaches the same server over a WebSocket and gets the same `Client`
([Many replicas](#many-replicas) and [The TypeScript client](#the-typescript-client)).

`authenticate` is yours and required: it receives what the client sent to open the connection and
returns who that is and in which scope, or `None` to refuse. The library never sees how credentials
work:

```python
async def authenticate(hello):
    user = await users.by_token(hello.get("token"))
    return (Identity(role="user", id=user.id, name=user.name), user.org) if user else None
```

## Core features

### Serving and connecting

`server.serve(connection)` runs one WebSocket until it closes. A connection is anything with
`receive_text()`, `send_text(text)` and `close(code, reason)`, which is what Starlette and FastAPI
hand you:

```python
app = FastAPI()
server = Server(PostgresStorage(pool), authenticate=authenticate)

@app.websocket("/thread")
async def thread_socket(websocket: WebSocket):
    await websocket.accept()
    await server.serve(websocket)
```

Any other framework adapts in the same few lines. The first frame must arrive within ten seconds
and pass `authenticate`; otherwise the socket is closed.

There are two ways to get a client, and they are the same class:

```python
remote = await Client.connect("wss://app.example.com/thread", hello={"token": token})
local = await server.connect(Identity(role="assistant", id="helper"), scope="acme")
```

`Client.connect` speaks the protocol over a WebSocket; `hello` is a dict, or an async function
returning one, called on every attempt so a token can refresh. `server.connect` is in-process and
trusted — your code names the identity. Both expose `client.identity` and `client.scope` and close
with `await client.close()`.

### Threads and members

```python
thread = await ana.create_thread(thread_id="support-ana", metadata={"topic": "billing"})
await ana.get_thread(thread.id)
await ana.list_threads(limit=20)
await ana.update_thread(thread.id, {"topic": "refunds"})
await ana.add_member(thread.id, Identity(role="user", id="bruno", name="Bruno"))
await ana.list_members(thread.id)
await ana.remove_member(thread.id, Identity(role="user", id="bruno"))
```

The id is yours: `create_thread` with an id that exists returns that thread, so creating is safe to
repeat; without one the server picks it. The creator is the first member. `list_threads` returns
the threads you are a member of, newest first, `before=` a thread id for the next page. Adding and
removing members and updating metadata write `MemberAdded`, `MemberRemoved` and `ThreadUpdated` to
the log. `delete_thread` removes the thread, its members and its log, and tells whoever has it open.

A member **joins** a thread to receive it live; membership is who may, joining is who does:

```python
joined = await helper.join(thread.id, since=0)
joined.thread, joined.seq, joined.truncated
await helper.leave(thread.id)
```

Without `since`, a join is live from now. With `since`, the events after that `seq` arrive first —
`since=0` for the whole log — then what is happening right now, then live traffic. A join replays
at most 500 events (`Server(replay_limit=)`); beyond that you get the latest 500 and
`truncated=True`, and `history(thread.id, before=seq)` pages back when you want more.

### Sending and streaming

Every verb that writes returns the stored event:

```python
message = await ana.send(thread.id, [Text(text="Here is the receipt."), Image(media_type="image/png", url=url)])
await ana.edit(thread.id, message.id, "Here is the right receipt.")
await ana.retract(thread.id, message.id)
await ana.read(thread.id, up_to_seq=message.seq)
await ana.custom(thread.id, "rating", {"stars": 5})
```

Only an event's author edits or retracts it; only messages are edited. `in_reply_to=` ties an
event to the one it answers and `triggered_by=` names who caused it — a scheduled assistant can say
which person set it up, or leave it out.

Everything that arrives — events and signals of the threads you joined, and invitations — comes
from one stream:

```python
async for item in helper.events():
    match item:
        case Message(author=author) if author.role == "user":
            await answer(item)
        case Cancel(to=to) if to.id == helper.identity.id:
            stop()
        case Invited():
            await helper.join(item.thread_id)
```

Your own events and signals are left out, so an assistant does not answer itself;
`events(include_own=True)` keeps them. Deciding when not to answer — another assistant, a loop —
is your code.

A reply can stream. The text travels as signals, and the message that finishes it is stored with
`client_id` equal to the stream's id, so a UI replaces the streamed bubble with it:

```python
async with helper.stream(thread.id, in_reply_to=message.id) as reply:
    for chunk in ["The second charge ", "was refunded."]:
        await reply.write(chunk)
```

Leaving the block stores the text written as one message; `await reply.finish(content)` stores
something else instead — the text and an image, say. An exception inside the block abandons the
stream: `StreamEnd` arrives and nothing is stored. The finishing message always arrives before
`StreamEnd`, so a `StreamEnd` with no message before it means the stream was abandoned. Only text
streams; media arrives in the message.

### Steps

A step is one thing an assistant did, filed under the reply it led to by sharing its
`in_reply_to`:

```python
await helper.working(thread.id, label="Reading rules/refunds.md")
await helper.step(thread.id, name="Read", input={"path": "rules/refunds.md"},
                  output=[Text(text="A double charge is refunded in full.")], duration_ms=12,
                  in_reply_to=message.id)
async with helper.stream(thread.id, in_reply_to=message.id) as reply:
    await reply.write("Refunded in full, per the refunds rule.")
await helper.idle(thread.id)
```

`Working` shows what is happening now and is gone when the assistant is idle or disconnects; the
steps stay in the log. A UI folds the steps under the reply that shares their `in_reply_to`. Which
steps to show — every tool call, only the ones a person would care about, none — is yours.

Showing an engine run's work is a few lines of your code: an `on_event` callback that turns a text
delta into `reply.write` and a finished tool call into `client.step`. A senses pipeline does the
same with what it hears and sees. The libraries never import each other.

### Signals and presence

```python
await helper.working(thread.id, label="Checking the invoice")
await helper.idle(thread.id)
await ana.cancel(thread.id, to=helper.identity)
await helper.present(thread.id)
```

Signals go to the connections that joined the thread, and only a connection that joined may send
one. `Present` and `Left` are sent for you when a member's first connection joins the thread and
its last one leaves or drops; `present(thread_id)` lists who has it open now. A connection that
joins receives the current state first — who is present, who is working, every open stream with its
text so far — so a late arrival sees the reply that is already half written.

An assistant that speaks first creates the thread and adds the person; the person's connections
receive `Invited`:

```python
reminders = await server.connect(Identity(role="assistant", id="reminders"), scope="acme")
renewal = await reminders.create_thread(thread_id="renewal-ana-2026-10")
await reminders.add_member(renewal.id, Identity(role="user", id="ana"))
await reminders.send(renewal.id, "Your plan renews Friday. Switch to yearly and save 15%?")
```

### Delivery and resume

From a client to the server, delivery is at least once. Every write goes through an outbox: in
order per thread, retried with backoff across reconnects until the server acknowledges it or a
minute passes, then a `TransportError`. The server deduplicates on the thread, the author and
`client_id`, and answers a repeat with the event it stored the first time.

From the server to a client, delivery is in order and complete per connection: a connection never
sees `seq` 7 before `seq` 6, and never skips one. A dropped socket reconnects with jittered backoff,
sends `hello` again, and re-joins every thread it had joined from the last `seq` it saw; the missed
events arrive before anything new. A re-join also arrives in `events()` as its `Joined`, and when it
says `truncated`, redraw from the latest page. A reconnect whose `hello` is refused stops the client,
and `events()` raises that error. Items wait in the client until you read them, so an assistant that
joins and then starts reading misses nothing.

A connection that cannot keep up — its outbound queue over its bound with no write finishing for
ten seconds — is closed, and its client resumes from its cursor. Stream deltas waiting for a slow
connection are merged, never dropped.

### Many replicas

Run as many servers as you like on one Postgres database. Each one `LISTEN`s for the others and
delivers to its own connections:

```python
server = Server(PostgresStorage(pool), authenticate=authenticate)
await server.start()
await server.stop()
```

An event is stored and announced in one transaction; every replica with a connection in that
thread reads it by `seq`. Signals are delivered on the sender's replica and mirrored to the others.
Each replica keeps who is present, working and streaming, and repeats its own every ten seconds; a
replica that stops repeating — it crashed — is forgotten within thirty. A missed announcement costs
latency, never an event: resume reads from storage. `start()` holds one connection from the pool
for `LISTEN`; `stop()` gives it back. Servers sharing one `InMemoryStorage` in one process link the
same way, which is how the tests run two replicas without a database.

### Permissions

Who someone is and what your product allows are yours; who is in the thread is the library's:

- `authenticate` decides who a connection is and its scope.
- Membership is enforced on every verb: only members read, write, signal or see presence. A
  non-member gets a `ForbiddenError`.
- `authorize(identity, action, thread)`, optional, decides the rest. The actions are `create`,
  `read`, `send`, `manage` (members and metadata) and `delete`; without it, members may do
  everything.

```python
async def authorize(identity, action, thread):
    if action == "delete":
        return thread.metadata.get("owner") == identity.id
    return True

server = Server(storage, authenticate=authenticate, authorize=authorize)
```

The server's own verbs act as the system, with no member behind them — for your REST endpoints,
jobs and back office:

```python
await server.create_thread("acme", thread_id="onboarding-bruno", metadata={"owner": "bruno"})
await server.add_member("acme", "onboarding-bruno", Identity(role="user", id="bruno"))
await server.delete_thread("acme", "onboarding-bruno")
```

### The TypeScript client

`@rfnry/thread` is the same client for the browser and Node, with no framework and no
dependencies:

```ts
import { connect, resolve } from '@rfnry/thread'

const client = await connect({
  url: 'wss://app.example.com/thread',
  hello: async () => ({ token: await session.token() }),
})
const unsubscribe = client.subscribe(render, { includeOwn: true })
const joined = await client.join(threadId, { since: 0 })
const message = await client.send(threadId, 'Can you check my invoice?')
```

The verbs are the Python ones in camelCase — `createThread`, `getThread`, `listThreads`,
`updateThread`, `deleteThread`, `join`, `leave`, `history`, `present`, `addMember`, `removeMember`,
`listMembers`, `send`, `stream`, `step`, `edit`, `retract`, `read`, `custom`, `working`, `idle`,
`cancel`, `close` — with the same outbox, reconnect and resume. Events and signals are the wire
shapes, fields as they travel (`thread_id`, `in_reply_to`); options are camelCase. Listeners get
what arrives after they subscribe, so subscribe before joining. A UI usually wants its own messages
too, echoed from other tabs, hence `includeOwn`. Streaming:

```ts
const reply = client.stream(threadId, { inReplyTo: message.id })
reply.write('Refunded ')
reply.write('in full.')
await reply.finish()
```

`finish(content)` stores other content; `abort()` abandons. `client.closed` resolves when the
client stops for good — `null` after `close()`, or the error when a reconnect's hello was refused,
an expired session say. Stores, hooks and rendering are your UI's.

## Tools

`ThreadTools` gives a model a thread: a `Tools` pinned to one client, one thread and one cause.

```python
tools = ThreadTools(helper, thread_id=thread.id, triggered_by=message.author, in_reply_to=message.id)
tools.definitions()
await tools.dispatch("working", {"label": "Checking the invoice"})
await tools.dispatch("send", {"text": "The second charge is refunded."})
recent = await tools.dispatch("read_recent", {"limit": 20})
await tools.dispatch("idle", {})
```

Four tools: `send(text)` posts a message with the bound `in_reply_to` and `triggered_by`;
`read_recent(limit)` returns the latest messages and steps, resolved, as parts — for each event a
`Text` with its `seq`, author and kind as JSON, then its content as it was sent, images included;
`working(label)` and `idle()` show what the assistant is doing. The thread is fixed when the tools
are built and a model cannot name another.

`Tool` and `Tools` are the family's tool block, copied byte for byte from rfnry-engine: a tool is a
name, a description, an input schema and a body; a `Tools` combines with `+` and narrows with
`only` and `without`:

```python
mine = Tools([Tool("now", "The current UTC time", body=now)])
combined = ThreadTools(helper, thread_id=thread.id).only("send", "read_recent") + mine
```

So it mounts on an engine run as it is, beside memory's tools, and neither library imports the
other:

```python
run = await engine.run("support", scope="acme", input=message.content,
                       tools=ThreadTools(helper, thread_id=thread.id, triggered_by=message.author))
```

## Storage

Two storages ship: `InMemoryStorage()` for tests, examples and one process, `PostgresStorage(pool)`
for everything else, taking an asyncpg-compatible pool you built (`rfnry-thread[postgres]` installs
asyncpg). `initialize()` creates three tables — `thread_threads`, `thread_members`, `thread_events`
— and is safe to run again. The prefix lets one database and one pool serve rfnry-engine's and
rfnry-memory's tables too:

```python
pool = await asyncpg.create_pool(dsn)
storage = PostgresStorage(pool)
await storage.initialize()
server = Server(storage, authenticate=authenticate)
```

`Storage` is the protocol both implement; `scope` is the first argument of every method that reads
or writes a row. The log is append-only: an event is never updated or deleted on its own, only with
its thread or its scope.

## Tenancy

`scope` is the tenant: `authenticate` returns it with the identity, `server.connect` takes it, and
every thread, member and event row carries it. A connection sees only its scope; the same thread id
in two scopes names two threads. To offboard:

```python
removed = await server.delete_scope("acme")
assert await server.verify_absent("acme")
```

`delete_scope` returns how many rows it removed; `verify_absent` counts every table independently.

## At a glance

Everything the package exports, by role.

- **Serve:** `Server`, `Connection`.
- **Take part:** `Client`, `Stream`, `Joined`.
- **Who and what:** `Identity`, `Thread`, `Member`; parts `Text`, `Image`, `Audio`, `Document`, `Part`, `Content`.
- **The log:** `Message`, `Step`, `Edit`, `Retract`, `Read`, `MemberAdded`, `MemberRemoved`, `ThreadUpdated`, `Custom`, `Event`; `resolve`.
- **Right now:** `Working`, `Idle`, `StreamStart`, `StreamDelta`, `StreamEnd`, `Cancel`, `Present`, `Left`, `Invited`, `ThreadDeleted`, `Signal`.
- **Give models a thread:** `ThreadTools`, `Tool`, `Tools`.
- **Store:** `Storage`, `InMemoryStorage`, `PostgresStorage`.
- **Fail loudly:** `ThreadException`, `NotFoundError`, `InputError`, `ConfigurationError`, `ForbiddenError`, `StorageError`, `TransportError`.

The TypeScript package exports `connect`, `resolve`, the same error classes, and the types of
everything above.

## Errors

`NotFoundError` for a thread, member or event the scope does not hold. `ForbiddenError` for a
refused `hello`, a verb by a non-member, an `authorize` that says no, an edit or retraction of
someone else's event, and a signal from a connection that has not joined the thread. `InputError`
for a request the library refuses: an empty scope or id, a media part with both or neither of
`data` and `url`, inline media over the cap, a custom payload over 64 KiB, an edit of anything but a
message, an unknown event or signal kind, a malformed frame. `ConfigurationError` for a server that
cannot work: no `authenticate`, a limit that is not positive, a `system` identity from
`authenticate` or `server.connect`. `StorageError` for the database. `TransportError` for the
socket: a write not acknowledged within its deadline, a client used after `close()`, a connection
that cannot be opened. On a client, an error the server raised is raised again with its class and
message.

All under `ThreadException` — the root takes the `Exception` suffix, every child ends in `Error`,
and the names avoid Python's `PermissionError` and `ConnectionError`. Every message names the
module that raised it, then says what went wrong in lowercase: `server: thread 'support-ana' was not
found`, `server: 'bruno' is not a member of thread 'support-ana'`, `server: inline media is 300000
bytes; the cap is 262144 — upload it and send a url`. The library logs nothing but unexpected
failures inside `serve`, as warnings.
