# rfnry-thread

rfnry-thread provides conversational text interfaces for humans and machines.

It is the chat layer of an AI product, where assistants are members exactly like people — person to
person, person to assistant, assistant to assistant — through one set of verbs, and anyone may speak
first. One WebSocket protocol, a Python server and client, a TypeScript client.

A **thread** is a conversation among its **members** — people in browsers, assistants on other
machines, assistants in the server's own process. Everything said in it is an **event** in a
durable, ordered log: messages, the steps an assistant took, edits, retractions, read cursors.
Everything that is only true right now is a **signal**, fanned out to whoever is connected and
never stored: someone is working, a reply is streaming, a member arrived or left. Every
participant sends, streams, signals and reads through one set of verbs, and any of them may speak
first. One WebSocket protocol, a Python server and client, a TypeScript client.

Every public name, with its signature, is in [the API reference](api.md).

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

Every verb that takes an identity — `serve`, `server.connect`, `add_member`, `remove_member`,
`triggered_by`, `cancel(to=)`, `ThreadTools(triggered_by=)` — takes an `IdentityLike` and reads it
by its fields: any object with `role`, `id`, `name` and `metadata` will do, such as rfnry-senses'
`Identity`, which has the same source. The values are validated as strictly as an `Identity`'s; one
that does not fit is an `InputError`.

**Parts** are the content of a message — the family's one vocabulary, shared byte for byte with
rfnry-engine, rfnry-memory and rfnry-senses, so a message's parts reach a model without
translation:

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
| `Message` | `content`, `metadata` | the conversation itself; the metadata is yours |
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
| `Marked` | `seq`, `unread` | your read position in the thread moved; only your other connections hear it |
| `ThreadDeleted` | — | the thread is gone |

`Signal` is any of them. `Thread` (`id`, `scope`, `metadata`, `created_at`) and `Member`
(`thread_id`, `identity`, `added_at`, `added_by`) complete the shapes.

## Getting started

Threads live in Postgres. Start one:

```bash
docker run -d --name thread-postgres -e POSTGRES_PASSWORD=thread -p 5432:5432 postgres:16
```

```python
pool = await asyncpg.create_pool("postgresql://postgres:thread@localhost:5432/postgres")
storage = PostgresStorage(pool)
await storage.initialize()
server = Server(storage)

ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="acme")
helper = await server.connect(identity=Identity(role="assistant", id="helper", name="Helper"), scope="acme")

thread = await ana.create_thread(thread_id="support-ana")
await ana.add_member(thread.id, helper.identity)
await helper.join(thread.id)
await ana.send(thread.id, "My invoice was charged twice.")

async for item in helper.events():
    if isinstance(item, Message):
        await helper.send(thread.id, "Looking into it.", in_reply_to=item.id)
        break
```

A server over Postgres, and two participants connected to it in the same process. A browser or an
assistant elsewhere reaches the same server over a WebSocket and gets the same `Client`
([Many replicas](#many-replicas) and [The TypeScript client](#the-typescript-client)).

Who a connection is, is yours to decide. Your web framework authenticates the WebSocket upgrade —
an ordinary HTTP request, with your cookies, headers and tokens — and hands the accepted socket to
the server with who it is and in which scope. The library never sees how credentials work.

## Core features

### Serving and connecting

`server.serve(connection, *, identity, scope)` runs one WebSocket until it closes. You authenticate
the upgrade with your own framework, accept it, and say who is on the other end. A connection is
anything with `receive_text()`, `send_text(text)` and `close(code, reason)`, which is what Starlette
and FastAPI hand you:

```python
app = FastAPI()
server = Server(PostgresStorage(pool))

@app.websocket("/thread")
async def thread_socket(websocket: WebSocket, user: User = Depends(current_user)):
    await websocket.accept()
    await server.serve(websocket, identity=Identity(role="user", id=user.id, name=user.name), scope=user.org)
```

Any other framework adapts in the same few lines. Refusing a connection happens before `serve`:
answer the upgrade with HTTP 401 or 403, or accept it and close it at once with 1008, policy
violation:

```python
offered = websocket.scope.get("subprotocols", [])
await websocket.accept(subprotocol=offered[0] if offered else None)
await websocket.close(code=1008)
```

Close only after the accept, which names a subprotocol the client offered: a close before the accept
goes out as HTTP 403, and a browser sees that, or a handshake naming none of the subprotocols it
offered, only as 1006, a dropped network, so the TypeScript client would retry forever. A client
stops on a refusal, with a `ForbiddenError`. `identity` and `scope` are keyword-only, on `serve` and
`server.connect` alike. Everything the socket says afterwards is authored as that identity, in that
scope.

There are two ways to get a client, and they are the same class:

```python
remote = await Client.connect("wss://app.example.com/thread", headers={"Authorization": f"Bearer {token}"})
local = await server.connect(identity=Identity(role="assistant", id="helper"), scope="acme")
```

`Client.connect` speaks the protocol over a WebSocket; `headers` go with the upgrade, whatever your
endpoint expects. They are a dict, or an async function returning one, called on every attempt so a
token can refresh. `server.connect` is in-process and trusted — your code names the identity. Both
expose `client.identity` and `client.scope`, as the server knows them, and close with
`await client.close()`.

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

The id is yours: `create_thread` with an id that exists returns that thread when you are a member
and may `read` it, so creating is safe to repeat, and raises `ForbiddenError` when you are not or
may not; without one the server picks it. The creator is the first member. `list_threads` returns
the threads you are a member of, newest first, `before=` a thread id for the next page; with an
`authorize`, the threads it refuses to `read` are skipped, a page still holds up to `limit` threads
you may read, and an empty page means there are no more. Adding and removing members and updating
metadata write `MemberAdded`, `MemberRemoved` and `ThreadUpdated` to the log. `delete_thread`
removes the thread, its members and its log, and tells whoever has it open.

A member **joins** a thread to receive it live; membership is who may, joining is who does:

```python
joined = await helper.join(thread.id, since=0)
joined.thread, joined.seq, joined.truncated
await helper.leave(thread.id)
```

Without `since`, a join is live from now. With `since`, the events after that `seq` arrive first —
`since=0` for the whole log — then what is happening right now, then live traffic. A join replays
at most 500 events (`Server(replay_limit=)`); beyond that you get the latest 500 and
`truncated=True`, and `history(thread.id, before=seq)` pages back when you want more. A `since`
past the thread's last `seq` — it was deleted and made again under the same id — replays nothing
and says `truncated=True` too, and the client follows the thread from `joined.seq`.

A participant that follows never needs this: a thread it is added to arrives on its own
([Following and catching up](#following-and-catching-up)).

### Sending and streaming

Every verb that writes returns the stored event:

```python
message = await ana.send(thread.id, [Text(text="Here is the receipt."), Image(media_type="image/png", url=url)])
await ana.edit(thread.id, message.id, "Here is the right receipt.")
await ana.retract(thread.id, message.id)
await ana.read(thread.id, up_to_seq=message.seq)
await ana.custom(thread.id, "rating", {"stars": 5})
```

Only an event's author edits or retracts it; only messages are edited, and only messages and
steps are retracted. `in_reply_to=` ties an event to the one it answers and `triggered_by=` names
who caused it — a scheduled assistant can say which person set it up, or leave it out.

`metadata=` on `send` and `stream` is yours, stored on the message: a JSON object of at most 8 KiB
— the engine run that wrote a reply, say, so a mark on the reply can be filed against the run. It
is set once, when the message is written; an edit changes the content, never the metadata, and
`resolve` keeps it. The library never reads it:

```python
answer = await helper.send(thread.id, "Refunded in full.", in_reply_to=message.id, metadata={"run_id": "9f2c"})
```

`client_id=` on `send`, `step`, `edit`, `retract` and `custom` (`clientId` in TypeScript) is the
write's idempotency key, generated when you leave it out; name it yourself and the same write sent
twice — a scheduled job that runs again, a retry after a crash — is stored once, and the second call
returns the event stored the first time:

```python
reminder = await helper.send(thread.id, "Your plan renews Friday.", client_id="renewal-ana-2026-10")
again = await helper.send(thread.id, "Your plan renews Friday.", client_id="renewal-ana-2026-10")
assert again.id == reminder.id
```

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

A join on `Invited` is live from that moment, so it misses what was said between the invitation
and the join; `join(item.thread_id, since=0)` reads the thread from its start. Your own events and
signals are left out, so an assistant does not answer itself; `events(include_own=True)` keeps
them. Deciding when not to answer — another assistant, a loop — is your code.

A reply can stream. The text travels as signals, and the message that finishes it is stored with
`client_id` equal to the stream's id, so a UI replaces the streamed bubble with it:

```python
async with helper.stream(thread.id, in_reply_to=message.id) as reply:
    for chunk in ["The second charge ", "was refunded."]:
        await reply.write(chunk)
```

Leaving the block stores the text written as one message, with the stream's `metadata=`;
`await reply.finish(content)` stores something else instead — the text and an image, say. An
exception inside the block, or a finishing message the server refuses, abandons the stream:
`StreamEnd` arrives and nothing is stored. The finishing message always arrives before
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

`Working` shows what is happening now and is gone when the assistant is idle, leaves or
disconnects; the steps stay in the log. A UI folds the steps under the reply that shares their
`in_reply_to`. Which steps to show — every tool call, only the ones a person would care about,
none — is yours.

Showing an engine run's work is a few lines of your code: an `on_event` callback that turns a text
delta into `reply.write` and a finished tool call into `client.step`. Open the stream on the first
delta with `metadata={"run_id": delta.run_id}` and the reply is tied to its run. A senses pipeline
does the same with what it hears and sees. The libraries never import each other.

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
reminders = await server.connect(identity=Identity(role="assistant", id="reminders"), scope="acme")
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
with fresh `headers`; a following client follows again from the last `seq` it saw in each thread
(and joins each one instead when that fails), and every client re-joins every thread it had joined
from there; the missed events arrive before anything new. A re-follow's or re-join's `Joined` also
arrives in `events()`, and when it says `truncated`, redraw from the latest page. A reconnect your
endpoint refuses (HTTP 401 or 403 on the upgrade, or 1008 before the hello is answered), or whose
`headers` function raises `ForbiddenError`, stops the client, and `events()` raises that error; an
error from `headers` that is not the library's is a failed attempt, retried with backoff and the
function called again. Items wait in the client until you read them, so an assistant that joins and
then starts reading misses nothing.

A client's `status` says where its connection is: `"open"`; `"reconnecting"` from the moment its
socket drops until it follows again, if it was following, and every thread it had joined is joined
again; `"closed"` once it stops, by `close()` or a refused reconnect. Each change arrives in
`events()` as a `StatusChanged` — `"open"` after the re-follow's and re-joins' `Joined` — the one
item that names no thread. It is how a person's page says it
is reconnecting.

A connection that cannot keep up — its outbound queue over its bound with no write finishing for
ten seconds — is closed, and its client resumes from its cursor. Stream deltas waiting for a slow
connection are merged, never dropped.

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
Following is not presence; a page that opens a thread still joins it, and `join(thread_id, since=)`
hands it the thread's events after `since` once while the thread keeps arriving live; two such joins
at the same time each get their own. `leave` stops one followed thread, across dropped sockets,
until you join it, follow again or are added to it again. `follow(resume=True)` first hands you what
each thread said after your **mark**, your private read position: `mark(thread_id, seq)` stores it,
never lowers it, and when it moved tells your other connections with `Marked`; `marked(thread_id)`
reads it, `None` without one, and writes nothing. A thread you were added to starts from the add; a
gap longer than the replay limit arrives as its latest events with `truncated=True`, and
`history(thread_id, after=mark, before=...)` reads the rest. A participant that joins its threads
one at a time resumes each the same way, with `join(thread_id, resume=True)`, whose `Joined` carries
the mark it resumed from as `thread.marked`; a join takes `since` or `resume`, not both. Resuming is
at least once: an event handled just before a crash comes again, so handle idempotently, and when
you handle events concurrently, mark only up to the lowest one not yet handled.

An inbox reads the same marks:

```python
inbox = await ana.list_threads(order="activity", preview=True)
inbox[0].unread, inbox[0].last_message
first_unread = await ana.history(inbox[0].id, after=inbox[0].marked)
if first_unread:
    older = await ana.history(inbox[0].id, before=first_unread[0].seq)
```

`order="activity"` puts the thread with the latest message first; `unread` counts messages by
others after your mark, up to 100; `preview=True` adds each thread's latest message, edits applied,
retractions skipped.

### Many replicas

Run as many servers as you like on one Postgres database. Each one `LISTEN`s for the others and
delivers to its own connections:

```python
server = Server(PostgresStorage(pool))
await server.start()
await server.stop()
```

An event is stored and announced in one transaction; every replica with a connection in that
thread reads it by `seq`. Signals are delivered on the sender's replica and mirrored to the others.
Each replica keeps who is present, working and streaming, and repeats its own every ten seconds; a
replica that stops repeating — it crashed — is forgotten within thirty. A missed announcement costs
latency, never an event: resume reads from storage. A replica whose `LISTEN` connection drops takes
another with jittered backoff, listens again and repeats its own state at once, so the others
rebuild it. `start()` holds one connection from the pool for `LISTEN`; `stop()` gives it back.

#### One handler per thread

Every replica's assistant follows the same threads, so every one of them hears each message. A
Postgres advisory lock on the same database, keyed by scope and thread, makes the runs on one thread
mutually exclusive; answering once is the holder's check, inside the lock, for a reply already in
the log:

```python
async with pool.acquire() as conn:
    key = f"{assistant.scope}:{item.thread_id}"
    if await conn.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key):
        try:
            later = await assistant.history(item.thread_id, after=item.seq, limit=500, kinds=["message"])
            mine = [e for e in later if e.in_reply_to == item.id and e.author.id == assistant.identity.id]
            if not mine:
                await answer(item)
            await assistant.mark(item.thread_id, item.seq)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", key)
```

Advisory locks share one key space per database with whatever else the application locks, and a
collision between try-locks silently skips a message, hence the 64-bit key. A session lock pins its
connection for the whole run: take it from a direct session, not a transaction-pooling proxy, on a
pool reserved for these locks and sized to the runs a replica allows at once, not the pool the
server and storage use.

A replica that dies releases its lock with its connection. Effects stay idempotent all the same: a
resume can hand the next holder an event the last one handled just before it died.

### Permissions

Who someone is and what your product allows are yours; who is in the thread is the library's:

- Your endpoint decides who a connection is and its scope, and hands them to `serve`.
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

server = Server(storage, authorize=authorize)
```

The server's own verbs act as the system, with no member behind them — for your REST endpoints,
jobs and back office:

```python
await server.create_thread("acme", thread_id="onboarding-bruno", metadata={"owner": "bruno"})
await server.add_member("acme", "onboarding-bruno", Identity(role="user", id="bruno"))
await server.list_threads("acme", metadata={"owner": "bruno"})
await server.update_thread("acme", "onboarding-bruno", {"owner": "carla"})
await server.remove_member("acme", "onboarding-bruno", Identity(role="user", id="bruno"))
await server.delete_thread("acme", "onboarding-bruno")
```

They write what the client's verbs write — `MemberAdded`, `ThreadUpdated`, `MemberRemoved`, authored
by the system — reach whoever has the thread open the same way, and raise the same errors; they are
never put to `authorize`.

`server.list_threads(scope, metadata=, before=, limit=50)` lists every thread of the scope, member
or not, newest first, `before=` a thread id for the next page. `metadata=` is containment: every key
given has that value, nested objects alike, compared as JSON — `True` matches `true`, never
`"true"`. A client's `list_threads` stays the threads it is a member of.

That is how a product offers **threads anyone may join** — a question elevated to the whole plant,
a channel: say so in the thread's metadata, list by it, and let your endpoint add whoever asks. The
new member's connections receive `Invited`, and their client joins with `since=0` for the whole
log; `server.remove_member` takes them out again when the thread turns private:

```python
await server.update_thread("acme", thread.id, {"elevated": True, "location": "reno"})
elevated = await server.list_threads("acme", metadata={"elevated": True, "location": "reno"})
await server.add_member("acme", elevated[0].id, Identity(role="user", id="bruno"))
```

**Redacting.** What must never be stored — card numbers, health records — your function masks
between receiving a message and storing it:

```python
async def redact(author, thread, content):
    return [Text(text=mask(part.text)) if part.type == "text" else part for part in content]

server = Server(storage, redact=redact)
```

It is called for every message and edit before it is stored, whoever writes it, and on the message
a stream ends in. What it returns is what is stored, delivered and handed back to the sender; the
original reaches no storage and no other connection. It is checked like any content — parts, the
inline cap — and must leave at least one part. Raising refuses the write and nothing is stored: an
`InputError` or `ForbiddenError` you raise reaches the sender as itself, anything else as the
server's generic failure, logged as a warning, and the outbox retries neither. A repeated
`client_id` gets the stored message back without another call. It never sees stream deltas —
signals, shown live and never stored, so text that must never be shown is not streamed — nor a
message's `metadata`, custom payloads, or a step's input and output, which are the writer's own.

Securing the upgrade is yours, like the rest of authentication. A short checklist:

- Serve over `wss://`, never `ws://`, outside your own machine.
- If you authenticate with cookies, check the `Origin` header of the upgrade against your own
  origins. Browsers attach cookies to a WebSocket upgrade from any site and WebSockets have no CORS,
  so without the check any page the person visits could speak as them (cross-site WebSocket
  hijacking).
- Prefer short-lived tokens. `headers` in Python and `protocols` in TypeScript may be async
  functions, called on every reconnect, so a fresh token goes with each attempt. Raise
  `ForbiddenError` from one to refuse the connection for good, as the example below does for an
  expired session; an error that is not the library's, a failed token fetch while the network is
  down say, counts as a failed attempt and the client keeps reconnecting.

### The TypeScript client

`@rfnry/thread` is the same client for the browser and Node, with no framework and no
dependencies:

```ts
import { connect, ForbiddenError, resolve } from '@rfnry/thread'

const client = await connect({
  url: 'wss://app.example.com/thread',
  protocols: async () => {
    if (session.expired) throw new ForbiddenError('session expired')
    return ['thread', await session.token()]
  },
})
const unsubscribe = client.subscribe(render, { includeOwn: true })
const joined = await client.join(threadId, { since: 0 })
const message = await client.send(threadId, 'Can you check my invoice?')
```

The verbs are the Python ones in camelCase — `createThread`, `getThread`, `listThreads`,
`updateThread`, `deleteThread`, `join`, `leave`, `follow`, `mark`, `marked`, `history`, `present`,
`addMember`, `removeMember`, `listMembers`, `send`, `stream`, `step`, `edit`, `retract`, `read`,
`custom`, `working`, `idle`, `cancel`, `close` — with the same outbox, reconnect and resume. Events
and signals are the wire shapes, fields as they travel (`thread_id`, `in_reply_to`); options are
camelCase, and `send` and `stream` take `metadata` beside `inReplyTo`. A browser cannot set headers
on a WebSocket, so the upgrade carries your session cookie — check its `Origin` — or a token in
`protocols`, the subprotocol list; an endpoint that reads a token there accepts with one of the
offered subprotocols (`await websocket.accept(subprotocol="thread")`). Listeners get what arrives
after they subscribe, so subscribe before joining. A UI usually wants its own messages too, echoed
from other tabs, hence `includeOwn`. Streaming:

```ts
const reply = client.stream(threadId, { inReplyTo: message.id })
reply.write('Refunded ')
reply.write('in full.')
await reply.finish()
```

`finish(content)` stores other content; `abort()` abandons. `client.status` is the connection's —
`'open'`, `'reconnecting'` or `'closed'` — and every listener hears each change as a
`StatusChanged`, `{ kind: 'status', status }`: the one item without a thread, so a listener that
follows one thread checks for it first. `client.closed` resolves when the client stops for good —
`null` after `close()`, or the error when your `protocols` function throws `ForbiddenError` on a
reconnect or your endpoint refuses it, an expired session say. A browser cannot see why an upgrade
failed, so an upgrade refused with HTTP 401 or 403 keeps retrying with backoff, `'reconnecting'` all
along; a connection accepted and closed with 1008 is the refusal it sees. Stores, hooks and
rendering are your UI's.

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

Four tools: `send(text)` posts a message with the bound `in_reply_to` and `triggered_by`, and no
metadata — what a message is tied to is your code's to say, not the model's; `read_recent(limit)`
returns the latest messages and steps, resolved, as parts — for each event a `Text` with its `seq`,
kind, author and `in_reply_to` as JSON, and for a step its `name`, `input` and `is_error` too, then
its content as it was sent (a step's output), images included; `working(label)` and `idle()` show
what the assistant is doing. `read_recent` reads the latest 200 messages, steps, edits and
retractions and resolves them, so a thread with many edits or retractions can return fewer than
`limit`. The thread is fixed when the tools are built and a model cannot name another.

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

Postgres is the storage: `PostgresStorage(pool)` takes an asyncpg-compatible pool you built
(`rfnry-thread[postgres]` installs asyncpg). `initialize()` creates four tables — `thread_threads`,
`thread_members`, `thread_events`, `thread_marks` — and is safe to run again. The prefix lets one
database and one pool serve rfnry-engine's and rfnry-memory's tables too:

```python
pool = await asyncpg.create_pool(dsn)
storage = PostgresStorage(pool)
await storage.initialize()
server = Server(storage)
```

`Storage` is the protocol `PostgresStorage` implements; a storage of your own implements it too, and
`Server` takes it the same way. `scope` is the first argument of every method that reads or writes a
row. The log is append-only: an event is never updated or deleted on its own, only with its thread
or its scope.

## Tenancy

`scope` is the tenant: `serve` and `server.connect` take it with the identity, and
every thread, member and event row carries it. A connection sees only its scope; the same thread id
in two scopes names two threads. To offboard:

```python
removed = await server.delete_scope("acme")
assert await server.verify_absent("acme")
```

`delete_scope` returns how many rows it removed; `verify_absent` counts every table independently.

## At a glance

Everything the package exports, by role.

- **Serve:** `Server`, with your `authorize` and `redact`; `Connection`.
- **Take part:** `Client`, `Stream`, `Joined`, `StatusChanged`.
- **Who and what:** `Identity`, `IdentityLike`, `Thread`, `Member`; parts `Text`, `Image`, `Audio`, `Document`, `Part`, `Content`.
- **The log:** `Message`, `Step`, `Edit`, `Retract`, `Read`, `MemberAdded`, `MemberRemoved`, `ThreadUpdated`, `Custom`, `Event`; `resolve`.
- **Right now:** `Working`, `Idle`, `StreamStart`, `StreamDelta`, `StreamEnd`, `Cancel`, `Present`, `Left`, `Invited`, `Marked`, `ThreadDeleted`, `Signal`.
- **Give models a thread:** `ThreadTools`, `Tool`, `Tools`.
- **Store:** `Storage`, `PostgresStorage`.
- **Fail loudly:** `ThreadException`, `NotFoundError`, `InputError`, `ConfigurationError`, `ForbiddenError`, `StorageError`, `TransportError`.

The TypeScript package is a client: it exports `connect`, the `Client` and `Stream` classes,
`resolve`, the same error classes, and the types of the identity, parts, events, signals and records
above, and nothing of the server, the storage or the tools; `Item` is what a listener receives — an
event, a signal, a re-join's `Joined` or a `StatusChanged` — `Status` is what `client.status` holds,
and `ConnectOptions`, `Protocols`, `Listener`, `StreamOptions`, `SendOptions`, `WriteOptions` and
`StepOptions` type the options.

## Errors

`NotFoundError` for a thread, member or event the scope does not hold; an empty thread id names no
thread, so it is a `NotFoundError` too, except in `create_thread`, which refuses it as an
`InputError`, and in a signal, which needs a join first. `ForbiddenError` for a verb by a
non-member, an `authorize` that says no, an edit or retraction of someone else's event, a signal
from a connection that has not joined the thread, and on a client, an upgrade refused with HTTP 401
or 403, a connection closed with 1008 before its hello is answered, or a `headers` or `protocols`
function that raised `ForbiddenError`. `InputError` for a request the library refuses: an empty
scope, an empty thread id given to `create_thread`, an identity whose fields do not fit, a `system`
identity added as a member, a `limit` outside 1 to 500 on `list_threads` or `history`, a join given
both `since` and `resume`, a `working` label over 1000 characters, a media part with both or neither
of `data` and `url`, inline media over the cap, a custom payload over 64 KiB, message metadata over
8 KiB or not a JSON object, a message or edit that redaction leaves empty, an edit of anything but a
message, a retraction of anything but a message or a step, an unknown event or signal kind, a
malformed frame. `ConfigurationError` for a setup that cannot work: a limit that is not positive, a
`system` identity given to `serve` or `server.connect`, a `redact` that returns something other than
parts, a `ThreadTools` with an empty `thread_id`, two tools with the same name in one `Tools`, a
`Tool` with no body when it is called. `StorageError` for the database. `TransportError` for the
socket: a write not acknowledged within its deadline, a client used after `close()`, a connection
that cannot be opened. On a client, an error the server raised is raised again with its class and
message.

All under `ThreadException` — the root takes the `Exception` suffix, every child ends in `Error`,
and the names avoid Python's `PermissionError` and `ConnectionError`. Every message names the
module that raised it, then says what went wrong in lowercase: `server: thread 'support-ana' was not
found`, `server: 'bruno' is not a member of thread 'support-ana'`, `server: inline media is 300000
bytes; the cap is 262144 — upload it and send a url`. The library logs only warnings, for what no
caller is there to receive: an unexpected failure inside `serve`, a failure between replicas —
mirroring to them or handling their notices — and a signal too large to mirror.
