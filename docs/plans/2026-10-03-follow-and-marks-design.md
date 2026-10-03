# Follow, marks and the inbox: design

2026-10-03. Approved by the owner section by section. `docs/thread.md` stays the contract: when this
document and the contract differ after the build, the contract wins, and the difference is reported.

## Why

Three products built on rfnry-thread each wrote the same delivery code around the library:

- **AIHelpdesk.** Its assistant runs in every backend process and has to `join` each ticket thread
  at start-up. The cost grows with the number of tickets, about 1–2 s for 77.
- **The support pilot.** Its recorder keeps its own cursor table so that, after a restart, it
  neither misses nor repeats a message.
- **The maintenance pilot.** One task must answer each thread, so it takes a Postgres advisory lock
  per thread.

The first two needs are delivery, which is this library's concern:
1. receiving every thread you belong to;
2. catching up after being away.

The third need is coordination. It stays in the application, as a documented pattern.

## Prior art

Each pattern comes from established chat systems. These references were checked against their
official documentation on 2026-10-03.

| Pattern | Matrix | Telegram | Slack | Discord | WhatsApp |
|---|---|---|---|---|---|
| Summary per conversation on connect | `/sync`: per room, a timeline capped by `limit` (`limited`, `prev_batch`) and counts | `messages.getDialogs`: `top_message`, `read_inbox_max_id`, `unread_count` | `conversations.info` (`unread_count` for DMs only) | `last_message_id` per channel | — |
| Older history pulled backwards, page by page | `/rooms/{id}/messages?dir=b&from=` | `messages.getHistory(offset_id, add_offset, limit)` | `conversations.history(cursor, latest, oldest)` | `GET /channels/{id}/messages?before=` (limit ≤ 100) | — |
| Catch up on the gap, with a "too long" cutoff | `limited: true` with `prev_batch` | `updates.getDifference` → `differenceTooLong` | — | — | the server holds undelivered messages |
| A private read position, separate from public receipts | `m.fully_read` (account data) vs `m.read` / `m.read.private` | `messages.readHistory(max_id)` | `conversations.mark(channel, ts)` | client read state (not officially documented) | — |
| The read position follows you to your other devices | account data on every device | `updateReadHistoryInbox` with `still_unread_count` | `channel_marked` to all of a user's connections | `MESSAGE_ACK` (client-internal) | — |
| New members see from when they joined | `m.room.history_visibility` (`joined`) | `channels.togglePreHistoryHidden` | — | — | the default; since 2026-02, members may share 25–100 recent messages |
| "Mark unread" kept apart from the pointer | `m.marked_unread` | `unread_mark` | — | `manual` ack | — |

**Sources:**
- spec.matrix.org: the client-server API's `sync`, `message_pagination`, `read_markers`,
  `receipts` and `history_visibility`.
- core.telegram.org/api: `updates`, `offsets`, and the `dialog` and `updateReadHistoryInbox`
  constructors.
- docs.slack.dev: `conversations.history`, `conversations.mark`, `conversations.info` and
  `channel_marked`.
- docs.discord.com: messages and channels.
- whatsapp.com: the privacy policy, the security whitepaper v9, and the group message history post.

**Not adopted, and why:**
- **History kept on the device** (WhatsApp): here the server's log is the truth.
- **An account-wide sequence** (Telegram's `pts`): a per-thread `seq` plus marks cover it.
- **Mention and notification counts** (Matrix, Telegram): those belong to the application.

## Decisions

| Question | Answer |
|---|---|
| What does the library solve? | Following every thread you belong to, and resuming after a restart. One handler per thread becomes a documented pattern. |
| Why not one handler per thread? | It would bend "no dispatcher". It would also break the rule that every connection that joined a thread receives everything, which is right for people. And it could not remove the need for idempotent effects. |
| What is the resume cursor? | A private mark per participant and thread: state, not a log event. Public `read()` receipts stay as they are. (At first this was the participant's own `Read` events; adopting the chat-app pattern replaced that, because a public event per handled message is traffic and a receipt in every UI.) |
| Where does a thread without a mark start? | Where the participant was added: the server writes the first mark at the add. A membership from before this change starts at "now". |
| How is follow built? | In the server: the connection enters every room it belongs to, and delivery reuses the join path. |
| Last-message preview? | Opt-in: `list_threads(..., preview=True)` returns each thread's last message, resolved for edits and retractions, at the cost of one more query per page. |

## Design

### 1. Verbs and semantics

```python
await client.follow()                    # live: every thread you belong to, from now
await client.follow(resume=True)         # each thread's gap after your mark, then live
await client.mark(thread_id, seq)        # your private read position; returns the stored seq
await client.history(thread_id, after=seq)              # forward; before= stays backward
await client.list_threads(order="activity", preview=True)
```

```ts
await client.follow({ resume: true })
await client.mark(threadId, seq)
await client.history(threadId, { after: seq })
await client.listThreads({ order: 'activity', preview: true })
```

**`follow(resume=False) -> list[Joined]`.** It delivers every thread the participant is a member
of, from now on.
- **What arrives:** each thread's `Joined` arrives on the client's stream (`events()` or
  `subscribe`), as a re-join's does today. The call also returns the list.
- **New memberships:** a thread the participant is added to later is delivered from the add, live.
  No `Invited` and `join(since=0)` are needed.
- **Authorization:** threads the `authorize` hook refuses to `read` are skipped, as in
  `list_threads`.

**`follow(resume=True)`.** For each thread, the events after the participant's mark come first,
oldest first, at most `replay_limit` (500) per thread. A longer gap arrives with
`truncated=True`, and the rest is paged with `history(after=)`. Then delivery is live.
- **Memberships with no mark:** a membership from before this change has none. The first resuming
  follow stores "now" as its mark, in one statement, so later resumes have a baseline.

**Presence.** Following is not presence: no `Present`/`Left` is announced for followed threads. A
connection that `join`s a thread is present there, as today.
- **Signals:** a connection may send a signal in, and receives the signals of, any thread it
  receives, whether joined or followed. `Cancel` and streams therefore keep working for an
  assistant that only follows.

**`leave(thread_id)`.** Stops one thread for this connection until the next `follow` or `join`.
There is no `unfollow`; closing the client ends following.

**`mark(thread_id, seq) -> int`.** Stores the participant's private read position in the thread.
- **Moves forward only:** it stores `max(old, new)`, so a stale write from another replica never
  moves it back. "Mark as unread" can come later as a separate flag, as Matrix and Telegram keep it
  apart from the pointer.
- **Checks:** it requires membership and `authorize` `read`. A `seq` must be between 0 and the
  thread's last `seq` (`InputError`).
- **Your other connections:** when the mark moved, the participant's own other connections, on
  every replica, receive `Marked(thread_id, seq, unread)`; the connection that marked does not.
  `unread` is the count still unread after the mark, so they don't recount.

**The handling pattern.** Handle, then mark. With concurrent handling, mark only up to the lowest
event not yet handled. Resume is at-least-once: an event handled just before a crash arrives again,
so effects must be idempotent. One way is to check the log for a reply you already wrote.

**The inbox.** `list_threads(order="created" | "activity", preview=False)`.
- **Activity order:** by `last_message_at`, falling back to `created_at` for a thread without
  messages. Pages may shift as threads receive messages, as Telegram's dialogs do.
- **Every listed thread** carries `last_seq` and `last_message_at`.
- **When a member lists its own threads,** each also carries:
  - `marked`: the mark, or `None` for a membership without one;
  - `unread`: `message` events by others after the mark, counted up to 100. A UI shows "99+"
    past that. A later retraction does not lower the count. `None` when there is no mark.
- **With `preview=True`,** `last_message` is the latest message not retracted, with its edits
  applied, looking back at most 5 messages.
- **Opening a thread at the first unread** is `history(after=thread.marked)`. Scrolling back is
  `history(before=…)`.

### 2. Server

**`follow` handling.**
1. **Lookup:** it pages through the participant's memberships with the storage listing: each
   thread, its `last_seq` and its mark.
2. **Authorization:** it skips threads `authorize` refuses.
3. **Rooms:** it adds the connection to each room, with its cursor set to the thread's `last_seq`,
   or to the mark when resuming. It pushes each room's current signals (working, open streams).
4. **Gaps:** when resuming, it pushes each thread's gap from storage, capped at `replay_limit`.
5. **Result:** it flags the connection as following and returns the `Joined` list.

**Two sets per connection.**
- **Delivered threads,** each with a cursor: what it receives and may signal in.
- **Present threads:** what it has `join`ed.

`follow` fills only the first; `join` fills both; `leave` removes the thread from both. The
existing `MemberRemoved` cut and `ThreadDeleted` handling apply to both sets.

**New memberships, on every replica.**
- **The notice:** adding a member appends `MemberAdded`, writes the new member's first mark at that
  event's `seq - 1`, and notifies the added identity's connections on every replica. Today the
  notice only goes to others, and is called "invited". Now it also reaches the identity's own
  connections when it added itself, as the creator of a thread does.
- **On each replica:** a following connection of that identity is added to the room from the
  `MemberAdded` `seq`, so nothing appended between the add and the notice is missed. A connection
  that is not following receives `Invited`, as today; on a self-add it receives nothing, as today.

**Marks across replicas.** `Marked` travels to the identity's connections through the same "to an
identity" notice.

**Cost.**
- **Memory:** a following connection holds one cursor per membership, as joining every thread does
  today.
- **Per `follow`:** one listing pass, plus one gap query per thread that has a gap (only when
  resuming).

### 3. Storage

Both `PostgresStorage` and `InMemoryStorage` implement this, and the shared storage suite covers
both.

**New table:**

```sql
CREATE TABLE IF NOT EXISTS thread_marks (
    scope TEXT NOT NULL, thread_id TEXT NOT NULL, identity_id TEXT NOT NULL,
    seq BIGINT NOT NULL, at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (scope, thread_id, identity_id),
    FOREIGN KEY (scope, thread_id, identity_id)
        REFERENCES thread_members (scope, thread_id, identity_id) ON DELETE CASCADE)
```

- **Removing a member** drops their mark. Rejoining starts at the new add.
- **Tenancy:** `delete_scope` and `verify_absent` cover the table.

**`thread_threads` gains `last_message_at TIMESTAMPTZ`:**
- **Maintenance:** set in the transaction that appends a `message` event. Other kinds don't move
  it.
- **Migration:** added by `initialize` with `ADD COLUMN IF NOT EXISTS`, and backfilled once from the
  log in the same `initialize` that adds it.
- **Ordering:** an expression index on `(scope, COALESCE(last_message_at, created_at) DESC, id DESC)`
  serves activity order. `before=` pages on the same tuple.

**Storage protocol additions:**
- **`mark(scope, thread_id, identity_id, seq) -> int`:** an upsert keeping `GREATEST`, returning the
  stored `seq`.
  It also writes the first mark at an add: a removal cascades the old mark away, so a new
  membership never has one.
- **`baseline_marks(scope, identity_id) -> int`:** one statement
  (`INSERT … SELECT … ON CONFLICT DO NOTHING`). It stores each thread's `last_seq` as the mark of
  every membership of `identity_id` that has none, returns how many it stored, and serves a first
  resume.
- **`list_threads(scope, member_id, before, limit, metadata, order="created", preview=False)`:**
  - for a member, it fills `marked` and `unread`, the count capped by a `LIMIT 100` subquery on
    `(scope, thread_id, seq)`;
  - with `preview`, it fills `last_message`. That costs one more query per page: the last 5
    messages of each listed thread, plus the edits and retractions after them, resolved.
- **`list_events(..., since=, before=)`:** already reads forward and serves `history(after=)`.

**Model:** `Thread` gains optional fields, so existing code is unaffected:
- `last_seq: int | None`;
- `last_message_at: datetime | None`;
- `marked: int | None`;
- `unread: int | None`;
- `last_message: Message | None`.

### 4. Protocol and clients

The golden protocol fixtures, shared by the Python and TypeScript suites, cover every new frame.

**Requests:**
- **`thread.follow`:** `{resume, since?}` → `{joined: [Joined]}`. `since` is an optional
  `{thread_id: seq}` map, sent by a client resuming after a dropped socket, that takes precedence
  over marks for the threads it names. Frames go up to 16 MB; a map for 10,000 threads is about
  500 KB.
- **`thread.mark`:** `{thread_id, seq}` → `{seq, unread}`.
- **`thread.history`:** gains `after`.
- **`thread.list`:** gains `order` and `preview`.

**Pushes:**
- **`joined`:** server-initiated, sent when a following connection starts receiving a thread it
  was just added to. Only following connections receive it.
- **A new signal, `Marked {thread_id, sender, seq, unread}`:** received, when the mark moved, only
  by the sender identity's other connections. It joins the item union in both clients.

**Both clients:**
- **`follow`:** `follow(resume=False)` (TS `follow({ resume })`) records that the client follows,
  and keeps a cursor for every delivered thread.
- **Reconnecting:** after a dropped socket, a following client sends one `thread.follow` with
  `resume` and its cursor map, then re-joins the threads it had explicitly joined (for presence).
  Events at or below its cursors are dropped, as today. `status` becomes `open` when both are done.
- **The other verbs:**
  - `mark(thread_id, seq)`;
  - `history(thread_id, before=, after=, limit=, kinds=)`;
  - `list_threads(before=, limit=, order=, preview=)` (TS `listThreads({ … })`).
- **The server's own verbs** (`server.list_threads(scope, …)`) gain `order` and `preview`. A
  system listing has no `marked` or `unread`.

**Compatibility:** everything is additive. No example in `docs/thread.md` stops working. A
consumer with an exhaustive switch over item kinds sees the new `marked` kind.

### 5. Docs, tests, adoption

**`docs/thread.md`:**
- **A new section, "Following and catching up":**
  - `follow` and `resume`;
  - marks, with the rule for concurrent handling;
  - the inbox (`list_threads(order=, preview=)` with `marked` and `unread`);
  - opening at the first unread, and scrolling back;
  - the handling loop in a few lines.
- **"Many replicas"** gains **"One handler per thread"** as a pattern: a Postgres advisory lock per
  thread, held while a run works on it, plus idempotent effects, because resume is at-least-once.
- **"Threads and members"** says a follower needs no `Invited` / `join(since=0)`.
- **`docs/api.md`** lists every new name and signature; the docs test keeps it in step.

**Tests:**
- **Unit** (in-memory, always run):
  - follow, live and resuming;
  - a membership added on one of two linked servers reaching a follower on the other;
  - the first mark at the add, and a membership from before starting at "now";
  - marks moving forward only, and `Marked` reaching only the identity's other connections;
  - `unread` capped at 100;
  - activity order;
  - the preview with an edited, and with a retracted, last message;
  - `history(after=)`, and with `before=` as a range;
  - follow announcing no presence, while followers may signal;
  - `authorize` skipping threads;
  - a reconnect re-following once, without loss.
- **Integration** (Postgres):
  - the shared storage suite on both storages: the marks table, the cascade, `last_message_at`
    with its backfill, activity order with paging, the preview query, `delete_scope` and the
    tenancy canary;
  - two replicas on one database.
- **Live:** the TypeScript client against a Python server: follow, a dropped socket, resume, mark.

**Adoption:**
- **`examples/customer_assistant`:** its assistant replaces its `Invited` → `join(since=0)` branch
  with one `follow()`. The other examples change only where they get shorter.
- **AIHelpdesk** (`backend_rfnry`):
  - the assistant starts with `follow(resume=True)` and marks after handling;
  - `Drafts.unresolved()`, `_follow` and the `Invited` branch go;
  - `comparison.md` and the README follow.
- **The pilot specs** (local):
  - the support recorder uses `follow(resume=True)` with `mark` instead of its cursor table;
  - the maintenance pilot's advisory lock is the documented pattern.

**Build order.** Each step keeps every gate green and is pushed when green:
1. storage, and its shared suite on both storages;
2. hub and verbs;
3. the protocol and the Python client;
4. the TypeScript client and the live tier;
5. `docs/thread.md` and `docs/api.md`;
6. the examples;
7. AIHelpdesk, on its local branch, never pushed.

## Pre-change checklist

1. **Wanted two model generations ahead?** Yes. A participant receiving its threads and catching up
   on what it missed does not depend on how capable the model at the other end is.
2. **More or less useful as models improve?** More. Assistants that act on their own across many
   threads need to follow and resume more, not less. This is substrate: delivery and read state.
3. **If less, a structural boundary?** Not applicable.
4. **Does it exist because a model is weak at X?** No. Nothing here filters, summarizes or
   second-guesses content.
5. **Generation only, decision only, both?** All of them. The thread carries whatever a participant
   sends; following and marks assume nothing about the other end.

**Against the engineering checklist:**
- **The concern:** this is delivery and read state, nothing else. The one-handler pattern stays in
  the docs.
- **No framework:** no registry and no dispatcher.
- **Simplicity:** one new table and four verbs or options.
- **Tenancy by construction:** marks carry `scope` and reference the membership.
- **Errors:** messages start with their module's name.

## Later, not now

- "Mark as unread", as a flag separate from the mark.
- Mention and notification counts.
- A history-visibility setting for new members; today the application's `authorize` decides.
- A metadata filter on `follow`, if a participant ever needs to receive only some of its threads.
