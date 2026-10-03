# rfnry-thread API

The reference: every public name, its signature, what it returns and what it raises. How and why to
use them is in [the guide](thread.md).

## Models

### `Identity`

```python
class Identity(BaseModel):
    role: Literal["user", "assistant", "system"]
    id: str
    name: str | None = None
    metadata: dict[str, Any] = {}
```

Anyone who takes part; `role` tells them apart, and `"system"` is reserved for what the library
itself authors. Every identity the library takes is an `IdentityLike`, read by these fields, and it
comes back as an `Identity`.

### `IdentityLike`

```python
class IdentityLike(Protocol):
    @property
    def role(self) -> Literal["user", "assistant", "system"]: ...
    @property
    def id(self) -> str: ...
    @property
    def name(self) -> str | None: ...
    @property
    def metadata(self) -> dict[str, Any]: ...
```

What everything that takes an identity accepts: `Server.serve` and `Server.connect`, the member
verbs `add_member` and `remove_member` on the server and the client, a client's `cancel` and every
`triggered_by`, and `ThreadTools`. Any object with these four fields will do, such as an `Identity`
or rfnry-senses'; its values are validated as strictly as an `Identity`'s, and one that does not fit
raises `InputError` from the call that took it.

### `Text`

```python
class Text(BaseModel):
    type: Literal["text"] = "text"
    text: str
```

A text part of a message.

### `Image`

```python
class Image(BaseModel):
    type: Literal["image"] = "image"
    media_type: str
    data: bytes | None = None
    url: str | None = None
```

An image part: exactly one of `data`, inline bytes sent as base64 on the wire, or `url`, which the
library carries and never fetches. Raises `InputError` when it has both or neither.

### `Audio`

```python
class Audio(BaseModel):
    type: Literal["audio"] = "audio"
    media_type: str
    data: bytes | None = None
    url: str | None = None
```

An audio part: exactly one of `data`, inline bytes sent as base64 on the wire, or `url`, which the
library carries and never fetches. Raises `InputError` when it has both or neither.

### `Document`

```python
class Document(BaseModel):
    type: Literal["document"] = "document"
    media_type: str
    data: bytes | None = None
    url: str | None = None
    name: str | None = None
```

A document part, with an optional file `name`: exactly one of `data`, inline bytes sent as base64 on
the wire, or `url`, which the library carries and never fetches. Raises `InputError` when it has
both or neither.

### `Part`

```python
Part = Annotated[Text | Image | Audio | Document, Field(discriminator="type")]
```

Any of the four parts, told apart by `type`. The vocabulary is shared byte for byte with the rest of
the family.

### `Content`

```python
Content = str | list[Part]
```

What a message carries; a string is sent as one `Text`.

## Server

### `Server`

```python
class Server:
    def __init__(
        self,
        storage: Storage,
        *,
        authorize: Authorize | None = None,
        redact: Redact | None = None,
        max_inline_bytes: int = 256 * 1024,
        replay_limit: int = 500,
    ) -> None: ...
```

The server over a storage. `Authorize` is `Callable[[Identity, str, Thread], Awaitable[bool]]`:
`authorize(identity, action, thread)` decides the actions `create`, `read`, `send`, `manage` and
`delete`, and without it members may do everything; `Redact` is `Callable[[Identity, Thread,
list[Part]], Awaitable[list[Part]]]`: `redact(author, thread, content)` returns what is stored in
place of every message's and edit's content. `max_inline_bytes` caps inline media per event and
`replay_limit` the events a join replays. Raises `ConfigurationError` when either limit is not
positive, and from any verb, `StorageError` when the storage fails.

### `Server.serve`

```python
async def serve(self, connection: Connection, *, identity: IdentityLike, scope: str) -> None: ...
```

Runs one accepted WebSocket until it closes, as `identity` in `scope`; returns when it closes. A
first frame that is not the protocol's hello is answered with an error and the socket closed. Raises
`ConfigurationError` when the identity's role is `system`, `InputError` when the identity does not
fit or the scope is empty.

### `Server.connect`

```python
async def connect(self, *, identity: IdentityLike, scope: str) -> Client: ...
```

Returns an in-process `Client` for `identity` in `scope`, trusted: no socket, no authentication.
Raises `ConfigurationError` when the identity's role is `system`, `InputError` when the identity
does not fit or the scope is empty.

### `Server.start`

```python
async def start(self) -> None: ...
```

Links this server to the other replicas on the same storage: it listens for their events and signals
and repeats its own presence, working and streams every ten seconds. Calling it again does nothing.
Raises `StorageError` when the storage cannot listen.

### `Server.stop`

```python
async def stop(self) -> None: ...
```

Stops listening to the other replicas and gives the listening connection back.

### `Server.create_thread`

```python
async def create_thread(
    self, scope: str, thread_id: str | None = None, metadata: dict[str, Any] | None = None
) -> Thread: ...
```

Creates a thread as the system, with no member, and returns it; with a `thread_id` the scope already
holds, returns that thread. Never put to `authorize`. Raises `InputError` when the scope or
`thread_id` is empty.

### `Server.list_threads`

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
) -> list[Thread]: ...
```

Returns every thread of the scope, member or not, newest first, up to `limit`; `metadata` is
containment compared as JSON, and `before` a thread id for the next page. Raises `InputError` when
the scope is empty or `limit` is not between 1 and 500.

`order` is an `Order`, which is `Literal["created", "activity"]`: `"activity"` lists by the latest
message. `preview=True` fills `last_message`; a system listing has no `marked` or `unread`.

### `Server.update_thread`

```python
async def update_thread(self, scope: str, thread_id: str, metadata: dict[str, Any]) -> Thread: ...
```

Replaces the thread's metadata, writes `ThreadUpdated` authored by the system and returns the
updated thread. Raises `InputError` when the scope is empty, `NotFoundError` when the scope holds no
such thread.

### `Server.add_member`

```python
async def add_member(self, scope: str, thread_id: str, identity: IdentityLike) -> Member: ...
```

Adds `identity` to the thread, writes `MemberAdded` authored by the system, sends `Invited` to their
connections and returns the membership; someone already a member gets theirs back and nothing is
written. Raises `InputError` when the scope is empty or the identity does not fit or is `system`,
`NotFoundError` when the scope holds no such thread.

### `Server.remove_member`

```python
async def remove_member(self, scope: str, thread_id: str, identity: IdentityLike) -> None: ...
```

Removes `identity` from the thread and writes `MemberRemoved` authored by the system. Raises
`InputError` when the scope is empty or the identity does not fit, `NotFoundError` when the scope
holds no such thread or the identity is not a member.

### `Server.delete_thread`

```python
async def delete_thread(self, scope: str, thread_id: str) -> None: ...
```

Removes the thread, its members and its log, and sends `ThreadDeleted` to whoever has it open.
Raises `InputError` when the scope is empty, `NotFoundError` when the scope holds no such thread.

### `Server.delete_scope`

```python
async def delete_scope(self, scope: str) -> int: ...
```

Removes every thread, member and event of the scope and returns how many rows it removed. Raises
`InputError` when the scope is empty.

### `Server.verify_absent`

```python
async def verify_absent(self, scope: str) -> bool: ...
```

Returns whether no row of the scope remains, counting every table on its own. Raises `InputError`
when the scope is empty.

### `Connection`

```python
class Connection(Protocol):
    async def receive_text(self) -> str: ...

    async def send_text(self, data: str) -> None: ...

    async def close(self, code: int = 1000, reason: str | None = None) -> None: ...
```

What `Server.serve` runs: anything with these three methods, which is what most Python web
frameworks hand you for an accepted WebSocket.

## Client

### `Client`

```python
class Client:
    identity: Identity
    scope: str

    def __init__(self, dial: Dial) -> None: ...
```

A participant's connection, the same class whether made by `Client.connect` over a WebSocket or by
`Server.connect` in process; the constructor, whose `Dial` is `Callable[[], Awaitable[Connection]]`,
is not called directly. `identity` and `scope` are as the server knows them. Every verb that talks
to the server raises `TransportError` when the client is closed or not connected, except those that
write an event, which retry across reconnects and raise it when not acknowledged within a minute,
and raises again, with its class and message, an error the server raised.

### `Client.connect`

```python
@classmethod
async def connect(cls, url: str, *, headers: HeadersSource | None = None) -> Client: ...
```

Connects over a WebSocket to `url` and returns the client. `headers` go with the upgrade; a
`HeadersSource` is `dict[str, str] | Callable[[], Awaitable[dict[str, str]]]`, a dict or an async
function returning one, called on every attempt so a token can refresh. Raises `ForbiddenError` when
the upgrade is refused with HTTP 401 or 403 or the socket is closed with 1008 before the hello is
answered, and `TransportError` when the socket cannot be opened or the hello goes unanswered. The
`headers` function refuses by raising `ForbiddenError`: a `ThreadException` it raises is raised as
it is, and anything else as a `TransportError`, which on a reconnect is a failed attempt, retried
with backoff.

### `Client.create_thread`

```python
async def create_thread(
    self, thread_id: str | None = None, metadata: dict[str, Any] | None = None
) -> Thread: ...
```

Creates a thread with `thread_id`, or one the server picks, makes the caller its first member and
returns it; with a `thread_id` the scope already holds, returns that thread. Raises `InputError`
when `thread_id` is empty, `ForbiddenError` when `authorize` refuses `create`, or the thread exists
and the caller is not a member or may not `read` it.

### `Client.get_thread`

```python
async def get_thread(self, thread_id: str) -> Thread: ...
```

Returns the thread. Raises `NotFoundError` when the scope holds no such thread, `ForbiddenError`
when the caller is not a member or `authorize` refuses `read`.

### `Client.list_threads`

```python
async def list_threads(
    self,
    before: str | None = None,
    limit: int = 50,
    order: Order = "created",
    preview: bool = False,
) -> list[Thread]: ...
```

Returns the threads the caller is a member of, newest first, up to `limit`; `before` is a thread
id, for the next page. `order` is an `Order`, which is `Literal["created", "activity"]`:
`"activity"` orders them by their latest message. Each carries
`last_seq`, `last_message_at`, `marked` and `unread`, and with `preview=True` its `last_message`.
With an `authorize`, the threads it refuses to `read` are skipped and a page still holds up to
`limit`. Raises `InputError` when `limit` is not between 1 and 500 or `order` is neither.

### `Client.update_thread`

```python
async def update_thread(self, thread_id: str, metadata: dict[str, Any]) -> Thread: ...
```

Replaces the thread's metadata, writes `ThreadUpdated` and returns the updated thread. Raises
`NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller is not a
member or `authorize` refuses `manage`.

### `Client.delete_thread`

```python
async def delete_thread(self, thread_id: str) -> None: ...
```

Removes the thread, its members and its log, and sends `ThreadDeleted` to whoever has it open.
Raises `NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller is not
a member or `authorize` refuses `delete`.

### `Client.join`

```python
async def join(self, thread_id: str, since: int | None = None, resume: bool = False) -> Joined: ...
```

Receives the thread live from now and returns a `Joined`. With `since`, the events after that `seq`
arrive first (`since=0` for the whole log, at most the server's `replay_limit`, beyond which the
latest and `truncated=True`), then the current state, then live traffic. With `resume=True`, the
events after the caller's mark arrive first instead, as `follow(resume=True)` hands each thread
(from where it was added when it has no mark yet; a membership older than marks starts now, and this
thread alone is marked there), and the `Joined`'s `thread.marked` is that mark, from which
`history(after=...)` reads a truncated gap. A thread the connection already receives, by following,
keeps arriving live, and `since` or `resume` hands its events after that `seq` or the mark up to
where the connection is, once; joins of it in flight at the same time each get their own. Raises
`InputError` when `since` is negative or given with `resume`, `NotFoundError` when the scope holds
no such thread, `ForbiddenError` when the caller is not a member or `authorize` refuses `read`.

### `Client.leave`

```python
async def leave(self, thread_id: str) -> None: ...
```

Stops receiving the thread live; the caller's open streams and `Working` there end, and `Left` goes
out when it was the identity's last connection in it. Leaving a thread not joined does nothing. A
followed thread left stays left, across dropped sockets, until the caller joins it, follows again
or is added to it again.

### `Client.follow`

```python
async def follow(self, resume: bool = False) -> list[Joined]: ...
```

Receives every thread the caller is a member of, from now on, and each thread it is added to later
from the moment it is added, as a `Joined` on `events()`; returns the `Joined` of each thread it
receives now. With `resume=True`, each thread's events after the caller's mark come first (from
where it was added when it has no mark yet; a membership older than marks starts now), at most the
server's `replay_limit`, beyond which the latest and `truncated=True`. Following is not presence:
no `Present` is announced, and the caller may still signal and receive signals in every thread.
After a dropped socket the client follows again from what it has seen, and leaves again the threads
it had left. Threads `authorize` refuses to `read` are left out. Raises `InputError` for a negative
position.

### `Client.mark`

```python
async def mark(self, thread_id: str, seq: int) -> int: ...
```

Stores the caller's private read position in the thread, never lowering it, and returns the stored
`seq`; when the mark moved, the caller's other connections receive `Marked`. Raises `InputError`
when `seq` is not
between 0 and the thread's last `seq`, `NotFoundError` when the scope holds no such thread,
`ForbiddenError` when the caller is not a member or `authorize` refuses `read`.

### `Client.marked`

```python
async def marked(self, thread_id: str) -> int | None: ...
```

Returns the caller's private read position in the thread, `None` without one; it writes nothing and
tells no one. Raises `NotFoundError` when the scope holds no such thread, `ForbiddenError` when the
caller is not a member or `authorize` refuses `read`.

### `Client.history`

```python
async def history(
    self,
    thread_id: str,
    before: int | None = None,
    limit: int = 50,
    kinds: list[str] | None = None,
    after: int | None = None,
) -> list[Event]: ...
```

Returns the latest `limit` events below `before` (a `seq`), in `seq` order; `kinds` keeps only those
kinds. With `after` (a `seq`), the first `limit` events after it, in `seq` order; `before` and
`after` together are a range. Raises `InputError` when `after` is negative or `limit` is not between 1 and 500, `NotFoundError` when the scope
holds no such thread, `ForbiddenError` when the caller is not a member or `authorize` refuses
`read`.

### `Client.present`

```python
async def present(self, thread_id: str) -> list[Identity]: ...
```

Returns who has the thread open now. Raises `NotFoundError` when the scope holds no such thread,
`ForbiddenError` when the caller is not a member or `authorize` refuses `read`.

### `Client.add_member`

```python
async def add_member(self, thread_id: str, identity: IdentityLike) -> Member: ...
```

Adds `identity` to the thread, writes `MemberAdded`, sends `Invited` to their connections and
returns the membership; someone already a member gets theirs back and nothing is written. Raises
`InputError` when the identity does not fit or is `system`, `NotFoundError` when the scope holds no
such thread, `ForbiddenError` when the caller is not a member or `authorize` refuses `manage`.

### `Client.remove_member`

```python
async def remove_member(self, thread_id: str, identity: IdentityLike) -> None: ...
```

Removes `identity` from the thread and writes `MemberRemoved`. Raises `InputError` when the identity
does not fit, `NotFoundError` when the scope holds no such thread or the identity is not a member,
`ForbiddenError` when the caller is not a member or `authorize` refuses `manage`.

### `Client.list_members`

```python
async def list_members(self, thread_id: str) -> list[Member]: ...
```

Returns the thread's members in the order they were added. Raises `NotFoundError` when the scope
holds no such thread, `ForbiddenError` when the caller is not a member or `authorize` refuses
`read`.

### `Client.send`

```python
async def send(
    self,
    thread_id: str,
    content: Content,
    in_reply_to: str | None = None,
    triggered_by: IdentityLike | None = None,
    client_id: str | None = None,
    *,
    metadata: dict[str, Any] | None = None,
) -> Message: ...
```

Sends a message and returns it as stored. `in_reply_to` is the event it answers, `triggered_by` who
caused it, `client_id` the write's idempotency key (generated when left out; a repeat returns the
message stored the first time), and `metadata` yours, a JSON object of at most 8 KiB set once.
Raises `InputError` when inline media is over the cap, the metadata is over 8 KiB or not an object,
redaction leaves nothing or `triggered_by` does not fit, `NotFoundError` when the scope holds no
such thread, `ForbiddenError` when the caller is not a member or `authorize` refuses `send`,
`ConfigurationError` when `redact` returns something other than parts, and an `InputError` or
`ForbiddenError` that `redact` raised.

### `Client.stream`

```python
def stream(
    self,
    thread_id: str,
    in_reply_to: str | None = None,
    triggered_by: IdentityLike | None = None,
    *,
    metadata: dict[str, Any] | None = None,
) -> Stream: ...
```

Returns a `Stream` for a reply, started when its `async with` block is entered; the message that
finishes it carries `in_reply_to`, `triggered_by` and `metadata`. Raises `InputError` when
`triggered_by` does not fit.

### `Client.step`

```python
async def step(
    self,
    thread_id: str,
    name: str,
    input: dict[str, Any] | None = None,
    output: list[Part] | None = None,
    is_error: bool = False,
    duration_ms: int | None = None,
    in_reply_to: str | None = None,
    triggered_by: IdentityLike | None = None,
    client_id: str | None = None,
) -> Step: ...
```

Records one thing an assistant did and returns the stored step; sharing `in_reply_to` with a reply
files it under that reply, and `client_id` is as on `send`. Raises `InputError` when inline media in
`output` is over the cap or `triggered_by` does not fit, `NotFoundError` when the scope holds no
such thread, `ForbiddenError` when the caller is not a member or `authorize` refuses `send`.

### `Client.edit`

```python
async def edit(
    self, thread_id: str, target: str, content: Content, client_id: str | None = None
) -> Edit: ...
```

Replaces the content of the caller's own message `target` and returns the stored edit; the message's
metadata stays. Raises `NotFoundError` when the scope holds no such thread or event,
`ForbiddenError` when `target` is someone else's, the caller is not a member or `authorize` refuses
`send`, `InputError` when `target` is not a message, inline media is over the cap or redaction
leaves nothing.

### `Client.retract`

```python
async def retract(self, thread_id: str, target: str, client_id: str | None = None) -> Retract: ...
```

Unsends the caller's own message or step `target`, whose `seq` the log keeps, and returns the stored
retraction. Raises `NotFoundError` when the scope holds no such thread or event, `ForbiddenError`
when `target` is someone else's, the caller is not a member or `authorize` refuses `send`,
`InputError` when `target` is neither a message nor a step.

### `Client.read`

```python
async def read(self, thread_id: str, up_to_seq: int) -> Read: ...
```

Records that the caller has read up to `up_to_seq` and returns the stored cursor. Raises
`NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller is not a
member or `authorize` refuses `send`.

### `Client.custom`

```python
async def custom(
    self, thread_id: str, name: str, payload: dict[str, Any], client_id: str | None = None
) -> Custom: ...
```

Writes an event of your own, `name` and `payload`, and returns it as stored. Raises `InputError`
when `payload` is over 64 KiB, `NotFoundError` when the scope holds no such thread, `ForbiddenError`
when the caller is not a member or `authorize` refuses `send`.

### `Client.working`

```python
async def working(self, thread_id: str, label: str | None = None) -> None: ...
```

Shows the thread that the caller is busy, `label` saying with what, until `idle`, a leave or a
disconnect. Raises `ForbiddenError` when the connection has not joined the thread or `authorize`
refuses `send`, `InputError` when `label` is over 1000 characters.

### `Client.idle`

```python
async def idle(self, thread_id: str) -> None: ...
```

Ends the caller's `Working` in the thread. Raises `ForbiddenError` when the connection has not
joined the thread.

### `Client.cancel`

```python
async def cancel(self, thread_id: str, to: IdentityLike) -> None: ...
```

Asks `to` to stop; what stopping means is its own. Raises `ForbiddenError` when the connection has
not joined the thread or `authorize` refuses `send`, `InputError` when `to` does not fit.

### `Client.events`

```python
async def events(self, include_own: bool = False) -> AsyncIterator[Item]: ...
```

Yields, in order of arrival, each `Item`, which is `Event | Signal | Joined | StatusChanged`: every
event and signal of the threads joined, each `Invited`, each re-join's `Joined`, and a
`StatusChanged` each time `status` changes; the caller's own are left out unless `include_own`.
Items wait in the client until read, and the iterator ends when the client closes. Raises the error
that stopped the client: `ForbiddenError` when a reconnect was refused with HTTP 401 or 403 or
closed with 1008 before its hello was answered, or the `ForbiddenError` or `ConfigurationError` the
`headers` function raised.

### `Client.status`

```python
@property
def status(self) -> Status: ...
```

The connection's status, a `Status`, which is `Literal["open", "reconnecting", "closed"]`: `"open"`
once connected, `"reconnecting"` from a dropped socket until it is back and every thread joined is
joined again, and `"closed"` for good after `close()` or a refused reconnect. Each change arrives in
`events()` as a `StatusChanged`.

### `Client.close`

```python
async def close(self) -> None: ...
```

Closes the connection and stops reconnecting; pending verbs raise `TransportError`, `status` becomes
`"closed"` and `events()` ends. Closing again does nothing.

### `Stream`

```python
class Stream:
    id: str
    text: str

    def __init__(
        self,
        client: Client,
        thread_id: str,
        in_reply_to: str | None,
        triggered_by: Identity | None,
        metadata: dict[str, Any] | None,
    ) -> None: ...
    async def __aenter__(self) -> Stream: ...
    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> None: ...
```

A reply being written, made by `Client.stream`. Entering the block sends `StreamStart`; leaving it
stores `text`, all that was written, as one message with `client_id` equal to `id`, and an exception
inside abandons the stream: `StreamEnd` arrives and nothing is stored. Raises `ForbiddenError`, on
entering, when the connection has not joined the thread or `authorize` refuses `send`.

### `Stream.write`

```python
async def write(self, text: str) -> None: ...
```

Appends `text` and sends it as the next `StreamDelta`, without waiting for an answer. Never raises.

### `Stream.finish`

```python
async def finish(self, content: Content | None = None) -> Message: ...
```

Stores the text written, or `content` instead, as the finishing message, ends the stream and returns
the message. Raises `InputError` when the stream has already ended, and what `Client.send` raises,
after which the stream has ended with nothing stored.

## Events and signals

### `Message`

```python
class Message(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["message"] = "message"
    content: list[Part]
    metadata: dict[str, Any] = {}
```

The conversation itself: `content` as parts, `metadata` the sender's own, set once when written. The
envelope, the fields before `kind`, is the one every event has (see `Event`).

### `Step`

```python
class Step(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["step"] = "step"
    name: str
    input: dict[str, Any] = {}
    output: list[Part] = []
    is_error: bool = False
    duration_ms: int | None = None
```

One thing an assistant did, filed under the reply that shares its `in_reply_to`.

### `Edit`

```python
class Edit(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["edit"] = "edit"
    target: str
    content: list[Part]
```

A correction of the author's own message `target`, with its new `content`.

### `Retract`

```python
class Retract(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["retract"] = "retract"
    target: str
```

"Unsend": the author's message or step `target` leaves the view, and the log keeps its `seq`.

### `Read`

```python
class Read(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["read"] = "read"
    up_to_seq: int
```

A member's read cursor: the author has read up to `up_to_seq`.

### `MemberAdded`

```python
class MemberAdded(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["member_added"] = "member_added"
    identity: Identity
```

`identity` joined the membership, added by the author.

### `MemberRemoved`

```python
class MemberRemoved(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["member_removed"] = "member_removed"
    identity: Identity
```

`identity` left the membership, removed by the author.

### `ThreadUpdated`

```python
class ThreadUpdated(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["thread_updated"] = "thread_updated"
    metadata: dict[str, Any]
```

The thread's new `metadata`, set by the author.

### `Custom`

```python
class Custom(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None
    kind: Literal["custom"] = "custom"
    name: str
    payload: dict[str, Any]
```

Yours: a `name` and a `payload` of at most 64 KiB.

### `Event`

```python
Event = Annotated[
    Message | Step | Edit | Retract | Read | MemberAdded | MemberRemoved | ThreadUpdated | Custom,
    Field(discriminator="kind"),
]
```

Any row of a thread's log, told apart by `kind`. Every event carries `id`, `thread_id`, `seq`
(thread-local and gapless), `author`, `created_at`, `client_id` (the sender's idempotency key), and
optionally `in_reply_to` (an event id) and `triggered_by` (who caused it, as the sender declares);
the server stamps `id`, `seq`, `author` and `created_at`.

### `resolve`

```python
def resolve(events: Sequence[Event]) -> list[Event]: ...
```

Applies edits and retractions and returns the current view in `seq` order: each edited message with
its latest content and its metadata kept, retracted events left out, and the edits and retractions
themselves left out.

### `Working`

```python
class Working(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["working"] = "working"
    label: str | None = None
```

The sender is busy, an assistant's live step or a person typing, with `label` saying what; ends with
`Idle`, a leave or a disconnect.

### `Idle`

```python
class Idle(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["idle"] = "idle"
```

Ends the sender's `Working`.

### `StreamStart`

```python
class StreamStart(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["stream.start"] = "stream.start"
    stream_id: str
    in_reply_to: str | None = None
```

A reply began streaming as `stream_id`, answering `in_reply_to`.

### `StreamDelta`

```python
class StreamDelta(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["stream.delta"] = "stream.delta"
    stream_id: str
    index: int
    text: str
```

The piece of text at `index` of stream `stream_id`.

### `StreamEnd`

```python
class StreamEnd(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["stream.end"] = "stream.end"
    stream_id: str
```

Stream `stream_id` ended. Its finishing message always arrives first, so a `StreamEnd` with no
message before it means the stream was abandoned.

### `Cancel`

```python
class Cancel(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["cancel"] = "cancel"
    to: Identity
```

Asks `to` to stop; what stopping means is its own.

### `Present`

```python
class Present(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["present"] = "present"
```

The sender's first connection joined the thread.

### `Left`

```python
class Left(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["left"] = "left"
```

The sender's last connection left the thread or dropped.

### `Invited`

```python
class Invited(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["invited"] = "invited"
    thread: Thread
```

You were added to `thread`, which you have not joined; `sender` added you.

### `Marked`

```python
class Marked(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["marked"] = "marked"
    seq: int
    unread: int
```

Your private read position in the thread moved to `seq`, with `unread` messages by others still
after it (at most 100); sent, only when the mark moved, to the marking identity's other
connections, never to the one that marked or to anyone else.

### `ThreadDeleted`

```python
class ThreadDeleted(BaseModel):
    thread_id: str
    sender: Identity
    kind: Literal["thread.deleted"] = "thread.deleted"
```

The thread is gone.

### `Signal`

```python
Signal = Annotated[
    Working
    | Idle
    | StreamStart
    | StreamDelta
    | StreamEnd
    | Cancel
    | Present
    | Left
    | Invited
    | ThreadDeleted
    | Marked,
    Field(discriminator="kind"),
]
```

Anything only true right now, told apart by `kind`: fanned out to whoever is connected and never
stored. Every signal carries `thread_id` and `sender`, stamped by the server.

## Records

### `Thread`

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
```

A conversation: `id` yours or the server's, `scope` its tenant, `metadata` yours. A listed thread
also carries `last_seq` and `last_message_at` (when its latest message was written); listed for a
member, `marked` (the member's private read position, `None` without one) and `unread` (messages by
others after it, at most 100); with `preview`, `last_message` (the latest message not retracted,
with its edits). Fields without a value are left out on the wire.

### `Member`

```python
class Member(BaseModel):
    thread_id: str
    identity: Identity
    added_at: datetime
    added_by: Identity
```

A thread's member: who, since when, and who added them.

### `Joined`

```python
class Joined(BaseModel):
    thread: Thread
    seq: int
    truncated: bool = False
```

What a join returns: the thread, its last `seq` when the join happened, and whether the replay was
cut short, or `since` was past that `seq`.

### `StatusChanged`

```python
class StatusChanged(BaseModel):
    kind: Literal["status"] = "status"
    status: Status
```

What `events()` yields when the client's `status` changes: `"reconnecting"` when its socket drops,
`"open"` after the re-joins that follow, `"closed"` when it stops. It names no thread and never
travels on the wire.

## Tools

### `ThreadTools`

```python
class ThreadTools(Tools):
    def __init__(
        self,
        client: Client,
        *,
        thread_id: str,
        triggered_by: IdentityLike | None = None,
        in_reply_to: str | None = None,
    ) -> None: ...
```

A `Tools` pinned to one client, one thread and one cause: `send(text)` posts a message with the
bound `in_reply_to` and `triggered_by`, `read_recent(limit)` returns the latest messages and steps,
resolved, as parts, from the latest 200 messages, steps, edits and retractions, so many edits can
leave fewer than `limit`, and `working(label)` and `idle()` show what the assistant is doing. Raises
`ConfigurationError` when `thread_id` is empty, `InputError` when `triggered_by` does not fit.

### `Tool`

```python
@dataclass(frozen=True)
class Tool:
    name: str
    description: str = ""
    body: Body | None = None
    args: type[BaseModel] | None = None
    input_schema: dict[str, Any] | None = None
```

One tool: a name, a description, an input schema and a body. `Body` is `Callable[[dict[str, Any]],
Awaitable[Result]]` and `Result` is `str | dict[str, Any] | list[Part]`; `args` is a model that
validates the arguments and gives the schema, unless `input_schema` gives it directly.

### `Tool.schema`

```python
def schema(self) -> dict[str, Any]: ...
```

Returns `input_schema` when given, else the JSON schema of `args`, else an object schema with no
properties.

### `Tool.definition`

```python
def definition(self) -> dict[str, Any]: ...
```

Returns the tool as a model is given it: `name`, `description` and `input_schema`.

### `Tool.call`

```python
async def call(self, args: dict[str, Any]) -> Result: ...
```

Validates the arguments against the `args` model when there is one, keeping only the fields given,
and returns what the body returns, a `Result`: `str | dict[str, Any] | list[Part]`. Raises
`ConfigurationError` when the tool has no body, `InputError` when the arguments do not validate.

### `Tools`

```python
class Tools:
    def __init__(self, tools: Iterable[Tool | ToolsLike] = ()) -> None: ...
    def __add__(self, other: Tools | ToolsLike) -> Tools: ...
    def __contains__(self, name: str) -> bool: ...
    def __len__(self) -> int: ...
```

Tools by name, built from `Tool`s and anything `ToolsLike`, a protocol: any object with
`definitions() -> list[dict[str, Any]]` and `async dispatch(name, args)`, such as another `Tools`.
`+` combines two, `in` asks for a name and `len` counts them. Raises `ConfigurationError` when two
tools share a name.

### `Tools.names`

```python
@property
def names(self) -> list[str]: ...
```

The tool names, in order.

### `Tools.definitions`

```python
def definitions(self) -> list[dict[str, Any]]: ...
```

Returns a copy of every tool's definition, in order.

### `Tools.dispatch`

```python
async def dispatch(self, name: str, args: dict[str, Any]) -> Result: ...
```

Calls the tool `name` with `args` and returns its `Result`: `str | dict[str, Any] | list[Part]`.
Raises `InputError` when there is no such tool, and whatever the tool raises.

### `Tools.only`

```python
def only(self, *names: str) -> Tools: ...
```

Returns a `Tools` of only the tools named. Raises `InputError` when a name is not one of them.

### `Tools.without`

```python
def without(self, *names: str) -> Tools: ...
```

Returns a `Tools` without the tools named; a name that is not one of them is ignored.

## Storage

### `Storage`

```python
@runtime_checkable
class Storage(Protocol):
    async def initialize(self) -> None: ...

    async def create_thread(self, scope: str, thread: Thread) -> tuple[Thread, bool]: ...

    async def get_thread(self, scope: str, thread_id: str) -> Thread | None: ...

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

    async def update_thread(
        self, scope: str, thread_id: str, metadata: dict[str, Any]
    ) -> Thread | None: ...

    async def delete_thread(self, scope: str, thread_id: str) -> bool: ...

    async def add_member(self, scope: str, member: Member) -> tuple[Member, bool]: ...

    async def remove_member(self, scope: str, thread_id: str, identity_id: str) -> bool: ...

    async def list_members(self, scope: str, thread_id: str) -> list[Member]: ...

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool: ...

    async def mark(
        self, scope: str, thread_id: str, identity_id: str, seq: int
    ) -> tuple[int, int, bool]: ...

    async def get_mark(self, scope: str, thread_id: str, identity_id: str) -> int | None: ...

    async def baseline_marks(self, scope: str, identity_id: str) -> int: ...

    async def append_event(self, scope: str, event: Event) -> tuple[Event, bool]: ...

    async def list_events(
        self,
        scope: str,
        thread_id: str,
        since: int | None = None,
        before: int | None = None,
        limit: int = 50,
        kinds: Sequence[str] | None = None,
    ) -> list[Event]: ...

    async def get_event(self, scope: str, thread_id: str, event_id: str) -> Event | None: ...

    async def find_event(
        self, scope: str, thread_id: str, author_id: str, client_id: str
    ) -> Event | None: ...

    async def delete_scope(self, scope: str) -> int: ...

    async def verify_absent(self, scope: str) -> bool: ...

    async def listen(
        self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop: ...

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None: ...
```

The protocol both storages implement; `scope` is the first argument of every method that reads or
writes a row, and the log is append-only: an event is removed only with its thread or its scope.
`create_thread`, `add_member` and `append_event` return the row stored and whether it is new (an
existing thread, membership, or event of the same author and `client_id` comes back with `False`,
and a new event gets the thread's next `seq`); the getters return `None` for what the scope does not
hold, `list_threads` returns threads newest first and `list_events` events in `seq` order, with
`since` the first after it and without it the latest. `order` is an `Order`, which is
`Literal["created", "activity"]`: `"activity"` lists by the latest message instead, a thread without
messages by its creation; `preview=True` fills each thread's
`last_message`. `listen` takes an `OnEvent`, `on_event(scope,
thread_id, seq)`, and an `OnSignal`, `on_signal(scope, thread_id, payload)`, called for what any
server on the storage appends or publishes, and an `OnResume`, `on_resume()`, called once a dropped
listener is back; it returns a `Stop`, `stop()`, that ends it, and all four are async functions
returning `None`. Raises `NotFoundError`, from `add_member` and `append_event`, when the scope holds
no such thread, and `StorageError`, from `create_thread`, when the thread's scope is not `scope`.
`mark` stores the identity's private read position in the thread, never lowering it, and returns
it with the count of `message` events by others after it, at most 100, and whether it moved (a
first mark, or a higher one); `get_mark` returns it, `None` without one; `baseline_marks` stores
each thread's last `seq` as the mark of every membership of `identity_id` that has none, and returns
how many it stored. `mark` raises `NotFoundError` when the identity is not a member of the thread.

### `InMemoryStorage`

```python
class InMemoryStorage:
    def __init__(self) -> None: ...
```

Implements `Storage` in this process's memory, for tests, examples and one process; `initialize`
does nothing. Servers that share one instance and call `start` link as replicas, as they would on
one database.

### `PostgresStorage`

```python
class PostgresStorage:
    def __init__(self, pool: Any) -> None: ...
```

Implements `Storage` on Postgres through an asyncpg-compatible pool you built, in three tables,
`thread_threads`, `thread_members` and `thread_events`, which `initialize` creates where missing,
under an advisory lock, so it is safe to run again. An event is stored and announced with `NOTIFY`
in one transaction, and `listen` holds one connection from the pool, taking another with jittered
backoff when it drops. Raises `StorageError` when the database fails, and from `publish`, when a
notification is over 7900 bytes.

## Errors

### `ThreadException`

```python
class ThreadException(Exception):
    code: ClassVar[str] = "internal"
```

The root of every error the library raises. `code` names the class on the wire, so a client raises a
server's error again with its class and message; every message starts with the module that raised
it.

### `NotFoundError`

```python
class NotFoundError(ThreadException):
    code = "not_found"
```

A thread, member or event the scope does not hold; an empty thread id names none, except in
`create_thread`, an `InputError`, and in a signal, which needs a join first.

### `InputError`

```python
class InputError(ThreadException):
    code = "input"
```

A request the library refuses: an empty scope, an empty thread id given to `create_thread`, an
identity whose fields do not fit, a media part with both or neither of `data` and `url`, a value
over its limit, a join given both `since` and `resume`, an edit or retraction of the wrong kind of
event, an unknown event or signal kind, an unknown tool, arguments a tool rejects.

### `ConfigurationError`

```python
class ConfigurationError(ThreadException):
    code = "configuration"
```

A setup that cannot work: a limit that is not positive, a `system` identity given to `serve` or
`Server.connect`, a `redact` that returns something other than parts, two tools with the same name
in one `Tools`, a `Tool` with no body when it is called, a `ThreadTools` with an empty `thread_id`.

### `ForbiddenError`

```python
class ForbiddenError(ThreadException):
    code = "forbidden"
```

A verb by a non-member, an `authorize` that says no, an edit or retraction of someone else's event,
a signal from a connection that has not joined the thread; on a client, an upgrade refused with HTTP
401 or 403, a socket closed with 1008 before its hello is answered, or a `headers` or `protocols`
function that raised `ForbiddenError`.

### `StorageError`

```python
class StorageError(ThreadException):
    code = "storage"
```

The database failed, or a row's scope is not the call's.

### `TransportError`

```python
class TransportError(ThreadException):
    code = "transport"
```

The socket: a write not acknowledged within its deadline, a client used after `close()`, a
connection that cannot be opened or was lost.

## TypeScript client

`@rfnry/thread` exports these, in the groups above. Events, signals and records are the wire shapes,
fields as they travel; options are camelCase.

### `Identity`

```ts
type Identity = {
  role: 'user' | 'assistant' | 'system'
  id: string
  name?: string | null
  metadata?: Record<string, unknown>
}
```

Anyone who takes part; `role` tells them apart, and `'system'` is reserved for what the library
itself authors.

### `Text`

```ts
type Text = { type: 'text'; text: string }
```

A text part of a message.

### `Image`

```ts
type Image = { type: 'image'; media_type: string; data?: string | null; url?: string | null }
```

An image part: exactly one of `data`, base64, or `url`, which the library carries and never fetches.

### `Audio`

```ts
type Audio = { type: 'audio'; media_type: string; data?: string | null; url?: string | null }
```

An audio part: exactly one of `data`, base64, or `url`, which the library carries and never fetches.

### `Document`

```ts
type Document = {
  type: 'document'
  media_type: string
  data?: string | null
  url?: string | null
  name?: string | null
}
```

A document part, with an optional file `name`: exactly one of `data`, base64, or `url`, which the
library carries and never fetches.

### `Part`

```ts
type Part = Text | Image | Audio | Document
```

Any of the four parts, told apart by `type`.

### `Content`

```ts
type Content = string | Part[]
```

What a message carries; a string is sent as one `Text`.

### `connect`

```ts
async function connect(options: ConnectOptions): Promise<Client>
```

Opens a WebSocket to `options.url`, offering `options.protocols` as the subprotocol list, and
returns the connected client. Raises `ForbiddenError` when the socket is closed with 1008 before the
hello is answered, and `TransportError` when the socket cannot be opened or the hello goes
unanswered. The `protocols` function refuses by throwing `ForbiddenError`: a `ThreadException` it
throws is raised as it is, and anything else as a `TransportError`, which on a reconnect is a failed
attempt, retried with backoff.

### `Client`

```ts
class Client {
  identity: Identity
  scope: string
  readonly status: Status
  readonly closed: Promise<ThreadException | null>
  constructor(options: ConnectOptions)
}
```

A participant's connection, made by `connect`; the constructor alone opens no socket. `identity` and
`scope` are as the server knows them. `status` is the connection's: `'open'` once connected,
`'reconnecting'` from a dropped socket until it is back and every thread joined is joined again, and
`'closed'` for good; listeners hear each change as a `StatusChanged`. `closed` resolves when the
client stops for good: `null` after `close()`, or the error that stopped it, a `ForbiddenError` when
a reconnect is closed with 1008 before its hello is answered, or the `ForbiddenError` or
`ConfigurationError` the `protocols` function throws on a reconnect; anything else it throws is a
failed attempt, retried with backoff. Every method that talks to the server raises `TransportError`
when the client is closed or not connected, except those that write an event, which retry across
reconnects and raise it when not acknowledged within a minute, and raises again, with its class and
message, an error the server raised.

### `Client.createThread`

```ts
async createThread(
  options: { threadId?: string; metadata?: Record<string, unknown> } = {}
): Promise<Thread>
```

Creates a thread with `threadId`, or one the server picks, makes the caller its first member and
returns it; with a `threadId` the scope already holds, returns that thread. Raises `InputError` when
`threadId` is empty, `ForbiddenError` when `authorize` refuses `create`, or the thread exists and
the caller is not a member or may not `read` it.

### `Client.getThread`

```ts
async getThread(threadId: string): Promise<Thread>
```

Returns the thread. Raises `NotFoundError` when the scope holds no such thread, `ForbiddenError`
when the caller is not a member or `authorize` refuses `read`.

### `Client.listThreads`

```ts
async listThreads(
  options: {
    before?: string
    limit?: number
    order?: 'created' | 'activity'
    preview?: boolean
  } = {}
): Promise<Thread[]>
```

Returns the threads the caller is a member of, newest first, up to `limit` (50 when left out);
`before` is a thread id, for the next page. `order: 'activity'` orders them by their latest
message. Each carries `last_seq`, `last_message_at`, `marked` and `unread`, and with `preview: true`
its `last_message`. With an `authorize`, the threads it refuses to `read` are skipped and a page
still holds up to `limit`. Raises `InputError` when `limit` is not between 1 and 500 or `order` is
neither.

### `Client.updateThread`

```ts
async updateThread(threadId: string, metadata: Record<string, unknown>): Promise<Thread>
```

Replaces the thread's metadata, writes `ThreadUpdated` and returns the updated thread. Raises
`NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller is not a
member or `authorize` refuses `manage`.

### `Client.deleteThread`

```ts
async deleteThread(threadId: string): Promise<void>
```

Removes the thread, its members and its log, and sends `ThreadDeleted` to whoever has it open.
Raises `NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller is not
a member or `authorize` refuses `delete`.

### `Client.join`

```ts
async join(threadId: string, options: { since?: number; resume?: boolean } = {}): Promise<Joined>
```

Receives the thread live from now and returns a `Joined`. With `since`, the events after that `seq`
arrive first (`since: 0` for the whole log), then the current state, then live traffic. With
`resume: true`, the events after the caller's mark arrive first instead, as
`follow({ resume: true })` hands each thread, and the `Joined`'s `thread.marked` is that mark. A
thread the connection already receives, by following, keeps arriving live, and `since` or `resume`
hands its events after that `seq` or the mark up to where the connection is, once; joins of it in
flight at the same time each get their own. Raises `InputError` when `since` is negative or given
with `resume`, `NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller
is not a member or `authorize` refuses `read`.

### `Client.leave`

```ts
async leave(threadId: string): Promise<void>
```

Stops receiving the thread live; the caller's open streams and `Working` there end. Leaving a thread
not joined does nothing. A followed thread left stays left, across dropped sockets, until the caller
joins it, follows again or is added to it again.

### `Client.follow`

```ts
async follow(options: { resume?: boolean } = {}): Promise<Joined[]>
```

Receives every thread the caller is a member of, from now on, and each thread it is added to later
from the moment it is added, as a `Joined` to the listeners; returns the `Joined` of each thread it
receives now. With `resume: true`, each thread's events after the caller's mark come first (from
where it was added when it has no mark yet; a membership older than marks starts now), at most the
server's `replay_limit`, beyond which the latest and `truncated: true`. Following is not presence:
no `Present` is announced, and the caller may still signal and receive signals in every thread.
After a dropped socket the client follows again from what it has seen, and leaves again the threads
it had left. Threads `authorize` refuses to `read` are left out. Raises `InputError` for a negative
position.

### `Client.mark`

```ts
async mark(threadId: string, seq: number): Promise<number>
```

Stores the caller's private read position in the thread, never lowering it, and returns the stored
`seq`; when the mark moved, the caller's other connections receive `Marked`. Raises `InputError`
when `seq` is not
between 0 and the thread's last `seq`, `NotFoundError` when the scope holds no such thread,
`ForbiddenError` when the caller is not a member or `authorize` refuses `read`.

### `Client.marked`

```ts
async marked(threadId: string): Promise<number | null>
```

Returns the caller's private read position in the thread, `null` without one; it writes nothing and
tells no one. Raises `NotFoundError` when the scope holds no such thread, `ForbiddenError` when the
caller is not a member or `authorize` refuses `read`.

### `Client.history`

```ts
async history(
  threadId: string,
  options: { before?: number; after?: number; limit?: number; kinds?: string[] } = {}
): Promise<Event[]>
```

Returns the latest `limit` events (50 when left out) below `before` (a `seq`), in `seq` order;
`kinds` keeps only those kinds. With `after` (a `seq`), the first `limit` events after it, in `seq`
order; `before` and `after` together are a range. Raises `InputError` when `after` is negative or
`limit` is not between 1 and 500, `NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller is not a
member or `authorize` refuses `read`.

### `Client.present`

```ts
async present(threadId: string): Promise<Identity[]>
```

Returns who has the thread open now. Raises `NotFoundError` when the scope holds no such thread,
`ForbiddenError` when the caller is not a member or `authorize` refuses `read`.

### `Client.addMember`

```ts
async addMember(threadId: string, identity: Identity): Promise<Member>
```

Adds `identity` to the thread, writes `MemberAdded`, sends `Invited` to their connections and
returns the membership; someone already a member gets theirs back. Raises `InputError` when the
identity does not fit or is `system`, `NotFoundError` when the scope holds no such thread,
`ForbiddenError` when the caller is not a member or `authorize` refuses `manage`.

### `Client.removeMember`

```ts
async removeMember(threadId: string, identity: Identity): Promise<void>
```

Removes `identity` from the thread and writes `MemberRemoved`. Raises `NotFoundError` when the scope
holds no such thread or the identity is not a member, `ForbiddenError` when the caller is not a
member or `authorize` refuses `manage`.

### `Client.listMembers`

```ts
async listMembers(threadId: string): Promise<Member[]>
```

Returns the thread's members in the order they were added. Raises `NotFoundError` when the scope
holds no such thread, `ForbiddenError` when the caller is not a member or `authorize` refuses
`read`.

### `Client.send`

```ts
send(threadId: string, content: Content, options: SendOptions = {}): Promise<Message>
```

Sends a message and returns it as stored; `clientId` is the write's idempotency key, generated when
left out, and a repeat returns the message stored the first time. Raises `InputError` when inline
media is over the cap, the metadata is over 8 KiB or redaction leaves nothing, `NotFoundError` when
the scope holds no such thread, `ForbiddenError` when the caller is not a member or `authorize`
refuses `send`, `ConfigurationError` when `redact` returns something other than parts, and an
`InputError` or `ForbiddenError` that `redact` raised.

### `Client.stream`

```ts
stream(threadId: string, options: StreamOptions = {}): Stream
```

Starts a reply that streams and returns its `Stream`; the message that finishes it carries the
options.

### `Client.step`

```ts
async step(threadId: string, options: StepOptions): Promise<Step>
```

Records one thing an assistant did and returns the stored step; sharing `inReplyTo` with a reply
files it under that reply. Raises `InputError` when inline media in `output` is over the cap,
`NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller is not a
member or `authorize` refuses `send`.

### `Client.edit`

```ts
async edit(
  threadId: string,
  target: string,
  content: Content,
  options: WriteOptions = {}
): Promise<Edit>
```

Replaces the content of the caller's own message `target` and returns the stored edit; the message's
metadata stays. Raises `NotFoundError` when the scope holds no such thread or event,
`ForbiddenError` when `target` is someone else's, the caller is not a member or `authorize` refuses
`send`, `InputError` when `target` is not a message, inline media is over the cap or redaction
leaves nothing.

### `Client.retract`

```ts
async retract(threadId: string, target: string, options: WriteOptions = {}): Promise<Retract>
```

Unsends the caller's own message or step `target`, whose `seq` the log keeps, and returns the stored
retraction. Raises `NotFoundError` when the scope holds no such thread or event, `ForbiddenError`
when `target` is someone else's, the caller is not a member or `authorize` refuses `send`,
`InputError` when `target` is neither a message nor a step.

### `Client.read`

```ts
async read(threadId: string, upToSeq: number): Promise<Read>
```

Records that the caller has read up to `upToSeq` and returns the stored cursor. Raises
`NotFoundError` when the scope holds no such thread, `ForbiddenError` when the caller is not a
member or `authorize` refuses `send`.

### `Client.custom`

```ts
async custom(
  threadId: string,
  name: string,
  payload: Record<string, unknown>,
  options: WriteOptions = {}
): Promise<Custom>
```

Writes an event of your own, `name` and `payload`, and returns it as stored. Raises `InputError`
when `payload` is over 64 KiB, `NotFoundError` when the scope holds no such thread, `ForbiddenError`
when the caller is not a member or `authorize` refuses `send`.

### `Client.working`

```ts
async working(threadId: string, label?: string): Promise<void>
```

Shows the thread that the caller is busy, `label` saying with what, until `idle`, a leave or a
disconnect. Raises `ForbiddenError` when the connection has not joined the thread or `authorize`
refuses `send`, `InputError` when `label` is over 1000 characters.

### `Client.idle`

```ts
async idle(threadId: string): Promise<void>
```

Ends the caller's `Working` in the thread. Raises `ForbiddenError` when the connection has not
joined the thread.

### `Client.cancel`

```ts
async cancel(threadId: string, to: Identity): Promise<void>
```

Asks `to` to stop; what stopping means is its own. Raises `ForbiddenError` when the connection has
not joined the thread or `authorize` refuses `send`, `InputError` when `to` does not fit.

### `Client.subscribe`

```ts
subscribe(listener: Listener, options: { includeOwn?: boolean } = {}): () => void
```

Calls `listener` with every event and signal of the threads joined, each `Invited`, each re-join's
`Joined` and a `StatusChanged` each time `status` changes, from now on; the caller's own are left
out unless `includeOwn`. Returns the function that unsubscribes; a listener that throws does not
stop the others.

### `Client.close`

```ts
async close(): Promise<void>
```

Closes the connection and stops reconnecting; pending methods raise `TransportError`, `status`
becomes `'closed'` and `closed` resolves with `null`. Closing again does nothing.

### `Stream`

```ts
class Stream {
  readonly id: string
  text: string
  constructor(wire: Wire, threadId: string, options: StreamOptions)
}
```

A reply being written, made by `client.stream`, which sends its start at once; `Wire` is the
client's internal channel, so the constructor is not called directly. `text` is all that was
written, and the finishing message is stored with `client_id` equal to `id`.

### `Stream.write`

```ts
write(text: string): void
```

Appends `text` and sends it as the next delta once the start is acknowledged. Never throws.

### `Stream.finish`

```ts
async finish(content?: Content): Promise<Message>
```

Stores the text written, or `content` instead, as the finishing message, ends the stream and returns
the message. Raises `InputError` when the stream has already ended, `ForbiddenError` when the start
was refused because the connection has not joined the thread, and what `Client.send` raises.

### `Stream.abort`

```ts
async abort(): Promise<void>
```

Abandons the stream: `StreamEnd` arrives and nothing is stored. Does nothing once the stream has
ended.

### `ConnectOptions`

```ts
type ConnectOptions = { url: string; protocols?: Protocols | (() => Promise<Protocols>) }
```

Where `connect` goes and what it offers on the upgrade: `protocols`, or an async function returning
them, called on every attempt so a token can refresh. Throw `ForbiddenError` to refuse.

### `Protocols`

```ts
type Protocols = string | string[]
```

The WebSocket subprotocol list offered on the upgrade.

### `Listener`

```ts
type Listener = (item: Item) => void
```

What `Client.subscribe` calls with each item.

### `StreamOptions`

```ts
type StreamOptions = {
  inReplyTo?: string
  triggeredBy?: Identity
  metadata?: Record<string, unknown>
}
```

The options of `Client.stream`: the event it answers, who caused it, and your metadata for the
message, a JSON object of at most 8 KiB.

### `SendOptions`

```ts
type SendOptions = StreamOptions & { clientId?: string }
```

The options of `Client.send`: those of a stream and the write's idempotency key.

### `WriteOptions`

```ts
type WriteOptions = { clientId?: string }
```

The options of `Client.edit`, `Client.retract` and `Client.custom`: the write's idempotency key.

### `StepOptions`

```ts
type StepOptions = Omit<SendOptions, 'metadata'> & {
  name: string
  input?: Record<string, unknown>
  output?: Part[]
  isError?: boolean
  durationMs?: number
}
```

The options of `Client.step`: what was done, with what, what came of it, whether it failed and how
long it took.

### `Message`

```ts
type Message = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'message'
  content: Part[]
  metadata: Record<string, unknown>
}
```

The conversation itself: `content` as parts, `metadata` the sender's own, set once when written.

### `Step`

```ts
type Step = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'step'
  name: string
  input: Record<string, unknown>
  output: Part[]
  is_error: boolean
  duration_ms: number | null
}
```

One thing an assistant did, filed under the reply that shares its `in_reply_to`.

### `Edit`

```ts
type Edit = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'edit'
  target: string
  content: Part[]
}
```

A correction of the author's own message `target`, with its new `content`.

### `Retract`

```ts
type Retract = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'retract'
  target: string
}
```

"Unsend": the author's message or step `target` leaves the view, and the log keeps its `seq`.

### `Read`

```ts
type Read = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'read'
  up_to_seq: number
}
```

A member's read cursor: the author has read up to `up_to_seq`.

### `MemberAdded`

```ts
type MemberAdded = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'member_added'
  identity: Identity
}
```

`identity` joined the membership, added by the author.

### `MemberRemoved`

```ts
type MemberRemoved = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'member_removed'
  identity: Identity
}
```

`identity` left the membership, removed by the author.

### `ThreadUpdated`

```ts
type ThreadUpdated = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'thread_updated'
  metadata: Record<string, unknown>
}
```

The thread's new `metadata`, set by the author.

### `Custom`

```ts
type Custom = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
  kind: 'custom'
  name: string
  payload: Record<string, unknown>
}
```

Yours: a `name` and a `payload` of at most 64 KiB.

### `Event`

```ts
type Event =
  | Message
  | Step
  | Edit
  | Retract
  | Read
  | MemberAdded
  | MemberRemoved
  | ThreadUpdated
  | Custom
```

Any row of a thread's log, told apart by `kind`; the fields before `kind` are the envelope every
event has, and the server stamps `id`, `seq`, `author` and `created_at`.

### `resolve`

```ts
function resolve(events: Event[]): Event[]
```

Applies edits and retractions and returns the current view in `seq` order: each edited message with
its latest content, retracted events left out, and the edits and retractions themselves left out.

### `Working`

```ts
type Working = { thread_id: string; sender: Identity; kind: 'working'; label: string | null }
```

The sender is busy, an assistant's live step or a person typing, with `label` saying what; ends with
`Idle`, a leave or a disconnect.

### `Idle`

```ts
type Idle = { thread_id: string; sender: Identity; kind: 'idle' }
```

Ends the sender's `Working`.

### `StreamStart`

```ts
type StreamStart = {
  thread_id: string
  sender: Identity
  kind: 'stream.start'
  stream_id: string
  in_reply_to: string | null
}
```

A reply began streaming as `stream_id`, answering `in_reply_to`.

### `StreamDelta`

```ts
type StreamDelta = {
  thread_id: string
  sender: Identity
  kind: 'stream.delta'
  stream_id: string
  index: number
  text: string
}
```

The piece of text at `index` of stream `stream_id`.

### `StreamEnd`

```ts
type StreamEnd = { thread_id: string; sender: Identity; kind: 'stream.end'; stream_id: string }
```

Stream `stream_id` ended. Its finishing message always arrives first, so a `StreamEnd` with no
message before it means the stream was abandoned.

### `Cancel`

```ts
type Cancel = { thread_id: string; sender: Identity; kind: 'cancel'; to: Identity }
```

Asks `to` to stop; what stopping means is its own.

### `Present`

```ts
type Present = { thread_id: string; sender: Identity; kind: 'present' }
```

The sender's first connection joined the thread.

### `Left`

```ts
type Left = { thread_id: string; sender: Identity; kind: 'left' }
```

The sender's last connection left the thread or dropped.

### `Invited`

```ts
type Invited = { thread_id: string; sender: Identity; kind: 'invited'; thread: Thread }
```

You were added to `thread`, which you have not joined; `sender` added you.

### `Marked`

```ts
type Marked = { kind: 'marked'; thread_id: string; sender: Identity; seq: number; unread: number }
```

Your private read position in the thread moved to `seq`, with `unread` messages by others still
after it (at most 100); sent, only when the mark moved, to the marking identity's other
connections, never to the one that marked or to anyone else.

### `ThreadDeleted`

```ts
type ThreadDeleted = { thread_id: string; sender: Identity; kind: 'thread.deleted' }
```

The thread is gone.

### `Signal`

```ts
type Signal =
  | Working
  | Idle
  | StreamStart
  | StreamDelta
  | StreamEnd
  | Cancel
  | Present
  | Left
  | Invited
  | ThreadDeleted
  | Marked
```

Anything only true right now, told apart by `kind`, never stored. Every signal carries `thread_id`
and `sender`, stamped by the server.

### `Item`

```ts
type Item = Event | Signal | Joined | StatusChanged
```

What a listener receives: an event, a signal, a re-join's `Joined`, or a `StatusChanged`, the one
item that names no thread.

### `Thread`

```ts
type Thread = {
  id: string
  scope: string
  metadata: Record<string, unknown>
  created_at: string
  last_seq?: number
  last_message_at?: string
  marked?: number
  unread?: number
  last_message?: Message
}
```

A conversation: `id` yours or the server's, `scope` its tenant, `metadata` yours. A listed thread
also carries `last_seq` and `last_message_at` (when its latest message was written); listed for a
member, `marked` (the member's private read position, omitted without one) and `unread` (messages by
others after it, at most 100); with `preview`, `last_message` (the latest message not retracted,
with its edits). Fields without a value are left out on the wire.

### `Member`

```ts
type Member = { thread_id: string; identity: Identity; added_at: string; added_by: Identity }
```

A thread's member: who, since when, and who added them.

### `Joined`

```ts
type Joined = { thread: Thread; seq: number; truncated: boolean }
```

What a join returns, and what a listener receives after a re-join: the thread, its last `seq` when
the join happened, and whether the replay was cut short, or `since` was past that `seq`.

### `Status`

```ts
type Status = 'open' | 'reconnecting' | 'closed'
```

Where the client's connection is, as `client.status` says.

### `StatusChanged`

```ts
type StatusChanged = { kind: 'status'; status: Status }
```

What a listener receives when the client's `status` changes: `'reconnecting'` when its socket drops,
`'open'` after the re-joins that follow, `'closed'` when it stops. It names no thread and never
travels on the wire.

### `ThreadException`

```ts
class ThreadException extends Error {
  readonly code: string = 'internal'
  constructor(message: string)
}
```

The root of every error the client raises; `code` names the class on the wire, and `name` is the
class's name.

### `NotFoundError`

```ts
class NotFoundError extends ThreadException {
  override readonly code: string = 'not_found'
}
```

A thread, member or event the scope does not hold; an empty thread id names none, except in
`createThread`, an `InputError`, and in a signal, which needs a join first.

### `InputError`

```ts
class InputError extends ThreadException {
  override readonly code: string = 'input'
}
```

A request the library refuses: an empty thread id given to `createThread`, an identity whose fields
do not fit, a value over its limit, a join given both `since` and `resume`, an edit or retraction of
the wrong kind of event, a stream finished twice.

### `ConfigurationError`

```ts
class ConfigurationError extends ThreadException {
  override readonly code: string = 'configuration'
}
```

A server that cannot work, such as a `redact` that returns something other than parts.

### `ForbiddenError`

```ts
class ForbiddenError extends ThreadException {
  override readonly code: string = 'forbidden'
}
```

A verb by a non-member, an `authorize` that says no, an edit or retraction of someone else's event,
a signal from a connection that has not joined the thread, a socket closed with 1008 before its
hello is answered, a `protocols` function that threw `ForbiddenError`.

### `StorageError`

```ts
class StorageError extends ThreadException {
  override readonly code: string = 'storage'
}
```

The server's database failed.

### `TransportError`

```ts
class TransportError extends ThreadException {
  override readonly code: string = 'transport'
}
```

The socket: a write not acknowledged within its deadline, a client used after `close()`, a
connection that cannot be opened or was lost.
