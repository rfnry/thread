# rfnry-thread Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build rfnry-thread from scratch — one WebSocket protocol, a Python server, a Python client and a TypeScript client — exactly as `docs/thread.md` documents it.

**Architecture:** A `Server` runs one session per connection (a WebSocket the consumer accepted, or an in-process pipe), dispatching protocol requests to verbs that check tenancy, membership and `authorize`, write the log through a `Storage`, and fan out through a per-replica `Hub` that keeps a per-connection `seq` cursor (ordered, complete delivery), a latest-state cache for signals, and a bounded outbound queue per connection. Replicas share one Postgres database: `append_event` commits and `NOTIFY`s in one transaction, signals are mirrored as `NOTIFY` payloads, and `InMemoryStorage` implements the same bus in-process so the unit tier proves two replicas. The Python `Client` (socket or in-process, same class) and the TypeScript client share an outbox, reconnect, resume and golden protocol fixtures.

**Tech Stack:** Python 3.12, pydantic 2, websockets (client transport), asyncpg (optional, `[postgres]`), pytest + pytest-asyncio, ruff, mypy strict, uv, hatchling, poethepoet. TypeScript 6 strict, `tsc` build (ESM only), vitest, Biome; Node ≥ 22 (global `WebSocket`).

**Spec:** `docs/plans/2026-09-24-thread-design.md` (read §16, the revisions made while writing the docs; §17, the owner's answers: the test database, re-LISTEN, caller-supplied `client_id`; and §18, which supersedes hello-token auth: the consumer authenticates the upgrade and calls `serve(connection, identity, scope)`) and the contract `docs/thread.md`. Where they differ, `docs/thread.md` wins; tell the human.

## Global Constraints

- Layout: `packages/python` (package `rfnry-thread`, import `rfnry_thread`) and `packages/typescript` (`@rfnry/thread`); shared golden fixtures in `fixtures/protocol/` at the repository root; examples in `examples/<name>/`.
- Python: `requires-python = ">=3.12"`; dependencies exactly `pydantic>=2.13.4`, `websockets>=15.0`; extra `postgres = ["asyncpg>=0.31.0,<1.0"]`. The library imports no web framework, no model SDK, and never imports asyncpg (the pool is passed in).
- ruff line-length 100, rules `E F I W B UP SIM`, `ignore = ["E501"]`; mypy `strict = true`; pytest `asyncio_mode = "auto"`, markers `postgres` and `live`, `addopts = "-m 'not postgres and not live'"`.
- Gate for every Python commit, from `packages/python`: `uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`. Postgres tier: `RFNRY_TEST_POSTGRES_URL=postgresql://thread:thread@127.0.0.1:55436/thread uv run poe test:integration`, no skips before the Postgres task is committed. That database is the `rfnry-thread-pg` container (restart policy set, dedicated to this repo; the tests truncate it freely).
- Gate for every TypeScript commit, from `packages/typescript`: `npm run check && npm run typecheck && npm test`.
- No comments, no docstrings (CLAUDE.md engineering rule 4). Names from the docs, verbatim.
- Rule 9: every error message starts with its module's file stem, then a lowercase sentence; identifiers keep their case: `server: thread 'support-ana' was not found`. Logs are lowercase; the library logs only unexpected failures inside `serve`, as warnings on the `rfnry_thread` logger.
- Errors: root `ThreadException`; children `NotFoundError`, `InputError`, `ConfigurationError`, `ForbiddenError`, `StorageError`, `TransportError`. Wire codes `internal`, `not_found`, `input`, `configuration`, `forbidden`, `storage`, `transport`.
- Tenancy: `scope` is required on every connection and thread, first argument of every storage method that reads or writes a row (`initialize` and `listen` are the only exceptions); no default scope, no cross-scope read; every row carries it. Authorship is stamped from the connection, never read from a client payload.
- Authentication is the consumer's, at the WebSocket upgrade: `Server` has no `authenticate`, `hello` carries only the protocol version, and there is no hello deadline. `server.serve(connection, identity, scope)` is told who the socket is.
- `Identity.role == "system"` is reserved for the library: `serve` or `server.connect` given one is a `ConfigurationError`; adding one as a member is an `InputError`.
- Family blocks are copied, never imported: the parts block (`class Text` … `Content = str | list[Part]`) and the tools block (`Result = ` … `if n not in names})`) byte-identical with `../engine`; `class Identity` byte-identical with `../senses` when that checkout has it.
- Tables `thread_threads`, `thread_members`, `thread_events`; `NOTIFY` channels `thread_events` and `thread_signals`; notify payloads ≤ 7900 bytes.
- Limits (constants, from the spec): inline media 256 KiB per event (`Server(max_inline_bytes=)`), replay 500 (`Server(replay_limit=)`), custom payload 64 KiB, outbound bound 1000 frames with a 10 s stall window, outbox deadline 60 s with backoff 0.5 s doubling to 8 s, reconnect backoff full jitter 0.5 s doubling to 30 s (the client's reconnect and the Postgres re-`LISTEN` alike), state re-announcement 10 s and expiry 30 s, `Working.label` ≤ 1000 characters.
- Close codes: 4000 protocol error, 4001 too slow. Protocol version `1`.
- Examples are standalone uv projects; keys (and `RFNRY_EXAMPLES_POSTGRES_URL`, the same database URL as the tests) come from `/home/frndvrgs/software/rfnry/.env.examples` via `uv run --env-file ../../../.env.examples main.py`, never committed; `examples/**/uv.lock` and `.env*` are git-ignored.
- Every commit message ends with the line `Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB`.

## Pre-change checklist (answered once for the whole build)

1. Wanted two generations ahead? Yes — durable ordered conversation, delivery, tenancy and identity do not shrink as models grow.
2. More or less useful as models improve? More: substrate (protocol, log, delivery, tenancy).
3. Not applicable (nothing here is compensation).
4. Exists because a model is weak at X? No. Nothing filters, summarizes or fits content for a model; `read_recent` hands parts as sent.
5. Generation only, decision only, both? The thread carries whatever a participant sends; nothing assumes a kind of model. `ThreadTools` is a `Tools`, usable by any loop that calls tools.

## Review Focus

1. **One delta frame shared by many connections.** The hub builds one `Push` for a `stream.delta` and hands it to every joined connection; merging in one slow connection's queue must not change what the others send. Pinned in Task 6 (`test_merging_never_touches_a_frame_other_queues_hold`).
2. **The same write arriving twice at once.** An outbox retry can race its original (two connections of one client, or two replicas): exactly one event is stored, both callers get it, and `seq` stays gapless. Pinned in Task 5 (`test_concurrent_duplicates_store_one_event_and_seq_stays_gapless`, 20 concurrent writers; it runs on in-memory there and on Postgres from Task 15) and Task 13 (`test_a_write_whose_ack_was_lost_is_stored_exactly_once`).
3. **A stream's message versus its `stream.end` on another replica.** The UI rule "a `stream.end` with no message before it means abandoned" breaks if a remote replica reads the event after it pushes the signal. Pinned in Task 11 (`test_the_finishing_message_reaches_a_remote_connection_before_stream_end`) and again on Postgres in Task 15.
4. **A person with two tabs.** Closing one tab must not announce `Left`; opening the second must not announce `Present` twice; only the last tab closing does. Pinned in Task 8 (`test_two_connections_of_one_member_are_one_presence`) and Task 10 (`test_two_tabs_are_one_presence_over_the_server`).
5. **A member removed while the thread is open.** After `MemberRemoved` reaches them, they receive nothing more from the thread and cannot signal in it — on every replica. Pinned in Task 8 (`test_a_removed_member_is_cut_off_after_the_event`), Task 9 (`test_a_removed_member_stops_receiving_over_the_server`), Task 11 and Task 15 (`test_removal_cuts_off_a_member_on_another_replica`).
6. **A replica whose `LISTEN` connection drops.** It must not go deaf for good: it re-acquires with jittered backoff, subscribes again, republishes its own state, and events said during the gap still arrive in order. Pinned in Task 11 (`test_a_resumed_listener_republishes_this_replicas_state`) and Task 15 (`test_a_dropped_listener_listens_again_and_resumes`, `test_signals_and_events_cross_again_after_the_listeners_drop`).

## File Structure

```
.gitignore
fixtures/protocol/*.json                      golden frames, written by Python, read by both suites
packages/python/
  pyproject.toml  README.md
  src/rfnry_thread/
    __init__.py        public names
    errors.py          the error family and wire codes
    models.py          family parts block, Identity, Thread, Member, events, signals, Joined
    resolve.py         resolve(events)
    protocol.py        frames, encode/decode, methods, close codes
    storage.py         Storage protocol, bus callback types
    inmemory.py        InMemoryStorage (rows + in-process bus)
    postgres.py        PostgresStorage (rows + LISTEN/NOTIFY)
    outbound.py        per-connection outbound queue: backpressure, delta merging
    state.py           latest-state cache
    hub.py             Session and Hub: per-replica fan-out, cursors, joins, presence, mirroring
    connection.py      the Connection protocol
    verbs.py           the verbs: membership, authorize, validation, the log
    server.py          Server: serve(connection, identity, scope), hello, frame loop, method table, connect/start/stop, system verbs
    pipe.py            in-process Connection pair
    client.py          Client, Stream, outbox, reconnect, resume
    websocket.py       the WebSocket transport of Client.connect
    tools.py           family Tool/Tools block, ThreadTools
  tests/
    unit/     conftest.py peer.py + one test file per task
    integration/  conftest.py test_storage.py test_replicas.py test_tenancy.py
    e2e/      conftest.py test_typescript.py
packages/typescript/
  package.json tsconfig.json tsconfig.build.json biome.json vitest.config.ts README.md
  src/  index.ts errors.ts types.ts protocol.ts resolve.ts timing.ts outbox.ts client.ts
  tests/ errors.test.ts golden.test.ts resolve.test.ts outbox.test.ts client.test.ts docs.test.ts fakes.ts
  tests/e2e/live.test.ts
examples/  README.md and one folder per example
```

Old code worth mining (read-only, `../rfnry-thread`), named where a task adapts it:
`packages/client-python/src/rfnry_thread_client/outbox.py` (per-thread FIFO + deadline + wake),
`packages/client-python/src/rfnry_thread_client/transport/reconnect.py` (full-jitter backoff, re-hello; its auth is not kept),
`packages/server-python/src/rfnry_thread_server/transport/session_loop.py` (frame loop, teardown; its hello deadline and auth are not kept),
`packages/server-python/src/rfnry_thread_server/transport/outbound.py` (bound + stall window),
`packages/server-python/src/rfnry_thread_server/store/postgres/{store.py,schema.sql}` (dedup index, `ON CONFLICT`),
`scripts/generate_fixtures.py` and `packages/client-react/tests/protocol/fixtures.test.ts` (fixture discipline),
`packages/client-react/src/outbox.ts` and `transport/reconnect.ts` (TypeScript outbox and backoff).
Everything else in the old tree is stripped (design §1).

---

### Task 1: Scaffold and the error family

Written fresh (the engine's `pyproject.toml` is the model).

**Files:**
- Create: `.gitignore`, `packages/python/pyproject.toml`, `packages/python/README.md`, `packages/python/src/rfnry_thread/__init__.py`, `packages/python/src/rfnry_thread/py.typed`, `packages/python/src/rfnry_thread/errors.py`
- Create: `packages/python/tests/unit/__init__.py`, `packages/python/tests/integration/__init__.py`, `packages/python/tests/e2e/__init__.py`
- Test: `packages/python/tests/unit/test_errors.py`

**Interfaces:**
- Produces: `ThreadException` (`code: ClassVar[str] = "internal"`), `NotFoundError` (`"not_found"`), `InputError` (`"input"`), `ConfigurationError` (`"configuration"`), `ForbiddenError` (`"forbidden"`), `StorageError` (`"storage"`), `TransportError` (`"transport"`); `raised(code: str, message: str) -> ThreadException` in `rfnry_thread.errors`.

- [ ] **Step 1: Write the scaffold**

`.gitignore` (repository root):

```
.venv/
__pycache__/
.pytest_cache/
.mypy_cache/
.ruff_cache/
dist/
node_modules/
.env*
examples/**/uv.lock
examples/**/package-lock.json
```

`packages/python/pyproject.toml`:

```toml
[project]
name = "rfnry-thread"
version = "0.1.0"
description = "Realtime conversation where people and AI assistants are equal participants"
readme = "README.md"
requires-python = ">=3.12"
authors = [{name = "rfnry", email = "contact@rfnry.dev"}]
dependencies = [
    "pydantic>=2.13.4",
    "websockets>=15.0",
]

[project.optional-dependencies]
postgres = ["asyncpg>=0.31.0,<1.0"]
dev = [
    "pytest>=9.1.1",
    "pytest-asyncio>=1.4.0",
    "ruff>=0.16.4",
    "mypy>=2.3.1",
    "poethepoet>=0.48.0",
    "asyncpg>=0.31.0,<1.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/rfnry_thread"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
pythonpath = ["src", "tests"]
markers = [
    "postgres: needs a real Postgres (set RFNRY_TEST_POSTGRES_URL)",
    "live: runs the TypeScript client against a live server (needs node and npm)",
]
addopts = "-m 'not postgres and not live'"

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.format]
exclude = ["src/rfnry_thread/models.py", "src/rfnry_thread/tools.py"]

[tool.ruff.lint]
select = ["E", "F", "I", "W", "B", "UP", "SIM"]
ignore = ["E501"]

[tool.ruff.lint.isort]
combine-as-imports = true

[tool.poe.tasks]
format = { cmd = "ruff format src tests" }
check = { cmd = "ruff check src tests" }
"check:fix" = { cmd = "ruff check --fix src tests" }
typecheck = { cmd = "mypy" }
test = { cmd = "pytest -q" }
"test:integration" = { cmd = "pytest -q -m postgres" }
"test:e2e" = { cmd = "pytest -q -m live" }
fixtures = { cmd = "pytest -q tests/unit/test_golden.py", env = { RFNRY_WRITE_FIXTURES = "1" } }

[tool.mypy]
strict = true
files = ["src/rfnry_thread", "tests"]

[[tool.mypy.overrides]]
module = "asyncpg.*"
ignore_missing_imports = true
```

`packages/python/README.md`:

```markdown
# rfnry-thread

Realtime conversation where people and AI assistants are equal participants with the same API.
The contract is [`docs/thread.md`](../../docs/thread.md).
```

`packages/python/src/rfnry_thread/__init__.py` starts as `__version__ = "0.1.0"`; `py.typed` is empty; the three `__init__.py` under `tests/` are empty (no `tests/__init__.py`: the tiers import each other as `unit.…`, as in the engine).

- [ ] **Step 2: Write the failing test**

`packages/python/tests/unit/test_errors.py`:

```python
import pytest

from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    NotFoundError,
    StorageError,
    ThreadException,
    TransportError,
    raised,
)

FAMILY = [NotFoundError, InputError, ConfigurationError, ForbiddenError, StorageError, TransportError]


@pytest.mark.parametrize("cls", FAMILY)
def test_every_child_is_a_thread_exception_named_error(cls: type[ThreadException]) -> None:
    assert issubclass(cls, ThreadException)
    assert cls.__name__.endswith("Error")


def test_the_names_do_not_shadow_builtins() -> None:
    assert not issubclass(ForbiddenError, PermissionError)
    assert not issubclass(TransportError, ConnectionError)


@pytest.mark.parametrize("cls", [ThreadException, *FAMILY])
def test_a_wire_code_raises_the_same_class_with_the_same_message(cls: type[ThreadException]) -> None:
    error = raised(cls.code, "server: thread 'x' was not found")
    assert type(error) is cls
    assert str(error) == "server: thread 'x' was not found"


def test_an_unknown_code_raises_the_root() -> None:
    assert type(raised("teapot", "server: no")) is ThreadException
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cd packages/python && uv sync --extra dev && uv run pytest tests/unit/test_errors.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'rfnry_thread.errors'`

- [ ] **Step 4: Implement**

`packages/python/src/rfnry_thread/errors.py`:

```python
from typing import ClassVar


class ThreadException(Exception):
    code: ClassVar[str] = "internal"


class NotFoundError(ThreadException):
    code = "not_found"


class InputError(ThreadException):
    code = "input"


class ConfigurationError(ThreadException):
    code = "configuration"


class ForbiddenError(ThreadException):
    code = "forbidden"


class StorageError(ThreadException):
    code = "storage"


class TransportError(ThreadException):
    code = "transport"


ERRORS: dict[str, type[ThreadException]] = {
    cls.code: cls
    for cls in (
        ThreadException,
        NotFoundError,
        InputError,
        ConfigurationError,
        ForbiddenError,
        StorageError,
        TransportError,
    )
}


def raised(code: str, message: str) -> ThreadException:
    return ERRORS.get(code, ThreadException)(message)
```

- [ ] **Step 5: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS (15 tests).

- [ ] **Step 6: Commit**

```bash
git add .gitignore packages/python
git commit -m "feat: scaffold rfnry-thread and the error family

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 2: Family blocks — parts, Identity, Tool/Tools

Copied byte for byte from `../engine/packages/python/src/rfnry_engine/models.py` (parts) and `../engine/packages/python/src/rfnry_engine/tools/tools.py` (tools); `Identity` from the design §2.

**Files:**
- Create: `packages/python/src/rfnry_thread/models.py`, `packages/python/src/rfnry_thread/tools.py`
- Test: `packages/python/tests/unit/test_models.py`, `packages/python/tests/unit/test_family.py`, `packages/python/tests/unit/test_tools.py`

**Interfaces:**
- Consumes: `InputError`, `ConfigurationError` (Task 1).
- Produces: `Text`, `Image`, `Audio`, `Document`, `Part`, `Content`, `Identity` in `rfnry_thread.models`; `Result`, `Body`, `ToolsLike`, `Tool`, `Tools` in `rfnry_thread.tools` with the engine's exact signatures (`Tool(name, description="", body=None, args=None, input_schema=None)`, `Tools(tools: Iterable[Tool | ToolsLike] = ())`, `definitions() -> list[dict[str, Any]]`, `async dispatch(name, args) -> Result`, `__add__`, `only(*names)`, `without(*names)`, `names`).

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_models.py`:

```python
import pytest
from pydantic import TypeAdapter

from rfnry_thread.errors import InputError
from rfnry_thread.models import Document, Identity, Image, Part, Text

PARTS: TypeAdapter[list[Part]] = TypeAdapter(list[Part])


def test_parts_parse_by_their_type() -> None:
    parsed = PARTS.validate_python([
        {"type": "text", "text": "hi"},
        {"type": "image", "media_type": "image/png", "url": "https://x/a.png"},
        {"type": "document", "media_type": "application/pdf", "name": "a.pdf", "url": "https://x/a.pdf"},
    ])
    assert [type(p) for p in parsed] == [Text, Image, Document]


@pytest.mark.parametrize("fields", [{}, {"data": b"x", "url": "https://x"}])
def test_a_media_part_carries_data_or_url_not_both(fields: dict[str, object]) -> None:
    with pytest.raises(InputError, match="^models: image carries data or url, not both$"):
        Image.model_validate({"media_type": "image/png", **fields})


def test_bytes_travel_as_standard_base64() -> None:
    image = Image(media_type="image/png", data=b"\xfb\xff")
    assert image.model_dump(mode="json")["data"] == "+/8="
    assert Image.model_validate_json(image.model_dump_json()).data == b"\xfb\xff"


def test_an_identity_is_one_class_with_a_role() -> None:
    ana = Identity(role="user", id="ana", name="Ana")
    assert ana.metadata == {}
    assert Identity.model_validate(ana.model_dump()) == ana
    with pytest.raises(ValueError):
        Identity.model_validate({"role": "robot", "id": "x"})
```

`packages/python/tests/unit/test_family.py`:

```python
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[4]
SRC = "packages/python/src"

BLOCKS = {
    "parts": ("engine", "rfnry_thread/models.py", "rfnry_engine/models.py", "class Text(BaseModel):", "Content = str | list[Part]\n"),
    "tools": ("engine", "rfnry_thread/tools.py", "rfnry_engine/tools/tools.py", "Result = ", "if n not in names})\n"),
    "identity": ("senses", "rfnry_thread/models.py", "rfnry_senses/models.py", "class Identity(BaseModel):", "    metadata: dict[str, Any] = {}\n"),
}


def cut(path: Path, start: str, end: str) -> str:
    text = path.read_text(encoding="utf-8")
    first = text.index(start)
    return text[first : text.index(end, first) + len(end)]


@pytest.mark.parametrize("name", BLOCKS)
def test_the_shared_block_is_byte_identical_with_the_sibling_library(name: str) -> None:
    sibling, own, theirs, start, end = BLOCKS[name]
    path = ROOT.parent / sibling / SRC / theirs
    if not path.exists() or start not in path.read_text(encoding="utf-8"):
        pytest.skip(f"no {start!r} in a sibling checkout at {path}")
    assert cut(ROOT / SRC / own, start, end) == cut(path, start, end)
```

`packages/python/tests/unit/test_tools.py`:

```python
from typing import Any

import pytest
from pydantic import BaseModel

from rfnry_thread.errors import ConfigurationError, InputError
from rfnry_thread.tools import Tool, Tools


class EchoArgs(BaseModel):
    text: str


async def echo(args: dict[str, Any]) -> str:
    return str(args["text"])


def test_tools_combine_narrow_and_refuse_duplicates() -> None:
    tools = Tools([Tool("echo", "Echo", body=echo, args=EchoArgs)]) + Tools([Tool("other", body=echo)])
    assert tools.names == ["echo", "other"]
    assert tools.only("echo").names == ["echo"]
    assert tools.without("echo").names == ["other"]
    with pytest.raises(ConfigurationError, match="^tools: duplicate tool name 'echo'$"):
        tools + Tools([Tool("echo", body=echo)])


async def test_dispatch_validates_arguments_and_names() -> None:
    tools = Tools([Tool("echo", "Echo", body=echo, args=EchoArgs)])
    assert await tools.dispatch("echo", {"text": "hi"}) == "hi"
    with pytest.raises(InputError, match="^tools: invalid arguments for echo"):
        await tools.dispatch("echo", {})
    with pytest.raises(InputError, match="^tools: unknown tool 'ghost'$"):
        await tools.dispatch("ghost", {})
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_models.py tests/unit/test_tools.py tests/unit/test_family.py -q`
Expected: FAIL — `No module named 'rfnry_thread.models'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/models.py` (this task writes the head of the file; Task 3 appends):

```python
from __future__ import annotations

import base64
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_serializer, model_validator

from rfnry_thread.errors import InputError


class Text(BaseModel):
    type: Literal["text"] = "text"
    text: str


class _Media(BaseModel):
    model_config = ConfigDict(val_json_bytes="base64")

    type: str
    media_type: str
    data: bytes | None = None
    url: str | None = None

    @model_validator(mode="after")
    def _data_or_url(self) -> Self:
        if (self.data is None) == (self.url is None):
            raise InputError(f"models: {self.type} carries data or url, not both")
        return self

    @field_serializer("data", when_used="json-unless-none")
    def _base64(self, data: bytes) -> str:
        return base64.b64encode(data).decode()


class Image(_Media):
    type: Literal["image"] = "image"


class Audio(_Media):
    type: Literal["audio"] = "audio"


class Document(_Media):
    type: Literal["document"] = "document"
    name: str | None = None


Part = Annotated[Text | Image | Audio | Document, Field(discriminator="type")]
Content = str | list[Part]


class Identity(BaseModel):
    role: Literal["user", "assistant", "system"]
    id: str
    name: str | None = None
    metadata: dict[str, Any] = {}
```

(Task 3 adds `from datetime import datetime` and `TypeAdapter` to these imports. The cut markers must stay exactly as written: the family test cuts from `class Text(BaseModel):` to `Content = str | list[Part]`.)

`packages/python/src/rfnry_thread/tools.py` (the engine block verbatim, from `Result = ` to the end of `without`; Task 14 appends `ThreadTools`):

```python
from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from functools import partial
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ValidationError

from rfnry_thread.errors import ConfigurationError, InputError
from rfnry_thread.models import Part

Result = str | dict[str, Any] | list[Part]
Body = Callable[[dict[str, Any]], Awaitable[Result]]
Dispatcher = Callable[[dict[str, Any]], Awaitable[Result]]


@runtime_checkable
class ToolsLike(Protocol):
    def definitions(self) -> list[dict[str, Any]]: ...

    async def dispatch(self, name: str, args: dict[str, Any]) -> str | dict[str, Any] | list[Any]: ...


@dataclass(frozen=True)
class Tool:
    name: str
    description: str = ""
    body: Body | None = None
    args: type[BaseModel] | None = None
    input_schema: dict[str, Any] | None = None

    def schema(self) -> dict[str, Any]:
        if self.input_schema is not None:
            return self.input_schema
        if self.args is not None:
            return self.args.model_json_schema()
        return {"type": "object", "properties": {}}

    def definition(self) -> dict[str, Any]:
        return {"name": self.name, "description": self.description, "input_schema": self.schema()}

    async def call(self, args: dict[str, Any]) -> Result:
        if self.body is None:
            raise ConfigurationError(f"tools: tool {self.name!r} has no body")
        if self.args is not None:
            try:
                args = self.args.model_validate(args).model_dump(exclude_unset=True)
            except ValidationError as exc:
                raise InputError(f"tools: invalid arguments for {self.name}: {exc.errors(include_url=False)}") from exc
        return await self.body(args)


class Tools:
    def __init__(self, tools: Iterable[Tool | ToolsLike] = ()) -> None:
        self._entries: dict[str, tuple[dict[str, Any], Dispatcher]] = {}
        for item in tools:
            if isinstance(item, Tool):
                self._put(item.definition(), item.call)
            else:
                for definition in item.definitions():
                    self._put(dict(definition), partial(item.dispatch, definition["name"]))

    def _put(self, definition: dict[str, Any], dispatcher: Dispatcher) -> None:
        name = definition["name"]
        if name in self._entries:
            raise ConfigurationError(f"tools: duplicate tool name {name!r}")
        self._entries[name] = (definition, dispatcher)

    @classmethod
    def _from(cls, entries: dict[str, tuple[dict[str, Any], Dispatcher]]) -> Tools:
        tools = cls()
        tools._entries = dict(entries)
        return tools

    @property
    def names(self) -> list[str]:
        return list(self._entries)

    def __contains__(self, name: str) -> bool:
        return name in self._entries

    def __len__(self) -> int:
        return len(self._entries)

    def definitions(self) -> list[dict[str, Any]]:
        return [dict(definition) for definition, _ in self._entries.values()]

    async def dispatch(self, name: str, args: dict[str, Any]) -> Result:
        entry = self._entries.get(name)
        if entry is None:
            raise InputError(f"tools: unknown tool {name!r}")
        return await entry[1](args)

    def __add__(self, other: Tools | ToolsLike) -> Tools:
        return Tools([self, other])

    def only(self, *names: str) -> Tools:
        missing = [n for n in names if n not in self._entries]
        if missing:
            raise InputError(f"tools: unknown tools {missing}")
        return Tools._from({n: self._entries[n] for n in names})

    def without(self, *names: str) -> Tools:
        return Tools._from({n: e for n, e in self._entries.items() if n not in names})
```

Before committing, diff the blocks by eye against the engine files: `diff <(sed -n '/^Result = /,/if n not in names})$/p' packages/python/src/rfnry_thread/tools.py) <(sed -n '/^Result = /,/if n not in names})$/p' ../engine/packages/python/src/rfnry_engine/tools/tools.py)` prints nothing. `ruff format` would rewrap the blocks (the engine never formats them), so `pyproject.toml` excludes `models.py` and `tools.py` from `ruff format`, as rfnry-senses does; `ruff check` still lints them, and code appended to those files is written formatted by hand.

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS; `test_family.py` passes `parts` and `tools`, and `identity` too once `../senses` has its `Identity` (it skips while it does not).

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: the family blocks — parts, Identity, Tool and Tools, pinned byte for byte

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 3: The shapes — threads, members, events, signals, `resolve`

Written fresh. The old `types/event.py` had twenty kinds; nine remain (design §4), the signal set replaces runs, typing and stream frames (design §5). `resolve` adapts the idea of `../rfnry-thread/packages/client-python/src/rfnry_thread_client/resolution.py` and returns plain events.

**Files:**
- Modify: `packages/python/src/rfnry_thread/models.py` (append)
- Create: `packages/python/src/rfnry_thread/resolve.py`
- Test: `packages/python/tests/unit/test_shapes.py`, `packages/python/tests/unit/test_resolve.py`, `packages/python/tests/unit/factories.py`

**Interfaces:**
- Consumes: parts and `Identity` (Task 2).
- Produces (in `rfnry_thread.models`): `SYSTEM: Identity`; `Thread(id, scope, metadata={}, created_at)`; `Member(thread_id, identity, added_at, added_by)`; events `Message(content)`, `Step(name, input={}, output=[], is_error=False, duration_ms=None)`, `Edit(target, content)`, `Retract(target)`, `Read(up_to_seq)`, `MemberAdded(identity)`, `MemberRemoved(identity)`, `ThreadUpdated(metadata)`, `Custom(name, payload)`, each with the envelope `id, thread_id, seq=0, author, created_at, client_id, in_reply_to=None, triggered_by=None` and a `kind` literal; `Event` (discriminated on `kind`) and `EVENTS: TypeAdapter[Event]`; signals `Working(label=None)`, `Idle`, `StreamStart(stream_id, in_reply_to=None)`, `StreamDelta(stream_id, index, text)`, `StreamEnd(stream_id)`, `Cancel(to)`, `Present`, `Left`, `Invited(thread)`, `ThreadDeleted`, each with `thread_id, sender` and a `kind` literal (`"working"`, `"idle"`, `"stream.start"`, `"stream.delta"`, `"stream.end"`, `"cancel"`, `"present"`, `"left"`, `"invited"`, `"thread.deleted"`); `Signal` and `SIGNALS: TypeAdapter[Signal]`; `CLIENT_EVENTS`, `CLIENT_SIGNALS: frozenset[str]`; `Joined(thread, seq, truncated=False)`; `parts(content: Content) -> list[Part]`.
- Produces: `resolve(events: Sequence[Event]) -> list[Event]` in `rfnry_thread.resolve`.
- Produces (tests): `tests/unit/factories.py` with `AT`, `ANA`, `HELPER`, `BRUNO` and `message(seq, text, author=ANA, event_id=None) -> Message`, used by later tasks.

- [ ] **Step 1: Write the factories and the failing tests**

`packages/python/tests/unit/factories.py`:

```python
from datetime import UTC, datetime

from rfnry_thread.models import Identity, Message, Text

AT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
ANA = Identity(role="user", id="ana", name="Ana")
BRUNO = Identity(role="user", id="bruno", name="Bruno")
HELPER = Identity(role="assistant", id="helper", name="Helper")


def message(seq: int, text: str, author: Identity = ANA, event_id: str | None = None) -> Message:
    return Message(
        id=event_id or f"e{seq}",
        thread_id="t1",
        seq=seq,
        author=author,
        created_at=AT,
        client_id=f"c{seq}",
        content=[Text(text=text)],
    )
```

`packages/python/tests/unit/test_shapes.py`:

```python
import pytest
from pydantic import ValidationError

from rfnry_thread.models import (
    CLIENT_EVENTS,
    CLIENT_SIGNALS,
    EVENTS,
    SIGNALS,
    Custom,
    Image,
    Joined,
    Message,
    Present,
    Step,
    StreamDelta,
    Text,
    Thread,
    parts,
)

from .factories import ANA, AT, HELPER


def envelope(kind: str, **fields: object) -> dict[str, object]:
    return {"kind": kind, "id": "e1", "thread_id": "t1", "seq": 1, "author": ANA.model_dump(),
            "created_at": AT.isoformat(), "client_id": "c1", **fields}


def test_events_parse_by_kind() -> None:
    assert isinstance(EVENTS.validate_python(envelope("message", content=[{"type": "text", "text": "hi"}])), Message)
    step = EVENTS.validate_python(envelope("step", name="Read", input={"path": "a.md"}, output=[]))
    assert isinstance(step, Step) and step.is_error is False and step.duration_ms is None
    custom = EVENTS.validate_python(envelope("custom", name="rating", payload={"stars": 5}))
    assert isinstance(custom, Custom) and custom.name == "rating"


def test_an_unknown_event_kind_is_refused() -> None:
    with pytest.raises(ValidationError):
        EVENTS.validate_python(envelope("run.started"))


def test_signals_parse_by_kind_and_carry_their_sender() -> None:
    delta = SIGNALS.validate_python({"kind": "stream.delta", "thread_id": "t1", "sender": HELPER.model_dump(),
                                     "stream_id": "s1", "index": 0, "text": "Re"})
    assert isinstance(delta, StreamDelta) and delta.sender == HELPER
    present = SIGNALS.validate_python({"kind": "present", "thread_id": "t1", "sender": ANA.model_dump()})
    assert isinstance(present, Present)


def test_clients_may_send_only_these_kinds() -> None:
    assert CLIENT_EVENTS == {"message", "step", "edit", "retract", "read", "custom"}
    assert CLIENT_SIGNALS == {"working", "idle", "stream.start", "stream.delta", "stream.end", "cancel"}


def test_content_is_a_string_or_parts() -> None:
    assert parts("hi") == [Text(text="hi")]
    image = Image(media_type="image/png", url="https://x/a.png")
    assert parts([Text(text="a"), image]) == [Text(text="a"), image]


def test_joined_round_trips() -> None:
    joined = Joined(thread=Thread(id="t1", scope="acme", created_at=AT), seq=7, truncated=True)
    assert Joined.model_validate_json(joined.model_dump_json()) == joined
```

`packages/python/tests/unit/test_resolve.py`:

```python
from rfnry_thread.models import Edit, Message, Retract, Step, Text
from rfnry_thread.resolve import resolve

from .factories import ANA, AT, HELPER, message


def edit(seq: int, target: str, text: str) -> Edit:
    return Edit(id=f"e{seq}", thread_id="t1", seq=seq, author=ANA, created_at=AT, client_id=f"c{seq}",
                target=target, content=[Text(text=text)])


def retract(seq: int, target: str) -> Retract:
    return Retract(id=f"e{seq}", thread_id="t1", seq=seq, author=ANA, created_at=AT, client_id=f"c{seq}", target=target)


def test_the_latest_edit_wins_and_edits_leave_the_view() -> None:
    view = resolve([message(1, "hello"), edit(2, "e1", "hullo"), edit(3, "e1", "hello!")])
    assert len(view) == 1 and isinstance(view[0], Message)
    assert (view[0].id, view[0].content) == ("e1", [Text(text="hello!")])


def test_a_retracted_event_and_the_retraction_leave_the_view() -> None:
    step = Step(id="e2", thread_id="t1", seq=2, author=HELPER, created_at=AT, client_id="c2", name="Read")
    view = resolve([message(1, "a"), step, retract(3, "e1"), retract(4, "e2"), message(5, "b")])
    assert [e.id for e in view] == ["e5"]


def test_the_view_is_in_seq_order_whatever_the_input_order() -> None:
    view = resolve([message(3, "c"), message(1, "a"), edit(4, "e1", "A"), message(2, "b")])
    assert [e.seq for e in view] == [1, 2, 3]
    assert isinstance(view[0], Message) and view[0].content == [Text(text="A")]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_shapes.py tests/unit/test_resolve.py -q`
Expected: FAIL — `ImportError: cannot import name 'CLIENT_EVENTS'`.

- [ ] **Step 3: Implement**

In `models.py`, add `from datetime import datetime` and `TypeAdapter` to the imports, then append:

```python
SYSTEM = Identity(role="system", id="system", name="system")


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


class _Envelope(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None


class Message(_Envelope):
    kind: Literal["message"] = "message"
    content: list[Part]


class Step(_Envelope):
    kind: Literal["step"] = "step"
    name: str
    input: dict[str, Any] = {}
    output: list[Part] = []
    is_error: bool = False
    duration_ms: int | None = None


class Edit(_Envelope):
    kind: Literal["edit"] = "edit"
    target: str
    content: list[Part]


class Retract(_Envelope):
    kind: Literal["retract"] = "retract"
    target: str


class Read(_Envelope):
    kind: Literal["read"] = "read"
    up_to_seq: int


class MemberAdded(_Envelope):
    kind: Literal["member_added"] = "member_added"
    identity: Identity


class MemberRemoved(_Envelope):
    kind: Literal["member_removed"] = "member_removed"
    identity: Identity


class ThreadUpdated(_Envelope):
    kind: Literal["thread_updated"] = "thread_updated"
    metadata: dict[str, Any]


class Custom(_Envelope):
    kind: Literal["custom"] = "custom"
    name: str
    payload: dict[str, Any]


Event = Annotated[
    Message | Step | Edit | Retract | Read | MemberAdded | MemberRemoved | ThreadUpdated | Custom,
    Field(discriminator="kind"),
]
EVENTS: TypeAdapter[Event] = TypeAdapter(Event)
CLIENT_EVENTS = frozenset({"message", "step", "edit", "retract", "read", "custom"})


class _Signal(BaseModel):
    thread_id: str
    sender: Identity


class Working(_Signal):
    kind: Literal["working"] = "working"
    label: str | None = None


class Idle(_Signal):
    kind: Literal["idle"] = "idle"


class StreamStart(_Signal):
    kind: Literal["stream.start"] = "stream.start"
    stream_id: str
    in_reply_to: str | None = None


class StreamDelta(_Signal):
    kind: Literal["stream.delta"] = "stream.delta"
    stream_id: str
    index: int
    text: str


class StreamEnd(_Signal):
    kind: Literal["stream.end"] = "stream.end"
    stream_id: str


class Cancel(_Signal):
    kind: Literal["cancel"] = "cancel"
    to: Identity


class Present(_Signal):
    kind: Literal["present"] = "present"


class Left(_Signal):
    kind: Literal["left"] = "left"


class Invited(_Signal):
    kind: Literal["invited"] = "invited"
    thread: Thread


class ThreadDeleted(_Signal):
    kind: Literal["thread.deleted"] = "thread.deleted"


Signal = Annotated[
    Working | Idle | StreamStart | StreamDelta | StreamEnd | Cancel | Present | Left | Invited | ThreadDeleted,
    Field(discriminator="kind"),
]
SIGNALS: TypeAdapter[Signal] = TypeAdapter(Signal)
CLIENT_SIGNALS = frozenset({"working", "idle", "stream.start", "stream.delta", "stream.end", "cancel"})


class Joined(BaseModel):
    thread: Thread
    seq: int
    truncated: bool = False


def parts(content: Content) -> list[Part]:
    return [Text(text=content)] if isinstance(content, str) else list(content)
```

`packages/python/src/rfnry_thread/resolve.py`:

```python
from collections.abc import Sequence

from rfnry_thread.models import Edit, Event, Message, Retract


def resolve(events: Sequence[Event]) -> list[Event]:
    ordered = sorted(events, key=lambda event: event.seq)
    edits: dict[str, Edit] = {}
    retracted: set[str] = set()
    for event in ordered:
        if isinstance(event, Edit):
            edits[event.target] = event
        elif isinstance(event, Retract):
            retracted.add(event.target)
    view: list[Event] = []
    for event in ordered:
        if isinstance(event, Edit | Retract) or event.id in retracted:
            continue
        if isinstance(event, Message) and event.id in edits:
            event = event.model_copy(update={"content": edits[event.id].content})
        view.append(event)
    return view
```

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: threads, members, the nine events, the ten signals, resolve

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 4: Protocol frames and golden fixtures

Frames written fresh (six types instead of the old connection.open request); fixture discipline adapted from `../rfnry-thread/scripts/generate_fixtures.py` — fixtures are generated from the Python models, committed, and read by both suites — without its `__meta__` key: the file name says what the frame is.

**Files:**
- Create: `packages/python/src/rfnry_thread/protocol.py`
- Create: `fixtures/protocol/*.json` (written by the test in write mode)
- Test: `packages/python/tests/unit/test_protocol.py`, `packages/python/tests/unit/test_golden.py`

**Interfaces:**
- Consumes: `InputError`, `ThreadException` (Task 1); events, signals, `Joined`, `Thread` (Task 3).
- Produces (in `rfnry_thread.protocol`): `PROTOCOL = 1`; `HELLO = "hello"`; `CLOSE_PROTOCOL = 4000`, `CLOSE_SLOW = 4001`; `METHODS: frozenset[str]`; frames `Hello(protocol)` (the protocol version only — authentication happened at the upgrade), `Request(id, method, payload={})`, `Response(id, payload={})`, `Failure(code, message)`, `ErrorFrame(id, error: Failure)`, `Push(name: Literal["event", "signal"], payload)`, `Notify(method, payload={})`, each with a `type` literal; `Frame`; `encode(frame: Frame) -> str`; `decode(text: str) -> Frame` (raises `InputError("protocol: malformed frame: …")`); `failure(request_id: str, error: ThreadException) -> ErrorFrame`.

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_protocol.py`:

```python
import pytest

from rfnry_thread.errors import ForbiddenError, InputError
from rfnry_thread.protocol import (
    METHODS,
    ErrorFrame,
    Hello,
    Notify,
    Push,
    Request,
    decode,
    encode,
    failure,
)


def test_frames_round_trip_by_type() -> None:
    for frame in [Hello(protocol=1), Request(id="1", method="thread.get", payload={"thread_id": "t"}),
                  Notify(method="signal.send", payload={}), Push(name="event", payload={"kind": "message"})]:
        assert decode(encode(frame)) == frame


@pytest.mark.parametrize("text", ["not json", '{"type": "shout"}', '{"type": "request", "method": "x"}', "[]"])
def test_a_malformed_frame_is_an_input_error(text: str) -> None:
    with pytest.raises(InputError, match="^protocol: malformed frame"):
        decode(text)


def test_an_error_frame_carries_the_code_and_message() -> None:
    frame = failure("7", ForbiddenError("server: 'bruno' is not a member of thread 't1'"))
    assert isinstance(frame, ErrorFrame)
    assert (frame.id, frame.error.code, frame.error.message) == ("7", "forbidden", "server: 'bruno' is not a member of thread 't1'")


def test_the_methods_are_the_designed_fourteen() -> None:
    assert METHODS == {
        "thread.create", "thread.get", "thread.list", "thread.update", "thread.delete", "thread.join",
        "thread.leave", "thread.history", "thread.present", "member.add", "member.remove", "member.list",
        "event.send", "signal.send",
    }
```

`packages/python/tests/unit/test_golden.py`:

```python
import json
import os
from pathlib import Path

import pytest

from rfnry_thread.models import (
    EVENTS,
    SIGNALS,
    Cancel,
    Custom,
    Edit,
    Idle,
    Image,
    Invited,
    Joined,
    Left,
    MemberAdded,
    MemberRemoved,
    Message,
    Present,
    Read,
    Retract,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Thread,
    ThreadDeleted,
    ThreadUpdated,
    Working,
)
from rfnry_thread.protocol import ErrorFrame, Failure, Frame, Hello, Notify, Push, Request, Response, decode, encode

from .factories import ANA, AT, BRUNO, HELPER

FIXTURES = Path(__file__).parents[4] / "fixtures" / "protocol"


def golden() -> dict[str, Frame]:
    thread = Thread(id="t1", scope="acme", metadata={"topic": "billing"}, created_at=AT)
    base = {"thread_id": "t1", "created_at": AT}
    events = [
        Message(id="e1", seq=1, author=ANA, client_id="c1", content=[Text(text="hello"), Image(media_type="image/png", data=b"\x89PNG")], **base),
        Step(id="e2", seq=2, author=HELPER, client_id="c2", name="Read", input={"path": "rules/refunds.md"},
             output=[Text(text="A double charge is refunded in full.")], duration_ms=12, in_reply_to="e1", triggered_by=ANA, **base),
        Edit(id="e3", seq=3, author=ANA, client_id="c3", target="e1", content=[Text(text="hello!")], **base),
        Retract(id="e4", seq=4, author=ANA, client_id="c4", target="e1", **base),
        Read(id="e5", seq=5, author=HELPER, client_id="c5", up_to_seq=4, **base),
        MemberAdded(id="e6", seq=6, author=ANA, client_id="c6", identity=BRUNO, **base),
        MemberRemoved(id="e7", seq=7, author=ANA, client_id="c7", identity=BRUNO, **base),
        ThreadUpdated(id="e8", seq=8, author=ANA, client_id="c8", metadata={"topic": "refunds"}, **base),
        Custom(id="e9", seq=9, author=ANA, client_id="c9", name="rating", payload={"stars": 5}, **base),
    ]
    signals = [
        Working(thread_id="t1", sender=HELPER, label="Reading rules/refunds.md"),
        Idle(thread_id="t1", sender=HELPER),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1", in_reply_to="e1"),
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text="Refunded "),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        Cancel(thread_id="t1", sender=ANA, to=HELPER),
        Present(thread_id="t1", sender=ANA),
        Left(thread_id="t1", sender=ANA),
        Invited(thread_id="t1", sender=ANA, thread=thread),
        ThreadDeleted(thread_id="t1", sender=ANA),
    ]
    frames: dict[str, Frame] = {
        "hello": Hello(protocol=1),
        "hello-response": Response(id="hello", payload={"identity": ANA.model_dump(mode="json"), "scope": "acme"}),
        "hello-wrong-protocol": ErrorFrame(id="hello", error=Failure(code="input", message="server: the first frame must be a hello for protocol 1")),
        "request-thread-join": Request(id="1", method="thread.join", payload={"thread_id": "t1", "since": 0}),
        "response-thread-join": Response(id="1", payload=Joined(thread=thread, seq=12, truncated=True).model_dump(mode="json")),
        "request-event-send": Request(id="2", method="event.send", payload={
            "thread_id": "t1", "kind": "message", "client_id": "c1", "content": [{"type": "text", "text": "hello"}],
            "in_reply_to": None, "triggered_by": HELPER.model_dump(mode="json")}),
        "error-forbidden": ErrorFrame(id="3", error=Failure(code="forbidden", message="server: 'bruno' is not a member of thread 't1'")),
        "notify-stream-delta": Notify(method="signal.send", payload={"thread_id": "t1", "kind": "stream.delta", "stream_id": "s1", "index": 1, "text": "in full."}),
    }
    for event in events:
        frames[f"push-event-{event.kind.replace('_', '-')}"] = Push(name="event", payload=event.model_dump(mode="json"))
    for signal in signals:
        frames[f"push-signal-{signal.kind.replace('.', '-')}"] = Push(name="signal", payload=signal.model_dump(mode="json"))
    return frames


def written(frame: Frame) -> str:
    return json.dumps(json.loads(encode(frame)), indent=2, sort_keys=True) + "\n"


def test_the_fixtures_are_what_the_models_write() -> None:
    frames = golden()
    if os.environ.get("RFNRY_WRITE_FIXTURES"):
        FIXTURES.mkdir(parents=True, exist_ok=True)
        for old in FIXTURES.glob("*.json"):
            old.unlink()
        for name, frame in frames.items():
            (FIXTURES / f"{name}.json").write_text(written(frame), encoding="utf-8")
    on_disk = {path.stem: path.read_text(encoding="utf-8") for path in FIXTURES.glob("*.json")}
    assert on_disk == {name: written(frame) for name, frame in frames.items()}


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*.json")), ids=lambda path: path.stem)
def test_every_fixture_round_trips(path: Path) -> None:
    wire = json.loads(path.read_text(encoding="utf-8"))
    frame = decode(json.dumps(wire))
    assert json.loads(encode(frame)) == wire
    if isinstance(frame, Push):
        adapter = EVENTS if frame.name == "event" else SIGNALS
        assert adapter.validate_python(frame.payload).model_dump(mode="json") == frame.payload
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_protocol.py tests/unit/test_golden.py -q`
Expected: FAIL — `No module named 'rfnry_thread.protocol'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/protocol.py`:

```python
from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from rfnry_thread.errors import InputError, ThreadException

PROTOCOL = 1
HELLO = "hello"
CLOSE_PROTOCOL = 4000
CLOSE_SLOW = 4001
METHODS = frozenset({
    "thread.create", "thread.get", "thread.list", "thread.update", "thread.delete", "thread.join",
    "thread.leave", "thread.history", "thread.present", "member.add", "member.remove", "member.list",
    "event.send", "signal.send",
})


class Hello(BaseModel):
    type: Literal["hello"] = "hello"
    protocol: int


class Request(BaseModel):
    type: Literal["request"] = "request"
    id: str
    method: str
    payload: dict[str, Any] = {}


class Response(BaseModel):
    type: Literal["response"] = "response"
    id: str
    payload: dict[str, Any] = {}


class Failure(BaseModel):
    code: str
    message: str


class ErrorFrame(BaseModel):
    type: Literal["error"] = "error"
    id: str
    error: Failure


class Push(BaseModel):
    type: Literal["push"] = "push"
    name: Literal["event", "signal"]
    payload: dict[str, Any]


class Notify(BaseModel):
    type: Literal["notify"] = "notify"
    method: str
    payload: dict[str, Any] = {}


Frame = Annotated[Hello | Request | Response | ErrorFrame | Push | Notify, Field(discriminator="type")]
FRAMES: TypeAdapter[Frame] = TypeAdapter(Frame)


def encode(frame: Frame) -> str:
    return frame.model_dump_json()


def decode(text: str) -> Frame:
    try:
        return FRAMES.validate_json(text)
    except ValidationError as exc:
        raise InputError(f"protocol: malformed frame: {exc.errors(include_url=False)[0]['msg'].lower()}") from exc


def failure(request_id: str, error: ThreadException) -> ErrorFrame:
    return ErrorFrame(id=request_id, error=Failure(code=error.code, message=str(error)))
```

- [ ] **Step 4: Write the fixtures and run the gate**

Run: `cd packages/python && uv run poe fixtures && git status --short ../../fixtures`
Expected: 27 new files under `fixtures/protocol/` (8 named frames, 9 event pushes, 10 signal pushes). Open `push-event-message.json` and check `"data": "iVBORw=="` — standard base64.

Run: `uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS; `test_every_fixture_round_trips` runs 27 cases.

- [ ] **Step 5: Commit**

```bash
git add fixtures packages/python
git commit -m "feat: protocol version 1 — six frame types, golden fixtures shared by both clients

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 5: The Storage protocol and InMemoryStorage

Written fresh, scope-first (the old store had no scope column and a global `seq`). Dedup semantics adapted from `../rfnry-thread/packages/server-python/src/rfnry_thread_server/store/memory/store.py` (`append_event`: the `(thread, author, client_id)` key answers with the stored event).

**Files:**
- Create: `packages/python/src/rfnry_thread/storage.py`, `packages/python/src/rfnry_thread/inmemory.py`
- Create: `packages/python/tests/integration/conftest.py`, `packages/python/tests/integration/test_storage.py`, `packages/python/tests/until.py`

**Interfaces:**
- Consumes: `Thread`, `Member`, `Event`, events (Task 3); `NotFoundError`, `StorageError` (Task 1).
- Produces (in `rfnry_thread.storage`): `OnEvent = Callable[[str, str, int], Awaitable[None]]` (scope, thread_id, seq); `OnSignal = Callable[[str, str, dict[str, Any]], Awaitable[None]]` (scope, thread_id, payload); `OnResume = Callable[[], Awaitable[None]]`; `Stop = Callable[[], Awaitable[None]]`; the `Storage` protocol:
  - `initialize() -> None`
  - `create_thread(scope, thread: Thread) -> tuple[Thread, bool]` (stored, created)
  - `get_thread(scope, thread_id) -> Thread | None`
  - `list_threads(scope, member_id, before: str | None = None, limit: int = 50) -> list[Thread]` (newest first by `(created_at, id)`; `before` is a thread id)
  - `update_thread(scope, thread_id, metadata) -> Thread | None`
  - `delete_thread(scope, thread_id) -> bool` (members and events go with it)
  - `add_member(scope, member: Member) -> tuple[Member, bool]`; `remove_member(scope, thread_id, identity_id) -> bool`; `list_members(scope, thread_id) -> list[Member]` (by `added_at`); `is_member(scope, thread_id, identity_id) -> bool`
  - `append_event(scope, event: Event) -> tuple[Event, bool]` — ignores `event.seq`, assigns `last + 1`; a repeat of `(thread_id, author.id, client_id)` returns the stored event and `False` and announces nothing; a new event is announced to every listener's `on_event`; a missing thread is `NotFoundError`
  - `list_events(scope, thread_id, since: int | None = None, before: int | None = None, limit: int = 50, kinds: Sequence[str] | None = None) -> list[Event]` — always ascending; with `since`, the first `limit` after it; without, the last `limit` below `before` (or overall)
  - `get_event(scope, thread_id, event_id) -> Event | None`
  - `delete_scope(scope) -> int` (rows removed); `verify_absent(scope) -> bool`
  - `listen(on_event, on_signal, on_resume: OnResume | None = None) -> Stop` (`on_resume` is called each time the listener is subscribed again after its connection dropped; `InMemoryStorage` never drops, so never calls it); `publish(scope, thread_id, payload: dict[str, Any]) -> None` (every listener, the publisher's own included, receives a copy)
- Produces: `InMemoryStorage()` in `rfnry_thread.inmemory`; `tests/until.py` with `async def until(check: Callable[[], bool], timeout: float = 2.0) -> None`.

- [ ] **Step 1: Write the helper, the fixture and the failing suite**

`packages/python/tests/until.py`:

```python
import asyncio
from collections.abc import Callable


async def until(check: Callable[[], bool], timeout: float = 2.0) -> None:
    async def wait() -> None:
        while not check():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), timeout)
```

`packages/python/tests/integration/conftest.py` (Task 15 adds the Postgres parameter):

```python
from collections.abc import AsyncIterator
from typing import Any

import pytest

from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.storage import Storage


@pytest.fixture(params=["inmemory"])
async def storage(request: Any) -> AsyncIterator[Storage]:
    yield InMemoryStorage()
```

`packages/python/tests/integration/test_storage.py`:

```python
import asyncio
from datetime import timedelta
from typing import Any

import pytest

from rfnry_thread.errors import NotFoundError
from rfnry_thread.models import SYSTEM, Event, Member, Message, Read, Text, Thread
from rfnry_thread.storage import Storage
from unit.factories import ANA, AT, BRUNO, HELPER
from until import until


def thread(thread_id: str, scope: str = "acme", minutes: int = 0) -> Thread:
    return Thread(id=thread_id, scope=scope, metadata={"n": minutes}, created_at=AT + timedelta(minutes=minutes))


def seqs(events: list[Event]) -> list[int]:
    return [event.seq for event in events]


def say(thread_id: str, client_id: str, text: str = "hi", author: Any = ANA) -> Message:
    return Message(id=f"id-{client_id}-{text}", thread_id=thread_id, author=author, created_at=AT,
                   client_id=client_id, content=[Text(text=text)])


async def room(storage: Storage, thread_id: str = "t1", scope: str = "acme", *members: Any) -> Thread:
    stored, _ = await storage.create_thread(scope, thread(thread_id, scope))
    for identity in members or (ANA,):
        await storage.add_member(scope, Member(thread_id=thread_id, identity=identity, added_at=AT, added_by=SYSTEM))
    return stored


async def test_create_is_get_or_create(storage: Storage) -> None:
    first, created = await storage.create_thread("acme", thread("t1"))
    again, created_again = await storage.create_thread("acme", thread("t1", minutes=5))
    assert (created, created_again) == (True, False)
    assert again == first
    assert await storage.get_thread("acme", "t1") == first
    assert await storage.get_thread("acme", "ghost") is None


async def test_threads_are_listed_for_a_member_newest_first_in_pages(storage: Storage) -> None:
    for minutes, thread_id in enumerate(["a", "b", "c", "d"]):
        await storage.create_thread("acme", thread(thread_id, minutes=minutes))
        if thread_id != "c":
            await storage.add_member("acme", Member(thread_id=thread_id, identity=ANA, added_at=AT, added_by=SYSTEM))
    assert [t.id for t in await storage.list_threads("acme", "ana")] == ["d", "b", "a"]
    assert [t.id for t in await storage.list_threads("acme", "ana", limit=2)] == ["d", "b"]
    assert [t.id for t in await storage.list_threads("acme", "ana", before="b")] == ["a"]
    assert await storage.list_threads("acme", "bruno") == []


async def test_update_and_delete_take_members_and_events_along(storage: Storage) -> None:
    await room(storage)
    await storage.append_event("acme", say("t1", "c1"))
    updated = await storage.update_thread("acme", "t1", {"topic": "refunds"})
    assert updated is not None and updated.metadata == {"topic": "refunds"}
    assert await storage.update_thread("acme", "ghost", {}) is None
    assert await storage.delete_thread("acme", "t1") is True
    assert await storage.delete_thread("acme", "t1") is False
    assert await storage.list_members("acme", "t1") == []
    assert await storage.list_events("acme", "t1") == []
    assert await storage.verify_absent("acme")


async def test_members_are_added_once_and_removed(storage: Storage) -> None:
    await room(storage)
    member, created = await storage.add_member("acme", Member(thread_id="t1", identity=BRUNO, added_at=AT, added_by=ANA))
    again, created_again = await storage.add_member("acme", Member(thread_id="t1", identity=BRUNO, added_at=AT + timedelta(1), added_by=HELPER))
    assert (created, created_again) == (True, False) and again == member
    assert [m.identity.id for m in await storage.list_members("acme", "t1")] == ["ana", "bruno"]
    assert await storage.is_member("acme", "t1", "bruno")
    assert await storage.remove_member("acme", "t1", "bruno") is True
    assert await storage.remove_member("acme", "t1", "bruno") is False
    assert not await storage.is_member("acme", "t1", "bruno")
    with pytest.raises(NotFoundError, match=r"^\w+: thread 'ghost' was not found$"):
        await storage.add_member("acme", Member(thread_id="ghost", identity=BRUNO, added_at=AT, added_by=ANA))


async def test_seq_is_thread_local_gapless_and_dedup_answers_with_the_stored_event(storage: Storage) -> None:
    await room(storage, "t1")
    await room(storage, "t2")
    first, created = await storage.append_event("acme", say("t1", "c1", "hello"))
    repeat, created_again = await storage.append_event("acme", say("t1", "c1", "hello, again"))
    other, _ = await storage.append_event("acme", say("t2", "c1"))
    second, _ = await storage.append_event("acme", say("t1", "c2"))
    assert (created, created_again) == (True, False)
    assert repeat == first and isinstance(repeat, Message) and repeat.content == [Text(text="hello")]
    assert (first.seq, second.seq, other.seq) == (1, 2, 1)
    bruno, _ = await storage.append_event("acme", say("t1", "c1", author=BRUNO))
    assert bruno.seq == 3
    with pytest.raises(NotFoundError):
        await storage.append_event("acme", say("ghost", "c1"))


async def test_concurrent_duplicates_store_one_event_and_seq_stays_gapless(storage: Storage) -> None:
    await room(storage)
    same = [storage.append_event("acme", say("t1", "dup")) for _ in range(20)]
    distinct = [storage.append_event("acme", say("t1", f"c{n}")) for n in range(20)]
    results = await asyncio.gather(*same, *distinct)
    assert sum(created for _, created in results[:20]) == 1
    assert len({event.id for event, _ in results[:20]}) == 1
    assert [e.seq for e in await storage.list_events("acme", "t1", limit=100)] == list(range(1, 22))


async def test_list_events_pages_both_ways_and_filters_kinds(storage: Storage) -> None:
    await room(storage)
    for n in range(1, 11):
        await storage.append_event("acme", say("t1", f"c{n}"))
    await storage.append_event("acme", Read(id="r", thread_id="t1", author=ANA, created_at=AT, client_id="r", up_to_seq=10))
    assert seqs(await storage.list_events("acme", "t1", limit=3)) == [9, 10, 11]
    assert seqs(await storage.list_events("acme", "t1", since=2, limit=3)) == [3, 4, 5]
    assert seqs(await storage.list_events("acme", "t1", before=5, limit=3)) == [2, 3, 4]
    assert seqs(await storage.list_events("acme", "t1", since=2, before=5)) == [3, 4]
    assert seqs(await storage.list_events("acme", "t1", kinds=["read"])) == [11]
    assert (await storage.get_event("acme", "t1", "r")) is not None
    assert await storage.get_event("acme", "t1", "ghost") is None


async def test_the_same_thread_id_in_two_scopes_is_two_threads(storage: Storage) -> None:
    await room(storage, "t1", "tenant-a")
    await room(storage, "t1", "tenant-b")
    await storage.append_event("tenant-a", say("t1", "c1", "MARKER-A"))
    assert await storage.list_events("tenant-b", "t1") == []
    assert await storage.get_event("tenant-b", "t1", "id-c1-MARKER-A") is None
    assert await storage.list_threads("tenant-b", "ana") == [await storage.get_thread("tenant-b", "t1")]
    assert await storage.delete_scope("tenant-a") == 3
    assert await storage.verify_absent("tenant-a")
    assert not await storage.verify_absent("tenant-b")


async def test_listeners_hear_new_events_and_published_signals(storage: Storage) -> None:
    await room(storage)
    events: list[tuple[str, str, int]] = []
    signals: list[tuple[str, str, dict[str, Any]]] = []

    async def on_event(scope: str, thread_id: str, seq: int) -> None:
        events.append((scope, thread_id, seq))

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        signals.append((scope, thread_id, payload))

    stop = await storage.listen(on_event, on_signal)
    await storage.append_event("acme", say("t1", "c1"))
    await storage.append_event("acme", say("t1", "c1"))
    await storage.publish("acme", "t1", {"o": "r1", "kind": "idle"})
    await until(lambda: len(events) == 1 and len(signals) == 1)
    assert events == [("acme", "t1", 1)]
    assert signals == [("acme", "t1", {"o": "r1", "kind": "idle"})]
    await stop()
    await storage.append_event("acme", say("t1", "c2"))
    await asyncio.sleep(0.1)
    assert len(events) == 1


def test_it_is_a_storage(storage: Storage) -> None:
    assert isinstance(storage, Storage)

```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd packages/python && uv run pytest tests/integration/test_storage.py -q`
Expected: FAIL — `No module named 'rfnry_thread.inmemory'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/storage.py`:

```python
from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol, runtime_checkable

from rfnry_thread.models import Event, Member, Thread

OnEvent = Callable[[str, str, int], Awaitable[None]]
OnSignal = Callable[[str, str, dict[str, Any]], Awaitable[None]]
OnResume = Callable[[], Awaitable[None]]
Stop = Callable[[], Awaitable[None]]


@runtime_checkable
class Storage(Protocol):
    async def initialize(self) -> None: ...

    async def create_thread(self, scope: str, thread: Thread) -> tuple[Thread, bool]: ...

    async def get_thread(self, scope: str, thread_id: str) -> Thread | None: ...

    async def list_threads(self, scope: str, member_id: str, before: str | None = None, limit: int = 50) -> list[Thread]: ...

    async def update_thread(self, scope: str, thread_id: str, metadata: dict[str, Any]) -> Thread | None: ...

    async def delete_thread(self, scope: str, thread_id: str) -> bool: ...

    async def add_member(self, scope: str, member: Member) -> tuple[Member, bool]: ...

    async def remove_member(self, scope: str, thread_id: str, identity_id: str) -> bool: ...

    async def list_members(self, scope: str, thread_id: str) -> list[Member]: ...

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool: ...

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

    async def delete_scope(self, scope: str) -> int: ...

    async def verify_absent(self, scope: str) -> bool: ...

    async def listen(self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None) -> Stop: ...

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None: ...
```

`packages/python/src/rfnry_thread/inmemory.py`:

```python
from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from rfnry_thread.errors import NotFoundError, StorageError
from rfnry_thread.models import Event, Member, Thread
from rfnry_thread.storage import OnEvent, OnResume, OnSignal, Stop

Key = tuple[str, str]


class InMemoryStorage:
    def __init__(self) -> None:
        self._threads: dict[Key, Thread] = {}
        self._members: dict[Key, dict[str, Member]] = {}
        self._events: dict[Key, list[Event]] = {}
        self._keys: dict[tuple[str, str, str, str], Event] = {}
        self._listeners: list[tuple[OnEvent, OnSignal]] = []

    async def initialize(self) -> None:
        return None

    async def create_thread(self, scope: str, thread: Thread) -> tuple[Thread, bool]:
        if thread.scope != scope:
            raise StorageError("inmemory: row scope does not match the call scope")
        key = (scope, thread.id)
        if key in self._threads:
            return self._threads[key], False
        self._threads[key] = thread
        self._members[key] = {}
        self._events[key] = []
        return thread, True

    async def get_thread(self, scope: str, thread_id: str) -> Thread | None:
        return self._threads.get((scope, thread_id))

    async def list_threads(self, scope: str, member_id: str, before: str | None = None, limit: int = 50) -> list[Thread]:
        threads = [
            thread for (owner, thread_id), thread in self._threads.items()
            if owner == scope and member_id in self._members[(owner, thread_id)]
        ]
        threads.sort(key=lambda t: (t.created_at, t.id), reverse=True)
        if before is not None:
            anchor = self._threads.get((scope, before))
            if anchor is None:
                return []
            threads = [t for t in threads if (t.created_at, t.id) < (anchor.created_at, anchor.id)]
        return threads[: max(limit, 0)]

    async def update_thread(self, scope: str, thread_id: str, metadata: dict[str, Any]) -> Thread | None:
        thread = self._threads.get((scope, thread_id))
        if thread is None:
            return None
        updated = thread.model_copy(update={"metadata": dict(metadata)})
        self._threads[(scope, thread_id)] = updated
        return updated

    async def delete_thread(self, scope: str, thread_id: str) -> bool:
        key = (scope, thread_id)
        if self._threads.pop(key, None) is None:
            return False
        self._members.pop(key, None)
        self._events.pop(key, None)
        self._keys = {k: v for k, v in self._keys.items() if k[:2] != key}
        return True

    async def add_member(self, scope: str, member: Member) -> tuple[Member, bool]:
        members = self._members.get((scope, member.thread_id))
        if members is None:
            raise NotFoundError(f"inmemory: thread {member.thread_id!r} was not found")
        existing = members.get(member.identity.id)
        if existing is not None:
            return existing, False
        members[member.identity.id] = member
        return member, True

    async def remove_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        return self._members.get((scope, thread_id), {}).pop(identity_id, None) is not None

    async def list_members(self, scope: str, thread_id: str) -> list[Member]:
        return sorted(self._members.get((scope, thread_id), {}).values(), key=lambda m: (m.added_at, m.identity.id))

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        return identity_id in self._members.get((scope, thread_id), {})

    async def append_event(self, scope: str, event: Event) -> tuple[Event, bool]:
        log = self._events.get((scope, event.thread_id))
        if log is None:
            raise NotFoundError(f"inmemory: thread {event.thread_id!r} was not found")
        dedup = (scope, event.thread_id, event.author.id, event.client_id)
        existing = self._keys.get(dedup)
        if existing is not None:
            return existing, False
        stored = event.model_copy(update={"seq": len(log) + 1})
        log.append(stored)
        self._keys[dedup] = stored
        for on_event, _ in list(self._listeners):
            await on_event(scope, stored.thread_id, stored.seq)
        return stored, True

    async def list_events(
        self,
        scope: str,
        thread_id: str,
        since: int | None = None,
        before: int | None = None,
        limit: int = 50,
        kinds: Sequence[str] | None = None,
    ) -> list[Event]:
        if limit < 1:
            return []
        chosen = [
            event for event in self._events.get((scope, thread_id), [])
            if (since is None or event.seq > since)
            and (before is None or event.seq < before)
            and (kinds is None or event.kind in kinds)
        ]
        return chosen[:limit] if since is not None else chosen[-limit:]

    async def get_event(self, scope: str, thread_id: str, event_id: str) -> Event | None:
        return next((e for e in self._events.get((scope, thread_id), []) if e.id == event_id), None)

    async def delete_scope(self, scope: str) -> int:
        keys = [key for key in self._threads if key[0] == scope]
        removed = 0
        for key in keys:
            removed += 1 + len(self._members.get(key, {})) + len(self._events.get(key, []))
            await self.delete_thread(*key)
        return removed

    async def verify_absent(self, scope: str) -> bool:
        return not any(key[0] == scope for key in [*self._threads, *self._members, *self._events])

    async def listen(self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None) -> Stop:
        entry = (on_event, on_signal)
        self._listeners.append(entry)

        async def stop() -> None:
            if entry in self._listeners:
                self._listeners.remove(entry)

        return stop

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        text = json.dumps(payload)
        for _, on_signal in list(self._listeners):
            await on_signal(scope, thread_id, json.loads(text))
```

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS (the integration suite's in-memory parameter runs in the default tier).

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: the Storage protocol and InMemoryStorage — scope first, thread-local seq, dedup, in-process bus

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 6: The outbound queue — backpressure and delta merging

Adapted from `../rfnry-thread/packages/server-python/src/rfnry_thread_server/transport/outbound.py` (bound + stall window, progress reset when the queue was empty, overflow closes). New: stream deltas merge per stream instead of queueing, into a private copy.

**Files:**
- Create: `packages/python/src/rfnry_thread/outbound.py`
- Test: `packages/python/tests/unit/test_outbound.py`

**Interfaces:**
- Consumes: `Push`, `Response`, `ErrorFrame`, `encode` (Task 4).
- Produces (in `rfnry_thread.outbound`): `BOUND = 1000`, `STALL = 10.0`; `Outgoing = Push | Response | ErrorFrame`; `Sink` protocol (`async send_text(data: str) -> None`); `Outbound(sink: Sink, on_stall: Callable[[], Awaitable[None]], *, bound: int = BOUND, stall: float = STALL, clock: Callable[[], float] = time.monotonic)` with `put(frame: Outgoing) -> None` (never blocks), `closed: bool`, `async drain(timeout: float = 2.0) -> None` (waits until sent), `async close() -> None`. Must be constructed inside a running loop (it starts its writer task).

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_outbound.py`:

```python
import asyncio
import json
from typing import Any

from rfnry_thread.outbound import Outbound
from rfnry_thread.protocol import Push, Response
from until import until

from .factories import HELPER


class Sink:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.gate = asyncio.Event()
        self.gate.set()

    async def send_text(self, data: str) -> None:
        await self.gate.wait()
        self.sent.append(json.loads(data))


def delta(text: str, index: int, stream: str = "s1") -> Push:
    return Push(name="signal", payload={"kind": "stream.delta", "thread_id": "t1", "sender": HELPER.model_dump(mode="json"),
                                        "stream_id": stream, "index": index, "text": text})


def event(seq: int) -> Push:
    return Push(name="event", payload={"kind": "message", "seq": seq})


async def nothing() -> None:
    return None


async def test_frames_leave_in_the_order_they_were_put() -> None:
    sink = Sink()
    queue = Outbound(sink, nothing)
    for seq in range(1, 6):
        queue.put(event(seq))
    queue.put(Response(id="1", payload={}))
    await queue.drain()
    assert [f.get("payload", {}).get("seq") for f in sink.sent] == [1, 2, 3, 4, 5, None]
    await queue.close()


async def test_deltas_waiting_for_a_slow_connection_are_merged_never_dropped() -> None:
    sink = Sink()
    sink.gate.clear()
    queue = Outbound(sink, nothing)
    queue.put(event(1))
    for index, text in enumerate(["Re", "fun", "ded"]):
        queue.put(delta(text, index))
    queue.put(delta("Other", 0, stream="s2"))
    sink.gate.set()
    await queue.drain()
    payloads = [f["payload"] for f in sink.sent]
    assert payloads[0]["seq"] == 1
    assert [(p["stream_id"], p["text"], p["index"]) for p in payloads[1:]] == [("s1", "Refunded", 2), ("s2", "Other", 0)]
    await queue.close()


async def test_merging_never_touches_a_frame_other_queues_hold() -> None:
    slow, fast = Sink(), Sink()
    slow.gate.clear()
    first, second = Outbound(slow, nothing), Outbound(fast, nothing)
    shared = delta("Re", 0)
    first.put(event(1))
    first.put(shared)
    second.put(shared)
    first.put(delta("funded", 1))
    await second.drain()
    assert shared.payload["text"] == "Re"
    assert fast.sent[0]["payload"]["text"] == "Re"
    slow.gate.set()
    await first.drain()
    assert slow.sent[1]["payload"]["text"] == "Refunded"
    await first.close()
    await second.close()


async def test_a_connection_over_its_bound_with_no_write_for_the_stall_window_is_closed() -> None:
    now = [0.0]
    stalled: list[bool] = []

    async def on_stall() -> None:
        stalled.append(True)

    sink = Sink()
    sink.gate.clear()
    queue = Outbound(sink, on_stall, bound=2, stall=10.0, clock=lambda: now[0])
    for seq in range(1, 5):
        queue.put(event(seq))
    await asyncio.sleep(0)
    assert not queue.closed and stalled == []
    now[0] = 11.0
    queue.put(event(5))
    await until(lambda: stalled == [True])
    assert queue.closed
    queue.put(event(6))
    await queue.close()


async def test_a_connection_that_keeps_writing_is_never_closed() -> None:
    now = [0.0]
    sink = Sink()
    queue = Outbound(sink, nothing, bound=2, stall=10.0, clock=lambda: now[0])
    for seq in range(1, 50):
        now[0] += 1.0
        queue.put(event(seq))
        await asyncio.sleep(0)
    await queue.drain()
    assert not queue.closed and len(sink.sent) == 49
    await queue.close()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_outbound.py -q`
Expected: FAIL — `No module named 'rfnry_thread.outbound'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/outbound.py`:

```python
from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Protocol

from rfnry_thread.protocol import ErrorFrame, Push, Response, encode

BOUND = 1000
STALL = 10.0

Outgoing = Push | Response | ErrorFrame


class Sink(Protocol):
    async def send_text(self, data: str) -> None: ...


def _delta_key(frame: Outgoing) -> tuple[str, str] | None:
    if isinstance(frame, Push) and frame.name == "signal" and frame.payload.get("kind") == "stream.delta":
        return (str(frame.payload["thread_id"]), str(frame.payload["stream_id"]))
    return None


class Outbound:
    def __init__(
        self,
        sink: Sink,
        on_stall: Callable[[], Awaitable[None]],
        *,
        bound: int = BOUND,
        stall: float = STALL,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._sink = sink
        self._on_stall = on_stall
        self._bound = bound
        self._stall = stall
        self._clock = clock
        self._frames: deque[Outgoing] = deque()
        self._deltas: dict[tuple[str, str], Push] = {}
        self._ready = asyncio.Event()
        self._progress = clock()
        self._stalled: asyncio.Task[None] | None = None
        self.closed = False
        self._writer = asyncio.create_task(self._run())

    def put(self, frame: Outgoing) -> None:
        if self.closed:
            return
        if not self._frames:
            self._progress = self._clock()
        key = _delta_key(frame)
        if key is not None and isinstance(frame, Push):
            pending = self._deltas.get(key)
            if pending is not None:
                pending.payload["text"] = str(pending.payload["text"]) + str(frame.payload["text"])
                pending.payload["index"] = frame.payload["index"]
                return
            frame = Push(name="signal", payload=dict(frame.payload))
            self._deltas[key] = frame
        self._frames.append(frame)
        self._ready.set()
        if len(self._frames) > self._bound and self._clock() - self._progress > self._stall:
            self.closed = True
            self._stalled = asyncio.create_task(self._on_stall())

    async def _run(self) -> None:
        try:
            while True:
                await self._ready.wait()
                while self._frames:
                    frame = self._frames.popleft()
                    key = _delta_key(frame)
                    if key is not None and self._deltas.get(key) is frame:
                        del self._deltas[key]
                    await self._sink.send_text(encode(frame))
                    self._progress = self._clock()
                self._ready.clear()
        except Exception:
            self.closed = True

    async def drain(self, timeout: float = 2.0) -> None:
        async def empty() -> None:
            while self._frames and not self._writer.done():
                await asyncio.sleep(0.005)
            await asyncio.sleep(0)

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(empty(), timeout)

    async def close(self) -> None:
        self.closed = True
        self._writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._writer
```

`drain` returns when the deque is empty; the last frame may still be inside `send_text`. The tests call `drain` after the gate is open, so the in-flight write completes within the extra `sleep(0)`; if a test flakes here, make `_run` track an `_inflight: bool` and have `empty()` also wait for it to be `False` — do that rather than adding sleeps.

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: the outbound queue — a stalled connection is closed, stream deltas merge into a private copy

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 7: The latest-state cache

Written fresh; replaces the old presence registry, presence store and stream frames (`../rfnry-thread/packages/server-python/src/rfnry_thread_server/presence/*`, stripped).

**Files:**
- Create: `packages/python/src/rfnry_thread/state.py`
- Test: `packages/python/tests/unit/test_state.py`

**Interfaces:**
- Consumes: signals, `Identity` (Task 3).
- Produces (in `rfnry_thread.state`): `REFRESH = 10.0`, `EXPIRY = 30.0`; `LatestState(origin: str, clock: Callable[[], float] = time.monotonic)` with:
  - `apply(scope: str, signal: Signal, origin: str) -> None` — `Present`/`Left` set presence, `Working`/`Idle` the working label, `StreamStart`/`StreamDelta`/`StreamEnd` the open streams and their text, `ThreadDeleted` forgets the thread; `Cancel` and `Invited` change nothing
  - `snapshot(scope, thread_id) -> list[Signal]` — one `Present` per present identity, then each `Working`, then per open stream its `StreamStart` and, when it has text, one `StreamDelta` carrying all of it
  - `present(scope, thread_id) -> list[Identity]` (one per identity id); `is_present(scope, thread_id, identity_id) -> bool`
  - `own() -> dict[tuple[str, str], list[dict[str, Any]]]` — this origin's entries per `(scope, thread_id)` as re-announcement items `{"sender", "present", "working": {"label"} | None, "streams": [{"stream_id", "in_reply_to"}]}`
  - `refresh(scope, thread_id, origin, items: list[dict[str, Any]]) -> None` — sets each named sender's entry for that origin exactly as the item says
  - mirrored entries (origin ≠ own) not seen for `EXPIRY` seconds are dropped on read; own entries never expire.

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_state.py`:

```python
from rfnry_thread.models import (
    Idle,
    Left,
    Present,
    StreamDelta,
    StreamEnd,
    StreamStart,
    ThreadDeleted,
    Working,
)
from rfnry_thread.state import EXPIRY, LatestState

from .factories import ANA, HELPER


def state(now: list[float]) -> LatestState:
    return LatestState("me", clock=lambda: now[0])


def test_a_snapshot_is_presence_then_work_then_streams_with_their_text() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply("acme", Working(thread_id="t1", sender=HELPER, label="Reading"), "me")
    cache.apply("acme", StreamStart(thread_id="t1", sender=HELPER, stream_id="s1", in_reply_to="e1"), "me")
    cache.apply("acme", StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text="Re"), "me")
    cache.apply("acme", StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=1, text="funded"), "me")
    kinds = [(s.kind, getattr(s, "text", None)) for s in cache.snapshot("acme", "t1")]
    assert kinds == [("present", None), ("working", None), ("stream.start", None), ("stream.delta", "Refunded")]
    assert cache.snapshot("other", "t1") == []


def test_idle_left_and_stream_end_clear_what_they_end() -> None:
    cache = state([0.0])
    for signal in [Present(thread_id="t1", sender=ANA), Working(thread_id="t1", sender=HELPER),
                   StreamStart(thread_id="t1", sender=HELPER, stream_id="s1")]:
        cache.apply("acme", signal, "me")
    for signal in [Left(thread_id="t1", sender=ANA), Idle(thread_id="t1", sender=HELPER),
                   StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1")]:
        cache.apply("acme", signal, "me")
    assert cache.snapshot("acme", "t1") == []
    assert cache.own() == {}


def test_presence_is_one_per_identity_across_replicas() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "other")
    assert cache.present("acme", "t1") == [ANA]
    cache.apply("acme", Left(thread_id="t1", sender=ANA), "me")
    assert cache.is_present("acme", "t1", "ana")


def test_a_silent_replica_is_forgotten_and_our_own_entries_are_not() -> None:
    now = [0.0]
    cache = state(now)
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "other")
    cache.apply("acme", Present(thread_id="t1", sender=HELPER), "me")
    now[0] = EXPIRY + 1
    assert cache.present("acme", "t1") == [HELPER]


def test_a_refresh_keeps_a_mirrored_entry_alive_and_heals_a_missed_end() -> None:
    now = [0.0]
    cache = state(now)
    cache.apply("acme", Working(thread_id="t1", sender=HELPER, label="Reading"), "other")
    cache.apply("acme", StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"), "other")
    now[0] = EXPIRY - 1
    cache.refresh("acme", "t1", "other", [{"sender": HELPER.model_dump(mode="json"), "present": True,
                                           "working": None, "streams": []}])
    now[0] = EXPIRY + 5
    assert [s.kind for s in cache.snapshot("acme", "t1")] == ["present"]


def test_a_refresh_teaches_a_late_replica_about_streams_it_never_saw_start() -> None:
    cache = state([0.0])
    cache.refresh("acme", "t1", "other", [{"sender": HELPER.model_dump(mode="json"), "present": False,
                                           "working": {"label": "Writing"},
                                           "streams": [{"stream_id": "s1", "in_reply_to": "e1"}]}])
    snapshot = cache.snapshot("acme", "t1")
    assert [s.kind for s in snapshot] == ["working", "stream.start"]
    assert isinstance(snapshot[1], StreamStart) and snapshot[1].in_reply_to == "e1"


def test_own_lists_this_replica_entries_for_re_announcement() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply("acme", StreamStart(thread_id="t1", sender=HELPER, stream_id="s1", in_reply_to="e1"), "me")
    cache.apply("acme", Present(thread_id="t1", sender=HELPER), "other")
    own = cache.own()
    assert list(own) == [("acme", "t1")]
    assert sorted(item["sender"]["id"] for item in own[("acme", "t1")]) == ["ana", "helper"]
    helper = next(item for item in own[("acme", "t1")] if item["sender"]["id"] == "helper")
    assert helper == {"sender": HELPER.model_dump(mode="json"), "present": False, "working": None,
                      "streams": [{"stream_id": "s1", "in_reply_to": "e1"}]}


def test_a_deleted_thread_is_forgotten() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply("acme", ThreadDeleted(thread_id="t1", sender=ANA), "me")
    assert cache.snapshot("acme", "t1") == [] and cache.own() == {}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_state.py -q`
Expected: FAIL — `No module named 'rfnry_thread.state'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/state.py`:

```python
from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from rfnry_thread.models import (
    Identity,
    Idle,
    Left,
    Present,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
    ThreadDeleted,
    Working,
)

REFRESH = 10.0
EXPIRY = 30.0


@dataclass
class _Stream:
    start: StreamStart
    index: int = 0
    text: str = ""


@dataclass
class _Entry:
    sender: Identity
    seen: float
    present: bool = False
    working: Working | None = None
    streams: dict[str, _Stream] = field(default_factory=dict)

    def empty(self) -> bool:
        return not self.present and self.working is None and not self.streams


class LatestState:
    def __init__(self, origin: str, clock: Callable[[], float] = time.monotonic) -> None:
        self.origin = origin
        self._clock = clock
        self._threads: dict[tuple[str, str], dict[tuple[str, str], _Entry]] = {}

    def apply(self, scope: str, signal: Signal, origin: str) -> None:
        key = (scope, signal.thread_id)
        if isinstance(signal, ThreadDeleted):
            self._threads.pop(key, None)
            return
        if not isinstance(signal, Present | Left | Working | Idle | StreamStart | StreamDelta | StreamEnd):
            return
        entries = self._threads.setdefault(key, {})
        slot = (origin, signal.sender.id)
        entry = entries.get(slot) or _Entry(sender=signal.sender, seen=self._clock())
        entry.seen = self._clock()
        if isinstance(signal, Present):
            entry.present = True
        elif isinstance(signal, Left):
            entry.present = False
        elif isinstance(signal, Working):
            entry.working = signal
        elif isinstance(signal, Idle):
            entry.working = None
        elif isinstance(signal, StreamStart):
            entry.streams[signal.stream_id] = _Stream(signal)
        elif isinstance(signal, StreamDelta):
            stream = entry.streams.setdefault(
                signal.stream_id,
                _Stream(StreamStart(thread_id=signal.thread_id, sender=signal.sender, stream_id=signal.stream_id)),
            )
            stream.index = signal.index
            stream.text += signal.text
        else:
            entry.streams.pop(signal.stream_id, None)
        self._store(key, slot, entry)

    def _store(self, key: tuple[str, str], slot: tuple[str, str], entry: _Entry) -> None:
        entries = self._threads.setdefault(key, {})
        if entry.empty():
            entries.pop(slot, None)
        else:
            entries[slot] = entry
        if not entries:
            del self._threads[key]

    def _live(self, scope: str, thread_id: str) -> list[_Entry]:
        entries = self._threads.get((scope, thread_id))
        if entries is None:
            return []
        now = self._clock()
        for slot in [s for s, e in entries.items() if s[0] != self.origin and now - e.seen > EXPIRY]:
            del entries[slot]
        if not entries:
            del self._threads[(scope, thread_id)]
        return list(entries.values())

    def snapshot(self, scope: str, thread_id: str) -> list[Signal]:
        present: dict[str, Signal] = {}
        rest: list[Signal] = []
        for entry in self._live(scope, thread_id):
            if entry.present:
                present.setdefault(entry.sender.id, Present(thread_id=thread_id, sender=entry.sender))
            if entry.working is not None:
                rest.append(entry.working)
            for stream in entry.streams.values():
                rest.append(stream.start)
                if stream.text:
                    rest.append(StreamDelta(thread_id=thread_id, sender=entry.sender, stream_id=stream.start.stream_id,
                                            index=stream.index, text=stream.text))
        return [*present.values(), *rest]

    def present(self, scope: str, thread_id: str) -> list[Identity]:
        found: dict[str, Identity] = {}
        for entry in self._live(scope, thread_id):
            if entry.present:
                found.setdefault(entry.sender.id, entry.sender)
        return list(found.values())

    def is_present(self, scope: str, thread_id: str, identity_id: str) -> bool:
        return any(e.present and e.sender.id == identity_id for e in self._live(scope, thread_id))

    def own(self) -> dict[tuple[str, str], list[dict[str, Any]]]:
        items: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for key, entries in self._threads.items():
            for (origin, _), entry in entries.items():
                if origin != self.origin:
                    continue
                items.setdefault(key, []).append({
                    "sender": entry.sender.model_dump(mode="json"),
                    "present": entry.present,
                    "working": None if entry.working is None else {"label": entry.working.label},
                    "streams": [{"stream_id": s.start.stream_id, "in_reply_to": s.start.in_reply_to} for s in entry.streams.values()],
                })
        return items

    def refresh(self, scope: str, thread_id: str, origin: str, items: list[dict[str, Any]]) -> None:
        key = (scope, thread_id)
        for item in items:
            sender = Identity.model_validate(item["sender"])
            slot = (origin, sender.id)
            entry = self._threads.get(key, {}).get(slot) or _Entry(sender=sender, seen=self._clock())
            entry.seen = self._clock()
            entry.present = bool(item["present"])
            working = item.get("working")
            entry.working = None if working is None else Working(thread_id=thread_id, sender=sender, label=working.get("label"))
            entry.streams = {
                s["stream_id"]: entry.streams.get(s["stream_id"]) or _Stream(
                    StreamStart(thread_id=thread_id, sender=sender, stream_id=s["stream_id"], in_reply_to=s.get("in_reply_to"))
                )
                for s in item.get("streams", [])
            }
            self._store(key, slot, entry)
```

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: the latest-state cache — presence, work and open streams per replica, re-announced, expired on read

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 8: The hub — per-replica fan-out

Written fresh. It replaces the old broadcaster, rooms, registries, distributed pub/sub and presence stores (`../rfnry-thread/packages/server-python/src/rfnry_thread_server/{broadcast,presence}/*`, stripped) with one class: sessions per thread, a lock per thread, a `seq` cursor per session and thread, the latest-state cache, and the mirror onto the storage's bus.

The rules this file enforces, each pinned by a test below:
- **Complete and ordered per connection.** A session receives an event only when its `seq` is above the session's cursor for that thread; a gap is filled from storage first. Repeats (local delivery plus the notice of the same event) are dropped.
- **One lock per thread.** Every fan-out for a thread — local events, notices, signals, joins, deletion — runs under that thread's `asyncio.Lock`, acquired as the first `await` of a notice handler, so notices are handled in arrival order.
- **Join is atomic with its replay.** Cursor, replay, snapshot and room membership are set under the lock; nothing can slip between.
- **Presence is per identity.** A replica announces `Present` when an identity's first local session joins and nobody else already shows it present, `Left` when its last local session leaves and no other replica still shows it.
- **Mirroring only when linked.** `linked` is set by `Server.start()`; a lone server never publishes.

**Files:**
- Create: `packages/python/src/rfnry_thread/hub.py`
- Test: `packages/python/tests/unit/hubkit.py`, `packages/python/tests/unit/test_hub.py`

**Interfaces:**
- Consumes: `Storage`, `OnEvent`/`OnSignal` shapes (Task 5); `Outbound` (Task 6); `LatestState` (Task 7); events, signals, `Joined`, `Thread`, `Identity`, `SIGNALS` (Task 3); `Push` (Task 4); `ThreadException` (Task 1).
- Produces (in `rfnry_thread.hub`): `NOTIFY_LIMIT = 7000`; `Session(identity: Identity, scope: str, outbound: Outbound)` dataclass with `id`, `joined: dict[str, int]` (thread → cursor), `streams: set[tuple[str, str]]`, `working: set[str]`, `push_event(event, payload)`, `push_signal(payload)`; `Hub(storage: Storage, replay_limit: int)` with `origin: str`, `state: LatestState`, `sessions: set[Session]`, `linked: bool`, and:
  - `async join(session, thread: Thread, since: int | None) -> Joined`
  - `async leave(session, thread_id) -> None`
  - `async deliver(scope, event: Event) -> None`
  - `async signal(session, signal: Signal) -> None`
  - `async invite(scope, thread: Thread, sender: Identity, identity: Identity) -> None`
  - `async deleted(scope, thread_id, sender: Identity) -> None`
  - `async drop(session) -> None` (a connection ended)
  - `async announce() -> None` (publish this replica's own state)
  - `async on_event(scope, thread_id, seq) -> None`, `async on_signal(scope, thread_id, payload) -> None` (the storage bus callbacks)

- [ ] **Step 1: Write the test kit and the failing tests**

`packages/python/tests/unit/hubkit.py`:

```python
import json
from typing import Any

from rfnry_thread.hub import Session
from rfnry_thread.models import Identity
from rfnry_thread.outbound import Outbound


class Recorder:
    def __init__(self) -> None:
        self.frames: list[dict[str, Any]] = []

    async def send_text(self, data: str) -> None:
        self.frames.append(json.loads(data))


async def nothing() -> None:
    return None


def open_session(identity: Identity, scope: str = "acme") -> tuple[Session, Recorder]:
    recorder = Recorder()
    return Session(identity=identity, scope=scope, outbound=Outbound(recorder, nothing)), recorder


async def pushes(session: Session, recorder: Recorder) -> list[dict[str, Any]]:
    await session.outbound.drain()
    found = [frame["payload"] for frame in recorder.frames if frame["type"] == "push"]
    recorder.frames.clear()
    return found


def kinds(payloads: list[dict[str, Any]]) -> list[str]:
    return [payload["kind"] for payload in payloads]
```

`packages/python/tests/unit/test_hub.py`:

```python
from typing import Any

from rfnry_thread.hub import Hub
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import (
    SYSTEM,
    Member,
    MemberRemoved,
    StreamDelta,
    StreamStart,
    Thread,
    Working,
)
from until import until

from .factories import ANA, AT, BRUNO, HELPER, message
from .hubkit import kinds, open_session, pushes


async def setup(replay_limit: int = 500, events: int = 0) -> tuple[InMemoryStorage, Hub, Thread]:
    storage = InMemoryStorage()
    thread, _ = await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=AT))
    for identity in (ANA, HELPER, BRUNO):
        await storage.add_member("acme", Member(thread_id="t1", identity=identity, added_at=AT, added_by=SYSTEM))
    for n in range(1, events + 1):
        await storage.append_event("acme", message(n, f"m{n}"))
    return storage, Hub(storage, replay_limit), thread


async def test_a_live_join_replays_nothing_and_reports_the_head() -> None:
    _, hub, thread = await setup(events=3)
    session, recorder = open_session(HELPER)
    joined = await hub.join(session, thread, None)
    assert (joined.seq, joined.truncated) == (3, False)
    assert kinds(await pushes(session, recorder)) == ["present"]
    assert session.joined == {"t1": 3}


async def test_a_join_since_replays_what_followed_and_truncates_to_the_latest_page() -> None:
    _, hub, thread = await setup(replay_limit=3, events=5)
    first, first_recorder = open_session(HELPER)
    joined = await hub.join(first, thread, 3)
    assert [p.get("seq") for p in await pushes(first, first_recorder)] == [4, 5, None]
    assert joined.truncated is False
    second, second_recorder = open_session(BRUNO)
    joined = await hub.join(second, thread, 0)
    assert [p["seq"] for p in await pushes(second, second_recorder) if p["kind"] == "message"] == [3, 4, 5]
    assert joined.truncated is True and second.joined["t1"] == 5


async def test_delivery_drops_repeats_and_fills_gaps_from_storage() -> None:
    storage, hub, thread = await setup(events=1)
    session, recorder = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    for n in (2, 3, 4):
        await storage.append_event("acme", message(n, f"m{n}"))
    fourth = (await storage.list_events("acme", "t1", since=3))[0]
    await hub.deliver("acme", fourth)
    await hub.deliver("acme", fourth)
    assert [p["seq"] for p in await pushes(session, recorder)] == [2, 3, 4]


async def test_a_notice_is_read_only_when_a_session_is_behind() -> None:
    storage, hub, thread = await setup()
    reads: list[int] = []
    original = storage.list_events

    async def counted(*args: Any, **kwargs: Any) -> Any:
        reads.append(1)
        return await original(*args, **kwargs)

    session, recorder = open_session(HELPER)
    await hub.join(session, thread, None)
    await pushes(session, recorder)
    storage.list_events = counted  # type: ignore[method-assign]
    stored, _ = await storage.append_event("acme", message(1, "hi"))
    await hub.on_event("acme", "t1", stored.seq)
    await hub.on_event("acme", "t1", stored.seq)
    await hub.on_event("acme", "other-thread", 1)
    assert [p["seq"] for p in await pushes(session, recorder)] == [1]
    assert len(reads) == 1


async def test_a_removed_member_is_cut_off_after_the_event() -> None:
    storage, hub, thread = await setup()
    bruno, bruno_recorder = open_session(BRUNO)
    helper, helper_recorder = open_session(HELPER)
    await hub.join(bruno, thread, None)
    await hub.join(helper, thread, None)
    await pushes(bruno, bruno_recorder)
    removal, _ = await storage.append_event("acme", MemberRemoved(
        id="r", thread_id="t1", author=ANA, created_at=AT, client_id="r", identity=BRUNO))
    await hub.deliver("acme", removal)
    after, _ = await storage.append_event("acme", message(2, "after"))
    await hub.deliver("acme", after)
    assert kinds(await pushes(bruno, bruno_recorder)) == ["member_removed"]
    assert "t1" not in bruno.joined
    assert kinds(await pushes(helper, helper_recorder))[-3:] == ["member_removed", "left", "message"]


async def test_a_joining_session_first_receives_the_current_state() -> None:
    _, hub, thread = await setup()
    helper, _ = open_session(HELPER)
    await hub.join(helper, thread, None)
    await hub.signal(helper, Working(thread_id="t1", sender=HELPER, label="Reading"))
    await hub.signal(helper, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    await hub.signal(helper, StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text="Half"))
    ana, recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    assert kinds(await pushes(ana, recorder)) == ["present", "working", "stream.start", "stream.delta", "present"]


async def test_two_connections_of_one_member_are_one_presence() -> None:
    _, hub, thread = await setup()
    helper, helper_recorder = open_session(HELPER)
    await hub.join(helper, thread, None)
    await pushes(helper, helper_recorder)
    tab_one, _ = open_session(ANA)
    tab_two, _ = open_session(ANA)
    await hub.join(tab_one, thread, None)
    await hub.join(tab_two, thread, None)
    assert kinds(await pushes(helper, helper_recorder)) == ["present"]
    await hub.leave(tab_one, "t1")
    assert await pushes(helper, helper_recorder) == []
    await hub.drop(tab_two)
    assert kinds(await pushes(helper, helper_recorder)) == ["left"]
    assert hub.state.present("acme", "t1") == [HELPER]


async def test_a_dropped_connection_ends_its_streams_and_work() -> None:
    _, hub, thread = await setup()
    helper, _ = open_session(HELPER)
    ana, recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    await hub.join(helper, thread, None)
    await hub.signal(helper, Working(thread_id="t1", sender=HELPER))
    await hub.signal(helper, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    await pushes(ana, recorder)
    await hub.drop(helper)
    assert kinds(await pushes(ana, recorder)) == ["stream.end", "idle", "left"]
    assert [s.kind for s in hub.state.snapshot("acme", "t1")] == ["present"]


async def test_an_invitation_reaches_only_connections_not_in_the_thread() -> None:
    _, hub, thread = await setup()
    joined, joined_recorder = open_session(BRUNO)
    elsewhere, elsewhere_recorder = open_session(BRUNO)
    other_scope, other_recorder = open_session(BRUNO, scope="other")
    hub.sessions.update({joined, elsewhere, other_scope})
    await hub.join(joined, thread, None)
    await pushes(joined, joined_recorder)
    await hub.invite("acme", thread, ANA, BRUNO)
    assert await pushes(joined, joined_recorder) == []
    invited = await pushes(elsewhere, elsewhere_recorder)
    assert kinds(invited) == ["invited"] and invited[0]["thread"]["id"] == "t1"
    assert await pushes(other_scope, other_recorder) == []


async def test_deletion_tells_the_room_and_empties_it() -> None:
    _, hub, thread = await setup()
    ana, recorder = open_session(ANA)
    await hub.join(ana, thread, None)
    await pushes(ana, recorder)
    await hub.deleted("acme", "t1", ANA)
    assert kinds(await pushes(ana, recorder)) == ["thread.deleted"]
    assert ana.joined == {} and hub.state.snapshot("acme", "t1") == []


async def test_linked_hubs_mirror_signals_and_drop_their_own() -> None:
    storage, near, thread = await setup()
    far = Hub(storage, 500)
    for hub in (near, far):
        hub.linked = True
        await storage.listen(hub.on_event, hub.on_signal)
    speaker, speaker_recorder = open_session(HELPER)
    listener, listener_recorder = open_session(ANA)
    await near.join(speaker, thread, None)
    await far.join(listener, thread, None)
    await pushes(speaker, speaker_recorder)
    await pushes(listener, listener_recorder)
    await near.signal(speaker, Working(thread_id="t1", sender=HELPER, label="Reading"))
    assert kinds(await pushes(listener, listener_recorder)) == ["working"]
    assert kinds(await pushes(speaker, speaker_recorder)) == ["working"]
    assert far.state.snapshot("acme", "t1")[-1] == Working(thread_id="t1", sender=HELPER, label="Reading")


async def test_a_long_delta_is_mirrored_in_pieces_under_the_notify_limit() -> None:
    storage, near, thread = await setup()
    far = Hub(storage, 500)
    published: list[int] = []
    original = storage.publish

    async def measured(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        published.append(len(str(payload)))
        await original(scope, thread_id, payload)

    storage.publish = measured  # type: ignore[method-assign]
    for hub in (near, far):
        hub.linked = True
        await storage.listen(hub.on_event, hub.on_signal)
    speaker, _ = open_session(HELPER)
    listener, recorder = open_session(ANA)
    await near.join(speaker, thread, None)
    await far.join(listener, thread, None)
    await pushes(listener, recorder)
    await near.signal(speaker, StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"))
    await near.signal(speaker, StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text="x" * 20000))
    received = await pushes(listener, recorder)
    assert "".join(p.get("text", "") for p in received) == "x" * 20000
    assert max(published) < 7900


async def test_announce_teaches_another_replica_who_is_here() -> None:
    storage, near, thread = await setup()
    far = Hub(storage, 500)
    ana, _ = open_session(ANA)
    await near.join(ana, thread, None)
    for hub in (near, far):
        hub.linked = True
        await storage.listen(hub.on_event, hub.on_signal)
    assert far.state.present("acme", "t1") == []
    await near.announce()
    await until(lambda: far.state.present("acme", "t1") == [ANA])
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_hub.py -q`
Expected: FAIL — `No module named 'rfnry_thread.hub'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/hub.py`:

```python
from __future__ import annotations

import asyncio
import json
import logging
import uuid
import weakref
from dataclasses import dataclass, field
from typing import Any

from rfnry_thread.errors import ThreadException
from rfnry_thread.models import (
    SIGNALS,
    Event,
    Identity,
    Idle,
    Invited,
    Joined,
    Left,
    MemberRemoved,
    Present,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Thread,
    ThreadDeleted,
    Working,
)
from rfnry_thread.outbound import Outbound
from rfnry_thread.protocol import Push
from rfnry_thread.state import LatestState
from rfnry_thread.storage import Storage

log = logging.getLogger("rfnry_thread")

NOTIFY_LIMIT = 7000
PIECE = 1000


@dataclass(eq=False)
class Session:
    identity: Identity
    scope: str
    outbound: Outbound
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    joined: dict[str, int] = field(default_factory=dict)
    streams: set[tuple[str, str]] = field(default_factory=set)
    working: set[str] = field(default_factory=set)

    def push_event(self, event: Event, payload: dict[str, Any]) -> None:
        self.joined[event.thread_id] = event.seq
        self.outbound.put(Push(name="event", payload=payload))

    def push_signal(self, payload: dict[str, Any]) -> None:
        self.outbound.put(Push(name="signal", payload=payload))


def _size(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload).encode())


class Hub:
    def __init__(self, storage: Storage, replay_limit: int) -> None:
        self.storage = storage
        self.replay_limit = replay_limit
        self.origin = uuid.uuid4().hex
        self.state = LatestState(self.origin)
        self.sessions: set[Session] = set()
        self.linked = False
        self._rooms: dict[tuple[str, str], set[Session]] = {}
        self._locks: weakref.WeakValueDictionary[tuple[str, str], asyncio.Lock] = weakref.WeakValueDictionary()
        self._local: dict[tuple[str, str, str], int] = {}

    def _lock(self, scope: str, thread_id: str) -> asyncio.Lock:
        lock = self._locks.get((scope, thread_id))
        if lock is None:
            lock = asyncio.Lock()
            self._locks[(scope, thread_id)] = lock
        return lock

    def _room(self, scope: str, thread_id: str) -> list[Session]:
        return list(self._rooms.get((scope, thread_id), ()))

    async def join(self, session: Session, thread: Thread, since: int | None) -> Joined:
        scope, thread_id = session.scope, thread.id
        again = thread_id in session.joined
        async with self._lock(scope, thread_id):
            latest = await self.storage.list_events(scope, thread_id, limit=1)
            head = latest[-1].seq if latest else 0
            replay: list[Event] = []
            truncated = False
            if since is not None and since < head:
                if head - since > self.replay_limit:
                    replay = await self.storage.list_events(scope, thread_id, before=head + 1, limit=self.replay_limit)
                    truncated = True
                else:
                    replay = await self.storage.list_events(scope, thread_id, since=since, limit=self.replay_limit)
            session.joined[thread_id] = replay[0].seq - 1 if replay else head
            self._rooms.setdefault((scope, thread_id), set()).add(session)
            for event in replay:
                session.push_event(event, event.model_dump(mode="json"))
            for signal in self.state.snapshot(scope, thread_id):
                session.push_signal(signal.model_dump(mode="json"))
        if not again:
            await self._arrive(session, thread_id)
        return Joined(thread=thread, seq=head, truncated=truncated)

    async def leave(self, session: Session, thread_id: str) -> None:
        if session.joined.pop(thread_id, None) is None:
            return
        room = self._rooms.get((session.scope, thread_id))
        if room is not None:
            room.discard(session)
            if not room:
                del self._rooms[(session.scope, thread_id)]
        await self._depart(session, thread_id)

    async def deliver(self, scope: str, event: Event) -> None:
        async with self._lock(scope, event.thread_id):
            cut = await self._deliver(scope, event)
        for session in cut:
            await self.leave(session, event.thread_id)

    async def _deliver(self, scope: str, event: Event) -> list[Session]:
        payload = event.model_dump(mode="json")
        cut: list[Session] = []
        for session in self._room(scope, event.thread_id):
            cursor = session.joined.get(event.thread_id)
            if cursor is None or event.seq <= cursor:
                continue
            if event.seq > cursor + 1:
                missing = await self.storage.list_events(scope, event.thread_id, since=cursor, before=event.seq, limit=event.seq - cursor - 1)
                for earlier in missing:
                    session.push_event(earlier, earlier.model_dump(mode="json"))
            session.push_event(event, payload)
            if isinstance(event, MemberRemoved) and event.identity.id == session.identity.id:
                cut.append(session)
        return cut

    async def on_event(self, scope: str, thread_id: str, seq: int) -> None:
        if not any(s.joined.get(thread_id, seq) < seq for s in self._room(scope, thread_id)):
            return
        try:
            async with self._lock(scope, thread_id):
                found = await self.storage.list_events(scope, thread_id, since=seq - 1, limit=1)
                cut = await self._deliver(scope, found[0]) if found and found[0].seq == seq else []
            for session in cut:
                await self.leave(session, thread_id)
        except ThreadException as error:
            log.warning("hub: a notice for thread %r failed: %s", thread_id, error)

    async def signal(self, session: Session, signal: Signal) -> None:
        key = (signal.thread_id, getattr(signal, "stream_id", ""))
        if isinstance(signal, StreamStart):
            session.streams.add(key)
        elif isinstance(signal, StreamEnd):
            session.streams.discard(key)
        elif isinstance(signal, Working):
            session.working.add(signal.thread_id)
        elif isinstance(signal, Idle):
            session.working.discard(signal.thread_id)
        await self._fanout(session.scope, signal, self.origin)
        await self._mirror(session.scope, signal)

    async def _fanout(self, scope: str, signal: Signal, origin: str) -> None:
        async with self._lock(scope, signal.thread_id):
            if self._visible(scope, signal, origin):
                payload = signal.model_dump(mode="json")
                for session in self._room(scope, signal.thread_id):
                    session.push_signal(payload)

    def _visible(self, scope: str, signal: Signal, origin: str) -> bool:
        if isinstance(signal, Present):
            before = self.state.is_present(scope, signal.thread_id, signal.sender.id)
            self.state.apply(scope, signal, origin)
            return not before
        self.state.apply(scope, signal, origin)
        if isinstance(signal, Left):
            return not self.state.is_present(scope, signal.thread_id, signal.sender.id)
        return True

    async def _arrive(self, session: Session, thread_id: str) -> None:
        key = (session.scope, thread_id, session.identity.id)
        self._local[key] = self._local.get(key, 0) + 1
        if self._local[key] == 1:
            await self.signal(session, Present(thread_id=thread_id, sender=session.identity))

    async def _depart(self, session: Session, thread_id: str) -> None:
        key = (session.scope, thread_id, session.identity.id)
        count = self._local.get(key, 0) - 1
        if count > 0:
            self._local[key] = count
            return
        self._local.pop(key, None)
        await self.signal(session, Left(thread_id=thread_id, sender=session.identity))

    async def invite(self, scope: str, thread: Thread, sender: Identity, identity: Identity) -> None:
        self._invite_here(scope, thread, sender, identity.id)
        await self._publish(scope, thread.id, {"kind": "invited", "thread_id": thread.id,
                                               "sender": sender.model_dump(mode="json"), "to": identity.id})

    def _invite_here(self, scope: str, thread: Thread, sender: Identity, identity_id: str) -> None:
        payload = Invited(thread_id=thread.id, sender=sender, thread=thread).model_dump(mode="json")
        for session in list(self.sessions):
            if session.scope == scope and session.identity.id == identity_id and thread.id not in session.joined:
                session.push_signal(payload)

    async def deleted(self, scope: str, thread_id: str, sender: Identity) -> None:
        signal = ThreadDeleted(thread_id=thread_id, sender=sender)
        await self._deleted_here(scope, signal)
        await self._mirror(scope, signal)

    async def _deleted_here(self, scope: str, signal: ThreadDeleted) -> None:
        thread_id = signal.thread_id
        async with self._lock(scope, thread_id):
            self.state.apply(scope, signal, self.origin)
            payload = signal.model_dump(mode="json")
            for session in self._room(scope, thread_id):
                session.push_signal(payload)
                session.joined.pop(thread_id, None)
                session.working.discard(thread_id)
                session.streams = {s for s in session.streams if s[0] != thread_id}
            self._rooms.pop((scope, thread_id), None)
            for key in [k for k in self._local if k[:2] == (scope, thread_id)]:
                del self._local[key]

    async def drop(self, session: Session) -> None:
        for thread_id, stream_id in sorted(session.streams):
            await self.signal(session, StreamEnd(thread_id=thread_id, sender=session.identity, stream_id=stream_id))
        for thread_id in sorted(session.working):
            await self.signal(session, Idle(thread_id=thread_id, sender=session.identity))
        for thread_id in list(session.joined):
            await self.leave(session, thread_id)
        self.sessions.discard(session)

    async def announce(self) -> None:
        for (scope, thread_id), items in self.state.own().items():
            batch: list[dict[str, Any]] = []
            for item in items:
                if _size({"entries": [*batch, item]}) > NOTIFY_LIMIT and batch:
                    await self._publish(scope, thread_id, {"kind": "refresh", "entries": batch})
                    batch = []
                batch.append(item)
            if batch:
                await self._publish(scope, thread_id, {"kind": "refresh", "entries": batch})

    async def _mirror(self, scope: str, signal: Signal) -> None:
        if not self.linked:
            return
        payload = signal.model_dump(mode="json")
        if _size(payload) <= NOTIFY_LIMIT:
            await self._publish(scope, signal.thread_id, payload)
            return
        if not isinstance(signal, StreamDelta):
            log.warning("hub: a %s signal of %d bytes is too large to mirror", signal.kind, _size(payload))
            return
        for start in range(0, len(signal.text), PIECE):
            piece = signal.model_copy(update={"text": signal.text[start : start + PIECE]})
            await self._publish(scope, signal.thread_id, piece.model_dump(mode="json"))

    async def _publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        if not self.linked:
            return
        try:
            await self.storage.publish(scope, thread_id, {"o": self.origin, **payload})
        except ThreadException as error:
            log.warning("hub: mirroring to other replicas failed: %s", error)

    async def on_signal(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        origin = payload.pop("o", None)
        if origin is None or origin == self.origin:
            return
        try:
            kind = payload.get("kind")
            if kind == "refresh":
                async with self._lock(scope, thread_id):
                    self.state.refresh(scope, thread_id, str(origin), payload["entries"])
                return
            if kind == "invited":
                thread = await self.storage.get_thread(scope, thread_id)
                if thread is not None:
                    self._invite_here(scope, thread, Identity.model_validate(payload["sender"]), str(payload["to"]))
                return
            signal = SIGNALS.validate_python(payload)
            if isinstance(signal, ThreadDeleted):
                await self._deleted_here(scope, signal)
                return
            await self._fanout(scope, signal, str(origin))
        except Exception as error:
            log.warning("hub: a notice from replica %s failed: %s", origin, error)
```

Notes for the implementer:
- `_mirror` is a no-op until `linked`; `_publish` checks again so `announce` and `invite` are too.
- `test_a_long_delta_is_mirrored_in_pieces_under_the_notify_limit` measures `len(str(payload))` of what the hub publishes; the storage adds `{"s", "t", "p"}` around it, still under Postgres's 8000-byte limit given `NOTIFY_LIMIT = 7000` and 1000-character pieces.

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: the hub — per-connection cursors, atomic joins, per-thread order, presence per identity, mirroring

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 9: The server core — serve, hello, membership, authorize, the verbs

The frame loop and the teardown are adapted from `../rfnry-thread/packages/server-python/src/rfnry_thread_server/transport/session_loop.py` (`run_session`: refuse-then-close, in-flight request tasks cancelled on exit, a cap above which requests run inline); its authentication and hello deadline are not: the consumer authenticates the upgrade and hands `serve` the identity and scope (design §18). The verbs are written fresh: scope-first, membership on every verb, `authorize` with five actions, authorship stamped from the session.

**Files:**
- Create: `packages/python/src/rfnry_thread/connection.py`, `packages/python/src/rfnry_thread/pipe.py`, `packages/python/src/rfnry_thread/verbs.py`, `packages/python/src/rfnry_thread/server.py`
- Test: `packages/python/tests/unit/conftest.py`, `packages/python/tests/unit/peer.py`, `packages/python/tests/unit/test_server.py`, `packages/python/tests/unit/test_verbs.py`

**Interfaces:**
- Consumes: `Hub`, `Session` (Task 8); `Outbound` (Task 6); `Storage`, `InMemoryStorage` (Task 5); frames, `encode`, `decode`, `failure`, close codes, `HELLO`, `PROTOCOL` (Task 4); models (Task 3); errors (Task 1).
- Produces:
  - `rfnry_thread.connection`: `Connection` protocol — `async receive_text() -> str`, `async send_text(data: str) -> None`, `async close(code: int = 1000, reason: str | None = None) -> None`.
  - `rfnry_thread.pipe`: `End` (a `Connection` with `code: int | None`, the close code either side used) and `pipe() -> tuple[End, End]`.
  - `rfnry_thread.verbs`: `Authorize = Callable[[Identity, str, Thread], Awaitable[bool]]`; `CUSTOM_LIMIT = 65536`; `LABEL_LIMIT = 1000`; `PAGE_LIMIT = 500`; `Verbs(storage, hub, authorize, max_inline_bytes)` with `thread(actor, scope, thread_id, action) -> Thread`, `create_thread(actor, scope, thread_id: str | None, metadata) -> Thread`, `get_thread`, `list_threads(actor, scope, before, limit) -> list[Thread]`, `update_thread(actor, scope, thread_id, metadata) -> Thread`, `delete_thread(actor, scope, thread_id) -> None`, `add_member(actor, scope, thread_id, identity) -> Member`, `remove_member(actor, scope, thread_id, identity_id) -> None`, `list_members(actor, scope, thread_id) -> list[Member]`, `send(actor, scope, payload: dict) -> Event`, `history(actor, scope, thread_id, before, limit, kinds) -> list[Event]`, `join(session, thread_id, since) -> Joined`, `leave(session, thread_id) -> None`. (Task 10 adds `signal` and `present`.)
  - `rfnry_thread.server`: `Server(storage, *, authorize=None, max_inline_bytes=262144, replay_limit=500)` with `async serve(connection, identity, scope)` (raises `ConfigurationError` for a `system` identity and `InputError` for an empty scope before reading the socket; then expects a `hello` for protocol 1 as the first frame, answering anything else with an error frame and close 4000), `_attach(identity, scope) -> End` (in-process, trusted; used by `connect` in Task 12 and by tests), and the system verbs `create_thread(scope, thread_id=None, metadata=None) -> Thread`, `add_member(scope, thread_id, identity) -> Member`, `delete_thread(scope, thread_id) -> None`, `delete_scope(scope) -> int`, `verify_absent(scope) -> bool`.
  - Tests: `tests/unit/peer.py` — `Peer.open(server, identity, scope="acme")`, `peer.call(method, **payload) -> dict` (raises the mapped error), `peer.notify(method, **payload)`, `peer.push(kind, timeout=1.0) -> dict`, `peer.quiet(wait=0.15) -> list[dict]`, `peer.close()`; `tests/unit/conftest.py` — fixtures `storage`, `server`, and `say(peer, thread_id, text, client_id=None) -> dict`.

- [ ] **Step 1: Write the test kit**

`packages/python/tests/unit/conftest.py`:

```python
import uuid
from typing import Any

import pytest

from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.server import Server

from .peer import Peer


@pytest.fixture
def storage() -> InMemoryStorage:
    return InMemoryStorage()


@pytest.fixture
def server(storage: InMemoryStorage) -> Server:
    return Server(storage)


async def say(peer: Peer, thread_id: str, text: str, client_id: str | None = None, **extra: Any) -> dict[str, Any]:
    reply = await peer.call("event.send", thread_id=thread_id, kind="message", client_id=client_id or uuid.uuid4().hex,
                            content=[{"type": "text", "text": text}], **extra)
    return dict(reply["event"])
```

`packages/python/tests/unit/peer.py`:

```python
from __future__ import annotations

import asyncio
import itertools
import json
from typing import Any

from rfnry_thread.errors import raised
from rfnry_thread.models import Identity
from rfnry_thread.pipe import End
from rfnry_thread.protocol import Hello, Notify, Request, encode
from rfnry_thread.server import Server


class Peer:
    def __init__(self, end: End) -> None:
        self.end = end
        self.pushes: list[dict[str, Any]] = []
        self._ids = itertools.count(1)

    @classmethod
    async def open(cls, server: Server, identity: Identity, scope: str = "acme") -> Peer:
        peer = cls(server._attach(identity, scope))
        await peer.end.send_text(encode(Hello(protocol=1)))
        reply = await peer.frame()
        assert reply["type"] == "response" and reply["id"] == "hello", reply
        return peer

    async def frame(self, timeout: float = 1.0) -> dict[str, Any]:
        return dict(json.loads(await asyncio.wait_for(self.end.receive_text(), timeout)))

    async def call(self, method: str, **payload: Any) -> dict[str, Any]:
        request_id = str(next(self._ids))
        await self.end.send_text(encode(Request(id=request_id, method=method, payload=payload)))
        while True:
            frame = await self.frame()
            if frame["type"] == "push":
                self.pushes.append(frame["payload"])
            elif frame.get("id") == request_id and frame["type"] == "error":
                raise raised(frame["error"]["code"], frame["error"]["message"])
            elif frame.get("id") == request_id:
                return dict(frame["payload"])

    async def notify(self, method: str, **payload: Any) -> None:
        await self.end.send_text(encode(Notify(method=method, payload=payload)))

    async def push(self, kind: str, timeout: float = 1.0) -> dict[str, Any]:
        async def find() -> dict[str, Any]:
            while True:
                for index, payload in enumerate(self.pushes):
                    if payload["kind"] == kind:
                        return self.pushes.pop(index)
                frame = await self.frame(timeout)
                if frame["type"] == "push":
                    self.pushes.append(frame["payload"])

        return await asyncio.wait_for(find(), timeout)

    async def quiet(self, wait: float = 0.15) -> list[dict[str, Any]]:
        try:
            while True:
                frame = await self.frame(wait)
                if frame["type"] == "push":
                    self.pushes.append(frame["payload"])
        except Exception:
            found, self.pushes = self.pushes, []
            return found

    async def close(self) -> None:
        await self.end.close()
```

- [ ] **Step 2: Write the failing tests**

`packages/python/tests/unit/test_server.py`:

```python
import asyncio
import json

import pytest

from rfnry_thread.errors import ConfigurationError, InputError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import SYSTEM
from rfnry_thread.pipe import pipe
from rfnry_thread.protocol import Hello, encode
from rfnry_thread.server import Server

from .factories import ANA
from .peer import Peer


def test_limits_are_positive() -> None:
    with pytest.raises(ConfigurationError, match="^server: max_inline_bytes must be positive$"):
        Server(InMemoryStorage(), max_inline_bytes=0)
    with pytest.raises(ConfigurationError, match="^server: replay_limit must be positive$"):
        Server(InMemoryStorage(), replay_limit=0)


async def test_a_hello_is_answered_with_who_and_where_the_consumer_said(server: Server) -> None:
    ours, theirs = pipe()
    task = asyncio.create_task(server.serve(theirs, identity=ANA, scope="acme"))
    await ours.send_text(encode(Hello(protocol=1)))
    reply = json.loads(await asyncio.wait_for(ours.receive_text(), 1.0))
    assert reply == {"type": "response", "id": "hello", "payload": {
        "identity": {"role": "user", "id": "ana", "name": "Ana", "metadata": {}}, "scope": "acme"}}
    await ours.close()
    await asyncio.wait_for(task, 1.0)


async def test_serve_refuses_the_system_role_and_an_empty_scope_before_reading(server: Server) -> None:
    _, theirs = pipe()
    with pytest.raises(ConfigurationError, match="^server: 'system' is the library's role"):
        await server.serve(theirs, identity=SYSTEM, scope="acme")
    with pytest.raises(InputError, match="^server: a scope is required$"):
        await server.serve(theirs, identity=ANA, scope="")
    with pytest.raises(ConfigurationError, match="^server: 'system' is the library's role"):
        server._attach(SYSTEM, "acme")
    with pytest.raises(InputError, match="^server: a scope is required$"):
        server._attach(ANA, " ")


@pytest.mark.parametrize("first", ["garbage", '{"type": "hello", "protocol": 2}', '{"type": "request", "id": "1", "method": "thread.get"}'])
async def test_a_bad_first_frame_closes_as_a_protocol_error(server: Server, first: str) -> None:
    ours, theirs = pipe()
    task = asyncio.create_task(server.serve(theirs, identity=ANA, scope="acme"))
    await ours.send_text(first)
    await asyncio.wait_for(task, 1.0)
    assert ours.code == 4000


async def test_a_wrong_protocol_is_told_which_one_the_server_speaks(server: Server) -> None:
    ours, theirs = pipe()
    task = asyncio.create_task(server.serve(theirs, identity=ANA, scope="acme"))
    await ours.send_text('{"type": "hello", "protocol": 2}')
    reply = json.loads(await asyncio.wait_for(ours.receive_text(), 1.0))
    assert reply == {"type": "error", "id": "hello", "error": {
        "code": "input", "message": "server: the first frame must be a hello for protocol 1"}}
    await asyncio.wait_for(task, 1.0)


async def test_a_malformed_frame_after_hello_closes(server: Server) -> None:
    peer = await Peer.open(server, ANA)
    await peer.end.send_text("{")
    await asyncio.sleep(0.05)
    assert peer.end.code == 4000


async def test_an_unknown_method_is_an_input_error(server: Server) -> None:
    peer = await Peer.open(server, ANA)
    with pytest.raises(InputError, match="^server: unknown method 'thread.shout'$"):
        await peer.call("thread.shout")
```

`packages/python/tests/unit/test_verbs.py`:

```python
import pytest

from rfnry_thread.errors import ForbiddenError, InputError, NotFoundError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import Identity, Thread
from rfnry_thread.server import Server

from .conftest import say
from .factories import ANA, BRUNO, HELPER
from .peer import Peer


async def room(server: Server, *members: Identity) -> Peer:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1", metadata={"topic": "billing"})
    for member in members:
        await ana.call("member.add", thread_id="t1", identity=member.model_dump(mode="json"))
    return ana


async def test_create_is_get_or_create_and_the_creator_is_the_first_member(server: Server) -> None:
    ana = await room(server)
    again = await ana.call("thread.create", thread_id="t1", metadata={"topic": "ignored"})
    assert again["thread"]["metadata"] == {"topic": "billing"}
    members = await ana.call("member.list", thread_id="t1")
    assert [m["identity"]["id"] for m in members["members"]] == ["ana"]
    history = await ana.call("thread.history", thread_id="t1")
    assert [(e["kind"], e["identity"]["id"]) for e in history["events"]] == [("member_added", "ana")]
    generated = await ana.call("thread.create")
    assert len(generated["thread"]["id"]) == 32


async def test_a_non_member_is_forbidden_everywhere_and_a_missing_thread_is_not_found(server: Server) -> None:
    await room(server)
    bruno = await Peer.open(server, BRUNO)
    for method, extra in [("thread.get", {}), ("thread.history", {}), ("thread.join", {}), ("member.list", {}),
                          ("thread.update", {"metadata": {}}), ("thread.delete", {}), ("thread.create", {})]:
        with pytest.raises(ForbiddenError, match="^server: 'bruno' is not a member of thread 't1'$"):
            await bruno.call(method, thread_id="t1", **extra)
    with pytest.raises(ForbiddenError):
        await say(bruno, "t1", "let me in")
    with pytest.raises(NotFoundError, match="^server: thread 'ghost' was not found$"):
        await bruno.call("thread.get", thread_id="ghost")


async def test_threads_are_listed_update_writes_an_event_and_delete_removes(server: Server) -> None:
    ana = await room(server)
    await ana.call("thread.create", thread_id="t2")
    assert [t["id"] for t in (await ana.call("thread.list"))["threads"]] == ["t2", "t1"]
    assert [t["id"] for t in (await ana.call("thread.list", before="t2"))["threads"]] == ["t1"]
    updated = await ana.call("thread.update", thread_id="t1", metadata={"topic": "refunds"})
    assert updated["thread"]["metadata"] == {"topic": "refunds"}
    kinds = [e["kind"] for e in (await ana.call("thread.history", thread_id="t1"))["events"]]
    assert kinds[-1] == "thread_updated"
    await ana.call("thread.delete", thread_id="t1")
    with pytest.raises(NotFoundError):
        await ana.call("thread.get", thread_id="t1")


async def test_members_are_added_and_removed_through_the_log(server: Server) -> None:
    ana = await room(server, BRUNO)
    again = await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    assert again["member"]["identity"]["id"] == "bruno"
    await ana.call("member.remove", thread_id="t1", identity_id="bruno")
    events = (await ana.call("thread.history", thread_id="t1"))["events"]
    assert [e["kind"] for e in events] == ["member_added", "member_added", "member_removed"]
    with pytest.raises(NotFoundError, match="^server: 'bruno' is not a member of thread 't1'$"):
        await ana.call("member.remove", thread_id="t1", identity_id="bruno")
    with pytest.raises(InputError, match="^server: the system identity cannot be a member$"):
        await ana.call("member.add", thread_id="t1", identity={"role": "system", "id": "system"})


async def test_authorship_is_stamped_and_a_repeat_is_answered_with_the_stored_event(server: Server) -> None:
    ana = await room(server)
    first = await say(ana, "t1", "hello", client_id="c1", author=BRUNO.model_dump(mode="json"), seq=99, id="forged")
    again = await say(ana, "t1", "hello, twice", client_id="c1")
    assert first["author"]["id"] == "ana" and first["seq"] == 2 and first["id"] != "forged"
    assert again == first


async def test_events_are_validated_before_they_are_stored(server: Server, storage: InMemoryStorage) -> None:
    small = Server(storage, max_inline_bytes=4)
    ana = await room(small)
    image = {"type": "image", "media_type": "image/png", "data": "AAAAAAAA"}
    with pytest.raises(InputError, match="^server: inline media is 6 bytes; the cap is 4 — upload it and send a url$"):
        await ana.call("event.send", thread_id="t1", kind="message", client_id="c1", content=[image])
    with pytest.raises(InputError, match="^models: image carries data or url, not both$"):
        await ana.call("event.send", thread_id="t1", kind="message", client_id="c2", content=[{"type": "image", "media_type": "image/png"}])
    with pytest.raises(InputError, match="^server: a custom payload is at most 65536 bytes$"):
        await ana.call("event.send", thread_id="t1", kind="custom", client_id="c3", name="big", payload={"x": "y" * 70000})
    with pytest.raises(InputError, match="^server: unknown event kind 'member_added'$"):
        await ana.call("event.send", thread_id="t1", kind="member_added", client_id="c4", identity=BRUNO.model_dump())
    with pytest.raises(InputError, match="^server: event.send needs a client_id$"):
        await ana.call("event.send", thread_id="t1", kind="message", content=[])
    with pytest.raises(InputError, match="^server: invalid message event"):
        await ana.call("event.send", thread_id="t1", kind="message", client_id="c5")


async def test_only_the_author_edits_or_retracts_and_only_what_can_be(server: Server) -> None:
    ana = await room(server, HELPER)
    helper = await Peer.open(server, HELPER)
    mine = await say(ana, "t1", "hello")
    step = (await helper.call("event.send", thread_id="t1", kind="step", client_id="s1", name="Read"))["event"]
    edit = await ana.call("event.send", thread_id="t1", kind="edit", client_id="e1", target=mine["id"], content=[{"type": "text", "text": "hi"}])
    assert edit["event"]["target"] == mine["id"]
    with pytest.raises(ForbiddenError, match=f"^server: only its author can edit event '{mine['id']}'$"):
        await helper.call("event.send", thread_id="t1", kind="edit", client_id="e2", target=mine["id"], content=[])
    with pytest.raises(InputError, match=f"^server: only a message can be edited; '{step['id']}' is a step$"):
        await helper.call("event.send", thread_id="t1", kind="edit", client_id="e3", target=step["id"], content=[])
    await helper.call("event.send", thread_id="t1", kind="retract", client_id="r1", target=step["id"])
    with pytest.raises(NotFoundError, match="^server: event 'ghost' was not found$"):
        await ana.call("event.send", thread_id="t1", kind="retract", client_id="r2", target="ghost")


async def test_history_pages_back_and_filters(server: Server) -> None:
    ana = await room(server)
    for n in range(5):
        await say(ana, "t1", f"m{n}")
    page = (await ana.call("thread.history", thread_id="t1", limit=2))["events"]
    assert [e["seq"] for e in page] == [5, 6]
    older = (await ana.call("thread.history", thread_id="t1", before=5, limit=2))["events"]
    assert [e["seq"] for e in older] == [3, 4]
    only = (await ana.call("thread.history", thread_id="t1", kinds=["member_added"]))["events"]
    assert [e["seq"] for e in only] == [1]
    with pytest.raises(InputError, match="^server: limit must be between 1 and 500$"):
        await ana.call("thread.history", thread_id="t1", limit=0)


async def test_authorize_decides_each_of_the_five_actions(storage: InMemoryStorage) -> None:
    asked: list[tuple[str, str]] = []

    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        asked.append((identity.id, action))
        return action != "delete"

    server = Server(storage, authorize=authorize)
    ana = await room(server, BRUNO)
    await ana.call("thread.get", thread_id="t1")
    await say(ana, "t1", "hi")
    with pytest.raises(ForbiddenError, match="^server: 'ana' may not delete in thread 't1'$"):
        await ana.call("thread.delete", thread_id="t1")
    assert [action for _, action in asked] == ["create", "manage", "read", "send", "delete"]


async def test_joined_members_receive_events_in_order_and_a_join_since_replays(server: Server) -> None:
    ana = await room(server, HELPER)
    helper = await Peer.open(server, HELPER)
    joined = await helper.call("thread.join", thread_id="t1")
    assert joined["seq"] == 2 and joined["truncated"] is False
    for n in range(3):
        await say(ana, "t1", f"m{n}")
    received = [p for p in await helper.quiet() if p["kind"] == "message"]
    assert [p["seq"] for p in received] == [3, 4, 5]
    late = await Peer.open(server, HELPER)
    await late.call("thread.join", thread_id="t1", since=3)
    assert [p["seq"] for p in await late.quiet() if p["kind"] == "message"] == [4, 5]


async def test_a_removed_member_stops_receiving_over_the_server(server: Server) -> None:
    ana = await room(server, BRUNO)
    bruno = await Peer.open(server, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    await ana.call("member.remove", thread_id="t1", identity_id="bruno")
    await say(ana, "t1", "after")
    assert [p["kind"] for p in await bruno.quiet() if p["kind"] in ("member_removed", "message")] == ["member_removed"]


async def test_nothing_crosses_scopes(server: Server) -> None:
    a = await Peer.open(server, ANA, scope="tenant-a")
    b = await Peer.open(server, ANA, scope="tenant-b")
    await a.call("thread.create", thread_id="t1")
    await b.call("thread.create", thread_id="t1")
    await a.call("thread.join", thread_id="t1")
    await b.call("thread.join", thread_id="t1")
    await say(a, "t1", "MARKER-A")
    assert all("MARKER-A" not in str(p) for p in await b.quiet())
    history = (await b.call("thread.history", thread_id="t1"))["events"]
    assert all("MARKER-A" not in str(e) for e in history)
    assert await server.delete_scope("tenant-a") > 0
    assert await server.verify_absent("tenant-a") and not await server.verify_absent("tenant-b")


async def test_the_server_verbs_act_as_the_system(server: Server) -> None:
    thread = await server.create_thread("acme", thread_id="onboarding-bruno", metadata={"owner": "bruno"})
    member = await server.add_member("acme", thread.id, BRUNO)
    assert member.added_by.role == "system"
    bruno = await Peer.open(server, BRUNO)
    events = (await bruno.call("thread.history", thread_id=thread.id))["events"]
    assert [(e["kind"], e["author"]["role"]) for e in events] == [("member_added", "system")]
    await server.delete_thread("acme", thread.id)
    with pytest.raises(NotFoundError):
        await bruno.call("thread.get", thread_id=thread.id)
```

- [ ] **Step 3: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_server.py tests/unit/test_verbs.py -q`
Expected: FAIL — `No module named 'rfnry_thread.pipe'` (collection error in `peer.py`).

- [ ] **Step 4: Implement the connection protocol and the pipe**

`packages/python/src/rfnry_thread/connection.py`:

```python
from typing import Protocol


class Connection(Protocol):
    async def receive_text(self) -> str: ...

    async def send_text(self, data: str) -> None: ...

    async def close(self, code: int = 1000, reason: str | None = None) -> None: ...
```

`packages/python/src/rfnry_thread/pipe.py`:

```python
from __future__ import annotations

import asyncio

from rfnry_thread.errors import TransportError


class End:
    def __init__(self, inbox: asyncio.Queue[str | None], outbox: asyncio.Queue[str | None]) -> None:
        self._inbox = inbox
        self._outbox = outbox
        self.peer: End | None = None
        self.code: int | None = None
        self.closed = False

    async def receive_text(self) -> str:
        text = await self._inbox.get()
        if text is None:
            self._inbox.put_nowait(None)
            raise TransportError("pipe: the other end closed")
        return text

    async def send_text(self, data: str) -> None:
        if self.closed:
            raise TransportError("pipe: this end is closed")
        self._outbox.put_nowait(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        if self.closed:
            return
        self.closed = True
        self.code = code
        if self.peer is not None:
            self.peer.code = code
            self.peer.closed = True
        self._outbox.put_nowait(None)
        self._inbox.put_nowait(None)


def pipe() -> tuple[End, End]:
    one: asyncio.Queue[str | None] = asyncio.Queue()
    two: asyncio.Queue[str | None] = asyncio.Queue()
    first, second = End(one, two), End(two, one)
    first.peer, second.peer = second, first
    return first, second
```

- [ ] **Step 5: Implement the verbs**

`packages/python/src/rfnry_thread/verbs.py`:

```python
from __future__ import annotations

import json
import uuid
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from rfnry_thread.errors import ForbiddenError, InputError, NotFoundError
from rfnry_thread.hub import Hub, Session
from rfnry_thread.models import (
    CLIENT_EVENTS,
    EVENTS,
    Custom,
    Edit,
    Event,
    Identity,
    Joined,
    Member,
    Message,
    Retract,
    Step,
    Text,
    Thread,
)
from rfnry_thread.storage import Storage

Authorize = Callable[[Identity, str, Thread], Awaitable[bool]]

CUSTOM_LIMIT = 64 * 1024
LABEL_LIMIT = 1000
PAGE_LIMIT = 500


def now() -> datetime:
    return datetime.now(UTC)


def _limit(limit: int) -> int:
    if not 1 <= limit <= PAGE_LIMIT:
        raise InputError(f"server: limit must be between 1 and {PAGE_LIMIT}")
    return limit


class Verbs:
    def __init__(self, storage: Storage, hub: Hub, authorize: Authorize | None, max_inline_bytes: int) -> None:
        self._storage = storage
        self._hub = hub
        self._authorize = authorize
        self._max_inline = max_inline_bytes

    async def _allow(self, actor: Identity, action: str, thread: Thread) -> None:
        if actor.role == "system" or self._authorize is None:
            return
        if not await self._authorize(actor, action, thread):
            raise ForbiddenError(f"server: {actor.id!r} may not {action} in thread {thread.id!r}")

    async def thread(self, actor: Identity, scope: str, thread_id: str, action: str) -> Thread:
        thread = await self._storage.get_thread(scope, thread_id)
        if thread is None:
            raise NotFoundError(f"server: thread {thread_id!r} was not found")
        if actor.role != "system" and not await self._storage.is_member(scope, thread_id, actor.id):
            raise ForbiddenError(f"server: {actor.id!r} is not a member of thread {thread_id!r}")
        await self._allow(actor, action, thread)
        return thread

    async def _append(self, scope: str, fields: dict[str, Any]) -> Event:
        event = EVENTS.validate_python({"id": uuid.uuid4().hex, "created_at": now(), **fields})
        stored, created = await self._storage.append_event(scope, event)
        if created:
            await self._hub.deliver(scope, stored)
        return stored

    async def create_thread(self, actor: Identity, scope: str, thread_id: str | None, metadata: dict[str, Any]) -> Thread:
        if thread_id is not None and not thread_id.strip():
            raise InputError("server: a thread id cannot be empty")
        if thread_id is not None and await self._storage.get_thread(scope, thread_id) is not None:
            return await self.thread(actor, scope, thread_id, "read")
        candidate = Thread(id=thread_id or uuid.uuid4().hex, scope=scope, metadata=metadata, created_at=now())
        await self._allow(actor, "create", candidate)
        thread, created = await self._storage.create_thread(scope, candidate)
        if not created:
            return await self.thread(actor, scope, thread.id, "read")
        if actor.role != "system":
            await self._admit(scope, thread, actor, actor)
        return thread

    async def _admit(self, scope: str, thread: Thread, identity: Identity, by: Identity) -> Member:
        member, created = await self._storage.add_member(
            scope, Member(thread_id=thread.id, identity=identity, added_at=now(), added_by=by)
        )
        if created:
            await self._append(scope, {"kind": "member_added", "thread_id": thread.id, "author": by,
                                       "client_id": uuid.uuid4().hex, "identity": identity})
            if identity.id != by.id:
                await self._hub.invite(scope, thread, by, identity)
        return member

    async def get_thread(self, actor: Identity, scope: str, thread_id: str) -> Thread:
        return await self.thread(actor, scope, thread_id, "read")

    async def list_threads(self, actor: Identity, scope: str, before: str | None, limit: int) -> list[Thread]:
        return await self._storage.list_threads(scope, actor.id, before, _limit(limit))

    async def update_thread(self, actor: Identity, scope: str, thread_id: str, metadata: dict[str, Any]) -> Thread:
        await self.thread(actor, scope, thread_id, "manage")
        updated = await self._storage.update_thread(scope, thread_id, metadata)
        if updated is None:
            raise NotFoundError(f"server: thread {thread_id!r} was not found")
        await self._append(scope, {"kind": "thread_updated", "thread_id": thread_id, "author": actor,
                                   "client_id": uuid.uuid4().hex, "metadata": metadata})
        return updated

    async def delete_thread(self, actor: Identity, scope: str, thread_id: str) -> None:
        await self.thread(actor, scope, thread_id, "delete")
        await self._storage.delete_thread(scope, thread_id)
        await self._hub.deleted(scope, thread_id, actor)

    async def add_member(self, actor: Identity, scope: str, thread_id: str, identity: Identity) -> Member:
        if identity.role == "system":
            raise InputError("server: the system identity cannot be a member")
        thread = await self.thread(actor, scope, thread_id, "manage")
        return await self._admit(scope, thread, identity, actor)

    async def remove_member(self, actor: Identity, scope: str, thread_id: str, identity_id: str) -> None:
        await self.thread(actor, scope, thread_id, "manage")
        member = next((m for m in await self._storage.list_members(scope, thread_id) if m.identity.id == identity_id), None)
        if member is None:
            raise NotFoundError(f"server: {identity_id!r} is not a member of thread {thread_id!r}")
        await self._storage.remove_member(scope, thread_id, identity_id)
        await self._append(scope, {"kind": "member_removed", "thread_id": thread_id, "author": actor,
                                   "client_id": uuid.uuid4().hex, "identity": member.identity})

    async def list_members(self, actor: Identity, scope: str, thread_id: str) -> list[Member]:
        await self.thread(actor, scope, thread_id, "read")
        return await self._storage.list_members(scope, thread_id)

    async def history(
        self, actor: Identity, scope: str, thread_id: str, before: int | None, limit: int, kinds: Sequence[str] | None
    ) -> list[Event]:
        await self.thread(actor, scope, thread_id, "read")
        return await self._storage.list_events(scope, thread_id, before=before, limit=_limit(limit), kinds=kinds)

    async def join(self, session: Session, thread_id: str, since: int | None) -> Joined:
        if since is not None and since < 0:
            raise InputError("server: since cannot be negative")
        thread = await self.thread(session.identity, session.scope, thread_id, "read")
        return await self._hub.join(session, thread, since)

    async def leave(self, session: Session, thread_id: str) -> None:
        await self._hub.leave(session, thread_id)

    async def send(self, actor: Identity, scope: str, payload: dict[str, Any]) -> Event:
        kind = payload.get("kind")
        if kind not in CLIENT_EVENTS:
            raise InputError(f"server: unknown event kind {kind!r}")
        thread_id = payload.get("thread_id")
        if not isinstance(thread_id, str):
            raise InputError("server: event.send needs a thread_id")
        if not payload.get("client_id"):
            raise InputError("server: event.send needs a client_id")
        await self.thread(actor, scope, thread_id, "send")
        fields = {k: v for k, v in payload.items() if k not in ("id", "seq", "author", "created_at")}
        try:
            event = EVENTS.validate_python({**fields, "id": uuid.uuid4().hex, "created_at": now(), "author": actor})
        except ValidationError as exc:
            raise InputError(f"server: invalid {kind} event: {exc.errors(include_url=False)[0]['msg'].lower()}") from exc
        await self._check(scope, actor, event)
        stored, created = await self._storage.append_event(scope, event)
        if created:
            await self._hub.deliver(scope, stored)
        return stored

    async def _check(self, scope: str, actor: Identity, event: Event) -> None:
        if isinstance(event, Message | Edit | Step):
            content = event.output if isinstance(event, Step) else event.content
            size = sum(len(part.data) for part in content if not isinstance(part, Text) and part.data is not None)
            if size > self._max_inline:
                raise InputError(f"server: inline media is {size} bytes; the cap is {self._max_inline} — upload it and send a url")
        if isinstance(event, Custom) and len(json.dumps(event.payload).encode()) > CUSTOM_LIMIT:
            raise InputError(f"server: a custom payload is at most {CUSTOM_LIMIT} bytes")
        if isinstance(event, Edit | Retract):
            verb = "edit" if isinstance(event, Edit) else "retract"
            target = await self._storage.get_event(scope, event.thread_id, event.target)
            if target is None:
                raise NotFoundError(f"server: event {event.target!r} was not found")
            if target.author.id != actor.id:
                raise ForbiddenError(f"server: only its author can {verb} event {event.target!r}")
            if isinstance(event, Edit) and not isinstance(target, Message):
                raise InputError(f"server: only a message can be edited; {event.target!r} is a {target.kind}")
            if isinstance(event, Retract) and not isinstance(target, Message | Step):
                raise InputError(f"server: only a message or a step can be retracted; {event.target!r} is a {target.kind}")
```

Why the inline test expects 6 bytes: `"AAAAAAAA"` is base64 for six zero bytes.

- [ ] **Step 6: Implement the server**

`packages/python/src/rfnry_thread/server.py`:

```python
from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError

from rfnry_thread.connection import Connection
from rfnry_thread.errors import ConfigurationError, InputError, ThreadException
from rfnry_thread.hub import Hub, Session
from rfnry_thread.models import SYSTEM, Identity, Member, Thread
from rfnry_thread.outbound import Outbound
from rfnry_thread.pipe import End, pipe
from rfnry_thread.protocol import (
    CLOSE_PROTOCOL,
    CLOSE_SLOW,
    HELLO,
    PROTOCOL,
    Hello,
    Notify,
    Request,
    Response,
    decode,
    encode,
    failure,
)
from rfnry_thread.storage import Stop, Storage
from rfnry_thread.verbs import Authorize, Verbs

log = logging.getLogger("rfnry_thread")

IN_FLIGHT = 64


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _Create(_Args):
    thread_id: str | None = None
    metadata: dict[str, Any] = {}


class _List(_Args):
    before: str | None = None
    limit: int = 50


class _Ref(_Args):
    thread_id: str


class _Update(_Ref):
    metadata: dict[str, Any]


class _Join(_Ref):
    since: int | None = None


class _History(_Ref):
    before: int | None = None
    limit: int = 50
    kinds: list[str] | None = None


class _Add(_Ref):
    identity: Identity


class _Remove(_Ref):
    identity_id: str


def _args[T: _Args](model: type[T], method: str, payload: dict[str, Any]) -> T:
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        raise InputError(f"server: invalid {method} payload: {exc.errors(include_url=False)[0]['msg'].lower()}") from exc


def _dump(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json")


def _scope(scope: str) -> str:
    if not isinstance(scope, str) or not scope.strip():
        raise InputError("server: a scope is required")
    return scope


def _identity(identity: Identity) -> Identity:
    if identity.role == "system":
        raise ConfigurationError("server: 'system' is the library's role; connect as a user or an assistant")
    return identity


async def _close(connection: Connection, code: int, reason: str) -> None:
    with contextlib.suppress(Exception):
        await connection.close(code, reason)


async def _refuse(connection: Connection, error: ThreadException) -> None:
    with contextlib.suppress(Exception):
        await connection.send_text(encode(failure(HELLO, error)))
    await _close(connection, CLOSE_PROTOCOL, str(error))


class Server:
    def __init__(
        self,
        storage: Storage,
        *,
        authorize: Authorize | None = None,
        max_inline_bytes: int = 256 * 1024,
        replay_limit: int = 500,
    ) -> None:
        if max_inline_bytes < 1:
            raise ConfigurationError("server: max_inline_bytes must be positive")
        if replay_limit < 1:
            raise ConfigurationError("server: replay_limit must be positive")
        self._storage = storage
        self._hub = Hub(storage, replay_limit)
        self._verbs = Verbs(storage, self._hub, authorize, max_inline_bytes)
        self._tasks: set[asyncio.Task[None]] = set()
        self._stop: Stop | None = None
        self._refresher: asyncio.Task[None] | None = None

    async def serve(self, connection: Connection, identity: Identity, scope: str) -> None:
        _identity(identity)
        _scope(scope)
        try:
            text = await connection.receive_text()
        except Exception:
            return
        try:
            hello = decode(text)
            if not isinstance(hello, Hello) or hello.protocol != PROTOCOL:
                raise InputError(f"server: the first frame must be a hello for protocol {PROTOCOL}")
        except InputError as error:
            await _refuse(connection, error)
            return
        await self._run(connection, identity, scope)

    def _attach(self, identity: Identity, scope: str) -> End:
        _identity(identity)
        _scope(scope)
        ours, theirs = pipe()
        task = asyncio.get_running_loop().create_task(self.serve(theirs, identity, scope))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return ours

    async def _run(self, connection: Connection, identity: Identity, scope: str) -> None:
        async def too_slow() -> None:
            await _close(connection, CLOSE_SLOW, "server: connection too slow")

        session = Session(identity=identity, scope=scope, outbound=Outbound(connection, too_slow))
        self._hub.sessions.add(session)
        session.outbound.put(Response(id=HELLO, payload={"identity": _dump(identity), "scope": scope}))
        tasks: set[asyncio.Task[None]] = set()
        try:
            while True:
                try:
                    text = await connection.receive_text()
                except Exception:
                    break
                try:
                    frame = decode(text)
                except InputError:
                    await session.outbound.drain()
                    await _close(connection, CLOSE_PROTOCOL, "server: malformed frame")
                    break
                if isinstance(frame, Request):
                    if len(tasks) >= IN_FLIGHT:
                        await self._answer(session, frame)
                        continue
                    task = asyncio.get_running_loop().create_task(self._answer(session, frame))
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
                elif isinstance(frame, Notify):
                    continue
                else:
                    await _close(connection, CLOSE_PROTOCOL, f"server: clients may not send {frame.type} frames")
                    break
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._hub.drop(session)
            await session.outbound.drain()
            await session.outbound.close()

    async def _answer(self, session: Session, frame: Request) -> None:
        try:
            payload = await self._call(session, frame.method, frame.payload)
        except ThreadException as error:
            session.outbound.put(failure(frame.id, error))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("server: %s failed: %s", frame.method, exc)
            session.outbound.put(failure(frame.id, ThreadException("server: request failed")))
        else:
            session.outbound.put(Response(id=frame.id, payload=payload))

    async def _call(self, session: Session, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        actor, scope, verbs = session.identity, session.scope, self._verbs
        if method == "thread.create":
            create = _args(_Create, method, payload)
            return {"thread": _dump(await verbs.create_thread(actor, scope, create.thread_id, create.metadata))}
        if method == "thread.get":
            return {"thread": _dump(await verbs.get_thread(actor, scope, _args(_Ref, method, payload).thread_id))}
        if method == "thread.list":
            listing = _args(_List, method, payload)
            return {"threads": [_dump(t) for t in await verbs.list_threads(actor, scope, listing.before, listing.limit)]}
        if method == "thread.update":
            update = _args(_Update, method, payload)
            return {"thread": _dump(await verbs.update_thread(actor, scope, update.thread_id, update.metadata))}
        if method == "thread.delete":
            await verbs.delete_thread(actor, scope, _args(_Ref, method, payload).thread_id)
            return {}
        if method == "thread.join":
            join = _args(_Join, method, payload)
            return _dump(await verbs.join(session, join.thread_id, join.since))
        if method == "thread.leave":
            await verbs.leave(session, _args(_Ref, method, payload).thread_id)
            return {}
        if method == "thread.history":
            history = _args(_History, method, payload)
            events = await verbs.history(actor, scope, history.thread_id, history.before, history.limit, history.kinds)
            return {"events": [_dump(e) for e in events]}
        if method == "member.add":
            add = _args(_Add, method, payload)
            return {"member": _dump(await verbs.add_member(actor, scope, add.thread_id, add.identity))}
        if method == "member.remove":
            remove = _args(_Remove, method, payload)
            await verbs.remove_member(actor, scope, remove.thread_id, remove.identity_id)
            return {}
        if method == "member.list":
            members = await verbs.list_members(actor, scope, _args(_Ref, method, payload).thread_id)
            return {"members": [_dump(m) for m in members]}
        if method == "event.send":
            return {"event": _dump(await verbs.send(actor, scope, payload))}
        raise InputError(f"server: unknown method {method!r}")

    async def create_thread(self, scope: str, thread_id: str | None = None, metadata: dict[str, Any] | None = None) -> Thread:
        return await self._verbs.create_thread(SYSTEM, _scope(scope), thread_id, metadata or {})

    async def add_member(self, scope: str, thread_id: str, identity: Identity) -> Member:
        return await self._verbs.add_member(SYSTEM, _scope(scope), thread_id, identity)

    async def delete_thread(self, scope: str, thread_id: str) -> None:
        await self._verbs.delete_thread(SYSTEM, _scope(scope), thread_id)

    async def delete_scope(self, scope: str) -> int:
        return await self._storage.delete_scope(_scope(scope))

    async def verify_absent(self, scope: str) -> bool:
        return await self._storage.verify_absent(_scope(scope))
```

Notes for the implementer:
- `_args` uses PEP 695 generics (Python 3.12). If mypy on your version rejects it, write `T = TypeVar("T", bound=_Args)` at module level instead.
- `serve` never authenticates: the consumer's framework did, at the upgrade, and names the identity and scope. `serve` validates them before reading anything and raises (the caller's bug), then waits for the hello without a deadline — the socket is already someone the consumer accepted.
- `test_authorize_decides_each_of_the_five_actions` expects exactly `create` (thread.create), `manage` (member.add), `read` (thread.get), `send` (event.send), `delete` (thread.delete): `_admit` during create appends through `_append`, which does not authorize.

- [ ] **Step 7: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add packages/python
git commit -m "feat: the server core — hello, membership on every verb, authorize, stamped authorship, the thread and member verbs

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 10: Signals and presence over the server

Written fresh (replaces the old stream frames, typing and run events).

**Files:**
- Modify: `packages/python/src/rfnry_thread/verbs.py` (add `signal`, `present`)
- Modify: `packages/python/src/rfnry_thread/server.py` (`_run` loop, `_call`)
- Test: `packages/python/tests/unit/test_signals.py`

**Interfaces:**
- Consumes: `Hub.signal`, `Hub.state` (Task 8); `Verbs.thread`, `Verbs._allow` (Task 9); `SIGNALS`, `CLIENT_SIGNALS` (Task 3).
- Produces: `Verbs.signal(session: Session, payload: dict[str, Any]) -> None`; `Verbs.present(actor, scope, thread_id) -> list[Identity]`; protocol methods `signal.send` (request: answered `{}`; notify: errors dropped) and `thread.present` (answered `{"identities": [...]}`). `signal.send` requests are handled inline in the frame loop, so signals from one connection keep their order.

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_signals.py`:

```python
import pytest

from rfnry_thread.errors import ForbiddenError, InputError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import Identity, Thread
from rfnry_thread.server import Server

from .factories import ANA, BRUNO, HELPER
from .peer import Peer


async def joined(server: Server, *identities: Identity) -> list[Peer]:
    owner = await Peer.open(server, ANA)
    await owner.call("thread.create", thread_id="t1")
    for identity in identities:
        if identity != ANA:
            await owner.call("member.add", thread_id="t1", identity=identity.model_dump(mode="json"))
    peers = []
    for identity in identities:
        peer = owner if identity == ANA else await Peer.open(server, identity)
        await peer.call("thread.join", thread_id="t1")
        await peer.quiet()
        peers.append(peer)
    for peer in peers:
        await peer.quiet()
    return peers


async def test_a_signal_needs_a_join(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    with pytest.raises(ForbiddenError, match="^server: signals need a join; 'ana' has not joined thread 't1'$"):
        await ana.call("signal.send", thread_id="t1", kind="working")


async def test_working_reaches_the_room_with_the_sender_stamped(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading rules/refunds.md",
                      sender=BRUNO.model_dump(mode="json"))
    working = await ana.push("working")
    assert working["sender"]["id"] == "helper" and working["label"] == "Reading rules/refunds.md"
    await helper.call("signal.send", thread_id="t1", kind="idle")
    assert (await ana.push("idle"))["sender"]["id"] == "helper"


async def test_signals_are_checked(server: Server) -> None:
    _, helper = await joined(server, ANA, HELPER)
    with pytest.raises(InputError, match="^server: a working label is at most 1000 characters$"):
        await helper.call("signal.send", thread_id="t1", kind="working", label="x" * 1001)
    with pytest.raises(InputError, match="^server: unknown signal kind 'present'$"):
        await helper.call("signal.send", thread_id="t1", kind="present")
    with pytest.raises(InputError, match="^server: stream 's1' was not started$"):
        await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    with pytest.raises(InputError, match="^server: stream 's1' was already started$"):
        await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")


async def test_a_stream_flows_start_deltas_end_and_bad_notifies_are_dropped(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.notify("signal.send", thread_id="t1", kind="stream.delta", stream_id="ghost", index=0, text="lost")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1", in_reply_to="e1")
    for index, text in enumerate(["Refunded ", "in full."]):
        await helper.notify("signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=index, text=text)
    await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
    received = [p for p in await ana.quiet() if p["kind"].startswith("stream.")]
    assert received[0]["kind"] == "stream.start" and received[0]["in_reply_to"] == "e1"
    assert "".join(p["text"] for p in received if p["kind"] == "stream.delta") == "Refunded in full."
    assert received[-1]["kind"] == "stream.end"
    assert all(p.get("stream_id") != "ghost" for p in received)


async def test_a_late_joiner_sees_the_half_written_reply_first(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await helper.call("signal.send", thread_id="t1", kind="working", label="Writing")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await helper.notify("signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="Half")
    await helper.call("thread.present", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    arrived = [(p["kind"], p.get("text")) for p in ana.pushes]
    assert arrived == [("present", None), ("working", None), ("stream.start", None), ("stream.delta", "Half"), ("present", None)]


async def test_present_lists_who_has_the_thread_open(server: Server) -> None:
    ana, _ = await joined(server, ANA, HELPER)
    identities = (await ana.call("thread.present", thread_id="t1"))["identities"]
    assert sorted(i["id"] for i in identities) == ["ana", "helper"]


async def test_two_tabs_are_one_presence_over_the_server(server: Server) -> None:
    helper, tab_one = await joined(server, HELPER, ANA)
    tab_two = await Peer.open(server, ANA)
    await tab_two.call("thread.join", thread_id="t1")
    assert [p["kind"] for p in await helper.quiet()] == []
    await tab_one.close()
    assert [p["kind"] for p in await helper.quiet()] == []
    await tab_two.close()
    assert [p["kind"] for p in await helper.quiet()] == ["left"]


async def test_a_dropped_connection_ends_its_stream_and_work(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await helper.call("signal.send", thread_id="t1", kind="working")
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await ana.quiet()
    await helper.close()
    assert [p["kind"] for p in await ana.quiet()] == ["stream.end", "idle", "left"]


async def test_cancel_names_who_should_stop(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await ana.call("signal.send", thread_id="t1", kind="cancel", to=HELPER.model_dump(mode="json"))
    cancel = await helper.push("cancel")
    assert cancel["to"]["id"] == "helper" and cancel["sender"]["id"] == "ana"


async def test_an_added_member_connected_elsewhere_is_invited(server: Server) -> None:
    ana = await Peer.open(server, ANA)
    bruno = await Peer.open(server, BRUNO)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    invited = await bruno.push("invited")
    assert invited["thread"]["id"] == "t1" and invited["sender"]["id"] == "ana"


async def test_deletion_tells_the_room_and_ends_its_joins(server: Server) -> None:
    ana, helper = await joined(server, ANA, HELPER)
    await ana.call("thread.delete", thread_id="t1")
    assert (await helper.push("thread.deleted"))["sender"]["id"] == "ana"
    with pytest.raises(ForbiddenError):
        await helper.call("signal.send", thread_id="t1", kind="working")


async def test_authorize_send_applies_to_signals_that_speak(storage: InMemoryStorage) -> None:
    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        return not (identity.id == "helper" and action == "send")

    server = Server(storage, authorize=authorize)
    _, helper = await joined(server, ANA, HELPER)
    with pytest.raises(ForbiddenError, match="^server: 'helper' may not send in thread 't1'$"):
        await helper.call("signal.send", thread_id="t1", kind="working")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_signals.py -q`
Expected: FAIL — `InputError: server: unknown method 'signal.send'`.

- [ ] **Step 3: Implement**

In `verbs.py`, extend the models import with `CLIENT_SIGNALS`, `SIGNALS`, `Cancel`, `StreamDelta`, `StreamEnd`, `StreamStart`, `Working`, then add to `Verbs`:

```python
    async def signal(self, session: Session, payload: dict[str, Any]) -> None:
        kind = payload.get("kind")
        if kind not in CLIENT_SIGNALS:
            raise InputError(f"server: unknown signal kind {kind!r}")
        thread_id = payload.get("thread_id")
        if not isinstance(thread_id, str) or thread_id not in session.joined:
            raise ForbiddenError(f"server: signals need a join; {session.identity.id!r} has not joined thread {thread_id!r}")
        try:
            signal = SIGNALS.validate_python({**payload, "sender": session.identity})
        except ValidationError as exc:
            raise InputError(f"server: invalid {kind} signal: {exc.errors(include_url=False)[0]['msg'].lower()}") from exc
        if isinstance(signal, Working) and signal.label is not None and len(signal.label) > LABEL_LIMIT:
            raise InputError(f"server: a working label is at most {LABEL_LIMIT} characters")
        if isinstance(signal, StreamStart) and (thread_id, signal.stream_id) in session.streams:
            raise InputError(f"server: stream {signal.stream_id!r} was already started")
        if isinstance(signal, StreamDelta | StreamEnd) and (thread_id, signal.stream_id) not in session.streams:
            raise InputError(f"server: stream {signal.stream_id!r} was not started")
        if self._authorize is not None and isinstance(signal, Working | StreamStart | Cancel):
            await self.thread(session.identity, session.scope, thread_id, "send")
        await self._hub.signal(session, signal)

    async def present(self, actor: Identity, scope: str, thread_id: str) -> list[Identity]:
        await self.thread(actor, scope, thread_id, "read")
        return self._hub.state.present(scope, thread_id)
```

In `server.py`, replace the request and notify branches of the `_run` loop:

```python
                if isinstance(frame, Request):
                    if frame.method == "signal.send" or len(tasks) >= IN_FLIGHT:
                        await self._answer(session, frame)
                        continue
                    task = asyncio.get_running_loop().create_task(self._answer(session, frame))
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
                elif isinstance(frame, Notify):
                    if frame.method == "signal.send":
                        with contextlib.suppress(ThreadException):
                            await self._verbs.signal(session, frame.payload)
```

and add to `_call`, before the final `raise`:

```python
        if method == "thread.present":
            present = await verbs.present(actor, scope, _args(_Ref, method, payload).thread_id)
            return {"identities": [_dump(identity) for identity in present]}
        if method == "signal.send":
            await verbs.signal(session, payload)
            return {}
```

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: signals and presence over the server — joined senders, ordered streams, the join snapshot, invitations

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 11: Many replicas — start, stop, and two servers on one storage

Written fresh (replaces `../rfnry-thread/packages/server-python/src/rfnry_thread_server/broadcast/distributed.py` and the consumer-backed pub/sub protocol).

**Files:**
- Modify: `packages/python/src/rfnry_thread/server.py` (add `start`, `stop`, `_refresh`)
- Test: `packages/python/tests/unit/test_replicas.py`

**Interfaces:**
- Consumes: `Hub.on_event`, `Hub.on_signal`, `Hub.announce`, `Hub.linked` (Task 8); `Storage.listen`, `OnEvent`, `OnSignal`, `OnResume`, `Stop` (Task 5); `REFRESH` (Task 7).
- Produces: `Server.start() -> None` (idempotent: listens on the storage with `Hub.announce` as `on_resume`, sets `linked`, starts re-announcing every `REFRESH` seconds) and `Server.stop() -> None` (idempotent: stops both).

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_replicas.py`:

```python
from collections.abc import AsyncIterator

import pytest

from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.server import Server
from rfnry_thread.state import EXPIRY
from rfnry_thread.storage import OnEvent, OnResume, OnSignal, Stop
from until import until

from .conftest import say
from .factories import ANA, BRUNO, HELPER
from .peer import Peer


@pytest.fixture
async def replicas(storage: InMemoryStorage) -> AsyncIterator[tuple[Server, Server]]:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    yield near, far
    await near.stop()
    await far.stop()


async def room(near: Server, far: Server) -> tuple[Peer, Peer]:
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await ana.call("thread.join", thread_id="t1")
    helper = await Peer.open(far, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await ana.quiet()
    await helper.quiet()
    return ana, helper


async def test_an_event_on_one_replica_reaches_a_member_on_another_once(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    for n in range(3):
        await say(ana, "t1", f"m{n}")
    messages = [p for p in await helper.quiet() if p["kind"] == "message"]
    assert [p["content"][0]["text"] for p in messages] == ["m0", "m1", "m2"]
    assert [p["kind"] for p in await ana.quiet()].count("message") == 3


async def test_signals_are_mirrored_and_never_echoed_twice(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working"))["label"] == "Reading"
    assert [p["kind"] for p in await helper.quiet()] == ["working"]
    assert [p["kind"] for p in await ana.quiet()] == []


async def test_the_finishing_message_reaches_a_remote_connection_before_stream_end(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await helper.notify("signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="Done.")
    await say(helper, "t1", "Done.", client_id="s1")
    await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
    order = [p["kind"] for p in await ana.quiet() if p["kind"] in ("message", "stream.end")]
    assert order == ["message", "stream.end"]


async def test_removal_cuts_off_a_member_on_another_replica(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    await say(ana, "t1", "after")
    kinds = [p["kind"] for p in await helper.quiet()]
    assert "member_removed" in kinds and "message" not in kinds


async def test_an_invitation_and_a_deletion_cross_replicas(replicas: tuple[Server, Server]) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    bruno = await Peer.open(far, BRUNO)
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    assert (await bruno.push("invited"))["thread"]["id"] == "t1"
    await ana.call("thread.delete", thread_id="t1")
    assert (await helper.push("thread.deleted"))["sender"]["id"] == "ana"


async def test_a_replica_learns_who_is_here_from_the_re_announcement(storage: InMemoryStorage) -> None:
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    far = Server(storage)
    await near.start()
    await far.start()
    await near._hub.announce()
    await until(lambda: far._hub.state.present("acme", "t1") == [ANA])
    now = far._hub.state._clock()
    far._hub.state._clock = lambda: now + EXPIRY + 1
    assert far._hub.state.present("acme", "t1") == []
    await near.stop()
    await far.stop()


async def test_a_resumed_listener_republishes_this_replicas_state(storage: InMemoryStorage) -> None:
    resumes: list[OnResume | None] = []
    listen = storage.listen

    async def spy(on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None) -> Stop:
        resumes.append(on_resume)
        return await listen(on_event, on_signal, on_resume)

    storage.listen = spy  # type: ignore[method-assign]
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    far = Server(storage)
    await near.start()
    await far.start()
    assert far._hub.state.present("acme", "t1") == []
    resume = resumes[0]
    assert resume is not None
    await resume()
    await until(lambda: far._hub.state.present("acme", "t1") == [ANA])
    await near.stop()
    await far.stop()


async def test_stop_stops_listening_and_start_is_idempotent(storage: InMemoryStorage) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await near.start()
    await far.start()
    ana, helper = await room(near, far)
    await far.stop()
    await far.stop()
    await say(ana, "t1", "unheard")
    assert [p for p in await helper.quiet() if p["kind"] == "message"] == []
    await near.stop()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_replicas.py -q`
Expected: FAIL — `AttributeError: 'Server' object has no attribute 'start'`.

- [ ] **Step 3: Implement**

In `server.py`, import `REFRESH` from `rfnry_thread.state` and add to `Server`:

```python
    async def start(self) -> None:
        if self._stop is not None:
            return
        self._stop = await self._storage.listen(self._hub.on_event, self._hub.on_signal, self._hub.announce)
        self._hub.linked = True
        self._refresher = asyncio.get_running_loop().create_task(self._refresh())

    async def stop(self) -> None:
        if self._refresher is not None:
            self._refresher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._refresher
            self._refresher = None
        if self._stop is not None:
            await self._stop()
            self._stop = None
        self._hub.linked = False

    async def _refresh(self) -> None:
        while True:
            await asyncio.sleep(REFRESH)
            await self._hub.announce()
```

Why `announce` is `on_resume`: a replica whose `LISTEN` dropped (Task 15 re-listens) may have been silent past the peers' 30 s expiry; republishing at once rebuilds its presence, work and streams on the others instead of waiting for the next 10 s tick.

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: many replicas — start listens and re-announces, stop lets go; two servers proven on one storage

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 12: The Python client — one class, in-process first

Written fresh around ideas proven in the old client: request ids with futures and a background reader (`../rfnry-thread/packages/client-python/src/rfnry_thread_client/transport/rpc.py`), a per-thread cursor dropping repeats (`resume.py`, `_track_cursor`). Everything else of the old client — handler registry, dispatcher lanes, run lifecycle, receipts, mentions, transcript, members cache, REST — is stripped.

This task builds the whole `Client` (verbs, `events()`, `Stream`, the outbox and reconnect code paths) and `server.connect`, tested in-process. Task 13 adds the WebSocket transport and exercises reconnect, resume and the outbox over a real socket.

**Files:**
- Create: `packages/python/src/rfnry_thread/client.py`
- Modify: `packages/python/src/rfnry_thread/server.py` (add `connect`)
- Test: `packages/python/tests/unit/test_client.py`

**Interfaces:**
- Consumes: `Server._attach` (Task 9); frames, `encode`, `decode`, `HELLO`, `PROTOCOL` (Task 4); models, `parts`, `EVENTS`, `SIGNALS` (Task 3); `Connection` (Task 9); errors and `raised` (Task 1).
- Produces (in `rfnry_thread.client`):
  - `Dial = Callable[[], Awaitable[Connection]]`; `Item = Event | Signal | Joined`
  - constants `HANDSHAKE = 10.0`, `DEADLINE = 60.0`, `BACKOFF = 0.5`, `BACKOFF_CAP = 8.0`, `RECONNECT = 0.5`, `RECONNECT_CAP = 30.0`
  - `Client` with `identity: Identity`, `scope: str` (as the server's hello response says); `classmethod async _open(dial: Dial) -> Client` (every connection attempt calls `dial` and then sends `Hello(protocol=1)`); verbs `create_thread(thread_id=None, metadata=None) -> Thread`, `get_thread(thread_id) -> Thread`, `list_threads(before=None, limit=50) -> list[Thread]`, `update_thread(thread_id, metadata) -> Thread`, `delete_thread(thread_id) -> None`, `join(thread_id, since=None) -> Joined`, `leave(thread_id) -> None`, `history(thread_id, before=None, limit=50, kinds=None) -> list[Event]`, `present(thread_id) -> list[Identity]`, `add_member(thread_id, identity) -> Member`, `remove_member(thread_id, identity) -> None`, `list_members(thread_id) -> list[Member]`, `send(thread_id, content, in_reply_to=None, triggered_by=None, client_id=None) -> Message`, `stream(thread_id, in_reply_to=None, triggered_by=None) -> Stream`, `step(thread_id, name, input=None, output=None, is_error=False, duration_ms=None, in_reply_to=None, triggered_by=None, client_id=None) -> Step`, `edit(thread_id, target, content, client_id=None) -> Edit`, `retract(thread_id, target, client_id=None) -> Retract`, `read(thread_id, up_to_seq) -> Read`, `custom(thread_id, name, payload, client_id=None) -> Custom` (a `client_id` left out is generated; one supplied is the server's dedup key, so the same one twice stores one event and returns it both times), `working(thread_id, label=None) -> None`, `idle(thread_id) -> None`, `cancel(thread_id, to) -> None`, `events(include_own=False) -> AsyncIterator[Item]`, `close() -> None`
  - `Stream` with `id: str`, `text: str`, `async write(text) -> None`, `async finish(content=None) -> Message`, async context manager
- Produces: `Server.connect(identity: Identity, scope: str) -> Client`.

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_client.py`:

```python
import asyncio
from typing import TypeVar

import pytest

from rfnry_thread.client import Client, Item
from rfnry_thread.errors import ConfigurationError, ForbiddenError, TransportError
from rfnry_thread.models import (
    SYSTEM,
    Cancel,
    Custom,
    Edit,
    Image,
    Invited,
    Message,
    Present,
    Read,
    Retract,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Working,
)
from rfnry_thread.resolve import resolve
from rfnry_thread.server import Server

from .factories import ANA, BRUNO, HELPER

T = TypeVar("T")


async def take(client: Client, kind: type[T], include_own: bool = False, timeout: float = 1.0) -> T:
    async def find() -> T:
        async for item in client.events(include_own=include_own):
            if isinstance(item, kind):
                return item
        raise AssertionError("the stream ended")

    return await asyncio.wait_for(find(), timeout)


async def drain(client: Client, wait: float = 0.15, include_own: bool = True) -> list[Item]:
    items: list[Item] = []

    async def collect() -> None:
        async for item in client.events(include_own=include_own):
            items.append(item)

    try:
        await asyncio.wait_for(collect(), wait)
    except TimeoutError:
        pass
    return items


async def pair(server: Server) -> tuple[Client, Client]:
    ana = await server.connect(ANA, scope="acme")
    helper = await server.connect(HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.join("t1")
    await helper.join("t1")
    await drain(ana)
    await drain(helper)
    return ana, helper


async def test_an_in_process_client_knows_who_and_where_it_is(server: Server) -> None:
    client = await server.connect(ANA, scope="acme")
    assert (client.identity, client.scope) == (ANA, "acme")
    with pytest.raises(ConfigurationError):
        await server.connect(SYSTEM, scope="acme")
    await client.close()


async def test_getting_started(server: Server) -> None:
    ana = await server.connect(ANA, scope="acme")
    helper = await server.connect(HELPER, scope="acme")
    thread = await ana.create_thread(thread_id="support-ana")
    await ana.add_member(thread.id, helper.identity)
    await helper.join(thread.id)
    await ana.send(thread.id, "My invoice was charged twice.")
    item = await take(helper, Message)
    reply = await helper.send(thread.id, "Looking into it.", in_reply_to=item.id)
    assert reply.in_reply_to == item.id and reply.author == HELPER
    assert item.content == [Text(text="My invoice was charged twice.")]


async def test_threads_and_members(server: Server) -> None:
    ana = await server.connect(ANA, scope="acme")
    thread = await ana.create_thread(thread_id="support-ana", metadata={"topic": "billing"})
    assert (await ana.get_thread(thread.id)).metadata == {"topic": "billing"}
    assert [t.id for t in await ana.list_threads(limit=20)] == ["support-ana"]
    assert (await ana.update_thread(thread.id, {"topic": "refunds"})).metadata == {"topic": "refunds"}
    member = await ana.add_member(thread.id, BRUNO)
    assert member.added_by == ANA
    assert [m.identity.id for m in await ana.list_members(thread.id)] == ["ana", "bruno"]
    await ana.remove_member(thread.id, BRUNO)
    assert [m.identity.id for m in await ana.list_members(thread.id)] == ["ana"]
    await ana.delete_thread(thread.id)
    assert await ana.list_threads() == []


async def test_own_items_are_left_out_unless_asked_for(server: Server) -> None:
    ana, helper = await pair(server)
    await ana.send("t1", "mine")
    await helper.send("t1", "theirs")
    received = [i for i in await drain(ana, include_own=False) if isinstance(i, Message)]
    assert [m.content for m in received] == [[Text(text="theirs")]]


async def test_every_writing_verb_returns_the_stored_event(server: Server) -> None:
    ana, helper = await pair(server)
    image = Image(media_type="image/png", url="https://files.example.com/receipt.png")
    message = await ana.send("t1", [Text(text="Here is the receipt."), image])
    assert message.content[1] == image and message.seq > 0
    assert isinstance(await ana.edit("t1", message.id, "Here is the right receipt."), Edit)
    step = await helper.step("t1", name="Read", input={"path": "rules/refunds.md"},
                             output=[Text(text="A double charge is refunded in full.")], duration_ms=12,
                             in_reply_to=message.id, triggered_by=ANA)
    assert isinstance(step, Step) and step.triggered_by == ANA and step.duration_ms == 12
    assert isinstance(await ana.read("t1", up_to_seq=message.seq), Read)
    assert isinstance(await ana.custom("t1", "rating", {"stars": 5}), Custom)
    assert isinstance(await ana.retract("t1", message.id), Retract)
    view = resolve(await ana.history("t1"))
    assert message.id not in [e.id for e in view]


async def test_a_caller_supplied_client_id_stores_one_event_and_returns_it_twice(server: Server) -> None:
    ana, helper = await pair(server)
    first = await helper.send("t1", "Your plan renews Friday.", client_id="renewal-ana-2026-10")
    again = await helper.send("t1", "Your plan renews Friday, again.", client_id="renewal-ana-2026-10")
    assert again == first and first.client_id == "renewal-ana-2026-10"
    step = await helper.step("t1", name="Read", client_id="read-1")
    assert await helper.step("t1", name="Read", client_id="read-1") == step
    edit = await helper.edit("t1", first.id, "Renews Friday.", client_id="edit-1")
    assert await helper.edit("t1", first.id, "Renews Friday.", client_id="edit-1") == edit
    custom = await helper.custom("t1", "rating", {"stars": 5}, client_id="rating-1")
    assert await helper.custom("t1", "rating", {"stars": 4}, client_id="rating-1") == custom
    retract = await helper.retract("t1", step.id, client_id="retract-1")
    assert await helper.retract("t1", step.id, client_id="retract-1") == retract
    mine = [e.client_id for e in await ana.history("t1") if e.author == HELPER]
    assert mine == ["renewal-ana-2026-10", "read-1", "edit-1", "rating-1", "retract-1"]


async def test_a_stream_is_signals_then_a_message_with_the_stream_id(server: Server) -> None:
    ana, helper = await pair(server)
    async with helper.stream("t1", in_reply_to="e1") as reply:
        for chunk in ["The second charge ", "was refunded."]:
            await reply.write(chunk)
    items = [i for i in await drain(ana) if not isinstance(i, Present)]
    assert [type(i) for i in items][0] is StreamStart
    assert "".join(i.text for i in items if isinstance(i, StreamDelta)) == "The second charge was refunded."
    message = next(i for i in items if isinstance(i, Message))
    assert message.client_id == reply.id and message.content == [Text(text="The second charge was refunded.")]
    assert items.index(message) < next(n for n, i in enumerate(items) if isinstance(i, StreamEnd))


async def test_finish_stores_other_content_and_an_exception_abandons(server: Server) -> None:
    ana, helper = await pair(server)
    async with helper.stream("t1") as reply:
        await reply.write("Here it is")
        stored = await reply.finish([Text(text="Here it is"), Image(media_type="image/png", url="https://x/a.png")])
    assert len(stored.content) == 2
    with pytest.raises(RuntimeError):
        async with helper.stream("t1") as abandoned:
            await abandoned.write("never mind")
            raise RuntimeError("boom")
    history = [e for e in await ana.history("t1") if isinstance(e, Message)]
    assert [e.client_id for e in history] == [reply.id]


async def test_signals_arrive_typed(server: Server) -> None:
    ana, helper = await pair(server)
    await helper.working("t1", label="Checking the invoice")
    assert (await take(ana, Working)).label == "Checking the invoice"
    await ana.cancel("t1", to=helper.identity)
    assert (await take(helper, Cancel)).to == HELPER
    assert sorted(i.id for i in await helper.present("t1")) == ["ana", "helper"]


async def test_an_assistant_can_speak_first(server: Server) -> None:
    ana = await server.connect(ANA, scope="acme")
    reminders = await server.connect(HELPER, scope="acme")
    renewal = await reminders.create_thread(thread_id="renewal-ana-2026-10")
    await reminders.add_member(renewal.id, ANA)
    await reminders.send(renewal.id, "Your plan renews Friday.")
    invited = await take(ana, Invited)
    assert invited.thread.id == renewal.id
    joined = await ana.join(invited.thread_id, since=0)
    assert joined.seq == 3
    assert [e.kind for e in await ana.history(renewal.id)] == ["member_added", "member_added", "message"]


async def test_server_errors_are_raised_with_their_class_and_message(server: Server) -> None:
    ana = await server.connect(ANA, scope="acme")
    bruno = await server.connect(BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    with pytest.raises(ForbiddenError, match="^server: 'bruno' is not a member of thread 't1'$"):
        await bruno.send("t1", "hi")
    with pytest.raises(ForbiddenError, match="^server: signals need a join"):
        await ana.working("t1")


async def test_a_closed_client_ends_its_stream_and_refuses_calls(server: Server) -> None:
    ana = await server.connect(ANA, scope="acme")
    await ana.close()
    assert [i async for i in ana.events()] == []
    with pytest.raises(TransportError, match="^client: the client is closed$"):
        await ana.create_thread()

```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_client.py -q`
Expected: FAIL — `No module named 'rfnry_thread.client'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/client.py`:

```python
from __future__ import annotations

import asyncio
import contextlib
import itertools
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from types import TracebackType
from typing import Any, TypeVar

from rfnry_thread.connection import Connection
from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    ThreadException,
    TransportError,
    raised,
)
from rfnry_thread.models import (
    EVENTS,
    SIGNALS,
    Content,
    Custom,
    Edit,
    Event,
    Identity,
    Joined,
    Member,
    MemberRemoved,
    Message,
    Part,
    Read,
    Retract,
    Signal,
    Step,
    Thread,
    ThreadDeleted,
    parts,
)
from rfnry_thread.protocol import (
    HELLO,
    PROTOCOL,
    ErrorFrame,
    Hello,
    Notify,
    Push,
    Request,
    Response,
    decode,
    encode,
)

Dial = Callable[[], Awaitable[Connection]]
Item = Event | Signal | Joined

HANDSHAKE = 10.0
DEADLINE = 60.0
BACKOFF = 0.5
BACKOFF_CAP = 8.0
RECONNECT = 0.5
RECONNECT_CAP = 30.0

E = TypeVar("E")


def _wire(content: Content) -> list[dict[str, Any]]:
    return [part.model_dump(mode="json") for part in parts(content)]


def _who(identity: Identity | None) -> dict[str, Any] | None:
    return None if identity is None else identity.model_dump(mode="json")


def _expect(event: Event, kind: type[E]) -> E:
    if not isinstance(event, kind):
        raise ThreadException(f"client: the server answered with a {event.kind}")
    return event


def _own(item: Item, identity: Identity) -> bool:
    if isinstance(item, Joined):
        return False
    who = getattr(item, "sender", None) or getattr(item, "author", None)
    return isinstance(who, Identity) and who.id == identity.id


class Stream:
    def __init__(self, client: Client, thread_id: str, in_reply_to: str | None, triggered_by: Identity | None) -> None:
        self.id = uuid.uuid4().hex
        self.text = ""
        self._client = client
        self._thread_id = thread_id
        self._in_reply_to = in_reply_to
        self._triggered_by = triggered_by
        self._index = 0
        self._ended = False

    async def __aenter__(self) -> Stream:
        await self._client._request("signal.send", {"thread_id": self._thread_id, "kind": "stream.start",
                                                   "stream_id": self.id, "in_reply_to": self._in_reply_to})
        return self

    async def write(self, text: str) -> None:
        self.text += text
        await self._client._notify("signal.send", {"thread_id": self._thread_id, "kind": "stream.delta",
                                                   "stream_id": self.id, "index": self._index, "text": text})
        self._index += 1

    async def finish(self, content: Content | None = None) -> Message:
        if self._ended:
            raise InputError(f"client: stream {self.id!r} has already ended")
        self._ended = True
        message = await self._client._message(self._thread_id, self.text if content is None else content,
                                               self._in_reply_to, self._triggered_by, self.id)
        await self._end()
        return message

    async def _end(self) -> None:
        with contextlib.suppress(ThreadException):
            await self._client._request("signal.send", {"thread_id": self._thread_id, "kind": "stream.end", "stream_id": self.id})

    async def __aexit__(
        self, kind: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        if self._ended:
            return
        if kind is None:
            await self.finish()
            return
        self._ended = True
        await self._end()


class Client:
    identity: Identity
    scope: str

    def __init__(self, dial: Dial) -> None:
        self._dial = dial
        self._connection: Connection | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._ids = itertools.count(1)
        self._inbox: asyncio.Queue[Item | None] = asyncio.Queue()
        self._cursors: dict[str, int] = {}
        self._outbox: dict[str, asyncio.Lock] = {}
        self._connected = asyncio.Event()
        self._reader: asyncio.Task[None] | None = None
        self._reconnector: asyncio.Task[None] | None = None
        self._closed = False
        self._failure: ThreadException | None = None

    @classmethod
    async def _open(cls, dial: Dial) -> Client:
        client = cls(dial)
        await client._establish()
        return client

    async def _establish(self) -> None:
        try:
            connection = await self._dial()
        except ThreadException:
            raise
        except Exception as exc:
            raise TransportError(f"client: could not connect: {exc}") from exc
        try:
            await connection.send_text(encode(Hello(protocol=PROTOCOL)))
            reply = decode(await asyncio.wait_for(connection.receive_text(), HANDSHAKE))
        except Exception as exc:
            with contextlib.suppress(Exception):
                await connection.close(1000, "client: hello failed")
            if isinstance(exc, ThreadException) and not isinstance(exc, InputError):
                raise
            raise TransportError(f"client: the hello was not answered: {exc}") from exc
        if isinstance(reply, ErrorFrame):
            raise raised(reply.error.code, reply.error.message)
        if not isinstance(reply, Response) or reply.id != HELLO:
            raise TransportError("client: the server did not answer the hello")
        self.identity = Identity.model_validate(reply.payload["identity"])
        self.scope = str(reply.payload["scope"])
        self._connection = connection
        self._reader = asyncio.get_running_loop().create_task(self._read(connection))
        self._connected.set()

    async def _read(self, connection: Connection) -> None:
        try:
            while True:
                frame = decode(await connection.receive_text())
                if isinstance(frame, Response | ErrorFrame):
                    future = self._pending.get(frame.id)
                    if future is None or future.done():
                        continue
                    if isinstance(frame, ErrorFrame):
                        future.set_exception(raised(frame.error.code, frame.error.message))
                    else:
                        future.set_result(frame.payload)
                elif isinstance(frame, Push):
                    self._receive(frame)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        self._lost(connection)

    def _receive(self, push: Push) -> None:
        if push.name == "event":
            event = EVENTS.validate_python(push.payload)
            cursor = self._cursors.get(event.thread_id)
            if cursor is not None:
                if event.seq <= cursor:
                    return
                self._cursors[event.thread_id] = event.seq
            if isinstance(event, MemberRemoved) and event.identity.id == self.identity.id:
                self._cursors.pop(event.thread_id, None)
            self._inbox.put_nowait(event)
            return
        signal = SIGNALS.validate_python(push.payload)
        if isinstance(signal, ThreadDeleted):
            self._cursors.pop(signal.thread_id, None)
        self._inbox.put_nowait(signal)

    def _lost(self, connection: Connection) -> None:
        if self._connection is not connection:
            return
        self._connection = None
        self._connected.clear()
        for future in self._pending.values():
            if not future.done():
                future.set_exception(TransportError("client: connection lost"))
        if not self._closed and self._reconnector is None:
            self._reconnector = asyncio.get_running_loop().create_task(self._reconnect())

    async def _reconnect(self) -> None:
        attempt = 0
        try:
            while not self._closed:
                ceiling = min(RECONNECT_CAP, RECONNECT * 2 ** (attempt - 1)) if attempt else 0.0
                attempt += 1
                await asyncio.sleep(random.random() * ceiling)
                try:
                    await self._establish()
                except (ForbiddenError, ConfigurationError) as error:
                    self._fail(error)
                    return
                except ThreadException:
                    continue
                await self._rejoin()
                return
        finally:
            self._reconnector = None

    async def _rejoin(self) -> None:
        for thread_id, cursor in list(self._cursors.items()):
            try:
                joined = await self._join(thread_id, cursor)
            except TransportError:
                return
            except ThreadException:
                self._cursors.pop(thread_id, None)
                continue
            self._inbox.put_nowait(joined)

    def _fail(self, error: ThreadException) -> None:
        self._failure = error
        self._closed = True
        self._inbox.put_nowait(None)

    async def _request(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self._closed:
            raise self._failure or TransportError("client: the client is closed")
        connection = self._connection
        if connection is None:
            raise TransportError("client: not connected")
        request_id = str(next(self._ids))
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        try:
            await connection.send_text(encode(Request(id=request_id, method=method, payload=payload)))
            return await future
        except ThreadException:
            raise
        except Exception as exc:
            raise TransportError(f"client: {method} failed: {exc}") from exc
        finally:
            self._pending.pop(request_id, None)

    async def _notify(self, method: str, payload: dict[str, Any]) -> None:
        connection = self._connection
        if connection is None:
            return
        with contextlib.suppress(Exception):
            await connection.send_text(encode(Notify(method=method, payload=payload)))

    async def _send(self, thread_id: str, fields: dict[str, Any], client_id: str | None = None) -> Event:
        payload = {"thread_id": thread_id, "client_id": client_id or uuid.uuid4().hex, **fields}
        lock = self._outbox.setdefault(thread_id, asyncio.Lock())
        async with lock:
            reply = await self._deliver(payload)
        return EVENTS.validate_python(reply["event"])

    async def _deliver(self, payload: dict[str, Any]) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        started = loop.time()
        attempt = 0
        while True:
            try:
                return await self._request("event.send", payload)
            except TransportError:
                if self._closed:
                    raise
                elapsed = loop.time() - started
                if elapsed >= DEADLINE:
                    raise TransportError(
                        f"client: event {payload['client_id']!r} for thread {payload['thread_id']!r} "
                        f"was not acknowledged within {DEADLINE:g}s"
                    ) from None
                delay = min(BACKOFF * 2**attempt, BACKOFF_CAP, DEADLINE - elapsed)
                attempt += 1
                if self._connected.is_set():
                    await asyncio.sleep(delay)
                else:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self._connected.wait(), delay)

    async def _message(
        self, thread_id: str, content: Content, in_reply_to: str | None, triggered_by: Identity | None, client_id: str | None = None
    ) -> Message:
        event = await self._send(thread_id, {"kind": "message", "content": _wire(content), "in_reply_to": in_reply_to,
                                             "triggered_by": _who(triggered_by)}, client_id)
        return _expect(event, Message)

    async def _join(self, thread_id: str, since: int | None) -> Joined:
        self._cursors[thread_id] = since or 0
        reply = await self._request("thread.join", {"thread_id": thread_id, "since": since})
        joined = Joined.model_validate(reply)
        self._cursors[thread_id] = max(self._cursors.get(thread_id, 0), joined.seq)
        return joined

    async def create_thread(self, thread_id: str | None = None, metadata: dict[str, Any] | None = None) -> Thread:
        reply = await self._request("thread.create", {"thread_id": thread_id, "metadata": metadata or {}})
        return Thread.model_validate(reply["thread"])

    async def get_thread(self, thread_id: str) -> Thread:
        return Thread.model_validate((await self._request("thread.get", {"thread_id": thread_id}))["thread"])

    async def list_threads(self, before: str | None = None, limit: int = 50) -> list[Thread]:
        reply = await self._request("thread.list", {"before": before, "limit": limit})
        return [Thread.model_validate(t) for t in reply["threads"]]

    async def update_thread(self, thread_id: str, metadata: dict[str, Any]) -> Thread:
        reply = await self._request("thread.update", {"thread_id": thread_id, "metadata": metadata})
        return Thread.model_validate(reply["thread"])

    async def delete_thread(self, thread_id: str) -> None:
        await self._request("thread.delete", {"thread_id": thread_id})
        self._cursors.pop(thread_id, None)

    async def join(self, thread_id: str, since: int | None = None) -> Joined:
        try:
            return await self._join(thread_id, since)
        except ThreadException:
            self._cursors.pop(thread_id, None)
            raise

    async def leave(self, thread_id: str) -> None:
        self._cursors.pop(thread_id, None)
        await self._request("thread.leave", {"thread_id": thread_id})

    async def history(
        self, thread_id: str, before: int | None = None, limit: int = 50, kinds: list[str] | None = None
    ) -> list[Event]:
        reply = await self._request("thread.history", {"thread_id": thread_id, "before": before, "limit": limit, "kinds": kinds})
        return [EVENTS.validate_python(e) for e in reply["events"]]

    async def present(self, thread_id: str) -> list[Identity]:
        reply = await self._request("thread.present", {"thread_id": thread_id})
        return [Identity.model_validate(i) for i in reply["identities"]]

    async def add_member(self, thread_id: str, identity: Identity) -> Member:
        reply = await self._request("member.add", {"thread_id": thread_id, "identity": _who(identity)})
        return Member.model_validate(reply["member"])

    async def remove_member(self, thread_id: str, identity: Identity) -> None:
        await self._request("member.remove", {"thread_id": thread_id, "identity_id": identity.id})

    async def list_members(self, thread_id: str) -> list[Member]:
        reply = await self._request("member.list", {"thread_id": thread_id})
        return [Member.model_validate(m) for m in reply["members"]]

    async def send(
        self,
        thread_id: str,
        content: Content,
        in_reply_to: str | None = None,
        triggered_by: Identity | None = None,
        client_id: str | None = None,
    ) -> Message:
        return await self._message(thread_id, content, in_reply_to, triggered_by, client_id)

    def stream(self, thread_id: str, in_reply_to: str | None = None, triggered_by: Identity | None = None) -> Stream:
        return Stream(self, thread_id, in_reply_to, triggered_by)

    async def step(
        self,
        thread_id: str,
        name: str,
        input: dict[str, Any] | None = None,
        output: list[Part] | None = None,
        is_error: bool = False,
        duration_ms: int | None = None,
        in_reply_to: str | None = None,
        triggered_by: Identity | None = None,
        client_id: str | None = None,
    ) -> Step:
        event = await self._send(thread_id, {
            "kind": "step", "name": name, "input": input or {}, "output": _wire(output or []), "is_error": is_error,
            "duration_ms": duration_ms, "in_reply_to": in_reply_to, "triggered_by": _who(triggered_by)}, client_id)
        return _expect(event, Step)

    async def edit(self, thread_id: str, target: str, content: Content, client_id: str | None = None) -> Edit:
        event = await self._send(thread_id, {"kind": "edit", "target": target, "content": _wire(content)}, client_id)
        return _expect(event, Edit)

    async def retract(self, thread_id: str, target: str, client_id: str | None = None) -> Retract:
        return _expect(await self._send(thread_id, {"kind": "retract", "target": target}, client_id), Retract)

    async def read(self, thread_id: str, up_to_seq: int) -> Read:
        return _expect(await self._send(thread_id, {"kind": "read", "up_to_seq": up_to_seq}), Read)

    async def custom(self, thread_id: str, name: str, payload: dict[str, Any], client_id: str | None = None) -> Custom:
        event = await self._send(thread_id, {"kind": "custom", "name": name, "payload": payload}, client_id)
        return _expect(event, Custom)

    async def working(self, thread_id: str, label: str | None = None) -> None:
        await self._request("signal.send", {"thread_id": thread_id, "kind": "working", "label": label})

    async def idle(self, thread_id: str) -> None:
        await self._request("signal.send", {"thread_id": thread_id, "kind": "idle"})

    async def cancel(self, thread_id: str, to: Identity) -> None:
        await self._request("signal.send", {"thread_id": thread_id, "kind": "cancel", "to": _who(to)})

    async def events(self, include_own: bool = False) -> AsyncIterator[Item]:
        while True:
            item = await self._inbox.get()
            if item is None:
                self._inbox.put_nowait(None)
                if self._failure is not None:
                    raise self._failure
                return
            if include_own or not _own(item, self.identity):
                yield item

    async def close(self) -> None:
        if self._closed and self._connection is None and self._reader is None:
            return
        self._closed = True
        for task in (self._reconnector, self._reader):
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._reconnector = self._reader = None
        connection, self._connection = self._connection, None
        self._connected.clear()
        if connection is not None:
            with contextlib.suppress(Exception):
                await connection.close(1000, "client: closed")
        for future in self._pending.values():
            if not future.done():
                future.set_exception(TransportError("client: the client is closed"))
        self._inbox.put_nowait(None)
```

Notes for the implementer:
- `Client.connect(url, headers=)` arrives in Task 13 with the WebSocket transport.
- A reconnect that raises `ForbiddenError` or `ConfigurationError` stops the client for good: over a socket that is an upgrade refused with HTTP 401/403, or a `headers` function that raised (Task 13).
- `_own` reads `sender` for signals and `author` for events.
- `client_id=` is the one knob on a write's identity: `_send` uses it as given, or generates one. The server's dedup answers a repeat with the event stored first, so the second call returns it unchanged — even with different content (the `test_a_caller_supplied_client_id…` test pins that).
- `Stream` is not constructed by consumers; it is public so it can be named in type hints.

Add to `server.py` (import `Client` from `rfnry_thread.client`):

```python
    async def connect(self, identity: Identity, scope: str) -> Client:
        _identity(identity)
        _scope(scope)

        async def dial() -> Connection:
            return self._attach(identity, scope)

        return await Client._open(dial)
```

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: the client — one class for every participant, typed events and signals, streams, the outbox

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 13: The socket — `Client.connect`, reconnect, resume, the outbox over a real WebSocket

The backoff is adapted from `../rfnry-thread/packages/client-python/src/rfnry_thread_client/transport/reconnect.py` (full jitter, first attempt immediate, fresh credentials and a hello on every attempt, a refusal ends reconnecting — here an upgrade refused with HTTP 401/403 or a `headers` function that raised); the outbox from `../rfnry-thread/packages/client-python/src/rfnry_thread_client/outbox.py` (per-thread FIFO, deadline, capped exponential backoff, woken by reconnection). Both already live in `client.py` since Task 12; this task adds the transport and proves them on a real socket.

**Files:**
- Create: `packages/python/src/rfnry_thread/websocket.py`
- Modify: `packages/python/src/rfnry_thread/client.py` (add `Client.connect`)
- Test: `packages/python/tests/unit/test_socket.py`

**Interfaces:**
- Consumes: `Client._open`, client constants (Task 12); `Server.serve`, `Server.connect` (Tasks 9, 12).
- Produces: `rfnry_thread.websocket.open_socket(url: str, headers: dict[str, str] | None = None) -> Connection` (`websockets` client, `headers` sent with the upgrade, frames up to 16 MiB; an upgrade answered 401 or 403 is `ForbiddenError("websocket: the server refused the connection with http 401")`); `HeadersSource = dict[str, str] | Callable[[], Awaitable[dict[str, str]]]` and `Client.connect(url: str, *, headers: HeadersSource | None = None) -> Client` in `rfnry_thread.client` (a `headers` function is awaited on every attempt; one that raises is `ForbiddenError("client: the headers function raised: …")`).

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_socket.py`:

```python
import asyncio
import contextlib
from collections.abc import AsyncIterator
from http import HTTPStatus

import pytest
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request, Response

from rfnry_thread import client as client_module
from rfnry_thread.client import Client, Item
from rfnry_thread.errors import ForbiddenError, TransportError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import Identity, Joined, Message, Text
from rfnry_thread.server import Server

from .factories import BRUNO
from .test_client import drain

ANA_HEADERS = {"Authorization": "Bearer ana"}


def who(request: Request | None) -> Identity | None:
    token = "" if request is None else request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    return Identity(role="user", id=token, name=token.title()) if token else None


class Live:
    def __init__(self, server: Server) -> None:
        self.server = server
        self.open: list[Served] = []
        self.paused = False
        self.drop_ack = False
        self.url = ""

    async def drop(self) -> None:
        for served in self.open:
            await served.socket.close()
        self.open.clear()


class Served:
    def __init__(self, socket: ServerConnection, live: Live) -> None:
        self.socket = socket
        self.live = live

    async def receive_text(self) -> str:
        data = await self.socket.recv()
        return data if isinstance(data, str) else data.decode()

    async def send_text(self, data: str) -> None:
        if self.live.drop_ack and data.startswith('{"type":"response"') and '"event":' in data:
            self.live.drop_ack = False
            await self.socket.close()
            return
        await self.socket.send(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        await self.socket.close(code, reason or "")


@contextlib.asynccontextmanager
async def running(server: Server) -> AsyncIterator[Live]:
    live = Live(server)

    def upgrade(socket: ServerConnection, request: Request) -> Response | None:
        if live.paused:
            return socket.respond(HTTPStatus.SERVICE_UNAVAILABLE, "paused\n")
        if who(request) is None:
            return socket.respond(HTTPStatus.UNAUTHORIZED, "who are you?\n")
        return None

    async def handler(socket: ServerConnection) -> None:
        identity = who(socket.request)
        assert identity is not None
        served = Served(socket, live)
        live.open.append(served)
        await server.serve(served, identity=identity, scope="acme")

    async with serve(handler, "127.0.0.1", 0, process_request=upgrade) as listening:
        live.url = f"ws://127.0.0.1:{next(iter(listening.sockets)).getsockname()[1]}"
        yield live


@pytest.fixture(autouse=True)
def fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    monkeypatch.setattr(client_module, "BACKOFF", 0.01)


@pytest.fixture
async def live(server: Server) -> AsyncIterator[Live]:
    async with running(server) as live:
        yield live


def first_text(message: Message) -> str:
    part = message.content[0]
    assert isinstance(part, Text)
    return part.text


async def until_joined(client: Client, timeout: float = 3.0) -> list[Item]:
    items: list[Item] = []

    async def collect() -> None:
        async for item in client.events():
            items.append(item)
            if isinstance(item, Joined):
                return

    await asyncio.wait_for(collect(), timeout)
    return items


async def room(live: Live) -> tuple[Client, Client]:
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    bruno = await live.server.connect(BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", BRUNO)
    await ana.join("t1")
    await drain(ana)
    return ana, bruno


async def read_to_the_end(client: Client) -> None:
    async for _ in client.events():
        pass


async def test_a_socket_client_is_the_same_client(live: Live) -> None:
    ana = await Client.connect(live.url, headers=ANA_HEADERS)
    assert (ana.identity.id, ana.scope) == ("ana", "acme")
    await ana.create_thread(thread_id="t1")
    await ana.send("t1", "hello")
    assert [e.kind for e in await ana.history("t1")] == ["member_added", "message"]
    await ana.close()


async def test_a_refused_upgrade_and_an_unreachable_server_raise(live: Live) -> None:
    with pytest.raises(ForbiddenError, match="^websocket: the server refused the connection with http 401$"):
        await Client.connect(live.url)
    with pytest.raises(TransportError, match="^client: could not connect"):
        await Client.connect("ws://127.0.0.1:1", headers=ANA_HEADERS)


async def test_a_dropped_socket_resumes_every_joined_thread_in_order_once(live: Live) -> None:
    calls: list[int] = []

    async def headers() -> dict[str, str]:
        calls.append(1)
        return ANA_HEADERS

    ana = await Client.connect(live.url, headers=headers)
    bruno = await live.server.connect(BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", BRUNO)
    await ana.join("t1")
    await drain(ana)
    live.paused = True
    await live.drop()
    for n in range(5):
        await bruno.send("t1", f"m{n}")
    live.paused = False
    items = await until_joined(ana)
    assert [first_text(i) for i in items if isinstance(i, Message)] == [f"m{n}" for n in range(5)]
    assert isinstance(items[-1], Joined) and items[-1].truncated is False
    assert len(calls) >= 2
    await bruno.send("t1", "live")
    after = [i for i in await drain(ana) if isinstance(i, Message)]
    assert [first_text(m) for m in after] == ["live"]


async def test_a_resume_beyond_the_replay_limit_says_truncated(storage: InMemoryStorage) -> None:
    async with running(Server(storage, replay_limit=3)) as live:
        ana, bruno = await room(live)
        live.paused = True
        await live.drop()
        for n in range(5):
            await bruno.send("t1", f"m{n}")
        live.paused = False
        items = await until_joined(ana)
        assert [first_text(i) for i in items if isinstance(i, Message)] == ["m2", "m3", "m4"]
        assert isinstance(items[-1], Joined) and items[-1].truncated is True


async def test_a_write_whose_ack_was_lost_is_stored_exactly_once(live: Live) -> None:
    ana, _ = await room(live)
    live.drop_ack = True
    message = await asyncio.wait_for(ana.send("t1", "exactly once"), 5.0)
    stored = [e for e in await ana.history("t1") if isinstance(e, Message)]
    assert [e.id for e in stored] == [message.id]


async def test_a_write_made_while_disconnected_is_delivered_after_reconnect(live: Live) -> None:
    ana, bruno = await room(live)
    live.paused = True
    await live.drop()
    pending = asyncio.create_task(ana.send("t1", "sent offline"))
    await asyncio.sleep(0.1)
    assert not pending.done()
    live.paused = False
    message = await asyncio.wait_for(pending, 5.0)
    assert [e.id for e in await bruno.history("t1") if isinstance(e, Message)] == [message.id]


async def test_a_write_not_acknowledged_within_the_deadline_fails(live: Live, monkeypatch: pytest.MonkeyPatch) -> None:
    ana, _ = await room(live)
    monkeypatch.setattr(client_module, "DEADLINE", 0.3)
    live.paused = True
    await live.drop()
    with pytest.raises(TransportError, match=r"^client: event '\w+' for thread 't1' was not acknowledged within 0.3s$"):
        await ana.send("t1", "lost")


async def test_an_upgrade_refused_on_reconnect_ends_the_stream_with_the_error(live: Live) -> None:
    answers = iter([ANA_HEADERS, {}])

    async def headers() -> dict[str, str]:
        return next(answers, {})

    ana = await Client.connect(live.url, headers=headers)
    await live.drop()
    with pytest.raises(ForbiddenError, match="^websocket: the server refused the connection with http 401$"):
        await asyncio.wait_for(read_to_the_end(ana), 3.0)


async def test_a_headers_function_that_raises_on_reconnect_ends_the_stream_with_the_error(live: Live) -> None:
    calls: list[int] = []

    async def headers() -> dict[str, str]:
        calls.append(1)
        if len(calls) > 1:
            raise RuntimeError("session expired")
        return ANA_HEADERS

    ana = await Client.connect(live.url, headers=headers)
    await live.drop()
    with pytest.raises(ForbiddenError, match="^client: the headers function raised: session expired$"):
        await asyncio.wait_for(read_to_the_end(ana), 3.0)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_socket.py -q`
Expected: FAIL — `AttributeError: type object 'Client' has no attribute 'connect'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/websocket.py`:

```python
from __future__ import annotations

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import InvalidStatus

from rfnry_thread.connection import Connection
from rfnry_thread.errors import ForbiddenError

MAX_FRAME = 16 * 1024 * 1024
REFUSED = frozenset({401, 403})


class _Socket:
    def __init__(self, socket: ClientConnection) -> None:
        self._socket = socket

    async def receive_text(self) -> str:
        data = await self._socket.recv()
        return data if isinstance(data, str) else data.decode()

    async def send_text(self, data: str) -> None:
        await self._socket.send(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        await self._socket.close(code, reason or "")


async def open_socket(url: str, headers: dict[str, str] | None = None) -> Connection:
    try:
        socket = await connect(url, additional_headers=headers, max_size=MAX_FRAME, open_timeout=10)
    except InvalidStatus as exc:
        status = exc.response.status_code
        if status in REFUSED:
            raise ForbiddenError(f"websocket: the server refused the connection with http {status}") from exc
        raise
    return _Socket(socket)
```

In `client.py`, add `from rfnry_thread.websocket import open_socket` to the imports, `HeadersSource = dict[str, str] | Callable[[], Awaitable[dict[str, str]]]` beside `Dial`, this function at module level:

```python
async def _headers(source: HeadersSource | None) -> dict[str, str] | None:
    if source is None or isinstance(source, dict):
        return source
    try:
        return await source()
    except ThreadException:
        raise
    except Exception as exc:
        raise ForbiddenError(f"client: the headers function raised: {exc}") from exc
```

and this classmethod to `Client`, above `_open`:

```python
    @classmethod
    async def connect(cls, url: str, *, headers: HeadersSource | None = None) -> Client:
        async def dial() -> Connection:
            return await open_socket(url, await _headers(headers))

        return await cls._open(dial)
```

The test server authenticates the upgrade the way a consumer would — `process_request` answers 401 to a request without a bearer token, and the handler hands `serve` the identity it read — so these tests exercise the whole contract of design §18 over a real socket.

If a test above fails, the fix belongs in `client.py`'s `_lost`, `_reconnect`, `_rejoin` or `_deliver` (Task 12), not in the test: the tests state the delivery contract of `docs/thread.md` § Delivery and resume.

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS. These tests exercise timing; run the file five times and expect no failure:
`for i in 1 2 3 4 5; do uv run pytest tests/unit/test_socket.py -q || break; done`.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: Client.connect over a WebSocket — headers on the upgrade, reconnect with jitter, resume from the cursor, writes stored exactly once

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 14: ThreadTools

Written fresh (the old library had no model-facing surface).

**Files:**
- Modify: `packages/python/src/rfnry_thread/tools.py` (append)
- Test: `packages/python/tests/unit/test_thread_tools.py`

**Interfaces:**
- Consumes: `Tool`, `Tools` (Task 2); `Client` (Task 12); `resolve` (Task 3).
- Produces: `ThreadTools(client: Client, *, thread_id: str, triggered_by: Identity | None = None, in_reply_to: str | None = None)` — a `Tools` of four: `send {text}` → `{"id", "seq"}`; `read_recent {limit=20, 1..200}` → `list[Part]`; `working {label}` → `"ok"`; `idle {}` → `"ok"`.

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_thread_tools.py`:

```python
import json

import pytest

from rfnry_thread.errors import ConfigurationError, InputError
from rfnry_thread.models import Image, Message, Text, Working
from rfnry_thread.server import Server
from rfnry_thread.tools import Tool, Tools, ThreadTools

from .factories import ANA
from .test_client import drain, pair, take


async def test_four_tools_pinned_to_one_thread(server: Server) -> None:
    ana, helper = await pair(server)
    tools = ThreadTools(helper, thread_id="t1", triggered_by=ANA, in_reply_to="e1")
    assert tools.names == ["send", "read_recent", "working", "idle"]
    assert all("thread_id" not in d["input_schema"].get("properties", {}) for d in tools.definitions())
    sent = await tools.dispatch("send", {"text": "The second charge is refunded."})
    assert isinstance(sent, dict) and sent["seq"] > 0
    message = await take(ana, Message)
    assert (message.in_reply_to, message.triggered_by) == ("e1", ANA)
    assert await tools.dispatch("working", {"label": "Checking the invoice"}) == "ok"
    assert (await take(ana, Working)).label == "Checking the invoice"
    assert await tools.dispatch("idle", {}) == "ok"


async def test_read_recent_returns_resolved_events_as_parts(server: Server) -> None:
    ana, helper = await pair(server)
    image = Image(media_type="image/png", url="https://files.example.com/receipt.png")
    first = await ana.send("t1", [Text(text="Here is the receipt."), image])
    await ana.edit("t1", first.id, [Text(text="Here is the right receipt."), image])
    await helper.step("t1", name="Read", input={"path": "rules/refunds.md"}, output=[Text(text="Refund in full.")])
    retracted = await ana.send("t1", "never mind")
    await ana.retract("t1", retracted.id)
    await drain(helper)
    recent = await ThreadTools(helper, thread_id="t1").dispatch("read_recent", {"limit": 10})
    assert isinstance(recent, list)
    headers = [json.loads(p.text) for p in recent if isinstance(p, Text) and p.text.startswith("{")]
    assert [(h["kind"], h["author"]["id"]) for h in headers] == [("message", "ana"), ("step", "helper")]
    assert recent[1:3] == [Text(text="Here is the right receipt."), image]
    assert headers[1]["name"] == "Read" and headers[1]["input"] == {"path": "rules/refunds.md"}


async def test_arguments_are_validated_and_the_thread_is_required(server: Server) -> None:
    _, helper = await pair(server)
    with pytest.raises(ConfigurationError, match="^tools: a thread_id is required$"):
        ThreadTools(helper, thread_id="")
    tools = ThreadTools(helper, thread_id="t1")
    with pytest.raises(InputError, match="^tools: invalid arguments for read_recent"):
        await tools.dispatch("read_recent", {"limit": 0})


async def test_it_combines_like_any_tools(server: Server) -> None:
    _, helper = await pair(server)

    async def now(args: dict[str, object]) -> str:
        return "2026-09-24T12:00:00Z"

    combined = ThreadTools(helper, thread_id="t1").only("send", "read_recent") + Tools([Tool("now", "The current UTC time", body=now)])
    assert combined.names == ["send", "read_recent", "now"]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_thread_tools.py -q`
Expected: FAIL — `ImportError: cannot import name 'ThreadTools'`.

- [ ] **Step 3: Implement**

Append to `packages/python/src/rfnry_thread/tools.py` (after the family block; add the imports at the top of the file — `json`, `from typing import TYPE_CHECKING`, `from pydantic import Field`, `from rfnry_thread.models import Identity, Message, Step, Text` and `from rfnry_thread.resolve import resolve`; import `Client` under `if TYPE_CHECKING:` to avoid a cycle):

```python
class _Send(BaseModel):
    text: str = Field(description="The message to post in the thread")


class _ReadRecent(BaseModel):
    limit: int = Field(default=20, ge=1, le=200, description="How many of the latest messages and steps to read")


class _Working(BaseModel):
    label: str = Field(description="What you are doing right now, as a person would read it")


class _Idle(BaseModel):
    pass


class ThreadTools(Tools):
    def __init__(
        self,
        client: Client,
        *,
        thread_id: str,
        triggered_by: Identity | None = None,
        in_reply_to: str | None = None,
    ) -> None:
        if not thread_id:
            raise ConfigurationError("tools: a thread_id is required")

        async def send(args: dict[str, Any]) -> Result:
            event = await client.send(thread_id, args["text"], in_reply_to=in_reply_to, triggered_by=triggered_by)
            return {"id": event.id, "seq": event.seq}

        async def read_recent(args: dict[str, Any]) -> Result:
            limit = int(args.get("limit", 20))
            events = await client.history(thread_id, limit=200, kinds=["message", "step", "edit", "retract"])
            view = [e for e in resolve(events) if isinstance(e, Message | Step)][-limit:]
            found: list[Part] = []
            for event in view:
                header: dict[str, Any] = {"seq": event.seq, "kind": event.kind, "in_reply_to": event.in_reply_to,
                                          "author": event.author.model_dump(mode="json", include={"role", "id", "name"})}
                if isinstance(event, Step):
                    header |= {"name": event.name, "input": event.input, "is_error": event.is_error}
                found.append(Text(text=json.dumps(header)))
                found.extend(event.output if isinstance(event, Step) else event.content)
            return found

        async def working(args: dict[str, Any]) -> Result:
            await client.working(thread_id, label=args["label"])
            return "ok"

        async def idle(args: dict[str, Any]) -> Result:
            await client.idle(thread_id)
            return "ok"

        super().__init__([
            Tool("send", "Post a message to the conversation", body=send, args=_Send),
            Tool("read_recent", "Read the latest messages and steps of the conversation, as they were sent", body=read_recent, args=_ReadRecent),
            Tool("working", "Show the people in the conversation what you are doing", body=working, args=_Working),
            Tool("idle", "Show that you are no longer working", body=idle, args=_Idle),
        ])
```

- [ ] **Step 4: Run the gate**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS; `test_family.py` still passes the tools block (the append is after its end marker).

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: ThreadTools — a thread as a family Tools, pinned to one thread and one cause

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 15: PostgresStorage — rows, dedup, gapless seq, LISTEN/NOTIFY, re-LISTEN; two replicas on one database

Adapted from `../rfnry-thread/packages/server-python/src/rfnry_thread_server/store/postgres/{store.py,schema.sql}`: the dedup key `(thread, author, client_id)` answered with the stored row, `ON CONFLICT DO NOTHING` for idempotent creation, an advisory lock around the DDL. New: `scope` leads every key, `seq` is thread-local and gapless (a `last_seq` column bumped under the thread row's lock in the event's transaction — the old global identity column had gaps), the `NOTIFY` rides the same transaction, the `LISTEN` connection is watched and re-acquired with jittered backoff when it drops (the old store went deaf for good), and the pool is the consumer's (the engine's `PostgresStorage(pool)` shape; no executor protocol, no driver import).

**Files:**
- Create: `packages/python/src/rfnry_thread/postgres.py`
- Modify: `packages/python/tests/integration/conftest.py` (the Postgres parameter)
- Test: `packages/python/tests/integration/test_replicas.py`, `packages/python/tests/integration/test_tenancy.py`

**Interfaces:**
- Consumes: the `Storage` protocol, `OnResume` and the suite (Task 5); `Server`, `start`/`stop` (Tasks 9, 11 — `start` passes `Hub.announce` as `on_resume`); `tests/unit/peer.py`, `tests/unit/conftest.py` (Task 9); `tests/until.py` (Task 5).
- Produces: `PostgresStorage(pool: Any)` in `rfnry_thread.postgres`, implementing `Storage`; channels `thread_events` (payload `{"s", "t", "q"}`) and `thread_signals` (payload `{"s", "t", "p"}`); `NOTIFY_LIMIT = 7900`; `RELISTEN = 0.5`, `RELISTEN_CAP = 30.0` (full-jitter backoff for re-acquiring a dropped `LISTEN` connection). `listen` holds one pool connection, watches it with asyncpg's termination listener, and on a drop releases it, acquires another after `random() * min(RELISTEN_CAP, RELISTEN * 2**attempt)` seconds (retrying until one subscribes), adds both channel listeners, then awaits `on_resume`.

- [ ] **Step 1: Add the Postgres parameter and the failing tests**

`packages/python/tests/integration/conftest.py` becomes:

```python
import os
from collections.abc import AsyncIterator
from typing import Any

import pytest

from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.storage import Storage

TABLES = "thread_events, thread_members, thread_threads"


async def postgres_pool() -> Any:
    url = os.environ.get("RFNRY_TEST_POSTGRES_URL")
    if not url:
        pytest.skip("RFNRY_TEST_POSTGRES_URL is not set")
    asyncpg = pytest.importorskip("asyncpg")
    return await asyncpg.create_pool(url, min_size=2, max_size=10)


async def fresh(pool: Any) -> PostgresStorage:
    storage = PostgresStorage(pool)
    await storage.initialize()
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {TABLES}")
    return storage


@pytest.fixture(params=["inmemory", pytest.param("postgres", marks=pytest.mark.postgres)])
async def storage(request: Any) -> AsyncIterator[Storage]:
    if request.param == "inmemory":
        yield InMemoryStorage()
        return
    pool = await postgres_pool()
    yield await fresh(pool)
    await pool.close()
```

`packages/python/tests/integration/test_replicas.py`:

```python
import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest

from rfnry_thread.models import Message, Text, Thread
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server
from unit.conftest import say
from unit.factories import ANA, HELPER
from unit.peer import Peer
from until import until

from .conftest import fresh, postgres_pool

pytestmark = pytest.mark.postgres


async def listeners(pool: Any) -> list[int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT pid FROM pg_stat_activity WHERE datname = current_database() "
            "AND pid <> pg_backend_pid() AND query = 'LISTEN \"thread_signals\"'"
        )
    return [row["pid"] for row in rows]


async def terminate(pool: Any, expected: int) -> list[int]:
    killed = await listeners(pool)
    assert len(killed) == expected
    async with pool.acquire() as conn:
        for pid in killed:
            await conn.execute("SELECT pg_terminate_backend($1)", pid)
    return killed


async def relistened(pool: Any, killed: list[int], expected: int, timeout: float = 5.0) -> None:
    async def wait() -> None:
        while len([pid for pid in await listeners(pool) if pid not in killed]) < expected:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(wait(), timeout)


@pytest.fixture
async def replicas() -> AsyncIterator[tuple[Server, Server]]:
    near_pool, far_pool = await postgres_pool(), await postgres_pool()
    near = Server(await fresh(near_pool))
    far = Server(PostgresStorage(far_pool))
    await near.start()
    await far.start()
    yield near, far
    await near.stop()
    await far.stop()
    await near_pool.close()
    await far_pool.close()


async def room(near: Server, far: Server) -> tuple[Peer, Peer]:
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await ana.call("thread.join", thread_id="t1")
    helper = await Peer.open(far, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await ana.quiet(0.3)
    await helper.quiet(0.3)
    return ana, helper


async def test_events_cross_databases_replicas_in_order_once(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    for n in range(10):
        await say(ana, "t1", f"m{n}")
    messages = [p for p in await helper.quiet(0.5) if p["kind"] == "message"]
    assert [p["seq"] for p in messages] == list(range(3, 13))


async def test_signals_are_mirrored_through_notify(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working", timeout=2.0))["label"] == "Reading"


async def test_the_finishing_message_reaches_a_remote_connection_before_stream_end(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    for attempt in range(20):
        stream_id = f"s{attempt}"
        await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id=stream_id)
        await say(helper, "t1", "Done.", client_id=stream_id)
        await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id=stream_id)
    received = [p for p in await ana.quiet(1.0) if p["kind"] in ("message", "stream.end")]
    for attempt in range(20):
        stream_id = f"s{attempt}"
        message = next(n for n, p in enumerate(received) if p["kind"] == "message" and p["client_id"] == stream_id)
        end = next(n for n, p in enumerate(received) if p["kind"] == "stream.end" and p["stream_id"] == stream_id)
        assert message < end


async def test_removal_cuts_off_a_member_on_another_replica(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    await say(ana, "t1", "after")
    kinds = [p["kind"] for p in await helper.quiet(0.5)]
    assert "member_removed" in kinds and "message" not in kinds


async def test_a_dropped_listener_listens_again_and_resumes() -> None:
    pool = await postgres_pool()
    storage = await fresh(pool)
    await storage.create_thread("acme", Thread(id="t1", scope="acme", created_at=datetime.now(UTC)))
    seen: list[int] = []
    heard: list[dict[str, Any]] = []
    resumed = asyncio.Event()

    async def on_event(scope: str, thread_id: str, seq: int) -> None:
        seen.append(seq)

    async def on_signal(scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        heard.append(payload)

    async def on_resume() -> None:
        resumed.set()

    stop = await storage.listen(on_event, on_signal, on_resume)
    killed = await terminate(pool, 1)
    await relistened(pool, killed, 1)
    await asyncio.wait_for(resumed.wait(), 5.0)
    await storage.publish("acme", "t1", {"kind": "idle"})
    await storage.append_event("acme", Message(id="m1", thread_id="t1", author=ANA, created_at=datetime.now(UTC),
                                               client_id="c1", content=[Text(text="back")]))
    await until(lambda: heard == [{"kind": "idle"}] and seen == [1])
    await stop()
    assert await listeners(pool) == []
    await pool.close()


async def test_signals_and_events_cross_again_after_the_listeners_drop(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    pool = await postgres_pool()
    killed = await terminate(pool, 2)
    await say(ana, "t1", "during")
    await relistened(pool, killed, 2)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working", timeout=2.0))["label"] == "Reading"
    await say(ana, "t1", "after")
    messages = [p for p in await helper.quiet(0.5) if p["kind"] == "message"]
    assert [p["content"][0]["text"] for p in messages] == ["during", "after"]
    await pool.close()
```

`listeners` matches a backend whose last statement was asyncpg's `LISTEN "thread_signals"` — the second of the two subscriptions, so a match means the connection hears both channels. Why "during" arrives: its notification may be lost with the terminated listener, but the notification for "after" finds the far connection's cursor two behind and the hub fills the gap from storage (Task 8) — the bus carries latency, storage carries completeness.

`packages/python/tests/integration/test_tenancy.py`:

```python
from datetime import UTC, datetime

from rfnry_thread.models import Thread
from rfnry_thread.server import Server
from rfnry_thread.storage import Storage
from unit.conftest import say
from unit.factories import ANA
from unit.peer import Peer


async def test_nothing_of_a_is_reachable_from_b(storage: Storage) -> None:
    server = Server(storage)
    a = await Peer.open(server, ANA, scope="tenant-a")
    b = await Peer.open(server, ANA, scope="tenant-b")
    for peer in (a, b):
        await peer.call("thread.create", thread_id="t1")
        await peer.call("thread.join", thread_id="t1")
    await say(a, "t1", "MARKER-A")
    assert all("MARKER-A" not in str(p) for p in await b.quiet())
    assert all("MARKER-A" not in str(e) for e in (await b.call("thread.history", thread_id="t1"))["events"])
    assert all("MARKER-A" not in str(t) for t in (await b.call("thread.list"))["threads"])


async def test_erasure_is_provable_and_falsifiable(storage: Storage) -> None:
    server = Server(storage)
    await server.create_thread("tenant-a", thread_id="t1")
    await server.add_member("tenant-a", "t1", ANA)
    await server.create_thread("tenant-b", thread_id="t1")
    assert not await server.verify_absent("tenant-a")
    assert await server.delete_scope("tenant-a") == 3
    assert await server.verify_absent("tenant-a")
    assert not await server.verify_absent("tenant-b")
    await storage.create_thread("tenant-a", Thread(id="orphan", scope="tenant-a", created_at=datetime.now(UTC)))
    assert not await server.verify_absent("tenant-a")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && RFNRY_TEST_POSTGRES_URL=postgresql://thread:thread@127.0.0.1:55436/thread uv run pytest tests/integration -q -m postgres`
Expected: FAIL — `No module named 'rfnry_thread.postgres'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/postgres.py`:

```python
from __future__ import annotations

import asyncio
import contextlib
import json
import random
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from rfnry_thread.errors import NotFoundError, StorageError, ThreadException
from rfnry_thread.models import EVENTS, Event, Member, Thread
from rfnry_thread.storage import OnEvent, OnResume, OnSignal, Stop

EVENTS_CHANNEL = "thread_events"
SIGNALS_CHANNEL = "thread_signals"
NOTIFY_LIMIT = 7900
RELISTEN = 0.5
RELISTEN_CAP = 30.0
_LOCK = 0x7468726561640001

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS thread_threads (
        scope TEXT NOT NULL, id TEXT NOT NULL, metadata JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL, last_seq BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (scope, id))
    """,
    "CREATE INDEX IF NOT EXISTS ix_thread_threads_created ON thread_threads (scope, created_at DESC, id DESC)",
    """
    CREATE TABLE IF NOT EXISTS thread_members (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, identity_id TEXT NOT NULL,
        added_at TIMESTAMPTZ NOT NULL, body JSONB NOT NULL,
        PRIMARY KEY (scope, thread_id, identity_id),
        FOREIGN KEY (scope, thread_id) REFERENCES thread_threads (scope, id) ON DELETE CASCADE)
    """,
    "CREATE INDEX IF NOT EXISTS ix_thread_members_identity ON thread_members (scope, identity_id)",
    """
    CREATE TABLE IF NOT EXISTS thread_events (
        scope TEXT NOT NULL, thread_id TEXT NOT NULL, seq BIGINT NOT NULL, id TEXT NOT NULL,
        kind TEXT NOT NULL, author_id TEXT NOT NULL, client_id TEXT NOT NULL, body JSONB NOT NULL,
        PRIMARY KEY (scope, thread_id, seq),
        UNIQUE (scope, thread_id, id),
        UNIQUE (scope, thread_id, author_id, client_id),
        FOREIGN KEY (scope, thread_id) REFERENCES thread_threads (scope, id) ON DELETE CASCADE)
    """,
]

_THREAD = "scope, id, metadata, created_at"


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _thread(row: Any) -> Thread:
    return Thread(id=row["id"], scope=row["scope"], metadata=_json(row["metadata"]), created_at=row["created_at"])


def _event(body: Any) -> Event:
    return EVENTS.validate_python(_json(body))


def _count(status: str) -> int:
    return int(status.rsplit(" ", 1)[-1])


class PostgresStorage:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    @asynccontextmanager
    async def _guard(self, operation: str) -> AsyncIterator[None]:
        try:
            yield
        except ThreadException:
            raise
        except Exception as exc:
            raise StorageError(f"postgres: storage rejected {operation}: {exc}") from exc

    async def initialize(self) -> None:
        async with self._guard("initialize"), self._pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock($1)", _LOCK)
            for ddl in _DDL:
                await conn.execute(ddl)

    async def create_thread(self, scope: str, thread: Thread) -> tuple[Thread, bool]:
        if thread.scope != scope:
            raise StorageError("postgres: row scope does not match the call scope")
        async with self._guard("create_thread"), self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"INSERT INTO thread_threads ({_THREAD}) VALUES ($1, $2, $3::jsonb, $4) "
                f"ON CONFLICT (scope, id) DO NOTHING RETURNING {_THREAD}",
                scope, thread.id, json.dumps(thread.metadata), thread.created_at,
            )
            if row is not None:
                return _thread(row), True
            row = await conn.fetchrow(f"SELECT {_THREAD} FROM thread_threads WHERE scope = $1 AND id = $2", scope, thread.id)
            return _thread(row), False

    async def get_thread(self, scope: str, thread_id: str) -> Thread | None:
        async with self._guard("get_thread"), self._pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT {_THREAD} FROM thread_threads WHERE scope = $1 AND id = $2", scope, thread_id)
        return None if row is None else _thread(row)

    async def list_threads(self, scope: str, member_id: str, before: str | None = None, limit: int = 50) -> list[Thread]:
        async with self._guard("list_threads"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT t.scope, t.id, t.metadata, t.created_at FROM thread_threads t "
                "JOIN thread_members m ON m.scope = t.scope AND m.thread_id = t.id AND m.identity_id = $2 "
                "WHERE t.scope = $1 AND ($3::text IS NULL OR (t.created_at, t.id) < "
                "(SELECT a.created_at, a.id FROM thread_threads a WHERE a.scope = $1 AND a.id = $3)) "
                "ORDER BY t.created_at DESC, t.id DESC LIMIT $4",
                scope, member_id, before, max(limit, 0),
            )
        return [_thread(row) for row in rows]

    async def update_thread(self, scope: str, thread_id: str, metadata: dict[str, Any]) -> Thread | None:
        async with self._guard("update_thread"), self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"UPDATE thread_threads SET metadata = $3::jsonb WHERE scope = $1 AND id = $2 RETURNING {_THREAD}",
                scope, thread_id, json.dumps(metadata),
            )
        return None if row is None else _thread(row)

    async def delete_thread(self, scope: str, thread_id: str) -> bool:
        async with self._guard("delete_thread"), self._pool.acquire() as conn:
            status = await conn.execute("DELETE FROM thread_threads WHERE scope = $1 AND id = $2", scope, thread_id)
        return _count(status) > 0

    async def add_member(self, scope: str, member: Member) -> tuple[Member, bool]:
        async with self._guard("add_member"), self._pool.acquire() as conn:
            body = await conn.fetchval(
                "INSERT INTO thread_members (scope, thread_id, identity_id, added_at, body) "
                "SELECT $1, $2, $3, $4, $5::jsonb WHERE EXISTS "
                "(SELECT 1 FROM thread_threads WHERE scope = $1 AND id = $2) "
                "ON CONFLICT (scope, thread_id, identity_id) DO NOTHING RETURNING body",
                scope, member.thread_id, member.identity.id, member.added_at, member.model_dump_json(),
            )
            if body is not None:
                return Member.model_validate(_json(body)), True
            existing = await conn.fetchval(
                "SELECT body FROM thread_members WHERE scope = $1 AND thread_id = $2 AND identity_id = $3",
                scope, member.thread_id, member.identity.id,
            )
        if existing is None:
            raise NotFoundError(f"postgres: thread {member.thread_id!r} was not found")
        return Member.model_validate(_json(existing)), False

    async def remove_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        async with self._guard("remove_member"), self._pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM thread_members WHERE scope = $1 AND thread_id = $2 AND identity_id = $3",
                scope, thread_id, identity_id,
            )
        return _count(status) > 0

    async def list_members(self, scope: str, thread_id: str) -> list[Member]:
        async with self._guard("list_members"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT body FROM thread_members WHERE scope = $1 AND thread_id = $2 ORDER BY added_at, identity_id",
                scope, thread_id,
            )
        return [Member.model_validate(_json(row["body"])) for row in rows]

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        async with self._guard("is_member"), self._pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM thread_members WHERE scope = $1 AND thread_id = $2 AND identity_id = $3)",
                scope, thread_id, identity_id,
            ))

    async def append_event(self, scope: str, event: Event) -> tuple[Event, bool]:
        async with self._guard("append_event"), self._pool.acquire() as conn, conn.transaction():
            last = await conn.fetchval(
                "SELECT last_seq FROM thread_threads WHERE scope = $1 AND id = $2 FOR UPDATE", scope, event.thread_id
            )
            if last is None:
                raise NotFoundError(f"postgres: thread {event.thread_id!r} was not found")
            existing = await conn.fetchval(
                "SELECT body FROM thread_events WHERE scope = $1 AND thread_id = $2 AND author_id = $3 AND client_id = $4",
                scope, event.thread_id, event.author.id, event.client_id,
            )
            if existing is not None:
                return _event(existing), False
            stored = event.model_copy(update={"seq": int(last) + 1})
            await conn.execute(
                "INSERT INTO thread_events (scope, thread_id, seq, id, kind, author_id, client_id, body) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)",
                scope, stored.thread_id, stored.seq, stored.id, stored.kind, stored.author.id, stored.client_id,
                stored.model_dump_json(),
            )
            await conn.execute("UPDATE thread_threads SET last_seq = $3 WHERE scope = $1 AND id = $2", scope, stored.thread_id, stored.seq)
            await conn.execute("SELECT pg_notify($1, $2)", EVENTS_CHANNEL,
                               json.dumps({"s": scope, "t": stored.thread_id, "q": stored.seq}))
        return stored, True

    async def list_events(
        self,
        scope: str,
        thread_id: str,
        since: int | None = None,
        before: int | None = None,
        limit: int = 50,
        kinds: Sequence[str] | None = None,
    ) -> list[Event]:
        if limit < 1:
            return []
        where = ["scope = $1", "thread_id = $2"]
        args: list[Any] = [scope, thread_id]
        if since is not None:
            args.append(since)
            where.append(f"seq > ${len(args)}")
        if before is not None:
            args.append(before)
            where.append(f"seq < ${len(args)}")
        if kinds is not None:
            args.append(list(kinds))
            where.append(f"kind = ANY(${len(args)}::text[])")
        args.append(limit)
        order = "ASC" if since is not None else "DESC"
        async with self._guard("list_events"), self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT body FROM thread_events WHERE {' AND '.join(where)} ORDER BY seq {order} LIMIT ${len(args)}", *args
            )
        events = [_event(row["body"]) for row in rows]
        return events if since is not None else events[::-1]

    async def get_event(self, scope: str, thread_id: str, event_id: str) -> Event | None:
        async with self._guard("get_event"), self._pool.acquire() as conn:
            body = await conn.fetchval(
                "SELECT body FROM thread_events WHERE scope = $1 AND thread_id = $2 AND id = $3", scope, thread_id, event_id
            )
        return None if body is None else _event(body)

    async def delete_scope(self, scope: str) -> int:
        async with self._guard("delete_scope"), self._pool.acquire() as conn, conn.transaction():
            removed = 0
            for table in ("thread_events", "thread_members", "thread_threads"):
                removed += _count(await conn.execute(f"DELETE FROM {table} WHERE scope = $1", scope))
        return removed

    async def verify_absent(self, scope: str) -> bool:
        async with self._guard("verify_absent"), self._pool.acquire() as conn:
            counts = [
                await conn.fetchval(f"SELECT count(*) FROM {table} WHERE scope = $1", scope)
                for table in ("thread_threads", "thread_members", "thread_events")
            ]
        return all(count == 0 for count in counts)

    async def listen(self, on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None) -> Stop:
        tasks: set[asyncio.Future[None]] = set()
        lost = asyncio.Event()
        held: list[Any] = [None]

        def receive(_connection: Any, _pid: int, channel: str, text: str) -> None:
            data = json.loads(text)
            if channel == EVENTS_CHANNEL:
                future = asyncio.ensure_future(on_event(data["s"], data["t"], int(data["q"])))
            else:
                future = asyncio.ensure_future(on_signal(data["s"], data["t"], data["p"]))
            tasks.add(future)
            future.add_done_callback(tasks.discard)

        def terminated(_connection: Any) -> None:
            lost.set()

        async def attach() -> None:
            conn = await self._pool.acquire()
            try:
                conn.add_termination_listener(terminated)
                await conn.add_listener(EVENTS_CHANNEL, receive)
                await conn.add_listener(SIGNALS_CHANNEL, receive)
            except BaseException:
                with contextlib.suppress(Exception):
                    await self._pool.release(conn)
                raise
            held[0] = conn

        async def release() -> None:
            conn, held[0] = held[0], None
            if conn is None:
                return
            with contextlib.suppress(Exception):
                conn.remove_termination_listener(terminated)
                await conn.remove_listener(EVENTS_CHANNEL, receive)
                await conn.remove_listener(SIGNALS_CHANNEL, receive)
            with contextlib.suppress(Exception):
                await self._pool.release(conn)

        async def relisten() -> None:
            while True:
                await lost.wait()
                lost.clear()
                await release()
                attempt = 0
                while held[0] is None:
                    await asyncio.sleep(random.random() * min(RELISTEN_CAP, RELISTEN * 2**attempt))
                    attempt += 1
                    with contextlib.suppress(Exception):
                        await attach()
                if on_resume is not None:
                    with contextlib.suppress(Exception):
                        await on_resume()

        async with self._guard("listen"):
            await attach()
        watcher = asyncio.get_running_loop().create_task(relisten())

        async def stop() -> None:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher
            await release()
            for future in list(tasks):
                future.cancel()

        return stop

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        text = json.dumps({"s": scope, "t": thread_id, "p": payload})
        size = len(text.encode())
        if size > NOTIFY_LIMIT:
            raise StorageError(f"postgres: a notification is at most {NOTIFY_LIMIT} bytes; this one is {size}")
        async with self._guard("publish"), self._pool.acquire() as conn:
            await conn.execute("SELECT pg_notify($1, $2)", SIGNALS_CHANNEL, text)
```

Why the order in `append_event`: the `FOR UPDATE` on the thread row serializes every append to one thread across replicas, so the dedup read after it sees any concurrent twin that committed first (READ COMMITTED takes a fresh snapshot per statement), and `last_seq + 1` never skips or repeats. A rollback undoes the bump with the row, so `seq` stays gapless.

Why `listen` re-listens itself: a database restart, a failover or a proxy idle timeout closes the `LISTEN` connection without an error anyone awaits; without the watcher the replica would keep serving its own connections and silently stop hearing the others. asyncpg's termination listener fires when the socket closes; the watcher releases the dead connection (the pool discards a closed one), backs off with full jitter so a fleet of replicas does not stampede a recovering database, subscribes on a fresh connection and calls `on_resume` — `Server.start` passes `Hub.announce`, so the replica's presence, work and open streams are republished at once. Nothing replays missed notifications: the next event notification for a thread gap-fills each cursor from storage, and clients resume from theirs. `on_resume` failing (the database still flapping) is swallowed; the next drop or the 10 s re-announcement covers it.

- [ ] **Step 4: Run both tiers**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Expected: PASS (Postgres parameters deselected).

Run: `RFNRY_TEST_POSTGRES_URL=postgresql://thread:thread@127.0.0.1:55436/thread uv run poe test:integration`
Expected: PASS with no skips — the storage suite (including 20 concurrent duplicates), the two-replica tests (including the 20-stream ordering test and the two re-LISTEN tests, which terminate the listener backends with `pg_terminate_backend`) and the tenancy canary, on Postgres. The URL is the `rfnry-thread-pg` container, dedicated to this repo; the role owns its backends, so it may terminate them.

- [ ] **Step 5: Commit**

```bash
git add packages/python
git commit -m "feat: PostgresStorage — scope-first tables, gapless seq under the thread lock, NOTIFY in the same transaction, re-LISTEN with backoff

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 16: The TypeScript package — toolchain, wire types, protocol, golden fixtures, `resolve`

Toolchain, chosen for the fewest moving parts that give a strict, tested, publishable ESM library with zero runtime dependencies:
- **TypeScript 6, `strict`, `tsc` as the build** — the library is plain ESM for browsers and Node ≥ 22; a bundler (the old package used tsup to emit CJS for React apps) adds a tool and a second output for no consumer we have.
- **vitest** — runs TypeScript directly, fast, the family's existing choice (`../rfnry-thread/packages/client-react`).
- **Biome** — formatter and linter in one binary with one config file, also the old package's choice (its `biome.json` is the model, minus JSX).
- `"module": "NodeNext"` so emitted imports carry `.js` and run unbundled in Node and browsers alike.

Fixture discipline adapted from `../rfnry-thread/packages/client-react/tests/protocol/fixtures.test.ts`: the TypeScript suite reads the same files the Python suite writes, and round-trips every one.

**Files:**
- Create: `packages/typescript/package.json`, `packages/typescript/tsconfig.json`, `packages/typescript/tsconfig.build.json`, `packages/typescript/biome.json`, `packages/typescript/vitest.config.ts`, `packages/typescript/README.md`
- Create: `packages/typescript/src/errors.ts`, `packages/typescript/src/types.ts`, `packages/typescript/src/protocol.ts`, `packages/typescript/src/resolve.ts`, `packages/typescript/src/index.ts`
- Test: `packages/typescript/tests/golden.test.ts`, `packages/typescript/tests/resolve.test.ts`, `packages/typescript/tests/errors.test.ts`

**Interfaces:**
- Consumes: `fixtures/protocol/*.json` (Task 4).
- Produces (TypeScript): error classes `ThreadException` (`code = 'internal'`), `NotFoundError`, `InputError`, `ConfigurationError`, `ForbiddenError`, `StorageError`, `TransportError` and `raised(code, message)`; wire types `Identity`, `Text`, `Image`, `Audio`, `Document`, `Part`, `Content`, `Thread`, `Member`, `Message`, `Step`, `Edit`, `Retract`, `Read`, `MemberAdded`, `MemberRemoved`, `ThreadUpdated`, `Custom`, `Event`, `Working`, `Idle`, `StreamStart`, `StreamDelta`, `StreamEnd`, `Cancel`, `Present`, `Left`, `Invited`, `ThreadDeleted`, `Signal`, `Joined`, `Item`; constants `EVENT_KINDS`, `SIGNAL_KINDS`; `PROTOCOL = 1`, frame types, `encode(frame)`, `decode(text)`; `resolve(events: Event[]): Event[]`.

- [ ] **Step 1: Write the toolchain files**

`packages/typescript/package.json`:

```json
{
  "name": "@rfnry/thread",
  "version": "0.1.0",
  "type": "module",
  "description": "Realtime conversation where people and AI assistants are equal participants",
  "license": "MIT",
  "engines": { "node": ">=22.12.0" },
  "exports": { ".": { "types": "./dist/index.d.ts", "default": "./dist/index.js" } },
  "files": ["dist"],
  "sideEffects": false,
  "scripts": {
    "build": "tsc -p tsconfig.build.json",
    "format": "biome format --write .",
    "check": "biome check .",
    "check:fix": "biome check --write .",
    "typecheck": "tsc --noEmit",
    "test": "vitest run --exclude 'tests/e2e/**'",
    "test:e2e": "vitest run tests/e2e"
  },
  "devDependencies": {
    "@biomejs/biome": "^2.5.2",
    "@types/node": "^25.9.4",
    "typescript": "^6.0.3",
    "vitest": "^4.1.10"
  }
}
```

`packages/typescript/tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "lib": ["ES2023", "DOM"],
    "types": ["node"],
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "exactOptionalPropertyTypes": false,
    "verbatimModuleSyntax": true,
    "skipLibCheck": true,
    "declaration": true,
    "sourceMap": true,
    "outDir": "dist"
  },
  "include": ["src", "tests"]
}
```

`packages/typescript/tsconfig.build.json`:

```json
{
  "extends": "./tsconfig.json",
  "compilerOptions": { "types": [], "rootDir": "src" },
  "include": ["src"]
}
```

`packages/typescript/biome.json`:

```json
{
  "$schema": "https://biomejs.dev/schemas/2.5.2/schema.json",
  "vcs": { "enabled": true, "clientKind": "git", "useIgnoreFile": true },
  "files": { "includes": ["src/**/*", "tests/**/*"] },
  "formatter": { "enabled": true, "indentStyle": "space", "indentWidth": 2, "lineEnding": "lf", "lineWidth": 100 },
  "assist": { "enabled": true, "actions": { "source": { "organizeImports": "on" } } },
  "linter": { "enabled": true, "rules": { "recommended": true, "style": { "noNonNullAssertion": "off" } } },
  "javascript": { "formatter": { "quoteStyle": "single", "semicolons": "asNeeded", "trailingCommas": "es5" } },
  "overrides": [
    { "includes": ["tests/**"], "linter": { "rules": { "suspicious": { "noExplicitAny": "off" } } } }
  ]
}
```

`packages/typescript/vitest.config.ts`:

```ts
import { defineConfig } from 'vitest/config'

export default defineConfig({ test: { environment: 'node' } })
```

`packages/typescript/README.md`:

```markdown
# @rfnry/thread

The TypeScript client of rfnry-thread. The contract is [`docs/thread.md`](../../docs/thread.md).
```

Run: `cd packages/typescript && npm install`
Expected: a `package-lock.json` (commit it) and `node_modules/` (ignored).

- [ ] **Step 2: Write the failing tests**

`packages/typescript/tests/errors.test.ts`:

```ts
import { describe, expect, it } from 'vitest'
import {
  ConfigurationError,
  ForbiddenError,
  InputError,
  NotFoundError,
  StorageError,
  ThreadException,
  TransportError,
  raised,
} from '../src/errors.js'

const FAMILY = [NotFoundError, InputError, ConfigurationError, ForbiddenError, StorageError, TransportError]

describe('errors', () => {
  it('maps every wire code to its class with the same message', () => {
    for (const Class of [ThreadException, ...FAMILY]) {
      const error = raised(new Class('x').code, "server: thread 'x' was not found")
      expect(error).toBeInstanceOf(Class)
      expect(error).toBeInstanceOf(ThreadException)
      expect(error.message).toBe("server: thread 'x' was not found")
      expect(error.name).toBe(Class.name)
    }
  })

  it('maps an unknown code to the root', () => {
    expect(raised('teapot', 'server: no').constructor).toBe(ThreadException)
  })
})
```

`packages/typescript/tests/golden.test.ts`:

```ts
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { decode, encode } from '../src/protocol.js'
import { EVENT_KINDS, SIGNAL_KINDS } from '../src/types.js'

const FIXTURES = join(import.meta.dirname, '../../../fixtures/protocol')
const files = readdirSync(FIXTURES).filter((name) => name.endsWith('.json'))

describe('golden fixtures', () => {
  it('exist', () => {
    expect(files.length).toBe(27)
  })

  it.each(files)('%s round-trips', (name) => {
    const text = readFileSync(join(FIXTURES, name), 'utf8')
    expect(JSON.parse(encode(decode(text)))).toEqual(JSON.parse(text))
  })

  it('cover every event and signal kind the types name', () => {
    const pushes = files
      .map((name) => JSON.parse(readFileSync(join(FIXTURES, name), 'utf8')))
      .filter((frame) => frame.type === 'push')
    const kinds = (name: string) =>
      new Set(pushes.filter((frame) => frame.name === name).map((frame) => frame.payload.kind))
    expect(kinds('event')).toEqual(new Set(EVENT_KINDS))
    expect(kinds('signal')).toEqual(new Set(SIGNAL_KINDS))
  })

  it('refuses a malformed frame', () => {
    expect(() => decode('not json')).toThrow(/^protocol: malformed frame/)
    expect(() => decode('{"type":"shout"}')).toThrow(/^protocol: malformed frame/)
  })
})
```

`packages/typescript/tests/resolve.test.ts`:

```ts
import { describe, expect, it } from 'vitest'
import { resolve } from '../src/resolve.js'
import type { Event, Identity } from '../src/types.js'

const ana: Identity = { role: 'user', id: 'ana', name: 'Ana', metadata: {} }
const base = (seq: number) => ({
  id: `e${seq}`,
  thread_id: 't1',
  seq,
  author: ana,
  created_at: '2026-01-01T12:00:00Z',
  client_id: `c${seq}`,
  in_reply_to: null,
  triggered_by: null,
})
const message = (seq: number, text: string): Event => ({
  ...base(seq),
  kind: 'message',
  content: [{ type: 'text', text }],
})

describe('resolve', () => {
  it('applies the latest edit and drops retractions and the events that carry them', () => {
    const view = resolve([
      message(3, 'c'),
      message(1, 'a'),
      { ...base(4), kind: 'edit', target: 'e1', content: [{ type: 'text', text: 'A' }] },
      { ...base(5), kind: 'edit', target: 'e1', content: [{ type: 'text', text: 'A!' }] },
      message(2, 'b'),
      { ...base(6), kind: 'retract', target: 'e2' },
    ])
    expect(view.map((event) => event.seq)).toEqual([1, 3])
    expect(view[0]).toMatchObject({ content: [{ type: 'text', text: 'A!' }] })
  })
})
```

- [ ] **Step 3: Run them to verify they fail**

Run: `cd packages/typescript && npm test`
Expected: FAIL — `Failed to load url ../src/errors.js`.

- [ ] **Step 4: Implement**

`packages/typescript/src/errors.ts`:

```ts
export class ThreadException extends Error {
  readonly code: string = 'internal'

  constructor(message: string) {
    super(message)
    this.name = new.target.name
  }
}

export class NotFoundError extends ThreadException {
  override readonly code: string = 'not_found'
}

export class InputError extends ThreadException {
  override readonly code: string = 'input'
}

export class ConfigurationError extends ThreadException {
  override readonly code: string = 'configuration'
}

export class ForbiddenError extends ThreadException {
  override readonly code: string = 'forbidden'
}

export class StorageError extends ThreadException {
  override readonly code: string = 'storage'
}

export class TransportError extends ThreadException {
  override readonly code: string = 'transport'
}

const CLASSES: Record<string, new (message: string) => ThreadException> = {
  internal: ThreadException,
  not_found: NotFoundError,
  input: InputError,
  configuration: ConfigurationError,
  forbidden: ForbiddenError,
  storage: StorageError,
  transport: TransportError,
}

export function raised(code: string, message: string): ThreadException {
  return new (CLASSES[code] ?? ThreadException)(message)
}
```

`packages/typescript/src/types.ts`:

```ts
export type Identity = {
  role: 'user' | 'assistant' | 'system'
  id: string
  name?: string | null
  metadata?: Record<string, unknown>
}

export type Text = { type: 'text'; text: string }
type Media = { media_type: string; data?: string | null; url?: string | null }
export type Image = Media & { type: 'image' }
export type Audio = Media & { type: 'audio' }
export type Document = Media & { type: 'document'; name?: string | null }
export type Part = Text | Image | Audio | Document
export type Content = string | Part[]

export type Thread = {
  id: string
  scope: string
  metadata: Record<string, unknown>
  created_at: string
}

export type Member = { thread_id: string; identity: Identity; added_at: string; added_by: Identity }

type Envelope = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
}

export type Message = Envelope & { kind: 'message'; content: Part[] }
export type Step = Envelope & {
  kind: 'step'
  name: string
  input: Record<string, unknown>
  output: Part[]
  is_error: boolean
  duration_ms: number | null
}
export type Edit = Envelope & { kind: 'edit'; target: string; content: Part[] }
export type Retract = Envelope & { kind: 'retract'; target: string }
export type Read = Envelope & { kind: 'read'; up_to_seq: number }
export type MemberAdded = Envelope & { kind: 'member_added'; identity: Identity }
export type MemberRemoved = Envelope & { kind: 'member_removed'; identity: Identity }
export type ThreadUpdated = Envelope & { kind: 'thread_updated'; metadata: Record<string, unknown> }
export type Custom = Envelope & { kind: 'custom'; name: string; payload: Record<string, unknown> }
export type Event =
  | Message
  | Step
  | Edit
  | Retract
  | Read
  | MemberAdded
  | MemberRemoved
  | ThreadUpdated
  | Custom

type Sent = { thread_id: string; sender: Identity }
export type Working = Sent & { kind: 'working'; label: string | null }
export type Idle = Sent & { kind: 'idle' }
export type StreamStart = Sent & { kind: 'stream.start'; stream_id: string; in_reply_to: string | null }
export type StreamDelta = Sent & { kind: 'stream.delta'; stream_id: string; index: number; text: string }
export type StreamEnd = Sent & { kind: 'stream.end'; stream_id: string }
export type Cancel = Sent & { kind: 'cancel'; to: Identity }
export type Present = Sent & { kind: 'present' }
export type Left = Sent & { kind: 'left' }
export type Invited = Sent & { kind: 'invited'; thread: Thread }
export type ThreadDeleted = Sent & { kind: 'thread.deleted' }
export type Signal =
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

export type Joined = { thread: Thread; seq: number; truncated: boolean }
export type Item = Event | Signal | Joined

export const EVENT_KINDS: Event['kind'][] = [
  'message',
  'step',
  'edit',
  'retract',
  'read',
  'member_added',
  'member_removed',
  'thread_updated',
  'custom',
]
export const SIGNAL_KINDS: Signal['kind'][] = [
  'working',
  'idle',
  'stream.start',
  'stream.delta',
  'stream.end',
  'cancel',
  'present',
  'left',
  'invited',
  'thread.deleted',
]
```

`packages/typescript/src/protocol.ts`:

```ts
import { InputError } from './errors.js'

export const PROTOCOL = 1
export const HELLO = 'hello'

type Payload = Record<string, unknown>
export type Hello = { type: 'hello'; protocol: number }
export type Request = { type: 'request'; id: string; method: string; payload: Payload }
export type Response = { type: 'response'; id: string; payload: Payload }
export type ErrorFrame = { type: 'error'; id: string; error: { code: string; message: string } }
export type Push = { type: 'push'; name: 'event' | 'signal'; payload: Payload }
export type Notify = { type: 'notify'; method: string; payload: Payload }
export type Frame = Hello | Request | Response | ErrorFrame | Push | Notify

const TYPES = new Set(['hello', 'request', 'response', 'error', 'push', 'notify'])

export function encode(frame: Frame): string {
  return JSON.stringify(frame)
}

export function decode(text: string): Frame {
  let frame: unknown
  try {
    frame = JSON.parse(text)
  } catch (error) {
    throw new InputError(`protocol: malformed frame: ${(error as Error).message.toLowerCase()}`)
  }
  if (typeof frame !== 'object' || frame === null || !TYPES.has((frame as { type?: string }).type ?? '')) {
    throw new InputError('protocol: malformed frame: unknown type')
  }
  return frame as Frame
}
```

`packages/typescript/src/resolve.ts`:

```ts
import type { Edit, Event } from './types.js'

export function resolve(events: Event[]): Event[] {
  const ordered = [...events].sort((a, b) => a.seq - b.seq)
  const edits = new Map<string, Edit>()
  const retracted = new Set<string>()
  for (const event of ordered) {
    if (event.kind === 'edit') edits.set(event.target, event)
    else if (event.kind === 'retract') retracted.add(event.target)
  }
  const view: Event[] = []
  for (const event of ordered) {
    if (event.kind === 'edit' || event.kind === 'retract' || retracted.has(event.id)) continue
    const edit = edits.get(event.id)
    view.push(event.kind === 'message' && edit ? { ...event, content: edit.content } : event)
  }
  return view
}
```

`packages/typescript/src/index.ts` (Task 17 adds `connect`, `Client`, `Stream`):

```ts
export {
  ConfigurationError,
  ForbiddenError,
  InputError,
  NotFoundError,
  StorageError,
  ThreadException,
  TransportError,
} from './errors.js'
export { resolve } from './resolve.js'
export type * from './types.js'
```

- [ ] **Step 5: Run the gate**

Run: `cd packages/typescript && npm run check:fix && npm run check && npm run typecheck && npm test`
Expected: PASS (27 fixtures round-trip).

- [ ] **Step 6: Commit**

```bash
git add packages/typescript
git commit -m "feat: @rfnry/thread — wire types, protocol and resolve, reading the same golden fixtures as Python

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 17: The TypeScript client — connect, reconnect, resume, outbox, subscribe, stream

The outbox is adapted from `../rfnry-thread/packages/client-react/src/outbox.ts` (per-thread promise tails, deadline, capped backoff, woken on reconnect) without its telemetry and abort ids; the reconnect loop from `../rfnry-thread/packages/client-react/src/transport/reconnect.ts` (full jitter, first attempt immediate, fresh `protocols` and a hello every attempt) without its callbacks. A browser cannot see why an upgrade failed, so a refused reconnect looks like an unreachable server and keeps retrying; only a `protocols` function that throws stops the client (design §18). Everything else of the old React package — stores, hooks, provider, codec, transcript, REST — is stripped. The shape mirrors the Python `Client` (Task 12) verb for verb.

**Files:**
- Create: `packages/typescript/src/timing.ts`, `packages/typescript/src/outbox.ts`, `packages/typescript/src/client.ts`
- Modify: `packages/typescript/src/index.ts`
- Test: `packages/typescript/tests/fakes.ts`, `packages/typescript/tests/outbox.test.ts`, `packages/typescript/tests/client.test.ts`

**Interfaces:**
- Consumes: errors, types, protocol (Task 16).
- Produces: `timing` (mutable tuning for tests: `handshakeMs`, `reconnectMs`, `reconnectCapMs`, `deadlineMs`, `backoffMs`, `backoffCapMs`); `Outbox`; `connect(options: { url: string; protocols?: string | string[] | (() => Promise<string | string[]>) }): Promise<Client>` (the subprotocol list offered with the upgrade — browsers cannot set other headers, so a token rides here or in the session cookie; a function is awaited on every attempt and one that throws is `ForbiddenError('client: the protocols function threw: …')`, which stops a reconnecting client); `Client` with `identity`, `scope`, `closed: Promise<ThreadException | null>`, `createThread(options?: { threadId?: string; metadata?: Record<string, unknown> })`, `getThread(threadId)`, `listThreads(options?: { before?: string; limit?: number })`, `updateThread(threadId, metadata)`, `deleteThread(threadId)`, `join(threadId, options?: { since?: number })`, `leave(threadId)`, `history(threadId, options?: { before?: number; limit?: number; kinds?: string[] })`, `present(threadId)`, `addMember(threadId, identity)`, `removeMember(threadId, identity)`, `listMembers(threadId)`, `send(threadId, content, options?: { inReplyTo?: string; triggeredBy?: Identity; clientId?: string })`, `stream(threadId, options?: { inReplyTo?: string; triggeredBy?: Identity })`, `step(threadId, options: { name, input?, output?, isError?, durationMs?, inReplyTo?, triggeredBy?, clientId? })`, `edit(threadId, target, content, options?: { clientId?: string })`, `retract(threadId, target, options?: { clientId?: string })`, `read(threadId, upToSeq)`, `custom(threadId, name, payload, options?: { clientId?: string })` (`clientId` as in Python: generated when left out, the dedup key when given), `working(threadId, label?)`, `idle(threadId)`, `cancel(threadId, to)`, `subscribe(listener, options?: { includeOwn?: boolean }): () => void`, `close()`; `Stream` with `id`, `text`, `write(text): void`, `finish(content?): Promise<Message>`, `abort(): Promise<void>`.

- [ ] **Step 1: Write the fake socket and the failing tests**

`packages/typescript/tests/fakes.ts`:

```ts
import { vi } from 'vitest'

type Frame = Record<string, any>
export type Handler = (payload: Frame) => Frame | Promise<Frame>

export class FakeSocket {
  readyState = 0
  onopen: ((event: unknown) => void) | null = null
  onmessage: ((event: { data: string }) => void) | null = null
  onclose: ((event: { code: number }) => void) | null = null
  onerror: ((event: unknown) => void) | null = null
  readonly sent: Frame[] = []

  constructor(
    readonly url: string,
    readonly server: FakeServer,
    protocols?: string | string[]
  ) {
    server.sockets.push(this)
    server.opened.push(protocols)
    setTimeout(() => {
      if (server.down) {
        this.readyState = 3
        this.onerror?.({})
        this.onclose?.({ code: 1006 })
        return
      }
      this.readyState = 1
      this.onopen?.({})
    }, 0)
  }

  send(text: string): void {
    if (this.readyState !== 1) throw new Error('fake: socket is not open')
    const frame = JSON.parse(text)
    this.sent.push(frame)
    setTimeout(() => void this.server.receive(this, frame), 0)
  }

  close(code = 1000, _reason?: string): void {
    if (this.readyState === 3) return
    this.readyState = 3
    setTimeout(() => this.onclose?.({ code }), 0)
  }

  deliver(frame: Frame): void {
    if (this.readyState === 1) this.onmessage?.({ data: JSON.stringify(frame) })
  }
}

export class FakeServer {
  readonly sockets: FakeSocket[] = []
  readonly handlers = new Map<string, Handler>()
  readonly hellos: Frame[] = []
  readonly opened: Array<string | string[] | undefined> = []
  readonly stored = new Map<string, Frame>()
  down = false
  dropAck = false
  seq = 0
  identity: Frame = { role: 'user', id: 'ana', name: 'Ana', metadata: {} }

  install(): this {
    const server = this
    vi.stubGlobal(
      'WebSocket',
      class extends FakeSocket {
        constructor(url: string, protocols?: string | string[]) {
          super(url, server, protocols)
        }
      }
    )
    this.handlers.set('event.send', (payload) => {
      const key = `${payload.thread_id}:${payload.client_id}`
      if (!this.stored.has(key)) {
        this.seq += 1
        this.stored.set(key, {
          ...payload,
          id: `e${this.seq}`,
          seq: this.seq,
          author: this.identity,
          created_at: '2026-01-01T12:00:00Z',
          in_reply_to: payload.in_reply_to ?? null,
          triggered_by: payload.triggered_by ?? null,
        })
      }
      return { event: this.stored.get(key) }
    })
    this.handlers.set('thread.join', (payload) => ({
      thread: { id: payload.thread_id, scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
      seq: this.seq,
      truncated: false,
    }))
    return this
  }

  get current(): FakeSocket | undefined {
    return this.sockets.filter((socket) => socket.readyState === 1).at(-1)
  }

  sent(method?: string): Frame[] {
    return this.sockets
      .flatMap((socket) => socket.sent)
      .filter((frame) => frame.type !== 'hello' && (!method || frame.method === method))
  }

  async receive(socket: FakeSocket, frame: Frame): Promise<void> {
    if (frame.type === 'hello') {
      this.hellos.push(frame)
      socket.deliver({ type: 'response', id: 'hello', payload: { identity: this.identity, scope: 'acme' } })
      return
    }
    if (frame.type !== 'request') return
    try {
      const payload = await (this.handlers.get(frame.method) ?? (() => ({})))(frame.payload)
      if (this.dropAck && frame.method === 'event.send') {
        this.dropAck = false
        socket.close(1006)
        return
      }
      socket.deliver({ type: 'response', id: frame.id, payload })
    } catch (error) {
      const { code, message } = error as { code: string; message: string }
      socket.deliver({ type: 'error', id: frame.id, error: { code, message } })
    }
  }

  push(name: 'event' | 'signal', payload: Frame): void {
    this.current?.deliver({ type: 'push', name, payload })
  }

  drop(): void {
    this.current?.close(1006)
  }
}

export const helper = { role: 'assistant', id: 'helper', name: 'Helper', metadata: {} } as const

export function message(seq: number, text: string, author: Frame = helper): Frame {
  return {
    kind: 'message',
    id: `m${seq}`,
    thread_id: 't1',
    seq,
    author,
    created_at: '2026-01-01T12:00:00Z',
    client_id: `c${seq}`,
    in_reply_to: null,
    triggered_by: null,
    content: [{ type: 'text', text }],
  }
}
```

`packages/typescript/tests/outbox.test.ts`:

```ts
import { afterEach, describe, expect, it } from 'vitest'
import { InputError, TransportError } from '../src/errors.js'
import { Outbox } from '../src/outbox.js'
import { timing } from '../src/timing.js'

const saved = { ...timing }
afterEach(() => Object.assign(timing, saved))

const hooks = { isConnected: () => true, isClosed: () => false, waitConnected: async () => {} }

describe('outbox', () => {
  it('keeps per-thread order even when an earlier write retries', async () => {
    timing.backoffMs = 1
    const order: string[] = []
    let failures = 1
    const outbox = new Outbox(async (payload) => {
      if (payload.client_id === 'a' && failures-- > 0) throw new TransportError('client: connection lost')
      order.push(String(payload.client_id))
      return { event: payload }
    }, hooks)
    await Promise.all([
      outbox.send('t1', { client_id: 'a' }),
      outbox.send('t1', { client_id: 'b' }),
    ])
    expect(order).toEqual(['a', 'b'])
  })

  it('gives up after the deadline and never retries what the server refused', async () => {
    timing.deadlineMs = 30
    timing.backoffMs = 5
    const outbox = new Outbox(async () => {
      throw new TransportError('client: not connected')
    }, hooks)
    await expect(outbox.send('t1', { client_id: 'c1' })).rejects.toThrow(
      "client: event 'c1' for thread 't1' was not acknowledged within 0.03s"
    )
    let calls = 0
    const refusing = new Outbox(async () => {
      calls += 1
      throw new InputError('server: unknown event kind')
    }, hooks)
    await expect(refusing.send('t1', { client_id: 'c2' })).rejects.toBeInstanceOf(InputError)
    expect(calls).toBe(1)
  })
})
```

`packages/typescript/tests/client.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { type Client, connect } from '../src/client.js'
import { ForbiddenError, TransportError } from '../src/errors.js'
import { timing } from '../src/timing.js'
import type { Item } from '../src/types.js'
import { FakeServer, helper, message } from './fakes.js'

const saved = { ...timing }
let server: FakeServer

beforeEach(() => {
  server = new FakeServer().install()
  Object.assign(timing, { reconnectMs: 1, backoffMs: 1 })
})

afterEach(() => {
  Object.assign(timing, saved)
  vi.unstubAllGlobals()
})

async function joined(): Promise<{ client: Client; items: Item[] }> {
  const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
  const items: Item[] = []
  client.subscribe((item) => items.push(item), { includeOwn: true })
  await client.join('t1', { since: 0 })
  return { client, items }
}

describe('client', () => {
  it('opens with the protocols and a hello, and knows who and where it is', async () => {
    const protocols = vi.fn(async () => ['thread', 'tok'])
    const client = await connect({ url: 'ws://fake', protocols })
    expect(client.identity.id).toBe('ana')
    expect(client.scope).toBe('acme')
    expect(server.opened).toEqual([['thread', 'tok']])
    expect(server.hellos).toEqual([{ type: 'hello', protocol: 1 }])
  })

  it('raises an unreachable server and a protocols function that throws', async () => {
    server.down = true
    await expect(connect({ url: 'ws://fake' })).rejects.toBeInstanceOf(TransportError)
    server.down = false
    const failing = connect({
      url: 'ws://fake',
      protocols: async () => {
        throw new Error('session expired')
      },
    })
    await expect(failing).rejects.toBeInstanceOf(ForbiddenError)
    await expect(failing).rejects.toThrow('client: the protocols function threw: session expired')
  })

  it('speaks the protocol verb for verb', async () => {
    const { client } = await joined()
    await client.createThread({ threadId: 't1', metadata: { topic: 'billing' } })
    await client.history('t1', { before: 5, limit: 2 })
    await client.working('t1', 'Reading')
    await client.cancel('t1', helper)
    const methods = server.sent().map((frame) => [frame.method, frame.payload])
    expect(methods).toContainEqual(['thread.create', { thread_id: 't1', metadata: { topic: 'billing' } }])
    expect(methods).toContainEqual(['thread.history', { thread_id: 't1', before: 5, limit: 2, kinds: null }])
    expect(methods).toContainEqual(['signal.send', { thread_id: 't1', kind: 'working', label: 'Reading' }])
    expect(methods).toContainEqual(['signal.send', { thread_id: 't1', kind: 'cancel', to: helper }])
  })

  it('hands items to listeners, leaves out its own unless asked, and drops repeats', async () => {
    const { client, items } = await joined()
    const others: Item[] = []
    const unsubscribe = client.subscribe((item) => others.push(item))
    server.push('event', message(1, 'hi'))
    server.push('event', message(1, 'hi'))
    server.push('event', message(2, 'mine', client.identity))
    server.push('signal', { kind: 'working', thread_id: 't1', sender: helper, label: 'Reading' })
    await vi.waitFor(() => expect(items).toHaveLength(3))
    expect(others.map((item) => ('kind' in item ? item.kind : 'joined'))).toEqual(['message', 'working'])
    unsubscribe()
  })

  it('sends a string as one text part and returns the stored event', async () => {
    const { client } = await joined()
    const sent = await client.send('t1', 'Can you check my invoice?', { inReplyTo: 'e0' })
    expect(sent.content).toEqual([{ type: 'text', text: 'Can you check my invoice?' }])
    expect(sent.in_reply_to).toBe('e0')
  })

  it('stores a caller-supplied clientId once and returns the stored event both times', async () => {
    const { client } = await joined()
    const first = await client.send('t1', 'Your plan renews Friday.', { clientId: 'renewal-ana-2026-10' })
    const again = await client.send('t1', 'Your plan renews Friday, again.', { clientId: 'renewal-ana-2026-10' })
    expect(again).toEqual(first)
    expect(first.client_id).toBe('renewal-ana-2026-10')
    await client.step('t1', { name: 'Read', clientId: 'read-1' })
    await client.edit('t1', first.id, 'Renews Friday.', { clientId: 'edit-1' })
    await client.retract('t1', first.id, { clientId: 'retract-1' })
    await client.custom('t1', 'rating', { stars: 5 }, { clientId: 'rating-1' })
    const ids = server.sent('event.send').map((frame) => frame.payload.client_id)
    expect(ids).toEqual(['renewal-ana-2026-10', 'renewal-ana-2026-10', 'read-1', 'edit-1', 'retract-1', 'rating-1'])
    expect(server.stored.size).toBe(5)
  })

  it('streams: start, deltas as notifies in order, the message with the stream id, then end', async () => {
    const { client } = await joined()
    const reply = client.stream('t1', { inReplyTo: 'e1' })
    reply.write('Refunded ')
    reply.write('in full.')
    const stored = await reply.finish()
    expect(stored.client_id).toBe(reply.id)
    const frames = server.sent().filter((f) => f.method === 'signal.send' || f.method === 'event.send')
    expect(frames.map((f) => (f.method === 'event.send' ? 'message' : f.payload.kind))).toEqual([
      'stream.start',
      'stream.delta',
      'stream.delta',
      'message',
      'stream.end',
    ])
    expect(frames.filter((f) => f.type === 'notify').map((f) => f.payload.text)).toEqual(['Refunded ', 'in full.'])
    const abandoned = client.stream('t1')
    abandoned.write('never mind')
    await abandoned.abort()
    expect(server.sent('event.send')).toHaveLength(1)
  })

  it('reconnects with fresh protocols, says hello again, and re-joins from the cursor', async () => {
    const protocols = vi.fn(async () => ['thread', 'tok'])
    const client = await connect({ url: 'ws://fake', protocols })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.join('t1', { since: 0 })
    server.push('event', message(4, 'before'))
    await vi.waitFor(() => expect(items).toHaveLength(1))
    server.drop()
    await vi.waitFor(() => expect(protocols).toHaveBeenCalledTimes(2))
    await vi.waitFor(() => expect(server.hellos).toHaveLength(2))
    await vi.waitFor(() => expect(items.some((item) => 'truncated' in item)).toBe(true))
    const joins = server.sent('thread.join').map((frame) => frame.payload.since)
    expect(joins).toEqual([0, 4])
  })

  it('stores a write whose ack was lost exactly once', async () => {
    const { client } = await joined()
    server.dropAck = true
    const sent = await client.send('t1', 'exactly once')
    const attempts = server.sent('event.send').map((frame) => frame.payload.client_id)
    expect(attempts).toHaveLength(2)
    expect(new Set(attempts).size).toBe(1)
    expect(server.stored.size).toBe(1)
    expect(sent.seq).toBe(1)
  })

  it('stops for good when the protocols function throws on a reconnect', async () => {
    let expired = false
    const client = await connect({
      url: 'ws://fake',
      protocols: async () => {
        if (expired) throw new Error('session expired')
        return ['thread', 'tok']
      },
    })
    await client.join('t1', { since: 0 })
    expired = true
    server.drop()
    const reason = await client.closed
    expect(reason).toBeInstanceOf(ForbiddenError)
    await expect(client.send('t1', 'late')).rejects.toBeInstanceOf(ForbiddenError)
  })

  it('refuses calls after close', async () => {
    const { client } = await joined()
    await client.close()
    expect(await client.closed).toBeNull()
    await expect(client.getThread('t1')).rejects.toThrow('client: the client is closed')
  })
})
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/typescript && npm test`
Expected: FAIL — `Failed to load url ../src/client.js`.

- [ ] **Step 3: Implement**

`packages/typescript/src/timing.ts`:

```ts
export const timing = {
  handshakeMs: 10_000,
  reconnectMs: 500,
  reconnectCapMs: 30_000,
  deadlineMs: 60_000,
  backoffMs: 500,
  backoffCapMs: 8_000,
}
```

`packages/typescript/src/outbox.ts`:

```ts
import { TransportError } from './errors.js'
import { timing } from './timing.js'

type Payload = Record<string, unknown>

export type OutboxHooks = {
  isConnected(): boolean
  isClosed(): boolean
  waitConnected(ms: number): Promise<void>
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

export class Outbox {
  private readonly tails = new Map<string, Promise<unknown>>()

  constructor(
    private readonly deliver: (payload: Payload) => Promise<Payload>,
    private readonly hooks: OutboxHooks
  ) {}

  send(threadId: string, payload: Payload): Promise<Payload> {
    const previous = this.tails.get(threadId) ?? Promise.resolve()
    const run = () => this.attempt(threadId, payload)
    const result = previous.then(run, run)
    this.tails.set(
      threadId,
      result.catch(() => undefined)
    )
    return result
  }

  private async attempt(threadId: string, payload: Payload): Promise<Payload> {
    const started = Date.now()
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.deliver(payload)
      } catch (error) {
        if (!(error instanceof TransportError) || this.hooks.isClosed()) throw error
        const elapsed = Date.now() - started
        if (elapsed >= timing.deadlineMs) {
          throw new TransportError(
            `client: event '${String(payload.client_id)}' for thread '${threadId}' was not acknowledged within ${timing.deadlineMs / 1000}s`
          )
        }
        const delay = Math.min(timing.backoffMs * 2 ** attempt, timing.backoffCapMs, timing.deadlineMs - elapsed)
        await (this.hooks.isConnected() ? sleep(delay) : this.hooks.waitConnected(delay))
      }
    }
  }
}
```

`packages/typescript/src/client.ts`:

```ts
import {
  ConfigurationError,
  ForbiddenError,
  InputError,
  type ThreadException,
  TransportError,
  raised,
} from './errors.js'
import { Outbox } from './outbox.js'
import { type Frame, HELLO, PROTOCOL, decode, encode } from './protocol.js'
import { timing } from './timing.js'
import type {
  Content,
  Custom,
  Edit,
  Event,
  Identity,
  Item,
  Joined,
  Member,
  Message,
  Part,
  Read,
  Retract,
  Step,
  Thread,
} from './types.js'

type Payload = Record<string, unknown>
export type Protocols = string | string[]
export type ConnectOptions = { url: string; protocols?: Protocols | (() => Promise<Protocols>) }
export type Listener = (item: Item) => void
export type StreamOptions = { inReplyTo?: string; triggeredBy?: Identity }
export type SendOptions = StreamOptions & { clientId?: string }
export type WriteOptions = { clientId?: string }
export type StepOptions = SendOptions & {
  name: string
  input?: Record<string, unknown>
  output?: Part[]
  isError?: boolean
  durationMs?: number
}

const parts = (content: Content): Part[] => (typeof content === 'string' ? [{ type: 'text', text: content }] : content)
const newId = (): string => crypto.randomUUID().replaceAll('-', '')
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

function open(url: string, protocols: Protocols | undefined): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    let socket: WebSocket
    try {
      socket = new WebSocket(url, protocols)
    } catch (error) {
      reject(new TransportError(`client: could not connect: ${String(error)}`))
      return
    }
    socket.onopen = () => resolve(socket)
    socket.onerror = () => reject(new TransportError(`client: could not connect to ${url}`))
  })
}

function handshake(socket: WebSocket): Promise<Payload> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new TransportError('client: the hello was not answered')), timing.handshakeMs)
    socket.onmessage = (message) => {
      clearTimeout(timer)
      let frame: Frame
      try {
        frame = decode(String(message.data))
      } catch (error) {
        reject(error)
        return
      }
      if (frame.type === 'error' && frame.id === HELLO) reject(raised(frame.error.code, frame.error.message))
      else if (frame.type === 'response' && frame.id === HELLO) resolve(frame.payload)
      else reject(new TransportError('client: the server did not answer the hello'))
    }
    socket.onclose = () => {
      clearTimeout(timer)
      reject(new TransportError('client: the hello was not answered'))
    }
    socket.send(encode({ type: 'hello', protocol: PROTOCOL }))
  })
}

type Wire = {
  request(method: string, payload: Payload): Promise<Payload>
  notify(method: string, payload: Payload): void
  message(threadId: string, content: Content, options: StreamOptions, clientId: string): Promise<Message>
}

export class Stream {
  readonly id = newId()
  text = ''
  private index = 0
  private ended = false
  private readonly started: Promise<unknown>

  constructor(
    private readonly wire: Wire,
    private readonly threadId: string,
    private readonly options: StreamOptions
  ) {
    this.started = wire.request('signal.send', {
      thread_id: threadId,
      kind: 'stream.start',
      stream_id: this.id,
      in_reply_to: options.inReplyTo ?? null,
    })
    this.started.catch(() => undefined)
  }

  write(text: string): void {
    this.text += text
    const index = this.index++
    void this.started.then(
      () =>
        this.wire.notify('signal.send', {
          thread_id: this.threadId,
          kind: 'stream.delta',
          stream_id: this.id,
          index,
          text,
        }),
      () => undefined
    )
  }

  async finish(content?: Content): Promise<Message> {
    if (this.ended) throw new InputError(`client: stream '${this.id}' has already ended`)
    this.ended = true
    await this.started.catch(() => undefined)
    const message = await this.wire.message(this.threadId, content ?? this.text, this.options, this.id)
    await this.end()
    return message
  }

  async abort(): Promise<void> {
    if (this.ended) return
    this.ended = true
    await this.started.catch(() => undefined)
    await this.end()
  }

  private async end(): Promise<void> {
    await this.wire
      .request('signal.send', { thread_id: this.threadId, kind: 'stream.end', stream_id: this.id })
      .catch(() => undefined)
  }
}

export class Client {
  identity!: Identity
  scope!: string
  readonly closed: Promise<ThreadException | null>
  private settle!: (reason: ThreadException | null) => void
  private socket: WebSocket | null = null
  private readonly pending = new Map<string, { resolve: (payload: Payload) => void; reject: (error: Error) => void }>()
  private nextId = 1
  private readonly listeners = new Map<Listener, boolean>()
  private readonly cursors = new Map<string, number>()
  private stopped = false
  private failure: ThreadException | null = null
  private waiting: Array<() => void> = []
  private readonly outbox: Outbox
  private readonly wire: Wire

  constructor(private readonly options: ConnectOptions) {
    this.closed = new Promise((resolve) => {
      this.settle = resolve
    })
    this.outbox = new Outbox((payload) => this.request('event.send', payload), {
      isConnected: () => this.socket !== null,
      isClosed: () => this.stopped,
      waitConnected: (ms) =>
        new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, ms)
          this.waiting.push(() => {
            clearTimeout(timer)
            resolve()
          })
        }),
    })
    this.wire = {
      request: (method, payload) => this.request(method, payload),
      notify: (method, payload) => this.notify(method, payload),
      message: (threadId, content, options, clientId) => this.message(threadId, content, options, clientId),
    }
  }

  async establish(): Promise<void> {
    const socket = await open(this.options.url, await this.offered())
    let reply: Payload
    try {
      reply = await handshake(socket)
    } catch (error) {
      socket.close()
      throw error
    }
    this.identity = reply.identity as Identity
    this.scope = String(reply.scope)
    this.socket = socket
    socket.onmessage = (message) => this.receive(String(message.data))
    socket.onclose = () => this.lost(socket)
    for (const wake of this.waiting.splice(0)) wake()
  }

  private async offered(): Promise<Protocols | undefined> {
    const { protocols } = this.options
    if (typeof protocols !== 'function') return protocols
    try {
      return await protocols()
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error)
      throw new ForbiddenError(`client: the protocols function threw: ${reason}`)
    }
  }

  private receive(text: string): void {
    let frame: Frame
    try {
      frame = decode(text)
    } catch {
      return
    }
    if (frame.type === 'response' || frame.type === 'error') {
      const waiter = this.pending.get(frame.id)
      if (!waiter) return
      this.pending.delete(frame.id)
      if (frame.type === 'error') waiter.reject(raised(frame.error.code, frame.error.message))
      else waiter.resolve(frame.payload)
      return
    }
    if (frame.type !== 'push') return
    if (frame.name === 'event') {
      const event = frame.payload as unknown as Event
      const cursor = this.cursors.get(event.thread_id)
      if (cursor !== undefined) {
        if (event.seq <= cursor) return
        this.cursors.set(event.thread_id, event.seq)
      }
      if (event.kind === 'member_removed' && event.identity.id === this.identity.id) this.cursors.delete(event.thread_id)
      this.emit(event)
      return
    }
    const signal = frame.payload as unknown as Item
    if ('kind' in signal && signal.kind === 'thread.deleted') this.cursors.delete(signal.thread_id)
    this.emit(signal)
  }

  private emit(item: Item): void {
    const who = 'author' in item ? item.author : 'sender' in item ? item.sender : null
    const own = who !== null && who.id === this.identity.id
    for (const [listener, includeOwn] of this.listeners) {
      if (!includeOwn && own) continue
      try {
        listener(item)
      } catch (error) {
        queueMicrotask(() => {
          throw error
        })
      }
    }
  }

  private lost(socket: WebSocket): void {
    if (this.socket !== socket) return
    this.socket = null
    for (const waiter of this.pending.values()) waiter.reject(new TransportError('client: connection lost'))
    this.pending.clear()
    if (!this.stopped) void this.reconnect()
  }

  private async reconnect(): Promise<void> {
    for (let attempt = 0; !this.stopped; attempt++) {
      const ceiling = attempt === 0 ? 0 : Math.min(timing.reconnectCapMs, timing.reconnectMs * 2 ** (attempt - 1))
      await sleep(Math.random() * ceiling)
      if (this.stopped) return
      try {
        await this.establish()
      } catch (error) {
        if (error instanceof ForbiddenError || error instanceof ConfigurationError) {
          this.fail(error)
          return
        }
        continue
      }
      await this.rejoin()
      return
    }
  }

  private async rejoin(): Promise<void> {
    for (const [threadId, cursor] of [...this.cursors]) {
      try {
        this.emit(await this.joinFrom(threadId, cursor))
      } catch (error) {
        if (error instanceof TransportError) return
        this.cursors.delete(threadId)
      }
    }
  }

  private fail(error: ThreadException): void {
    this.failure = error
    this.stopped = true
    this.settle(error)
  }

  private request(method: string, payload: Payload): Promise<Payload> {
    if (this.stopped) return Promise.reject(this.failure ?? new TransportError('client: the client is closed'))
    const socket = this.socket
    if (!socket) return Promise.reject(new TransportError('client: not connected'))
    const id = String(this.nextId++)
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      try {
        socket.send(encode({ type: 'request', id, method, payload }))
      } catch (error) {
        this.pending.delete(id)
        reject(new TransportError(`client: ${method} failed: ${String(error)}`))
      }
    })
  }

  private notify(method: string, payload: Payload): void {
    try {
      this.socket?.send(encode({ type: 'notify', method, payload }))
    } catch {
      return
    }
  }

  private async event(threadId: string, fields: Payload, clientId?: string): Promise<Event> {
    const reply = await this.outbox.send(threadId, { thread_id: threadId, client_id: clientId ?? newId(), ...fields })
    return reply.event as Event
  }

  private async message(threadId: string, content: Content, options: StreamOptions, clientId?: string): Promise<Message> {
    const fields = {
      kind: 'message',
      content: parts(content),
      in_reply_to: options.inReplyTo ?? null,
      triggered_by: options.triggeredBy ?? null,
    }
    return (await this.event(threadId, fields, clientId)) as Message
  }

  private async joinFrom(threadId: string, since: number | null): Promise<Joined> {
    this.cursors.set(threadId, since ?? 0)
    const joined = (await this.request('thread.join', { thread_id: threadId, since })) as unknown as Joined
    this.cursors.set(threadId, Math.max(this.cursors.get(threadId) ?? 0, joined.seq))
    return joined
  }

  async createThread(options: { threadId?: string; metadata?: Record<string, unknown> } = {}): Promise<Thread> {
    const reply = await this.request('thread.create', { thread_id: options.threadId ?? null, metadata: options.metadata ?? {} })
    return reply.thread as Thread
  }

  async getThread(threadId: string): Promise<Thread> {
    return (await this.request('thread.get', { thread_id: threadId })).thread as Thread
  }

  async listThreads(options: { before?: string; limit?: number } = {}): Promise<Thread[]> {
    const reply = await this.request('thread.list', { before: options.before ?? null, limit: options.limit ?? 50 })
    return reply.threads as Thread[]
  }

  async updateThread(threadId: string, metadata: Record<string, unknown>): Promise<Thread> {
    return (await this.request('thread.update', { thread_id: threadId, metadata })).thread as Thread
  }

  async deleteThread(threadId: string): Promise<void> {
    await this.request('thread.delete', { thread_id: threadId })
    this.cursors.delete(threadId)
  }

  async join(threadId: string, options: { since?: number } = {}): Promise<Joined> {
    try {
      return await this.joinFrom(threadId, options.since ?? null)
    } catch (error) {
      this.cursors.delete(threadId)
      throw error
    }
  }

  async leave(threadId: string): Promise<void> {
    this.cursors.delete(threadId)
    await this.request('thread.leave', { thread_id: threadId })
  }

  async history(threadId: string, options: { before?: number; limit?: number; kinds?: string[] } = {}): Promise<Event[]> {
    const reply = await this.request('thread.history', {
      thread_id: threadId,
      before: options.before ?? null,
      limit: options.limit ?? 50,
      kinds: options.kinds ?? null,
    })
    return reply.events as Event[]
  }

  async present(threadId: string): Promise<Identity[]> {
    return (await this.request('thread.present', { thread_id: threadId })).identities as Identity[]
  }

  async addMember(threadId: string, identity: Identity): Promise<Member> {
    return (await this.request('member.add', { thread_id: threadId, identity })).member as Member
  }

  async removeMember(threadId: string, identity: Identity): Promise<void> {
    await this.request('member.remove', { thread_id: threadId, identity_id: identity.id })
  }

  async listMembers(threadId: string): Promise<Member[]> {
    return (await this.request('member.list', { thread_id: threadId })).members as Member[]
  }

  send(threadId: string, content: Content, options: SendOptions = {}): Promise<Message> {
    return this.message(threadId, content, options, options.clientId)
  }

  stream(threadId: string, options: StreamOptions = {}): Stream {
    return new Stream(this.wire, threadId, options)
  }

  async step(threadId: string, options: StepOptions): Promise<Step> {
    return (await this.event(threadId, {
      kind: 'step',
      name: options.name,
      input: options.input ?? {},
      output: options.output ?? [],
      is_error: options.isError ?? false,
      duration_ms: options.durationMs ?? null,
      in_reply_to: options.inReplyTo ?? null,
      triggered_by: options.triggeredBy ?? null,
    }, options.clientId)) as Step
  }

  async edit(threadId: string, target: string, content: Content, options: WriteOptions = {}): Promise<Edit> {
    return (await this.event(threadId, { kind: 'edit', target, content: parts(content) }, options.clientId)) as Edit
  }

  async retract(threadId: string, target: string, options: WriteOptions = {}): Promise<Retract> {
    return (await this.event(threadId, { kind: 'retract', target }, options.clientId)) as Retract
  }

  async read(threadId: string, upToSeq: number): Promise<Read> {
    return (await this.event(threadId, { kind: 'read', up_to_seq: upToSeq })) as Read
  }

  async custom(threadId: string, name: string, payload: Record<string, unknown>, options: WriteOptions = {}): Promise<Custom> {
    return (await this.event(threadId, { kind: 'custom', name, payload }, options.clientId)) as Custom
  }

  async working(threadId: string, label?: string): Promise<void> {
    await this.request('signal.send', { thread_id: threadId, kind: 'working', label: label ?? null })
  }

  async idle(threadId: string): Promise<void> {
    await this.request('signal.send', { thread_id: threadId, kind: 'idle' })
  }

  async cancel(threadId: string, to: Identity): Promise<void> {
    await this.request('signal.send', { thread_id: threadId, kind: 'cancel', to })
  }

  subscribe(listener: Listener, options: { includeOwn?: boolean } = {}): () => void {
    this.listeners.set(listener, options.includeOwn ?? false)
    return () => {
      this.listeners.delete(listener)
    }
  }

  async close(): Promise<void> {
    if (this.stopped && this.socket === null) return
    this.stopped = true
    const socket = this.socket
    this.socket = null
    for (const waiter of this.pending.values()) waiter.reject(new TransportError('client: the client is closed'))
    this.pending.clear()
    socket?.close(1000, 'client: closed')
    this.settle(null)
  }
}

export async function connect(options: ConnectOptions): Promise<Client> {
  const client = new Client(options)
  await client.establish()
  return client
}
```

`packages/typescript/src/index.ts` gains:

```ts
export { Client, connect, Stream } from './client.js'
```

The option types (`ConnectOptions`, `SendOptions`, …) stay unexported: every public name must appear in `docs/thread.md` (Task 18 pins it), and a caller writes them inline.

Notes for the implementer:
- `establish()` is public only because `connect` calls it; it is not documented and not exported from `index.ts` beyond the class.
- The fake socket assigns `onopen`/`onmessage`/`onclose`/`onerror` as properties, which is exactly what the client uses; do not switch the client to `addEventListener` without switching the fake.
- `clientId` goes to `event()` as given, or `newId()` makes one; the fake's store keys on `thread_id:client_id` like the server's dedup, so "stores a caller-supplied clientId once…" sees the first stored frame come back for the repeat. A stream keeps `client_id = stream.id` and takes no `clientId`, hence `StreamOptions`.
- In "stores a write whose ack was lost exactly once", the fake closes the socket instead of answering; the outbox retries with the same `client_id` after the reconnect and the fake's store answers with the stored event.
- `offered()` wraps a throwing `protocols` function in a `ForbiddenError`, which `reconnect` treats like any refusal: it stops the client and settles `closed` with it.

- [ ] **Step 4: Run the gate**

Run: `cd packages/typescript && npm run check:fix && npm run check && npm run typecheck && npm test && npm run build`
Expected: PASS; `dist/index.js` and `dist/index.d.ts` exist.

- [ ] **Step 5: Commit**

```bash
git add packages/typescript
git commit -m "feat: the TypeScript client — connect, reconnect with jitter, resume, the outbox, subscribe, streams

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 18: Public names, the docs pin, the docs examples

Written fresh, on the engine's model (`../engine/packages/python/tests/unit/test_public_api.py`, `test_docs.py`, `test_docs_examples.py`).

**Files:**
- Modify: `packages/python/src/rfnry_thread/__init__.py`, `packages/typescript/src/index.ts` (already complete after Task 17; the test pins it)
- Test: `packages/python/tests/unit/test_public_api.py`, `packages/python/tests/unit/test_docs.py`, `packages/python/tests/unit/test_docs_examples.py`, `packages/typescript/tests/docs.test.ts`

**Interfaces:**
- Consumes: every public name from Tasks 1–17; `docs/thread.md`.
- Produces: `rfnry_thread.__all__` — exactly the names in `docs/thread.md` § At a glance.

- [ ] **Step 1: Write the failing tests**

`packages/python/tests/unit/test_public_api.py`:

```python
import subprocess
import sys

import rfnry_thread

EXPECTED = {
    "Server", "Connection",
    "Client", "Stream", "Joined",
    "Identity", "Thread", "Member", "Text", "Image", "Audio", "Document", "Part", "Content",
    "Message", "Step", "Edit", "Retract", "Read", "MemberAdded", "MemberRemoved", "ThreadUpdated", "Custom", "Event",
    "resolve",
    "Working", "Idle", "StreamStart", "StreamDelta", "StreamEnd", "Cancel", "Present", "Left", "Invited",
    "ThreadDeleted", "Signal",
    "ThreadTools", "Tool", "Tools",
    "Storage", "InMemoryStorage", "PostgresStorage",
    "ThreadException", "NotFoundError", "InputError", "ConfigurationError", "ForbiddenError", "StorageError",
    "TransportError",
}

CLIENT = {
    "connect", "create_thread", "get_thread", "list_threads", "update_thread", "delete_thread", "join", "leave",
    "history", "present", "add_member", "remove_member", "list_members", "send", "stream", "step", "edit",
    "retract", "read", "custom", "working", "idle", "cancel", "events", "close",
}

SERVER = {"serve", "connect", "start", "stop", "create_thread", "add_member", "delete_thread", "delete_scope", "verify_absent"}


def test_public_names_are_exactly_these() -> None:
    assert set(rfnry_thread.__all__) == EXPECTED
    for name in EXPECTED:
        assert getattr(rfnry_thread, name) is not None


def test_the_client_and_the_server_show_their_verbs_and_hide_the_rest() -> None:
    assert {n for n in vars(rfnry_thread.Client) if not n.startswith("_")} == CLIENT
    assert {n for n in vars(rfnry_thread.Server) if not n.startswith("_")} == SERVER


def test_importing_the_package_imports_no_driver_and_no_framework() -> None:
    probe = "import rfnry_thread, sys; print(sorted(m for m in sys.modules if m.split('.')[0] in {'asyncpg', 'fastapi', 'starlette'}))"
    assert subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True).stdout.strip() == "[]"
```

`packages/python/tests/unit/test_docs.py`:

```python
import re
from pathlib import Path

import rfnry_thread

from .test_public_api import CLIENT

ROOT = Path(__file__).parents[4]
DOCS = ROOT / "docs" / "thread.md"
TYPESCRIPT = ROOT / "packages" / "typescript" / "src" / "index.ts"


def test_every_public_name_is_documented() -> None:
    text = DOCS.read_text(encoding="utf-8")
    assert [name for name in rfnry_thread.__all__ if f"`{name}`" not in text and f"{name}(" not in text] == []


def test_every_client_verb_is_documented() -> None:
    text = DOCS.read_text(encoding="utf-8")
    assert [verb for verb in CLIENT if f".{verb}(" not in text and f"`{verb}`" not in text] == []


def test_every_typescript_export_is_documented() -> None:
    text = DOCS.read_text(encoding="utf-8")
    exported = re.findall(r"export \{([^}]*)\} from", TYPESCRIPT.read_text(encoding="utf-8"))
    names = {name.strip().removeprefix("type ") for group in exported for name in group.split(",") if name.strip()}
    assert sorted(name for name in names if name not in text) == []
```

`packages/python/tests/unit/test_docs_examples.py`:

```python
import ast
import inspect
import re
from pathlib import Path
from typing import Any

import pytest

import rfnry_thread
from rfnry_thread import Identity, InMemoryStorage, Server

DOCS = Path(__file__).parents[4] / "docs" / "thread.md"
BLOCKS: list[tuple[str, str]] = re.findall(r"```(\w*)\n(.*?)```", DOCS.read_text(encoding="utf-8"), re.S)
PYTHON = [body for lang, body in BLOCKS if lang == "python"]

ILLUSTRATIVE = {
    "app = FastAPI()": "needs a web framework; the examples serve a socket with websockets",
    'remote = await Client.connect("wss://app.example.com/thread", headers={"Authorization": f"Bearer {token}"})': "needs a live socket; test_socket.py covers it",
    "async for item in helper.events():": "reads until the stream ends; test_client.py covers the pattern",
    "server = Server(PostgresStorage(pool))": "needs Postgres; the integration tier covers it",
    'run = await engine.run("support", scope="acme", input=message.content,': "needs rfnry-engine; examples/engine_agent_in_a_thread runs it",
    "pool = await asyncpg.create_pool(dsn)": "needs Postgres; the integration tier covers it",
}


def first_line(block: str) -> str:
    return block.splitlines()[0]


async def now(args: dict[str, Any]) -> str:
    return "2026-09-24T12:00:00Z"


async def context() -> dict[str, Any]:
    server = Server(InMemoryStorage())
    ana = await server.connect(Identity(role="user", id="ana", name="Ana"), scope="acme")
    helper = await server.connect(Identity(role="assistant", id="helper", name="Helper"), scope="acme")
    thread = await ana.create_thread(thread_id="support-ana")
    await ana.add_member(thread.id, helper.identity)
    await ana.join(thread.id)
    await helper.join(thread.id)
    message = await ana.send(thread.id, "My invoice was charged twice.")
    return {
        **{name: getattr(rfnry_thread, name) for name in rfnry_thread.__all__},
        "server": server, "ana": ana, "helper": helper, "thread": thread, "message": message,
        "storage": InMemoryStorage(), "now": now,
        "wav": b"RIFF", "url": "https://files.example.com/receipt.png",
    }


def runnable() -> list[Any]:
    return [pytest.param(block, id=first_line(block)[:60]) for block in PYTHON if first_line(block) not in ILLUSTRATIVE]


def test_every_named_block_is_in_the_docs_and_first_lines_are_unique() -> None:
    lines = [first_line(block) for block in PYTHON]
    assert len(lines) == len(set(lines))
    assert set(ILLUSTRATIVE) <= set(lines)


@pytest.mark.parametrize("block", runnable())
async def test_the_block_runs(block: str) -> None:
    namespace = await context()
    code = compile(block, "<docs/thread.md>", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
    result = eval(code, namespace)
    if inspect.iscoroutine(result):
        await result
    for name in ("ana", "helper"):
        await namespace[name].close()


@pytest.mark.parametrize("block", [b for b in PYTHON if first_line(b) in ILLUSTRATIVE], ids=first_line)
def test_an_illustrative_block_still_compiles(block: str) -> None:
    compile(block, "<docs/thread.md>", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
```

`packages/typescript/tests/docs.test.ts`:

```ts
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { Client, Stream } from '../src/client.js'
import * as api from '../src/index.js'

const docs = readFileSync(join(import.meta.dirname, '../../../docs/thread.md'), 'utf8')
const blocks = [...docs.matchAll(/```ts\n([\s\S]*?)```/g)].map((match) => match[1] ?? '')
const section = docs.split('### The TypeScript client')[1]?.split('\n## ')[0] ?? ''
const NOT_VERBS = new Set(['connect', 'resolve', 'thread_id', 'in_reply_to', 'includeOwn', 'null', 'protocols'])

describe('docs', () => {
  it('names only verbs the client has', () => {
    const used = blocks.flatMap((block) => [...block.matchAll(/client\.(\w+)\(/g)].map((m) => m[1]))
    const listed = [...section.matchAll(/`(\w+)`/g)].map((m) => m[1]).filter((name) => /^[a-z]/.test(name ?? ''))
    const verbs = new Set([...used, ...listed].filter((name) => !NOT_VERBS.has(name ?? '')))
    for (const verb of verbs) expect(typeof (Client.prototype as unknown as Record<string, unknown>)[verb ?? ''], verb).toBe('function')
  })

  it('names only stream methods the stream has', () => {
    const used = blocks.flatMap((block) => [...block.matchAll(/reply\.(\w+)\(/g)].map((m) => m[1]))
    for (const name of used) expect(typeof (Stream.prototype as unknown as Record<string, unknown>)[name ?? '']).toBe('function')
  })

  it('imports only what the package exports', () => {
    const imported = blocks.flatMap((block) =>
      [...block.matchAll(/import \{([^}]*)\} from '@rfnry\/thread'/g)].flatMap((m) => (m[1] ?? '').split(',').map((s) => s.trim()))
    )
    for (const name of imported) expect(api, name).toHaveProperty(name)
  })
})
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd packages/python && uv run pytest tests/unit/test_public_api.py tests/unit/test_docs.py tests/unit/test_docs_examples.py -q`
Expected: FAIL — `AttributeError: module 'rfnry_thread' has no attribute '__all__'`.

- [ ] **Step 3: Implement**

`packages/python/src/rfnry_thread/__init__.py`:

```python
from rfnry_thread.client import Client, Stream
from rfnry_thread.connection import Connection
from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    NotFoundError,
    StorageError,
    ThreadException,
    TransportError,
)
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import (
    Audio,
    Cancel,
    Content,
    Custom,
    Document,
    Edit,
    Event,
    Identity,
    Idle,
    Image,
    Invited,
    Joined,
    Left,
    Member,
    MemberAdded,
    MemberRemoved,
    Message,
    Part,
    Present,
    Read,
    Retract,
    Signal,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Thread,
    ThreadDeleted,
    ThreadUpdated,
    Working,
)
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.resolve import resolve
from rfnry_thread.server import Server
from rfnry_thread.storage import Storage
from rfnry_thread.tools import ThreadTools, Tool, Tools

__version__ = "0.1.0"

__all__ = [
    "Server", "Connection", "Client", "Stream", "Joined", "Identity", "Thread", "Member", "Text", "Image", "Audio",
    "Document", "Part", "Content", "Message", "Step", "Edit", "Retract", "Read", "MemberAdded", "MemberRemoved",
    "ThreadUpdated", "Custom", "Event", "resolve", "Working", "Idle", "StreamStart", "StreamDelta", "StreamEnd",
    "Cancel", "Present", "Left", "Invited", "ThreadDeleted", "Signal", "ThreadTools", "Tool", "Tools", "Storage",
    "InMemoryStorage", "PostgresStorage", "ThreadException", "NotFoundError", "InputError", "ConfigurationError",
    "ForbiddenError", "StorageError", "TransportError",
]
```

If a docs block fails, fix the library when the docs describe the design, and fix the docs only when the block is wrong about the design (then say so in the commit). A block whose meaning cannot be tested offline goes in `ILLUSTRATIVE` with its reason; nothing else does.

- [ ] **Step 4: Run both gates**

Run: `cd packages/python && uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`
Run: `cd packages/typescript && npm run check && npm run typecheck && npm test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/python packages/typescript docs/thread.md
git commit -m "test: the public surface is exactly the documented one, and every docs example runs

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 19: Examples

Standalone projects in the engine's shape (`../engine/examples/README.md`): each folder is a uv project whose `main.py` reads top to bottom, with a `pyproject.toml` showing how few dependencies it needs. A folder that uses a model carries `providers.py` copied **byte for byte** from `../engine/examples/image_question/providers.py` (it holds `AnthropicProvider` and `GoogleProvider`; the class hashes must match the engine's), and runs the model through rfnry-engine — rfnry-thread ships no model. Keys come from the family's key file, never from the repository:

```bash
cd examples/<name>
uv run --env-file ../../../.env.examples main.py
```

Every Python example serves the WebSocket with the `websockets` package (already a dependency of rfnry-thread) through the same five-line adapter, because a consumer's framework is theirs. The same package plays the consumer's part in authentication: `process_request` refuses an upgrade without a known token (HTTP 401), and the handler hands `serve` the identity it read. Examples that never open a socket have no one to authenticate and build `Server(InMemoryStorage())`:

```python
class Socket:
    def __init__(self, socket):
        self.socket = socket

    async def receive_text(self):
        return await self.socket.recv()

    async def send_text(self, data):
        await self.socket.send(data)

    async def close(self, code=1000, reason=None):
        await self.socket.close(code, reason or "")
```

**Files:**
- Create: `examples/README.md`
- Create per folder: `pyproject.toml`, `main.py`, and where named `providers.py`, `agents/…`
  - `examples/streaming_with_steps/` (no model)
  - `examples/proactive_assistant/` (no model, real socket)
  - `examples/person_and_assistant/` (engine + `AnthropicProvider`, real socket, terminal person)
  - `examples/two_assistants/` (engine + `AnthropicProvider` and `GoogleProvider`, in-process)
  - `examples/engine_agent_in_a_thread/` (engine + `AnthropicProvider`, `ThreadTools`, steps mirrored from the run)
  - `examples/two_replicas/` (Postgres, two servers on two ports)
  - `examples/typescript_client/` (`serve.py` + `package.json` + `main.mjs`)

**Interfaces:**
- Consumes: the whole public surface of both packages; rfnry-engine's `Engine`, `TextDelta`, `ToolCallStarted` and run records (`run.steps`: `tool`, `args`, `result`, `is_error`, `duration_ms`).

- [ ] **Step 1: The shared `pyproject.toml` shape**

For a folder without a model (swap the name):

```toml
[project]
name = "streaming_with_steps"
version = "0"
requires-python = ">=3.12"
dependencies = ["rfnry-thread"]

[dependency-groups]
dev = ["ruff"]

[tool.uv]
package = false

[tool.uv.sources]
rfnry-thread = { path = "../../packages/python", editable = true }

[tool.ruff]
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "W", "B", "UP", "SIM"]
```

A folder with a model adds `"rfnry-engine"` and the vendor SDKs its `providers.py` imports (`"anthropic"`, `"google-genai"` — check the engine folder's `pyproject.toml` for the exact names), `rfnry-engine = { path = "../../../engine/packages/python", editable = true }` under `[tool.uv.sources]`, and `known-local-folder = ["providers"]` under `[tool.ruff.lint.isort]`. `two_replicas` adds `"rfnry-thread[postgres]"` instead of `"rfnry-thread"`.

- [ ] **Step 2: `streaming_with_steps/main.py`**

```python
import asyncio

from rfnry_thread import (
    Identity,
    InMemoryStorage,
    Message,
    Server,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Working,
    resolve,
)


async def main() -> None:
    server = Server(InMemoryStorage())
    ana = await server.connect(Identity(role="user", id="ana", name="Ana"), scope="demo")
    helper = await server.connect(Identity(role="assistant", id="helper", name="Helper"), scope="demo")
    thread = await ana.create_thread(thread_id="refund")
    await ana.add_member(thread.id, helper.identity)
    await ana.join(thread.id)
    await helper.join(thread.id)

    question = await ana.send(thread.id, "My invoice was charged twice.")
    await helper.working(thread.id, label="Reading rules/refunds.md")
    await helper.step(thread.id, name="Read", input={"path": "rules/refunds.md"},
                      output=[Text(text="A double charge is refunded in full.")], duration_ms=12,
                      in_reply_to=question.id)
    async with helper.stream(thread.id, in_reply_to=question.id) as reply:
        for word in "The second charge is refunded in full, per the refunds rule.".split():
            await reply.write(word + " ")
            await asyncio.sleep(0.05)
        await reply.finish(reply.text.strip())
    await helper.idle(thread.id)

    async for item in ana.events():
        match item:
            case Working(label=label):
                print(f"[helper is working: {label}]")
            case Step(name=name, duration_ms=ms):
                print(f"[step {name} · {ms} ms]")
            case StreamStart():
                print("helper: ", end="")
            case StreamDelta(text=text):
                print(text, end="", flush=True)
            case Message(author=author) if author.role == "assistant":
                print(f"\n[stored as event {item.seq}, client_id {item.client_id}]")
            case StreamEnd():
                break

    print("\nthe log, resolved:")
    for event in resolve(await ana.history(thread.id)):
        under = " (under the question)" if event.in_reply_to == question.id else ""
        print(f"  {event.seq} {event.kind}{under}")
    await ana.close()
    await helper.close()


asyncio.run(main())
```

- [ ] **Step 3: `proactive_assistant/main.py`**

```python
import asyncio

from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server, resolve
from websockets.asyncio.server import serve

PEOPLE = {"ana-token": Identity(role="user", id="ana", name="Ana")}


class Socket:
    def __init__(self, socket):
        self.socket = socket

    async def receive_text(self):
        return await self.socket.recv()

    async def send_text(self, data):
        await self.socket.send(data)

    async def close(self, code=1000, reason=None):
        await self.socket.close(code, reason or "")


def who(request):
    return PEOPLE.get(request.headers.get("Authorization", "").removeprefix("Bearer "))


def upgrade(connection, request):
    return None if who(request) else connection.respond(401, "who are you?\n")


async def main() -> None:
    server = Server(InMemoryStorage())

    async def handler(socket):
        await server.serve(Socket(socket), identity=who(socket.request), scope="demo")

    async with serve(handler, "127.0.0.1", 8765, process_request=upgrade):
        ana = await Client.connect("ws://127.0.0.1:8765", headers={"Authorization": "Bearer ana-token"})
        reminders = await server.connect(Identity(role="assistant", id="reminders", name="Reminders"), scope="demo")

        renewal = await reminders.create_thread(thread_id="renewal-ana-2026-10")
        await reminders.add_member(renewal.id, ana.identity)
        await reminders.join(renewal.id)
        print("present before:", [i.id for i in await reminders.present(renewal.id)])
        await reminders.send(renewal.id, "Your plan renews Friday. Switch to yearly and save 15%?")

        async for item in ana.events():
            if isinstance(item, Invited):
                print(f"ana was invited to {item.thread.id} by {item.sender.name}")
                await ana.join(item.thread_id, since=0)
                break
        for event in resolve(await ana.history(renewal.id)):
            if isinstance(event, Message):
                print(f"  {event.author.name}: {event.content[0].text}")
        print("present after:", [i.id for i in await reminders.present(renewal.id)])

        await ana.send(renewal.id, "Yes, switch it.")
        async for item in reminders.events():
            if isinstance(item, Message):
                print(f"reminders heard: {item.content[0].text}")
                break
        await ana.close()
        await reminders.close()


asyncio.run(main())
```

- [ ] **Step 4: `person_and_assistant/`**

`agents/assistant/AGENT.md`:

```markdown
---
name: assistant
kind: generation
description: Answers the person in the thread
---
You are a helpful assistant in a chat. Answer in one or two plain sentences.
```

`main.py`:

```python
import asyncio

from rfnry_engine import Engine, Message as Turn, TextDelta
from rfnry_thread import Client, Identity, InMemoryStorage, Invited, Message, Server, StreamDelta, resolve
from websockets.asyncio.server import serve

from providers import AnthropicProvider


class Socket:
    def __init__(self, socket):
        self.socket = socket

    async def receive_text(self):
        return await self.socket.recv()

    async def send_text(self, data):
        await self.socket.send(data)

    async def close(self, code=1000, reason=None):
        await self.socket.close(code, reason or "")


YOU = Identity(role="user", id="you", name="You")


def upgrade(connection, request):
    if request.headers.get("Authorization") != "Bearer you":
        return connection.respond(401, "who are you?\n")
    return None


async def assistant(server, engine):
    me = await server.connect(Identity(role="assistant", id="assistant", name="Assistant"), scope="demo")
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        if not (isinstance(item, Message) and item.author.role == "user"):
            continue
        earlier = [e for e in resolve(await me.history(item.thread_id, before=item.seq)) if isinstance(e, Message)]
        history = [Turn(role="user" if e.author.role == "user" else "assistant", content=e.content) for e in earlier]
        async with me.stream(item.thread_id, in_reply_to=item.id) as reply:

            async def show(event):
                if isinstance(event, TextDelta):
                    await reply.write(event.text)

            run = await engine.run("assistant", scope="demo", input=item.content, history=history, on_event=show)
            await reply.finish(run.reply)


async def person():
    you = await Client.connect("ws://127.0.0.1:8766", headers={"Authorization": "Bearer you"})
    thread = await you.create_thread(thread_id="chat")
    await you.add_member(thread.id, Identity(role="assistant", id="assistant", name="Assistant"))
    await you.join(thread.id)

    async def show():
        async for item in you.events():
            if isinstance(item, StreamDelta):
                print(item.text, end="", flush=True)
            elif isinstance(item, Message):
                print()

    printer = asyncio.create_task(show())
    while (line := await asyncio.to_thread(input, "you: ")).strip():
        await you.send(thread.id, line)
        await asyncio.sleep(0.2)
        print("assistant: ", end="", flush=True)
    printer.cancel()
    await you.close()


async def main() -> None:
    server = Server(InMemoryStorage())
    engine = Engine("agents", generation=AnthropicProvider())

    async def handler(socket):
        await server.serve(Socket(socket), identity=YOU, scope="demo")

    async with serve(handler, "127.0.0.1", 8766, process_request=upgrade):
        helper = asyncio.create_task(assistant(server, engine))
        await person()
        helper.cancel()


asyncio.run(main())
```

- [ ] **Step 5: `two_assistants/`**

`agents/optimist/AGENT.md` and `agents/skeptic/AGENT.md` (the optimist argues for, the skeptic against; one or two sentences, answer the last message):

```markdown
---
name: optimist
kind: generation
description: Argues for the proposal
---
You argue for the proposal under discussion. Reply to the last message in one or two sentences.
```

```markdown
---
name: skeptic
kind: generation
description: Argues against the proposal
---
You argue against the proposal under discussion. Reply to the last message in one or two sentences.
```

`main.py`:

```python
import asyncio

from rfnry_engine import Engine
from rfnry_thread import Identity, InMemoryStorage, Invited, Message, Server

from providers import AnthropicProvider, GoogleProvider

TURNS = 3


async def speaker(server, engine, name, opens):
    me = await server.connect(Identity(role="assistant", id=name, name=name.title()), scope="demo")
    spoken = 0
    if opens:
        thread = await me.create_thread(thread_id="debate")
        await me.add_member(thread.id, Identity(role="assistant", id="skeptic", name="Skeptic"))
        await me.join(thread.id)
        await me.send(thread.id, "Proposal: the team moves to a four-day week.")
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        elif isinstance(item, Message) and spoken < TURNS:
            run = await engine.run(name, scope="demo", input=item.content)
            await me.send(item.thread_id, run.reply, in_reply_to=item.id)
            print(f"{name}: {run.reply}\n")
            spoken += 1
        if spoken == TURNS:
            return


async def main() -> None:
    server = Server(InMemoryStorage())
    claude = Engine("agents", generation=AnthropicProvider())
    gemini = Engine("agents", generation=GoogleProvider())
    skeptic = asyncio.create_task(speaker(server, gemini, "skeptic", opens=False))
    await asyncio.sleep(0.1)
    await speaker(server, claude, "optimist", opens=True)
    await skeptic


asyncio.run(main())
```

The loop guard is the example's own (`TURNS`): the library carries messages between assistants and leaves when to stop to them.

- [ ] **Step 6: `engine_agent_in_a_thread/`**

`agents/support/AGENT.md`, `agents/support/INDEX.md` (`rules/_index.md: what you must do`), `agents/support/rules/_index.md` (`rules/refunds.md: refunds and double charges`), `agents/support/rules/refunds.md` (`A double charge is refunded in full within five days.`):

```markdown
---
name: support
kind: generation
description: Answers customers in their thread
---
You are the support desk. Use read_recent to read the conversation, working to say what you are
doing, and send to answer in the thread. Cite the rule you apply.
```

`main.py`:

```python
import asyncio
import json

from rfnry_engine import Engine, ToolCallStarted
from rfnry_thread import Identity, InMemoryStorage, Message, Server, Text, ThreadTools, resolve

from providers import AnthropicProvider


def output(result):
    if isinstance(result, str):
        return [Text(text=result)]
    if isinstance(result, dict):
        return [Text(text=json.dumps(result))]
    return result


async def main() -> None:
    server = Server(InMemoryStorage())
    engine = Engine("agents", generation=AnthropicProvider())
    ana = await server.connect(Identity(role="user", id="ana", name="Ana"), scope="demo")
    helper = await server.connect(Identity(role="assistant", id="support", name="Support"), scope="demo")
    thread = await ana.create_thread(thread_id="invoice")
    await ana.add_member(thread.id, helper.identity)
    await helper.join(thread.id)
    question = await ana.send(thread.id, "My invoice was charged twice. What happens now?")

    tools = ThreadTools(helper, thread_id=thread.id, triggered_by=question.author, in_reply_to=question.id)

    async def mirror(event):
        if isinstance(event, ToolCallStarted) and event.tool not in tools:
            await helper.working(thread.id, label=f"{event.tool}")

    run = await engine.run("support", scope="demo", input="A new message is waiting in your thread.",
                           tools=tools, on_event=mirror)
    for step in run.steps:
        if step.tool not in tools:
            await helper.step(thread.id, name=step.tool, input=step.args, output=output(step.result),
                              is_error=step.is_error, duration_ms=step.duration_ms, in_reply_to=question.id)
    await helper.idle(thread.id)

    for event in resolve(await ana.history(thread.id)):
        if isinstance(event, Message):
            print(f"{event.author.name}: {event.content[0].text}")
        else:
            print(f"  [{event.kind} {getattr(event, 'name', '')}]")
    await ana.close()
    await helper.close()


asyncio.run(main())
```

The engine and the thread never import each other; this file is the glue.

- [ ] **Step 7: `two_replicas/main.py`**

```python
import asyncio
import os

import asyncpg
from rfnry_thread import Client, Identity, Message, PostgresStorage, Server, Working
from websockets.asyncio.server import serve

PEOPLE = {"ana": Identity(role="user", id="ana", name="Ana"), "bruno": Identity(role="user", id="bruno", name="Bruno")}


class Socket:
    def __init__(self, socket):
        self.socket = socket

    async def receive_text(self):
        return await self.socket.recv()

    async def send_text(self, data):
        await self.socket.send(data)

    async def close(self, code=1000, reason=None):
        await self.socket.close(code, reason or "")


def who(request):
    return PEOPLE.get(request.headers.get("Authorization", "").removeprefix("Bearer "))


def upgrade(connection, request):
    return None if who(request) else connection.respond(401, "who are you?\n")


async def replica(pool, port):
    storage = PostgresStorage(pool)
    await storage.initialize()
    server = Server(storage)
    await server.start()

    async def handler(socket):
        await server.serve(Socket(socket), identity=who(socket.request), scope="demo")

    return server, await serve(handler, "127.0.0.1", port, process_request=upgrade)


async def main() -> None:
    url = os.environ["RFNRY_EXAMPLES_POSTGRES_URL"]
    one, two = await asyncpg.create_pool(url), await asyncpg.create_pool(url)
    (first, first_socket), (second, second_socket) = await replica(one, 8771), await replica(two, 8772)

    ana = await Client.connect("ws://127.0.0.1:8771", headers={"Authorization": "Bearer ana"})
    bruno = await Client.connect("ws://127.0.0.1:8772", headers={"Authorization": "Bearer bruno"})
    thread = await ana.create_thread(thread_id="across")
    await ana.add_member(thread.id, bruno.identity)
    await ana.join(thread.id)
    await bruno.join(thread.id)
    await ana.working(thread.id, label="typing")
    await ana.send(thread.id, "Hello from replica one.")

    async for item in bruno.events():
        if isinstance(item, Working):
            print(f"bruno sees ana {item.label} (a signal mirrored through NOTIFY)")
        if isinstance(item, Message):
            print(f"bruno, on replica two, reads seq {item.seq}: {item.content[0].text}")
            break

    await ana.close()
    await bruno.close()
    for server, socket in ((first, first_socket), (second, second_socket)):
        await server.stop()
        socket.close()
        await socket.wait_closed()
    await server.delete_scope("demo")
    await one.close()
    await two.close()


asyncio.run(main())
```

Needs `RFNRY_EXAMPLES_POSTGRES_URL`, which `../.env.examples` sets to the `rfnry-thread-pg` container (`postgresql://thread:thread@127.0.0.1:55436/thread`); the README says so.

- [ ] **Step 8: `typescript_client/`**

`serve.py` (a server with an echo assistant in its own process — no model; the browser-shaped client offers `["thread", <token>]` as its subprotocols, the server accepts with `thread` and reads the token from the offer):

```python
import asyncio

from rfnry_thread import Identity, InMemoryStorage, Invited, Message, Server
from websockets.asyncio.server import serve


class Socket:
    def __init__(self, socket):
        self.socket = socket

    async def receive_text(self):
        return await self.socket.recv()

    async def send_text(self, data):
        await self.socket.send(data)

    async def close(self, code=1000, reason=None):
        await self.socket.close(code, reason or "")


BROWSER = Identity(role="user", id="browser", name="Browser")


def offered(request):
    return [p.strip() for p in request.headers.get("Sec-WebSocket-Protocol", "").split(",")]


def upgrade(connection, request):
    return None if offered(request) == ["thread", "browser"] else connection.respond(401, "who are you?\n")


async def echo(server):
    me = await server.connect(Identity(role="assistant", id="echo", name="Echo"), scope="demo")
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        elif isinstance(item, Message):
            async with me.stream(item.thread_id, in_reply_to=item.id) as reply:
                for word in f"You said: {item.content[0].text}".split():
                    await reply.write(word + " ")
                    await asyncio.sleep(0.05)


async def main() -> None:
    server = Server(InMemoryStorage())

    async def handler(socket):
        await server.serve(Socket(socket), identity=BROWSER, scope="demo")

    async with serve(handler, "127.0.0.1", 8780, subprotocols=["thread"], process_request=upgrade):
        await echo(server)


asyncio.run(main())
```

`package.json`:

```json
{
  "name": "typescript_client",
  "private": true,
  "type": "module",
  "dependencies": { "@rfnry/thread": "file:../../packages/typescript" }
}
```

`main.mjs`:

```js
import { connect, resolve } from '@rfnry/thread'

const client = await connect({ url: 'ws://127.0.0.1:8780', protocols: async () => ['thread', 'browser'] })
const thread = await client.createThread({ threadId: 'hello' })
await client.addMember(thread.id, { role: 'assistant', id: 'echo', name: 'Echo' })
const done = new Promise((finish) => {
  client.subscribe((item) => {
    if (item.kind === 'stream.delta') process.stdout.write(item.text)
    if (item.kind === 'stream.end') finish()
  })
})
await client.join(thread.id, { since: 0 })
await client.send(thread.id, 'hello from node')
await done
console.log('\n', resolve(await client.history(thread.id)).map((event) => `${event.seq} ${event.kind}`))
await client.close()
```

Run: `cd packages/typescript && npm run build`, then `cd examples/typescript_client && npm install && (uv run serve.py &) && sleep 1 && node main.mjs`.

- [ ] **Step 9: `examples/README.md`**

```markdown
# Examples

Each folder is a standalone project: a `main.py` that reads top to bottom and a `pyproject.toml`
that shows how few dependencies it needs. A folder that uses a model runs it through rfnry-engine
and carries the engine's `providers.py`, byte for byte — rfnry-thread ships no model. Servers are
served with the `websockets` package through a five-line adapter, which also stands in for your
authentication: it refuses an upgrade without a known token and hands `serve` who it is. Your
framework and your authentication are yours.

| example | what it shows |
| --- | --- |
| `streaming_with_steps` | a working label, a step under the question, a streamed reply stored as one message, the resolved log |
| `proactive_assistant` | an assistant opens a thread, adds a person, and speaks first; the person's socket client receives `Invited`, joins with `since=0`; presence before and after |
| `person_and_assistant` | you in the terminal over a WebSocket, an engine agent in the server's process answering with a stream |
| `two_assistants` | two engine agents on two vendors debating in one thread; when to stop is their code |
| `engine_agent_in_a_thread` | `ThreadTools` mounted on an engine run; the run's tool calls mirrored as steps and a working label |
| `two_replicas` | two servers on one Postgres, a person on each; events and signals cross through `LISTEN/NOTIFY` |
| `typescript_client` | `@rfnry/thread` in Node against a Python server with an echo assistant |

## Running

The keys live outside the repository, in a file with `ANTHROPIC_API_KEY` and `GOOGLE_API_KEY`.
Run an example from inside its folder:

```bash
cd examples/person_and_assistant
uv run --env-file ../../../.env.examples main.py
```

`two_replicas` also needs `RFNRY_EXAMPLES_POSTGRES_URL`, a Postgres database, in the same file.
`uv` builds each folder's environment on first run; the per-folder `uv.lock` files are ignored by git. Lint an example with
its own ruff: `uv run ruff format . && uv run ruff check .`.
```

- [ ] **Step 10: Check the providers are byte-identical and run every example once**

Run: `cd examples && for f in */providers.py; do cmp "$f" ../../engine/examples/image_question/providers.py || echo "DIFFERS: $f"; done`
Expected: no output.

Run, from each folder: `uv run ruff format . && uv run ruff check .`, then `uv run --env-file ../../../.env.examples main.py` (the terminal example: type two lines, then an empty line). Expected: each prints what its README row says; `two_replicas` with `RFNRY_EXAMPLES_POSTGRES_URL` set.

Run the key-prefix `git grep` from the executor brief over the whole repository — expected: nothing.

- [ ] **Step 11: Commit**

```bash
git add examples
git commit -m "docs: examples — streaming with steps, a proactive assistant, a person and an assistant, two assistants, an engine agent in a thread, two replicas, the TypeScript client

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 20: The e2e tier — the TypeScript client against a live Python server

Written fresh (the old `e2e/` package drove the React client through a browser; this tier drives the plain client from Node).

**Files:**
- Create: `packages/python/tests/e2e/conftest.py`, `packages/python/tests/e2e/test_typescript.py`, `packages/typescript/tests/e2e/live.test.ts`

**Interfaces:**
- Consumes: `Server`, `Server.connect`, `Client` events (Tasks 9–13); `connect`, `Client`, `Stream` (Task 17).
- Produces: `uv run poe test:e2e` — starts a Python server with an in-process assistant on a free port, runs `npm run test:e2e` in `packages/typescript` with `THREAD_URL`, and passes when that suite passes.

- [ ] **Step 1: Write the harness and the TypeScript suite**

`packages/python/tests/e2e/conftest.py`:

```python
import shutil

import pytest


@pytest.fixture(autouse=True)
def node() -> None:
    if shutil.which("npm") is None:
        pytest.skip("npm is not installed")
```

`packages/python/tests/e2e/test_typescript.py`:

```python
import asyncio
import os
from pathlib import Path

from http import HTTPStatus

import pytest
from websockets.asyncio.server import ServerConnection, serve
from websockets.http11 import Request, Response

from rfnry_thread import Custom, Identity, InMemoryStorage, Invited, Message, Server, Text

pytestmark = pytest.mark.live

TYPESCRIPT = Path(__file__).parents[3] / "typescript"


def who(request: Request | None) -> Identity | None:
    offered = [] if request is None else [p.strip() for p in request.headers.get("Sec-WebSocket-Protocol", "").split(",")]
    if len(offered) != 2 or offered[0] != "thread" or not offered[1]:
        return None
    return Identity(role="user", id=offered[1], name=offered[1].title())


def upgrade(socket: ServerConnection, request: Request) -> Response | None:
    return None if who(request) else socket.respond(HTTPStatus.UNAUTHORIZED, "who are you?\n")


class Socket:
    def __init__(self, socket: ServerConnection, open_sockets: list["Socket"]) -> None:
        self.socket = socket
        open_sockets.append(self)

    async def receive_text(self) -> str:
        data = await self.socket.recv()
        return data if isinstance(data, str) else data.decode()

    async def send_text(self, data: str) -> None:
        await self.socket.send(data)

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        await self.socket.close(code, reason or "")


async def assistant(server: Server, open_sockets: list[Socket]) -> None:
    me = await server.connect(Identity(role="assistant", id="bot-helper", name="Helper"), scope="acme")
    async for item in me.events():
        if isinstance(item, Invited):
            await me.join(item.thread_id)
        elif isinstance(item, Custom) and item.name == "drop":
            for socket in list(open_sockets):
                await socket.socket.close()
            open_sockets.clear()
        elif isinstance(item, Message) and item.author.role == "user":
            await me.working(item.thread_id, label="Reading rules/refunds.md")
            await me.step(item.thread_id, name="Read", input={"path": "rules/refunds.md"},
                          output=[Text(text="A double charge is refunded in full.")], duration_ms=3, in_reply_to=item.id)
            async with me.stream(item.thread_id, in_reply_to=item.id) as reply:
                for chunk in ["Refunded ", "in full."]:
                    await reply.write(chunk)
            await me.idle(item.thread_id)


async def test_the_typescript_client_against_a_live_server() -> None:
    server = Server(InMemoryStorage())
    open_sockets: list[Socket] = []

    async def handler(socket: ServerConnection) -> None:
        identity = who(socket.request)
        assert identity is not None
        await server.serve(Socket(socket, open_sockets), identity=identity, scope="acme")

    async with serve(handler, "127.0.0.1", 0, subprotocols=["thread"], process_request=upgrade) as listening:
        port = next(iter(listening.sockets)).getsockname()[1]
        helper = asyncio.create_task(assistant(server, open_sockets))
        process = await asyncio.create_subprocess_exec(
            "npm", "run", "test:e2e", cwd=TYPESCRIPT, env={**os.environ, "THREAD_URL": f"ws://127.0.0.1:{port}"},
        )
        code = await asyncio.wait_for(process.wait(), 120)
        helper.cancel()
    assert code == 0
```

`packages/typescript/tests/e2e/live.test.ts`:

```ts
import { describe, expect, it } from 'vitest'
import { connect, resolve } from '../../src/index.js'
import type { Item } from '../../src/types.js'

const url = process.env.THREAD_URL ?? ''
const helper = { role: 'assistant' as const, id: 'bot-helper', name: 'Helper' }

async function until(check: () => boolean, ms = 5000): Promise<void> {
  const started = Date.now()
  while (!check()) {
    if (Date.now() - started > ms) throw new Error('e2e: timed out')
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

describe.skipIf(!url)('a live server', () => {
  it('carries a turn with a step, a stream and its message, in order', async () => {
    const client = await connect({ url, protocols: async () => ['thread', 'ana'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    const thread = await client.createThread({ threadId: 'e2e-turn' })
    await client.addMember(thread.id, helper)
    await client.join(thread.id, { since: 0 })
    const question = await client.send(thread.id, 'My invoice was charged twice.')
    await until(() => items.some((item) => 'kind' in item && item.kind === 'stream.end'))
    const kinds = items.flatMap((item) => ('kind' in item ? [item.kind] : []))
    expect(kinds).toContain('working')
    expect(kinds.indexOf('stream.start')).toBeLessThan(kinds.indexOf('message'))
    expect(kinds.lastIndexOf('message')).toBeLessThan(kinds.indexOf('stream.end'))
    const view = resolve(await client.history(thread.id))
    const underQuestion = view.filter((event) => event.in_reply_to === question.id).map((event) => event.kind)
    expect(underQuestion).toEqual(['step', 'message'])
    await client.close()
  })

  it('resumes after the server drops the socket and misses nothing', async () => {
    const client = await connect({ url, protocols: ['thread', 'ana'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item), { includeOwn: true })
    const thread = await client.createThread({ threadId: 'e2e-resume' })
    await client.addMember(thread.id, helper)
    await client.join(thread.id)
    await client.custom(thread.id, 'drop', {})
    await until(() => items.some((item) => 'truncated' in item))
    const sent = await client.send(thread.id, 'after the drop')
    await until(() => items.some((item) => 'kind' in item && item.kind === 'stream.end'))
    const seqs = items.flatMap((item) => ('kind' in item && 'seq' in item ? [item.seq] : []))
    expect(new Set(seqs).size).toBe(seqs.length)
    expect(seqs).toContain(sent.seq)
    await client.close()
  })
})
```

- [ ] **Step 2: Run the tier**

Run: `cd packages/typescript && npm run build && cd ../python && uv run poe test:e2e`
Expected: PASS — 1 Python test, whose subprocess runs 2 TypeScript tests. Without `THREAD_URL`, `npm test` skips the e2e folder (it is excluded) and `npm run test:e2e` skips the suite.

- [ ] **Step 3: Commit**

```bash
git add packages/python/tests/e2e packages/typescript/tests/e2e
git commit -m "test: the e2e tier — the TypeScript client against a live Python server, a turn and a resume

Claude-Session: https://claude.ai/code/session_01GevsYJaAyM8ARLvEnXsoAB"
```

---

### Task 21: Close

- [ ] **Step 1: Full verification**

Run, from `packages/python`: `uv run poe format && uv run poe check && uv run poe typecheck && uv run poe test`, then `RFNRY_TEST_POSTGRES_URL=postgresql://thread:thread@127.0.0.1:55436/thread uv run poe test:integration` (no skips), then `uv run poe test:e2e`.
Run, from `packages/typescript`: `npm run check && npm run typecheck && npm test && npm run build`.
Run: `git grep -nE 'anthropic|openai|google|gemini|claude' -- packages` — expected: nothing (vendor-neutral core; examples may name vendors).
Run: `git grep -n '"""' -- packages/python/src` and `git grep -nE '^\s*#' -- packages/python/src` — expected: nothing (no docstrings, no comments).

- [ ] **Step 2: Push**

```bash
git push origin main
```
