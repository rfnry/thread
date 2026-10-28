import ast
import inspect
import re
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from database import DSN, SCHEMA

import rfnry_thread
from rfnry_thread import Identity, PostgresStorage, Server

DOCS = Path(__file__).parents[4] / "docs" / "thread.md"
QUICKSTART = "postgresql://postgres:thread@localhost:5432/postgres"
BLOCKS: list[tuple[str, str]] = re.findall(
    r"```(\w*)\n(.*?)```", DOCS.read_text(encoding="utf-8"), re.S
)
PYTHON = [body for lang, body in BLOCKS if lang == "python"]

ILLUSTRATIVE = {
    "app = FastAPI()": "needs a web framework and a signed-in user; test_socket.py serves a real socket",
    'remote = await Client.connect("wss://app.example.com/thread", headers={"Authorization": f"Bearer {token}"})': "needs a live socket; test_socket.py covers it",
    "async for item in helper.events():": "reads until the stream ends; test_client.py covers the pattern",
    "server = Server(PostgresStorage(pool))": "needs Postgres; the integration tier covers it",
    'run = await engine.run("support", scope="acme", input=message.content,': "needs rfnry-engine and a model; test_thread_tools.py covers the tools",
    "await helper.follow(resume=True)": "follows until the stream ends; test_follow.py covers the pattern",
    "async with pool.acquire() as conn:": "needs Postgres; the integration tier covers it",
    'offered = websocket.scope.get("subprotocols", [])': "needs a web framework's socket; test_socket.py covers a refusal with 1008",
    'pool = await asyncpg.create_pool(dsn, server_settings={"search_path": "thread"})': "needs Postgres; the integration tier covers it",
    "kept: dict[str, tuple[list, int]] = {}": "needs rfnry-engine and a model; the consumer's code",
    "events = await helper.history(thread.id, after=ingested, limit=500)": "needs rfnry-memory; the consumer's code",
}


def first_line(block: str) -> str:
    return block.splitlines()[0]


async def now(args: dict[str, Any]) -> str:
    return "2026-09-24T12:00:00Z"


async def context(storage: PostgresStorage) -> dict[str, Any]:
    server = Server(storage)
    ana = await server.connect(identity=Identity(role="user", id="ana", name="Ana"), scope="acme")
    helper = await server.connect(
        identity=Identity(role="assistant", id="helper", name="Helper"), scope="acme"
    )
    thread = await ana.create_thread(thread_id="support-ana")
    await ana.add_member(thread.id, helper.identity)
    await ana.join(thread.id)
    await helper.join(thread.id)
    message = await ana.send(thread.id, "My invoice was charged twice.")
    return {
        **{name: getattr(rfnry_thread, name) for name in rfnry_thread.__all__},
        "asyncpg": asyncpg,
        "server": server,
        "ana": ana,
        "helper": helper,
        "thread": thread,
        "message": message,
        "storage": storage,
        "now": now,
        "wav": b"RIFF",
        "url": "https://files.example.com/receipt.png",
    }


def runnable() -> list[Any]:
    return [
        pytest.param(block, id=first_line(block)[:60])
        for block in PYTHON
        if first_line(block) not in ILLUSTRATIVE
    ]


def test_every_named_block_is_in_the_docs_and_first_lines_are_unique() -> None:
    lines = [first_line(block) for block in PYTHON]
    assert len(lines) == len(set(lines))
    assert set(ILLUSTRATIVE) <= set(lines)


@pytest.mark.parametrize("block", runnable())
async def test_the_block_runs(block: str, storage: PostgresStorage) -> None:
    namespace = await context(storage)
    opened = [namespace["ana"], namespace["helper"]]
    code = compile(
        block.replace(QUICKSTART, DSN).replace(
            '"search_path": "thread"', f'"search_path": "{SCHEMA}"'
        ),
        "<docs/thread.md>",
        "exec",
        flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
    )
    result = eval(code, namespace)
    if inspect.iscoroutine(result):
        await result
    for client in [*opened, namespace["ana"], namespace["helper"]]:
        await client.close()
    if "pool" in namespace:
        await namespace["pool"].close()


@pytest.mark.parametrize(
    "block", [b for b in PYTHON if first_line(b) in ILLUSTRATIVE], ids=first_line
)
def test_an_illustrative_block_still_compiles(block: str) -> None:
    compile(block, "<docs/thread.md>", "exec", flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT)
