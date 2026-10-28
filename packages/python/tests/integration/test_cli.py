import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from database import URL, Schemas, quoted

from rfnry_thread import cli, migrations
from rfnry_thread.migrations import Migration
from rfnry_thread.postgres import PostgresStorage

from .test_migrations import TABLES, initialized, written_before

NOW = migrations.LATEST
NEXT = NOW + 1
NOTES = Migration(NEXT, "notes", ("CREATE TABLE thread_notes (id INTEGER)",))


async def run(*argv: str) -> int:
    return await asyncio.to_thread(cli.main, [argv[0], "--url", URL, *argv[1:]])


async def exists(pool: Any, name: str) -> bool:
    async with pool.acquire() as conn:
        return bool(
            await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", name
            )
        )


def noting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(migrations, "MIGRATIONS", [*migrations.MIGRATIONS, NOTES])
    monkeypatch.setattr(migrations, "LATEST", NEXT)


async def test_create_makes_a_schema_the_app_can_check(
    schemas: Schemas, capsys: pytest.CaptureFixture[str]
) -> None:
    name = schemas.name()
    assert await run("create", "--schema", name) == 0
    steps = "".join(f"  {m.version:04d} {m.name}\n" for m in migrations.MIGRATIONS)
    assert capsys.readouterr().out == f"{steps}schema {name} is at version {NOW}\n"
    await PostgresStorage(await schemas.pool(name)).check()
    assert await run("check", "--schema", name) == 0
    assert capsys.readouterr().out == f"schema {name} fits rfnry-thread: version {NOW}\n"


async def test_create_refuses_a_shared_or_system_schema(
    capsys: pytest.CaptureFixture[str],
) -> None:
    for name in ("public", "pg_thread", "information_schema"):
        assert await run("create", "--schema", name) == 1
        assert (
            capsys.readouterr().err
            == f"cli: thread is not created in {name!r} — name a schema of its own\n"
        )


async def test_create_refuses_a_schema_that_holds_thread_tables(
    schemas: Schemas, capsys: pytest.CaptureFixture[str]
) -> None:
    name = schemas.name()
    assert await run("create", "--schema", name) == 0
    capsys.readouterr()
    assert await run("create", "--schema", name) == 1
    assert "already holds thread tables" in capsys.readouterr().err


async def test_create_with_sql_prints_every_statement_and_makes_nothing(
    schemas: Schemas, capsys: pytest.CaptureFixture[str]
) -> None:
    name = schemas.name()
    assert await run("create", "--schema", name, "--sql") == 0
    out = capsys.readouterr().out
    assert out.startswith(
        f'SET search_path TO "{name}";\nBEGIN;\nCREATE SCHEMA IF NOT EXISTS "{name}";\n'
        f"{migrations.LOCK};\n"
    )
    assert "CREATE TABLE IF NOT EXISTS thread_events" in out
    last = migrations.MIGRATIONS[-1]
    assert out.endswith(
        f"INSERT INTO thread_migrations (version, name) VALUES ({last.version}, '{last.name}');\n"
        "COMMIT;\n"
    )
    assert not await exists(await schemas.pool("public"), name)


async def test_a_schema_name_that_needs_quoting_works_through_every_command(
    schemas: Schemas, capsys: pytest.CaptureFixture[str]
) -> None:
    name = schemas.name(' Odd-"Name"')
    assert await run("create", "--schema", name) == 0
    assert await run("check", "--schema", name) == 0
    assert await run("upgrade", "--schema", name) == 0
    assert capsys.readouterr().out.endswith(f"schema {name}: nothing to do\n")
    await PostgresStorage(await schemas.pool(name)).check()


async def test_upgrade_asks_before_it_runs_and_sql_runs_nothing(
    schemas: Schemas, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    name = schemas.name()
    assert await run("create", "--schema", name) == 0
    capsys.readouterr()
    noting(monkeypatch)
    assert await run("check", "--schema", name) == 1
    assert f"is at version {NOW}, this rfnry-thread needs {NEXT}" in capsys.readouterr().err
    assert await run("upgrade", "--schema", name, "--sql") == 0
    assert capsys.readouterr().out == (
        f"SET search_path TO {quoted(name)};\n"
        "BEGIN;\n"
        f"{migrations.LOCK};\n"
        "CREATE TABLE thread_notes (id INTEGER);\n"
        f"INSERT INTO thread_migrations (version, name) VALUES ({NEXT}, 'notes');\n"
        "COMMIT;\n"
    )
    monkeypatch.setattr("builtins.input", lambda prompt: "n")
    assert await run("upgrade", "--schema", name) == 0
    out = capsys.readouterr().out
    assert f"  {NEXT:04d} notes\n" in out and "take a snapshot" in out
    assert out.endswith("nothing was run\n")
    assert await run("check", "--schema", name) == 1
    monkeypatch.setattr("builtins.input", lambda prompt: "y")
    assert await run("upgrade", "--schema", name) == 0
    assert capsys.readouterr().out.endswith(f"schema {name} is at version {NEXT}\n")
    assert await run("check", "--schema", name) == 0


async def test_upgrade_from_moves_the_tables_initialize_made_into_the_schema(
    schemas: Schemas, capsys: pytest.CaptureFixture[str]
) -> None:
    source, held = await initialized(schemas)
    await written_before(held)
    target = schemas.name()
    assert await run("upgrade", "--schema", target, "--from", source, "--yes") == 0
    out = capsys.readouterr().out
    assert f"  move {', '.join(sorted(TABLES))} from {source}\n" in out
    assert out.endswith(f"schema {target} is at version {NOW}\n")
    storage = PostgresStorage(await schemas.pool(target))
    await storage.check()
    assert [e.seq for e in await storage.list_events("acme", "t1")] == [1, 2]
    assert await run("upgrade", "--schema", target, "--from", source, "--yes") == 0
    assert capsys.readouterr().out == f"schema {target}: nothing to do\n"


async def test_from_and_schema_name_two_schemas(capsys: pytest.CaptureFixture[str]) -> None:
    assert await run("upgrade", "--schema", "thread", "--from", "thread") == 1
    assert capsys.readouterr().err == (
        "cli: --from names the schema the tables are in, --schema the one they move to\n"
    )


async def test_check_and_upgrade_name_a_schema_that_does_not_exist(
    schemas: Schemas, capsys: pytest.CaptureFixture[str]
) -> None:
    name = schemas.name()
    for command in ("check", "upgrade"):
        assert await run(command, "--schema", name) == 1
        assert (
            capsys.readouterr().err
            == f"cli: there is no schema {name} — run rfnry-thread create --schema {name}\n"
        )


async def test_check_names_what_does_not_fit(
    schemas: Schemas, capsys: pytest.CaptureFixture[str]
) -> None:
    name = await schemas.create()
    assert await run("check", "--schema", name) == 1
    assert (
        capsys.readouterr().err
        == f"cli: no thread schema in {name} — run rfnry-thread create --schema {name}\n"
    )


async def test_a_connection_string_never_appears_in_what_the_command_prints(
    capsys: pytest.CaptureFixture[str],
) -> None:
    url = "postgresql://thread:hunter2@127.0.0.1:1/thread"
    assert await asyncio.to_thread(cli.main, ["check", "--url", url]) == 1
    printed = capsys.readouterr()
    assert printed.err.startswith("cli: ")
    assert "hunter2" not in printed.out + printed.err and url not in printed.err


def test_a_message_that_quotes_the_connection_string_is_scrubbed() -> None:
    url = "postgresql://thread:hunter2@db/thread"
    assert cli._scrubbed(f"cannot reach {url}", url) == "cannot reach <url>"
    assert cli._scrubbed("dsn thread:hunter2@db is bad", url) == "dsn thread:***@db is bad"
    assert cli._scrubbed("thread refused", url) == "thread refused"


def test_without_asyncpg_the_command_says_what_to_install(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "find_spec", lambda name: None)
    assert cli.main(["check"]) == 1
    assert capsys.readouterr().err == "cli: install rfnry-thread[postgres] to use this command\n"


async def test_a_closed_input_at_the_prompt_runs_nothing(
    schemas: Schemas, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    name = schemas.name()
    assert await run("create", "--schema", name) == 0
    noting(monkeypatch)

    def closed(prompt: str) -> str:
        raise EOFError

    monkeypatch.setattr("builtins.input", closed)
    assert await run("upgrade", "--schema", name) == 0
    assert capsys.readouterr().out.endswith("nothing was run\n")
    assert await run("check", "--schema", name) == 1


def test_an_interrupt_at_the_prompt_runs_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupted(prompt: str) -> str:
        raise KeyboardInterrupt

    monkeypatch.setattr("builtins.input", interrupted)
    with ThreadPoolExecutor(1) as worker:
        asked = worker.submit(cli._confirmed, argparse.Namespace(yes=False))
        assert asked.exception() is None
        assert asked.result() is False


async def test_upgrade_into_a_missing_schema_points_at_tables_initialize_made(
    schemas: Schemas, capsys: pytest.CaptureFixture[str]
) -> None:
    old, _ = await initialized(schemas)
    name = schemas.name()
    for command in ("check", "upgrade"):
        assert await run(command, "--schema", name) == 1
        err = capsys.readouterr().err
        assert f"run rfnry-thread upgrade --schema {name} --from {old}" in err
