import argparse
import asyncio
import sys
from functools import partial
from importlib.util import find_spec
from typing import Any
from urllib.parse import urlsplit

from rfnry_thread.errors import ThreadException

_WARNING = "take a snapshot of the database and stop app instances running the old version first"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rfnry-thread", description="prepare and upgrade the database rfnry-thread runs on"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    def command(name: str, summary: str) -> argparse.ArgumentParser:
        sub = commands.add_parser(name, help=summary, description=summary)
        sub.add_argument("--url", help="postgres connection string; without it, the PG* variables")
        sub.add_argument("--schema", default="thread", help="the postgres schema (default: thread)")
        return sub

    create = command("create", "create the schema with this version's tables")
    create.add_argument("--sql", action="store_true", help="print the statements and run nothing")
    command("check", "say whether the schema fits this version")
    upgrade = command("upgrade", "carry the schema to this version")
    upgrade.add_argument(
        "--from",
        dest="source",
        help="the schema holding the tables initialize() made, moved into --schema",
    )
    upgrade.add_argument("--sql", action="store_true", help="print the statements and run nothing")
    upgrade.add_argument("--yes", action="store_true", help="run without asking")
    return parser


def _scrubbed(text: str, url: str | None) -> str:
    if not url:
        return text
    password = urlsplit(url).password
    text = text.replace(url, "<url>")
    return text.replace(f":{password}@", ":***@") if password else text


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _confirmed(args: argparse.Namespace) -> bool:
    if args.yes:
        return True
    print(_WARNING)
    try:
        answer = input("run these steps? [y/N] ")
    except EOFError, KeyboardInterrupt:
        print()
        return False
    return answer.strip().lower() in ("y", "yes")


async def _run(args: argparse.Namespace) -> int:
    import asyncpg

    from rfnry_thread import migrations

    conn: Any = await asyncpg.connect(args.url)
    try:
        setting = f"SET search_path TO {_quoted(args.schema)}"
        await conn.execute(setting)
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)", args.schema
        )
        moving = args.command == "upgrade" and args.source is not None
        if not exists and args.command != "create" and not moving:
            print(
                f"cli: there is no schema {args.schema} — "
                f"run rfnry-thread create --schema {args.schema}",
                file=sys.stderr,
            )
            return 1
        if args.command == "check":
            found = await migrations.problems(conn)
            if found:
                for problem in found:
                    print(f"cli: {problem}", file=sys.stderr)
                return 1
            print(f"schema {args.schema} fits rfnry-thread: version {migrations.LATEST}")
            return 0
        before: list[str] = []
        planner: migrations.Planner
        if args.command == "create" or moving:
            before = [f"CREATE SCHEMA IF NOT EXISTS {_quoted(args.schema)}"]
        if args.command == "create":
            planner = migrations.plan_create
        else:
            planner = partial(migrations.plan_upgrade, source=args.source)
        shown = await migrations.apply(conn, planner, run=False, before=before)
        if args.sql:
            print(f"{setting};")
            for statement in shown.statements:
                print(f"{statement};")
            return 0
        if not shown.steps:
            print(f"schema {args.schema}: nothing to do")
            return 0
        for step in shown.steps:
            print(f"  {step}")
        if args.command != "create" and not _confirmed(args):
            print("nothing was run")
            return 0
        await migrations.apply(conn, planner, expected=shown, before=before)
        print(f"schema {args.schema} is at version {migrations.LATEST}")
        return 0
    finally:
        await conn.close()


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.schema in ("public", "information_schema") or args.schema.startswith("pg_"):
        print(
            f"cli: thread is not created in {args.schema!r} — name a schema of its own",
            file=sys.stderr,
        )
        return 1
    if getattr(args, "source", None) == args.schema:
        print(
            "cli: --from names the schema the tables are in, --schema the one they move to",
            file=sys.stderr,
        )
        return 1
    if find_spec("asyncpg") is None:
        print("cli: install rfnry-thread[postgres] to use this command", file=sys.stderr)
        return 1
    try:
        return asyncio.run(_run(args))
    except ThreadException as exc:
        print(_scrubbed(str(exc), args.url), file=sys.stderr)
        return 1
    except Exception as exc:
        print(_scrubbed(f"cli: {type(exc).__name__}: {exc}", args.url), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
