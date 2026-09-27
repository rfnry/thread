import ast
from pathlib import Path

import rfnry_thread

SRC = Path(rfnry_thread.__file__).parent

LOG_CALLS = {"debug", "info", "warning", "error", "exception", "critical", "log"}

LOGGERS = {"log", "logger", "logging"}

PREFIXES = {"verbs.py": "server", "hub.py": "server"}

ALLOWED: dict[tuple[str, str], str] = {
    ("client.py", "raised(reply.error.code, reply.error.message)"): "the server's error, as sent",
    ("client.py", "self._failure"): "the error that stopped the client, raised again",
    (
        "client.py",
        "self._failure or TransportError('client: the client is closed')",
    ): "the error that stopped the client, or a closed client",
}


def _sources() -> list[Path]:
    return sorted(SRC.rglob("*.py"))


def _leading(node: ast.expr) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        text = ""
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                text += part.value
            else:
                return text + "{"
        return text
    return None


def _raises(path: Path) -> list[tuple[int, str, str | None]]:
    found = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if not isinstance(node, ast.Raise) or node.exc is None:
            continue
        exc = node.exc
        first = exc.args[0] if isinstance(exc, ast.Call) and exc.args else None
        found.append(
            (node.lineno, ast.unparse(exc), _leading(first) if first is not None else None)
        )
    return found


def _logs(path: Path) -> list[tuple[int, str, str | None]]:
    found = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        owner = node.func.value
        if not (isinstance(owner, ast.Name) and owner.id in LOGGERS):
            continue
        if node.func.attr not in LOG_CALLS:
            continue
        args = node.args[1:] if node.func.attr == "log" else node.args
        found.append((node.lineno, ast.unparse(node), _leading(args[0]) if args else None))
    return found


def _key(path: Path, source: str) -> tuple[str, str]:
    return str(path.relative_to(SRC)), source


def test_every_error_message_starts_with_its_module_name() -> None:
    wrong = []
    for path in _sources():
        prefix = f"{PREFIXES.get(path.name, path.stem)}: "
        for line, source, text in _raises(path):
            if text is None:
                if _key(path, source) not in ALLOWED:
                    wrong.append(f"{path.name}:{line}: no literal message: {source}")
                continue
            rest = text[len(prefix) :]
            if not text.startswith(prefix) or not rest or rest[0].isupper() or rest[0].isspace():
                wrong.append(f"{path.name}:{line}: {text!r}")
    assert wrong == []


def test_every_log_message_is_lowercase() -> None:
    found = [log for path in _sources() for log in _logs(path)]
    assert found
    assert [
        f"{line}: {source}" for line, source, text in found if not text or text[0].isupper()
    ] == []


def test_the_prefix_map_and_the_allowlist_name_only_what_exists() -> None:
    assert {path.name for path in _sources()} >= set(PREFIXES)
    present = {
        _key(path, source)
        for path in _sources()
        for _, source, text in _raises(path)
        if text is None
    }
    assert set(ALLOWED) <= present
