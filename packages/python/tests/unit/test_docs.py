import re
from pathlib import Path

import rfnry_thread

from .test_public_api import CLIENT

ROOT = Path(__file__).parents[4]
DOCS = ROOT / "docs" / "thread.md"
TYPESCRIPT = ROOT / "packages" / "typescript" / "src" / "index.ts"


def test_every_public_name_is_documented() -> None:
    text = DOCS.read_text(encoding="utf-8")
    assert [
        name for name in rfnry_thread.__all__ if f"`{name}`" not in text and f"{name}(" not in text
    ] == []


def test_every_client_verb_is_documented() -> None:
    text = DOCS.read_text(encoding="utf-8")
    assert [verb for verb in CLIENT if f".{verb}(" not in text and f"`{verb}`" not in text] == []


def typescript_exports() -> set[str]:
    index = TYPESCRIPT.read_text(encoding="utf-8")
    exported = re.findall(r"export (?:type )?\{([^}]*)\} from", index)
    names = {
        name.strip().removeprefix("type ")
        for group in exported
        for name in group.split(",")
        if name.strip()
    }
    for module in re.findall(r"export (?:type )?\* from '\./(\w+)\.js'", index):
        source = (TYPESCRIPT.parent / f"{module}.ts").read_text(encoding="utf-8")
        names |= set(re.findall(r"^export (?:type|const|function|class) (\w+)", source, re.M))
    return names


def test_every_typescript_export_is_documented() -> None:
    text = DOCS.read_text(encoding="utf-8")
    names = typescript_exports()
    assert names
    assert sorted(name for name in names if f"`{name}`" not in text) == []
