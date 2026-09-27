from pathlib import Path

import pytest

ROOT = Path(__file__).parents[4]
SRC = "packages/python/src"

BLOCKS = {
    "parts": (
        "engine",
        "rfnry_thread/models.py",
        "rfnry_engine/models.py",
        "class Text(BaseModel):",
        "Content = str | list[Part]\n",
    ),
    "tools": (
        "engine",
        "rfnry_thread/tools.py",
        "rfnry_engine/tools/tools.py",
        "Result = ",
        "if n not in names})\n",
    ),
    "identity": (
        "senses",
        "rfnry_thread/models.py",
        "rfnry_senses/models.py",
        "class Identity(BaseModel):",
        'raise InputError(f"models: not an identity: {exc.errors(include_url=False)}") from exc\n',
    ),
}


def cut(path: Path, start: str, end: str) -> str:
    text = path.read_text(encoding="utf-8")
    first = text.index(start)
    return text[first : text.index(end, first) + len(end)]


@pytest.mark.parametrize("name", BLOCKS)
def test_the_shared_block_is_byte_identical_with_the_sibling_library(name: str) -> None:
    sibling, own, theirs, start, end = BLOCKS[name]
    checkout = ROOT.parent / sibling
    if not checkout.exists():
        pytest.skip(f"no sibling checkout at {checkout}")
    assert cut(ROOT / SRC / own, start, end) == cut(checkout / SRC / theirs, start, end)
