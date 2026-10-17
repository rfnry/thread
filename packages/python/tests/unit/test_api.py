import annotationlib
import ast
import dataclasses
import inspect
import re
from collections.abc import Callable
from functools import cache
from typing import Any

from pydantic import BaseModel

import rfnry_thread

from .test_docs import ROOT, TYPESCRIPT, typescript_exports

API = ROOT / "docs" / "api.md"
SOURCE = annotationlib.Format.STRING
PUBLIC: dict[str, Any] = {name: getattr(rfnry_thread, name) for name in rfnry_thread.__all__}
FENCES = {"python": "python", "typescript": "ts"}

Function = ast.FunctionDef | ast.AsyncFunctionDef


@cache
def entries(language: str) -> list[tuple[str, str]]:
    python, typescript = API.read_text(encoding="utf-8").split("\n## TypeScript client\n")
    part = python if language == "python" else typescript
    headings = re.findall(r"^### `([^`]+)`$", part, re.M)
    found: list[tuple[str, str]] = re.findall(
        rf"^### `([^`]+)`\n\n```{FENCES[language]}\n(.*?)^```$", part, re.M | re.S
    )
    assert [name for name, _ in found] == headings
    return found


def names(language: str) -> list[str]:
    return [name for name, _ in entries(language)]


def repeated(listed: list[str]) -> list[str]:
    return sorted({name for name in listed if listed.count(name) > 1})


def implemented(value: object) -> type | None:
    if not isinstance(value, type):
        return None
    return next(
        (
            protocol
            for protocol in PUBLIC.values()
            if getattr(protocol, "_is_runtime_protocol", False)
            and protocol is not value
            and issubclass(value, protocol)
        ),
        None,
    )


def is_core(value: object) -> bool:
    return (
        isinstance(value, type)
        and not issubclass(value, (BaseModel, BaseException))
        and not getattr(value, "_is_protocol", False)
        and implemented(value) is None
    )


def unwrap(value: object) -> Callable[..., Any] | None:
    if isinstance(value, (classmethod, staticmethod)):
        value = value.__func__
    elif isinstance(value, property):
        value = value.fget
    return value if inspect.isfunction(value) else None


def methods(owner: type) -> dict[str, tuple[bool, str]]:
    return {
        name: (
            inspect.iscoroutinefunction(function),
            str(inspect.signature(function, annotation_format=SOURCE)),
        )
        for name, member in vars(owner).items()
        if not name.startswith("_") and (function := unwrap(member)) is not None
    }


def decorator(value: object) -> list[str]:
    return [
        kind.__name__ for kind in (classmethod, staticmethod, property) if isinstance(value, kind)
    ]


def verbs() -> set[str]:
    return {
        f"{owner}.{name}"
        for owner, value in PUBLIC.items()
        if is_core(value)
        for name, member in vars(value).items()
        if not name.startswith("_") and unwrap(member) is not None
    }


def mismatch(node: Function, value: object) -> str | None:
    real = unwrap(value)
    if real is None:
        return "not a function"
    if [ast.unparse(d) for d in node.decorator_list] != decorator(value):
        return "decorators differ"
    if isinstance(node, ast.AsyncFunctionDef) != (
        inspect.iscoroutinefunction(real) or inspect.isasyncgenfunction(real)
    ):
        return "async differs"
    node.decorator_list = []
    namespace: dict[str, Any] = dict(real.__globals__)
    exec(compile(ast.unparse(node), str(API), "exec", dont_inherit=True), namespace)
    written = inspect.signature(namespace[node.name], annotation_format=SOURCE)
    actual = inspect.signature(real, annotation_format=SOURCE)
    return None if str(written) == str(actual) else f"{written} != {actual}"


def test_every_public_name_has_one_entry_and_every_entry_names_something_public() -> None:
    listed = names("python")
    assert repeated(listed) == []
    assert sorted(set(PUBLIC) - set(listed)) == []
    assert sorted(set(listed) - set(PUBLIC) - verbs()) == []


def test_every_verb_of_a_core_class_has_an_entry_right_after_its_class() -> None:
    listed = names("python")
    assert sorted(verbs() - set(listed)) == []
    owner, misplaced = "", []
    for name in listed:
        if "." not in name:
            owner = name
        elif name.split(".")[0] != owner:
            misplaced.append(name)
    assert misplaced == []


def test_every_signature_is_the_one_the_code_has() -> None:
    wrong: dict[str, str] = {}
    for name, block in entries("python"):
        owner, _, verb = name.partition(".")
        tree = ast.parse(block)
        declared = {
            node.name if isinstance(node, Function | ast.ClassDef) else ast.unparse(node.targets[0])
            for node in tree.body
            if isinstance(node, Function | ast.ClassDef | ast.Assign)
        }
        if (verb or owner) not in declared:
            wrong[name] = "the block does not declare it"
        for node in tree.body:
            if isinstance(node, Function):
                value = vars(PUBLIC[owner]).get(verb) if verb else PUBLIC[owner]
                if problem := mismatch(node, value):
                    wrong[name] = problem
            elif isinstance(node, ast.ClassDef):
                members = vars(PUBLIC.get(node.name, object))
                for item in node.body:
                    if isinstance(item, Function) and (
                        problem := mismatch(item, members.get(item.name))
                    ):
                        wrong[f"{name}: {item.name}"] = problem
    assert wrong == {}


def test_every_model_lists_exactly_its_fields() -> None:
    wrong: dict[str, tuple[list[str] | None, list[str]]] = {}
    for name, block in entries("python"):
        value = PUBLIC.get(name)
        if not isinstance(value, type):
            continue
        if issubclass(value, BaseModel):
            fields = list(value.model_fields)
        elif dataclasses.is_dataclass(value):
            fields = [field.name for field in dataclasses.fields(value)]
        else:
            continue
        classes = [
            n for n in ast.parse(block).body if isinstance(n, ast.ClassDef) and n.name == name
        ]
        written = (
            [
                item.target.id
                for item in classes[0].body
                if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)
            ]
            if classes
            else None
        )
        if written != fields:
            wrong[name] = (written, fields)
    assert wrong == {}


def test_every_storage_has_exactly_the_methods_of_the_protocol_it_implements() -> None:
    implementations = {
        name: protocol
        for name, value in PUBLIC.items()
        if (protocol := implemented(value)) is not None
    }
    assert implementations
    differ = {
        name: sorted(set(methods(PUBLIC[name]).items()) ^ set(methods(protocol).items()))
        for name, protocol in implementations.items()
        if methods(PUBLIC[name]) != methods(protocol)
    }
    assert differ == {}


def typescript_methods() -> set[str]:
    exported = typescript_exports()
    methods: set[str] = set()
    for path in TYPESCRIPT.parent.glob("*.ts"):
        source = path.read_text(encoding="utf-8")
        for owner, body in re.findall(r"^export class (\w+)[^{]*\{\n(.*?)^\}", source, re.M | re.S):
            if owner in exported:
                methods |= {
                    f"{owner}.{method}"
                    for method in re.findall(r"^  (?:async )?(\w+)\(", body, re.M)
                    if method != "constructor"
                }
    return methods


def test_every_typescript_export_and_method_has_one_entry_and_nothing_else_does() -> None:
    listed = names("typescript")
    expected = typescript_exports() | typescript_methods()
    assert repeated(listed) == []
    assert sorted(expected - set(listed)) == []
    assert sorted(set(listed) - expected) == []
    undeclared = []
    for name, block in entries("typescript"):
        owner, _, method = name.partition(".")
        pattern = rf"^(?:async )?{method}\(" if method else rf"\b(?:type|class|function) {owner}\b"
        if not re.search(pattern, block, re.M):
            undeclared.append(name)
    assert undeclared == []
