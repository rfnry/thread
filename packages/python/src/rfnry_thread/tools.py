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
