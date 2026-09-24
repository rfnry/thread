from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field, ValidationError

from rfnry_thread.errors import ConfigurationError, InputError
from rfnry_thread.models import IdentityLike, Message, Part, Step, Text, as_identity
from rfnry_thread.resolve import resolve

if TYPE_CHECKING:
    from rfnry_thread.client import Client

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


class _Send(BaseModel):
    text: str = Field(description="The message to post in the thread")


class _ReadRecent(BaseModel):
    limit: int = Field(
        default=20, ge=1, le=200, description="How many of the latest messages and steps to read"
    )


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
        triggered_by: IdentityLike | None = None,
        in_reply_to: str | None = None,
    ) -> None:
        if not thread_id:
            raise ConfigurationError("tools: a thread_id is required")
        cause = None if triggered_by is None else as_identity(triggered_by)

        async def send(args: dict[str, Any]) -> Result:
            event = await client.send(
                thread_id, args["text"], in_reply_to=in_reply_to, triggered_by=cause
            )
            return {"id": event.id, "seq": event.seq}

        async def read_recent(args: dict[str, Any]) -> Result:
            limit = int(args.get("limit", 20))
            events = await client.history(
                thread_id, limit=200, kinds=["message", "step", "edit", "retract"]
            )
            view = [e for e in resolve(events) if isinstance(e, Message | Step)][-limit:]
            found: list[Part] = []
            for event in view:
                header: dict[str, Any] = {
                    "seq": event.seq,
                    "kind": event.kind,
                    "in_reply_to": event.in_reply_to,
                    "author": event.author.model_dump(mode="json", include={"role", "id", "name"}),
                }
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

        super().__init__(
            [
                Tool("send", "Post a message to the conversation", body=send, args=_Send),
                Tool(
                    "read_recent",
                    "Read the latest messages and steps of the conversation, as they were sent",
                    body=read_recent,
                    args=_ReadRecent,
                ),
                Tool(
                    "working",
                    "Show the people in the conversation what you are doing",
                    body=working,
                    args=_Working,
                ),
                Tool("idle", "Show that you are no longer working", body=idle, args=_Idle),
            ]
        )
