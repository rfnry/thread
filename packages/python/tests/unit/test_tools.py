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
    tools = Tools([Tool("echo", "Echo", body=echo, args=EchoArgs)]) + Tools(
        [Tool("other", body=echo)]
    )
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


async def test_a_tool_with_no_body_is_a_configuration_error_when_called() -> None:
    tools = Tools([Tool("described", "Only described")])
    assert tools.definitions()[0]["name"] == "described"
    with pytest.raises(ConfigurationError, match="^tools: tool 'described' has no body$"):
        await tools.dispatch("described", {})
