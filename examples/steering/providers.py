import asyncio
import base64
import os
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

import httpx
from anthropic import AsyncAnthropic
from anthropic.types import Message as AnthropicMessage
from google import genai
from google.genai import types
from rfnry_engine import (
    Answer,
    Audio,
    CheckAnswer,
    ChoiceAnswer,
    Content,
    Decided,
    Document,
    Image,
    Message,
    Part,
    Question,
    Reply,
    ScoreAnswer,
    State,
    Text,
    ToolCall,
    ToolSpec,
    Usage,
)


class AnthropicProvider:
    media_types = {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "application/pdf",
        "text/*",
    }
    stops = {
        "end_turn": "end_turn",
        "stop_sequence": "end_turn",
        "tool_use": "tool_use",
        "max_tokens": "max_tokens",
    }

    def __init__(
        self, model: str = "claude-sonnet-5", max_tokens: int = 4096, effort: str = "low"
    ) -> None:
        self.model = model
        self.max_tokens = max_tokens
        self.effort = effort
        self.client = AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"], max_retries=3)

    async def generate(self, system: str, messages: list[Message], tools: list[ToolSpec]) -> Reply:
        response = await self.client.messages.create(**self._request(system, messages, tools))
        return self._reply(response)

    async def generate_stream(
        self,
        system: str,
        messages: list[Message],
        tools: list[ToolSpec],
        on_delta: Callable[[str], Awaitable[None]],
    ) -> Reply:
        async with self.client.messages.stream(**self._request(system, messages, tools)) as stream:
            async for text in stream.text_stream:
                await on_delta(text)
            return self._reply(await stream.get_final_message())

    def _request(
        self, system: str, messages: list[Message], tools: list[ToolSpec]
    ) -> dict[str, Any]:
        return {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": system,
            "messages": self._turns(messages),
            "tools": [
                {"name": t.name, "description": t.description, "input_schema": t.input_schema}
                for t in tools
            ],
            "output_config": {"effort": self.effort},
        }

    def _turns(self, messages: list[Message]) -> list[dict[str, Any]]:
        turns: list[dict[str, Any]] = []
        for message in messages:
            content: list[dict[str, Any]]
            if message.role == "tool":
                result = {
                    "type": "tool_result",
                    "tool_use_id": message.tool_call_id,
                    "content": message.content
                    if isinstance(message.content, str)
                    else self._blocks(message.content),
                }
                role, content = "user", [result | ({"is_error": True} if message.is_error else {})]
            elif message.role == "assistant" and message.reasoning:
                role, content = "assistant", message.reasoning
            elif message.role == "assistant":
                calls = [
                    {"type": "tool_use", "id": c.id, "name": c.name, "input": c.args}
                    for c in message.tool_calls
                ]
                role, content = "assistant", self._blocks(message.content) + calls
            else:
                role, content = "user", self._blocks(message.content)
            if turns and turns[-1]["role"] == role:
                turns[-1]["content"] = turns[-1]["content"] + content
            else:
                turns.append({"role": role, "content": content})
        return turns

    def _blocks(self, content: Content) -> list[dict[str, Any]]:
        if isinstance(content, str):
            return [{"type": "text", "text": content}] if content else []
        return [self._block(part) for part in content]

    def _block(self, part: Part) -> dict[str, Any]:
        if isinstance(part, Text):
            return {"type": "text", "text": part.text}
        if isinstance(part, Audio):
            raise ValueError("anthropic: audio input is not supported")
        if part.url is not None:
            source = {"type": "url", "url": part.url}
        elif isinstance(part, Document) and part.media_type.startswith("text/"):
            source = {
                "type": "text",
                "media_type": "text/plain",
                "data": (part.data or b"").decode(),
            }
        else:
            data = base64.b64encode(part.data or b"").decode()
            source = {"type": "base64", "media_type": part.media_type, "data": data}
        block = {"type": part.type, "source": source}
        return block | ({"title": part.name} if isinstance(part, Document) and part.name else {})

    def _reply(self, response: AnthropicMessage) -> Reply:
        blocks = [block.model_dump(exclude_none=True) for block in response.content]
        thinking = any(block["type"] in ("thinking", "redacted_thinking") for block in blocks)
        usage = response.usage
        return Reply(
            text="".join(block["text"] for block in blocks if block["type"] == "text"),
            tool_calls=[
                ToolCall(id=b["id"], name=b["name"], args=b["input"])
                for b in blocks
                if b["type"] == "tool_use"
            ],
            usage=Usage(
                input=usage.input_tokens,
                output=usage.output_tokens,
                cache_creation=usage.cache_creation_input_tokens or 0,
                cache_read=usage.cache_read_input_tokens or 0,
            ),
            stop_reason=self.stops.get(response.stop_reason or "", "error"),
            model=response.model,
            reasoning=blocks if thinking else None,
        )


class GoogleProvider:
    stops = {"STOP": "end_turn", "MAX_TOKENS": "max_tokens"}
    local_id = "local-"

    def __init__(self, model: str = "gemini-3.1-flash-lite", max_tokens: int = 4096) -> None:
        self.model = model
        self.max_tokens = max_tokens
        self.client = genai.Client(
            api_key=os.environ["GOOGLE_API_KEY"],
            http_options=types.HttpOptions(retry_options=types.HttpRetryOptions(attempts=4)),
        )

    async def generate(self, system: str, messages: list[Message], tools: list[ToolSpec]) -> Reply:
        declarations = [
            types.FunctionDeclaration(
                name=t.name, description=t.description, parameters_json_schema=t.input_schema
            )
            for t in tools
        ]
        response = await self.client.aio.models.generate_content(
            model=self.model,
            contents=self._contents(messages),
            config=types.GenerateContentConfig(
                system_instruction=system,
                max_output_tokens=self.max_tokens,
                tools=[types.Tool(function_declarations=declarations)],
                automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            ),
        )
        return self._reply(response)

    def _contents(self, messages: list[Message]) -> list[types.Content]:
        names: dict[str, str] = {}
        contents: list[types.Content] = []
        for message in messages:
            if message.role == "assistant":
                names.update({c.id: c.name for c in message.tool_calls})
                if message.reasoning:
                    contents.append(types.Content.model_validate(message.reasoning))
                    continue
                calls = [
                    types.Part(function_call=types.FunctionCall(id=c.id, name=c.name, args=c.args))
                    for c in message.tool_calls
                ]
                contents.append(
                    types.Content(role="model", parts=self._parts(message.content) + calls)
                )
            elif message.role == "tool":
                call_id = message.tool_call_id or ""
                parts = [] if isinstance(message.content, str) else message.content
                text = (
                    message.content
                    if isinstance(message.content, str)
                    else "\n".join(p.text for p in parts if isinstance(p, Text))
                )
                response = types.FunctionResponse(
                    id=None if call_id.startswith(self.local_id) else call_id,
                    name=names[call_id],
                    response={"error" if message.is_error else "result": text},
                    parts=[self._media(p) for p in parts if not isinstance(p, Text)] or None,
                )
                last = contents[-1] if contents else None
                if last is not None and last.parts and last.parts[0].function_response:
                    last.parts = [*last.parts, types.Part(function_response=response)]
                else:
                    contents.append(
                        types.Content(role="user", parts=[types.Part(function_response=response)])
                    )
            else:
                contents.append(types.Content(role="user", parts=self._parts(message.content)))
        return contents

    def _parts(self, content: Content) -> list[types.Part]:
        if isinstance(content, str):
            return [types.Part(text=content)] if content else []
        return [
            types.Part(text=p.text)
            if isinstance(p, Text)
            else types.Part(file_data=types.FileData(file_uri=p.url, mime_type=p.media_type))
            if p.url is not None
            else types.Part(inline_data=types.Blob(data=p.data, mime_type=p.media_type))
            for p in content
        ]

    def _media(self, part: Image | Audio | Document) -> types.FunctionResponsePart:
        if part.url is not None:
            uri = types.FunctionResponseFileData(file_uri=part.url, mime_type=part.media_type)
            return types.FunctionResponsePart(file_data=uri)
        blob = types.FunctionResponseBlob(data=part.data, mime_type=part.media_type)
        return types.FunctionResponsePart(inline_data=blob)

    def _reply(self, response: types.GenerateContentResponse) -> Reply:
        if not response.candidates or response.candidates[0].content is None:
            return Reply(stop_reason="error", model=self.model)
        candidate = response.candidates[0]
        parts = candidate.content.parts or []
        calls = [
            ToolCall(
                id=p.function_call.id or f"{self.local_id}{uuid4().hex}",
                name=p.function_call.name or "",
                args=p.function_call.args or {},
            )
            for p in parts
            if p.function_call
        ]
        finish = str(candidate.finish_reason.value if candidate.finish_reason else "STOP")
        usage = response.usage_metadata
        return Reply(
            text="".join(p.text for p in parts if p.text and not p.thought),
            tool_calls=calls,
            stop_reason="tool_use" if calls else self.stops.get(finish, "error"),
            usage=Usage(
                input=(
                    (usage.prompt_token_count or 0) - (usage.cached_content_token_count or 0)
                    if usage
                    else 0
                ),
                output=(
                    (usage.candidates_token_count or 0) + (usage.thoughts_token_count or 0)
                    if usage
                    else 0
                ),
                cache_read=(usage.cached_content_token_count or 0) if usage else 0,
            ),
            model=response.model_version or self.model,
            reasoning=candidate.content.model_dump(mode="json", exclude_none=True),
        )


class TypeSafeProvider:
    url = "https://api.typesafe.ai/v1/systemone"
    retried = (429, 500, 502, 503, 529)

    def __init__(self, model: str = "jev-latest", attempts: int = 4) -> None:
        self.model = model
        self.attempts = attempts
        self.client = httpx.AsyncClient(
            timeout=60, headers={"Authorization": f"Bearer {os.environ['DECISION_API_KEY']}"}
        )

    async def decide(self, state: State, questions: dict[str, Question]) -> Decided:
        body = {
            "state": state,
            "model": self.model,
            "questions": {qid: self._question(q) for qid, q in questions.items()},
        }
        for attempt in range(self.attempts):
            response = await self.client.post(self.url, json=body)
            if response.status_code not in self.retried or attempt == self.attempts - 1:
                break
            await asyncio.sleep(float(response.headers.get("retry-after", 2**attempt)))
        response.raise_for_status()
        data = response.json()
        usage = data.get("usage") or {}
        return Decided(
            answers={qid: self._answer(q, data["answers"][qid]) for qid, q in questions.items()},
            usage=Usage(input=usage.get("input_tokens", 0), output=usage.get("output_tokens", 0)),
            model=data.get("model"),
        )

    def _question(self, question: Question) -> dict[str, Any]:
        if question.type == "choice":
            return {
                "type": "choice",
                "instructions": question.instructions,
                "criteria": question.options,
            }
        if question.type == "score":
            return {
                "type": "score",
                "instructions": question.instructions,
                "criteria": question.levels,
            }
        body: dict[str, Any] = {"type": "noul", "instructions": question.instructions}
        if question.yes or question.no:
            body["criteria"] = {"true": question.yes or "Yes", "false": question.no or "No"}
        return body

    def _answer(self, question: Question, raw: dict[str, Any]) -> Answer:
        if question.type == "choice":
            probabilities = self._normalized(
                {option: raw["probabilities"].get(option, 0.0) for option in question.options}
            )
            choice = raw.get("choice")
            if choice not in probabilities:
                choice = max(probabilities, key=lambda option: probabilities[option])
            return ChoiceAnswer(choice=choice, probabilities=probabilities)
        if question.type == "score":
            levels = self._normalized(
                {str(i): raw["probabilities"].get(str(i), 0.0) for i in range(len(question.levels))}
            )
            return ScoreAnswer(score=float(raw["score"]), probabilities=list(levels.values()))
        return CheckAnswer(probability=min(1.0, max(0.0, float(raw["noul"]))))

    def _normalized(self, probabilities: dict[str, float]) -> dict[str, float]:
        total = sum(float(value) for value in probabilities.values())
        if total <= 0:
            return {key: 1 / len(probabilities) for key in probabilities}
        return {key: float(value) / total for key, value in probabilities.items()}
