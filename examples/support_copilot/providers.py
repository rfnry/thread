import asyncio
import base64
import os
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import httpx
from anthropic import AsyncAnthropic
from anthropic.types import Message as AnthropicMessage
from rfnry_engine import (
    Answer,
    Audio,
    CheckAnswer,
    ChoiceAnswer,
    Content,
    Decided,
    Document,
    Message,
    Part,
    QuestionLike,
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


class TypeSafeProvider:
    url = "https://api.typesafe.ai/v1/systemone"
    retried = (429, 500, 502, 503, 529)
    most_options = 255
    most_levels = 10

    def __init__(self, model: str = "jev-latest", attempts: int = 4) -> None:
        self.model = model
        self.attempts = attempts
        self.client = httpx.AsyncClient(
            timeout=60, headers={"Authorization": f"Bearer {os.environ['DECISION_API_KEY']}"}
        )

    async def decide(self, state: State, questions: Mapping[str, QuestionLike]) -> Decided:
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

    def _question(self, question: QuestionLike) -> dict[str, Any]:
        if question.type == "choice":
            if len(question.options) > self.most_options:
                raise ValueError(f"typesafe: a choice takes {self.most_options} options at most")
            return {
                "type": "choice",
                "instructions": question.instructions,
                "criteria": dict(question.options),
            }
        if question.type == "score":
            if len(question.levels) > self.most_levels:
                raise ValueError(f"typesafe: a score takes {self.most_levels} levels at most")
            return {
                "type": "score",
                "instructions": question.instructions,
                "criteria": list(question.levels),
            }
        body: dict[str, Any] = {"type": "noul", "instructions": question.instructions}
        if question.yes is not None or question.no is not None:
            body["criteria"] = {"true": question.yes, "false": question.no}
        return body

    def _answer(self, question: QuestionLike, raw: dict[str, Any]) -> Answer:
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
