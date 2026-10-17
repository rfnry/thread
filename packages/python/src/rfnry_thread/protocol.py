from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, TypeAdapter, ValidationError

from rfnry_thread.errors import InputError, ThreadException

PROTOCOL = 1
HELLO = "hello"
CLOSE_REFUSED = 1008
CLOSE_PROTOCOL = 4000
CLOSE_SLOW = 4001
METHODS = frozenset(
    {
        "thread.create",
        "thread.get",
        "thread.list",
        "thread.update",
        "thread.delete",
        "thread.join",
        "thread.follow",
        "thread.leave",
        "thread.history",
        "thread.mark",
        "thread.marked",
        "thread.present",
        "member.add",
        "member.remove",
        "member.list",
        "event.send",
        "signal.send",
    }
)


class Hello(BaseModel):
    type: Literal["hello"] = "hello"
    protocol: int


class Request(BaseModel):
    type: Literal["request"] = "request"
    id: str
    method: str
    payload: dict[str, Any] = {}


class Response(BaseModel):
    type: Literal["response"] = "response"
    id: str
    payload: dict[str, Any] = {}


class Failure(BaseModel):
    code: str
    message: str


class ErrorFrame(BaseModel):
    type: Literal["error"] = "error"
    id: str
    error: Failure


class Push(BaseModel):
    type: Literal["push"] = "push"
    name: Literal["event", "signal", "joined"]
    payload: dict[str, Any]


class Notify(BaseModel):
    type: Literal["notify"] = "notify"
    method: str
    payload: dict[str, Any] = {}


Frame = Annotated[
    Hello | Request | Response | ErrorFrame | Push | Notify, Field(discriminator="type")
]
FRAMES: TypeAdapter[Frame] = TypeAdapter(Frame)


def encode(frame: Frame) -> str:
    return frame.model_dump_json()


def decode(text: str) -> Frame:
    try:
        return FRAMES.validate_json(text)
    except ValidationError as exc:
        raise InputError(
            f"protocol: malformed frame: {exc.errors(include_url=False)[0]['msg'].lower()}"
        ) from exc


def failure(request_id: str, error: ThreadException) -> ErrorFrame:
    return ErrorFrame(id=request_id, error=Failure(code=error.code, message=str(error)))
