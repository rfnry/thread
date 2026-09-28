from __future__ import annotations

import base64
from datetime import datetime
from typing import Annotated, Any, Literal, Protocol, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_serializer,
    model_validator,
)

from rfnry_thread.errors import InputError


class Text(BaseModel):
    type: Literal["text"] = "text"
    text: str


class _Media(BaseModel):
    model_config = ConfigDict(val_json_bytes="base64")

    type: str
    media_type: str
    data: bytes | None = None
    url: str | None = None

    @model_validator(mode="after")
    def _data_or_url(self) -> Self:
        if (self.data is None) == (self.url is None):
            raise InputError(f"models: {self.type} carries exactly one of data or url")
        return self

    @field_serializer("data", when_used="json-unless-none")
    def _base64(self, data: bytes) -> str:
        return base64.b64encode(data).decode()


class Image(_Media):
    type: Literal["image"] = "image"


class Audio(_Media):
    type: Literal["audio"] = "audio"


class Document(_Media):
    type: Literal["document"] = "document"
    name: str | None = None


Part = Annotated[Text | Image | Audio | Document, Field(discriminator="type")]
Content = str | list[Part]


class Identity(BaseModel):
    role: Literal["user", "assistant", "system"]
    id: str
    name: str | None = None
    metadata: dict[str, Any] = {}


class IdentityLike(Protocol):
    @property
    def role(self) -> Literal["user", "assistant", "system"]: ...
    @property
    def id(self) -> str: ...
    @property
    def name(self) -> str | None: ...
    @property
    def metadata(self) -> dict[str, Any]: ...


def as_identity(value: IdentityLike) -> Identity:
    try:
        return Identity.model_validate(value, from_attributes=True)
    except ValidationError as exc:
        raise InputError(f"models: not an identity: {exc.errors(include_url=False)}") from exc


SYSTEM = Identity(role="system", id="system", name="system")


class Thread(BaseModel):
    id: str
    scope: str
    metadata: dict[str, Any] = {}
    created_at: datetime


class Member(BaseModel):
    thread_id: str
    identity: Identity
    added_at: datetime
    added_by: Identity


class _Envelope(BaseModel):
    id: str
    thread_id: str
    seq: int = 0
    author: Identity
    created_at: datetime
    client_id: str
    in_reply_to: str | None = None
    triggered_by: Identity | None = None


class Message(_Envelope):
    kind: Literal["message"] = "message"
    content: list[Part]
    metadata: dict[str, Any] = {}


class Step(_Envelope):
    kind: Literal["step"] = "step"
    name: str
    input: dict[str, Any] = {}
    output: list[Part] = []
    is_error: bool = False
    duration_ms: int | None = None


class Edit(_Envelope):
    kind: Literal["edit"] = "edit"
    target: str
    content: list[Part]


class Retract(_Envelope):
    kind: Literal["retract"] = "retract"
    target: str


class Read(_Envelope):
    kind: Literal["read"] = "read"
    up_to_seq: int


class MemberAdded(_Envelope):
    kind: Literal["member_added"] = "member_added"
    identity: Identity


class MemberRemoved(_Envelope):
    kind: Literal["member_removed"] = "member_removed"
    identity: Identity


class ThreadUpdated(_Envelope):
    kind: Literal["thread_updated"] = "thread_updated"
    metadata: dict[str, Any]


class Custom(_Envelope):
    kind: Literal["custom"] = "custom"
    name: str
    payload: dict[str, Any]


Event = Annotated[
    Message | Step | Edit | Retract | Read | MemberAdded | MemberRemoved | ThreadUpdated | Custom,
    Field(discriminator="kind"),
]
EVENTS: TypeAdapter[Event] = TypeAdapter(Event)
CLIENT_EVENTS = frozenset({"message", "step", "edit", "retract", "read", "custom"})


class _Signal(BaseModel):
    thread_id: str
    sender: Identity


class Working(_Signal):
    kind: Literal["working"] = "working"
    label: str | None = None


class Idle(_Signal):
    kind: Literal["idle"] = "idle"


class StreamStart(_Signal):
    kind: Literal["stream.start"] = "stream.start"
    stream_id: str
    in_reply_to: str | None = None


class StreamDelta(_Signal):
    kind: Literal["stream.delta"] = "stream.delta"
    stream_id: str
    index: int
    text: str


class StreamEnd(_Signal):
    kind: Literal["stream.end"] = "stream.end"
    stream_id: str


class Cancel(_Signal):
    kind: Literal["cancel"] = "cancel"
    to: Identity


class Present(_Signal):
    kind: Literal["present"] = "present"


class Left(_Signal):
    kind: Literal["left"] = "left"


class Invited(_Signal):
    kind: Literal["invited"] = "invited"
    thread: Thread


class ThreadDeleted(_Signal):
    kind: Literal["thread.deleted"] = "thread.deleted"


Signal = Annotated[
    Working
    | Idle
    | StreamStart
    | StreamDelta
    | StreamEnd
    | Cancel
    | Present
    | Left
    | Invited
    | ThreadDeleted,
    Field(discriminator="kind"),
]
SIGNALS: TypeAdapter[Signal] = TypeAdapter(Signal)
CLIENT_SIGNALS = frozenset(
    {"working", "idle", "stream.start", "stream.delta", "stream.end", "cancel"}
)


class Joined(BaseModel):
    thread: Thread
    seq: int
    truncated: bool = False


def parts(content: Content) -> list[Part]:
    return [Text(text=content)] if isinstance(content, str) else list(content)
