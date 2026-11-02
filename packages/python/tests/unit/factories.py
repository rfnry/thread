import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from rfnry_thread.models import (
    SYSTEM,
    Identity,
    MemberAdded,
    MemberRemoved,
    Message,
    Text,
    ThreadUpdated,
)

AT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
ANA = Identity(role="user", id="ana", name="Ana")
BRUNO = Identity(role="user", id="bruno", name="Bruno")
HELPER = Identity(role="assistant", id="helper", name="Helper")


@dataclass(frozen=True)
class Lookalike:
    role: Literal["user", "assistant", "system"]
    id: str
    name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def lookalike(identity: Identity) -> Lookalike:
    return Lookalike(identity.role, identity.id, identity.name, dict(identity.metadata))


def message(seq: int, text: str, author: Identity = ANA, event_id: str | None = None) -> Message:
    return Message(
        id=event_id or f"e{seq}",
        thread_id="t1",
        seq=seq,
        author=author,
        created_at=AT,
        client_id=f"c{seq}",
        content=[Text(text=text)],
    )


def added(thread_id: str, identity: Identity, by: Identity = SYSTEM) -> MemberAdded:
    return MemberAdded(
        id=uuid.uuid4().hex,
        thread_id=thread_id,
        author=by,
        created_at=AT,
        client_id=uuid.uuid4().hex,
        identity=identity,
    )


def removed(thread_id: str, identity: Identity, by: Identity = SYSTEM) -> MemberRemoved:
    return MemberRemoved(
        id=uuid.uuid4().hex,
        thread_id=thread_id,
        author=by,
        created_at=AT,
        client_id=uuid.uuid4().hex,
        identity=identity,
    )


def updated(thread_id: str, metadata: dict[str, Any], by: Identity = SYSTEM) -> ThreadUpdated:
    return ThreadUpdated(
        id=uuid.uuid4().hex,
        thread_id=thread_id,
        author=by,
        created_at=AT,
        client_id=uuid.uuid4().hex,
        metadata=metadata,
    )
