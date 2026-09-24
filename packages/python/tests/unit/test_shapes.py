import pytest
from pydantic import ValidationError

from rfnry_thread.models import (
    CLIENT_EVENTS,
    CLIENT_SIGNALS,
    EVENTS,
    SIGNALS,
    Custom,
    Image,
    Joined,
    Message,
    Present,
    Step,
    StreamDelta,
    Text,
    Thread,
    parts,
)

from .factories import ANA, AT, HELPER


def envelope(kind: str, **fields: object) -> dict[str, object]:
    return {
        "kind": kind,
        "id": "e1",
        "thread_id": "t1",
        "seq": 1,
        "author": ANA.model_dump(),
        "created_at": AT.isoformat(),
        "client_id": "c1",
        **fields,
    }


def test_events_parse_by_kind() -> None:
    assert isinstance(
        EVENTS.validate_python(envelope("message", content=[{"type": "text", "text": "hi"}])),
        Message,
    )
    step = EVENTS.validate_python(envelope("step", name="Read", input={"path": "a.md"}, output=[]))
    assert isinstance(step, Step) and step.is_error is False and step.duration_ms is None
    custom = EVENTS.validate_python(envelope("custom", name="rating", payload={"stars": 5}))
    assert isinstance(custom, Custom) and custom.name == "rating"


def test_an_unknown_event_kind_is_refused() -> None:
    with pytest.raises(ValidationError):
        EVENTS.validate_python(envelope("run.started"))


def test_signals_parse_by_kind_and_carry_their_sender() -> None:
    delta = SIGNALS.validate_python(
        {
            "kind": "stream.delta",
            "thread_id": "t1",
            "sender": HELPER.model_dump(),
            "stream_id": "s1",
            "index": 0,
            "text": "Re",
        }
    )
    assert isinstance(delta, StreamDelta) and delta.sender == HELPER
    present = SIGNALS.validate_python(
        {"kind": "present", "thread_id": "t1", "sender": ANA.model_dump()}
    )
    assert isinstance(present, Present)


def test_clients_may_send_only_these_kinds() -> None:
    assert {"message", "step", "edit", "retract", "read", "custom"} == CLIENT_EVENTS
    assert {
        "working",
        "idle",
        "stream.start",
        "stream.delta",
        "stream.end",
        "cancel",
    } == CLIENT_SIGNALS


def test_content_is_a_string_or_parts() -> None:
    assert parts("hi") == [Text(text="hi")]
    image = Image(media_type="image/png", url="https://x/a.png")
    assert parts([Text(text="a"), image]) == [Text(text="a"), image]


def test_joined_round_trips() -> None:
    joined = Joined(thread=Thread(id="t1", scope="acme", created_at=AT), seq=7, truncated=True)
    assert Joined.model_validate_json(joined.model_dump_json()) == joined
