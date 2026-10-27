import json
import os
from pathlib import Path
from typing import Any

import pytest

from rfnry_thread.models import (
    EVENTS,
    SIGNALS,
    Cancel,
    Custom,
    Edit,
    Event,
    Idle,
    Image,
    Invited,
    Joined,
    Left,
    Marked,
    MemberAdded,
    MemberRemoved,
    Message,
    Present,
    Read,
    Retract,
    Signal,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Thread,
    ThreadDeleted,
    ThreadUpdated,
    Working,
)
from rfnry_thread.protocol import (
    ErrorFrame,
    Failure,
    Frame,
    Hello,
    Notify,
    Push,
    Request,
    Response,
    decode,
    encode,
)

from .factories import ANA, AT, BRUNO, HELPER

FIXTURES = Path(__file__).parents[1] / "fixtures" / "protocol"


def golden() -> dict[str, Frame]:
    thread = Thread(id="t1", scope="acme", metadata={"topic": "billing"}, created_at=AT)
    base: dict[str, Any] = {"thread_id": "t1", "created_at": AT}
    events: list[Event] = [
        Message(
            id="e1",
            seq=1,
            author=ANA,
            client_id="c1",
            content=[Text(text="hello"), Image(media_type="image/png", data=b"\x89PNG")],
            **base,
        ),
        Step(
            id="e2",
            seq=2,
            author=HELPER,
            client_id="c2",
            name="Read",
            input={"path": "rules/refunds.md"},
            output=[Text(text="A double charge is refunded in full.")],
            duration_ms=12,
            in_reply_to="e1",
            triggered_by=ANA,
            **base,
        ),
        Edit(
            id="e3",
            seq=3,
            author=ANA,
            client_id="c3",
            target="e1",
            content=[Text(text="hello!")],
            **base,
        ),
        Retract(id="e4", seq=4, author=ANA, client_id="c4", target="e1", **base),
        Read(id="e5", seq=5, author=HELPER, client_id="c5", up_to_seq=4, **base),
        MemberAdded(id="e6", seq=6, author=ANA, client_id="c6", identity=BRUNO, **base),
        MemberRemoved(id="e7", seq=7, author=ANA, client_id="c7", identity=BRUNO, **base),
        ThreadUpdated(
            id="e8", seq=8, author=ANA, client_id="c8", metadata={"topic": "refunds"}, **base
        ),
        Custom(
            id="e9", seq=9, author=ANA, client_id="c9", name="rating", payload={"stars": 5}, **base
        ),
    ]
    signals: list[Signal] = [
        Working(thread_id="t1", sender=HELPER, label="Reading rules/refunds.md"),
        Idle(thread_id="t1", sender=HELPER),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1", in_reply_to="e1"),
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text="Refunded "),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        Cancel(thread_id="t1", sender=ANA, to=HELPER),
        Present(thread_id="t1", sender=ANA),
        Left(thread_id="t1", sender=ANA),
        Invited(thread_id="t1", sender=ANA, thread=thread),
        ThreadDeleted(thread_id="t1", sender=ANA),
        Marked(thread_id="t1", sender=HELPER, seq=7, unread=2),
    ]
    frames: dict[str, Frame] = {
        "hello": Hello(protocol=1),
        "hello-response": Response(
            id="hello", payload={"identity": ANA.model_dump(mode="json"), "scope": "acme"}
        ),
        "hello-wrong-protocol": ErrorFrame(
            id="hello",
            error=Failure(
                code="input", message="server: the first frame must be a hello for protocol 1"
            ),
        ),
        "request-thread-list": Request(
            id="7",
            method="thread.list",
            payload={"before": "t0", "limit": 20, "order": "activity", "preview": True},
        ),
        "response-thread-list": Response(
            id="7",
            payload={
                "threads": [
                    thread.model_copy(
                        update={
                            "last_seq": 9,
                            "last_message_at": AT,
                            "marked": 7,
                            "unread": 2,
                            "last_message": Message(
                                id="e1",
                                seq=1,
                                author=ANA,
                                client_id="c1",
                                content=[Text(text="hello!")],
                                **base,
                            ),
                        }
                    ).model_dump(mode="json")
                ]
            },
        ),
        "request-thread-join": Request(
            id="1", method="thread.join", payload={"thread_id": "t1", "since": 0}
        ),
        "request-thread-join-resume": Request(
            id="6", method="thread.join", payload={"thread_id": "t1", "since": None, "resume": True}
        ),
        "response-thread-join": Response(
            id="1", payload=Joined(thread=thread, seq=12, truncated=True).model_dump(mode="json")
        ),
        "request-thread-follow": Request(
            id="4", method="thread.follow", payload={"resume": True, "since": {"t1": 9}}
        ),
        "request-thread-follow-left": Request(
            id="4",
            method="thread.follow",
            payload={"resume": True, "since": {"t1": 9}, "left": {"t2": 4}},
        ),
        "response-thread-follow": Response(
            id="4",
            payload={"joined": [Joined(thread=thread, seq=12).model_dump(mode="json")]},
        ),
        "request-thread-mark": Request(
            id="5", method="thread.mark", payload={"thread_id": "t1", "seq": 7}
        ),
        "response-thread-mark": Response(id="5", payload={"seq": 7, "unread": 2}),
        "request-thread-marked": Request(
            id="6", method="thread.marked", payload={"thread_id": "t1"}
        ),
        "response-thread-marked": Response(id="6", payload={"seq": 7}),
        "request-event-get": Request(
            id="8", method="event.get", payload={"thread_id": "t1", "event_id": "e1"}
        ),
        "response-event-get": Response(
            id="8", payload={"event": events[0].model_dump(mode="json")}
        ),
        "push-joined": Push(
            name="joined", payload=Joined(thread=thread, seq=12).model_dump(mode="json")
        ),
        "request-event-send": Request(
            id="2",
            method="event.send",
            payload={
                "thread_id": "t1",
                "kind": "message",
                "client_id": "c1",
                "content": [{"type": "text", "text": "hello"}],
                "in_reply_to": None,
                "triggered_by": HELPER.model_dump(mode="json"),
            },
        ),
        "error-forbidden": ErrorFrame(
            id="3",
            error=Failure(
                code="forbidden", message="server: 'bruno' is not a member of thread 't1'"
            ),
        ),
        "notify-stream-delta": Notify(
            method="signal.send",
            payload={
                "thread_id": "t1",
                "kind": "stream.delta",
                "stream_id": "s1",
                "index": 1,
                "text": "in full.",
            },
        ),
    }
    for event in events:
        frames[f"push-event-{event.kind.replace('_', '-')}"] = Push(
            name="event", payload=event.model_dump(mode="json")
        )
    frames["push-event-message-metadata"] = Push(
        name="event",
        payload=Message(
            id="e10",
            seq=10,
            author=HELPER,
            client_id="s1",
            content=[Text(text="Refunded in full.")],
            in_reply_to="e1",
            triggered_by=ANA,
            metadata={"run_id": "r1", "attempt": 2, "tags": ["refund"], "reviewed": False},
            **base,
        ).model_dump(mode="json"),
    )
    for signal in signals:
        frames[f"push-signal-{signal.kind.replace('.', '-')}"] = Push(
            name="signal", payload=signal.model_dump(mode="json")
        )
    return frames


def written(frame: Frame) -> str:
    return json.dumps(json.loads(encode(frame)), indent=2, sort_keys=True) + "\n"


def test_the_fixtures_are_what_the_models_write() -> None:
    frames = golden()
    if os.environ.get("RFNRY_WRITE_FIXTURES"):
        FIXTURES.mkdir(parents=True, exist_ok=True)
        for old in FIXTURES.glob("*.json"):
            old.unlink()
        for name, frame in frames.items():
            (FIXTURES / f"{name}.json").write_text(written(frame), encoding="utf-8")
    on_disk = {path.stem: path.read_text(encoding="utf-8") for path in FIXTURES.glob("*.json")}
    assert on_disk == {name: written(frame) for name, frame in frames.items()}


@pytest.mark.parametrize("path", sorted(FIXTURES.glob("*.json")), ids=lambda path: path.stem)
def test_every_fixture_round_trips(path: Path) -> None:
    wire = json.loads(path.read_text(encoding="utf-8"))
    frame = decode(json.dumps(wire))
    assert json.loads(encode(frame)) == wire
    if isinstance(frame, Push):
        if frame.name == "joined":
            assert Joined.model_validate(frame.payload).model_dump(mode="json") == frame.payload
        else:
            adapter = EVENTS if frame.name == "event" else SIGNALS
            assert adapter.validate_python(frame.payload).model_dump(mode="json") == frame.payload
