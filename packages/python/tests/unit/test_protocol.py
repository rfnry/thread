import pytest

from rfnry_thread.errors import ForbiddenError, InputError
from rfnry_thread.protocol import (
    METHODS,
    ErrorFrame,
    Frame,
    Hello,
    Notify,
    Push,
    Request,
    decode,
    encode,
    failure,
)


def test_frames_round_trip_by_type() -> None:
    frames: list[Frame] = [
        Hello(protocol=1),
        Request(id="1", method="thread.get", payload={"thread_id": "t"}),
        Notify(method="signal.send", payload={}),
        Push(name="event", payload={"kind": "message"}),
    ]
    for frame in frames:
        assert decode(encode(frame)) == frame


@pytest.mark.parametrize(
    "text", ["not json", '{"type": "shout"}', '{"type": "request", "method": "x"}', "[]"]
)
def test_a_malformed_frame_is_an_input_error(text: str) -> None:
    with pytest.raises(InputError, match="^protocol: malformed frame"):
        decode(text)


def test_an_error_frame_carries_the_code_and_message() -> None:
    frame = failure("7", ForbiddenError("server: 'bruno' is not a member of thread 't1'"))
    assert isinstance(frame, ErrorFrame)
    assert (frame.id, frame.error.code, frame.error.message) == (
        "7",
        "forbidden",
        "server: 'bruno' is not a member of thread 't1'",
    )


def test_the_methods_are_the_designed_sixteen() -> None:
    assert {
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
        "thread.present",
        "member.add",
        "member.remove",
        "member.list",
        "event.send",
        "signal.send",
    } == METHODS
