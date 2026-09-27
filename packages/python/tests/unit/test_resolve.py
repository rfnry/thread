from rfnry_thread.models import Edit, Message, Retract, Step, Text
from rfnry_thread.resolve import resolve

from .factories import ANA, AT, HELPER, message


def edit(seq: int, target: str, text: str) -> Edit:
    return Edit(
        id=f"e{seq}",
        thread_id="t1",
        seq=seq,
        author=ANA,
        created_at=AT,
        client_id=f"c{seq}",
        target=target,
        content=[Text(text=text)],
    )


def retract(seq: int, target: str) -> Retract:
    return Retract(
        id=f"e{seq}",
        thread_id="t1",
        seq=seq,
        author=ANA,
        created_at=AT,
        client_id=f"c{seq}",
        target=target,
    )


def test_the_latest_edit_wins_and_edits_leave_the_view() -> None:
    view = resolve([message(1, "hello"), edit(2, "e1", "hullo"), edit(3, "e1", "hello!")])
    assert len(view) == 1 and isinstance(view[0], Message)
    assert (view[0].id, view[0].content) == ("e1", [Text(text="hello!")])


def test_an_edited_message_keeps_its_metadata() -> None:
    original = message(1, "hello").model_copy(update={"metadata": {"run_id": "r1"}})
    view = resolve([original, edit(2, "e1", "hello!")])
    assert isinstance(view[0], Message)
    assert (view[0].content, view[0].metadata) == ([Text(text="hello!")], {"run_id": "r1"})


def test_a_retracted_event_and_the_retraction_leave_the_view() -> None:
    step = Step(
        id="e2", thread_id="t1", seq=2, author=HELPER, created_at=AT, client_id="c2", name="Read"
    )
    view = resolve([message(1, "a"), step, retract(3, "e1"), retract(4, "e2"), message(5, "b")])
    assert [e.id for e in view] == ["e5"]


def test_the_view_is_in_seq_order_whatever_the_input_order() -> None:
    view = resolve([message(3, "c"), message(1, "a"), edit(4, "e1", "A"), message(2, "b")])
    assert [e.seq for e in view] == [1, 2, 3]
    assert isinstance(view[0], Message) and view[0].content == [Text(text="A")]
