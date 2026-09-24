from datetime import UTC, datetime

from rfnry_thread.models import Identity, Message, Text

AT = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
ANA = Identity(role="user", id="ana", name="Ana")
BRUNO = Identity(role="user", id="bruno", name="Bruno")
HELPER = Identity(role="assistant", id="helper", name="Helper")


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
