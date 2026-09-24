from rfnry_thread.models import (
    Idle,
    Left,
    Present,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
    ThreadDeleted,
    Working,
)
from rfnry_thread.state import EXPIRY, LatestState

from .factories import ANA, HELPER


def state(now: list[float]) -> LatestState:
    return LatestState("me", clock=lambda: now[0])


def test_a_snapshot_is_presence_then_work_then_streams_with_their_text() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply("acme", Working(thread_id="t1", sender=HELPER, label="Reading"), "me")
    cache.apply(
        "acme", StreamStart(thread_id="t1", sender=HELPER, stream_id="s1", in_reply_to="e1"), "me"
    )
    cache.apply(
        "acme", StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=0, text="Re"), "me"
    )
    cache.apply(
        "acme",
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=1, text="funded"),
        "me",
    )
    kinds = [(s.kind, getattr(s, "text", None)) for s in cache.snapshot("acme", "t1")]
    assert kinds == [
        ("present", None),
        ("working", None),
        ("stream.start", None),
        ("stream.delta", "Refunded"),
    ]
    assert cache.snapshot("other", "t1") == []


def test_idle_left_and_stream_end_clear_what_they_end() -> None:
    cache = state([0.0])
    opened: list[Signal] = [
        Present(thread_id="t1", sender=ANA),
        Working(thread_id="t1", sender=HELPER),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"),
    ]
    closed: list[Signal] = [
        Left(thread_id="t1", sender=ANA),
        Idle(thread_id="t1", sender=HELPER),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
    ]
    for signal in [*opened, *closed]:
        cache.apply("acme", signal, "me")
    assert cache.snapshot("acme", "t1") == []
    assert cache.own() == {}


def test_presence_is_one_per_identity_across_replicas() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "other")
    assert cache.present("acme", "t1") == [ANA]
    cache.apply("acme", Left(thread_id="t1", sender=ANA), "me")
    assert cache.is_present("acme", "t1", "ana")


def test_a_silent_replica_is_forgotten_and_our_own_entries_are_not() -> None:
    now = [0.0]
    cache = state(now)
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "other")
    cache.apply("acme", Present(thread_id="t1", sender=HELPER), "me")
    now[0] = EXPIRY + 1
    assert cache.present("acme", "t1") == [HELPER]


def test_a_refresh_keeps_a_mirrored_entry_alive_and_heals_a_missed_end() -> None:
    now = [0.0]
    cache = state(now)
    cache.apply("acme", Working(thread_id="t1", sender=HELPER, label="Reading"), "other")
    cache.apply("acme", StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"), "other")
    now[0] = EXPIRY - 1
    cache.refresh(
        "acme",
        "t1",
        "other",
        [
            {
                "sender": HELPER.model_dump(mode="json"),
                "present": True,
                "working": None,
                "streams": [],
            }
        ],
    )
    now[0] = EXPIRY + 5
    assert [s.kind for s in cache.snapshot("acme", "t1")] == ["present"]


def test_a_refresh_teaches_a_late_replica_about_streams_it_never_saw_start() -> None:
    cache = state([0.0])
    cache.refresh(
        "acme",
        "t1",
        "other",
        [
            {
                "sender": HELPER.model_dump(mode="json"),
                "present": False,
                "working": {"label": "Writing"},
                "streams": [{"stream_id": "s1", "in_reply_to": "e1"}],
            }
        ],
    )
    snapshot = cache.snapshot("acme", "t1")
    assert [s.kind for s in snapshot] == ["working", "stream.start"]
    assert isinstance(snapshot[1], StreamStart) and snapshot[1].in_reply_to == "e1"


def test_own_lists_this_replica_entries_for_re_announcement() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply(
        "acme", StreamStart(thread_id="t1", sender=HELPER, stream_id="s1", in_reply_to="e1"), "me"
    )
    cache.apply("acme", Present(thread_id="t1", sender=HELPER), "other")
    own = cache.own()
    assert list(own) == [("acme", "t1")]
    assert sorted(item["sender"]["id"] for item in own[("acme", "t1")]) == ["ana", "helper"]
    helper = next(item for item in own[("acme", "t1")] if item["sender"]["id"] == "helper")
    assert helper == {
        "sender": HELPER.model_dump(mode="json"),
        "present": False,
        "working": None,
        "streams": [{"stream_id": "s1", "in_reply_to": "e1"}],
    }


def test_a_deleted_thread_is_forgotten() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply("acme", ThreadDeleted(thread_id="t1", sender=ANA), "me")
    assert cache.snapshot("acme", "t1") == [] and cache.own() == {}
