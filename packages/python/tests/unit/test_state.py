from rfnry_thread.models import (
    Idle,
    Left,
    Present,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
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


def test_an_expired_slot_ends_once_after_any_read_and_leaves_presence_held_elsewhere() -> None:
    now = [0.0]
    cache = state(now)
    remote: list[Signal] = [
        Present(thread_id="t1", sender=HELPER),
        Working(thread_id="t1", sender=HELPER),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s2"),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"),
        Present(thread_id="t1", sender=ANA),
    ]
    for signal in remote:
        cache.apply("acme", signal, "other")
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    now[0] = EXPIRY + 1
    assert cache.present("acme", "t1") == [ANA]
    assert cache.snapshot("acme", "t1") == [Present(thread_id="t1", sender=ANA)]
    assert cache.expiring() == [("acme", "t1")]
    assert cache.expire("acme", "t1") == [
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s2"),
        Idle(thread_id="t1", sender=HELPER),
        Left(thread_id="t1", sender=HELPER),
    ]
    assert cache.expire("acme", "t1") == [] and cache.expiring() == []


def test_a_refresh_keeps_a_mirrored_entry_alive_and_heals_a_missed_end() -> None:
    now = [0.0]
    cache = state(now)
    cache.apply("acme", Working(thread_id="t1", sender=HELPER, label="Reading"), "other")
    cache.apply("acme", StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"), "other")
    now[0] = EXPIRY - 1
    told = cache.refresh(
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
    assert told == [
        Present(thread_id="t1", sender=HELPER),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        Idle(thread_id="t1", sender=HELPER),
    ]
    now[0] = EXPIRY + 5
    assert [s.kind for s in cache.snapshot("acme", "t1")] == ["present"]


def test_a_refresh_older_than_a_slots_last_signal_changes_nothing() -> None:
    cache = state([0.0])
    cache.apply("acme", Present(thread_id="t1", sender=HELPER), "other", 1)
    cache.apply("acme", StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"), "other", 2)
    cache.apply("acme", StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"), "other", 4)
    item = {
        "sender": HELPER.model_dump(mode="json"),
        "present": True,
        "working": None,
        "streams": [{"stream_id": "s1", "in_reply_to": None}],
    }
    assert cache.refresh("acme", "t1", "other", [item], 3) == []
    cache.apply("acme", Left(thread_id="t1", sender=HELPER), "other", 6)
    assert cache.refresh("acme", "t1", "other", [item], 5) == []
    assert cache.snapshot("acme", "t1") == []
    assert cache.refresh("acme", "t1", "other", [item], 7) == [
        Present(thread_id="t1", sender=HELPER),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"),
    ]


def test_a_refresh_teaches_a_late_replica_about_streams_it_never_saw_start() -> None:
    cache = state([0.0])
    told = cache.refresh(
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
    assert told == snapshot


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


def test_a_deleted_thread_is_forgotten_and_an_older_repeat_cannot_bring_it_back() -> None:
    now = [0.0]
    cache = state(now)
    cache.apply("acme", Present(thread_id="t1", sender=ANA), "me")
    cache.apply("acme", Present(thread_id="t1", sender=HELPER), "far", 5)
    assert cache.delete("acme", "t1") == [
        {"sender": ANA.model_dump(mode="json"), "present": False, "working": None, "streams": []}
    ]
    assert cache.snapshot("acme", "t1") == [] and cache.own() == {}
    repeat = [{"sender": HELPER.model_dump(mode="json"), "present": True, "working": None}]
    assert cache.refresh("acme", "t1", "far", repeat, 4) == []
    assert cache.present("acme", "t1") == []
    now[0] = EXPIRY + 1
    assert cache.expire("acme", "t1") == [] and cache.threads("acme") == []
