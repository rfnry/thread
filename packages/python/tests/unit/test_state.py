import asyncio
from typing import Any

import pytest
from pydantic import ValidationError
from until import until

from rfnry_thread.models import (
    Identity,
    Idle,
    Left,
    Present,
    Signal,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Working,
)
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server
from rfnry_thread.state import EXPIRY, LatestState

from .conftest import say
from .factories import ANA, BRUNO, HELPER
from .notices import Muffled, Notice
from .peer import Peer
from .writes import epoch


def state(now: list[float]) -> LatestState:
    return LatestState("me", clock=lambda: now[0])


def entry(
    sender: Identity,
    present: bool = True,
    working: str | None = None,
    streams: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "sender": sender.model_dump(mode="json"),
        "present": present,
        "working": None if working is None else {"label": working},
        "streams": [{"stream_id": stream_id, "in_reply_to": None} for stream_id in streams],
    }


def said(signal: Signal) -> dict[str, Any]:
    return signal.model_dump(mode="json")


def pushed(peer: Peer) -> list[tuple[str, str]]:
    found, peer.pushes = peer.pushes, []
    return [(p["kind"], p["sender"]["id"]) for p in found]


def test_a_snapshot_replaces_an_origins_slice_whole_and_tells_the_difference() -> None:
    cache = state([0.0])
    cache.apply("acme", 1, Present(thread_id="t1", sender=ANA), "me", 1)
    start = StreamStart(thread_id="t1", sender=HELPER, stream_id="s1")
    assert cache.apply("acme", 1, start, "far", 1) == ([start], False)
    half = StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=3, text="Half")
    assert cache.apply("acme", 1, half, "far", 2) == ([half], False)
    told = cache.replace(
        "acme",
        "t1",
        1,
        "far",
        3,
        [entry(ANA), entry(HELPER, working="Writing", streams=("s1", "s2"))],
    )
    assert told == [
        Present(thread_id="t1", sender=HELPER),
        Working(thread_id="t1", sender=HELPER, label="Writing"),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s2"),
    ]
    assert [
        (s.kind, s.sender.id, getattr(s, "index", None), getattr(s, "text", None))
        for s in cache.snapshot("acme", "t1", 1)
    ] == [
        ("present", "ana", None, None),
        ("present", "helper", None, None),
        ("working", "helper", None, None),
        ("stream.start", "helper", None, None),
        ("stream.delta", "helper", 3, "Half"),
        ("stream.start", "helper", None, None),
    ]
    assert cache.replace("acme", "t1", 1, "far", 4, [entry(BRUNO)]) == [
        Present(thread_id="t1", sender=BRUNO),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s2"),
        Idle(thread_id="t1", sender=HELPER),
        Left(thread_id="t1", sender=HELPER),
    ]
    assert cache.present("acme", "t1", 1) == [ANA, BRUNO]


def test_anything_older_than_what_a_slice_holds_is_dropped() -> None:
    cache = state([0.0])
    present = Present(thread_id="t1", sender=HELPER)
    start = StreamStart(thread_id="t1", sender=HELPER, stream_id="s1")
    end = StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1")
    left = Left(thread_id="t1", sender=HELPER)
    taken_before = [entry(HELPER, streams=("s1",))]
    assert cache.apply("acme", 1, present, "far", 1) == ([present], False)
    assert cache.apply("acme", 1, start, "far", 2) == ([start], False)
    assert cache.apply("acme", 1, end, "far", 4) == ([end], False)
    assert cache.replace("acme", "t1", 1, "far", 3, taken_before) == []
    assert cache.apply("acme", 1, start, "far", 4) == ([], False)
    assert cache.apply("acme", 1, left, "far", 6) == ([left], True)
    assert cache.replace("acme", "t1", 1, "far", 5, taken_before) == []
    assert cache.apply("acme", 1, present, "far", 5) == ([], False)
    assert cache.present("acme", "t1", 1) == [] and cache.snapshot("acme", "t1", 1) == []
    assert cache.replace("acme", "t1", 1, "far", 7, taken_before) == [present, start]
    assert cache.replace("acme", "t1", 0, "far", 8, [entry(BRUNO)]) == []
    newer = Present(thread_id="t1", sender=BRUNO)
    assert cache.apply("acme", 2, newer, "far", 1) == ([newer], False)
    assert cache.present("acme", "t1", 1) == [] and cache.present("acme", "t1", 2) == [BRUNO]
    here, gone = Present(thread_id="t1", sender=ANA), Left(thread_id="t1", sender=ANA)
    assert cache.apply("acme", 2, here, "me", 1) == ([here], False)
    assert cache.apply("acme", 2, gone, "me", 2) == ([gone], True)
    assert cache.own() == []


def test_a_malformed_snapshot_changes_nothing_and_a_later_one_still_applies() -> None:
    cache = state([0.0])
    cache.replace("acme", "t1", 1, "far", 1, [entry(HELPER, streams=("s1",))])
    malformed = [entry(ANA), {"sender": {"id": "mallory"}, "present": True}]
    for epoch_, n in ((2, 2), (1, 2)):
        with pytest.raises(ValidationError):
            cache.replace("acme", "t1", epoch_, "far", n, malformed)
    assert cache.present("acme", "t1", 1) == [HELPER]
    assert cache.replace("acme", "t1", 1, "far", 2, [entry(ANA)]) == [
        Present(thread_id="t1", sender=ANA),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        Left(thread_id="t1", sender=HELPER),
    ]


async def test_a_slice_of_another_epoch_is_never_told_to_a_room_or_shown_to_a_joiner(
    storage: PostgresStorage,
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    ana = await Peer.open(near, ANA)
    helper = await Peer.open(far, HELPER)
    try:
        await ana.call("thread.create", thread_id="t1")
        await ana.call("thread.join", thread_id="t1")
        old = await epoch(storage, "acme", "t1")
        await ana.call("thread.delete", thread_id="t1")
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        await helper.call("thread.create", thread_id="t2")
        await helper.call("thread.join", thread_id="t2")
        await ana.quiet()
        await helper.quiet()
        gone = {"o": "gone", "e": old}
        await storage.publish(
            "acme", "t1", {**gone, "n": 1, **said(Present(thread_id="t1", sender=BRUNO))}
        )
        await storage.publish(
            "acme",
            "t1",
            {
                **gone,
                "n": 2,
                "kind": "refresh",
                "part": 0,
                "parts": 1,
                "entries": [entry(BRUNO, working="Old", streams=("s1",))],
            },
        )
        here = {"o": "gone", "n": 3, "e": await epoch(storage, "acme", "t2")}
        await storage.publish("acme", "t2", {**here, **said(Present(thread_id="t2", sender=BRUNO))})
        assert (await helper.push("present", timeout=2.0))["thread_id"] == "t2"
        await helper.call("thread.join", thread_id="t1")
        assert pushed(helper) == [("present", "ana"), ("present", "helper")]
        assert [(p["kind"], p["sender"]["id"]) for p in await ana.quiet()] == [
            ("present", "helper")
        ]
        identities = (await helper.call("thread.present", thread_id="t1"))["identities"]
        assert sorted(i["id"] for i in identities) == ["ana", "helper"]
        now = far._hub.state._clock()
        far._hub.state._clock = lambda: now + EXPIRY + 1
        far._hub.sweep()
        ended = [(p["kind"], p["thread_id"], p["sender"]["id"]) for p in await helper.quiet()]
        assert [told for told in ended if told[1:] == ("t1", "bruno")] == []
    finally:
        await ana.close()
        await helper.close()
        await near.stop()
        await far.stop()


def test_a_slice_in_parts_applies_only_when_every_part_arrived() -> None:
    now = [0.0]
    cache = state(now)
    assert cache.part("acme", "t1", 1, "far", 1, 0, 2, [entry(HELPER)]) == []
    assert cache.part("acme", "t1", 1, "far", 1, 0, 2, [entry(HELPER)]) == []
    assert cache.present("acme", "t1", 1) == []
    assert cache.part("acme", "t1", 1, "far", 1, 1, 2, [entry(ANA)]) == [
        Present(thread_id="t1", sender=HELPER),
        Present(thread_id="t1", sender=ANA),
    ]
    assert cache.part("acme", "t1", 1, "far", 2, 0, 2, [entry(HELPER)]) == []
    assert cache.present("acme", "t1", 1) == [HELPER, ANA]
    assert cache.part("acme", "t1", 1, "far", 3, 1, 2, []) == []
    assert cache.part("acme", "t1", 1, "far", 2, 1, 2, []) == []
    assert cache.present("acme", "t1", 1) == [HELPER, ANA]
    assert cache.part("acme", "t1", 1, "far", 3, 0, 2, [entry(HELPER)]) == [
        Left(thread_id="t1", sender=ANA)
    ]
    assert cache.part("acme", "t1", 1, "far", 4, 0, 2, [entry(BRUNO)]) == []
    now[0] = EXPIRY + 1
    assert cache.expired() == [("acme", "t1", 1, [Left(thread_id="t1", sender=HELPER)])]
    assert cache.part("acme", "t1", 1, "far", 4, 1, 2, []) == []
    assert cache.present("acme", "t1", 1) == []


async def test_a_snapshot_larger_than_one_notice_is_applied_only_once_every_part_arrived(
    pool: Any,
) -> None:
    big = [
        Identity(role="user", id=f"big{n}", name=f"Big {n}", metadata={"bio": "x" * 4000})
        for n in range(3)
    ]
    missing = {"left": True, "part": True}

    def drop(notice: Notice) -> bool:
        kind = notice.payload.get("kind")
        if kind == "left" and missing["left"]:
            missing["left"] = False
            return True
        if kind == "refresh" and notice.payload.get("part") == 1 and missing["part"]:
            missing["part"] = False
            return True
        return False

    muffled = Muffled(pool, drop)
    near, far = Server(muffled), Server(PostgresStorage(pool))
    starting = [Server(PostgresStorage(pool)), Server(PostgresStorage(pool))]
    await near.start()
    await far.start()
    ana = await Peer.open(near, ANA)
    peers = [await Peer.open(far, identity) for identity in big]
    try:
        await ana.call("thread.create", thread_id="t1")
        for identity in big:
            await ana.call("member.add", thread_id="t1", identity=identity.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        await ana.quiet()
        for peer in peers:
            await peer.call("thread.join", thread_id="t1")
        for _ in big:
            await ana.push("present", timeout=2.0)
        await peers[1].call("thread.leave", thread_id="t1")
        await starting[0].start()
        await until(lambda: not missing["part"])
        assert [p["kind"] for p in await ana.quiet()] == []
        identities = (await ana.call("thread.present", thread_id="t1"))["identities"]
        assert sorted(i["id"] for i in identities) == ["ana", "big0", "big1", "big2"]
        await starting[1].start()
        assert (await ana.push("left", timeout=2.0))["sender"]["id"] == "big1"
        identities = (await ana.call("thread.present", thread_id="t1"))["identities"]
        assert sorted(i["id"] for i in identities) == ["ana", "big0", "big2"]
        assert [p["kind"] for p in await ana.quiet()] == []
    finally:
        for peer in [ana, *peers]:
            await peer.close()
        for server in [near, far, *starting]:
            await server.stop()


async def test_a_delta_for_a_stream_never_started_here_is_dropped(pool: Any) -> None:
    muffled = Muffled(pool, lambda notice: notice.payload.get("kind") == "stream.start")
    near, far = Server(muffled), Server(PostgresStorage(pool))
    await near.start()
    await far.start()
    ana = await Peer.open(near, ANA)
    helper = await Peer.open(far, HELPER)
    try:
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        await helper.call("thread.join", thread_id="t1")
        await ana.quiet()
        await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
        await helper.notify(
            "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="Half"
        )
        await helper.call("signal.send", thread_id="t1", kind="working", label="Writing")
        await ana.push("working", timeout=2.0)
        await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
        await helper.call("signal.send", thread_id="t1", kind="idle")
        await ana.push("idle", timeout=2.0)
        assert [p["kind"] for p in await ana.quiet()] == []
        assert [n.payload["kind"] for n in muffled.dropped] == ["stream.start"]
    finally:
        await ana.close()
        await helper.close()
        await near.stop()
        await far.stop()


def test_a_silent_origins_slices_expire_whole_and_end_once() -> None:
    now = [0.0]
    cache = state(now)
    cache.apply("acme", 1, Present(thread_id="t1", sender=ANA), "me", 1)
    busy = [entry(HELPER, working="Writing", streams=("s2", "s1")), entry(ANA)]
    for n, thread_id in enumerate(("t1", "t2"), 1):
        cache.replace("acme", thread_id, 1, "far", n, busy)
    now[0] = EXPIRY + 1

    def ends(thread_id: str) -> list[Signal]:
        return [
            StreamEnd(thread_id=thread_id, sender=HELPER, stream_id="s1"),
            StreamEnd(thread_id=thread_id, sender=HELPER, stream_id="s2"),
            Idle(thread_id=thread_id, sender=HELPER),
            Left(thread_id=thread_id, sender=HELPER),
        ]

    assert cache.expired() == [
        ("acme", "t1", 1, ends("t1")),
        ("acme", "t2", 1, [*ends("t2"), Left(thread_id="t2", sender=ANA)]),
    ]
    assert cache.expired() == []
    assert cache.present("acme", "t1", 1) == [ANA]
    late: list[Signal] = [
        StreamDelta(thread_id="t1", sender=HELPER, stream_id="s1", index=1, text="more"),
        StreamEnd(thread_id="t1", sender=HELPER, stream_id="s1"),
        Idle(thread_id="t1", sender=HELPER),
        Left(thread_id="t1", sender=HELPER),
    ]
    assert [cache.apply("acme", 1, signal, "far", n) for n, signal in enumerate(late, 3)] == [
        ([], False)
    ] * 4
    again = [entry(HELPER, working="Writing", streams=("s1",))]
    assert cache.replace("acme", "t1", 1, "far", 7, again) == [
        Present(thread_id="t1", sender=HELPER),
        Working(thread_id="t1", sender=HELPER, label="Writing"),
        StreamStart(thread_id="t1", sender=HELPER, stream_id="s1"),
    ]
    start = StreamStart(thread_id="t1", sender=HELPER, stream_id="s1")
    assert cache.apply("acme", 1, start, "far", 8) == ([], False)


async def test_a_replica_that_starts_asks_the_others_to_announce_now(
    storage: PostgresStorage,
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    ana = await Peer.open(near, ANA)
    try:
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1")
        await far.start()
        helper = await Peer.open(far, HELPER)

        async def seen() -> None:
            while True:
                found = (await helper.call("thread.present", thread_id="t1"))["identities"]
                if [i["id"] for i in found] == ["ana"]:
                    return
                await asyncio.sleep(0.02)

        await asyncio.wait_for(seen(), 1.0)
        await helper.call("thread.join", thread_id="t1")
        assert pushed(helper) == [("present", "ana"), ("present", "helper")]
        await helper.close()
    finally:
        await ana.close()
        await near.stop()
        await far.stop()


class Recorded(PostgresStorage):
    def __init__(self, pool: Any) -> None:
        super().__init__(pool)
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, scope: str, thread_id: str, payload: dict[str, Any]) -> None:
        self.published.append((thread_id, payload))
        await super().publish(scope, thread_id, payload)


async def test_stop_and_delete_send_an_empty_snapshot_for_each_slice_held(pool: Any) -> None:
    storage = Recorded(pool)
    server = Server(storage)
    await server.start()
    ana = await Peer.open(server, ANA)
    for thread_id in ("t1", "t2", "t3"):
        await ana.call("thread.create", thread_id=thread_id)
        await ana.call("thread.join", thread_id=thread_id)
    await ana.call("signal.send", thread_id="t2", kind="working")
    await ana.quiet()
    epochs = {
        thread_id: await epoch(storage, "acme", thread_id) for thread_id in ("t1", "t2", "t3")
    }
    storage.published.clear()
    await ana.call("thread.delete", thread_id="t3")
    await server.stop()
    snapshots = [
        (thread_id, p["e"], p["part"], p["parts"], p["entries"])
        for thread_id, p in storage.published
        if p.get("kind") == "refresh"
    ]
    assert snapshots == [
        ("t3", epochs["t3"], 0, 1, []),
        ("t1", epochs["t1"], 0, 1, []),
        ("t2", epochs["t2"], 0, 1, []),
    ]
    await ana.close()


async def test_the_last_connection_to_leave_a_thread_sends_one_empty_snapshot(pool: Any) -> None:
    storage = Recorded(pool)
    server = Server(storage)
    await server.start()
    ana, tab = await Peer.open(server, ANA), await Peer.open(server, ANA)
    try:
        await ana.call("thread.create", thread_id="t1")
        for peer in (ana, tab):
            await peer.call("thread.join", thread_id="t1")
            await peer.quiet()
        storage.published.clear()
        await tab.call("thread.leave", thread_id="t1")
        await ana.call("signal.send", thread_id="t1", kind="working")
        await ana.call("thread.leave", thread_id="t1")
        await server.stop()
        assert [(p["kind"], p.get("entries")) for _, p in storage.published] == [
            ("working", None),
            ("idle", None),
            ("left", None),
            ("refresh", []),
        ]
    finally:
        await ana.close()
        await tab.close()


async def streaming(
    pool: Any, drop: Any = lambda notice: False
) -> tuple[Muffled, Server, Server, Peer, Peer]:
    muffled = Muffled(pool, drop)
    near, far = Server(PostgresStorage(pool)), Server(muffled)
    await near.start()
    await far.start()
    helper = await Peer.open(near, HELPER)
    ana = await Peer.open(far, ANA)
    await helper.call("thread.create", thread_id="t1")
    await helper.call("member.add", thread_id="t1", identity=ANA.model_dump(mode="json"))
    await helper.call("thread.join", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    await ana.quiet()
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    return muffled, near, far, helper, ana


def delta(helper: Peer, index: int, text: str) -> Any:
    return helper.call(
        "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=index, text=text
    )


async def test_a_remote_delta_that_skips_marks_the_stream_gapped_and_it_stops_growing(
    pool: Any,
) -> None:
    muffled, near, far, helper, ana = await streaming(
        pool, lambda notice: notice.payload.get("text") == "B"
    )
    try:
        for index, text in enumerate("ABC"):
            await delta(helper, index, text)
        await ana.push("stream.start", timeout=2.0)
        assert (await ana.push("stream.delta", timeout=2.0))["text"] == "A"
        assert [p["kind"] for p in await ana.quiet(0.5)] == []
        assert [n.payload["text"] for n in muffled.dropped] == ["B"]
        await delta(helper, 3, "D")
        assert [p["kind"] for p in await ana.quiet(0.5)] == []
    finally:
        await ana.close()
        await helper.close()
        await near.stop()
        await far.stop()


async def test_a_gapped_streams_end_and_message_still_arrive(pool: Any) -> None:
    _, near, far, helper, ana = await streaming(
        pool, lambda notice: notice.payload.get("text") == "B"
    )
    try:
        for index, text in enumerate("ABC"):
            await delta(helper, index, text)
        await say(helper, "t1", "ABC", client_id="s1")
        await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
        kinds = [p["kind"] for p in await ana.quiet(1.0)]
        assert kinds == ["stream.start", "stream.delta", "message", "stream.end"]
    finally:
        await ana.close()
        await helper.close()
        await near.stop()
        await far.stop()


async def test_pieces_of_an_oversized_delta_continue_each_other(pool: Any) -> None:
    _, near, far, helper, ana = await streaming(pool)
    big = "x" * 9000
    try:
        await delta(helper, 0, big)
        await delta(helper, 1, "!")
        deltas = [p for p in await ana.quiet(1.0) if p["kind"] == "stream.delta"]
        assert "".join(p["text"] for p in deltas) == big + "!"
    finally:
        await ana.close()
        await helper.close()
        await near.stop()
        await far.stop()
