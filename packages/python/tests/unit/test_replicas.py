import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest
from until import until

from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server
from rfnry_thread.state import EXPIRY
from rfnry_thread.storage import OnEvent, OnResume, OnSignal, Stop

from .conftest import say
from .factories import ANA, BRUNO, HELPER
from .peer import Peer


@pytest.fixture
async def replicas(storage: PostgresStorage) -> AsyncIterator[tuple[Server, Server]]:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    yield near, far
    await near.stop()
    await far.stop()


async def room(near: Server, far: Server) -> tuple[Peer, Peer]:
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await ana.call("thread.join", thread_id="t1")
    helper = await Peer.open(far, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await ana.quiet()
    await helper.quiet()
    return ana, helper


async def test_an_event_on_one_replica_reaches_a_member_on_another_once(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    for n in range(3):
        await say(ana, "t1", f"m{n}")
    messages = [p for p in await helper.quiet() if p["kind"] == "message"]
    assert [p["content"][0]["text"] for p in messages] == ["m0", "m1", "m2"]
    assert [p["kind"] for p in await ana.quiet()].count("message") == 3


async def test_signals_are_mirrored_and_never_echoed_twice(replicas: tuple[Server, Server]) -> None:
    ana, helper = await room(*replicas)
    await helper.call("signal.send", thread_id="t1", kind="working", label="Reading")
    assert (await ana.push("working"))["label"] == "Reading"
    assert [p["kind"] for p in await helper.quiet()] == ["working"]
    assert [p["kind"] for p in await ana.quiet()] == []


async def test_the_finishing_message_reaches_a_remote_connection_before_stream_end(
    replicas: tuple[Server, Server],
) -> None:
    ana, helper = await room(*replicas)
    await helper.call("signal.send", thread_id="t1", kind="stream.start", stream_id="s1")
    await helper.notify(
        "signal.send", thread_id="t1", kind="stream.delta", stream_id="s1", index=0, text="Done."
    )
    await say(helper, "t1", "Done.", client_id="s1")
    await helper.call("signal.send", thread_id="t1", kind="stream.end", stream_id="s1")
    order = [p["kind"] for p in await ana.quiet() if p["kind"] in ("message", "stream.end")]
    assert order == ["message", "stream.end"]


async def test_an_invitation_and_a_deletion_cross_replicas(replicas: tuple[Server, Server]) -> None:
    near, far = replicas
    ana, helper = await room(near, far)
    bruno = await Peer.open(far, BRUNO)
    await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    assert (await bruno.push("invited"))["thread"]["id"] == "t1"
    await ana.call("thread.delete", thread_id="t1")
    assert (await helper.push("thread.deleted"))["sender"]["id"] == "ana"


async def test_a_replica_learns_who_is_here_from_the_re_announcement(
    storage: PostgresStorage,
) -> None:
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    far = Server(storage)
    await near.start()
    await far.start()
    await near._hub.announce()
    await until(lambda: far._hub.state.present("acme", "t1") == [ANA])
    now = far._hub.state._clock()
    far._hub.state._clock = lambda: now + EXPIRY + 1
    assert far._hub.state.present("acme", "t1") == []
    await near.stop()
    await far.stop()


async def test_a_resumed_listener_republishes_this_replicas_state(storage: PostgresStorage) -> None:
    resumes: list[OnResume | None] = []
    listen = storage.listen

    async def spy(
        on_event: OnEvent, on_signal: OnSignal, on_resume: OnResume | None = None
    ) -> Stop:
        resumes.append(on_resume)
        return await listen(on_event, on_signal, on_resume)

    storage.listen = spy  # type: ignore[method-assign]
    near = Server(storage)
    ana = await Peer.open(near, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("thread.join", thread_id="t1")
    far = Server(storage)
    await near.start()
    await far.start()
    assert far._hub.state.present("acme", "t1") == []
    resume = resumes[0]
    assert resume is not None
    await resume()
    await until(lambda: far._hub.state.present("acme", "t1") == [ANA])
    await near.stop()
    await far.stop()


async def test_stop_stops_listening_and_start_is_idempotent(storage: PostgresStorage) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await near.start()
    await far.start()
    ana, helper = await room(near, far)
    await far.stop()
    await far.stop()
    await say(ana, "t1", "unheard")
    assert [p for p in await helper.quiet() if p["kind"] == "message"] == []
    await near.stop()


async def test_a_thread_another_replica_wrote_between_a_listing_and_its_enter_is_followed_whole(
    replicas: tuple[Server, Server],
    storage: PostgresStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    near, far = replicas
    ana = await Peer.open(far, ANA)
    await ana.call("thread.create", thread_id="t1")
    await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
    await asyncio.sleep(0.3)
    said: list[str] = []
    listing = storage.list_threads

    async def listed(*args: Any, **kwargs: Any) -> Any:
        found = await listing(*args, **kwargs)
        if args[1] == "helper" and not said:
            said.append((await say(ana, "t1", "from far"))["id"])
            await asyncio.sleep(0.3)
        return found

    monkeypatch.setattr(storage, "list_threads", listed)
    helper = await Peer.open(near, HELPER)
    reply = await helper.call("thread.follow", since={"t1": 0})
    assert [j["seq"] for j in reply["joined"]] == [3]
    pushed = [p["content"][0]["text"] for p in await helper.quiet() if p["kind"] == "message"]
    assert pushed == ["from far"]


async def test_a_removal_after_a_follow_listing_cuts_the_follower_when_its_notice_is_late(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(storage)
    heard = near._hub.on_event

    async def late(scope: str, thread_id: str, seq: int) -> None:
        await asyncio.sleep(0.3)
        await heard(scope, thread_id, seq)

    monkeypatch.setattr(near._hub, "on_event", late)
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(far, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await say(ana, "t1", "one")
        await asyncio.sleep(0.6)
        listing = storage.list_threads
        removed: list[bool] = []

        async def listed(*args: Any, **kwargs: Any) -> Any:
            found = await listing(*args, **kwargs)
            if args[1] == "helper" and not removed:
                removed.append(True)
                await ana.call("member.remove", thread_id="t1", identity_id="helper")
            return found

        monkeypatch.setattr(storage, "list_threads", listed)
        helper = await Peer.open(near, HELPER)
        await helper.call("thread.follow", since={"t1": 0})
        kinds = [p["kind"] for p in await helper.quiet(0.6) if "seq" in p]
        assert kinds[-1] == "member_removed"
        await say(ana, "t1", "said after helper was removed")
        assert await helper.quiet(0.8) == []
    finally:
        await near.stop()
        await far.stop()


async def test_a_notice_landing_while_a_follower_enters_is_delivered_at_once(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    near, far = Server(storage), Server(storage)
    await near.start()
    await far.start()
    try:
        ana = await Peer.open(far, ANA)
        await ana.call("thread.create", thread_id="t1")
        await ana.call("member.add", thread_id="t1", identity=HELPER.model_dump(mode="json"))
        await say(ana, "t1", "one")
        await asyncio.sleep(0.3)
        real = storage.list_events
        done: list[bool] = []

        async def replaying(*args: Any, **kwargs: Any) -> Any:
            found = await real(*args, **kwargs)
            if kwargs.get("since") == 0 and not done:
                done.append(True)
                await say(ana, "t1", "late")
                await asyncio.sleep(0.4)
            return found

        monkeypatch.setattr(storage, "list_events", replaying)
        helper = await Peer.open(near, HELPER)
        await helper.call("thread.follow", since={"t1": 0})
        pushed = [
            p["content"][0]["text"] for p in await helper.quiet(0.6) if p["kind"] == "message"
        ]
        assert pushed == ["one", "late"]
    finally:
        await near.stop()
        await far.stop()
