import asyncio
import random
from typing import Any

import pytest
from database import open_pool
from unit.factories import ANA, BRUNO, HELPER
from unit.notices import Muffled
from unit.peer import Peer

from rfnry_thread import server as server_module
from rfnry_thread.client import Client
from rfnry_thread.server import Server


def events(pushed: list[dict[str, Any]]) -> list[int]:
    return [p["seq"] for p in pushed if "author" in p]


async def history(peer: Peer) -> list[dict[str, Any]]:
    return list((await peer.call("thread.history", thread_id="t1", limit=500))["events"])


@pytest.mark.parametrize("rate", [0.3, 1.0])
async def test_every_connection_ends_with_storages_events_when_notices_drop(
    pool: Any, monkeypatch: pytest.MonkeyPatch, rate: float
) -> None:
    monkeypatch.setattr(server_module, "REFRESH", 0.2)
    far_pool = await open_pool()
    near_dice, far_dice = random.Random(7), random.Random(7)
    near = Server(Muffled(pool, lambda notice: near_dice.random() < rate))
    far = Server(Muffled(far_pool, lambda notice: far_dice.random() < rate))
    await near.start()
    await far.start()
    writers: list[Client] = []
    try:
        ana = await Peer.open(near, ANA)
        await ana.call("thread.create", thread_id="t1")
        for identity in (BRUNO, HELPER):
            await ana.call("member.add", thread_id="t1", identity=identity.model_dump(mode="json"))
        await ana.call("thread.join", thread_id="t1", since=0)
        helper = await Peer.open(far, HELPER)
        await helper.call("thread.join", thread_id="t1", since=0)
        bruno = await Peer.open(far, BRUNO)
        await bruno.call("thread.follow", since={"t1": {"seq": 0}})
        writers.append(await near.connect(identity=ANA, scope="acme"))
        writers.append(await far.connect(identity=HELPER, scope="acme"))

        async def said(numbers: range) -> None:
            await asyncio.gather(
                *(writers[n % 2].send("t1", f"m{n}") for n in numbers),
            )

        await said(range(16))
        await far.remove_member("acme", "t1", BRUNO)
        await said(range(16, 31))
        await far.add_member("acme", "t1", BRUNO)
        await said(range(31, 40))
        await asyncio.sleep(1.0)
        pushed = {
            name: await peer.quiet(0.5)
            for name, peer in (("ana", ana), ("helper", helper), ("bruno", bruno))
        }
        stored = await history(ana)
        assert len(stored) == 45
        assert events(pushed["ana"]) == events(stored)
        assert events(pushed["helper"]) == events(await history(helper))
        out = next(e["seq"] for e in stored if e["kind"] == "member_removed")
        back = next(e["seq"] for e in stored if e["kind"] == "member_added" and e["seq"] > out)
        kept = [s for s in events(await history(bruno)) if not out < s < back]
        assert events(pushed["bruno"]) == kept
        assert [(j["thread"]["id"], j["seq"] >= back) for j in bruno.joined] == [("t1", True)]
    finally:
        for writer in writers:
            await writer.close()
        await near.stop()
        await far.stop()
        await asyncio.wait_for(far_pool.close(), 5)
