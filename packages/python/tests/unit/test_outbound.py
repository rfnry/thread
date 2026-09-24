import asyncio
import json
from typing import Any

from until import until

from rfnry_thread.outbound import Outbound
from rfnry_thread.protocol import Push, Response

from .factories import HELPER


class Sink:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.gate = asyncio.Event()
        self.gate.set()

    async def send_text(self, data: str) -> None:
        await self.gate.wait()
        self.sent.append(json.loads(data))


def delta(text: str, index: int, stream: str = "s1") -> Push:
    return Push(
        name="signal",
        payload={
            "kind": "stream.delta",
            "thread_id": "t1",
            "sender": HELPER.model_dump(mode="json"),
            "stream_id": stream,
            "index": index,
            "text": text,
        },
    )


def event(seq: int) -> Push:
    return Push(name="event", payload={"kind": "message", "seq": seq})


async def nothing() -> None:
    return None


async def test_frames_leave_in_the_order_they_were_put() -> None:
    sink = Sink()
    queue = Outbound(sink, nothing)
    for seq in range(1, 6):
        queue.put(event(seq))
    queue.put(Response(id="1", payload={}))
    await queue.drain()
    assert [f.get("payload", {}).get("seq") for f in sink.sent] == [1, 2, 3, 4, 5, None]
    await queue.close()


async def test_deltas_waiting_for_a_slow_connection_are_merged_never_dropped() -> None:
    sink = Sink()
    sink.gate.clear()
    queue = Outbound(sink, nothing)
    queue.put(event(1))
    for index, text in enumerate(["Re", "fun", "ded"]):
        queue.put(delta(text, index))
    queue.put(delta("Other", 0, stream="s2"))
    sink.gate.set()
    await queue.drain()
    payloads = [f["payload"] for f in sink.sent]
    assert payloads[0]["seq"] == 1
    assert [(p["stream_id"], p["text"], p["index"]) for p in payloads[1:]] == [
        ("s1", "Refunded", 2),
        ("s2", "Other", 0),
    ]
    await queue.close()


async def test_merging_never_touches_a_frame_other_queues_hold() -> None:
    slow, fast = Sink(), Sink()
    slow.gate.clear()
    first, second = Outbound(slow, nothing), Outbound(fast, nothing)
    shared = delta("Re", 0)
    first.put(event(1))
    first.put(shared)
    second.put(shared)
    first.put(delta("funded", 1))
    await second.drain()
    assert shared.payload["text"] == "Re"
    assert fast.sent[0]["payload"]["text"] == "Re"
    slow.gate.set()
    await first.drain()
    assert slow.sent[1]["payload"]["text"] == "Refunded"
    await first.close()
    await second.close()


async def test_a_connection_over_its_bound_with_no_write_for_the_stall_window_is_closed() -> None:
    now = [0.0]
    stalled: list[bool] = []

    async def on_stall() -> None:
        stalled.append(True)

    sink = Sink()
    sink.gate.clear()
    queue = Outbound(sink, on_stall, bound=2, stall=10.0, clock=lambda: now[0])
    for seq in range(1, 5):
        queue.put(event(seq))
    await asyncio.sleep(0)
    assert not queue.closed and stalled == []
    now[0] = 11.0
    queue.put(event(5))
    await until(lambda: stalled == [True])
    assert queue.closed
    queue.put(event(6))
    await queue.close()


async def test_a_connection_that_keeps_writing_is_never_closed() -> None:
    now = [0.0]
    sink = Sink()
    queue = Outbound(sink, nothing, bound=2, stall=10.0, clock=lambda: now[0])
    for seq in range(1, 50):
        now[0] += 1.0
        queue.put(event(seq))
        await asyncio.sleep(0)
    await queue.drain()
    assert not queue.closed and len(sink.sent) == 49
    await queue.close()
