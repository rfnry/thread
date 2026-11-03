import asyncio
import contextlib
import inspect
import random
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

import pytest
from pydantic import ValidationError
from until import until

from rfnry_thread import client as client_module
from rfnry_thread.client import Client, Dial, Item
from rfnry_thread.connection import Connection
from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    NotFoundError,
    StorageError,
    ThreadException,
    TransportError,
)
from rfnry_thread.models import (
    SYSTEM,
    Cancel,
    Custom,
    Edit,
    Event,
    Identity,
    Image,
    Invited,
    Joined,
    Marked,
    MemberAdded,
    MemberRemoved,
    Message,
    Present,
    Read,
    Retract,
    StatusChanged,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Thread,
    ThreadDeleted,
    Working,
)
from rfnry_thread.pipe import End, pipe
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.protocol import ErrorFrame, Failure, Push, Response, encode
from rfnry_thread.resolve import resolve
from rfnry_thread.server import Server

from .factories import ANA, AT, BRUNO, HELPER, Lookalike, added, lookalike, message, removed
from .legacy import enroll
from .writes import admit, append, expel


async def take[T](
    client: Client, kind: type[T], include_own: bool = False, timeout: float = 1.0
) -> T:
    async def find() -> T:
        async for item in client.events(include_own=include_own):
            if isinstance(item, kind):
                return item
        raise AssertionError("the stream ended")

    return await asyncio.wait_for(find(), timeout)


async def drain(client: Client, wait: float = 0.15, include_own: bool = True) -> list[Item]:
    items: list[Item] = []

    async def collect() -> None:
        async for item in client.events(include_own=include_own):
            items.append(item)

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(collect(), wait)
    return items


async def pair(server: Server) -> tuple[Client, Client]:
    ana = await server.connect(identity=ANA, scope="acme")
    helper = await server.connect(identity=HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.join("t1")
    await helper.join("t1")
    await drain(ana)
    await drain(helper)
    return ana, helper


async def test_an_in_process_client_knows_who_and_where_it_is(server: Server) -> None:
    client = await server.connect(identity=ANA, scope="acme")
    assert (client.identity, client.scope) == (ANA, "acme")
    with pytest.raises(ConfigurationError):
        await server.connect(identity=SYSTEM, scope="acme")
    await client.close()


async def test_getting_started(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    helper = await server.connect(identity=HELPER, scope="acme")
    thread = await ana.create_thread(thread_id="support-ana")
    await ana.add_member(thread.id, helper.identity)
    await helper.join(thread.id)
    await ana.send(thread.id, "My invoice was charged twice.")
    item = await take(helper, Message)
    reply = await helper.send(thread.id, "Looking into it.", in_reply_to=item.id)
    assert reply.in_reply_to == item.id and reply.author == HELPER
    assert item.content == [Text(text="My invoice was charged twice.")]


async def test_threads_and_members(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    thread = await ana.create_thread(thread_id="support-ana", metadata={"topic": "billing"})
    assert (await ana.get_thread(thread.id)).metadata == {"topic": "billing"}
    assert [t.id for t in await ana.list_threads(limit=20)] == ["support-ana"]
    assert (await ana.update_thread(thread.id, {"topic": "refunds"})).metadata == {
        "topic": "refunds"
    }
    member = await ana.add_member(thread.id, BRUNO)
    assert member.added_by == ANA
    assert [m.identity.id for m in await ana.list_members(thread.id)] == ["ana", "bruno"]
    await ana.remove_member(thread.id, BRUNO)
    assert [m.identity.id for m in await ana.list_members(thread.id)] == ["ana"]
    await ana.delete_thread(thread.id)
    assert await ana.list_threads() == []


async def test_own_items_are_left_out_unless_asked_for(server: Server) -> None:
    ana, helper = await pair(server)
    await ana.send("t1", "mine")
    await helper.send("t1", "theirs")
    received = [i for i in await drain(ana, include_own=False) if isinstance(i, Message)]
    assert [m.content for m in received] == [[Text(text="theirs")]]


async def test_every_writing_verb_returns_the_stored_event(server: Server) -> None:
    ana, helper = await pair(server)
    image = Image(media_type="image/png", url="https://files.example.com/receipt.png")
    message = await ana.send("t1", [Text(text="Here is the receipt."), image])
    assert message.content[1] == image and message.seq > 0
    assert isinstance(await ana.edit("t1", message.id, "Here is the right receipt."), Edit)
    step = await helper.step(
        "t1",
        name="Read",
        input={"path": "rules/refunds.md"},
        output=[Text(text="A double charge is refunded in full.")],
        duration_ms=12,
        in_reply_to=message.id,
        triggered_by=ANA,
    )
    assert isinstance(step, Step) and step.triggered_by == ANA and step.duration_ms == 12
    assert isinstance(await ana.read("t1", up_to_seq=message.seq), Read)
    assert isinstance(await ana.custom("t1", "rating", {"stars": 5}), Custom)
    assert isinstance(await ana.retract("t1", message.id), Retract)
    view = resolve(await ana.history("t1"))
    assert message.id not in [e.id for e in view]


async def test_a_caller_supplied_client_id_stores_one_event_and_returns_it_twice(
    server: Server,
) -> None:
    ana, helper = await pair(server)
    first = await helper.send("t1", "Your plan renews Friday.", client_id="renewal-ana-2026-10")
    again = await helper.send(
        "t1", "Your plan renews Friday, again.", client_id="renewal-ana-2026-10"
    )
    assert again == first and first.client_id == "renewal-ana-2026-10"
    step = await helper.step("t1", name="Read", client_id="read-1")
    assert await helper.step("t1", name="Read", client_id="read-1") == step
    edit = await helper.edit("t1", first.id, "Renews Friday.", client_id="edit-1")
    assert await helper.edit("t1", first.id, "Renews Friday.", client_id="edit-1") == edit
    custom = await helper.custom("t1", "rating", {"stars": 5}, client_id="rating-1")
    assert await helper.custom("t1", "rating", {"stars": 4}, client_id="rating-1") == custom
    retract = await helper.retract("t1", step.id, client_id="retract-1")
    assert await helper.retract("t1", step.id, client_id="retract-1") == retract
    mine = [e.client_id for e in await ana.history("t1") if e.author == HELPER]
    assert mine == ["renewal-ana-2026-10", "read-1", "edit-1", "rating-1", "retract-1"]


async def test_a_stream_is_signals_then_a_message_with_the_stream_id(server: Server) -> None:
    ana, helper = await pair(server)
    async with helper.stream("t1", in_reply_to="e1") as reply:
        for chunk in ["The second charge ", "was refunded."]:
            await reply.write(chunk)
    items = [i for i in await drain(ana) if not isinstance(i, Present)]
    assert [type(i) for i in items][0] is StreamStart
    assert (
        "".join(i.text for i in items if isinstance(i, StreamDelta))
        == "The second charge was refunded."
    )
    message = next(i for i in items if isinstance(i, Message))
    assert message.client_id == reply.id and message.content == [
        Text(text="The second charge was refunded.")
    ]
    assert items.index(message) < next(n for n, i in enumerate(items) if isinstance(i, StreamEnd))


async def test_finish_stores_other_content_and_an_exception_abandons(server: Server) -> None:
    ana, helper = await pair(server)
    async with helper.stream("t1") as reply:
        await reply.write("Here it is")
        stored = await reply.finish(
            [Text(text="Here it is"), Image(media_type="image/png", url="https://x/a.png")]
        )
    assert len(stored.content) == 2
    with pytest.raises(RuntimeError):
        async with helper.stream("t1") as abandoned:
            await abandoned.write("never mind")
            raise RuntimeError("boom")
    history = [e for e in await ana.history("t1") if isinstance(e, Message)]
    assert [e.client_id for e in history] == [reply.id]


async def test_a_message_carries_its_metadata_through_send_stream_and_history(
    server: Server,
) -> None:
    ana, helper = await pair(server)
    question = await ana.send("t1", "Charged twice?")
    assert question.metadata == {}
    sent = await helper.send("t1", "Refunded.", in_reply_to=question.id, metadata={"run_id": "r1"})
    assert sent.metadata == {"run_id": "r1"}
    async with helper.stream("t1", in_reply_to=question.id, metadata={"run_id": "r2"}) as reply:
        await reply.write("Refunded ")
        await reply.write("in full.")
    async with helper.stream("t1", metadata={"run_id": "r3"}) as other:
        finished = await other.finish([Text(text="Here it is")])
    assert finished.metadata == {"run_id": "r3"}
    received = [i for i in await drain(ana, include_own=False) if isinstance(i, Message)]
    assert [m.metadata for m in received] == [{"run_id": "r1"}, {"run_id": "r2"}, {"run_id": "r3"}]
    await helper.edit("t1", sent.id, "Refunded in full.")
    view = [e for e in resolve(await ana.history("t1")) if isinstance(e, Message)]
    assert [(m.content, m.metadata) for m in view] == [
        ([Text(text="Charged twice?")], {}),
        ([Text(text="Refunded in full.")], {"run_id": "r1"}),
        ([Text(text="Refunded in full.")], {"run_id": "r2"}),
        ([Text(text="Here it is")], {"run_id": "r3"}),
    ]


async def test_a_stream_sets_its_metadata_when_it_finishes(server: Server) -> None:
    ana, helper = await pair(server)
    opening = {"run_id": "r1", "through": 0}
    async with helper.stream("t1", metadata=opening) as reply:
        await reply.write("Refunded.")
        finished = await reply.finish(metadata={"through": 7})
    async with helper.stream("t1") as bare:
        await bare.write("Done.")
        only = await bare.finish(metadata={"through": 8})
    stored = await ana.get_event("t1", finished.id)
    assert isinstance(stored, Message)
    assert finished.metadata == stored.metadata == {"run_id": "r1", "through": 7}
    assert only.metadata == {"through": 8} and opening == {"run_id": "r1", "through": 0}


async def test_metadata_over_the_cap_after_the_merge_abandons_the_stream(server: Server) -> None:
    ana, helper = await pair(server)
    async with helper.stream("t1", metadata={"a": "x" * 5000}) as reply:
        await reply.write("Refunded.")
        with pytest.raises(InputError, match="^server: message metadata is at most 8192 bytes$"):
            await reply.finish(metadata={"b": "y" * 5000})
    items = await drain(ana)
    assert not [i for i in items if isinstance(i, Message)]
    assert [i for i in items if isinstance(i, StreamEnd)]


async def test_a_finish_that_cannot_merge_its_metadata_still_ends_the_stream(
    server: Server,
) -> None:
    ana, helper = await pair(server)
    with pytest.raises(TypeError):
        async with helper.stream("t1", metadata={"run_id": "r1"}) as reply:
            await reply.write("Refunded.")
            await reply.finish(metadata=cast(Any, ["through"]))
    items = await drain(ana)
    assert not [i for i in items if isinstance(i, Message)]
    assert [i for i in items if isinstance(i, StreamEnd)]


async def test_a_member_reads_an_event_by_its_id(server: Server) -> None:
    ana, helper = await pair(server)
    reply = await helper.send("t1", "Refunded.", metadata={"run_id": "r1"})
    read = await ana.get_event("t1", reply.id)
    await ana.create_thread(thread_id="t2")
    elsewhere = await ana.send("t2", "Hello.")
    with pytest.raises(NotFoundError, match=f"^server: event '{elsewhere.id}' was not found$"):
        await ana.get_event("t1", elsewhere.id)
    bruno = await server.connect(identity=BRUNO, scope="acme")
    with pytest.raises(ForbiddenError):
        await bruno.get_event("t1", reply.id)
    assert isinstance(read, Message) and read == reply and read.metadata == {"run_id": "r1"}
    assert await server.get_event("acme", "t1", reply.id) == reply
    with pytest.raises(NotFoundError):
        await server.get_event("globex", "t1", reply.id)


def test_metadata_is_keyword_only_at_the_end() -> None:
    for verb in (Client.send, Client.stream):
        parameters = list(inspect.signature(verb).parameters.values())
        assert parameters[-1].name == "metadata"
        assert parameters[-1].kind is inspect.Parameter.KEYWORD_ONLY


async def test_signals_arrive_typed(server: Server) -> None:
    ana, helper = await pair(server)
    await helper.working("t1", label="Checking the invoice")
    assert (await take(ana, Working)).label == "Checking the invoice"
    await ana.cancel("t1", to=helper.identity)
    assert (await take(helper, Cancel)).to == HELPER
    assert sorted(i.id for i in await helper.present("t1")) == ["ana", "helper"]


async def test_an_assistant_can_speak_first(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    reminders = await server.connect(identity=HELPER, scope="acme")
    renewal = await reminders.create_thread(thread_id="renewal-ana-2026-10")
    await reminders.add_member(renewal.id, ANA)
    await reminders.send(renewal.id, "Your plan renews Friday.")
    invited = await take(ana, Invited)
    assert invited.thread.id == renewal.id
    joined = await ana.join(invited.thread_id, since=0)
    assert joined.seq == 3
    assert [e.kind for e in await ana.history(renewal.id)] == [
        "member_added",
        "member_added",
        "message",
    ]


async def test_server_errors_are_raised_with_their_class_and_message(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    with pytest.raises(ForbiddenError, match="^server: 'bruno' is not a member of thread 't1'$"):
        await bruno.send("t1", "hi")
    with pytest.raises(ForbiddenError, match="^server: signals need a join"):
        await ana.working("t1")


async def test_a_closed_client_says_so_once_ends_its_stream_and_refuses_calls(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    opened = ana.status
    await ana.close()
    await ana.close()
    assert (opened, ana.status) == ("open", "closed")
    assert [i async for i in ana.events()] == [StatusChanged(status="closed")]
    assert [i async for i in ana.events()] == []
    with pytest.raises(TransportError, match="^client: the client is closed$"):
        await ana.create_thread()


async def test_every_verb_reads_an_identity_by_its_fields(server: Server) -> None:
    ana = await server.connect(identity=lookalike(ANA), scope="acme")
    helper = await server.connect(identity=lookalike(HELPER), scope="acme")
    assert (ana.identity, helper.identity) == (ANA, HELPER)
    await ana.create_thread(thread_id="t1")
    assert (await ana.add_member("t1", lookalike(HELPER))).identity == HELPER
    assert (await server.add_member("acme", "t1", lookalike(BRUNO))).identity == BRUNO
    await helper.join("t1")
    await ana.join("t1")
    sent = await ana.send("t1", "hi", triggered_by=lookalike(HELPER))
    step = await helper.step("t1", name="Read", triggered_by=lookalike(ANA))
    async with helper.stream("t1", triggered_by=lookalike(ANA)) as reply:
        await reply.write("Looking.")
        streamed = await reply.finish()
    assert (sent.triggered_by, step.triggered_by, streamed.triggered_by) == (HELPER, ANA, ANA)
    await ana.cancel("t1", to=lookalike(HELPER))
    assert (await take(helper, Cancel)).to == HELPER
    await ana.remove_member("t1", lookalike(BRUNO))
    assert [m.identity.id for m in await ana.list_members("t1")] == ["ana", "helper"]


async def test_a_value_that_is_not_an_identity_is_refused_at_the_boundary(server: Server) -> None:
    ana, _ = await pair(server)
    robot: Any = Lookalike(cast(Any, "robot"), "r2")
    with pytest.raises(InputError, match="^models: not an identity"):
        await ana.send("t1", "hi", triggered_by=robot)
    with pytest.raises(InputError, match="^models: not an identity"):
        await server.connect(identity=object(), scope="acme")  # type: ignore[arg-type]
    with pytest.raises(ConfigurationError, match="^server: 'system' is the library's role"):
        await server.connect(identity=lookalike(SYSTEM), scope="acme")


def test_serve_and_connect_take_who_and_where_by_keyword() -> None:
    for verb in (Server.serve, Server.connect):
        parameters = inspect.signature(verb).parameters
        assert parameters["identity"].kind is inspect.Parameter.KEYWORD_ONLY
        assert parameters["scope"].kind is inspect.Parameter.KEYWORD_ONLY
    listing = inspect.signature(Server.list_threads).parameters
    assert [name for name, p in listing.items() if p.kind is p.KEYWORD_ONLY] == [
        "metadata",
        "before",
        "limit",
        "order",
        "preview",
        "query",
    ]


async def test_a_client_follows_its_threads_and_is_handed_new_ones(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    helper = await server.connect(identity=HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    [joined] = await helper.follow()
    assert joined.thread.id == "t1"
    await ana.create_thread(thread_id="t2")
    await ana.add_member("t2", HELPER)
    await ana.send("t2", "hello")
    kinds = [type(item).__name__ for item in await drain(helper, include_own=False)]
    assert kinds == ["Invited", "Joined", "MemberAdded", "Joined", "Message"]


async def test_mark_returns_the_stored_position_and_tells_the_markers_other_clients(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    phone = await server.connect(identity=HELPER, scope="acme")
    laptop = await server.connect(identity=HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    message = await ana.send("t1", "hi")
    assert await phone.mark("t1", message.seq) == message.seq
    assert await phone.mark("t1", 0) == message.seq
    told = [item for item in await drain(laptop, include_own=False) if isinstance(item, Marked)]
    assert [(m.thread_id, m.seq, m.unread) for m in told] == [("t1", message.seq, 0)]
    assert [item for item in await drain(phone) if isinstance(item, Marked)] == []


async def test_mark_back_lowers_the_mark_and_tells_the_markers_other_clients(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    phone = await server.connect(identity=HELPER, scope="acme")
    laptop = await server.connect(identity=HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    first = await ana.send("t1", "one")
    second = await ana.send("t1", "two")
    assert await phone.mark("t1", second.seq) == second.seq
    await drain(laptop)
    assert await phone.mark("t1", first.seq - 1, back=True) == first.seq - 1
    told = [item for item in await drain(laptop, include_own=False) if isinstance(item, Marked)]
    assert [(m.seq, m.unread) for m in told] == [(first.seq - 1, 2)]
    assert await laptop.marked("t1") == first.seq - 1


async def test_marked_reads_the_callers_own_mark_without_moving_it(
    server: Server, storage: PostgresStorage
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    helper = await server.connect(identity=HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await enroll(storage, "t1", HELPER)
    message = await ana.send("t1", "hi")
    assert await helper.marked("t1") is None
    await helper.mark("t1", message.seq)
    assert await helper.marked("t1") == message.seq
    assert await helper.marked("t1") == message.seq
    zed = Identity(id="zed", role="user", name="Zed")
    outsider = await server.connect(identity=zed, scope="acme")
    with pytest.raises(ForbiddenError, match="^server: 'zed' is not a member of thread 't1'$"):
        await outsider.marked("t1")


async def test_a_follower_that_joins_since_a_seq_reads_the_thread_from_it_once(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    for n in range(3):
        await ana.send("t1", f"m{n}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    joined = await page.join("t1", since=0)
    assert joined.seq == 5
    seqs = [item.seq for item in await drain(page) if isinstance(item, Message | MemberAdded)]
    assert seqs == [1, 2, 3, 4, 5]
    await ana.send("t1", "live")
    said = [item for item in await drain(page) if isinstance(item, Message)]
    assert [(m.seq, m.content) for m in said] == [(6, [Text(text="live")])]


async def test_a_follower_joining_since_a_seq_while_a_live_event_lands_gets_each_event_once(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    for n in range(3):
        await ana.send("t1", f"m{n}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    request = page._request

    async def land(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            live = await ana.send("t1", "live")
            await until(lambda: held(page, "t1", live.seq))
        return await request(method, payload)

    page._request = land  # type: ignore[method-assign]
    joined = await page.join("t1", since=0)
    assert joined.seq == 6
    items = await drain(page)
    assert sorted(item.seq for item in items if isinstance(item, Message | MemberAdded)) == [
        1,
        2,
        3,
        4,
        5,
        6,
    ]
    said = [
        part.text
        for item in items
        if isinstance(item, Message)
        for part in item.content
        if isinstance(part, Text)
    ]
    assert sorted(said) == ["live", "m0", "m1", "m2"]
    assert page._cursors["t1"] == 6
    await ana.send("t1", "after")
    assert [item.seq for item in await drain(page) if isinstance(item, Message)] == [7]


async def widen_arrival(server: Server, who: str, ana: Client, *texts: str) -> None:
    arrive = server._hub._arrive
    widened: list[str] = []

    async def slow(session: Any, thread_id: str) -> None:
        if session.identity.id == who and not widened:
            widened.append(thread_id)
            for text in texts:
                await ana.send(thread_id, text)
            await asyncio.sleep(0.05)
        await arrive(session, thread_id)

    server._hub._arrive = slow  # type: ignore[method-assign]


def held(client: Client, thread_id: str, seq: int) -> bool:
    hold = client._holds.get(thread_id)
    return hold is not None and any(event.seq == seq for event in hold.events)


def said(items: Sequence[Item]) -> list[str]:
    return [
        part.text
        for item in items
        if isinstance(item, Message)
        for part in item.content
        if isinstance(part, Text)
    ]


async def test_a_join_past_the_head_keeps_the_event_that_races_its_answer(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    page = await server.connect(identity=HELPER, scope="acme")
    await widen_arrival(server, HELPER.id, ana, "raced")
    joined = await asyncio.wait_for(page.join("t1", since=40), 2)
    assert joined.truncated
    await ana.send("t1", "after")
    assert said(await drain(page)) == ["raced", "after"]


async def test_a_join_past_the_head_emits_the_events_around_the_sent_position_in_order(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    head = (await ana.history("t1"))[-1].seq
    page = await server.connect(identity=HELPER, scope="acme")
    await widen_arrival(server, HELPER.id, ana, "a", "b", "c", "d")
    joined = await asyncio.wait_for(page.join("t1", since=head + 3), 2)
    assert joined.seq == head + 4
    items = await drain(page)
    assert said(items) == ["a", "b", "c", "d"]
    assert [item.seq for item in items if isinstance(item, Message)] == [
        head + 1,
        head + 2,
        head + 3,
        head + 4,
    ]
    assert page._cursors["t1"] == head + 4


async def test_overlapping_joins_each_keep_their_own_hold(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    page = await server.connect(identity=HELPER, scope="acme")
    request = page._request
    entered = asyncio.Event()
    gate = asyncio.Event()

    async def gated(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        reply = await request(method, payload)
        if payload.get("since") == 40:
            entered.set()
            await gate.wait()
        return reply

    page._request = gated  # type: ignore[method-assign]
    past = asyncio.create_task(page.join("t1", since=40))
    await asyncio.wait_for(entered.wait(), 2)
    await ana.send("t1", "raced")
    await asyncio.sleep(0.05)
    plain = asyncio.create_task(page.join("t1"))
    gate.set()
    assert (await asyncio.wait_for(past, 2)).truncated
    await asyncio.wait_for(plain, 2)
    await ana.send("t1", "after")
    assert said(await drain(page)) == ["raced", "after"]
    assert not page._holds


async def test_a_join_whose_answer_cannot_be_read_leaves_no_hold(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    page = await server.connect(identity=HELPER, scope="acme")
    request = page._request
    garbled = [True]

    async def unreadable(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        reply = await request(method, payload)
        if garbled:
            garbled.clear()
            await ana.send("t1", "raced")
            await asyncio.sleep(0.05)
            return {"garbled": True}
        return reply

    page._request = unreadable  # type: ignore[method-assign]
    with pytest.raises(ValidationError):
        await page.join("t1", since=40)
    assert not page._holds
    await page.join("t1", since=40)
    await ana.send("t1", "after")
    assert said(await drain(page)) == ["after"]


async def test_a_join_retried_after_a_timeout_keeps_the_events_after_its_answer(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    page = await server.connect(identity=HELPER, scope="acme")
    arrive = server._hub._arrive
    arrived = asyncio.Event()

    async def slow(session: Any, thread_id: str) -> None:
        if session.identity.id == HELPER.id:
            await asyncio.sleep(0.2)
            arrived.set()
        await arrive(session, thread_id)

    server._hub._arrive = slow  # type: ignore[method-assign]
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(page.join("t1", since=40), 0.05)
    await asyncio.wait_for(arrived.wait(), 2)
    server._hub._arrive = arrive  # type: ignore[method-assign]
    await asyncio.wait_for(page.join("t1", since=40), 2)
    await ana.send("t1", "after")
    assert said(await drain(page)) == ["after"]


@pytest.mark.parametrize("left", [False, True], ids=["fresh", "left"])
async def test_overlapping_joins_past_the_head_trust_no_cursor_the_first_one_guessed(
    server: Server, left: bool
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    page = await server.connect(identity=HELPER, scope="acme")
    if left:
        await page.join("t1")
        await page.leave("t1")
    await widen_arrival(server, HELPER.id, ana, "raced")
    first, _ = await asyncio.wait_for(
        asyncio.gather(page.join("t1", since=40), page.join("t1", since=40)), 2
    )
    assert first.truncated
    await ana.send("t1", "after")
    await ana.send("t1", "later")
    assert said(await drain(page)) == ["raced", "after", "later"]


async def test_overlapping_joins_past_the_head_emit_each_event_once(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    page = await server.connect(identity=HELPER, scope="acme")
    request = page._request
    gates = [asyncio.Event(), asyncio.Event()]
    entered: list[asyncio.Event] = [asyncio.Event(), asyncio.Event()]
    calls: list[int] = []

    async def gated(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        n = len(calls)
        calls.append(n)
        reply = await request(method, payload)
        entered[n].set()
        await gates[n].wait()
        return reply

    page._request = gated  # type: ignore[method-assign]
    first = asyncio.create_task(page.join("t1", since=40))
    await asyncio.wait_for(entered[0].wait(), 2)
    second = asyncio.create_task(page.join("t1", since=40))
    await ana.send("t1", "raced")
    await asyncio.sleep(0.05)
    gates[0].set()
    await asyncio.wait_for(first, 2)
    await asyncio.wait_for(entered[1].wait(), 2)
    gates[1].set()
    await asyncio.wait_for(second, 2)
    await ana.send("t1", "after")
    assert said(await drain(page)) == ["raced", "after"]


async def test_a_plain_join_leaves_a_stale_cursor_and_the_past_the_head_join_still_keeps_the_event(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    page = await server.connect(identity=HELPER, scope="acme")
    request = page._request
    go = asyncio.Event()

    async def delayed(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("since") == 40:
            await asyncio.wait_for(go.wait(), 2)
        return await request(method, payload)

    page._request = delayed  # type: ignore[method-assign]
    await asyncio.wait_for(page.join("t1"), 2)
    past = asyncio.create_task(page.join("t1", since=40))
    await asyncio.sleep(0.02)
    await ana.send("t1", "raced")
    await asyncio.sleep(0.05)
    go.set()
    await asyncio.wait_for(past, 2)
    await ana.send("t1", "after")
    assert said(await drain(page)) == ["raced", "after"]


async def test_a_rewind_does_not_emit_again_a_copy_of_an_event_emitted_before_a_hold_registered(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    for n in range(3):
        await ana.send("t1", f"m{n}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.join("t1")
    await drain(page)
    request = page._request
    go = asyncio.Event()
    finished = asyncio.Event()

    async def ordered(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("since") == 0:
            await asyncio.wait_for(go.wait(), 2)
        reply = await request(method, payload)
        if payload.get("since") == 40:
            await asyncio.wait_for(finished.wait(), 2)
        return reply

    page._request = ordered  # type: ignore[method-assign]
    rewind = asyncio.create_task(page.join("t1", since=0))
    await until(lambda: "t1" in page._holds)
    live = await ana.send("t1", "live")
    await until(lambda: held(page, "t1", live.seq))
    past = asyncio.create_task(page.join("t1", since=40))
    await asyncio.sleep(0.05)
    go.set()
    await asyncio.wait_for(rewind, 2)
    finished.set()
    await asyncio.wait_for(past, 2)
    await ana.send("t1", "after")
    heard = said(await drain(page))
    assert heard.count("live") == 1
    assert heard[-1] == "after"


async def test_a_follow_into_threads_made_again_keeps_the_event_that_races_its_answer(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    for thread_id in ("t1", "t2"):
        await ana.create_thread(thread_id=thread_id)
        await ana.add_member(thread_id, HELPER)
        await ana.send(thread_id, "first")
    page = await server.connect(identity=HELPER, scope="acme")
    page._epochs.update({"t1": 0, "t2": 0})
    page._cursors.update({"t1": 40, "t2": 40})
    enter = server._hub._enter

    async def slow(session: Any, thread: Any, *args: Any) -> Any:
        entered = await enter(session, thread, *args)
        if thread.id == "t1":
            await ana.send("t1", "raced")
            await asyncio.sleep(0.05)
        return entered

    server._hub._enter = slow  # type: ignore[method-assign,assignment]
    found = await asyncio.wait_for(page._follow(True, {"t1": 40, "t2": 40}, None), 2)
    assert [joined.truncated for joined in found] == [True, True]
    await ana.send("t1", "after")
    items = await drain(page)
    assert said([i for i in items if isinstance(i, Message) and i.thread_id == "t1"]) == [
        "first",
        "raced",
        "after",
    ]
    assert said([i for i in items if isinstance(i, Message) and i.thread_id == "t2"]) == ["first"]


async def test_overlapping_joins_since_a_followed_thread_each_replay_their_own_window(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    for n in range(18):
        await ana.send("t1", f"m{n}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    assert page._cursors["t1"] == 20
    request = page._request
    joining: list[int] = []
    second: list[asyncio.Task[Any]] = []

    async def overlap(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            joining.append(payload["since"])
            if payload["since"] == 5:
                second.append(asyncio.create_task(page.join("t1", since=10)))
            else:
                await widen_arrival(server, HELPER.id, ana, "live")
        return await request(method, payload)

    page._request = overlap  # type: ignore[method-assign]
    first = await page.join("t1", since=5)
    other = await second[0]
    assert joining == [5, 10]
    assert (first.seq, other.seq) == (20, 21)
    seqs = sorted(item.seq for item in await drain(page) if isinstance(item, Message))
    assert seqs == sorted([*range(6, 21), *range(11, 21), 21])
    await ana.send("t1", "after")
    assert [item.seq for item in await drain(page) if isinstance(item, Message)] == [22]


async def test_a_second_join_of_a_thread_waits_for_the_first_answer(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    await ana.send("t1", "first")
    page = await server.connect(identity=HELPER, scope="acme")
    request = page._request
    joins: list[int | None] = []
    gate = asyncio.Event()

    async def gated(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            joins.append(payload["since"])
        reply = await request(method, payload)
        if payload.get("since") == 0:
            await gate.wait()
        return reply

    page._request = gated  # type: ignore[method-assign]
    first = asyncio.create_task(page.join("t1", since=0))
    await until(lambda: joins == [0])
    second = asyncio.create_task(page.join("t1"))
    await asyncio.sleep(0.05)
    assert joins == [0]
    gate.set()
    await asyncio.wait_for(asyncio.gather(first, second), 2)
    assert joins == [0, None]
    await ana.send("t1", "after")
    assert said(await drain(page)) == ["first", "after"]


async def test_a_rewind_on_a_joined_thread_delivers_from_since_in_order_once(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    page = await server.connect(identity=HELPER, scope="acme")
    joined = await page.join("t1")
    for n in range(2):
        await ana.send("t1", f"m{n}")
    assert said(await drain(page)) == ["m0", "m1"]
    request = page._request

    async def landed(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            live = await ana.send("t1", "live")
            await until(lambda: held(page, "t1", live.seq))
        return await request(method, payload)

    page._request = landed  # type: ignore[method-assign]
    await asyncio.wait_for(page.join("t1", since=joined.seq), 2)
    await ana.send("t1", "after")
    items = await drain(page)
    assert said(items) == ["m0", "m1", "live", "after"]
    assert [item.seq for item in items if isinstance(item, Message)] == [
        joined.seq + n for n in range(1, 5)
    ]


async def test_a_rewind_across_an_own_removal_and_a_later_add_delivers_every_event_from_its_since(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    await bruno.send("t1", "a")
    await bruno.remove_member("t1", ANA)
    await bruno.send("t1", "b")
    await bruno.add_member("t1", ANA)
    await bruno.send("t1", "c")
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.join("t1")
    await drain(ana)
    await ana.join("t1", since=0)
    live = await bruno.send("t1", "live")
    items = await drain(ana)
    assert [
        (type(i).__name__, i.seq)
        for i in items
        if isinstance(i, MemberAdded | MemberRemoved | Message)
    ] == [
        ("MemberAdded", 1),
        ("MemberAdded", 2),
        ("Message", 3),
        ("MemberRemoved", 4),
        ("Message", 5),
        ("MemberAdded", 6),
        ("Message", 7),
        ("Message", 8),
    ]
    assert ana._cursors["t1"] == live.seq and "t1" in ana._present


async def test_a_join_whose_thread_is_made_again_before_its_answer_admits_nothing_of_the_old_one(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    page = await server.connect(identity=HELPER, scope="acme")
    await page.join("t1")
    await drain(page)
    request = page._request

    async def remade(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            old = await bruno.send("t1", "old")
            await until(lambda: held(page, "t1", old.seq))
            await server.delete_thread("acme", "t1")
            await bruno.create_thread(thread_id="t1")
            await bruno.add_member("t1", HELPER)
            await bruno.send("t1", "new")
        return await request(method, payload)

    page._request = remade  # type: ignore[method-assign]
    joined = await asyncio.wait_for(page.join("t1", since=0), 2)
    items = await drain(page)
    gone = next(n for n, item in enumerate(items) if isinstance(item, ThreadDeleted))
    assert said(items[:gone]) == ["old"]
    after = [item for item in items[gone + 1 :] if isinstance(item, MemberAdded | Message)]
    assert [item.seq for item in after] == [1, 2, 3] == list(range(1, joined.seq + 1))
    assert said(after) == ["new"]


async def test_a_failed_join_keeps_the_cursor_of_a_thread_the_client_follows(
    storage: PostgresStorage,
) -> None:
    refusing = False

    async def authorize(actor: Identity, action: str, thread: Thread) -> bool:
        return not (refusing and actor.id == HELPER.id)

    server = Server(storage, authorize=authorize)
    ana = await server.connect(identity=ANA, scope="acme")
    for thread_id in ("t1", "t2"):
        await ana.create_thread(thread_id=thread_id)
    await ana.add_member("t1", HELPER)
    old = await ana.send("t1", "old")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    refusing = True
    for since in (None, 0):
        with pytest.raises(ForbiddenError):
            await page.join("t1", since=since)
        assert page._cursors["t1"] == old.seq
        page._receive(
            Push(name="event", payload=old.model_dump(mode="json")),
            cast(Connection, page._connection),
        )
        assert [item for item in await drain(page) if isinstance(item, Message)] == []
    with pytest.raises(ForbiddenError):
        await page.join("t2")
    assert "t2" not in page._cursors
    live = await ana.send("t1", "live")
    assert [item.seq for item in await drain(page) if isinstance(item, Message)] == [live.seq]


async def test_an_assistant_that_joins_with_resume_hears_what_was_said_while_it_was_away(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    first = await ana.send("t1", "hello")
    helper = await server.connect(identity=HELPER, scope="acme")
    first_join = await helper.join("t1", resume=True)
    heard = await take(helper, Message)
    assert heard.id == first.id
    assert first_join.thread.marked == 1
    await helper.mark("t1", heard.seq)
    await helper.close()
    away = await ana.send("t1", "sent while it was away")
    again = await server.connect(identity=HELPER, scope="acme")
    joined = await again.join("t1", resume=True)
    assert (joined.seq, joined.truncated, joined.thread.marked) == (away.seq, False, heard.seq)
    said = [item for item in await drain(again, include_own=False) if isinstance(item, Message)]
    assert [m.id for m in said] == [away.id]
    live = await ana.send("t1", "live")
    said = [item for item in await drain(again, include_own=False) if isinstance(item, Message)]
    assert [m.id for m in said] == [live.id]
    with pytest.raises(InputError, match="^server: a join takes since or resume, not both$"):
        await again.join("t1", since=0, resume=True)


async def test_a_follower_that_joins_with_resume_reads_the_thread_from_its_mark_once(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    for n in range(3):
        await ana.send("t1", f"m{n}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    await page.mark("t1", 3)
    joined = await page.join("t1", resume=True)
    assert joined.seq == 5
    assert [item.seq for item in await drain(page) if isinstance(item, Message)] == [4, 5]
    assert page._cursors["t1"] == 5
    await ana.send("t1", "live")
    assert [item.seq for item in await drain(page) if isinstance(item, Message)] == [6]


async def test_a_join_with_resume_beyond_the_replay_limit_hands_the_mark_to_read_the_rest(
    storage: PostgresStorage,
) -> None:
    server = Server(storage, replay_limit=2)
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    sent = [await ana.send("t1", f"m{n}") for n in range(4)]
    helper = await server.connect(identity=HELPER, scope="acme")
    joined = await helper.join("t1", resume=True)
    assert (joined.seq, joined.truncated) == (sent[-1].seq, True)
    replayed = [item for item in await drain(helper) if isinstance(item, Message)]
    assert [m.id for m in replayed] == [m.id for m in sent[2:]]
    assert joined.thread.marked is not None
    rest = await helper.history("t1", after=joined.thread.marked, before=replayed[0].seq)
    assert [e.id for e in rest if isinstance(e, Message)] == [m.id for m in sent[:2]]


async def test_a_join_sends_resume_only_when_asked(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    sent: list[dict[str, Any]] = []
    request = ana._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append(payload)
        return await request(method, payload)

    ana._request = record  # type: ignore[method-assign]
    await ana.join("t1")
    await ana.join("t1", resume=True)
    assert sent == [
        {"thread_id": "t1", "since": None},
        {"thread_id": "t1", "since": None, "resume": True},
    ]


async def test_history_and_list_threads_leave_out_the_new_fields_at_their_defaults(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    sent: list[tuple[str, dict[str, Any]]] = []
    request = ana._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append((method, payload))
        return await request(method, payload)

    ana._request = record  # type: ignore[method-assign]
    await ana.history("t1")
    await ana.list_threads()
    await ana.history("t1", after=0)
    await ana.list_threads(order="activity", preview=True)
    await ana.list_threads(metadata={"team": "ops"}, query="pump")
    await ana.search("t1", "pump")
    assert sent == [
        ("thread.history", {"thread_id": "t1", "before": None, "limit": 50, "kinds": None}),
        ("thread.list", {"before": None, "limit": 50}),
        (
            "thread.history",
            {"thread_id": "t1", "before": None, "limit": 50, "kinds": None, "after": 0},
        ),
        ("thread.list", {"before": None, "limit": 50, "order": "activity", "preview": True}),
        (
            "thread.list",
            {"before": None, "limit": 50, "metadata": {"team": "ops"}, "query": "pump"},
        ),
        ("thread.search", {"thread_id": "t1", "query": "pump", "before": None, "limit": 50}),
    ]


async def test_history_reads_forward_and_the_inbox_lists_by_activity(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    for thread_id in ("t1", "t2"):
        await ana.create_thread(thread_id=thread_id)
    first = await ana.send("t1", "one")
    await ana.send("t1", "two")
    assert [e.seq for e in await ana.history("t1", after=first.seq)] == [first.seq + 1]
    listed = await ana.list_threads(order="activity", preview=True)
    assert [t.id for t in listed] == ["t1", "t2"]
    assert listed[0].last_message is not None
    assert listed[0].last_message.content == [Text(text="two")]


async def test_a_page_after_the_last_thread_neither_repeats_nor_skips(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    for thread_id in "abcdef":
        await ana.create_thread(thread_id=thread_id)
        await ana.send(thread_id, f"hi {thread_id}")
    page = await ana.list_threads(order="activity", limit=3)
    assert [t.id for t in page] == ["f", "e", "d"]
    await ana.send("d", "new activity on the anchor")
    rest = await ana.list_threads(order="activity", before=page[-1], limit=3)
    assert [t.id for t in rest] == ["c", "b", "a"]


async def test_an_anchor_without_activity_pages_from_its_creation(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    for thread_id in "abc":
        await ana.create_thread(thread_id=thread_id)
    page = await ana.list_threads(order="activity", limit=2)
    assert [t.id for t in page] == ["c", "b"]
    assert page[-1].last_message_at is None
    await ana.send("b", "now it has activity")
    rest = await ana.list_threads(order="activity", before=page[-1])
    assert [t.id for t in rest] == ["a"]


async def test_a_thread_read_by_id_anchors_a_page_like_its_id(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    for thread_id in "abc":
        await ana.create_thread(thread_id=thread_id)
    await ana.send("a", "late")
    plain = await ana.get_thread("a")
    assert plain.last_seq is None
    rest = await ana.list_threads(order="activity", before=plain)
    assert [t.id for t in rest] == ["c", "b"]


async def test_an_anchor_id_is_checked_like_any_other_id(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    anchor = Thread(id="x" * 257, scope="acme", created_at=AT)
    with pytest.raises(InputError, match="server: a thread id is at most 256 bytes"):
        await ana.list_threads(before=anchor)
    with pytest.raises(InputError, match="server: text cannot hold a NUL character"):
        await ana.list_threads(before=anchor.model_copy(update={"id": "a\x00b"}))


async def test_a_follow_replay_for_a_thread_without_a_cursor_arrives_once_before_its_joined(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    for text in ("one", "two"):
        await ana.send("t1", text)
    helper = await server.connect(identity=HELPER, scope="acme")
    await helper.follow(resume=True)
    await ana.send("t1", "three")
    items = await drain(helper)
    assert [(type(i).__name__, getattr(i, "seq", None)) for i in items] == [
        ("MemberAdded", 2),
        ("Message", 3),
        ("Message", 4),
        ("Joined", 4),
        ("Message", 5),
    ]


async def test_a_push_the_client_cannot_read_is_skipped(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", BRUNO)
    await ana.join("t1")
    await bruno.join("t1")
    await drain(ana)
    await drain(bruno)
    session = next(s for s in server._hub.sessions if s.identity == ANA)
    connection = ana._connection
    session.outbound.put(
        Push(name="signal", payload={"kind": "from.the.future", "thread_id": "t1"})
    )
    await bruno.send("t1", "still here")
    assert (await take(ana, Message)).content == [Text(text="still here")]
    assert ana.status == "open" and ana._connection is connection
    assert len(server._hub.sessions) == 2
    await ana.close()
    await bruno.close()


async def test_a_push_with_a_name_the_client_does_not_know_is_skipped_and_the_connection_kept() -> (
    None
):
    ours, theirs = pipe()
    dials = 0

    async def dial() -> Connection:
        nonlocal dials
        dials += 1
        return ours

    async def answer() -> None:
        await theirs.receive_text()
        await theirs.send_text(hello_reply())

    helper = asyncio.create_task(answer())
    client = await Client._open(dial)
    await helper
    await theirs.send_text('{"type":"push","name":"from.the.future","payload":{"thread_id":"t1"}}')
    await theirs.send_text(
        encode(Push(name="event", payload=message(1, "still here").model_dump(mode="json")))
    )
    assert (await take(client, Message, include_own=True)).content == [Text(text="still here")]
    assert dials == 1 and client.status == "open"
    await client.close()


async def test_a_frame_that_is_not_a_push_and_cannot_be_read_still_loses_the_connection() -> None:
    ours, theirs = pipe()
    dials = 0

    async def dial() -> Connection:
        nonlocal dials
        dials += 1
        return ours

    async def answer() -> None:
        await theirs.receive_text()
        await theirs.send_text(hello_reply())

    helper = asyncio.create_task(answer())
    client = await Client._open(dial)
    await helper
    await theirs.send_text('{"type":"from.the.future"}')
    await until(lambda: client.status == "reconnecting" or dials == 2)
    await client.close()


async def test_a_push_whose_media_has_neither_data_nor_url_is_skipped(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", BRUNO)
    await ana.join("t1")
    await bruno.join("t1")
    await drain(ana)
    await drain(bruno)
    session = next(s for s in server._hub.sessions if s.identity == ANA)
    connection = ana._connection
    unreadable = {
        "kind": "message",
        "thread_id": "t1",
        "seq": 99,
        "author": {"role": "user", "id": "bruno"},
        "content": [{"type": "image", "media_type": "image/png"}],
    }
    session.outbound.put(Push(name="event", payload=unreadable))
    await bruno.send("t1", "still here")
    assert (await take(ana, Message)).content == [Text(text="still here")]
    assert ana.status == "open" and ana._connection is connection
    await ana.close()
    await bruno.close()


class Silent:
    def __init__(self) -> None:
        self.closed = False

    async def receive_text(self) -> str:
        await asyncio.sleep(3600)
        return ""

    async def send_text(self, data: str) -> None:
        return None

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        self.closed = True


def hello_reply() -> str:
    return encode(
        Response(id="hello", payload={"identity": {"role": "user", "id": "ana"}, "scope": "acme"})
    )


async def test_close_during_a_reconnect_hello_closes_that_connection() -> None:
    first, theirs = pipe()
    silent = Silent()
    dials = 0

    async def dial() -> Connection:
        nonlocal dials
        dials += 1
        return first if dials == 1 else cast(Connection, silent)

    async def answer() -> None:
        await theirs.receive_text()
        await theirs.send_text(hello_reply())

    helper = asyncio.create_task(answer())
    client = await Client._open(dial)
    await helper
    await theirs.close()
    await until(lambda: dials == 2)
    await asyncio.sleep(0.05)
    await client.close()
    assert silent.closed


async def test_a_hello_refused_on_reconnect_ends_the_client() -> None:
    first, theirs = pipe()
    second, there = pipe()
    dials = 0

    async def dial() -> Connection:
        nonlocal dials
        dials += 1
        return first if dials == 1 else second

    async def answer() -> None:
        await theirs.receive_text()
        await theirs.send_text(hello_reply())

    async def refuse() -> None:
        await there.receive_text()
        await there.send_text(
            encode(ErrorFrame(id="hello", error=Failure(code="input", message="server: no")))
        )

    helper = asyncio.create_task(answer())
    client = await Client._open(dial)
    await helper
    refusal = asyncio.create_task(refuse())
    await theirs.close()
    await refusal

    async def read() -> None:
        async for _ in client.events():
            pass

    with pytest.raises(InputError, match="^server: no$"):
        await asyncio.wait_for(read(), 3.0)
    assert client.status == "closed" and dials == 2
    assert second.closed


async def test_a_hello_refused_for_its_deadline_keeps_the_client_reconnecting() -> None:
    first, theirs = pipe()
    dials = 0
    refused = encode(
        ErrorFrame(
            id="hello",
            error=Failure(code="transport", message="server: no hello within 10 seconds"),
        )
    )

    async def dial() -> Connection:
        nonlocal dials
        dials += 1
        if dials == 1:
            return first
        ours, there = pipe()
        asyncio.create_task(answer(there, refused if dials == 2 else hello_reply()))
        return ours

    async def answer(end: Connection, reply: str) -> None:
        await end.receive_text()
        await end.send_text(reply)

    helper = asyncio.create_task(answer(theirs, hello_reply()))
    client = await Client._open(dial)
    await helper
    await theirs.close()
    await until(lambda: client.status == "open" and dials == 3)
    await client.close()


async def test_a_hello_reply_without_an_identity_keeps_the_client_reconnecting() -> None:
    first, theirs = pipe()
    second, there = pipe()
    third, over = pipe()
    ends = [first, second, third]

    async def dial() -> Connection:
        return ends.pop(0)

    async def answer(end: Connection, reply: str) -> None:
        await end.receive_text()
        await end.send_text(reply)

    bad = encode(Response(id="hello", payload={"scope": "acme"}))
    helper = asyncio.create_task(answer(theirs, hello_reply()))
    client = await Client._open(dial)
    await helper
    tasks = [
        asyncio.create_task(answer(there, bad)),
        asyncio.create_task(answer(over, hello_reply())),
    ]
    await theirs.close()
    await asyncio.wait_for(asyncio.gather(*tasks), 3.0)
    await until(lambda: client.status == "open" and client._connection is not None)
    assert second.closed
    await client.close()


async def test_a_connection_that_ends_on_garbage_is_closed_and_the_client_reconnects() -> None:
    first, theirs = pipe()
    second, _ = pipe()
    ends = [first, second]

    async def dial() -> Connection:
        return ends.pop(0)

    async def answer() -> None:
        await theirs.receive_text()
        await theirs.send_text(hello_reply())
        await theirs.send_text("garbage")

    helper = asyncio.create_task(answer())
    client = await Client._open(dial)
    await helper
    await until(lambda: client.status == "reconnecting")
    assert first.closed
    await client.close()


async def test_a_follower_removed_and_added_again_behind_the_hub_gets_its_cursor_back(
    storage: PostgresStorage,
) -> None:
    server = Server(storage)
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", BRUNO)
    await bruno.follow()
    await until(lambda: bruno._cursors.get("t1") is not None, timeout=5)
    await append(storage, "acme", message(90, "behind the hub"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await append(storage, "acme", message(92, "while out"))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    sent = await ana.send("t1", "after")
    await until(lambda: bruno._cursors.get("t1") == sent.seq, timeout=5)
    live = await ana.send("t1", "live")
    await until(lambda: bruno._cursors.get("t1") == live.seq, timeout=5)


async def test_a_client_not_following_is_invited_when_added_again_behind_the_hub(
    storage: PostgresStorage,
) -> None:
    server = Server(storage)
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", BRUNO)
    await bruno.join("t1")
    await append(storage, "acme", message(90, "behind the hub"))
    await expel(storage, "acme", removed("t1", BRUNO, ANA))
    await admit(storage, "acme", added("t1", BRUNO, ANA))
    await ana.send("t1", "after")
    assert (await take(bruno, Invited, timeout=5)).thread_id == "t1"


async def dials_that_drop(lives: list[float], dialled: list[float], keep: asyncio.Event) -> Dial:
    loop = asyncio.get_running_loop()

    async def serve(end: Connection, life: float | None) -> None:
        await end.receive_text()
        await end.send_text(hello_reply())
        if life is None:
            keep.set()
            return
        await asyncio.sleep(life)
        await end.close()

    async def dial() -> Connection:
        dialled.append(loop.time())
        ours, there = pipe()
        index = len(dialled) - 1
        asyncio.create_task(serve(there, lives[index] if index < len(lives) else None))
        return ours

    return dial


async def test_a_connection_that_dies_right_after_its_hello_backs_off_before_the_next_dial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.05)
    monkeypatch.setattr(random, "random", lambda: 1.0)
    dialled: list[float] = []
    keep = asyncio.Event()
    dial = await dials_that_drop([0.0, 0.0, 0.0, 0.0], dialled, keep)
    client = await Client._open(dial)
    await asyncio.wait_for(keep.wait(), 3.0)
    await client.close()
    gaps = [later - earlier for earlier, later in zip(dialled, dialled[1:], strict=False)]
    assert len(gaps) == 4
    assert gaps[1] >= 0.045 and gaps[2] >= 0.095 and gaps[3] >= 0.195


async def test_a_connection_that_stayed_open_past_the_healthy_window_redials_at_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 3.0)
    monkeypatch.setattr(client_module, "HEALTHY", 0.1)
    monkeypatch.setattr(random, "random", lambda: 1.0)
    dialled: list[float] = []
    keep = asyncio.Event()
    dial = await dials_that_drop([0.0, 0.15], dialled, keep)
    client = await Client._open(dial)
    await asyncio.wait_for(keep.wait(), 3.0)
    await client.close()
    gaps = [later - earlier for earlier, later in zip(dialled, dialled[1:], strict=False)]
    assert len(gaps) == 2
    assert 0.15 <= gaps[1] < 1.5


async def test_close_while_backing_off_ends_the_reconnect_and_dials_no_more(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 5.0)
    monkeypatch.setattr(random, "random", lambda: 1.0)
    dialled: list[float] = []
    keep = asyncio.Event()
    dial = await dials_that_drop([0.0, 0.0], dialled, keep)
    client = await Client._open(dial)
    await until(lambda: len(dialled) == 2 and client._attempt == 2)
    reconnector = client._reconnector
    await asyncio.wait_for(client.close(), 1.0)
    assert reconnector is not None and reconnector.done()
    await asyncio.sleep(0.05)
    assert len(dialled) == 2


async def dropped(client: Client) -> list[Item]:
    connection = client._connection
    assert connection is not None
    await connection.close()
    await until(lambda: client.status == "open" and client._connection is not connection)
    return await drain(client)


def failing_once(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, name: str
) -> list[bool]:
    armed = [True]
    real = getattr(storage, name)

    async def flaky(*args: Any, **kwargs: Any) -> Any:
        if armed:
            armed.pop()
            raise StorageError(f"postgres: storage rejected {name}: connection reset")
        return await real(*args, **kwargs)

    monkeypatch.setattr(storage, name, flaky)
    return armed


def dialling(client: Client) -> list[int]:
    dials: list[int] = []
    dial = client._dial

    async def counted() -> Connection:
        dials.append(1)
        return await dial()

    client._dial = counted
    return dials


def reopened(client: Client, connection: Connection) -> bool:
    return client.status == "open" and client._connection not in (None, connection)


def ana_in(server: Server, thread_id: str) -> bool:
    return any(s.identity.id == ANA.id and thread_id in s.joined for s in server._hub.sessions)


def ana_follows(server: Server) -> list[bool]:
    return [s.following for s in server._hub.sessions if s.identity.id == ANA.id]


async def test_a_rejoin_refused_by_a_storage_failure_is_tried_again_on_the_same_connection(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    server = Server(storage)
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    await ana.join("t1")
    await drain(ana)
    dials = dialling(ana)
    armed = failing_once(storage, monkeypatch, "get_thread")
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(ana, connection) and ana_in(server, "t1"))
    items = await drain(ana)
    assert not armed and dials == [1] and "t1" in ana._present
    told = [i.status if isinstance(i, StatusChanged) else type(i).__name__ for i in items]
    assert told == ["reconnecting", "open", "Present", "Joined"]
    sent = await bruno.send("t1", "after the reconnect")
    assert (await take(ana, Message)).id == sent.id


async def test_a_refollow_refused_by_a_storage_failure_joins_each_thread_and_follows_again(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    server = Server(storage)
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    await ana.follow()
    await drain(ana)
    dials = dialling(ana)
    armed = failing_once(storage, monkeypatch, "list_threads")
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(ana, connection) and ana_follows(server) == [True])
    assert not armed and dials == [1] and ana._following
    await drain(ana)
    await bruno.create_thread(thread_id="t2")
    await bruno.add_member("t2", ANA)
    assert (await take(ana, Joined)).thread.id == "t2"


def unreliable(broken: set[str]) -> Callable[[Identity, str, Thread], Awaitable[bool]]:
    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        if thread.id in broken:
            raise RuntimeError("policy service timed out")
        return True

    return authorize


async def test_a_thread_whose_rejoin_keeps_failing_is_tried_again_while_the_others_resume(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    broken: set[str] = set()
    server = Server(storage, authorize=unreliable(broken))
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ANA)
        await ana.join(thread_id)
    await drain(ana)
    dials = dialling(ana)
    broken.add("t1")
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(ana, connection))
    sent = await bruno.send("t2", "in the healthy thread")
    assert (await take(ana, Message)).id == sent.id
    assert set(ana._cursors) == {"t1", "t2"} and not ana_in(server, "t1") and dials == [1]
    broken.clear()
    assert (await take(ana, Joined, timeout=3)).thread.id == "t1"
    sent = await bruno.send("t1", "in the thread that came back")
    assert (await take(ana, Message)).id == sent.id and dials == [1]


async def test_a_refollow_that_keeps_failing_joins_each_thread_and_is_tried_again(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    broken: set[str] = set()
    server = Server(storage, authorize=unreliable(broken))
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ANA)
    await ana.follow()
    await drain(ana)
    dials = dialling(ana)
    broken.add("t1")
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(ana, connection))
    sent = await bruno.send("t2", "in the healthy thread")
    assert (await take(ana, Message)).id == sent.id
    assert ana._following and ana._refollow and dials == [1]
    broken.clear()
    await until(lambda: ana_follows(server) == [True], timeout=3)
    await bruno.create_thread(thread_id="t3")
    await bruno.add_member("t3", ANA)
    found = [i for i in await drain(ana, 0.3) if isinstance(i, Joined)]
    assert {j.thread.id for j in found} >= {"t1", "t3"} and dials == [1]


async def ended(client: Client) -> ThreadException:
    async def read() -> None:
        async for _ in client.events():
            pass

    with pytest.raises(ThreadException) as raised:
        await asyncio.wait_for(read(), 3.0)
    assert client.status == "closed"
    return raised.value


@pytest.mark.parametrize("verb", ["join", "follow"])
@pytest.mark.parametrize("refusal", [InputError, ConfigurationError])
async def test_a_rejoin_or_refollow_refused_as_a_wrong_request_ends_the_client_with_that_error(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch, verb: str, refusal: type[Exception]
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    server = Server(storage)
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    await (ana.follow() if verb == "follow" else ana.join("t1"))
    await drain(ana)
    dials: list[int] = []
    dial = ana._dial

    async def counted() -> Connection:
        dials.append(1)
        return await dial()

    async def refused(*args: Any, **kwargs: Any) -> Any:
        raise refusal("server: invalid thread.join payload: extra inputs are not permitted")

    ana._dial = counted
    monkeypatch.setattr(server._verbs, verb, refused)
    connection = ana._connection
    assert connection is not None
    await connection.close()
    error = await ended(ana)
    assert type(error) is refusal and str(error).startswith("server: invalid thread.join payload")
    assert dials == [1] and ana._connection is None
    await ana.close()


async def remade_while_away(server: Server, follow: bool) -> tuple[Client, list[Item], list[int]]:
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    if follow:
        await ana.follow()
    else:
        await ana.join("t1")
    for n in range(3):
        await bruno.send("t1", f"old {n}")
    await until(lambda: ana._cursors.get("t1") == 5)
    await drain(ana)
    released = asyncio.Event()
    dial = ana._dial

    async def held() -> Connection:
        await released.wait()
        return await dial()

    ana._dial = held
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await until(lambda: ana.status == "reconnecting")
    await server.delete_thread("acme", "t1")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    for n in range(4):
        await bruno.send("t1", f"new {n}")
    released.set()
    await until(lambda: ana.status == "open")
    log = [e.seq for e in await bruno.history("t1")]
    return ana, await drain(ana), log


@pytest.mark.parametrize("follow", [False, True])
async def test_a_thread_made_again_while_the_client_was_away_is_resumed_whole_and_truncated(
    server: Server, follow: bool
) -> None:
    ana, items, log = await remade_while_away(server, follow)
    joined = [i for i in items if isinstance(i, Joined)]
    assert [(j.seq, j.truncated) for j in joined] == [(6, True)]
    assert (
        [i.seq for i in items if isinstance(i, MemberAdded | Message)] == log == [1, 2, 3, 4, 5, 6]
    )
    assert said(items) == ["new 0", "new 1", "new 2", "new 3"]
    assert ana._cursors["t1"] == 6


async def back_after(client: Client, meanwhile: Callable[[], Awaitable[None]]) -> list[Item]:
    released = asyncio.Event()
    dial = client._dial

    async def held() -> Connection:
        await released.wait()
        return await dial()

    client._dial = held
    connection = client._connection
    assert connection is not None
    await connection.close()
    await until(lambda: client.status == "reconnecting")
    await meanwhile()
    released.set()
    await until(lambda: client.status == "open")
    client._dial = dial
    return [i for i in await drain(client) if isinstance(i, Joined | MemberAdded | Message)]


async def test_a_rejoin_answered_with_another_epoch_delivers_the_new_thread_from_its_start_in_order(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    await ana.join("t1")
    await drain(ana)

    async def missed() -> None:
        await bruno.send("t1", "missed")

    items = await back_after(ana, missed)
    assert [type(i).__name__ for i in items] == ["Message", "Joined"]
    assert said(items) == ["missed"]

    async def made_again() -> None:
        await server.delete_thread("acme", "t1")
        await bruno.create_thread(thread_id="t1", history="since_added")
        for n in range(3):
            await bruno.send("t1", f"before the add {n}")
        await bruno.add_member("t1", ANA)
        for n in range(2):
            await bruno.send("t1", f"new {n}")
        await widen_arrival(server, ANA.id, bruno, "raced")

    items = await back_after(ana, made_again)
    assert [type(i).__name__ for i in items] == [
        "Joined",
        "MemberAdded",
        "Message",
        "Message",
        "Message",
    ]
    assert isinstance(items[0], Joined) and items[0].truncated
    assert said(items) == ["new 0", "new 1", "raced"]
    assert [i.seq for i in items if isinstance(i, MemberAdded | Message)] == [5, 6, 7, 8]
    await bruno.send("t1", "after")
    assert said(await drain(ana)) == ["after"]


@pytest.mark.parametrize("verb", ["join", "follow"])
async def test_a_thread_made_again_arrives_once_from_its_start_when_its_rejoin_or_refollow_fails_after_the_replay(
    server: Server, monkeypatch: pytest.MonkeyPatch, verb: str
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    await (ana.follow() if verb == "follow" else ana.join("t1"))
    await bruno.send("t1", "old")
    await until(lambda: ana._cursors.get("t1") == 3)
    await drain(ana)
    request = ana._request
    failed: list[str] = []

    async def failing(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        reply = await request(method, payload)
        if method == f"thread.{verb}" and not failed:
            failed.append(method)
            raise StorageError("postgres: storage rejected the reply")
        return reply

    async def made_again() -> None:
        await server.delete_thread("acme", "t1")
        await bruno.create_thread(thread_id="t1")
        await bruno.add_member("t1", ANA)
        for n in range(3):
            await bruno.send("t1", f"new {n}")
        ana._request = failing  # type: ignore[method-assign]

    items = await back_after(ana, made_again)
    await until(lambda: not ana._unjoined and not ana._refollow)
    items += [i for i in await drain(ana) if isinstance(i, Joined | MemberAdded | Message)]
    assert failed and isinstance(items[0], Joined) and items[0].truncated
    assert [i.seq for i in items if isinstance(i, MemberAdded | Message)] == [1, 2, 3, 4, 5]
    assert said(items) == ["new 0", "new 1", "new 2"]


def gated(client: Client) -> asyncio.Event:
    released = asyncio.Event()
    dial = client._dial

    async def held_back() -> Connection:
        await released.wait()
        return await dial()

    client._dial = held_back
    return released


def failing_once_after_the_answer(client: Client, name: str) -> list[str]:
    request = client._request
    failed: list[str] = []

    async def failing(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        reply = await request(method, payload)
        if method == name and not failed:
            failed.append(method)
            raise StorageError("postgres: storage rejected the reply")
        return reply

    client._request = failing  # type: ignore[method-assign]
    return failed


async def remade_with(server: Server, bruno: Client, who: Identity) -> None:
    await server.delete_thread("acme", "t1")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", who)
    for n in range(3):
        await bruno.send("t1", f"new {n}")


def whole_from_its_start(items: list[Item]) -> None:
    assert isinstance(items[0], Joined) and items[0].truncated
    assert [i.seq for i in items if isinstance(i, MemberAdded | Message)] == [1, 2, 3, 4, 5]
    assert said(items) == ["new 0", "new 1", "new 2"]


async def test_a_join_answered_as_its_connection_drops_settles_nothing_on_the_next_one(
    server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    await bruno.send("t1", "old")
    await ana.join("t1")
    await drain(ana)
    released = gated(ana)
    theirs = cast(End, cast(End, ana._connection).peer)
    send = theirs.send_text

    async def closing_after_the_answer(data: str) -> None:
        await send(data)
        if '"truncated"' in data:
            await theirs.close()

    theirs.send_text = closing_after_the_answer  # type: ignore[method-assign]
    await ana.join("t1")
    assert ana.status == "reconnecting" and ana._connection is None
    await remade_with(server, bruno, ANA)
    failed = failing_once_after_the_answer(ana, "thread.join")
    released.set()
    await until(lambda: bool(failed) and ana.status == "open" and not ana._unjoined)
    whole_from_its_start(
        [i for i in await drain(ana) if isinstance(i, Joined | MemberAdded | Message)]
    )


async def test_a_joined_push_read_after_its_connection_was_lost_settles_nothing_on_the_next_one(
    server: Server, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(client_module, "RECONNECT", 0.01)
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    await bruno.send("t1", "old")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    released = gated(page)
    old = cast(End, page._connection)
    joined = Joined(thread=await bruno.get_thread("t1"), seq=3)
    await cast(End, old.peer).send_text(
        encode(Push(name="joined", payload=joined.model_dump(mode="json")))
    )
    await old.close()
    page._lost(old)
    assert [type(i).__name__ for i in await drain(page)] == ["StatusChanged", "Joined"]
    await remade_with(server, bruno, HELPER)
    failed = failing_once_after_the_answer(page, "thread.follow")
    released.set()
    await until(lambda: bool(failed) and page.status == "open" and not page._refollow)
    whole_from_its_start(
        [i for i in await drain(page) if isinstance(i, Joined | MemberAdded | Message)]
    )


async def test_a_leave_called_behind_two_joins_of_a_thread_goes_out_after_both(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    request = ana._request
    wire: list[str] = []

    async def recorded(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        wire.append(method)
        return await request(method, payload)

    ana._request = recorded  # type: ignore[method-assign]
    await asyncio.wait_for(asyncio.gather(ana.join("t1"), ana.join("t1"), ana.leave("t1")), 2)
    assert wire == ["thread.join", "thread.join", "thread.leave"]
    assert not ana._cursors and not ana._present and not ana_in(server, "t1")
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(ana, connection))
    assert wire == ["thread.join", "thread.join", "thread.leave"] and not ana_in(server, "t1")


def methods(client: Client) -> list[str]:
    request = client._request
    sent: list[str] = []

    async def recorded(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append(f"{method}:{payload.get('thread_id', '')}")
        return await request(method, payload)

    client._request = recorded  # type: ignore[method-assign]
    return sent


def in_room(server: Server, who: Identity, thread_id: str) -> tuple[bool, bool]:
    sessions = [s for s in server._hub.sessions if s.identity.id == who.id]
    return (
        any(thread_id in s.joined for s in sessions),
        any(thread_id in s.present for s in sessions),
    )


@pytest.mark.parametrize(
    ("joined_first", "calls"),
    [(False, ["leave", "join"]), (False, ["join", "leave", "join"]), (True, ["leave", "join"])],
    ids=["leave-join", "join-leave-join", "joined-then-leave-join"],
)
async def test_leaves_and_joins_called_together_go_out_in_call_order_and_end_joined(
    server: Server, joined_first: bool, calls: list[str]
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    if joined_first:
        await ana.join("t1")
    sent = methods(ana)
    await asyncio.wait_for(asyncio.gather(*(getattr(ana, call)("t1") for call in calls)), 2)
    assert sent == [f"thread.{call}:t1" for call in calls]
    assert (
        "t1" in ana._cursors and "t1" in ana._present and in_room(server, ANA, "t1") == (True, True)
    )


async def test_a_follower_that_leaves_a_thread_and_follows_again_together_follows_it_again(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    sent = methods(page)
    await asyncio.wait_for(asyncio.gather(page.leave("t1"), page.follow()), 2)
    assert sent == ["thread.leave:t1", "thread.follow:"]
    assert "t1" in page._cursors and not page._left and in_room(server, HELPER, "t1")[0]


@pytest.mark.parametrize(
    "during", ["thread.follow", "thread.join"], ids=["refollow", "previous-rejoin"]
)
async def test_a_present_thread_left_during_the_reconnect_is_not_rejoined(
    server: Server, during: str
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t0", "t1"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, HELPER)
        await bruno.send(thread_id, f"old {thread_id}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    for thread_id in ("t0", "t1"):
        await page.join(thread_id)
    await drain(page)
    first, second = page._cursors
    request = page._request
    sent: list[str] = []
    leaves: list[asyncio.Task[None]] = []

    async def leaving(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append(f"{method}:{payload.get('thread_id', '')}")
        if method == during and not leaves:
            leaves.append(asyncio.ensure_future(page.leave(second)))
        return await request(method, payload)

    page._request = leaving  # type: ignore[method-assign]
    connection = page._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(page, connection))
    await asyncio.gather(*leaves)
    await drain(page)
    await bruno.send(second, "after the leave")
    assert said(await drain(page)) == []
    assert sent == ["thread.follow:", f"thread.join:{first}", f"thread.leave:{second}"]
    assert second not in page._cursors and second not in page._present
    assert page._left == {second: 3} and in_room(server, HELPER, second) == (False, False)


async def test_an_away_called_behind_two_joins_of_a_thread_goes_out_after_both(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    sent = methods(ana)
    await asyncio.wait_for(asyncio.gather(ana.join("t1"), ana.join("t1"), ana.away("t1")), 2)
    assert sent == ["thread.join:t1", "thread.join:t1", "thread.away:t1"]
    assert "t1" not in ana._present and in_room(server, ANA, "t1") == (True, False)


@pytest.mark.parametrize("remade", [False, True], ids=["kept", "made-again"])
async def test_a_reconnect_of_a_follower_present_in_a_thread_announces_one_joined_for_it(
    server: Server, remade: bool
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await page.join("t1")
    await drain(page)
    old = page._epochs["t1"]

    async def meanwhile() -> None:
        if remade:
            await remade_with(server, bruno, HELPER)

    items = await back_after(page, meanwhile)
    joined = [i for i in items if isinstance(i, Joined) and i.thread.id == "t1"]
    assert [(j.thread.epoch > old, j.truncated) for j in joined] == [(remade, remade)]
    assert "t1" in page._present and in_room(server, HELPER, "t1") == (True, True)


async def test_a_plain_join_answered_past_its_own_removal_and_re_add_keeps_the_thread(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", ANA)
    await bruno.send("t1", "one")
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.join("t1")
    await drain(ana)
    request = ana._request

    async def cut(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            await bruno.remove_member("t1", ANA)
            await until(lambda: any(isinstance(e, MemberRemoved) for e in ana._holds["t1"].events))
            await bruno.add_member("t1", ANA)
        return await request(method, payload)

    ana._request = cut  # type: ignore[method-assign]
    joined = await ana.join("t1")
    ana._request = request  # type: ignore[method-assign]
    live = await bruno.send("t1", "live")
    items = await drain(ana)
    assert [(type(i).__name__, i.seq) for i in items if isinstance(i, MemberRemoved | Message)] == [
        ("MemberRemoved", 4),
        ("Message", 6),
    ]
    assert joined.seq == 5 and ana._cursors["t1"] == live.seq and "t1" in ana._present
    sent = methods(ana)
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(ana, connection))
    assert sent == ["thread.join:t1"] and in_room(server, ANA, "t1") == (True, True)


async def test_a_rewind_crossed_by_its_own_removal_and_re_add_delivers_from_its_since(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    for n in range(3):
        await bruno.send("t1", f"m{n}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await page.join("t1")
    await drain(page)
    arrive = server._hub._arrive
    cut: list[str] = []

    async def cutting(session: Any, thread_id: str) -> None:
        if session.identity.id == HELPER.id and not cut:
            cut.append(thread_id)
            await bruno.remove_member("t1", HELPER)
            await bruno.add_member("t1", HELPER)
            await asyncio.sleep(0.2)
        await arrive(session, thread_id)

    server._hub._arrive = cutting  # type: ignore[method-assign]
    joined = await page.join("t1", since=0)
    await bruno.send("t1", "live")
    items = await drain(page)
    assert joined.seq == 7
    assert [
        (type(i).__name__, i.seq)
        for i in items
        if isinstance(i, Joined | MemberAdded | MemberRemoved | Message)
    ] == [
        ("MemberAdded", 1),
        ("MemberAdded", 2),
        ("Message", 3),
        ("Message", 4),
        ("Message", 5),
        ("MemberRemoved", 6),
        ("MemberAdded", 7),
        ("Joined", 7),
        ("Message", 8),
    ]


def told(items: list[Item]) -> list[tuple[Any, ...]]:
    out: list[tuple[Any, ...]] = []
    for item in items:
        if isinstance(item, Joined):
            out.append(("Joined", item.thread.epoch, item.seq, item.truncated))
        elif isinstance(item, MemberAdded | MemberRemoved | Message):
            out.append((type(item).__name__, item.seq))
        elif isinstance(item, ThreadDeleted):
            out.append(("ThreadDeleted",))
    return out


async def made_again_with_history(server: Server, bruno: Client, who: Identity) -> None:
    await server.delete_thread("acme", "t1")
    await bruno.create_thread(thread_id="t1")
    for n in range(3):
        await bruno.send("t1", f"pre {n}")
    await bruno.add_member("t1", who)


async def test_a_quiet_rejoin_crossing_a_remake_that_re_adds_the_follower_resumes_from_the_push(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await page.join("t1")
    await drain(page)
    old = page._epochs["t1"]
    request = page._request
    fired: list[str] = []

    async def crossing(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join" and not fired:
            fired.append(method)
            await made_again_with_history(server, bruno, HELPER)
            await until(lambda: page._epochs.get("t1") != old)
        return await request(method, payload)

    page._request = crossing  # type: ignore[method-assign]
    connection = page._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(page, connection) and bool(fired) and not page._holds)
    page._request = request  # type: ignore[method-assign]
    await bruno.send("t1", "live")
    new = page._epochs["t1"]
    assert told(await drain(page)) == [
        ("Joined", old, 2, False),
        ("ThreadDeleted",),
        ("MemberAdded", 5),
        ("Joined", new, 5, False),
        ("Joined", new, 5, True),
        ("Message", 6),
    ]


async def test_a_rejoin_crossing_a_remake_and_then_a_deletion_hands_nothing_over_twice(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    page = await server.connect(identity=HELPER, scope="acme")
    await page.join("t1")
    await drain(page)
    old = page._epochs["t1"]
    request = page._request
    join = server._verbs.join
    fired: list[int] = []

    async def crossing(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join" and not fired:
            fired.append(1)
            await page.follow()
            await made_again_with_history(server, bruno, HELPER)
            await until(lambda: page._epochs.get("t1") != old)
        return await request(method, payload)

    async def deleting(session: Any, thread_id: str, *args: Any) -> Any:
        joined = await join(session, thread_id, *args)
        if session.identity.id == HELPER.id and fired == [1]:
            fired.append(2)
            await server.delete_thread("acme", "t1")
        return joined

    page._request = crossing  # type: ignore[method-assign]
    server._verbs.join = deleting  # type: ignore[method-assign, assignment]
    connection = page._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(page, connection) and len(fired) == 2 and not page._holds)
    page._request = request  # type: ignore[method-assign]
    server._verbs.join = join  # type: ignore[method-assign]
    new = page._epochs["t1"]
    assert told(await drain(page)) == [
        ("Joined", old, 2, False),
        ("ThreadDeleted",),
        ("MemberAdded", 5),
        ("Joined", new, 5, False),
        ("ThreadDeleted",),
        ("Joined", new, 5, True),
    ]


async def test_a_drain_floored_above_an_own_removal_keeps_the_thread_despite_an_older_own_add(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    ana._cursors["t1"] = 0
    events: list[Event] = [
        added("t1", ANA).model_copy(update={"seq": 2}),
        removed("t1", ANA).model_copy(update={"seq": 4}),
        message(5, "after"),
    ]
    ana._drain("t1", events, 0, 6)
    assert ana._cursors["t1"] == 5
    assert [type(i).__name__ for i in await drain(ana)] == [
        "MemberAdded",
        "MemberRemoved",
        "Message",
    ]


async def test_a_join_answered_after_another_join_of_the_thread_was_refused_keeps_it_present(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    page = await server.connect(identity=HELPER, scope="acme")
    request = page._request
    sent: list[dict[str, Any]] = []

    async def refusing_once(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        reply = await request(method, payload)
        if method == "thread.join":
            sent.append(payload)
            if len(sent) == 1:
                raise StorageError("postgres: storage rejected the reply")
        return reply

    page._request = refusing_once  # type: ignore[method-assign]
    first, second = await asyncio.gather(page.join("t1"), page.join("t1"), return_exceptions=True)
    assert isinstance(first, StorageError) and isinstance(second, Joined)
    assert "t1" in page._present
    connection = page._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(page, connection))
    assert sent[-1] == {"thread_id": "t1", "since": second.seq, "epoch": second.thread.epoch}


@pytest.mark.parametrize(
    ("since", "remade", "heard"),
    [
        (0, False, ["away 0", "away 1", "live"]),
        (None, False, ["live"]),
        (None, True, ["live"]),
    ],
    ids=["rewind", "plain", "made-again"],
)
async def test_a_rejoin_waiting_behind_a_join_of_the_same_thread_resumes_where_that_join_left_it(
    server: Server, since: int | None, remade: bool, heard: list[str]
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    bruno = await server.connect(identity=BRUNO, scope="acme")
    for thread_id in ("t1", "t2"):
        await bruno.create_thread(thread_id=thread_id)
        await bruno.add_member(thread_id, ANA)
        await ana.join(thread_id)
    await drain(ana)
    request = ana._request
    rejoining, resumed, rejoined = asyncio.Event(), asyncio.Event(), asyncio.Event()
    released = asyncio.Event()
    live: list[Message] = []

    async def gated(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        again = method == "thread.join" and "epoch" in payload
        if again and payload["thread_id"] == "t1":
            rejoining.set()
            await resumed.wait()
        reply = await request(method, payload)
        if again and payload["thread_id"] == "t1":
            rejoined.set()
        if method == "thread.join" and payload["thread_id"] == "t2" and not again:
            live.append(await bruno.send("t2", "live"))
            await released.wait()
        return reply

    ana._request = gated  # type: ignore[method-assign]
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await asyncio.wait_for(rejoining.wait(), 2)
    if remade:
        await server.delete_thread("acme", "t2")
        await bruno.create_thread(thread_id="t2")
        await bruno.add_member("t2", ANA)
    for n in range(2):
        await bruno.send("t2", f"away {n}")
    user = asyncio.create_task(ana.join("t2", since=since))
    await until(lambda: bool(live) and held(ana, "t2", live[0].seq))
    resumed.set()
    await asyncio.wait_for(rejoined.wait(), 2)
    released.set()
    await asyncio.wait_for(user, 2)
    await until(lambda: ana.status == "open")
    items = await drain(ana, 0.3)
    assert said([i for i in items if isinstance(i, Message) and i.thread_id == "t2"]) == heard


@pytest.mark.parametrize("stale", [False, True], ids=["answered", "stale"])
async def test_a_join_answered_after_its_thread_was_made_again_delivers_the_new_thread_once_in_order(
    server: Server, stale: bool
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    for n in range(6):
        await bruno.send("t1", f"old {n}")
    page = await server.connect(identity=HELPER, scope="acme")
    [before] = await page.follow()
    await drain(page)
    arrive = server._hub._arrive
    armed = [True]

    async def remade(session: Any, thread_id: str) -> None:
        if session.identity.id == HELPER.id and armed:
            armed.clear()
            await server.delete_thread("acme", "t1")
            await bruno.create_thread(thread_id="t1")
            await bruno.add_member("t1", HELPER)
            for n in range(2):
                await bruno.send("t1", f"new {n}")
        await arrive(session, thread_id)

    server._hub._arrive = remade  # type: ignore[method-assign]
    request = page._request

    async def answered(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        reply = await request(method, payload)
        return before.model_dump(mode="json") if stale else reply

    page._request = answered  # type: ignore[method-assign]
    joined = await asyncio.wait_for(page.join("t1"), 2)
    for n in range(2, 8):
        await bruno.send("t1", f"new {n}")
    items = await drain(page, 0.3)
    gone = next(n for n, item in enumerate(items) if isinstance(item, ThreadDeleted))
    after = [i for i in items[gone + 1 :] if isinstance(i, Joined | MemberAdded | Message)]
    assert [(type(i).__name__, i.seq) for i in after] == [
        ("MemberAdded", 2),
        ("Joined", 2),
        *(("Message", seq) for seq in range(3, 11)),
    ]
    assert said(after) == [f"new {n}" for n in range(8)]
    pushed = after[1]
    assert isinstance(pushed, Joined)
    if stale:
        assert joined == before
    else:
        assert (joined.thread, joined.seq, joined.truncated) == (pushed.thread, 4, True)


def news(items: Sequence[Item], thread_id: str = "t1") -> list[tuple[str, int]]:
    return [
        (type(i).__name__, i.seq)
        for i in items
        if isinstance(i, MemberAdded | Message) and i.thread_id == thread_id
    ]


async def followed_with_six(server: Server) -> tuple[Client, Client]:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    for n in range(6):
        await bruno.send("t1", f"old {n}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    return page, bruno


async def test_a_refollow_that_finds_its_thread_made_again_delivers_the_new_one_once_in_order(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = Server(storage)
    page, bruno = await followed_with_six(server)
    listing = storage.list_threads
    armed: list[bool] = []

    async def listed(*args: Any, **kwargs: Any) -> list[Thread]:
        found = await listing(*args, **kwargs)
        if args[1] == HELPER.id and armed:
            armed.clear()
            await server.delete_thread("acme", "t1")
            await bruno.create_thread(thread_id="t1")
            await bruno.add_member("t1", HELPER)
        return found

    monkeypatch.setattr(storage, "list_threads", listed)
    armed.append(True)
    connection = page._connection
    assert connection is not None
    await connection.close()
    await until(lambda: not armed and reopened(page, connection) and not page._holds)
    for n in range(3):
        await bruno.send("t1", f"new {n}")
    items = await drain(page, 0.3)
    assert news(items) == [("MemberAdded", 2), *(("Message", seq) for seq in (3, 4, 5))]
    assert said(items) == ["new 0", "new 1", "new 2"]


async def test_a_followed_thread_deleted_while_away_and_made_again_arrives_whole(
    server: Server,
) -> None:
    page, bruno = await followed_with_six(server)
    request = page._request
    gate = asyncio.Event()

    async def gated(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.follow":
            await gate.wait()
        return await request(method, payload)

    page._request = gated  # type: ignore[method-assign]
    connection = page._connection
    assert connection is not None
    await connection.close()
    await server.delete_thread("acme", "t1")
    gate.set()
    await until(lambda: reopened(page, connection) and not page._holds)
    await drain(page)
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    for n in range(3):
        await bruno.send("t1", f"new {n}")
    items = await drain(page, 0.3)
    assert news(items) == [("MemberAdded", 2), *(("Message", seq) for seq in (3, 4, 5))]
    assert said(items) == ["new 0", "new 1", "new 2"]


@pytest.mark.parametrize("before", [0, 3])
async def test_a_rewind_raced_by_its_thread_made_again_delivers_every_event_from_its_since(
    server: Server, before: int
) -> None:
    page, bruno = await followed_with_six(server)
    old = page._epochs["t1"]
    request = page._request

    async def remade(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            await server.delete_thread("acme", "t1")
            await bruno.create_thread(thread_id="t1")
            for n in range(before):
                await bruno.send("t1", f"before {n}")
            await bruno.add_member("t1", HELPER)
            await until(lambda: page._epochs.get("t1") != old)
        return await request(method, payload)

    page._request = remade  # type: ignore[method-assign]
    await asyncio.wait_for(page.join("t1", since=0), 2)
    await bruno.send("t1", "new 0")
    items = await drain(page, 0.3)
    add = before + 2
    assert news(items) == [
        ("MemberAdded", add),
        ("MemberAdded", 1),
        *(("Message", seq) for seq in range(2, add)),
        ("MemberAdded", add),
        ("Message", add + 1),
    ]


async def test_a_first_join_since_a_seq_raced_by_its_own_add_delivers_every_event_from_it(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t2")
    for n in range(3):
        await bruno.send("t2", f"before {n}")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    request = page._request

    async def added(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            await bruno.add_member("t2", HELPER)
            await until(lambda: "t2" in page._epochs)
        return await request(method, payload)

    page._request = added  # type: ignore[method-assign]
    await asyncio.wait_for(page.join("t2", since=0), 2)
    items = await drain(page, 0.3)
    assert news(items, "t2") == [
        ("MemberAdded", 5),
        ("MemberAdded", 1),
        *(("Message", seq) for seq in (2, 3, 4)),
        ("MemberAdded", 5),
    ]


async def test_a_join_and_a_leave_called_together_leave_no_thread_to_rejoin(
    server: Server,
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await asyncio.wait_for(asyncio.gather(ana.join("t1"), ana.leave("t1")), 2)
    sent: list[str] = []
    request = ana._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        sent.append(method)
        return await request(method, payload)

    ana._request = record  # type: ignore[method-assign]
    connection = ana._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(ana, connection))
    assert sent == [] and not ana_in(server, "t1")


async def test_a_follower_that_joins_and_leaves_a_left_thread_together_keeps_it_left_after_a_drop(
    server: Server,
) -> None:
    bruno = await server.connect(identity=BRUNO, scope="acme")
    await bruno.create_thread(thread_id="t1")
    await bruno.add_member("t1", HELPER)
    await bruno.send("t1", "one")
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    await page.leave("t1")
    await asyncio.wait_for(asyncio.gather(page.join("t1"), page.leave("t1")), 2)
    follows: list[dict[str, Any]] = []
    request = page._request

    async def record(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.follow":
            follows.append(payload)
        return await request(method, payload)

    page._request = record  # type: ignore[method-assign]
    connection = page._connection
    assert connection is not None
    await connection.close()
    await until(lambda: reopened(page, connection))
    items = await drain(page, 0.3)
    made = (await bruno.get_thread("t1")).epoch
    assert [payload["left"] for payload in follows] == [{"t1": {"seq": 3, "epoch": made}}]
    assert [i for i in items if isinstance(i, Joined)] == []
    assert not any(s.identity.id == HELPER.id and "t1" in s.joined for s in server._hub.sessions)


async def test_a_join_refused_while_its_followed_thread_speaks_still_delivers_what_it_said(
    storage: PostgresStorage,
) -> None:
    refusing = False

    async def authorize(actor: Identity, action: str, thread: Thread) -> bool:
        return not (refusing and actor.id == HELPER.id)

    server = Server(storage, authorize=authorize)
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.create_thread(thread_id="t1")
    await ana.add_member("t1", HELPER)
    page = await server.connect(identity=HELPER, scope="acme")
    await page.follow()
    await drain(page)
    request = page._request

    async def landed(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            live = await ana.send("t1", "live")
            await until(lambda: held(page, "t1", live.seq))
        return await request(method, payload)

    page._request = landed  # type: ignore[method-assign]
    refusing = True
    with pytest.raises(ForbiddenError):
        await asyncio.wait_for(page.join("t1", since=0), 2)
    assert said(await drain(page)) == ["live"]
