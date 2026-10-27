import asyncio
import contextlib
import inspect
from typing import Any, cast

import pytest
from until import until

from rfnry_thread.client import Client, Item
from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    NotFoundError,
    TransportError,
)
from rfnry_thread.models import (
    SYSTEM,
    Cancel,
    Custom,
    Edit,
    Identity,
    Image,
    Invited,
    Marked,
    Member,
    MemberAdded,
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
    Working,
)
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.protocol import Push
from rfnry_thread.resolve import resolve
from rfnry_thread.server import Server

from .factories import ANA, AT, BRUNO, HELPER, Lookalike, lookalike


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


async def test_marked_reads_the_callers_own_mark_without_moving_it(
    server: Server, storage: PostgresStorage
) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    helper = await server.connect(identity=HELPER, scope="acme")
    await ana.create_thread(thread_id="t1")
    await storage.add_member(
        "acme", Member(thread_id="t1", identity=HELPER, added_at=AT, added_by=SYSTEM)
    )
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
            sent = await ana.send("t1", "live")
            await until(lambda: page._cursors.get("t1") == sent.seq)
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
    landed = asyncio.Event()
    joining: list[int] = []
    second: list[asyncio.Task[Any]] = []
    live: list[Message] = []

    async def overlap(method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if method == "thread.join":
            joining.append(payload["since"])
        if joining == [5]:
            second.append(asyncio.create_task(page.join("t1", since=10)))
            await until(lambda: joining == [5, 10])
            live.append(await ana.send("t1", "live"))
            await until(lambda: page._cursors.get("t1") == live[0].seq)
            landed.set()
        elif method == "thread.join":
            await landed.wait()
        return await request(method, payload)

    page._request = overlap  # type: ignore[method-assign]
    first = await page.join("t1", since=5)
    other = await second[0]
    assert (first.seq, other.seq) == (21, 21)
    seqs = sorted(item.seq for item in await drain(page) if isinstance(item, Message))
    assert seqs == sorted([*range(6, 21), *range(11, 21), 21])
    assert not page._rewinds
    await ana.send("t1", "after")
    assert [item.seq for item in await drain(page) if isinstance(item, Message)] == [22]


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
        page._receive(Push(name="event", payload=old.model_dump(mode="json")))
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
    assert page._cursors["t1"] == 5 and not page._rewinds
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
    assert sent == [
        ("thread.history", {"thread_id": "t1", "before": None, "limit": 50, "kinds": None}),
        ("thread.list", {"before": None, "limit": 50}),
        (
            "thread.history",
            {"thread_id": "t1", "before": None, "limit": 50, "kinds": None, "after": 0},
        ),
        ("thread.list", {"before": None, "limit": 50, "order": "activity", "preview": True}),
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
