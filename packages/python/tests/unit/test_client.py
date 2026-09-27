import asyncio
import contextlib
import inspect
from typing import Any, cast

import pytest

from rfnry_thread.client import Client, Item
from rfnry_thread.errors import ConfigurationError, ForbiddenError, InputError, TransportError
from rfnry_thread.models import (
    SYSTEM,
    Cancel,
    Custom,
    Edit,
    Image,
    Invited,
    Message,
    Present,
    Read,
    Retract,
    Step,
    StreamDelta,
    StreamEnd,
    StreamStart,
    Text,
    Working,
)
from rfnry_thread.resolve import resolve
from rfnry_thread.server import Server

from .factories import ANA, BRUNO, HELPER, Lookalike, lookalike


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


async def test_a_closed_client_ends_its_stream_and_refuses_calls(server: Server) -> None:
    ana = await server.connect(identity=ANA, scope="acme")
    await ana.close()
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
