import asyncio
from typing import Any

import pytest

from rfnry_thread.errors import ForbiddenError, InputError, NotFoundError
from rfnry_thread.inmemory import InMemoryStorage
from rfnry_thread.models import Identity, Thread
from rfnry_thread.server import Server

from .conftest import say
from .factories import ANA, BRUNO, HELPER, lookalike
from .peer import Peer


async def room(server: Server, *members: Identity) -> Peer:
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1", metadata={"topic": "billing"})
    for member in members:
        await ana.call("member.add", thread_id="t1", identity=member.model_dump(mode="json"))
    return ana


async def test_create_is_get_or_create_and_the_creator_is_the_first_member(server: Server) -> None:
    ana = await room(server)
    again = await ana.call("thread.create", thread_id="t1", metadata={"topic": "ignored"})
    assert again["thread"]["metadata"] == {"topic": "billing"}
    members = await ana.call("member.list", thread_id="t1")
    assert [m["identity"]["id"] for m in members["members"]] == ["ana"]
    history = await ana.call("thread.history", thread_id="t1")
    assert [(e["kind"], e["identity"]["id"]) for e in history["events"]] == [
        ("member_added", "ana")
    ]
    generated = await ana.call("thread.create")
    assert len(generated["thread"]["id"]) == 32


async def test_a_non_member_is_forbidden_everywhere_and_a_missing_thread_is_not_found(
    server: Server,
) -> None:
    await room(server)
    bruno = await Peer.open(server, BRUNO)
    calls: list[tuple[str, dict[str, Any]]] = [
        ("thread.get", {}),
        ("thread.history", {}),
        ("thread.join", {}),
        ("member.list", {}),
        ("thread.update", {"metadata": {}}),
        ("thread.delete", {}),
        ("thread.create", {}),
    ]
    for method, extra in calls:
        with pytest.raises(
            ForbiddenError, match="^server: 'bruno' is not a member of thread 't1'$"
        ):
            await bruno.call(method, thread_id="t1", **extra)
    with pytest.raises(ForbiddenError):
        await say(bruno, "t1", "let me in")
    with pytest.raises(NotFoundError, match="^server: thread 'ghost' was not found$"):
        await bruno.call("thread.get", thread_id="ghost")


async def test_threads_are_listed_update_writes_an_event_and_delete_removes(server: Server) -> None:
    ana = await room(server)
    await ana.call("thread.create", thread_id="t2")
    assert [t["id"] for t in (await ana.call("thread.list"))["threads"]] == ["t2", "t1"]
    assert [t["id"] for t in (await ana.call("thread.list", before="t2"))["threads"]] == ["t1"]
    updated = await ana.call("thread.update", thread_id="t1", metadata={"topic": "refunds"})
    assert updated["thread"]["metadata"] == {"topic": "refunds"}
    kinds = [e["kind"] for e in (await ana.call("thread.history", thread_id="t1"))["events"]]
    assert kinds[-1] == "thread_updated"
    await ana.call("thread.delete", thread_id="t1")
    with pytest.raises(NotFoundError):
        await ana.call("thread.get", thread_id="t1")


async def test_members_are_added_and_removed_through_the_log(server: Server) -> None:
    ana = await room(server, BRUNO)
    again = await ana.call("member.add", thread_id="t1", identity=BRUNO.model_dump(mode="json"))
    assert again["member"]["identity"]["id"] == "bruno"
    await ana.call("member.remove", thread_id="t1", identity_id="bruno")
    events = (await ana.call("thread.history", thread_id="t1"))["events"]
    assert [e["kind"] for e in events] == ["member_added", "member_added", "member_removed"]
    with pytest.raises(NotFoundError, match="^server: 'bruno' is not a member of thread 't1'$"):
        await ana.call("member.remove", thread_id="t1", identity_id="bruno")
    with pytest.raises(InputError, match="^server: the system identity cannot be a member$"):
        await ana.call("member.add", thread_id="t1", identity={"role": "system", "id": "system"})


async def test_authorship_is_stamped_and_a_repeat_is_answered_with_the_stored_event(
    server: Server,
) -> None:
    ana = await room(server)
    first = await say(
        ana,
        "t1",
        "hello",
        client_id="c1",
        author=BRUNO.model_dump(mode="json"),
        seq=99,
        id="forged",
    )
    again = await say(ana, "t1", "hello, twice", client_id="c1")
    assert first["author"]["id"] == "ana" and first["seq"] == 2 and first["id"] != "forged"
    assert again == first


async def test_events_are_validated_before_they_are_stored(
    server: Server, storage: InMemoryStorage
) -> None:
    small = Server(storage, max_inline_bytes=4)
    ana = await room(small)
    image = {"type": "image", "media_type": "image/png", "data": "AAAAAAAA"}
    with pytest.raises(
        InputError,
        match="^server: inline media is 6 bytes; the cap is 4 — upload it and send a url$",
    ):
        await ana.call(
            "event.send", thread_id="t1", kind="message", client_id="c1", content=[image]
        )
    with pytest.raises(InputError, match="^models: image carries data or url, not both$"):
        await ana.call(
            "event.send",
            thread_id="t1",
            kind="message",
            client_id="c2",
            content=[{"type": "image", "media_type": "image/png"}],
        )
    with pytest.raises(InputError, match="^server: a custom payload is at most 65536 bytes$"):
        await ana.call(
            "event.send",
            thread_id="t1",
            kind="custom",
            client_id="c3",
            name="big",
            payload={"x": "y" * 70000},
        )
    with pytest.raises(InputError, match="^server: unknown event kind 'member_added'$"):
        await ana.call(
            "event.send",
            thread_id="t1",
            kind="member_added",
            client_id="c4",
            identity=BRUNO.model_dump(),
        )
    with pytest.raises(InputError, match="^server: event.send needs a client_id$"):
        await ana.call("event.send", thread_id="t1", kind="message", content=[])
    with pytest.raises(InputError, match="^server: invalid message event"):
        await ana.call("event.send", thread_id="t1", kind="message", client_id="c5")


async def test_a_message_carries_metadata_set_once_and_bounded(server: Server) -> None:
    ana = await room(server)
    sent = await say(ana, "t1", "hello", metadata={"run_id": "r1", "nested": {"ok": True}})
    assert sent["metadata"] == {"run_id": "r1", "nested": {"ok": True}}
    assert (await say(ana, "t1", "plain"))["metadata"] == {}
    await ana.call(
        "event.send",
        thread_id="t1",
        kind="edit",
        client_id="e1",
        target=sent["id"],
        content=[{"type": "text", "text": "hello!"}],
        metadata={"run_id": "forged"},
    )
    history = (await ana.call("thread.history", thread_id="t1", kinds=["message"]))["events"]
    assert history[0]["metadata"] == {"run_id": "r1", "nested": {"ok": True}}
    assert (await say(ana, "t1", "at the limit", metadata={"x": "y" * 8183}))["metadata"]
    with pytest.raises(InputError, match="^server: message metadata is at most 8192 bytes$"):
        await say(ana, "t1", "over", metadata={"x": "y" * 8184})
    for wrong in (["run_id"], "r1", 7, None):
        with pytest.raises(InputError, match="^server: invalid message event"):
            await say(ana, "t1", "not an object", metadata=wrong)


async def test_only_the_author_edits_or_retracts_and_only_what_can_be(server: Server) -> None:
    ana = await room(server, HELPER)
    helper = await Peer.open(server, HELPER)
    mine = await say(ana, "t1", "hello")
    step = (
        await helper.call("event.send", thread_id="t1", kind="step", client_id="s1", name="Read")
    )["event"]
    edit = await ana.call(
        "event.send",
        thread_id="t1",
        kind="edit",
        client_id="e1",
        target=mine["id"],
        content=[{"type": "text", "text": "hi"}],
    )
    assert edit["event"]["target"] == mine["id"]
    with pytest.raises(
        ForbiddenError, match=f"^server: only its author can edit event '{mine['id']}'$"
    ):
        await helper.call(
            "event.send", thread_id="t1", kind="edit", client_id="e2", target=mine["id"], content=[]
        )
    with pytest.raises(
        InputError, match=f"^server: only a message can be edited; '{step['id']}' is a step$"
    ):
        await helper.call(
            "event.send", thread_id="t1", kind="edit", client_id="e3", target=step["id"], content=[]
        )
    await helper.call(
        "event.send", thread_id="t1", kind="retract", client_id="r1", target=step["id"]
    )
    read = (await ana.call("event.send", thread_id="t1", kind="read", client_id="d1", up_to_seq=1))[
        "event"
    ]
    with pytest.raises(
        InputError,
        match=f"^server: only a message or a step can be retracted; '{read['id']}' is a read$",
    ):
        await ana.call(
            "event.send", thread_id="t1", kind="retract", client_id="r3", target=read["id"]
        )
    with pytest.raises(NotFoundError, match="^server: event 'ghost' was not found$"):
        await ana.call("event.send", thread_id="t1", kind="retract", client_id="r2", target="ghost")


async def test_history_pages_back_and_filters(server: Server) -> None:
    ana = await room(server)
    for n in range(5):
        await say(ana, "t1", f"m{n}")
    page = (await ana.call("thread.history", thread_id="t1", limit=2))["events"]
    assert [e["seq"] for e in page] == [5, 6]
    older = (await ana.call("thread.history", thread_id="t1", before=5, limit=2))["events"]
    assert [e["seq"] for e in older] == [3, 4]
    only = (await ana.call("thread.history", thread_id="t1", kinds=["member_added"]))["events"]
    assert [e["seq"] for e in only] == [1]
    with pytest.raises(InputError, match="^server: limit must be between 1 and 500$"):
        await ana.call("thread.history", thread_id="t1", limit=0)


async def test_authorize_decides_each_of_the_five_actions(storage: InMemoryStorage) -> None:
    asked: list[tuple[str, str]] = []

    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        asked.append((identity.id, action))
        return action != "delete"

    server = Server(storage, authorize=authorize)
    ana = await room(server, BRUNO)
    await ana.call("thread.get", thread_id="t1")
    await say(ana, "t1", "hi")
    with pytest.raises(ForbiddenError, match="^server: 'ana' may not delete in thread 't1'$"):
        await ana.call("thread.delete", thread_id="t1")
    assert [action for _, action in asked] == ["create", "manage", "read", "send", "delete"]


async def test_listing_skips_what_authorize_refuses_and_reads_on_to_fill_the_page(
    storage: InMemoryStorage,
) -> None:
    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        return not (action == "read" and thread.metadata.get("archived"))

    server = Server(storage, authorize=authorize)
    ana = await Peer.open(server, ANA)
    for thread_id in ("a1", "a2", "a3"):
        await ana.call("thread.create", thread_id=thread_id)
    for thread_id in ("r1", "r2", "r3", "r4"):
        await ana.call("thread.create", thread_id=thread_id, metadata={"archived": True})
    with pytest.raises(ForbiddenError, match="^server: 'ana' may not read in thread 'r4'$"):
        await ana.call("thread.get", thread_id="r4")
    reads: list[str | None] = []
    original = storage.list_threads

    async def counted(
        scope: str,
        member_id: str | None,
        before: str | None = None,
        limit: int = 50,
        metadata: dict[str, Any] | None = None,
    ) -> list[Thread]:
        reads.append(before)
        return await original(scope, member_id, before, limit, metadata)

    storage.list_threads = counted  # type: ignore[method-assign]

    async def page(before: str | None) -> list[str]:
        reads.clear()
        listed = (await ana.call("thread.list", before=before, limit=2))["threads"]
        return [t["id"] for t in listed]

    assert await page(None) == ["a3", "a2"] and reads == [None, "r3", "r1"]
    assert await page("a2") == ["a1"] and reads == ["a2"]
    assert await page("a1") == [] and reads == ["a1"]


async def test_joined_members_receive_events_in_order_and_a_join_since_replays(
    server: Server,
) -> None:
    ana = await room(server, HELPER)
    helper = await Peer.open(server, HELPER)
    joined = await helper.call("thread.join", thread_id="t1")
    assert joined["seq"] == 2 and joined["truncated"] is False
    for n in range(3):
        await say(ana, "t1", f"m{n}")
    received = [p for p in await helper.quiet() if p["kind"] == "message"]
    assert [p["seq"] for p in received] == [3, 4, 5]
    late = await Peer.open(server, HELPER)
    await late.call("thread.join", thread_id="t1", since=3)
    assert [p["seq"] for p in await late.quiet() if p["kind"] == "message"] == [4, 5]


async def test_a_removed_member_stops_receiving_over_the_server(server: Server) -> None:
    ana = await room(server, BRUNO)
    bruno = await Peer.open(server, BRUNO)
    await bruno.call("thread.join", thread_id="t1")
    await ana.call("member.remove", thread_id="t1", identity_id="bruno")
    await say(ana, "t1", "after")
    assert [
        p["kind"] for p in await bruno.quiet() if p["kind"] in ("member_removed", "message")
    ] == ["member_removed"]


class HeldAtTheDoor(InMemoryStorage):
    def __init__(self, who: str) -> None:
        super().__init__()
        self.who: str | None = who
        self.waiting = asyncio.Event()
        self.release = asyncio.Event()

    async def is_member(self, scope: str, thread_id: str, identity_id: str) -> bool:
        answer = await super().is_member(scope, thread_id, identity_id)
        if identity_id == self.who:
            self.who = None
            self.waiting.set()
            await self.release.wait()
        return answer


async def test_a_member_removed_while_joining_is_refused_and_receives_nothing() -> None:
    storage = HeldAtTheDoor("bruno")
    server = Server(storage)
    ana = await room(server, BRUNO)
    bruno = await Peer.open(server, BRUNO)
    joining = asyncio.create_task(bruno.call("thread.join", thread_id="t1"))
    await storage.waiting.wait()
    await ana.call("member.remove", thread_id="t1", identity_id="bruno")
    storage.release.set()
    with pytest.raises(ForbiddenError, match="^server: 'bruno' is not a member of thread 't1'$"):
        await joining
    await say(ana, "t1", "after")
    assert [p for p in await bruno.quiet() if p["kind"] == "message"] == []


async def test_nothing_crosses_scopes(server: Server) -> None:
    a = await Peer.open(server, ANA, scope="tenant-a")
    b = await Peer.open(server, ANA, scope="tenant-b")
    await a.call("thread.create", thread_id="t1")
    await b.call("thread.create", thread_id="t1")
    await a.call("thread.join", thread_id="t1")
    await b.call("thread.join", thread_id="t1")
    await say(a, "t1", "MARKER-A")
    assert all("MARKER-A" not in str(p) for p in await b.quiet())
    history = (await b.call("thread.history", thread_id="t1"))["events"]
    assert all("MARKER-A" not in str(e) for e in history)
    assert await server.delete_scope("tenant-a") > 0
    assert await server.verify_absent("tenant-a") and not await server.verify_absent("tenant-b")


async def test_the_server_verbs_act_as_the_system(server: Server) -> None:
    thread = await server.create_thread(
        "acme", thread_id="onboarding-bruno", metadata={"owner": "bruno"}
    )
    member = await server.add_member("acme", thread.id, BRUNO)
    assert member.added_by.role == "system"
    bruno = await Peer.open(server, BRUNO)
    events = (await bruno.call("thread.history", thread_id=thread.id))["events"]
    assert [(e["kind"], e["author"]["role"]) for e in events] == [("member_added", "system")]
    await server.delete_thread("acme", thread.id)
    with pytest.raises(NotFoundError):
        await bruno.call("thread.get", thread_id=thread.id)


async def test_the_server_updates_metadata_and_removes_members_as_the_system(
    storage: InMemoryStorage,
) -> None:
    asked: list[str] = []

    async def authorize(identity: Identity, action: str, thread: Thread) -> bool:
        asked.append(action)
        return True

    server = Server(storage, authorize=authorize)
    await server.create_thread("acme", thread_id="t1", metadata={"topic": "billing"})
    for identity in (ANA, BRUNO):
        await server.add_member("acme", "t1", identity)
    ana = await Peer.open(server, ANA)
    bruno = await Peer.open(server, BRUNO)
    await ana.call("thread.join", thread_id="t1")
    await bruno.call("thread.join", thread_id="t1")
    await ana.quiet()
    asked.clear()
    updated = await server.update_thread("acme", "t1", {"topic": "billing", "elevated": True})
    assert updated.metadata == {"topic": "billing", "elevated": True}
    for peer in (ana, bruno):
        pushed = await peer.push("thread_updated")
        assert pushed["metadata"] == updated.metadata and pushed["author"]["role"] == "system"
    await server.remove_member("acme", "t1", lookalike(BRUNO))
    removed = await bruno.push("member_removed")
    assert (removed["identity"]["id"], removed["author"]["role"]) == ("bruno", "system")
    assert (await ana.push("member_removed"))["identity"]["id"] == "bruno"
    assert (await ana.push("left"))["sender"]["id"] == "bruno"
    assert asked == []
    await say(ana, "t1", "after")
    assert "message" not in [p["kind"] for p in await bruno.quiet()]
    history = (await ana.call("thread.history", thread_id="t1"))["events"]
    assert [(e["kind"], e["author"]["role"]) for e in history][-3:] == [
        ("thread_updated", "system"),
        ("member_removed", "system"),
        ("message", "user"),
    ]
    with pytest.raises(NotFoundError, match="^server: thread 'ghost' was not found$"):
        await server.update_thread("acme", "ghost", {})
    with pytest.raises(NotFoundError, match="^server: thread 'ghost' was not found$"):
        await server.remove_member("acme", "ghost", BRUNO)
    with pytest.raises(NotFoundError, match="^server: 'bruno' is not a member of thread 't1'$"):
        await server.remove_member("acme", "t1", BRUNO)
    with pytest.raises(InputError, match="^server: a scope is required$"):
        await server.update_thread("", "t1", {})


async def test_the_server_lists_every_thread_of_a_scope_by_metadata(server: Server) -> None:
    catalog: list[tuple[str, dict[str, Any]]] = [
        ("a", {"elevated": True, "location": "reno", "machine": {"id": "press-4", "line": 2}}),
        ("b", {"elevated": "true", "location": "reno"}),
        ("c", {"elevated": True, "location": "austin", "tags": ["leak", "urgent"]}),
        ("d", {"elevated": False, "count": 1}),
        ("e", {}),
    ]
    for thread_id, metadata in catalog:
        await server.create_thread("acme", thread_id=thread_id, metadata=metadata)
    await server.create_thread("globex", thread_id="g", metadata={"elevated": True})

    async def listed(**query: Any) -> list[str]:
        return [t.id for t in await server.list_threads("acme", **query)]

    assert await listed() == ["e", "d", "c", "b", "a"]
    assert await listed(metadata={}) == ["e", "d", "c", "b", "a"]
    assert await listed(metadata={"elevated": True}) == ["c", "a"]
    assert await listed(metadata={"elevated": "true"}) == ["b"]
    assert await listed(metadata={"elevated": True, "location": "reno"}) == ["a"]
    assert await listed(metadata={"machine": {"id": "press-4"}}) == ["a"]
    assert await listed(metadata={"tags": ["urgent"]}) == ["c"]
    assert await listed(metadata={"count": True}) == []
    assert await listed(metadata={"elevated": True}, limit=1) == ["c"]
    assert await listed(metadata={"elevated": True}, before="c") == ["a"]
    assert [t.scope for t in await server.list_threads("globex")] == ["globex"]
    with pytest.raises(InputError, match="^server: limit must be between 1 and 500$"):
        await server.list_threads("acme", limit=0)
    with pytest.raises(InputError, match="^server: a scope is required$"):
        await server.list_threads("")


async def test_a_members_own_list_is_unchanged_and_takes_no_metadata(server: Server) -> None:
    await server.create_thread("acme", thread_id="elsewhere", metadata={"elevated": True})
    ana = await room(server)
    assert [t["id"] for t in (await ana.call("thread.list"))["threads"]] == ["t1"]
    with pytest.raises(InputError, match="^server: invalid thread.list payload"):
        await ana.call("thread.list", metadata={"elevated": True})
    assert [t.id for t in await server.list_threads("acme")] == ["t1", "elsewhere"]
