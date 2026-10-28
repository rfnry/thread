from typing import Any

import pytest

from rfnry_thread.errors import ForbiddenError, InputError, NotFoundError
from rfnry_thread.models import Identity
from rfnry_thread.postgres import PostgresStorage
from rfnry_thread.server import Server

from .conftest import say
from .factories import ANA, HELPER
from .peer import Peer


def who(identity: Identity) -> dict[str, Any]:
    return identity.model_dump(mode="json")


def kinds(pushes: list[dict[str, Any]]) -> list[str]:
    events = [p for p in pushes if "seq" in p and "author" in p]
    return [e["content"][0]["text"] if e["kind"] == "message" else e["kind"] for e in events]


async def made(server: Server, history: str = "since_added") -> tuple[Peer, list[dict[str, Any]]]:
    ana = await Peer.open(server, ANA)
    reply = await ana.call("thread.create", thread_id="t1", history=history)
    assert reply["thread"]["history"] == history
    before = [await say(ana, "t1", f"before {n}") for n in range(2)]
    await ana.call("member.add", thread_id="t1", identity=who(HELPER))
    return ana, before


async def edit(ana: Peer, target: str, text: str) -> dict[str, Any]:
    reply = await ana.call(
        "event.send",
        thread_id="t1",
        kind="edit",
        client_id=f"edit-{target}-{text}",
        target=target,
        content=[{"type": "text", "text": text}],
    )
    return dict(reply["event"])


async def history(peer: Peer, **options: Any) -> list[str]:
    return kinds((await peer.call("thread.history", thread_id="t1", **options))["events"])


async def test_a_member_of_a_since_added_thread_reads_from_its_add_and_the_creator_all_of_it(
    server: Server,
) -> None:
    ana, before = await made(server)
    after = await say(ana, "t1", "after")
    helper = await Peer.open(server, HELPER)
    assert await history(helper) == ["member_added", "after"]
    assert await history(helper, after=0) == ["member_added", "after"]
    assert await history(helper, before=after["seq"]) == ["member_added"]
    assert await history(ana) == ["member_added", "before 0", "before 1", "member_added", "after"]
    with pytest.raises(NotFoundError, match=f"^server: event {before[0]['id']!r} was not found$"):
        await helper.call("event.get", thread_id="t1", event_id=before[0]["id"])
    got = await helper.call("event.get", thread_id="t1", event_id=after["id"])
    assert got["event"]["id"] == after["id"]


async def test_a_thread_keeps_its_whole_history_for_a_new_member_by_default(
    server: Server,
) -> None:
    await made(server, "all")
    helper = await Peer.open(server, HELPER)
    assert await history(helper) == ["member_added", "before 0", "before 1", "member_added"]


async def test_a_join_a_follow_and_a_rewind_replay_from_the_add(server: Server) -> None:
    ana, _ = await made(server)
    await say(ana, "t1", "after")
    joiner = await Peer.open(server, HELPER)
    await joiner.call("thread.join", thread_id="t1", since=0)
    assert kinds(await joiner.quiet()) == ["member_added", "after"]
    await joiner.call("thread.join", thread_id="t1", since=0)
    assert kinds(await joiner.quiet()) == ["member_added", "after"]
    follower = await Peer.open(server, HELPER)
    await follower.call("thread.follow", since={"t1": 0})
    assert kinds(await follower.quiet()) == ["member_added", "after"]
    resumer = await Peer.open(server, HELPER)
    await resumer.call("thread.follow", resume=True)
    assert kinds(await resumer.quiet()) == ["member_added", "after"]


async def test_an_edit_of_what_came_before_the_add_never_reaches_the_member(
    server: Server,
) -> None:
    ana, before = await made(server)
    helper = await Peer.open(server, HELPER)
    await helper.call("thread.join", thread_id="t1")
    await helper.quiet()
    hidden = await edit(ana, before[0]["id"], "rewritten")
    after = await say(ana, "t1", "after")
    shown = await edit(ana, after["id"], "after, edited")
    assert kinds(await helper.quiet()) == ["after", "edit"]
    assert await history(helper) == ["member_added", "after", "edit"]
    with pytest.raises(NotFoundError):
        await helper.call("event.get", thread_id="t1", event_id=hidden["id"])
    await helper.call("event.get", thread_id="t1", event_id=shown["id"])
    late = await Peer.open(server, HELPER)
    await late.call("thread.join", thread_id="t1", since=0)
    assert kinds(await late.quiet()) == ["member_added", "after", "edit"]


async def test_a_since_added_inbox_and_mark_start_at_the_add(server: Server) -> None:
    ana, _ = await made(server)
    helper = await Peer.open(server, HELPER)
    [listed] = (await helper.call("thread.list", preview=True))["threads"]
    assert listed["history"] == "since_added"
    assert "last_message" not in listed and listed["unread"] == 0
    with pytest.raises(InputError, match="^server: seq must be between 3 and 4$"):
        await helper.call("thread.mark", thread_id="t1", seq=2, back=True)
    await say(ana, "t1", "after")
    [listed] = (await helper.call("thread.list", preview=True))["threads"]
    assert listed["last_message"]["content"][0]["text"] == "after" and listed["unread"] == 1


async def test_a_member_added_again_reads_from_the_new_add(server: Server) -> None:
    ana, _ = await made(server)
    await say(ana, "t1", "while in")
    await ana.call("member.remove", thread_id="t1", identity_id="helper")
    await say(ana, "t1", "while out")
    await ana.call("member.add", thread_id="t1", identity=who(HELPER))
    await say(ana, "t1", "back")
    helper = await Peer.open(server, HELPER)
    assert await history(helper) == ["member_added", "back"]


async def test_the_server_makes_a_since_added_thread_and_refuses_an_unknown_setting(
    server: Server,
) -> None:
    made = await server.create_thread("acme", "t2", history="since_added")
    assert made.history == "since_added"
    ana = await Peer.open(server, ANA)
    with pytest.raises(InputError):
        await ana.call("thread.create", thread_id="t3", history="some")


async def test_a_long_history_before_the_add_does_not_truncate_a_replay_from_it(
    storage: PostgresStorage,
) -> None:
    server = Server(storage, replay_limit=3)
    ana = await Peer.open(server, ANA)
    await ana.call("thread.create", thread_id="t1", history="since_added")
    for n in range(4):
        await say(ana, "t1", f"before {n}")
    await ana.call("member.add", thread_id="t1", identity=who(HELPER))
    await say(ana, "t1", "after")
    helper = await Peer.open(server, HELPER)
    joined = await helper.call("thread.join", thread_id="t1", since=0)
    assert joined["truncated"] is False
    assert kinds(await helper.quiet()) == ["member_added", "after"]
    rewound = await helper.call("thread.join", thread_id="t1", since=0)
    assert rewound["truncated"] is False
    assert kinds(await helper.quiet()) == ["member_added", "after"]


async def test_a_member_removed_between_the_checks_reads_nothing(
    storage: PostgresStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = Server(storage)
    await made(server)
    helper = await Peer.open(server, HELPER)
    real = storage.is_member

    async def removed_meanwhile(scope: str, thread_id: str, identity_id: str) -> bool:
        found = await real(scope, thread_id, identity_id)
        if identity_id == "helper":
            await storage.remove_member(scope, thread_id, identity_id)
        return found

    monkeypatch.setattr(storage, "is_member", removed_meanwhile)
    with pytest.raises(ForbiddenError, match="^server: 'helper' is not a member of thread 't1'$"):
        await helper.call("thread.history", thread_id="t1")


async def test_an_event_before_the_add_cannot_be_probed_by_editing_or_retracting_it(
    server: Server,
) -> None:
    ana, before = await made(server)
    await say(ana, "t1", "see above", in_reply_to=before[0]["id"])
    helper = await Peer.open(server, HELPER)
    for kind, extra in (("retract", {}), ("edit", {"content": [{"type": "text", "text": "x"}]})):
        for target in (before[0]["id"], "nope"):
            with pytest.raises(NotFoundError, match=f"^server: event {target!r} was not found$"):
                await helper.call(
                    "event.send",
                    thread_id="t1",
                    kind=kind,
                    client_id=f"{kind}-{target}",
                    target=target,
                    **extra,
                )
