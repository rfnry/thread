import inspect
import subprocess
import sys
from collections.abc import Callable
from typing import Any

import rfnry_thread
from rfnry_thread import models

EXPECTED = {
    "Server",
    "Connection",
    "Client",
    "Stream",
    "Joined",
    "StatusChanged",
    "Identity",
    "IdentityLike",
    "Thread",
    "Member",
    "Text",
    "Image",
    "Audio",
    "Document",
    "Part",
    "Content",
    "Message",
    "Step",
    "Edit",
    "Retract",
    "Read",
    "MemberAdded",
    "MemberRemoved",
    "ThreadUpdated",
    "Custom",
    "Event",
    "resolve",
    "Working",
    "Idle",
    "StreamStart",
    "StreamDelta",
    "StreamEnd",
    "Cancel",
    "Present",
    "Left",
    "Invited",
    "ThreadDeleted",
    "Marked",
    "Signal",
    "ThreadTools",
    "Tool",
    "Tools",
    "Storage",
    "InMemoryStorage",
    "PostgresStorage",
    "ThreadException",
    "NotFoundError",
    "InputError",
    "ConfigurationError",
    "ForbiddenError",
    "StorageError",
    "TransportError",
}

CLIENT = {
    "connect",
    "create_thread",
    "get_thread",
    "list_threads",
    "update_thread",
    "delete_thread",
    "join",
    "leave",
    "follow",
    "mark",
    "marked",
    "history",
    "present",
    "add_member",
    "remove_member",
    "list_members",
    "send",
    "stream",
    "step",
    "edit",
    "retract",
    "read",
    "custom",
    "working",
    "idle",
    "cancel",
    "events",
    "status",
    "close",
}

SERVER = {
    "serve",
    "connect",
    "start",
    "stop",
    "create_thread",
    "list_threads",
    "update_thread",
    "add_member",
    "remove_member",
    "delete_thread",
    "delete_scope",
    "verify_absent",
}


def test_public_names_are_exactly_these() -> None:
    assert set(rfnry_thread.__all__) == EXPECTED
    for name in EXPECTED:
        assert getattr(rfnry_thread, name) is not None


def test_the_client_and_the_server_show_their_verbs_and_hide_the_rest() -> None:
    assert {n for n in vars(rfnry_thread.Client) if not n.startswith("_")} == CLIENT
    assert {n for n in vars(rfnry_thread.Server) if not n.startswith("_")} == SERVER


def test_the_identity_the_verbs_take_is_exported() -> None:
    assert rfnry_thread.IdentityLike is models.IdentityLike
    takes: dict[Callable[..., Any], str] = {
        rfnry_thread.Server.serve: "identity",
        rfnry_thread.Server.connect: "identity",
        rfnry_thread.Server.add_member: "identity",
        rfnry_thread.Server.remove_member: "identity",
        rfnry_thread.Client.add_member: "identity",
        rfnry_thread.Client.remove_member: "identity",
        rfnry_thread.Client.send: "triggered_by",
        rfnry_thread.Client.stream: "triggered_by",
        rfnry_thread.Client.step: "triggered_by",
        rfnry_thread.Client.cancel: "to",
        rfnry_thread.ThreadTools.__init__: "triggered_by",
    }
    for verb, name in takes.items():
        annotation = inspect.signature(verb).parameters[name].annotation
        assert annotation.removesuffix(" | None") == "IdentityLike", verb


def test_importing_the_package_imports_no_driver_and_no_framework() -> None:
    probe = "import rfnry_thread, sys; print(sorted(m for m in sys.modules if m.split('.')[0] in {'asyncpg', 'fastapi', 'starlette'}))"
    assert (
        subprocess.run(
            [sys.executable, "-c", probe], capture_output=True, text=True, check=True
        ).stdout.strip()
        == "[]"
    )
