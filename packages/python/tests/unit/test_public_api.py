import subprocess
import sys

import rfnry_thread

EXPECTED = {
    "Server",
    "Connection",
    "Client",
    "Stream",
    "Joined",
    "Identity",
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


def test_importing_the_package_imports_no_driver_and_no_framework() -> None:
    probe = "import rfnry_thread, sys; print(sorted(m for m in sys.modules if m.split('.')[0] in {'asyncpg', 'fastapi', 'starlette'}))"
    assert (
        subprocess.run(
            [sys.executable, "-c", probe], capture_output=True, text=True, check=True
        ).stdout.strip()
        == "[]"
    )
