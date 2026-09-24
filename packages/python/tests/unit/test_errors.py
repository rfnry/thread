import pytest

from rfnry_thread.errors import (
    ConfigurationError,
    ForbiddenError,
    InputError,
    NotFoundError,
    StorageError,
    ThreadException,
    TransportError,
    raised,
)

FAMILY = [
    NotFoundError,
    InputError,
    ConfigurationError,
    ForbiddenError,
    StorageError,
    TransportError,
]


@pytest.mark.parametrize("cls", FAMILY)
def test_every_child_is_a_thread_exception_named_error(cls: type[ThreadException]) -> None:
    assert issubclass(cls, ThreadException)
    assert cls.__name__.endswith("Error")


def test_the_names_do_not_shadow_builtins() -> None:
    assert not issubclass(ForbiddenError, PermissionError)
    assert not issubclass(TransportError, ConnectionError)


@pytest.mark.parametrize("cls", [ThreadException, *FAMILY])
def test_a_wire_code_raises_the_same_class_with_the_same_message(
    cls: type[ThreadException],
) -> None:
    error = raised(cls.code, "server: thread 'x' was not found")
    assert type(error) is cls
    assert str(error) == "server: thread 'x' was not found"


def test_an_unknown_code_raises_the_root() -> None:
    assert type(raised("teapot", "server: no")) is ThreadException
