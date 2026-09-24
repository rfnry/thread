from typing import ClassVar


class ThreadException(Exception):
    code: ClassVar[str] = "internal"


class NotFoundError(ThreadException):
    code = "not_found"


class InputError(ThreadException):
    code = "input"


class ConfigurationError(ThreadException):
    code = "configuration"


class ForbiddenError(ThreadException):
    code = "forbidden"


class StorageError(ThreadException):
    code = "storage"


class TransportError(ThreadException):
    code = "transport"


ERRORS: dict[str, type[ThreadException]] = {
    cls.code: cls
    for cls in (
        ThreadException,
        NotFoundError,
        InputError,
        ConfigurationError,
        ForbiddenError,
        StorageError,
        TransportError,
    )
}


def raised(code: str, message: str) -> ThreadException:
    return ERRORS.get(code, ThreadException)(message)
