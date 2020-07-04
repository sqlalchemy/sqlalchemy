from ... import exc


class AsyncMethodRequired(exc.InvalidRequestError):
    """an API can't be used because its result would not be
    compatible with async"""


class AsyncContextNotStarted(exc.InvalidRequestError):
    """a startable context manager has not been started."""


class AsyncContextAlreadyStarted(exc.InvalidRequestError):
    """a startable context manager is already started."""
