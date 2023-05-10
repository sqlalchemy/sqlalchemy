.. change::
    :tags: bug, asyncio

    Fixed issue in semi-private ``await_only()`` and ``await_fallback()``
    concurrency functions where the given awaitable would remain un-awaited if
    the function threw a ``GreenletError``, which could cause "was not awaited"
    warnings later on if the program continued. In this case, the given
    awaitable is now cancelled before the exception is thrown.
