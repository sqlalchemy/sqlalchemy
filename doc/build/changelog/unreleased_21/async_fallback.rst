.. change::
    :tags: change, asyncio

    Removed the compatibility ``async_fallback`` mode for async dialects,
    since it's no longer used by SQLAlchemy tests.
    Also removed the internal function ``await_fallback()`` and renamed
    the internal function ``await_only()`` to ``await_()``.
    No change is expected to user code.
