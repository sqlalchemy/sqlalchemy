.. change::
    :tags: change, asyncio

    The ``async_fallback`` dialect argument is now deprecated, and will be
    removed in SQLAlchemy 2.1.   This flag has not been used for SQLAlchemy's
    test suite for some time.   asyncio dialects can still run in a synchronous
    style by running code within a greenlet using :func:`_util.greenlet_spawn`.
