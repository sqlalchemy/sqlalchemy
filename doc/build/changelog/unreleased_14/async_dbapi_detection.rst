.. change::
    :tags: asyncio

    The SQLAlchemy async mode now detects and raises an informative
    error when an non asyncio compatible :term:`DBAPI` is used.
    Using a standard ``DBAPI`` with async SQLAlchemy will cause
    it to block like any sync call, interrupting the executing asyncio
    loop.
