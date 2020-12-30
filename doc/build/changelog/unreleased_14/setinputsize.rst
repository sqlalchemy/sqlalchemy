.. change::
    :tags: bug, engine, postgresql, oracle

    Adjusted the "setinputsizes" logic relied upon by the cx_Oracle, asyncpg
    and pg8000 dialects to support a :class:`.TypeDecorator` that includes
    an override the :meth:`.TypeDecorator.get_dbapi_type()` method.

