.. change::
    :tags: change, postgresql

    In support of new PostgreSQL features including the psycopg3 dialect as
    well as extended "fast insertmany" support, the system by which typing
    information for bound parameters is passed to the PostgreSQL database has
    been redesigned to use inline casts emitted by the SQL compiler, and is now
    applied to all PostgreSQL dialects. This is in contrast to the previous
    approach which would rely upon the DBAPI in use to render these casts
    itself, which in cases such as that of pg8000 and the adapted asyncpg
    driver, would use the pep-249 ``setinputsizes()`` method, or with the
    psycopg2 driver would rely on the driver itself in most cases, with some
    special exceptions made for ARRAY.

    The new approach now has all PostgreSQL dialects rendering these casts as
    needed using PostgreSQL double-colon style within the compiler, and the use
    of ``setinputsizes()`` is removed for PostgreSQL dialects, as this was not
    generally part of these DBAPIs in any case (pg8000 being the only
    exception, which added the method at the request of SQLAlchemy developers).

    Advantages to this approach include per-statement performance, as no second
    pass over the compiled statement is required at execution time, better
    support for all DBAPIs, as there is now one consistent system of applying
    typing information, and improved transparency, as the SQL logging output,
    as well as the string output of a compiled statement, will show these casts
    present in the statement directly, whereas previously these casts were not
    visible in logging output as they would occur after the statement were
    logged.


