.. change::
    :tags: bug, sqlite

    Fixed bug where the error message for SQLite invalid isolation level on the
    pysqlite driver would fail to indicate that "AUTOCOMMIT" is one of the
    valid isolation levels.
