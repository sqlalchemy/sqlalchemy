.. change::
    :tags: engine, removed

    Removed the previously deprecated ``case_sensitive`` parameter from
    :func:`_sa.create_engine`, which would impact only the lookup of string
    column names in Core-only result set rows; it had no effect on the behavior
    of the ORM. The effective behavior of what ``case_sensitive`` refers
    towards remains at its default value of ``True``, meaning that string names
    looked up in ``row._mapping`` will match case-sensitively, just like any
    other Python mapping.

    Note that the ``case_sensitive`` parameter was not in any way related to
    the general subject of case sensitivity control, quoting, and "name
    normalization" (i.e. converting for databases that consider all uppercase
    words to be case insensitive) for DDL identifier names, which remains a
    normal core feature of SQLAlchemy.


