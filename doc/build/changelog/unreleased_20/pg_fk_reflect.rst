.. change::
    :tags: bug, postgresql

    Improved the foreign key reflection regular expression pattern used by the
    PostgreSQL dialect to be more permissive in matching identifier characters,
    allowing it to correctly handle unicode characters in table and column
    names. This change improves compatibility with PostgreSQL variants such as
    CockroachDB that may use different quoting patterns in combination with
    unicode characters in their identifiers.  Pull request courtesy Gord
    Thompson.
