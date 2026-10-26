.. change::
    :tags: bug, sql, postgresql, oracle

    Restored validation of the keyword phrases accepted by the
    :paramref:`_schema.ForeignKeyConstraint.ondelete` and
    :paramref:`_schema.ForeignKeyConstraint.match` parameters in three places
    where it was missing or too loose.  The PostgreSQL column list for ``SET
    NULL`` / ``SET DEFAULT``, added in 2.0.40, accepted any characters between
    the parenthesis and now accepts only a comma-separated list of plain or
    double-quoted column names; the Oracle dialect rendered ``ondelete``
    without validating it at all; and ``MATCH`` was not validated on any
    backend.  These values now raise :class:`.CompileError` for unexpected
    phrases, consistent with ``onupdate`` and ``initially``.
