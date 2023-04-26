.. change::
    :tags: bug, sql

    Fixed the base class for dialect-specific float/double types; Oracle
    :class:`_oracle.BINARY_DOUBLE` now subclasses :class:`_sqltypes.Double`,
    and internal types for :class:`_sqltypes.Float` for asyncpg and pg8000 now
    correctly subclass :class:`_sqltypes.Float`.
