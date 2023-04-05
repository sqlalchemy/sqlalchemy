.. change::
    :tags: bug, oracle

    Fixed issue where the :class:`_sqltypes.Uuid` datatype could not be used in
    an INSERT..RETURNING clause with the Oracle dialect.
