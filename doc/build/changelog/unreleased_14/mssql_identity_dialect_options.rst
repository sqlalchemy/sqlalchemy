.. change::
    :tags: mssql, reflection
    :tickets: 5527

    As part of the support for reflecting :class:`_schema.Identity` objects,
    the method :meth:`_reflection.Inspector.get_columns` no longer returns
    ``mssql_identity_start`` and ``mssql_identity_increment`` as part of the
    ``dialect_options``. Use the information in the ``identity`` key instead.
