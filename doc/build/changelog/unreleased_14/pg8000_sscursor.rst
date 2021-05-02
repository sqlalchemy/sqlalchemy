.. change::
    :tags: usecase, postgresql
    :tickets: 6198

    Add support for server side cursors in the pg8000 dialect for PostgreSQL.
    This allows use of the
    :paramref:`.Connection.execution_options.stream_results` option.
