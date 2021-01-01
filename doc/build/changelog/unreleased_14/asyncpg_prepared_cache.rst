.. change::
    :tags: postgresql, performance

    Enhanced the performance of the asyncpg dialect by caching the asyncpg
    PreparedStatement objects on a per-connection basis. For a test case that
    makes use of the same statement on a set of pooled connections this appears
    to grant a 10-20% speed improvement.  The cache size is adjustable and may
    also be disabled.

    .. seealso::

        :ref:`asyncpg_prepared_statement_cache`
