.. change::
    :tags: usecase, engine

    Implemented new :paramref:`_engine.Connection.execution_options.yield_per`
    execution option for :class:`_engine.Connection` in Core, to mirror that of
    the same :ref:`yield_per <orm_queryguide_yield_per>` option available in
    the ORM. The option sets both the
    :paramref:`_engine.Connection.execution_options.stream_results` option at
    the same time as invoking :meth:`_engine.Result.yield_per`, to provide the
    most common streaming result configuration which also mirrors that of the
    ORM use case in its usage pattern.

    .. seealso::

        :ref:`engine_stream_results` - revised documentation


.. change::
    :tags: bug, engine

    Fixed bug in :class:`_engine.Result` where the usage of a buffered result
    strategy would not be used if the dialect in use did not support an
    explicit "server side cursor" setting, when using
    :paramref:`_engine.Connection.execution_options.stream_results`. This is in
    error as DBAPIs such as that of SQLite and Oracle already use a
    non-buffered result fetching scheme, which still benefits from usage of
    partial result fetching.   The "buffered" strategy is now used in all
    cases where :paramref:`_engine.Connection.execution_options.stream_results`
    is set.


.. change::
    :tags: bug, engine
    :tickets: 8199

    Added :meth:`.FilterResult.yield_per` so that result implementations
    such as :class:`.MappingResult`, :class:`.ScalarResult` and
    :class:`.AsyncResult` have access to this method.
