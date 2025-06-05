.. change::
    :tags: engine

    Improved validation of execution parameters passed to the
    :meth:`_engine.Connection.execute` and similar methods to
    provided a better error when tuples are passed in.
    Previously the execution would fail with a difficult to
    understand error message.
