.. change::
    :tags: change, engine

    The ``server_side_cursors`` engine-wide parameter is deprecated and will be
    removed in a future release.  For unbuffered cursors, the
    :paramref:`_engine.Connection.execution_options.stream_results` execution
    option should be used on a per-execution basis.
