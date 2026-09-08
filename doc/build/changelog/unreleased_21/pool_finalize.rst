.. change::
    :tags: changed, engine

    Improved the implementation of :class:`_pool.Pool` to use
    ``weakref.finalize()`` instead of a ``weakref.ref()`` finalizer to handle
    GC cleanup of non-detached, pooled connections that were not explicitly
    closed.
