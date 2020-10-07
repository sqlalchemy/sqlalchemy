.. change::
    :tags: change, bug

    The internal clock used by the :class:`_pool.Pool` object is now
    time.monotonic_time() under Python 3.  Under Python 2, time.time() is still
    used, which is legacy. This clock is used to measure the age of a
    connection against its starttime, and used in comparisons against the
    pool_timeout setting as well as the last time the pool was marked as
    invalid to determine if the connection should be recycled. Previously,
    time.time() was used which was subject to inaccuracies as a result of
    system clock changes as well as poor time resolution on windows.
