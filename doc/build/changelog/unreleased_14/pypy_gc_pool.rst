.. change::
    :tags: bug, pool, pypy
    :tickets: 5842

    Fixed issue where connection pool would not return connections to the pool
    or otherwise be finalized upon garbage collection under pypy if the checked
    out connection fell out of scope without being closed.   This is a long
    standing issue due to pypy's difference in GC behavior that does not call
    weakref finalizers if they are relative to another object that is also
    being garbage collected.  A strong reference to the related record is now
    maintained so that the weakref has a strong-referenced "base" to trigger
    off of.
