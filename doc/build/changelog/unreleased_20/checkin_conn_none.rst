.. change::
    :tags: bug, typing

    Fixed the type signature for the :meth:`.PoolEvents.checkin` event to
    indicate that the given :class:`.DBAPIConnection` argument may be ``None``
    in the case where the connection has been invalidated.
