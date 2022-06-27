.. change::
    :tags: bug, engine

    Passing a :class:`.DefaultGenerator` object such as a :class:`.Sequence` to
    the :meth:`.Connection.execute` method is deprecated, as this method is
    typed as returning a :class:`.CursorResult` object, and not a plain scalar
    value. The :meth:`.Connection.scalar` method should be used instead, which
    has been reworked with new internal codepaths to suit invoking a SELECT for
    default generation objects without going through the
    :meth:`.Connection.execute` method.