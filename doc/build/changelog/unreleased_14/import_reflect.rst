.. change::
    :tags: bug, engine, regression

    Restored top level import for ``sqlalchemy.engine.reflection``. This
    ensures that the base :class:`_reflection.Inspector` class is properly
    registered so that :func:`_sa.inspect` works for third party dialects that
    don't otherwise import this package.

