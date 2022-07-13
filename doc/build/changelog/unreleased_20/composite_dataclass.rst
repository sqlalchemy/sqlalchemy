.. change::
    :tags: feature, orm

    The :func:`_orm.composite` mapping construct now supports automatic
    resolution of values when used with a Python ``dataclass``; the
    ``__composite_values__()`` method no longer needs to be implemented as this
    method is derived from inspection of the dataclass.

    Additionally, classes mapped by :class:`_orm.composite` now support
    ordering comparison operations, e.g. ``<``, ``>=``, etc.

    See the new documentation at :ref:`mapper_composite` for examples.