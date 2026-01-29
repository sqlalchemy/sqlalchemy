.. change::
    :tags: usecase, sql

    The :class:`.ColumnCollection` class hierarchy has been refactored to allow
    column names such as ``add``, ``remove``, ``update``, ``extend``, and
    ``clear`` to be used without conflicts. :class:`.ColumnCollection` is now
    an abstract base class, with mutation operations moved to
    :class:`.WriteableColumnCollection` and :class:`.DedupeColumnCollection`
    subclasses. The :class:`.ReadOnlyColumnCollection` exposed as attributes
    such as :attr:`.Table.c` no longer includes mutation methods that raised
    :class:`.NotImplementedError`, allowing these common column names to be
    accessed naturally, e.g. ``table.c.add``, ``table.c.remove``,
    ``table.c.update``, etc.
