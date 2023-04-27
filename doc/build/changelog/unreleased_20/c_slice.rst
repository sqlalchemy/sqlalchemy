.. change::
    :tags: usecase, sql
    :tickets: 8285

    Added support for slice access with :class:`.ColumnCollection`, e.g.
    ``table.c[0:5]``, ``subquery.c[:-1]`` etc. Slice access returns a sub
    :class:`.ColumnCollection` in the same way as passing a tuple of keys. This
    is a natural continuation of the key-tuple access added for :ticket:`8285`,
    where it appears to be an oversight that the slice access use case was
    omitted.
