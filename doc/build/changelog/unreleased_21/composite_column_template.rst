.. change::
    :tags: feature, orm

    Added :paramref:`_orm.composite.column_template` parameter to
    :func:`_orm.composite`.  When the composite class is a dataclass, this
    parameter accepts a string template such as ``"person_%s"``, containing
    exactly one ``%s`` placeholder, that's used to generate column names for
    dataclass fields that don't otherwise have an explicit name, rather than
    using the bare field name.  This removes the need to hand-write a
    :func:`_orm.mapped_column` for each field when the same composite
    dataclass is mapped multiple times on the same class with different
    column-name prefixes.
