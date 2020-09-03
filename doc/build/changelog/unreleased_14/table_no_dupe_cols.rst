.. change::
    :tags: change, sql
    :tickets: 5526

    The :class:`_schema.Table` class now raises a deprecation warning
    when columns with the same name are defined. To replace a column a new
    parameter :paramref:`_schema.Table.append_column.replace_existing` was
    added to the :meth:`_schema.Table.append_column` method.

    The :meth:`_expression.ColumnCollection.contains_column` will now
    raises an error when called with a string, suggesting the caller
    to use ``in`` instead.
