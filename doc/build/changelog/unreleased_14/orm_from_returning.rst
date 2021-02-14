.. change::
    :tags: feature, orm

    The ORM used in :term:`2.0 style` can now return ORM objects from the rows
    returned by an UPDATE..RETURNING or INSERT..RETURNING statement, by
    supplying the construct to :meth:`_sql.Select.from_statement` in an ORM
    context.

    .. seealso::

      :ref:`orm_dml_returning_objects`


