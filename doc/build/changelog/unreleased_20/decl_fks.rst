.. change::
    :tags: feature, orm

    Declarative mixins which use :class:`_schema.Column` objects that contain
    :class:`_schema.ForeignKey` references no longer need to use
    :func:`_orm.declared_attr` to achieve this mapping; the
    :class:`_schema.ForeignKey` object is copied along with the
    :class:`_schema.Column` itself when the column is applied to the declared
    mapping.