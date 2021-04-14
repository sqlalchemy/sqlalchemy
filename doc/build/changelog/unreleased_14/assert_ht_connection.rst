.. change::
    :tags: bug, engine

    The :meth:`_engine.Dialect.has_table` method now raises an informative
    exception if a non-Connection is passed to it, as this incorrect behavior
    seems to be common.  This method is not intended for external use outside
    of a dialect.  Please use the :meth:`.Inspector.has_table` method
    or for cross-compatibility with older SQLAlchemy versions, the
    :meth:`_engine.Engine.has_table` method.

