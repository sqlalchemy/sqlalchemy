.. change::
    :tags: feature, engine

    Dialect-specific constructs such as
    :meth:`_postgresql.Insert.on_conflict_do_update` can now stringify in-place
    without the need to specify an explicit dialect object.  The constructs,
    when called upon for ``str()``, ``print()``, etc. now have internal
    direction to call upon their appropriate dialect rather than the
    "default"dialect which doesn't know how to stringify these.   The approach
    is also adapted to generic schema-level create/drop such as
    :class:`_schema.AddConstraint`, which will adapt its stringify dialect to
    one indicated by the element within it, such as the
    :class:`_postgresql.ExcludeConstraint` object.

