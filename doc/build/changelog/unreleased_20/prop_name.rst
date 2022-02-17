.. change::
    :tags: change, orm

    To better accommodate explicit typing, the names of some ORM constructs
    that are typically constructed internally, but nonetheless are sometimes
    visible in messaging as well as typing, have been changed to more succinct
    names which also match the name of their constructing function (with
    different casing), in all cases maintaining aliases to the old names for
    the forseeable future:

    * :class:`_orm.RelationshipProperty` becomes an alias for the primary name
      :class:`_orm.Relationship`, which is constructed as always from the
      :func:`_orm.relationship` function
    * :class:`_orm.SynonymProperty` becomes an alias for the primary name
      :class:`_orm.Synonym`, constructed as always from the
      :func:`_orm.synonym` function
    * :class:`_orm.CompositeProperty` becomes an alias for the primary name
      :class:`_orm.Composite`, constructed as always from the
      :func:`_orm.composite` function