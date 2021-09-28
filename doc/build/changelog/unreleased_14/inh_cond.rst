.. change::
    :tags: bug, orm

    Improved the exception message generated when configuring a mapping with
    joined table inheritance where the two tables either have no foreign key
    relationships set up, or where they have multiple foreign key relationships
    set up. The message is now ORM specific and includes context that the
    :paramref:`_orm.Mapper.inherit_condition` parameter may be needed
    particularly for the ambiguous foreign keys case.

