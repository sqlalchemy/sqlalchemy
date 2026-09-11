.. change::
    :tags: bug, orm
    :tickets: 13583
    :versions: 2.1.0

    Fixed issue where using :func:`_orm.aliased` with both the
    :paramref:`_orm.aliased.name` and :paramref:`_orm.aliased.flat`
    parameters, against an entity that is mapped to a join which includes
    anonymously named aliases, would embed the anonymous name symbol within
    the names generated for each element of the join, producing unusable
    SQL.  An anonymously named element of the join is now aliased
    anonymously.
