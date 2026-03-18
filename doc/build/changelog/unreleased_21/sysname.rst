.. change::
    :tags: bug, mssql, reflection

    Fixed MSSQL column reflection to resolve alias types (such as ``sysname``)
    by falling back to the base system type from ``sys.types``. The reflection
    query now joins ``sys.types`` a second time to look up the base type when
    the user type name is not present in
    :attr:`.MSDialect.ischema_names`. Previously, alias types like ``sysname``
    (a SQL Server built-in alias for ``NVARCHAR(128) NOT NULL``) would produce
    a "Did not recognize type" warning and be reflected as :class:`.NullType`.
    The join uses a left outer join so that CLR types such as ``geography``,
    ``geometry``, and ``hierarchyid``, which have no corresponding base type
    row, continue to work as before.
