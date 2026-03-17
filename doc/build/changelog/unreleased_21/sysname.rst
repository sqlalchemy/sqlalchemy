.. change::
    :tags: bug, mssql, reflection

    Added ``sysname`` to the list of recognized MSSQL types
    (:attr:`.MSDialect.ischema_names`), mapping it to :class:`.NVARCHAR`.
    ``sysname`` is a SQL Server system-defined alias for ``NVARCHAR(128) NOT
    NULL`` and is commonly used in system catalogs and stored procedure
    parameters. Previously, columns using the ``sysname`` type would produce
    a "Did not recognize type" warning and be reflected as :class:`.NullType`.
