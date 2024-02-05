.. change::
    :tags: bug, mssql

    Fixed an issue regarding the use of the :class:`.Uuid` datatype with the
    :paramref:`.Uuid.as_uuid` parameter set to False, when using the pymssql
    dialect. ORM-optimized INSERT statements (e.g. the "insertmanyvalues"
    feature) would not correctly align primary key UUID values for bulk INSERT
    statements, resulting in errors.  Similar issues were fixed for the
    PostgreSQL drivers as well.


.. change::
    :tags: bug, postgresql

    Fixed an issue regarding the use of the :class:`.Uuid` datatype with the
    :paramref:`.Uuid.as_uuid` parameter set to False, when using the pymssql
    dialect. ORM-optimized INSERT statements (e.g. the "insertmanyvalues"
    feature) would not correctly align primary key UUID values for bulk INSERT
    statements, resulting in errors.  Similar issues were fixed for the
    pymssql driver as well.
