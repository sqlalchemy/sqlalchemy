.. change::
    :tags: bug, mssql, orm

    Enabled the "sane_rowcount" flag for the pymssql dialect, indicating
    that the DBAPI now reports the correct number of rows affected from
    an UPDATE or DELETE statement.  This impacts mostly the ORM versioning
    feature in that it now can verify the number of rows affected on a
    target version.