.. change::
    :tags: feature, mysql

    Added new construct :func:`_mysql.limit` which can be applied to any
    :func:`_sql.update` or :func:`_sql.delete` to provide the LIMIT keyword to
    UPDATE and DELETE.  This new construct supersedes the use of the
    "mysql_limit" dialect keyword argument.

