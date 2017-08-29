.. change::
    :tags: feature, mssql

    Added support for "AUTOCOMMIT" isolation level, as established
    via :meth:`.Connection.execution_options`, to the
    PyODBC and pymssql dialects.   This isolation level sets the
    appropriate DBAPI-specific flags on the underlying
    connection object.