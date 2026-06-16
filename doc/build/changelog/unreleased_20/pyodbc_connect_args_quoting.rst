.. change::
    :tags: bug, mssql

    Tightened the construction of the ODBC connection string in the pyodbc
    connector so that the driver name and the names of pass-through
    connection parameters are brace-quoted in the same way their values
    already are.  Previously a ``}`` in the driver name, or a ``;`` in the
    name of a pass-through parameter, could close the surrounding token
    early and leave the remainder of the string to be read as further
    connection attributes.  The same fix is applied to the mssql-python
    connector.  Pull request courtesy dxbjavid.
