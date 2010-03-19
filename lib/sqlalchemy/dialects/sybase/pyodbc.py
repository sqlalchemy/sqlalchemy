"""
Support for Sybase via pyodbc.

http://pypi.python.org/pypi/pyodbc/

Connect strings are of the form::

    sybase+pyodbc://<username>:<password>@<dsn>/
    sybase+pyodbc://<username>:<password>@<host>/<database>

Unicode Support
---------------

The pyodbc driver currently supports usage of these Sybase types with 
Unicode or multibyte strings::

    CHAR
    NCHAR
    NVARCHAR
    TEXT
    VARCHAR

Currently *not* supported are::

    UNICHAR
    UNITEXT
    UNIVARCHAR
    
"""

from sqlalchemy.dialects.sybase.base import SybaseDialect, SybaseExecutionContext
from sqlalchemy.connectors.pyodbc import PyODBCConnector, PyODBCNumeric

from sqlalchemy import types as sqltypes, util

class _SybNumeric_pyodbc(PyODBCNumeric):
    convert_large_decimals_to_string = False

class SybaseExecutionContext_pyodbc(SybaseExecutionContext):
    def set_ddl_autocommit(self, connection, value):
        if value:
            connection.autocommit = True
        else:
            connection.autocommit = False



class SybaseDialect_pyodbc(PyODBCConnector, SybaseDialect):
    execution_ctx_cls = SybaseExecutionContext_pyodbc

    colspecs = {
        sqltypes.Numeric:_SybNumeric_pyodbc,
    }

dialect = SybaseDialect_pyodbc
