"""
Support for Sybase via pyodbc.

http://pypi.python.org/pypi/pyodbc/

Connect strings are of the form::

    sybase+pyodbc://<username>:<password>@<dsn>/
    sybase+pyodbc://<username>:<password>@<host>/<database>


"""

from sqlalchemy.dialects.sybase.base import SybaseDialect, SybaseExecutionContext
from sqlalchemy.connectors.pyodbc import PyODBCConnector

class SybaseExecutionContext_pyodbc(SybaseExecutionContext):
    def set_ddl_autocommit(self, connection, value):
        if value:
            connection.autocommit = True
        else:
            connection.autocommit = False


class SybaseDialect_pyodbc(PyODBCConnector, SybaseDialect):
    execution_ctx_cls = SybaseExecutionContext_pyodbc

dialect = SybaseDialect_pyodbc
