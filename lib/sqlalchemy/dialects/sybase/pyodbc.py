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
from sqlalchemy.connectors.pyodbc import PyODBCConnector

import decimal
from sqlalchemy import processors, types as sqltypes

# TODO: should this be part of pyodbc connectors ??? applies to MSSQL too ?
class _SybNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        super_process = super(_SybNumeric, self).bind_processor(dialect)
        
        def process(value):
            if self.asdecimal and \
                    isinstance(value, decimal.Decimal) and \
                    value.adjusted() < -6:
                return processors.to_float(value)
            elif super_process:
                return super_process(value)
            else:
                return value
        return process


class SybaseExecutionContext_pyodbc(SybaseExecutionContext):
    def set_ddl_autocommit(self, connection, value):
        if value:
            connection.autocommit = True
        else:
            connection.autocommit = False



class SybaseDialect_pyodbc(PyODBCConnector, SybaseDialect):
    execution_ctx_cls = SybaseExecutionContext_pyodbc

    colspecs = {
        sqltypes.Numeric:_SybNumeric,
        sqltypes.Float:sqltypes.Float,
    }

dialect = SybaseDialect_pyodbc
