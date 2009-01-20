from sqlalchemy.dialects.mssql.base import MSExecutionContext, MSDialect, MSDateTimeAsDate, MSDateTimeAsTime
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy import types as sqltypes

import sys

class MSExecutionContext_pyodbc(MSExecutionContext):
    def pre_exec(self):
        """where appropriate, issue "select scope_identity()" in the same statement"""
        super(MSSQLExecutionContext_pyodbc, self).pre_exec()
        if self.compiled.isinsert and self.HASIDENT and not self.IINSERT \
                and len(self.parameters) == 1 and self.dialect.use_scope_identity:
            self.statement += "; select scope_identity()"

    def post_exec(self):
        if self.HASIDENT and not self.IINSERT and self.dialect.use_scope_identity and not self.executemany:
            import pyodbc
            # Fetch the last inserted id from the manipulated statement
            # We may have to skip over a number of result sets with no data (due to triggers, etc.)
            while True:
                try:
                    row = self.cursor.fetchone()
                    break
                except pyodbc.Error, e:
                    self.cursor.nextset()
            self._last_inserted_ids = [int(row[0])]
        else:
            super(MSSQLExecutionContext_pyodbc, self).post_exec()


class MSDialect_pyodbc(PyODBCConnector, MSDialect):
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    # PyODBC unicode is broken on UCS-4 builds
    supports_unicode = sys.maxunicode == 65535
    supports_unicode_statements = supports_unicode
    execution_ctx_cls = MSExecutionContext_pyodbc

    pyodbc_driver_name = 'SQL Server'

    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_pyodbc, self).__init__(**params)
        self.description_encoding = description_encoding
        self.use_scope_identity = self.dbapi and hasattr(self.dbapi.Cursor, 'nextset')
        
        if self.server_version_info < (10,):
            self.colspecs = MSDialect.colspecs.copy()
            self.colspecs[sqltypes.Date] = MSDateTimeAsDate
            self.colspecs[sqltypes.Time] = MSDateTimeAsTime

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.ProgrammingError):
            return "The cursor's connection has been closed." in str(e) or 'Attempt to use a closed connection.' in str(e)
        elif isinstance(e, self.dbapi.Error):
            return '[08S01]' in str(e)
        else:
            return False

dialect = MSDialect_pyodbc