import re
import sys

from sqlalchemy import types as sqltypes
from sqlalchemy.connectors.mxodbc import MxODBCConnector
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.dialects.mssql.pyodbc import MSExecutionContext_pyodbc

# The pyodbc execution context seems to work for mxODBC; reuse it here

class MSExecutionContext_mxodbc(MSExecutionContext_pyodbc):
    
    def post_exec(self):
        # snag rowcount before the cursor is closed
        if not self.cursor.description:
            self._rowcount = self.cursor.rowcount
        super(MSExecutionContext_mxodbc, self).post_exec()
        
    @property
    def rowcount(self):
        if hasattr(self, '_rowcount'):
            return self._rowcount
        else:
            return self.cursor.rowcount

class MSDialect_mxodbc(MxODBCConnector, MSDialect):

    execution_ctx_cls = MSExecutionContext_mxodbc

    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_mxodbc, self).__init__(**params)
        self.description_encoding = description_encoding

dialect = MSDialect_mxodbc

