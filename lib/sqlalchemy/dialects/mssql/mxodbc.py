import re
import sys

from sqlalchemy import types as sqltypes
from sqlalchemy.connectors.mxodbc import MxODBCConnector
from sqlalchemy.dialects.mssql.base import MSDialect
from sqlalchemy.dialects.mssql.pyodbc import MSExecutionContext_pyodbc

# The pyodbc execution context seems to work for mxODBC; reuse it here

class MSExecutionContext_mxodbc(MSExecutionContext_pyodbc):
    pass

class MSDialect_mxodbc(MxODBCConnector, MSDialect):

    execution_ctx_cls = MSExecutionContext_mxodbc

    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_mxodbc, self).__init__(**params)
        self.description_encoding = description_encoding

dialect = MSDialect_mxodbc

