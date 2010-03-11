import re
import sys

from sqlalchemy import types as sqltypes
from sqlalchemy.connectors.mxodbc import MxODBCConnector
from sqlalchemy.dialects.mssql.pyodbc import MSExecutionContext_pyodbc
from sqlalchemy.dialects.mssql.base import MSExecutionContext, MSDialect


class MSExecutionContext_mxodbc(MSExecutionContext_pyodbc):
    """
    The pyodbc execution context is useful for enabling
    SELECT SCOPE_IDENTITY in cases where OUTPUT clause
    does not work (tables with insert triggers).
    """
    #todo - investigate whether the pyodbc execution context
    #       is really only being used in cases where OUTPUT
    #       won't work.

class MSDialect_mxodbc(MxODBCConnector, MSDialect):

    execution_ctx_cls = MSExecutionContext_mxodbc

    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_mxodbc, self).__init__(**params)
        self.description_encoding = description_encoding

dialect = MSDialect_mxodbc

