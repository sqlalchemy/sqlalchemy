from sqlalchemy.dialects.mssql.base import MSExecutionContext, MSDialect
from sqlalchemy.connectors.mxodbc import MxODBCConnector
from sqlalchemy import types as sqltypes
import re
import sys

from sqlalchemy.dialects.mssql.pyodbc import MSExecutionContext_pyodbc

# The pyodbc execution context seems to work for mxODBC; reuse it here
MSExecutionContext_mxodbc = MSExecutionContext_pyodbc


class MSDialect_mxodbc(MxODBCConnector, MSDialect):
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False

    execution_ctx_cls = MSExecutionContext_mxodbc


    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_mxodbc, self).__init__(**params)
        self.description_encoding = description_encoding
        
    def initialize(self, connection):
        super(MSDialect_mxodbc, self).initialize(connection)
        dbapi_con = connection.connection
        
dialect = MSDialect_mxodbc
from sqlalchemy.dialects.mssql.base import MSExecutionContext, MSDialect
from sqlalchemy.connectors.mxodbc import MxODBCConnector
from sqlalchemy import types as sqltypes
import re
import sys

from sqlalchemy.dialects.mssql.pyodbc import MSExecutionContext_pyodbc

# The pyodbc execution context seems to work for mxODBC; reuse it here
MSExecutionContext_mxodbc = MSExecutionContext_pyodbc


class MSDialect_mxodbc(MxODBCConnector, MSDialect):
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False

    execution_ctx_cls = MSExecutionContext_mxodbc


    def __init__(self, description_encoding='latin-1', **params):
        super(MSDialect_mxodbc, self).__init__(**params)
        self.description_encoding = description_encoding
        
    def initialize(self, connection):
        super(MSDialect_mxodbc, self).initialize(connection)
        dbapi_con = connection.connection
        
dialect = MSDialect_mxodbc
