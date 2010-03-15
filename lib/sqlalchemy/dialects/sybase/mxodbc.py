"""
Support for Sybase via mxodbc.

This dialect is a stub only and is likely non functional at this time.


"""
from sqlalchemy.dialects.sybase.base import SybaseDialect, SybaseExecutionContext
from sqlalchemy.connectors.mxodbc import MxODBCConnector

class SybaseExecutionContext_mxodbc(SybaseExecutionContext):
    pass

class SybaseDialect_mxodbc(MxODBCConnector, SybaseDialect):
    execution_ctx_cls = SybaseExecutionContext_mxodbc

dialect = SybaseDialect_mxodbc
