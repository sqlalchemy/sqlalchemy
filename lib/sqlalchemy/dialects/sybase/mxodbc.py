from sqlalchemy.dialects.sybase.base import SybaseDialect, SybaseExecutionContext
from sqlalchemy.connectors.mxodbc import MxODBCConnector

class SybaseExecutionContext_mxodbc(SybaseExecutionContext):
    pass

class Sybase_mxodbc(MxODBCConnector, SybaseDialect):
    execution_ctx_cls = SybaseExecutionContext_mxodbc

dialect = Sybase_mxodbc