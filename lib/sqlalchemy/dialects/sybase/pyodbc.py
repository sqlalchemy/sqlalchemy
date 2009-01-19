from sqlalchemy.dialects.sybase.base import SybaseDialect, SybaseExecutionContext
from sqlalchemy.connectors.pyodbc import PyODBCConnector

class SybaseExecutionContext_pyodbc(SybaseExecutionContext):
    pass


class Sybase_pyodbc(PyODBCConnector, SybaseDialect):
    execution_ctx_cls = SybaseExecutionContext_pyodbc

dialect = Sybase_pyodbc