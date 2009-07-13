from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy.engine import default

class PostgreSQL_jdbcExecutionContext(default.DefaultExecutionContext):
    pass

class PostgreSQL_jdbc(ZxJDBCConnector, PGDialect):
    execution_ctx_cls = PostgreSQL_jdbcExecutionContext

    jdbc_db_name = 'postgresql'
    jdbc_driver_name = "org.postgresql.Driver"
    

    def _get_server_version_info(self, connection):
        return tuple(int(x) for x in connection.connection.dbversion.split('.'))
        
dialect = PostgreSQL_jdbc