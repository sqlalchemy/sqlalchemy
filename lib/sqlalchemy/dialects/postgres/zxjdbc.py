from sqlalchemy.dialects.postgres.base import PGDialect
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy.engine import default

class Postgres_jdbcExecutionContext(default.DefaultExecutionContext):
    pass

class Postgres_jdbc(ZxJDBCConnector, PGDialect):
    execution_ctx_cls = Postgres_jdbcExecutionContext

    jdbc_db_name = 'postgresql'
    jdbc_driver_name = "org.postgresql.Driver"
    

    def _get_server_version_info(self, connection):
        return tuple(int(x) for x in connection.connection.dbversion.split('.'))
        
dialect = Postgres_jdbc