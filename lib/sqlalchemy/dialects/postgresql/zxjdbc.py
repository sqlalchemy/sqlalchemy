"""Support for the PostgreSQL database via the zxjdbc JDBC connector.

JDBC Driver
-----------

The official Postgresql JDBC driver is at http://jdbc.postgresql.org/.

"""
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy.dialects.postgresql.base import PGDialect

class PGDialect_zxjdbc(ZxJDBCConnector, PGDialect):
    jdbc_db_name = 'postgresql'
    jdbc_driver_name = 'org.postgresql.Driver'

    def _get_server_version_info(self, connection):
        return tuple(int(x) for x in connection.connection.dbversion.split('.'))

dialect = PGDialect_zxjdbc
