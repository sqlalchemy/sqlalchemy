"""Support for the Oracle database via the zxjdbc JDBC connector."""
import re

from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy.dialects.oracle.base import OracleDialect

class Oracle_jdbc(ZxJDBCConnector, OracleDialect):

    jdbc_db_name = 'oracle'
    jdbc_driver_name = 'oracle.jdbc.driver.OracleDriver'

    def create_connect_args(self, url):
        hostname = url.host
        port = url.port or '1521'
        dbname = url.database
        jdbc_url = 'jdbc:oracle:thin:@%s:%s:%s' % (hostname, port, dbname)
        return [[jdbc_url, url.username, url.password, self.jdbc_driver_name],
                self._driver_kwargs()]
        
    def _get_server_version_info(self, connection):
        version = re.search(r'Release ([\d\.]+)', connection.connection.dbversion).group(1)
        return tuple(int(x) for x in version.split('.'))
        
dialect = Oracle_jdbc
