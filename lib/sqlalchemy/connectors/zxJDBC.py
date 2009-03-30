from sqlalchemy.connectors import Connector

import sys
import re
import urllib

class ZxJDBCConnector(Connector):
    driver='zxjdbc'
    
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_unicode_binds = True
    supports_unicode_statements = False
    default_paramstyle = 'qmark'
    
    jdbc_db_name = None
    jdbc_driver_name = None
    
    @classmethod
    def dbapi(cls):
        from com.ziclix.python.sql import zxJDBC
        return zxJDBC

    def _driver_kwargs(self):
        """return kw arg dict to be sent to connect()."""
        return {}
        
    def create_connect_args(self, url):
        hostname = url.host
        dbname = url.database
        d, u, p, v = "jdbc:%s://%s/%s" % (self.jdbc_db_name, hostname, dbname), url.username, url.password, self.jdbc_driver_name
        return [[d, u, p, v], self._driver_kwargs()]
        
    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.ProgrammingError):
            return "The cursor's connection has been closed." in str(e) or 'Attempt to use a closed connection.' in str(e)
        elif isinstance(e, self.dbapi.Error):
            return '[08S01]' in str(e)
        else:
            return False

    def _get_server_version_info(self, connection):
        # use connection.connection.dbversion, and parse appropriately
        # to get a tuple
        raise NotImplementedError()
