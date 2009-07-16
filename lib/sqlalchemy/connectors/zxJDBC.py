from sqlalchemy.connectors import Connector

import sys
import re
import urllib

class ZxJDBCConnector(Connector):
    driver='zxjdbc'
    
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    
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
        return isinstance(e, self.dbapi.ProgrammingError) and 'connection is closed' in str(e)

    def _get_server_version_info(self, connection):
        # use connection.connection.dbversion, and parse appropriately
        # to get a tuple
        raise NotImplementedError()
