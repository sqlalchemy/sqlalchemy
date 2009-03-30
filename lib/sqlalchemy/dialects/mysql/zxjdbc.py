from sqlalchemy.dialects.mysql.base import MySQLDialect, MySQLExecutionContext
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy import util
import re

class MySQL_jdbcExecutionContext(MySQLExecutionContext):
    def _real_lastrowid(self, cursor):
        return cursor.lastrowid

    def _lastrowid(self, cursor):
        cursor.execute("SELECT LAST_INSERT_ID()")
        return cursor.fetchone()[0]

class MySQL_jdbc(ZxJDBCConnector, MySQLDialect):
    execution_ctx_cls = MySQL_jdbcExecutionContext

    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False

    jdbc_db_name = 'mysql'
    jdbc_driver_name = "org.gjt.mm.mysql.Driver"
    
    def _detect_charset(self, connection):
        """Sniff out the character set in use for connection results."""

        # Prefer 'character_set_results' for the current connection over the
        # value in the driver.  SET NAMES or individual variable SETs will
        # change the charset without updating the driver's view of the world.
        #
        # If it's decided that issuing that sort of SQL leaves you SOL, then
        # this can prefer the driver value.
        rs = connection.execute("SHOW VARIABLES LIKE 'character_set%%'")
        opts = dict([(row[0], row[1]) for row in self._compat_fetchall(rs)])
        for key in ('character_set_connection', 'character_set'):
            if opts.get(key, None):
                return opts[key]

        util.warn("Could not detect the connection character set.  Assuming latin1.")
        return 'latin1'

    def _driver_kwargs(self):
        """return kw arg dict to be sent to connect()."""
        
        return {'CHARSET':self.encoding}
    
    def _extract_error_code(self, exception):
        # e.g.: DBAPIError: (Error) Table 'test.u2' doesn't exist [SQLCode: 1146], [SQLState: 42S02] 'DESCRIBE `u2`' ()
        
        m = re.compile(r"\[SQLCode\: (\d+)\]").search(str(exception.orig.args))
        c = m.group(1)
        if c:
            return int(c)
        else:
            return None

    def _get_server_version_info(self,connection):
        dbapi_con = connection.connection
        version = []
        r = re.compile('[.\-]')
        for n in r.split(dbapi_con.dbversion):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)

dialect = MySQL_jdbc