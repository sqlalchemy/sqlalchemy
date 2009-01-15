from sqlalchemy.dialects.mysql.base import MySQLDialect, MySQLExecutionContext
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.engine import base as engine_base
from sqlalchemy import util
import re

class MySQL_pyodbcExecutionContext(MySQLExecutionContext):
    def _lastrowid(self, cursor):
        cursor.execute("SELECT LAST_INSERT_ID()")
        return cursor.fetchone()[0]

class MySQL_pyodbc(PyODBCConnector, MySQLDialect):
    supports_unicode_statements = False
    execution_ctx_cls = MySQL_pyodbcExecutionContext
    
    def __init__(self, **kw):
        # deal with http://code.google.com/p/pyodbc/issues/detail?id=25
        kw.setdefault('convert_unicode', True)
        MySQLDialect.__init__(self, **kw)
        PyODBCConnector.__init__(self, **kw)

    @engine_base.connection_memoize(('mysql', 'charset'))
    def _detect_charset(self, connection):
        """Sniff out the character set in use for connection results."""

        # Allow user override, won't sniff if force_charset is set.
        if ('mysql', 'force_charset') in connection.info:
            return connection.info[('mysql', 'force_charset')]

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
    
    def _extract_error_code(self, exception):
        m = re.compile(r"\((\d+)\)").search(str(exception.orig.args))
        c = m.group(1)
        if c:
            return int(c)
        else:
            return None

dialect = MySQL_pyodbc