"""Support for the MySQL database via the pyodbc adapter.

pyodbc is available at:

    http://pypi.python.org/pypi/pyodbc/

Connecting
----------

Connect string::

    mysql+pyodbc://<username>:<password>@<dsnname>

Limitations
-----------

The mysql-pyodbc dialect is subject to unresolved character encoding issues 
which exist within the current ODBC drivers available.
(see http://code.google.com/p/pyodbc/issues/detail?id=25).   Consider usage
of OurSQL, MySQLdb, or MySQL-connector/Python.

"""

from sqlalchemy.dialects.mysql.base import MySQLDialect, MySQLExecutionContext
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.engine import base as engine_base
from sqlalchemy import util
import re

class MySQLExecutionContext_pyodbc(MySQLExecutionContext):

    def get_lastrowid(self):
        cursor = self.create_cursor()
        cursor.execute("SELECT LAST_INSERT_ID()")
        lastrowid = cursor.fetchone()[0]
        cursor.close()
        return lastrowid

class MySQLDialect_pyodbc(PyODBCConnector, MySQLDialect):
    supports_unicode_statements = False
    execution_ctx_cls = MySQLExecutionContext_pyodbc

    pyodbc_driver_name = "MySQL"
    
    def __init__(self, **kw):
        # deal with http://code.google.com/p/pyodbc/issues/detail?id=25
        kw.setdefault('convert_unicode', True)
        super(MySQLDialect_pyodbc, self).__init__(**kw)

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
    
    def _extract_error_code(self, exception):
        m = re.compile(r"\((\d+)\)").search(str(exception.orig.args))
        c = m.group(1)
        if c:
            return int(c)
        else:
            return None

dialect = MySQLDialect_pyodbc
