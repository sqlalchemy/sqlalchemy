from sqlalchemy.dialects.mysql.base import MySQLDialect, MySQLExecutionContext
from sqlalchemy.connectors.pyodbc import PyODBCConnector
import re

class MySQL_pyodbcExecutionContext(MySQLExecutionContext):
    def _lastrowid(self, cursor):
        cursor.execute("SELECT LAST_INSERT_ID()")
        return cursor.fetchone()[0]

class MySQL_pyodbc(PyODBCConnector, MySQLDialect):
    supports_unicode_statements = False
    execution_ctx_cls = MySQL_pyodbcExecutionContext
    
    def __init__(self, **kw):
        MySQLDialect.__init__(self, **kw)
        PyODBCConnector.__init__(self, **kw)
    
    def _extract_error_code(self, exception):
        m = re.compile(r"\((\d+)\)").search(str(exception.orig.args))
        c = m.group(1)
        if c:
            return int(c)
        else:
            return None

dialect = MySQL_pyodbc