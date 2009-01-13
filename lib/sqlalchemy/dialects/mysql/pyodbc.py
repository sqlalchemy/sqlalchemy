from sqlalchemy.dialects.mysql.base import MySQLDialect, MySQLExecutionContext

class MySQL_pyodbcExecutionContext(MySQLExecutionContext):
    def _lastrowid(self, cursor):
        cursor.execute("SELECT LAST_INSERT_ID()")
        return cursor.fetchone()[0]

class MySQL_pyodbc(MySQLDialect):
    pass


dialect = MySQL_pyodbc