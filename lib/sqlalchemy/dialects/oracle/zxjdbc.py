"""Support for the Oracle database via the zxjdbc JDBC connector."""
import decimal
import re

try:
    from com.ziclix.python.sql.handler import OracleDataHandler
except ImportError:
    OracleDataHandler = None

from sqlalchemy import types as sqltypes, util
from sqlalchemy.connectors.zxJDBC import ZxJDBCConnector
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.engine.default import DefaultExecutionContext

class _JDBCDate(sqltypes.Date):

    def result_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            else:
                return value.date()
        return process


class _JDBCNumeric(sqltypes.Numeric):

    def result_processor(self, dialect):
        if self.asdecimal:
            return None
        else:
            def process(value):
                if isinstance(value, decimal.Decimal):
                    return float(value)
                else:
                    return value
            return process


class Oracle_jdbcExecutionContext(DefaultExecutionContext):

    def create_cursor(self):
        cursor = self._connection.connection.cursor()
        cursor.cursor.datahandler = OracleDataHandler(cursor.cursor.datahandler)
        return cursor


class Oracle_jdbc(ZxJDBCConnector, OracleDialect):
    execution_ctx_cls = Oracle_jdbcExecutionContext
    jdbc_db_name = 'oracle'
    jdbc_driver_name = 'oracle.jdbc.driver.OracleDriver'

    implicit_returning = False

    colspecs = util.update_copy(
        OracleDialect.colspecs,
        {
            sqltypes.Date : _JDBCDate,
            sqltypes.Numeric: _JDBCNumeric
        }
    )

    def initialize(self, connection):
        super(Oracle_jdbc, self).initialize(connection)
        self.implicit_returning = False

    def _create_jdbc_url(self, url):
        return 'jdbc:oracle:thin:@%s:%s:%s' % (url.host, url.port or 1521, url.database)

    def _get_server_version_info(self, connection):
        version = re.search(r'Release ([\d\.]+)', connection.connection.dbversion).group(1)
        return tuple(int(x) for x in version.split('.'))

dialect = Oracle_jdbc
