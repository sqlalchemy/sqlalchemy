"""Support for the MySQL database via the MySQL Connector/Python adapter.

# TODO: add docs/notes here regarding MySQL Connector/Python

"""

import re

from sqlalchemy.dialects.mysql.base import MySQLDialect, MySQLExecutionContext,\
                                            MySQLCompiler, MySQLIdentifierPreparer
                                            
from sqlalchemy.engine import base as engine_base, default
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import exc, log, schema, sql, types as sqltypes, util

class MySQL_mysqlconnectorExecutionContext(MySQLExecutionContext):
    
    def get_lastrowid(self):
        return self.cursor.lastrowid
    
        
class MySQL_mysqlconnectorCompiler(MySQLCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)
    
    def post_process_text(self, text):
        return text.replace('%', '%%')


class MySQL_mysqlconnectorIdentifierPreparer(MySQLIdentifierPreparer):
    
    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace("%", "%%")

class MySQL_mysqlconnector(MySQLDialect):
    driver = 'mysqlconnector'
    supports_unicode_statements = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    default_paramstyle = 'format'
    execution_ctx_cls = MySQL_mysqlconnectorExecutionContext
    statement_compiler = MySQL_mysqlconnectorCompiler
    
    preparer = MySQL_mysqlconnectorIdentifierPreparer
    
    @classmethod
    def dbapi(cls):
        from mysql import connector
        return connector

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        return [[], opts]

    def _get_server_version_info(self, connection):
        dbapi_con = connection.connection
        version = dbapi_con.get_server_version()
        return tuple(version)

    def _detect_charset(self, connection):
        """Sniff out the character set in use for connection results."""
        
        return connection.connection.get_characterset_info()

    def _extract_error_code(self, exception):
        m = re.compile(r"\(.*\)\s+(\d+)").search(str(exception))
        c = m.group(1)
        if c:
            return int(c)
        else:
            return None

dialect = MySQL_mysqlconnector
