"""Support for the MySQL database via the MySQL Connector/Python adapter.

This dialect is in development pending further progress on this
new DBAPI.

current issue (2009-10-18):

fetchone() does not obey PEP 249

https://bugs.launchpad.net/myconnpy/+bug/454782

"""

import re

from sqlalchemy.dialects.mysql.base import MySQLDialect, MySQLExecutionContext,\
                                            MySQLCompiler, MySQLIdentifierPreparer
                                            
from sqlalchemy.engine import base as engine_base, default
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import exc, log, schema, sql, types as sqltypes, util

class MySQL_myconnpyExecutionContext(MySQLExecutionContext):
    # DBAPI BUG:
    # fetchone() improperly raises an exception when no rows remain
    
    
    def get_lastrowid(self):
        # DBAPI BUG: wrong name of attribute
        # https://bugs.launchpad.net/myconnpy/+bug/454782
        return self.cursor._lastrowid
        
        # this is the fallback approach.
#        cursor = self.create_cursor()
#        cursor.execute("SELECT LAST_INSERT_ID()")
#        lastrowid = cursor.fetchone()[0]
#        cursor.close()
#        return lastrowid
    
        
class MySQL_myconnpyCompiler(MySQLCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)
    
    def post_process_text(self, text):
        return text.replace('%', '%%')


class MySQL_myconnpyIdentifierPreparer(MySQLIdentifierPreparer):
    
    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace("%", "%%")

class MySQL_myconnpy(MySQLDialect):
    driver = 'myconnpy'
    supports_unicode_statements = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    default_paramstyle = 'format'
    execution_ctx_cls = MySQL_myconnpyExecutionContext
    statement_compiler = MySQL_myconnpyCompiler
    
    preparer = MySQL_myconnpyIdentifierPreparer
    
    def __init__(self, **kw):
        # DBAPI BUG:
        # named parameters don't work:
        # "Parameters must be given as a sequence."
        # https://bugs.launchpad.net/myconnpy/+bug/454782
        kw['paramstyle'] = 'format'
        MySQLDialect.__init__(self, **kw)
        
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
        version = []
        r = re.compile('[.\-]')
        for n in r.split(dbapi_con.get_server_version()):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
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

dialect = MySQL_myconnpy
