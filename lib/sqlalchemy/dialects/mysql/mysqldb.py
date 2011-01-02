# mysql/mysqldb.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the MySQL database via the MySQL-python adapter.

MySQL-Python is available at:

    http://sourceforge.net/projects/mysql-python

At least version 1.2.1 or 1.2.2 should be used.

Connecting
-----------

Connect string format::

    mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>

Character Sets
--------------

Many MySQL server installations default to a ``latin1`` encoding for client
connections.  All data sent through the connection will be converted into
``latin1``, even if you have ``utf8`` or another character set on your tables
and columns.  With versions 4.1 and higher, you can change the connection
character set either through server configuration or by including the
``charset`` parameter in the URL used for ``create_engine``.  The ``charset``
option is passed through to MySQL-Python and has the side-effect of also
enabling ``use_unicode`` in the driver by default.  For regular encoded
strings, also pass ``use_unicode=0`` in the connection arguments::

  # set client encoding to utf8; all strings come back as unicode
  create_engine('mysql+mysqldb:///mydb?charset=utf8')

  # set client encoding to utf8; all strings come back as utf8 str
  create_engine('mysql+mysqldb:///mydb?charset=utf8&use_unicode=0')

Known Issues
-------------

MySQL-python at least as of version 1.2.2 has a serious memory leak related
to unicode conversion, a feature which is disabled via ``use_unicode=0``.
The recommended connection form with SQLAlchemy is::

    engine = create_engine('mysql://scott:tiger@localhost/test?charset=utf8&use_unicode=0', pool_recycle=3600)


"""

import re

from sqlalchemy.dialects.mysql.base import (MySQLDialect, MySQLExecutionContext,
                                            MySQLCompiler, MySQLIdentifierPreparer)
from sqlalchemy.engine import base as engine_base, default
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import exc, log, schema, sql, types as sqltypes, util
from sqlalchemy import processors

class MySQLExecutionContext_mysqldb(MySQLExecutionContext):

    @property
    def rowcount(self):
        if hasattr(self, '_rowcount'):
            return self._rowcount
        else:
            return self.cursor.rowcount


class MySQLCompiler_mysqldb(MySQLCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)

    def post_process_text(self, text):
        return text.replace('%', '%%')


class MySQLIdentifierPreparer_mysqldb(MySQLIdentifierPreparer):

    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace("%", "%%")

class MySQLDialect_mysqldb(MySQLDialect):
    driver = 'mysqldb'
    supports_unicode_statements = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_native_decimal = True

    default_paramstyle = 'format'
    execution_ctx_cls = MySQLExecutionContext_mysqldb
    statement_compiler = MySQLCompiler_mysqldb
    preparer = MySQLIdentifierPreparer_mysqldb

    colspecs = util.update_copy(
        MySQLDialect.colspecs,
        {
        }
    )

    @classmethod
    def dbapi(cls):
        return __import__('MySQLdb')

    def do_executemany(self, cursor, statement, parameters, context=None):
        rowcount = cursor.executemany(statement, parameters)
        if context is not None:
            context._rowcount = rowcount

    def create_connect_args(self, url):
        opts = url.translate_connect_args(database='db', username='user',
                                          password='passwd')
        opts.update(url.query)

        util.coerce_kw_type(opts, 'compress', bool)
        util.coerce_kw_type(opts, 'connect_timeout', int)
        util.coerce_kw_type(opts, 'client_flag', int)
        util.coerce_kw_type(opts, 'local_infile', int)
        # Note: using either of the below will cause all strings to be returned
        # as Unicode, both in raw SQL operations and with column types like
        # String and MSString.
        util.coerce_kw_type(opts, 'use_unicode', bool)
        util.coerce_kw_type(opts, 'charset', str)

        # Rich values 'cursorclass' and 'conv' are not supported via
        # query string.

        ssl = {}
        for key in ['ssl_ca', 'ssl_key', 'ssl_cert', 'ssl_capath', 'ssl_cipher']:
            if key in opts:
                ssl[key[4:]] = opts[key]
                util.coerce_kw_type(ssl, key[4:], str)
                del opts[key]
        if ssl:
            opts['ssl'] = ssl

        # FOUND_ROWS must be set in CLIENT_FLAGS to enable
        # supports_sane_rowcount.
        client_flag = opts.get('client_flag', 0)
        if self.dbapi is not None:
            try:
                from MySQLdb.constants import CLIENT as CLIENT_FLAGS
                client_flag |= CLIENT_FLAGS.FOUND_ROWS
            except:
                pass
            opts['client_flag'] = client_flag
        return [[], opts]

    def _get_server_version_info(self, connection):
        dbapi_con = connection.connection
        version = []
        r = re.compile('[.\-]')
        for n in r.split(dbapi_con.get_server_info()):
            try:
                version.append(int(n))
            except ValueError:
                version.append(n)
        return tuple(version)

    def _extract_error_code(self, exception):
        return exception.args[0]

    def _detect_charset(self, connection):
        """Sniff out the character set in use for connection results."""

        # Note: MySQL-python 1.2.1c7 seems to ignore changes made
        # on a connection via set_character_set()
        if self.server_version_info < (4, 1, 0):
            try:
                return connection.connection.character_set_name()
            except AttributeError:
                # < 1.2.1 final MySQL-python drivers have no charset support.
                # a query is needed.
                pass

        # Prefer 'character_set_results' for the current connection over the
        # value in the driver.  SET NAMES or individual variable SETs will
        # change the charset without updating the driver's view of the world.
        #
        # If it's decided that issuing that sort of SQL leaves you SOL, then
        # this can prefer the driver value.
        rs = connection.execute("SHOW VARIABLES LIKE 'character_set%%'")
        opts = dict([(row[0], row[1]) for row in self._compat_fetchall(rs)])

        if 'character_set_results' in opts:
            return opts['character_set_results']
        try:
            return connection.connection.character_set_name()
        except AttributeError:
            # Still no charset on < 1.2.1 final...
            if 'character_set' in opts:
                return opts['character_set']
            else:
                util.warn(
                    "Could not detect the connection character set with this "
                    "combination of MySQL server and MySQL-python. "
                    "MySQL-python >= 1.2.2 is recommended.  Assuming latin1.")
                return 'latin1'


dialect = MySQLDialect_mysqldb
