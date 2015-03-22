# mysql/mysqldb.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

.. dialect:: mysql+mysqldb
    :name: MySQL-Python
    :dbapi: mysqldb
    :connectstring: mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>
    :url: http://sourceforge.net/projects/mysql-python

.. _mysqldb_unicode:

Unicode
-------

Please see :ref:`mysql_unicode` for current recommendations on unicode
handling.

Py3K Support
------------

Currently, MySQLdb only runs on Python 2 and development has been stopped.
`mysqlclient`_ is fork of MySQLdb and provides Python 3 support as well
as some bugfixes.

.. _mysqlclient: https://github.com/PyMySQL/mysqlclient-python

Using MySQLdb with Google Cloud SQL
-----------------------------------

Google Cloud SQL now recommends use of the MySQLdb dialect.  Connect
using a URL like the following::

    mysql+mysqldb://root@/<dbname>?unix_socket=/cloudsql/<projectid>:<instancename>

"""

from .base import (MySQLDialect, MySQLExecutionContext,
                   MySQLCompiler, MySQLIdentifierPreparer)
from .base import TEXT
from ... import sql
from ... import util
import re


class MySQLExecutionContext_mysqldb(MySQLExecutionContext):

    @property
    def rowcount(self):
        if hasattr(self, '_rowcount'):
            return self._rowcount
        else:
            return self.cursor.rowcount


class MySQLCompiler_mysqldb(MySQLCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + " %% " + \
            self.process(binary.right, **kw)

    def post_process_text(self, text):
        return text.replace('%', '%%')


class MySQLIdentifierPreparer_mysqldb(MySQLIdentifierPreparer):

    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace("%", "%%")


class MySQLDialect_mysqldb(MySQLDialect):
    driver = 'mysqldb'
    supports_unicode_statements = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_native_decimal = True

    default_paramstyle = 'format'
    execution_ctx_cls = MySQLExecutionContext_mysqldb
    statement_compiler = MySQLCompiler_mysqldb
    preparer = MySQLIdentifierPreparer_mysqldb

    @classmethod
    def dbapi(cls):
        return __import__('MySQLdb')

    def do_executemany(self, cursor, statement, parameters, context=None):
        rowcount = cursor.executemany(statement, parameters)
        if context is not None:
            context._rowcount = rowcount

    def _check_unicode_returns(self, connection):
        # work around issue fixed in
        # https://github.com/farcepest/MySQLdb1/commit/cd44524fef63bd3fcb71947392326e9742d520e8
        # specific issue w/ the utf8_bin collation and unicode returns

        has_utf8_bin = self.server_version_info > (5, ) and \
            connection.scalar(
                "show collation where %s = 'utf8' and %s = 'utf8_bin'"
                % (
                    self.identifier_preparer.quote("Charset"),
                    self.identifier_preparer.quote("Collation")
                ))
        if has_utf8_bin:
            additional_tests = [
                sql.collate(sql.cast(
                    sql.literal_column(
                            "'test collated returns'"),
                    TEXT(charset='utf8')), "utf8_bin")
            ]
        else:
            additional_tests = []
        return super(MySQLDialect_mysqldb, self)._check_unicode_returns(
            connection, additional_tests)

    def create_connect_args(self, url):
        opts = url.translate_connect_args(database='db', username='user',
                                          password='passwd')
        opts.update(url.query)

        util.coerce_kw_type(opts, 'compress', bool)
        util.coerce_kw_type(opts, 'connect_timeout', int)
        util.coerce_kw_type(opts, 'read_timeout', int)
        util.coerce_kw_type(opts, 'client_flag', int)
        util.coerce_kw_type(opts, 'local_infile', int)
        # Note: using either of the below will cause all strings to be
        # returned as Unicode, both in raw SQL operations and with column
        # types like String and MSString.
        util.coerce_kw_type(opts, 'use_unicode', bool)
        util.coerce_kw_type(opts, 'charset', str)

        # Rich values 'cursorclass' and 'conv' are not supported via
        # query string.

        ssl = {}
        keys = ['ssl_ca', 'ssl_key', 'ssl_cert', 'ssl_capath', 'ssl_cipher']
        for key in keys:
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
                CLIENT_FLAGS = __import__(
                    self.dbapi.__name__ + '.constants.CLIENT'
                ).constants.CLIENT
                client_flag |= CLIENT_FLAGS.FOUND_ROWS
            except (AttributeError, ImportError):
                self.supports_sane_rowcount = False
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

        try:
            # note: the SQL here would be
            # "SHOW VARIABLES LIKE 'character_set%%'"
            cset_name = connection.connection.character_set_name
        except AttributeError:
            util.warn(
                "No 'character_set_name' can be detected with "
                "this MySQL-Python version; "
                "please upgrade to a recent version of MySQL-Python.  "
                "Assuming latin1.")
            return 'latin1'
        else:
            return cset_name()


dialect = MySQLDialect_mysqldb
