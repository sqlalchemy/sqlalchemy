# connectors/mysqldb.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Define behaviors common to MySQLdb dialects.

Currently includes MySQL and Drizzle.

"""

from . import Connector
from ..engine import base as engine_base, default
from ..sql import operators as sql_operators
from .. import exc, log, schema, sql, types as sqltypes, util, processors
import re


# the subclassing of Connector by all classes
# here is not strictly necessary


class MySQLDBExecutionContext(Connector):

    @property
    def rowcount(self):
        if hasattr(self, '_rowcount'):
            return self._rowcount
        else:
            return self.cursor.rowcount


class MySQLDBCompiler(Connector):
    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + " %% " + \
                    self.process(binary.right, **kw)

    def post_process_text(self, text):
        return text.replace('%', '%%')


class MySQLDBIdentifierPreparer(Connector):

    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace("%", "%%")


class MySQLDBConnector(Connector):
    driver = 'mysqldb'
    supports_unicode_statements = False
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_native_decimal = True

    default_paramstyle = 'format'

    @classmethod
    def dbapi(cls):
        # is overridden when pymysql is used
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
        util.coerce_kw_type(opts, 'read_timeout', int)
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

