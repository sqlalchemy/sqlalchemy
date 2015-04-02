# mysql/mysqlconnector.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
.. dialect:: mysql+mysqlconnector
    :name: MySQL Connector/Python
    :dbapi: myconnpy
    :connectstring: mysql+mysqlconnector://<user>:<password>@\
<host>[:<port>]/<dbname>
    :url: http://dev.mysql.com/downloads/connector/python/


Unicode
-------

Please see :ref:`mysql_unicode` for current recommendations on unicode
handling.

"""

from .base import (MySQLDialect, MySQLExecutionContext,
                   MySQLCompiler, MySQLIdentifierPreparer,
                   BIT)

from ... import util
import re


class MySQLExecutionContext_mysqlconnector(MySQLExecutionContext):

    def get_lastrowid(self):
        return self.cursor.lastrowid


class MySQLCompiler_mysqlconnector(MySQLCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        if self.dialect._mysqlconnector_double_percents:
            return self.process(binary.left, **kw) + " %% " + \
                self.process(binary.right, **kw)
        else:
            return self.process(binary.left, **kw) + " % " + \
                self.process(binary.right, **kw)

    def post_process_text(self, text):
        if self.dialect._mysqlconnector_double_percents:
            return text.replace('%', '%%')
        else:
            return text

    def escape_literal_column(self, text):
        if self.dialect._mysqlconnector_double_percents:
            return text.replace('%', '%%')
        else:
            return text


class MySQLIdentifierPreparer_mysqlconnector(MySQLIdentifierPreparer):

    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        if self.dialect._mysqlconnector_double_percents:
            return value.replace("%", "%%")
        else:
            return value


class _myconnpyBIT(BIT):
    def result_processor(self, dialect, coltype):
        """MySQL-connector already converts mysql bits, so."""

        return None


class MySQLDialect_mysqlconnector(MySQLDialect):
    driver = 'mysqlconnector'

    supports_unicode_binds = True

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_native_decimal = True

    default_paramstyle = 'format'
    execution_ctx_cls = MySQLExecutionContext_mysqlconnector
    statement_compiler = MySQLCompiler_mysqlconnector

    preparer = MySQLIdentifierPreparer_mysqlconnector

    colspecs = util.update_copy(
        MySQLDialect.colspecs,
        {
            BIT: _myconnpyBIT,
        }
    )

    @util.memoized_property
    def supports_unicode_statements(self):
        return util.py3k or self._mysqlconnector_version_info > (2, 0)

    @classmethod
    def dbapi(cls):
        from mysql import connector
        return connector

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')

        opts.update(url.query)

        util.coerce_kw_type(opts, 'buffered', bool)
        util.coerce_kw_type(opts, 'raise_on_warnings', bool)

        # unfortunately, MySQL/connector python refuses to release a
        # cursor without reading fully, so non-buffered isn't an option
        opts.setdefault('buffered', True)

        # FOUND_ROWS must be set in ClientFlag to enable
        # supports_sane_rowcount.
        if self.dbapi is not None:
            try:
                from mysql.connector.constants import ClientFlag
                client_flags = opts.get(
                    'client_flags', ClientFlag.get_default())
                client_flags |= ClientFlag.FOUND_ROWS
                opts['client_flags'] = client_flags
            except Exception:
                pass
        return [[], opts]

    @util.memoized_property
    def _mysqlconnector_version_info(self):
        if self.dbapi and hasattr(self.dbapi, '__version__'):
            m = re.match(r'(\d+)\.(\d+)(?:\.(\d+))?',
                         self.dbapi.__version__)
            if m:
                return tuple(
                    int(x)
                    for x in m.group(1, 2, 3)
                    if x is not None)

    @util.memoized_property
    def _mysqlconnector_double_percents(self):
        return not util.py3k and self._mysqlconnector_version_info < (2, 0)

    def _get_server_version_info(self, connection):
        dbapi_con = connection.connection
        version = dbapi_con.get_server_version()
        return tuple(version)

    def _detect_charset(self, connection):
        return connection.connection.charset

    def _extract_error_code(self, exception):
        return exception.errno

    def is_disconnect(self, e, connection, cursor):
        errnos = (2006, 2013, 2014, 2045, 2055, 2048)
        exceptions = (self.dbapi.OperationalError, self.dbapi.InterfaceError)
        if isinstance(e, exceptions):
            return e.errno in errnos or \
                "MySQL Connection not available." in str(e)
        else:
            return False

    def _compat_fetchall(self, rp, charset=None):
        return rp.fetchall()

    def _compat_fetchone(self, rp, charset=None):
        return rp.fetchone()

dialect = MySQLDialect_mysqlconnector
