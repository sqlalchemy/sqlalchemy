# mysql/pyodbc.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

r"""


.. dialect:: mysql+pyodbc
    :name: PyODBC
    :dbapi: pyodbc
    :connectstring: mysql+pyodbc://<username>:<password>@<dsnname>
    :url: http://pypi.python.org/pypi/pyodbc/

    .. note:: The PyODBC for MySQL dialect is not well supported, and
       is subject to unresolved character encoding issues
       which exist within the current ODBC drivers available.
       (see http://code.google.com/p/pyodbc/issues/detail?id=25).
       Other dialects for MySQL are recommended.

Pass through exact pyodbc connection string::

    import urllib
    connection_string = (
        'DRIVER=MySQL ODBC 8.0 ANSI Driver;'
        'SERVER=localhost;'
        'PORT=3307;'
        'DATABASE=mydb;'
        'UID=root;'
        'PWD=(whatever);'
        'charset=utf8mb4;'
    )
    params = urllib.parse.quote_plus(connection_string)
    connection_uri = "mysql+pyodbc:///?odbc_connect=%s" % params

"""  # noqa

import re

from .base import MySQLDialect
from .base import MySQLExecutionContext
from .types import TIME
from ... import util
from ...connectors.pyodbc import PyODBCConnector
from ...sql.sqltypes import Time


class _pyodbcTIME(TIME):
    def result_processor(self, dialect, coltype):
        def process(value):
            # pyodbc returns a datetime.time object; no need to convert
            return value

        return process


class MySQLExecutionContext_pyodbc(MySQLExecutionContext):
    def get_lastrowid(self):
        cursor = self.create_cursor()
        cursor.execute("SELECT LAST_INSERT_ID()")
        lastrowid = cursor.fetchone()[0]
        cursor.close()
        return lastrowid


class MySQLDialect_pyodbc(PyODBCConnector, MySQLDialect):
    colspecs = util.update_copy(MySQLDialect.colspecs, {Time: _pyodbcTIME})
    supports_unicode_statements = False
    execution_ctx_cls = MySQLExecutionContext_pyodbc

    pyodbc_driver_name = "MySQL"

    def __init__(self, **kw):
        # deal with http://code.google.com/p/pyodbc/issues/detail?id=25
        kw.setdefault("convert_unicode", True)
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
        opts = {row[0]: row[1] for row in self._compat_fetchall(rs)}
        for key in ("character_set_connection", "character_set"):
            if opts.get(key, None):
                return opts[key]

        util.warn(
            "Could not detect the connection character set.  "
            "Assuming latin1."
        )
        return "latin1"

    def _extract_error_code(self, exception):
        m = re.compile(r"\((\d+)\)").search(str(exception.args))
        c = m.group(1)
        if c:
            return int(c)
        else:
            return None


dialect = MySQLDialect_pyodbc
