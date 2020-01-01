# mysql/pymysql.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

r"""

.. dialect:: mysql+pymysql
    :name: PyMySQL
    :dbapi: pymysql
    :connectstring: mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
    :url: https://pymysql.readthedocs.io/

Unicode
-------

Please see :ref:`mysql_unicode` for current recommendations on unicode
handling.

MySQL-Python Compatibility
--------------------------

The pymysql DBAPI is a pure Python port of the MySQL-python (MySQLdb) driver,
and targets 100% compatibility.   Most behavioral notes for MySQL-python apply
to the pymysql driver as well.

"""  # noqa

from .mysqldb import MySQLDialect_mysqldb
from ...util import langhelpers
from ...util import py3k


class MySQLDialect_pymysql(MySQLDialect_mysqldb):
    driver = "pymysql"

    description_encoding = None

    # generally, these two values should be both True
    # or both False.   PyMySQL unicode tests pass all the way back
    # to 0.4 either way.  See [ticket:3337]
    supports_unicode_statements = True
    supports_unicode_binds = True

    def __init__(self, server_side_cursors=False, **kwargs):
        super(MySQLDialect_pymysql, self).__init__(**kwargs)
        self.server_side_cursors = server_side_cursors

    @langhelpers.memoized_property
    def supports_server_side_cursors(self):
        try:
            cursors = __import__("pymysql.cursors").cursors
            self._sscursor = cursors.SSCursor
            return True
        except (ImportError, AttributeError):
            return False

    @classmethod
    def dbapi(cls):
        return __import__("pymysql")

    def is_disconnect(self, e, connection, cursor):
        if super(MySQLDialect_pymysql, self).is_disconnect(
            e, connection, cursor
        ):
            return True
        elif isinstance(e, self.dbapi.Error):
            str_e = str(e).lower()
            return (
                "already closed" in str_e or "connection was killed" in str_e
            )
        else:
            return False

    if py3k:

        def _extract_error_code(self, exception):
            if isinstance(exception.args[0], Exception):
                exception = exception.args[0]
            return exception.args[0]


dialect = MySQLDialect_pymysql
