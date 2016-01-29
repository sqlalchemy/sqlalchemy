# mysql/pymysql.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

.. dialect:: mysql+pymysql
    :name: PyMySQL
    :dbapi: pymysql
    :connectstring: mysql+pymysql://<username>:<password>@<host>/<dbname>\
[?<options>]
    :url: http://www.pymysql.org/

Unicode
-------

Please see :ref:`mysql_unicode` for current recommendations on unicode
handling.

MySQL-Python Compatibility
--------------------------

The pymysql DBAPI is a pure Python port of the MySQL-python (MySQLdb) driver,
and targets 100% compatibility.   Most behavioral notes for MySQL-python apply
to the pymysql driver as well.

"""

from .mysqldb import MySQLDialect_mysqldb
from ...util import py3k


class MySQLDialect_pymysql(MySQLDialect_mysqldb):
    driver = 'pymysql'

    description_encoding = None

    # generally, these two values should be both True
    # or both False.   PyMySQL unicode tests pass all the way back
    # to 0.4 either way.  See [ticket:3337]
    supports_unicode_statements = True
    supports_unicode_binds = True

    @classmethod
    def dbapi(cls):
        return __import__('pymysql')

    if py3k:
        def _extract_error_code(self, exception):
            if isinstance(exception.args[0], Exception):
                exception = exception.args[0]
            return exception.args[0]

dialect = MySQLDialect_pymysql
