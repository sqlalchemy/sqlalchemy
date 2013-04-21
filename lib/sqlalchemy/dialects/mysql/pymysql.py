# mysql/pymysql.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

.. dialect:: mysql+pymysql
    :name: PyMySQL
    :dbapi: pymysql
    :connectstring: mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
    :url: http://code.google.com/p/pymysql/

MySQL-Python Compatibility
--------------------------

The pymysql DBAPI is a pure Python port of the MySQL-python (MySQLdb) driver,
and targets 100% compatibility.   Most behavioral notes for MySQL-python apply to
the pymysql driver as well.

"""

from .mysqldb import MySQLDialect_mysqldb


class MySQLDialect_pymysql(MySQLDialect_mysqldb):
    driver = 'pymysql'

    description_encoding = None
    # Py3K
    #supports_unicode_statements = True
    # Py2K
    # end Py2K

    @classmethod
    def dbapi(cls):
        return __import__('pymysql')

    # Py3K
    #def _extract_error_code(self, exception):
    #    if isinstance(exception.args[0], Exception):
    #        exception = exception.args[0]
    #    return exception.args[0]
    # Py2K
    # end Py2K

dialect = MySQLDialect_pymysql
