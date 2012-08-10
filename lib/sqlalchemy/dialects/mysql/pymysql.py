# mysql/pymysql.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the MySQL database via the pymysql adapter.

pymysql is available at:

    http://code.google.com/p/pymysql/

Connecting
----------

Connect string::

    mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]

MySQL-Python Compatibility
--------------------------

The pymysql DBAPI is a pure Python port of the MySQL-python (MySQLdb) driver,
and targets 100% compatibility.   Most behavioral notes for MySQL-python apply to
the pymysql driver as well.

"""

from sqlalchemy.dialects.mysql.mysqldb import MySQLDialect_mysqldb

class MySQLDialect_pymysql(MySQLDialect_mysqldb):
    driver = 'pymysql'

    description_encoding = None
    @classmethod
    def dbapi(cls):
        return __import__('pymysql')

dialect = MySQLDialect_pymysql