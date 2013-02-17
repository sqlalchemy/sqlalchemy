# mysql/cymysql.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

.. dialect:: mysql+cymysql
    :name: CyMySQL
    :dbapi: cymysql
    :connectstring: mysql+cymysql://<username>:<password>@<host>/<dbname>[?<options>]
    :url: https://github.com/nakagami/CyMySQL

"""

from .mysqldb import MySQLDialect_mysqldb


class MySQLDialect_cymysql(MySQLDialect_mysqldb):
    driver = 'cymysql'

    description_encoding = None

    @classmethod
    def dbapi(cls):
        return __import__('cymysql')

dialect = MySQLDialect_cymysql
