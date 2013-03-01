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
from .base import (BIT, MySQLDialect)
from ... import util

class _cymysqlBIT(BIT):
    def result_processor(self, dialect, coltype):
        """Convert a MySQL's 64 bit, variable length binary string to a long.
        """

        def process(value):
            if value is not None:
                # Py2K
                value = 0L
                for i in map(ord, value):
                    value = value << 8 | i
                # end Py2K
                # Py3K
                #value = 0
                #for i in value:
                #    value = value << 8 | i 
            return value
        return process


class MySQLDialect_cymysql(MySQLDialect_mysqldb):
    driver = 'cymysql'

    description_encoding = None

    colspecs = util.update_copy(
        MySQLDialect.colspecs,
        {
            BIT: _cymysqlBIT,
        }
    )

    @classmethod
    def dbapi(cls):
        return __import__('cymysql')

    def _extract_error_code(self, exception):
        # Py2K
        return exception[0]
        # end Py2K
        # Py3K
        #return exception.args[0].args[0]

dialect = MySQLDialect_cymysql
