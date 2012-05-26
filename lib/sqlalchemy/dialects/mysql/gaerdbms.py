# mysql/gaerdbms.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Support for Google Cloud SQL on Google App Engine.

This dialect is based primarily on the :mod:`.mysql.mysqldb` dialect with minimal
changes.

.. versionadded:: 0.7.8

Connecting
----------

Connect string format::

    mysql+gaerdbms:///<dbname>

E.g.::

  create_engine('mysql+gaerdbms:///mydb',
                 connect_args={"instance":"instancename"})

Pooling
-------

Google App Engine connections appear to be randomly recycled,
so the dialect does not pool connections.  The :class:`.NullPool`
implementation is installed within the :class:`.Engine` by 
default.

"""

from sqlalchemy.dialects.mysql.mysqldb import MySQLDialect_mysqldb
from sqlalchemy.pool import NullPool
import re


class MySQLDialect_gaerdbms(MySQLDialect_mysqldb): 

    @classmethod 
    def dbapi(cls): 
        from google.appengine.api import rdbms
        return rdbms

    @classmethod
    def get_pool_class(cls, url):
        # Cloud SQL connections die at any moment
        return NullPool

    def create_connect_args(self, url):
        return [[],{'database':url.database}]

    def _extract_error_code(self, exception):
        match = re.compile(r"^(\d+):").match(str(exception))
        code = match.group(1)
        if code:
            return int(code)

dialect = MySQLDialect_gaerdbms