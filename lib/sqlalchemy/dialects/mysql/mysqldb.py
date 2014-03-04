# mysql/mysqldb.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

.. dialect:: mysql+mysqldb
    :name: MySQL-Python
    :dbapi: mysqldb
    :connectstring: mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>
    :url: http://sourceforge.net/projects/mysql-python


Unicode
-------

MySQLdb requires a "charset" parameter to be passed in order for it
to handle non-ASCII characters correctly.   When this parameter is passed,
MySQLdb will also implicitly set the "use_unicode" flag to true, which means
that it will return Python unicode objects instead of bytestrings.
However, SQLAlchemy's decode process, when C extensions are enabled,
is orders of magnitude faster than that of MySQLdb as it does not call into
Python functions to do so.  Therefore, the **recommended URL to use for
unicode** will include both charset and use_unicode=0::

    create_engine("mysql+mysqldb://user:pass@host/dbname?charset=utf8&use_unicode=0")

As of this writing, MySQLdb only runs on Python 2.   It is not known how
MySQLdb behaves on Python 3 as far as unicode decoding.


Known Issues
-------------

MySQL-python version 1.2.2 has a serious memory leak related
to unicode conversion, a feature which is disabled via ``use_unicode=0``.
It is strongly advised to use the latest version of MySQL-Python.

"""

from .base import (MySQLDialect, MySQLExecutionContext,
                                            MySQLCompiler, MySQLIdentifierPreparer)
from ...connectors.mysqldb import (
                        MySQLDBExecutionContext,
                        MySQLDBCompiler,
                        MySQLDBIdentifierPreparer,
                        MySQLDBConnector
                    )


class MySQLExecutionContext_mysqldb(MySQLDBExecutionContext, MySQLExecutionContext):
    pass


class MySQLCompiler_mysqldb(MySQLDBCompiler, MySQLCompiler):
    pass


class MySQLIdentifierPreparer_mysqldb(MySQLDBIdentifierPreparer, MySQLIdentifierPreparer):
    pass


class MySQLDialect_mysqldb(MySQLDBConnector, MySQLDialect):
    execution_ctx_cls = MySQLExecutionContext_mysqldb
    statement_compiler = MySQLCompiler_mysqldb
    preparer = MySQLIdentifierPreparer_mysqldb

dialect = MySQLDialect_mysqldb
