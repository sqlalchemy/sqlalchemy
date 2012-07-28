# mysql/mysqldb.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the MySQL database via the MySQL-python adapter.

MySQL-Python is available at:

    http://sourceforge.net/projects/mysql-python

At least version 1.2.1 or 1.2.2 should be used.

Connecting
-----------

Connect string format::

    mysql+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>

Unicode
-------

MySQLdb will accommodate Python ``unicode`` objects if the
``use_unicode=1`` parameter, or the ``charset`` parameter,
is passed as a connection argument.

Without this setting, many MySQL server installations default to
a ``latin1`` encoding for client connections, which has the effect
of all data being converted into ``latin1``, even if you have ``utf8``
or another character set configured on your tables
and columns.  With versions 4.1 and higher, you can change the connection
character set either through server configuration or by including the
``charset`` parameter.  The ``charset``
parameter as received by MySQL-Python also has the side-effect of
enabling ``use_unicode=1``::

    # set client encoding to utf8; all strings come back as unicode
    create_engine('mysql+mysqldb:///mydb?charset=utf8')

Manually configuring ``use_unicode=0`` will cause MySQL-python to
return encoded strings::

    # set client encoding to utf8; all strings come back as utf8 str
    create_engine('mysql+mysqldb:///mydb?charset=utf8&use_unicode=0')

Known Issues
-------------

MySQL-python version 1.2.2 has a serious memory leak related
to unicode conversion, a feature which is disabled via ``use_unicode=0``.
It is strongly advised to use the latest version of MySQL-Python.

"""

from sqlalchemy.dialects.mysql.base import (MySQLDialect, MySQLExecutionContext,
                                            MySQLCompiler, MySQLIdentifierPreparer)
from sqlalchemy.connectors.mysqldb import (
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
