# mysql/mysqldb.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
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

Character Sets
--------------

Many MySQL server installations default to a ``latin1`` encoding for client
connections.  All data sent through the connection will be converted into
``latin1``, even if you have ``utf8`` or another character set on your tables
and columns.  With versions 4.1 and higher, you can change the connection
character set either through server configuration or by including the
``charset`` parameter in the URL used for ``create_engine``.  The ``charset``
option is passed through to MySQL-Python and has the side-effect of also
enabling ``use_unicode`` in the driver by default.  For regular encoded
strings, also pass ``use_unicode=0`` in the connection arguments::

  # set client encoding to utf8; all strings come back as unicode
  create_engine('mysql+mysqldb:///mydb?charset=utf8')

  # set client encoding to utf8; all strings come back as utf8 str
  create_engine('mysql+mysqldb:///mydb?charset=utf8&use_unicode=0')

Known Issues
-------------

MySQL-python version 1.2.2 has a serious memory leak related
to unicode conversion, a feature which is disabled via ``use_unicode=0``.
Using a more recent version of MySQL-python is recommended. The 
recommended connection form with SQLAlchemy is::
  
    engine = create_engine('mysql://scott:tiger@localhost/test?charset=utf8&use_unicode=0', pool_recycle=3600)


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
