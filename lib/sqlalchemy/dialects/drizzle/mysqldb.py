"""Support for the Drizzle database via the Drizzle-python adapter.

Drizzle-Python is available at:

    http://sourceforge.net/projects/mysql-python

At least version 1.2.1 or 1.2.2 should be used.

Connecting
-----------

Connect string format::

    drizzle+mysqldb://<user>:<password>@<host>[:<port>]/<dbname>

Unicode
-------

Drizzle accommodates Python ``unicode`` objects directly and 
uses the ``utf8`` encoding in all cases.

Known Issues
-------------

Drizzle-python at least as of version 1.2.2 has a serious memory leak related
to unicode conversion, a feature which is disabled via ``use_unicode=0``.
The recommended connection form with SQLAlchemy is::

    engine = create_engine('mysql://scott:tiger@localhost/test?charset=utf8&use_unicode=0', pool_recycle=3600)


"""

from sqlalchemy.dialects.drizzle.base import (DrizzleDialect, 
                                DrizzleExecutionContext,
                                DrizzleCompiler, DrizzleIdentifierPreparer)
from sqlalchemy.connectors.mysqldb import (
                        MySQLDBExecutionContext, 
                        MySQLDBCompiler, 
                        MySQLDBIdentifierPreparer, 
                        MySQLDBConnector
                    )

class DrizzleExecutionContext_mysqldb(
                        MySQLDBExecutionContext, 
                        DrizzleExecutionContext):
    pass


class DrizzleCompiler_mysqldb(MySQLDBCompiler, DrizzleCompiler):
    pass


class DrizzleIdentifierPreparer_mysqldb(
                        MySQLDBIdentifierPreparer, 
                        DrizzleIdentifierPreparer):
    pass

class DrizzleDialect_mysqldb(MySQLDBConnector, DrizzleDialect):
    execution_ctx_cls = DrizzleExecutionContext_mysqldb
    statement_compiler = DrizzleCompiler_mysqldb
    preparer = DrizzleIdentifierPreparer_mysqldb

    def _detect_charset(self, connection):
        """Sniff out the character set in use for connection results."""
        return 'utf8'


dialect = DrizzleDialect_mysqldb
