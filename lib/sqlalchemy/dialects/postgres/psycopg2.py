"""Support for the PostgreSQL database via the psycopg2 driver.

Driver
------

The psycopg2 driver is supported, available at http://pypi.python.org/pypi/psycopg2/ .
The dialect has several behaviors  which are specifically tailored towards compatibility 
with this module.

Note that psycopg1 is **not** supported.

Connecting
----------

URLs are of the form `postgres+psycopg2://user@password@host:port/dbname[?key=value&key=value...]`.

psycopg2-specific keyword arguments which are accepted by :func:`~sqlalchemy.create_engine()` are:

* *server_side_cursors* - Enable the usage of "server side cursors" for SQL statements which support
  this feature.  What this essentially means from a psycopg2 point of view is that the cursor is 
  created using a name, e.g. `connection.cursor('some name')`, which has the effect that result rows
  are not immediately pre-fetched and buffered after statement execution, but are instead left 
  on the server and only retrieved as needed.    SQLAlchemy's :class:`~sqlalchemy.engine.base.ResultProxy`
  uses special row-buffering behavior when this feature is enabled, such that groups of 100 rows 
  at a time are fetched over the wire to reduce conversational overhead.

Transactions
------------

The psycopg2 dialect fully supports SAVEPOINT and two-phase commit operations.


"""

import decimal, random, re
from sqlalchemy import util
from sqlalchemy.engine import base, default
from sqlalchemy.sql import expression
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgres.base import PGDialect, PGCompiler

class PGNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect):
        if self.asdecimal:
            return None
        else:
            def process(value):
                if isinstance(value, decimal.Decimal):
                    return float(value)
                else:
                    return value
            return process


# TODO: filter out 'FOR UPDATE' statements
SERVER_SIDE_CURSOR_RE = re.compile(
    r'\s*SELECT',
    re.I | re.UNICODE)

class Postgres_psycopg2ExecutionContext(default.DefaultExecutionContext):
    def create_cursor(self):
        # TODO: coverage for server side cursors + select.for_update()
        is_server_side = \
            self.dialect.server_side_cursors and \
            ((self.compiled and isinstance(self.compiled.statement, expression.Selectable) 
                and not getattr(self.compiled.statement, 'for_update', False)) \
            or \
            (
                (not self.compiled or isinstance(self.compiled.statement, expression._TextClause)) 
                and self.statement and SERVER_SIDE_CURSOR_RE.match(self.statement))
            )

        self.__is_server_side = is_server_side
        if is_server_side:
            # use server-side cursors:
            # http://lists.initd.org/pipermail/psycopg/2007-January/005251.html
            ident = "c_%s_%s" % (hex(id(self))[2:], hex(random.randint(0, 65535))[2:])
            return self._connection.connection.cursor(ident)
        else:
            return self._connection.connection.cursor()

    def get_result_proxy(self):
        if self.__is_server_side:
            return base.BufferedRowResultProxy(self)
        else:
            return base.ResultProxy(self)

class Postgres_psycopg2Compiler(PGCompiler):
    operators = util.update_copy(
        PGCompiler.operators, 
        {
            sql_operators.mod : '%%',
        }
    )
    
    def post_process_text(self, text):
        return text.replace('%', '%%')

class Postgres_psycopg2(PGDialect):
    driver = 'psycopg2'
    supports_unicode_statements = False
    default_paramstyle = 'pyformat'
    supports_sane_multi_rowcount = False
    execution_ctx_cls = Postgres_psycopg2ExecutionContext
    statement_compiler = Postgres_psycopg2Compiler

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric : PGNumeric,
            sqltypes.Float: sqltypes.Float,  # prevents PGNumeric from being used
        }
    )

    
    def __init__(self, server_side_cursors=False, **kwargs):
        PGDialect.__init__(self, **kwargs)
        self.server_side_cursors = server_side_cursors

    @classmethod
    def dbapi(cls):
        psycopg = __import__('psycopg2')
        return psycopg

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        return ([], opts)

    def is_disconnect(self, e):
        if isinstance(e, self.dbapi.OperationalError):
            return 'closed the connection' in str(e) or 'connection not open' in str(e)
        elif isinstance(e, self.dbapi.InterfaceError):
            return 'connection already closed' in str(e) or 'cursor already closed' in str(e)
        elif isinstance(e, self.dbapi.ProgrammingError):
            # yes, it really says "losed", not "closed"
            return "losed the connection unexpectedly" in str(e)
        else:
            return False

dialect = Postgres_psycopg2
    