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

Sequences/SERIAL
----------------

Postgres supports sequences, and SQLAlchemy uses these as the default means of creating
new primary key values for integer-based primary key columns.   When creating tables, 
SQLAlchemy will issue the ``SERIAL`` datatype for integer-based primary key columns, 
which generates a sequence corresponding to the column and associated with it based on
a naming convention.

To specify a specific named sequence to be used for primary key generation, use the
:func:`~sqlalchemy.schema.Sequence` construct::

    Table('sometable', metadata, 
            Column('id', Integer, Sequence('some_id_seq'), primary_key=True)
        )

Currently, when SQLAlchemy issues a single insert statement, to fulfill the contract of
having the "last insert identifier" available, the sequence is executed independently
beforehand and the new value is retrieved, to be used in the subsequent insert.  Note
that when an :func:`~sqlalchemy.sql.expression.insert()` construct is executed using 
"executemany" semantics, the sequence is not pre-executed and normal PG SERIAL behavior
is used.

Postgres 8.3 supports an ``INSERT...RETURNING`` syntax which SQLAlchemy supports 
as well.  A future release of SQLA will use this feature by default in lieu of 
sequence pre-execution in order to retrieve new primary key values, when available.

INSERT/UPDATE...RETURNING
-------------------------

The dialect supports PG 8.3's ``INSERT..RETURNING`` and ``UPDATE..RETURNING`` syntaxes, 
but must be explicitly enabled on a per-statement basis::

    # INSERT..RETURNING
    result = table.insert(postgres_returning=[table.c.col1, table.c.col2]).\\
        values(name='foo')
    print result.fetchall()
    
    # UPDATE..RETURNING
    result = table.update(postgres_returning=[table.c.col1, table.c.col2]).\\
        where(table.c.name=='foo').values(name='bar')
    print result.fetchall()

Indexes
-------

PostgreSQL supports partial indexes. To create them pass a postgres_where
option to the Index constructor::

  Index('my_index', my_table.c.id, postgres_where=tbl.c.value > 10)

Transactions
------------

The psycopg2 dialect fully supports SAVEPOINT and two-phase commit operations.


"""

import decimal, random, re, string

from sqlalchemy import sql, schema, exc, util
from sqlalchemy.engine import base, default
from sqlalchemy.sql import compiler, expression
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgres.base import PGDialect, PGInet, PGCidr, PGMacAddr, PGArray, \
 PGBigInteger, PGInterval

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


colspecs = {
    sqltypes.Numeric : PGNumeric,
    sqltypes.Float: sqltypes.Float,  # prevents PGNumeric from being used
}

ischema_names = {
    'integer' : sqltypes.Integer,
    'bigint' : PGBigInteger,
    'smallint' : sqltypes.SmallInteger,
    'character varying' : sqltypes.String,
    'character' : sqltypes.CHAR,
    'text' : sqltypes.Text,
    'numeric' : PGNumeric,
    'float' : sqltypes.Float,
    'real' : sqltypes.Float,
    'inet': PGInet,
    'cidr': PGCidr,
    'macaddr': PGMacAddr,
    'double precision' : sqltypes.Float,
    'timestamp' : sqltypes.DateTime,
    'timestamp with time zone' : sqltypes.DateTime,
    'timestamp without time zone' : sqltypes.DateTime,
    'time with time zone' : sqltypes.Time,
    'time without time zone' : sqltypes.Time,
    'date' : sqltypes.Date,
    'time': sqltypes.Time,
    'bytea' : sqltypes.Binary,
    'boolean' : sqltypes.Boolean,
    'interval':PGInterval,
}

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

class Postgres_psycopg2(PGDialect):
    driver = 'psycopg2'
    supports_unicode_statements = False
    default_paramstyle = 'pyformat'
    supports_sane_multi_rowcount = False
    execution_ctx_cls = Postgres_psycopg2ExecutionContext
    ischema_names = ischema_names
    
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

    def type_descriptor(self, typeobj):
        return sqltypes.adapt_type(typeobj, colspecs)

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
    