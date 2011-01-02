# postgresql/psycopg2.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the PostgreSQL database via the psycopg2 driver.

Driver
------

The psycopg2 driver is supported, available at http://pypi.python.org/pypi/psycopg2/ .
The dialect has several behaviors  which are specifically tailored towards compatibility 
with this module.

Note that psycopg1 is **not** supported.

Unicode
-------

By default, the Psycopg2 driver uses the ``psycopg2.extensions.UNICODE``
extension, such that the DBAPI receives and returns all strings as Python
Unicode objects directly - SQLAlchemy passes these values through without
change. Note that this setting requires that the PG client encoding be set to
one which can accomodate the kind of character data being passed - typically
``utf-8``. If the Postgresql database is configured for ``SQL_ASCII``
encoding, which is often the default for PG installations, it may be necessary
for non-ascii strings to be encoded into a specific encoding before being
passed to the DBAPI. If changing the database's client encoding setting is not
an option, specify ``use_native_unicode=False`` as a keyword argument to
``create_engine()``, and take note of the ``encoding`` setting as well, which
also defaults to ``utf-8``. Note that disabling "native unicode" mode has a
slight performance penalty, as SQLAlchemy now must translate unicode strings
to/from an encoding such as utf-8, a task that is handled more efficiently
within the Psycopg2 driver natively.

Connecting
----------

URLs are of the form
``postgresql+psycopg2://user:password@host:port/dbname[?key=value&key=value...]``.

psycopg2-specific keyword arguments which are accepted by
:func:`.create_engine()` are:

* *server_side_cursors* - Enable the usage of "server side cursors" for SQL
  statements which support this feature. What this essentially means from a
  psycopg2 point of view is that the cursor is created using a name, e.g.
  `connection.cursor('some name')`, which has the effect that result rows are
  not immediately pre-fetched and buffered after statement execution, but are
  instead left on the server and only retrieved as needed. SQLAlchemy's
  :class:`~sqlalchemy.engine.base.ResultProxy` uses special row-buffering
  behavior when this feature is enabled, such that groups of 100 rows at a
  time are fetched over the wire to reduce conversational overhead.
* *use_native_unicode* - Enable the usage of Psycopg2 "native unicode" mode
  per connection. True by default.

Transactions
------------

The psycopg2 dialect fully supports SAVEPOINT and two-phase commit operations.

Transaction Isolation Level
---------------------------

The ``isolation_level`` parameter of :func:`.create_engine` here makes use
psycopg2's ``set_isolation_level()`` connection method, rather than
issuing a ``SET SESSION CHARACTERISTICS`` command.   This because psycopg2
resets the isolation level on each new transaction, and needs to know
at the API level what level should be used.

NOTICE logging
---------------

The psycopg2 dialect will log Postgresql NOTICE messages via the 
``sqlalchemy.dialects.postgresql`` logger::

    import logging
    logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)


Per-Statement Execution Options
-------------------------------

The following per-statement execution options are respected:

* *stream_results* - Enable or disable usage of server side cursors for the SELECT-statement.
  If *None* or not set, the *server_side_cursors* option of the connection is used. If
  auto-commit is enabled, the option is ignored.

"""

import random
import re
import decimal
import logging

from sqlalchemy import util, exc
from sqlalchemy import processors
from sqlalchemy.engine import base, default
from sqlalchemy.sql import expression
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects.postgresql.base import PGDialect, PGCompiler, \
                                PGIdentifierPreparer, PGExecutionContext, \
                                ENUM, ARRAY, _DECIMAL_TYPES, _FLOAT_TYPES,\
                                _INT_TYPES


logger = logging.getLogger('sqlalchemy.dialects.postgresql')


class _PGNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype in _FLOAT_TYPES:
                return processors.to_decimal_processor_factory(decimal.Decimal)
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                # pg8000 returns Decimal natively for 1700
                return None
            else:
                raise exc.InvalidRequestError(
                            "Unknown PG numeric type: %d" % coltype)
        else:
            if coltype in _FLOAT_TYPES:
                # pg8000 returns float natively for 701
                return None
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                return processors.to_float
            else:
                raise exc.InvalidRequestError(
                            "Unknown PG numeric type: %d" % coltype)

class _PGEnum(ENUM):
    def __init__(self, *arg, **kw):
        super(_PGEnum, self).__init__(*arg, **kw)
        if self.convert_unicode:
            self.convert_unicode = "force"

class _PGArray(ARRAY):
    def __init__(self, *arg, **kw):
        super(_PGArray, self).__init__(*arg, **kw)
        # FIXME: this check won't work for setups that
        # have convert_unicode only on their create_engine().
        if isinstance(self.item_type, sqltypes.String) and \
                    self.item_type.convert_unicode:
            self.item_type.convert_unicode = "force"

# When we're handed literal SQL, ensure it's a SELECT-query. Since
# 8.3, combining cursors and "FOR UPDATE" has been fine.
SERVER_SIDE_CURSOR_RE = re.compile(
    r'\s*SELECT',
    re.I | re.UNICODE)

class PGExecutionContext_psycopg2(PGExecutionContext):
    def create_cursor(self):
        # TODO: coverage for server side cursors + select.for_update()

        if self.dialect.server_side_cursors:
            is_server_side = \
                self.execution_options.get('stream_results', True) and (
                    (self.compiled and isinstance(self.compiled.statement, expression.Selectable) \
                    or \
                    (
                        (not self.compiled or 
                        isinstance(self.compiled.statement, expression._TextClause)) 
                        and self.statement and SERVER_SIDE_CURSOR_RE.match(self.statement))
                    )
                )
        else:
            is_server_side = self.execution_options.get('stream_results', False)

        self.__is_server_side = is_server_side
        if is_server_side:
            # use server-side cursors:
            # http://lists.initd.org/pipermail/psycopg/2007-January/005251.html
            ident = "c_%s_%s" % (hex(id(self))[2:], hex(random.randint(0, 65535))[2:])
            return self._connection.connection.cursor(ident)
        else:
            return self._connection.connection.cursor()

    def get_result_proxy(self):
        if logger.isEnabledFor(logging.INFO):
            self._log_notices(self.cursor)

        if self.__is_server_side:
            return base.BufferedRowResultProxy(self)
        else:
            return base.ResultProxy(self)

    def _log_notices(self, cursor):
        for notice in cursor.connection.notices:
            # NOTICE messages have a 
            # newline character at the end
            logger.info(notice.rstrip())

        cursor.connection.notices[:] = []


class PGCompiler_psycopg2(PGCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)

    def post_process_text(self, text):
        return text.replace('%', '%%')


class PGIdentifierPreparer_psycopg2(PGIdentifierPreparer):
    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace('%', '%%')

class PGDialect_psycopg2(PGDialect):
    driver = 'psycopg2'
    supports_unicode_statements = False
    default_paramstyle = 'pyformat'
    supports_sane_multi_rowcount = False
    execution_ctx_cls = PGExecutionContext_psycopg2
    statement_compiler = PGCompiler_psycopg2
    preparer = PGIdentifierPreparer_psycopg2

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric : _PGNumeric,
            ENUM : _PGEnum, # needs force_unicode
            sqltypes.Enum : _PGEnum, # needs force_unicode
            ARRAY : _PGArray, # needs force_unicode
        }
    )

    def __init__(self, server_side_cursors=False, use_native_unicode=True, **kwargs):
        PGDialect.__init__(self, **kwargs)
        self.server_side_cursors = server_side_cursors
        self.use_native_unicode = use_native_unicode
        self.supports_unicode_binds = use_native_unicode

    @classmethod
    def dbapi(cls):
        psycopg = __import__('psycopg2')
        return psycopg

    def on_connect(self):
        if self.isolation_level is not None:
            extensions = __import__('psycopg2.extensions').extensions
            isol = {
            'READ_COMMITTED':extensions.ISOLATION_LEVEL_READ_COMMITTED, 
            'READ_UNCOMMITTED':extensions.ISOLATION_LEVEL_READ_UNCOMMITTED, 
            'REPEATABLE_READ':extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            'SERIALIZABLE':extensions.ISOLATION_LEVEL_SERIALIZABLE

            }
            def base_on_connect(conn):
                try:
                    conn.set_isolation_level(isol[self.isolation_level])
                except:
                    raise exc.InvalidRequestError(
                                "Invalid isolation level: '%s'" % 
                                self.isolation_level)
        else:
            base_on_connect = None

        if self.dbapi and self.use_native_unicode:
            extensions = __import__('psycopg2.extensions').extensions
            def connect(conn):
                extensions.register_type(extensions.UNICODE, conn)
                if base_on_connect:
                    base_on_connect(conn)
            return connect
        else:
            return base_on_connect

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

dialect = PGDialect_psycopg2

