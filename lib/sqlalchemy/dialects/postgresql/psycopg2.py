# postgresql/psycopg2.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Support for the PostgreSQL database via the psycopg2 driver.

Driver
------

The psycopg2 driver is available at http://pypi.python.org/pypi/psycopg2/ .
The dialect has several behaviors  which are specifically tailored towards compatibility
with this module.

Note that psycopg1 is **not** supported.

Connecting
----------

URLs are of the form
``postgresql+psycopg2://user:password@host:port/dbname[?key=value&key=value...]``.

psycopg2-specific keyword arguments which are accepted by
:func:`.create_engine()` are:

* *server_side_cursors* - Enable the usage of "server side cursors" for SQL
  statements which support this feature. What this essentially means from a
  psycopg2 point of view is that the cursor is created using a name, e.g.
  ``connection.cursor('some name')``, which has the effect that result rows are
  not immediately pre-fetched and buffered after statement execution, but are
  instead left on the server and only retrieved as needed. SQLAlchemy's
  :class:`~sqlalchemy.engine.base.ResultProxy` uses special row-buffering
  behavior when this feature is enabled, such that groups of 100 rows at a
  time are fetched over the wire to reduce conversational overhead.
  Note that the ``stream_results=True`` execution option is a more targeted
  way of enabling this mode on a per-execution basis.
* *use_native_unicode* - Enable the usage of Psycopg2 "native unicode" mode
  per connection. True by default.

Unix Domain Connections
------------------------

psycopg2 supports connecting via Unix domain connections.   When the ``host``
portion of the URL is omitted, SQLAlchemy passes ``None`` to psycopg2,
which specifies Unix-domain communication rather than TCP/IP communication::

    create_engine("postgresql+psycopg2://user:password@/dbname")

By default, the socket file used is to connect to a Unix-domain socket
in ``/tmp``, or whatever socket directory was specified when PostgreSQL
was built.  This value can be overridden by passing a pathname to psycopg2,
using ``host`` as an additional keyword argument::

    create_engine("postgresql+psycopg2://user:password@/dbname?host=/var/lib/postgresql")

See also:

`PQconnectdbParams <http://www.postgresql.org/docs/9.1/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS>`_

Per-Statement/Connection Execution Options
-------------------------------------------

The following DBAPI-specific options are respected when used with
:meth:`.Connection.execution_options`, :meth:`.Executable.execution_options`,
:meth:`.Query.execution_options`, in addition to those not specific to DBAPIs:

* isolation_level - Set the transaction isolation level for the lifespan of a
  :class:`.Connection` (can only be set on a connection, not a statement or query).
  This includes the options ``SERIALIZABLE``, ``READ COMMITTED``,
  ``READ UNCOMMITTED`` and ``REPEATABLE READ``.
* stream_results - Enable or disable usage of server side cursors.
  If ``None`` or not set, the ``server_side_cursors`` option of the :class:`.Engine` is used.

Unicode
-------

By default, the psycopg2 driver uses the ``psycopg2.extensions.UNICODE``
extension, such that the DBAPI receives and returns all strings as Python
Unicode objects directly - SQLAlchemy passes these values through without
change.   Psycopg2 here will encode/decode string values based on the
current "client encoding" setting; by default this is the value in
the ``postgresql.conf`` file, which often defaults to ``SQL_ASCII``.
Typically, this can be changed to ``utf-8``, as a more useful default::

    #client_encoding = sql_ascii # actually, defaults to database
                                 # encoding
    client_encoding = utf8

A second way to affect the client encoding is to set it within Psycopg2
locally.   SQLAlchemy will call psycopg2's ``set_client_encoding()``
method (see: http://initd.org/psycopg/docs/connection.html#connection.set_client_encoding)
on all new connections based on the value passed to
:func:`.create_engine` using the ``client_encoding`` parameter::

    engine = create_engine("postgresql://user:pass@host/dbname", client_encoding='utf8')

This overrides the encoding specified in the Postgresql client configuration.

.. versionadded:: 0.7.3
    The psycopg2-specific ``client_encoding`` parameter to :func:`.create_engine`.

SQLAlchemy can also be instructed to skip the usage of the psycopg2
``UNICODE`` extension and to instead utilize it's own unicode encode/decode
services, which are normally reserved only for those DBAPIs that don't
fully support unicode directly.  Passing ``use_native_unicode=False``
to :func:`.create_engine` will disable usage of ``psycopg2.extensions.UNICODE``.
SQLAlchemy will instead encode data itself into Python bytestrings on the way
in and coerce from bytes on the way back,
using the value of the :func:`.create_engine` ``encoding`` parameter, which
defaults to ``utf-8``.
SQLAlchemy's own unicode encode/decode functionality is steadily becoming
obsolete as more DBAPIs support unicode fully along with the approach of
Python 3; in modern usage psycopg2 should be relied upon to handle unicode.

Transactions
------------

The psycopg2 dialect fully supports SAVEPOINT and two-phase commit operations.

.. _psycopg2_isolation:

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


"""

import re
import logging

from sqlalchemy import util, exc
from sqlalchemy.util.compat import decimal
from sqlalchemy import processors
from sqlalchemy.engine import base
from sqlalchemy.sql import expression
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
        # Py2K
        if self.convert_unicode:
            self.convert_unicode = "force"
        # end Py2K

class _PGArray(ARRAY):
    def __init__(self, *arg, **kw):
        super(_PGArray, self).__init__(*arg, **kw)
        # Py2K
        # FIXME: this check won't work for setups that
        # have convert_unicode only on their create_engine().
        if isinstance(self.item_type, sqltypes.String) and \
                    self.item_type.convert_unicode:
            self.item_type.convert_unicode = "force"
        # end Py2K

# When we're handed literal SQL, ensure it's a SELECT-query. Since
# 8.3, combining cursors and "FOR UPDATE" has been fine.
SERVER_SIDE_CURSOR_RE = re.compile(
    r'\s*SELECT',
    re.I | re.UNICODE)

_server_side_id = util.counter()

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
            ident = "c_%s_%s" % (hex(id(self))[2:], hex(_server_side_id())[2:])
            return self._dbapi_connection.cursor(ident)
        else:
            return self._dbapi_connection.cursor()

    def get_result_proxy(self):
        # TODO: ouch
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
    # Py2K
    supports_unicode_statements = False
    # end Py2K
    default_paramstyle = 'pyformat'
    supports_sane_multi_rowcount = False
    execution_ctx_cls = PGExecutionContext_psycopg2
    statement_compiler = PGCompiler_psycopg2
    preparer = PGIdentifierPreparer_psycopg2
    psycopg2_version = (0, 0)

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric : _PGNumeric,
            ENUM : _PGEnum, # needs force_unicode
            sqltypes.Enum : _PGEnum, # needs force_unicode
            ARRAY : _PGArray, # needs force_unicode
        }
    )

    def __init__(self, server_side_cursors=False, use_native_unicode=True,
                        client_encoding=None, **kwargs):
        PGDialect.__init__(self, **kwargs)
        self.server_side_cursors = server_side_cursors
        self.use_native_unicode = use_native_unicode
        self.supports_unicode_binds = use_native_unicode
        self.client_encoding = client_encoding
        if self.dbapi and hasattr(self.dbapi, '__version__'):
            m = re.match(r'(\d+)\.(\d+)(?:\.(\d+))?',
                                self.dbapi.__version__)
            if m:
                self.psycopg2_version = tuple(
                                            int(x)
                                            for x in m.group(1, 2, 3)
                                            if x is not None)

    @classmethod
    def dbapi(cls):
        psycopg = __import__('psycopg2')
        return psycopg

    @util.memoized_property
    def _isolation_lookup(self):
        extensions = __import__('psycopg2.extensions').extensions
        return {
            'READ COMMITTED':extensions.ISOLATION_LEVEL_READ_COMMITTED,
            'READ UNCOMMITTED':extensions.ISOLATION_LEVEL_READ_UNCOMMITTED,
            'REPEATABLE READ':extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            'SERIALIZABLE':extensions.ISOLATION_LEVEL_SERIALIZABLE
        }

    def set_isolation_level(self, connection, level):
        try:
            level = self._isolation_lookup[level.replace('_', ' ')]
        except KeyError:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s" %
                (level, self.name, ", ".join(self._isolation_lookup))
                )

        connection.set_isolation_level(level)

    def on_connect(self):
        fns = []
        if self.client_encoding is not None:
            def on_connect(conn):
                conn.set_client_encoding(self.client_encoding)
            fns.append(on_connect)

        if self.isolation_level is not None:
            def on_connect(conn):
                self.set_isolation_level(conn, self.isolation_level)
            fns.append(on_connect)

        if self.dbapi and self.use_native_unicode:
            extensions = __import__('psycopg2.extensions').extensions
            def on_connect(conn):
                extensions.register_type(extensions.UNICODE, conn)
            fns.append(on_connect)

        if fns:
            def on_connect(conn):
                for fn in fns:
                    fn(conn)
            return on_connect
        else:
            return None

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        return ([], opts)

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.OperationalError):
            # these error messages from libpq: interfaces/libpq/fe-misc.c.
            # TODO: these are sent through gettext in libpq and we can't
            # check within other locales - consider using connection.closed
            return 'terminating connection' in str(e) or \
                    'closed the connection' in str(e) or \
                    'connection not open' in str(e) or \
                    'could not receive data from server' in str(e)
        elif isinstance(e, self.dbapi.InterfaceError):
            # psycopg2 client errors, psycopg2/conenction.h, psycopg2/cursor.h
            return 'connection already closed' in str(e) or \
                    'cursor already closed' in str(e)
        elif isinstance(e, self.dbapi.ProgrammingError):
            # not sure where this path is originally from, it may
            # be obsolete.   It really says "losed", not "closed".
            return "losed the connection unexpectedly" in str(e)
        else:
            return False

dialect = PGDialect_psycopg2

