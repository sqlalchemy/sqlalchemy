# postgresql/psycopg2.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
.. dialect:: postgresql+psycopg2
    :name: psycopg2
    :dbapi: psycopg2
    :connectstring: postgresql+psycopg2://user:password@host:port/dbname\
[?key=value&key=value...]
    :url: http://pypi.python.org/pypi/psycopg2/

psycopg2 Connect Arguments
-----------------------------------

psycopg2-specific keyword arguments which are accepted by
:func:`.create_engine()` are:

* ``server_side_cursors``: Enable the usage of "server side cursors" for SQL
  statements which support this feature. What this essentially means from a
  psycopg2 point of view is that the cursor is created using a name, e.g.
  ``connection.cursor('some name')``, which has the effect that result rows
  are not immediately pre-fetched and buffered after statement execution, but
  are instead left on the server and only retrieved as needed. SQLAlchemy's
  :class:`~sqlalchemy.engine.ResultProxy` uses special row-buffering
  behavior when this feature is enabled, such that groups of 100 rows at a
  time are fetched over the wire to reduce conversational overhead.
  Note that the ``stream_results=True`` execution option is a more targeted
  way of enabling this mode on a per-execution basis.
* ``use_native_unicode``: Enable the usage of Psycopg2 "native unicode" mode
  per connection.  True by default.

  .. seealso::

    :ref:`psycopg2_disable_native_unicode`

* ``isolation_level``: This option, available for all PostgreSQL dialects,
  includes the ``AUTOCOMMIT`` isolation level when using the psycopg2
  dialect.

  .. seealso::

    :ref:`psycopg2_isolation_level`

* ``client_encoding``: sets the client encoding in a libpq-agnostic way,
  using psycopg2's ``set_client_encoding()`` method.

  .. seealso::

    :ref:`psycopg2_unicode`

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

`PQconnectdbParams <http://www.postgresql.org/docs/9.1/static\
/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS>`_

Per-Statement/Connection Execution Options
-------------------------------------------

The following DBAPI-specific options are respected when used with
:meth:`.Connection.execution_options`, :meth:`.Executable.execution_options`,
:meth:`.Query.execution_options`, in addition to those not specific to DBAPIs:

* isolation_level - Set the transaction isolation level for the lifespan of a
  :class:`.Connection` (can only be set on a connection, not a statement
  or query).   See :ref:`psycopg2_isolation_level`.

* stream_results - Enable or disable usage of psycopg2 server side cursors -
  this feature makes use of "named" cursors in combination with special
  result handling methods so that result rows are not fully buffered.
  If ``None`` or not set, the ``server_side_cursors`` option of the
  :class:`.Engine` is used.

.. _psycopg2_unicode:

Unicode with Psycopg2
----------------------

By default, the psycopg2 driver uses the ``psycopg2.extensions.UNICODE``
extension, such that the DBAPI receives and returns all strings as Python
Unicode objects directly - SQLAlchemy passes these values through without
change.   Psycopg2 here will encode/decode string values based on the
current "client encoding" setting; by default this is the value in
the ``postgresql.conf`` file, which often defaults to ``SQL_ASCII``.
Typically, this can be changed to ``utf8``, as a more useful default::

    # postgresql.conf file

    # client_encoding = sql_ascii # actually, defaults to database
                                 # encoding
    client_encoding = utf8

A second way to affect the client encoding is to set it within Psycopg2
locally.   SQLAlchemy will call psycopg2's
:meth:`psycopg2:connection.set_client_encoding` method
on all new connections based on the value passed to
:func:`.create_engine` using the ``client_encoding`` parameter::

    # set_client_encoding() setting;
    # works for *all* Postgresql versions
    engine = create_engine("postgresql://user:pass@host/dbname",
                           client_encoding='utf8')

This overrides the encoding specified in the Postgresql client configuration.
When using the parameter in this way, the psycopg2 driver emits
``SET client_encoding TO 'utf8'`` on the connection explicitly, and works
in all Postgresql versions.

Note that the ``client_encoding`` setting as passed to :func:`.create_engine`
is **not the same** as the more recently added ``client_encoding`` parameter
now supported by libpq directly.   This is enabled when ``client_encoding``
is passed directly to ``psycopg2.connect()``, and from SQLAlchemy is passed
using the :paramref:`.create_engine.connect_args` parameter::

    # libpq direct parameter setting;
    # only works for Postgresql **9.1 and above**
    engine = create_engine("postgresql://user:pass@host/dbname",
                           connect_args={'client_encoding': 'utf8'})

    # using the query string is equivalent
    engine = create_engine("postgresql://user:pass@host/dbname?client_encoding=utf8")

The above parameter was only added to libpq as of version 9.1 of Postgresql,
so using the previous method is better for cross-version support.

.. _psycopg2_disable_native_unicode:

Disabling Native Unicode
^^^^^^^^^^^^^^^^^^^^^^^^

SQLAlchemy can also be instructed to skip the usage of the psycopg2
``UNICODE`` extension and to instead utilize its own unicode encode/decode
services, which are normally reserved only for those DBAPIs that don't
fully support unicode directly.  Passing ``use_native_unicode=False`` to
:func:`.create_engine` will disable usage of ``psycopg2.extensions.UNICODE``.
SQLAlchemy will instead encode data itself into Python bytestrings on the way
in and coerce from bytes on the way back,
using the value of the :func:`.create_engine` ``encoding`` parameter, which
defaults to ``utf-8``.
SQLAlchemy's own unicode encode/decode functionality is steadily becoming
obsolete as most DBAPIs now support unicode fully.

Transactions
------------

The psycopg2 dialect fully supports SAVEPOINT and two-phase commit operations.

.. _psycopg2_isolation_level:

Psycopg2 Transaction Isolation Level
-------------------------------------

As discussed in :ref:`postgresql_isolation_level`,
all Postgresql dialects support setting of transaction isolation level
both via the ``isolation_level`` parameter passed to :func:`.create_engine`,
as well as the ``isolation_level`` argument used by
:meth:`.Connection.execution_options`.  When using the psycopg2 dialect, these
options make use of psycopg2's ``set_isolation_level()`` connection method,
rather than emitting a Postgresql directive; this is because psycopg2's
API-level setting is always emitted at the start of each transaction in any
case.

The psycopg2 dialect supports these constants for isolation level:

* ``READ COMMITTED``
* ``READ UNCOMMITTED``
* ``REPEATABLE READ``
* ``SERIALIZABLE``
* ``AUTOCOMMIT``

.. versionadded:: 0.8.2 support for AUTOCOMMIT isolation level when using
   psycopg2.

.. seealso::

    :ref:`postgresql_isolation_level`

    :ref:`pg8000_isolation_level`


NOTICE logging
---------------

The psycopg2 dialect will log Postgresql NOTICE messages via the
``sqlalchemy.dialects.postgresql`` logger::

    import logging
    logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)

.. _psycopg2_hstore::

HSTORE type
------------

The ``psycopg2`` DBAPI includes an extension to natively handle marshalling of
the HSTORE type.   The SQLAlchemy psycopg2 dialect will enable this extension
by default when it is detected that the target database has the HSTORE
type set up for use.   In other words, when the dialect makes the first
connection, a sequence like the following is performed:

1. Request the available HSTORE oids using
   ``psycopg2.extras.HstoreAdapter.get_oids()``.
   If this function returns a list of HSTORE identifiers, we then determine
   that the ``HSTORE`` extension is present.

2. If the ``use_native_hstore`` flag is at its default of ``True``, and
   we've detected that ``HSTORE`` oids are available, the
   ``psycopg2.extensions.register_hstore()`` extension is invoked for all
   connections.

The ``register_hstore()`` extension has the effect of **all Python
dictionaries being accepted as parameters regardless of the type of target
column in SQL**. The dictionaries are converted by this extension into a
textual HSTORE expression.  If this behavior is not desired, disable the
use of the hstore extension by setting ``use_native_hstore`` to ``False`` as
follows::

    engine = create_engine("postgresql+psycopg2://scott:tiger@localhost/test",
                use_native_hstore=False)

The ``HSTORE`` type is **still supported** when the
``psycopg2.extensions.register_hstore()`` extension is not used.  It merely
means that the coercion between Python dictionaries and the HSTORE
string format, on both the parameter side and the result side, will take
place within SQLAlchemy's own marshalling logic, and not that of ``psycopg2``
which may be more performant.

"""
from __future__ import absolute_import

import re
import logging

from ... import util, exc
import decimal
from ... import processors
from ...engine import result as _result
from ...sql import expression
from ... import types as sqltypes
from .base import PGDialect, PGCompiler, \
    PGIdentifierPreparer, PGExecutionContext, \
    ENUM, ARRAY, _DECIMAL_TYPES, _FLOAT_TYPES,\
    _INT_TYPES
from .hstore import HSTORE
from .json import JSON


logger = logging.getLogger('sqlalchemy.dialects.postgresql')


class _PGNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype in _FLOAT_TYPES:
                return processors.to_decimal_processor_factory(
                    decimal.Decimal,
                    self._effective_decimal_return_scale)
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
    def result_processor(self, dialect, coltype):
        if util.py2k and self.convert_unicode is True:
            # we can't easily use PG's extensions here because
            # the OID is on the fly, and we need to give it a python
            # function anyway - not really worth it.
            self.convert_unicode = "force_nocheck"
        return super(_PGEnum, self).result_processor(dialect, coltype)


class _PGHStore(HSTORE):
    def bind_processor(self, dialect):
        if dialect._has_native_hstore:
            return None
        else:
            return super(_PGHStore, self).bind_processor(dialect)

    def result_processor(self, dialect, coltype):
        if dialect._has_native_hstore:
            return None
        else:
            return super(_PGHStore, self).result_processor(dialect, coltype)


class _PGJSON(JSON):

    def result_processor(self, dialect, coltype):
        if dialect._has_native_json:
            return None
        else:
            return super(_PGJSON, self).result_processor(dialect, coltype)

# When we're handed literal SQL, ensure it's a SELECT query. Since
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
                    (self.compiled and isinstance(self.compiled.statement,
                                                  expression.Selectable)
                     or
                     (
                        (not self.compiled or
                         isinstance(self.compiled.statement,
                                    expression.TextClause))
                        and self.statement and SERVER_SIDE_CURSOR_RE.match(
                            self.statement))
                     )
                )
        else:
            is_server_side = \
                self.execution_options.get('stream_results', False)

        self.__is_server_side = is_server_side
        if is_server_side:
            # use server-side cursors:
            # http://lists.initd.org/pipermail/psycopg/2007-January/005251.html
            ident = "c_%s_%s" % (hex(id(self))[2:],
                                 hex(_server_side_id())[2:])
            return self._dbapi_connection.cursor(ident)
        else:
            return self._dbapi_connection.cursor()

    def get_result_proxy(self):
        # TODO: ouch
        if logger.isEnabledFor(logging.INFO):
            self._log_notices(self.cursor)

        if self.__is_server_side:
            return _result.BufferedRowResultProxy(self)
        else:
            return _result.ResultProxy(self)

    def _log_notices(self, cursor):
        for notice in cursor.connection.notices:
            # NOTICE messages have a
            # newline character at the end
            logger.info(notice.rstrip())

        cursor.connection.notices[:] = []


class PGCompiler_psycopg2(PGCompiler):
    def visit_mod_binary(self, binary, operator, **kw):
        return self.process(binary.left, **kw) + " %% " + \
            self.process(binary.right, **kw)

    def post_process_text(self, text):
        return text.replace('%', '%%')


class PGIdentifierPreparer_psycopg2(PGIdentifierPreparer):
    def _escape_identifier(self, value):
        value = value.replace(self.escape_quote, self.escape_to_quote)
        return value.replace('%', '%%')


class PGDialect_psycopg2(PGDialect):
    driver = 'psycopg2'
    if util.py2k:
        supports_unicode_statements = False

    default_paramstyle = 'pyformat'
    # set to true based on psycopg2 version
    supports_sane_multi_rowcount = False
    execution_ctx_cls = PGExecutionContext_psycopg2
    statement_compiler = PGCompiler_psycopg2
    preparer = PGIdentifierPreparer_psycopg2
    psycopg2_version = (0, 0)

    _has_native_hstore = False
    _has_native_json = False

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric: _PGNumeric,
            ENUM: _PGEnum,  # needs force_unicode
            sqltypes.Enum: _PGEnum,  # needs force_unicode
            HSTORE: _PGHStore,
            JSON: _PGJSON
        }
    )

    def __init__(self, server_side_cursors=False, use_native_unicode=True,
                 client_encoding=None,
                 use_native_hstore=True,
                 **kwargs):
        PGDialect.__init__(self, **kwargs)
        self.server_side_cursors = server_side_cursors
        self.use_native_unicode = use_native_unicode
        self.use_native_hstore = use_native_hstore
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

    def initialize(self, connection):
        super(PGDialect_psycopg2, self).initialize(connection)
        self._has_native_hstore = self.use_native_hstore and \
            self._hstore_oids(connection.connection) \
            is not None
        self._has_native_json = self.psycopg2_version >= (2, 5)

        # http://initd.org/psycopg/docs/news.html#what-s-new-in-psycopg-2-0-9
        self.supports_sane_multi_rowcount = self.psycopg2_version >= (2, 0, 9)

    @classmethod
    def dbapi(cls):
        import psycopg2
        return psycopg2

    @util.memoized_property
    def _isolation_lookup(self):
        from psycopg2 import extensions
        return {
            'AUTOCOMMIT': extensions.ISOLATION_LEVEL_AUTOCOMMIT,
            'READ COMMITTED': extensions.ISOLATION_LEVEL_READ_COMMITTED,
            'READ UNCOMMITTED': extensions.ISOLATION_LEVEL_READ_UNCOMMITTED,
            'REPEATABLE READ': extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            'SERIALIZABLE': extensions.ISOLATION_LEVEL_SERIALIZABLE
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
        from psycopg2 import extras, extensions

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
            def on_connect(conn):
                extensions.register_type(extensions.UNICODE, conn)
                extensions.register_type(extensions.UNICODEARRAY, conn)
            fns.append(on_connect)

        if self.dbapi and self.use_native_hstore:
            def on_connect(conn):
                hstore_oids = self._hstore_oids(conn)
                if hstore_oids is not None:
                    oid, array_oid = hstore_oids
                    if util.py2k:
                        extras.register_hstore(conn, oid=oid,
                                               array_oid=array_oid,
                                               unicode=True)
                    else:
                        extras.register_hstore(conn, oid=oid,
                                               array_oid=array_oid)
            fns.append(on_connect)

        if self.dbapi and self._json_deserializer:
            def on_connect(conn):
                extras.register_default_json(
                    conn, loads=self._json_deserializer)
            fns.append(on_connect)

        if fns:
            def on_connect(conn):
                for fn in fns:
                    fn(conn)
            return on_connect
        else:
            return None

    @util.memoized_instancemethod
    def _hstore_oids(self, conn):
        if self.psycopg2_version >= (2, 4):
            from psycopg2 import extras
            oids = extras.HstoreAdapter.get_oids(conn)
            if oids is not None and oids[0]:
                return oids[0:2]
        return None

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'port' in opts:
            opts['port'] = int(opts['port'])
        opts.update(url.query)
        return ([], opts)

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.Error):
            # check the "closed" flag.  this might not be
            # present on old psycopg2 versions.   Also,
            # this flag doesn't actually help in a lot of disconnect
            # situations, so don't rely on it.
            if getattr(connection, 'closed', False):
                return True

            # checks based on strings.  in the case that .closed
            # didn't cut it, fall back onto these.
            str_e = str(e).partition("\n")[0]
            for msg in [
                # these error messages from libpq: interfaces/libpq/fe-misc.c
                # and interfaces/libpq/fe-secure.c.
                'terminating connection',
                'closed the connection',
                'connection not open',
                'could not receive data from server',
                'could not send data to server',
                # psycopg2 client errors, psycopg2/conenction.h,
                # psycopg2/cursor.h
                'connection already closed',
                'cursor already closed',
                # not sure where this path is originally from, it may
                # be obsolete.   It really says "losed", not "closed".
                'losed the connection unexpectedly',
                # these can occur in newer SSL
                'connection has been closed unexpectedly',
                'SSL SYSCALL error: Bad file descriptor',
                'SSL SYSCALL error: EOF detected',
            ]:
                idx = str_e.find(msg)
                if idx >= 0 and '"' not in str_e[:idx]:
                    return True
        return False

dialect = PGDialect_psycopg2
