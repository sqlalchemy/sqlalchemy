# postgresql/psycopg2.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
r"""
.. dialect:: postgresql+psycopg2
    :name: psycopg2
    :dbapi: psycopg2
    :connectstring: postgresql+psycopg2://user:password@host:port/dbname[?key=value&key=value...]
    :url: http://pypi.python.org/pypi/psycopg2/

psycopg2 Connect Arguments
-----------------------------------

psycopg2-specific keyword arguments which are accepted by
:func:`_sa.create_engine()` are:

* ``server_side_cursors``: Enable the usage of "server side cursors" for SQL
  statements which support this feature. What this essentially means from a
  psycopg2 point of view is that the cursor is created using a name, e.g.
  ``connection.cursor('some name')``, which has the effect that result rows
  are not immediately pre-fetched and buffered after statement execution, but
  are instead left on the server and only retrieved as needed. SQLAlchemy's
  :class:`~sqlalchemy.engine.ResultProxy` uses special row-buffering
  behavior when this feature is enabled, such that groups of 100 rows at a
  time are fetched over the wire to reduce conversational overhead.
  Note that the :paramref:`.Connection.execution_options.stream_results`
  execution option is a more targeted
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

* ``executemany_mode``, ``executemany_batch_page_size``,
  ``executemany_values_page_size``: Allows use of psycopg2
  extensions for optimizing "executemany"-stye queries.  See the referenced
  section below for details.

  .. seealso::

    :ref:`psycopg2_executemany_mode`

* ``use_batch_mode``: this is the previous setting used to affect "executemany"
  mode and is now deprecated.


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

.. seealso::

    `PQconnectdbParams \
    <http://www.postgresql.org/docs/9.1/static/libpq-connect.html#LIBPQ-PQCONNECTDBPARAMS>`_

.. _psycopg2_multi_host:

Specfiying multiple fallback hosts
------------------------------------

psycopg2 supports multiple connection points in the connection string.
When the ``host`` parameter is used multiple times in the query section of
the URL, SQLAlchemy will create a single string of the host and port
information provided to make the connections::

    create_engine(
        "postgresql+psycopg2://user:password@/dbname?host=HostA:port1&host=HostB&host=HostC"
    )

A connection to each host is then attempted until either a connection is successful
or all connections are unsuccessful in which case an error is raised.

.. versionadded:: 1.3.20 Support for multiple hosts in PostgreSQL connection
   string.

.. seealso::

    `PQConnString \
    <https://www.postgresql.org/docs/10/libpq-connect.html#LIBPQ-CONNSTRING>`_

Empty DSN Connections / Environment Variable Connections
---------------------------------------------------------

The psycopg2 DBAPI can connect to PostgreSQL by passing an empty DSN to the
libpq client library, which by default indicates to connect to a localhost
PostgreSQL database that is open for "trust" connections.  This behavior can be
further tailored using a particular set of environment variables which are
prefixed with ``PG_...``, which are  consumed by ``libpq`` to take the place of
any or all elements of the connection string.

For this form, the URL can be passed without any elements other than the
initial scheme::

    engine = create_engine('postgresql+psycopg2://')

In the above form, a blank "dsn" string is passed to the ``psycopg2.connect()``
function which in turn represents an empty DSN passed to libpq.

.. versionadded:: 1.3.2 support for parameter-less connections with psycopg2.

.. seealso::

    `Environment Variables\
    <https://www.postgresql.org/docs/current/libpq-envars.html>`_ -
    PostgreSQL documentation on how to use ``PG_...``
    environment variables for connections.

.. _psycopg2_execution_options:

Per-Statement/Connection Execution Options
-------------------------------------------

The following DBAPI-specific options are respected when used with
:meth:`_engine.Connection.execution_options`,
:meth:`.Executable.execution_options`,
:meth:`_query.Query.execution_options`,
in addition to those not specific to DBAPIs:

* ``isolation_level`` - Set the transaction isolation level for the lifespan
  of a :class:`_engine.Connection` (can only be set on a connection,
  not a statement
  or query).   See :ref:`psycopg2_isolation_level`.

* ``stream_results`` - Enable or disable usage of psycopg2 server side
  cursors - this feature makes use of "named" cursors in combination with
  special result handling methods so that result rows are not fully buffered.
  If ``None`` or not set, the ``server_side_cursors`` option of the
  :class:`_engine.Engine` is used.

* ``max_row_buffer`` - when using ``stream_results``, an integer value that
  specifies the maximum number of rows to buffer at a time.  This is
  interpreted by the :class:`.BufferedRowResultProxy`, and if omitted the
  buffer will grow to ultimately store 1000 rows at a time.

  .. versionadded:: 1.0.6

.. _psycopg2_batch_mode:

.. _psycopg2_executemany_mode:

Psycopg2 Fast Execution Helpers
-------------------------------

Modern versions of psycopg2 include a feature known as
`Fast Execution Helpers \
<http://initd.org/psycopg/docs/extras.html#fast-execution-helpers>`_, which
have been shown in benchmarking to improve psycopg2's executemany()
performance, primarily with INSERT statements, by multiple orders of magnitude.
SQLAlchemy allows this extension to be used for all ``executemany()`` style
calls invoked by an :class:`_engine.Engine`
when used with :ref:`multiple parameter
sets <execute_multiple>`, which includes the use of this feature both by the
Core as well as by the ORM for inserts of objects with non-autogenerated
primary key values, by adding the ``executemany_mode`` flag to
:func:`_sa.create_engine`::

    engine = create_engine(
        "postgresql+psycopg2://scott:tiger@host/dbname",
        executemany_mode='batch')


.. versionchanged:: 1.3.7  - the ``use_batch_mode`` flag has been superseded
   by a new parameter ``executemany_mode`` which provides support both for
   psycopg2's ``execute_batch`` helper as well as the ``execute_values``
   helper.

Possible options for ``executemany_mode`` include:

* ``None`` - By default, psycopg2's extensions are not used, and the usual
  ``cursor.executemany()`` method is used when invoking batches of statements.

* ``'batch'`` - Uses ``psycopg2.extras.execute_batch`` so that multiple copies
  of a SQL query, each one corresponding to a parameter set passed to
  ``executemany()``, are joined into a single SQL string separated by a
  semicolon.   This is the same behavior as was provided by the
  ``use_batch_mode=True`` flag.

* ``'values'``- For Core :func:`_expression.insert`
  constructs only (including those
  emitted by the ORM automatically), the ``psycopg2.extras.execute_values``
  extension is used so that multiple parameter sets are grouped into a single
  INSERT statement and joined together with multiple VALUES expressions.   This
  method requires that the string text of the VALUES clause inside the
  INSERT statement is manipulated, so is only supported with a compiled
  :func:`_expression.insert` construct where the format is predictable.
  For all other
  constructs,  including plain textual INSERT statements not rendered  by the
  SQLAlchemy expression language compiler, the
  ``psycopg2.extras.execute_batch``  method is used.   It is therefore important
  to note that **"values" mode implies that "batch" mode is also used for
  all statements for which "values" mode does not apply**.

For both strategies, the ``executemany_batch_page_size`` and
``executemany_values_page_size`` arguments control how many parameter sets
should be represented in each execution.  Because "values" mode implies a
fallback down to "batch" mode for non-INSERT statements, there are two
independent page size arguments.  For each, the default value of ``None`` means
to use psycopg2's defaults, which at the time of this writing are quite low at
100.   For the ``execute_values`` method, a number as high as 10000 may prove
to be performant, whereas for ``execute_batch``, as the number represents
full statements repeated, a number closer to the default of 100 is likely
more appropriate::

    engine = create_engine(
        "postgresql+psycopg2://scott:tiger@host/dbname",
        executemany_mode='values',
        executemany_values_page_size=10000, executemany_batch_page_size=500)


.. seealso::

    :ref:`execute_multiple` - General information on using the
    :class:`_engine.Connection`
    object to execute statements in such a way as to make
    use of the DBAPI ``.executemany()`` method.

.. versionchanged:: 1.3.7 - Added support for
   ``psycopg2.extras.execute_values``.   The ``use_batch_mode`` flag is
   superseded by the ``executemany_mode`` flag.


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
:func:`_sa.create_engine` using the ``client_encoding`` parameter::

    # set_client_encoding() setting;
    # works for *all* PostgreSQL versions
    engine = create_engine("postgresql://user:pass@host/dbname",
                           client_encoding='utf8')

This overrides the encoding specified in the PostgreSQL client configuration.
When using the parameter in this way, the psycopg2 driver emits
``SET client_encoding TO 'utf8'`` on the connection explicitly, and works
in all PostgreSQL versions.

Note that the ``client_encoding`` setting as passed to
:func:`_sa.create_engine`
is **not the same** as the more recently added ``client_encoding`` parameter
now supported by libpq directly.   This is enabled when ``client_encoding``
is passed directly to ``psycopg2.connect()``, and from SQLAlchemy is passed
using the :paramref:`_sa.create_engine.connect_args` parameter::

    engine = create_engine(
        "postgresql://user:pass@host/dbname",
        connect_args={'client_encoding': 'utf8'})

    # using the query string is equivalent
    engine = create_engine("postgresql://user:pass@host/dbname?client_encoding=utf8")

The above parameter was only added to libpq as of version 9.1 of PostgreSQL,
so using the previous method is better for cross-version support.

.. _psycopg2_disable_native_unicode:

Disabling Native Unicode
^^^^^^^^^^^^^^^^^^^^^^^^

SQLAlchemy can also be instructed to skip the usage of the psycopg2
``UNICODE`` extension and to instead utilize its own unicode encode/decode
services, which are normally reserved only for those DBAPIs that don't
fully support unicode directly.  Passing ``use_native_unicode=False`` to
:func:`_sa.create_engine` will disable usage of ``psycopg2.extensions.
UNICODE``.
SQLAlchemy will instead encode data itself into Python bytestrings on the way
in and coerce from bytes on the way back,
using the value of the :func:`_sa.create_engine` ``encoding`` parameter, which
defaults to ``utf-8``.
SQLAlchemy's own unicode encode/decode functionality is steadily becoming
obsolete as most DBAPIs now support unicode fully.

Bound Parameter Styles
----------------------

The default parameter style for the psycopg2 dialect is "pyformat", where
SQL is rendered using ``%(paramname)s`` style.   This format has the limitation
that it does not accommodate the unusual case of parameter names that
actually contain percent or parenthesis symbols; as SQLAlchemy in many cases
generates bound parameter names based on the name of a column, the presence
of these characters in a column name can lead to problems.

There are two solutions to the issue of a :class:`_schema.Column`
that contains
one of these characters in its name.  One is to specify the
:paramref:`.schema.Column.key` for columns that have such names::

    measurement = Table('measurement', metadata,
        Column('Size (meters)', Integer, key='size_meters')
    )

Above, an INSERT statement such as ``measurement.insert()`` will use
``size_meters`` as the parameter name, and a SQL expression such as
``measurement.c.size_meters > 10`` will derive the bound parameter name
from the ``size_meters`` key as well.

.. versionchanged:: 1.0.0 - SQL expressions will use
   :attr:`_schema.Column.key`
   as the source of naming when anonymous bound parameters are created
   in SQL expressions; previously, this behavior only applied to
   :meth:`_schema.Table.insert` and :meth:`_schema.Table.update`
   parameter names.

The other solution is to use a positional format; psycopg2 allows use of the
"format" paramstyle, which can be passed to
:paramref:`_sa.create_engine.paramstyle`::

    engine = create_engine(
        'postgresql://scott:tiger@localhost:5432/test', paramstyle='format')

With the above engine, instead of a statement like::

    INSERT INTO measurement ("Size (meters)") VALUES (%(Size (meters))s)
    {'Size (meters)': 1}

we instead see::

    INSERT INTO measurement ("Size (meters)") VALUES (%s)
    (1, )

Where above, the dictionary style is converted into a tuple with positional
style.


Transactions
------------

The psycopg2 dialect fully supports SAVEPOINT and two-phase commit operations.

.. _psycopg2_isolation_level:

Psycopg2 Transaction Isolation Level
-------------------------------------

As discussed in :ref:`postgresql_isolation_level`,
all PostgreSQL dialects support setting of transaction isolation level
both via the ``isolation_level`` parameter passed to :func:`_sa.create_engine`
,
as well as the ``isolation_level`` argument used by
:meth:`_engine.Connection.execution_options`.  When using the psycopg2 dialect
, these
options make use of psycopg2's ``set_isolation_level()`` connection method,
rather than emitting a PostgreSQL directive; this is because psycopg2's
API-level setting is always emitted at the start of each transaction in any
case.

The psycopg2 dialect supports these constants for isolation level:

* ``READ COMMITTED``
* ``READ UNCOMMITTED``
* ``REPEATABLE READ``
* ``SERIALIZABLE``
* ``AUTOCOMMIT``

.. seealso::

    :ref:`postgresql_isolation_level`

    :ref:`pg8000_isolation_level`


NOTICE logging
---------------

The psycopg2 dialect will log PostgreSQL NOTICE messages
via the ``sqlalchemy.dialects.postgresql`` logger.  When this logger
is set to the ``logging.INFO`` level, notice messages will be logged::

    import logging

    logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)

Above, it is assumed that logging is configured externally.  If this is not
the case, configuration such as ``logging.basicConfig()`` must be utilized::

    import logging

    logging.basicConfig()   # log messages to stdout
    logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)

.. seealso::

    `Logging HOWTO <https://docs.python.org/3/howto/logging.html>`_ - on the python.org website

.. _psycopg2_hstore:

HSTORE type
------------

The ``psycopg2`` DBAPI includes an extension to natively handle marshalling of
the HSTORE type.   The SQLAlchemy psycopg2 dialect will enable this extension
by default when psycopg2 version 2.4 or greater is used, and
it is detected that the target database has the HSTORE type set up for use.
In other words, when the dialect makes the first
connection, a sequence like the following is performed:

1. Request the available HSTORE oids using
   ``psycopg2.extras.HstoreAdapter.get_oids()``.
   If this function returns a list of HSTORE identifiers, we then determine
   that the ``HSTORE`` extension is present.
   This function is **skipped** if the version of psycopg2 installed is
   less than version 2.4.

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

"""  # noqa
from __future__ import absolute_import

import decimal
import logging
import re

from .base import _DECIMAL_TYPES
from .base import _FLOAT_TYPES
from .base import _INT_TYPES
from .base import ENUM
from .base import PGCompiler
from .base import PGDialect
from .base import PGExecutionContext
from .base import PGIdentifierPreparer
from .base import UUID
from .hstore import HSTORE
from .json import JSON
from .json import JSONB
from ... import exc
from ... import processors
from ... import types as sqltypes
from ... import util
from ...engine import result as _result
from ...util import collections_abc

try:
    from uuid import UUID as _python_UUID  # noqa
except ImportError:
    _python_UUID = None


logger = logging.getLogger("sqlalchemy.dialects.postgresql")


class _PGNumeric(sqltypes.Numeric):
    def bind_processor(self, dialect):
        return None

    def result_processor(self, dialect, coltype):
        if self.asdecimal:
            if coltype in _FLOAT_TYPES:
                return processors.to_decimal_processor_factory(
                    decimal.Decimal, self._effective_decimal_return_scale
                )
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                # pg8000 returns Decimal natively for 1700
                return None
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype
                )
        else:
            if coltype in _FLOAT_TYPES:
                # pg8000 returns float natively for 701
                return None
            elif coltype in _DECIMAL_TYPES or coltype in _INT_TYPES:
                return processors.to_float
            else:
                raise exc.InvalidRequestError(
                    "Unknown PG numeric type: %d" % coltype
                )


class _PGEnum(ENUM):
    def result_processor(self, dialect, coltype):
        if util.py2k and self._expect_unicode is True:
            # for py2k, if the enum type needs unicode data (which is set up as
            # part of the Enum() constructor based on values passed as py2k
            # unicode objects) we have to use our own converters since
            # psycopg2's don't work, a rare exception to the "modern DBAPIs
            # support unicode everywhere" theme of deprecating
            # convert_unicode=True. Use the special "force_nocheck" directive
            # which forces unicode conversion to happen on the Python side
            # without an isinstance() check.   in py3k psycopg2 does the right
            # thing automatically.
            self._expect_unicode = "force_nocheck"
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


class _PGJSONB(JSONB):
    def result_processor(self, dialect, coltype):
        if dialect._has_native_jsonb:
            return None
        else:
            return super(_PGJSONB, self).result_processor(dialect, coltype)


class _PGUUID(UUID):
    def bind_processor(self, dialect):
        if not self.as_uuid and dialect.use_native_uuid:

            def process(value):
                if value is not None:
                    value = _python_UUID(value)
                return value

            return process

    def result_processor(self, dialect, coltype):
        if not self.as_uuid and dialect.use_native_uuid:

            def process(value):
                if value is not None:
                    value = str(value)
                return value

            return process


_server_side_id = util.counter()


class PGExecutionContext_psycopg2(PGExecutionContext):
    def create_server_side_cursor(self):
        # use server-side cursors:
        # http://lists.initd.org/pipermail/psycopg/2007-January/005251.html
        ident = "c_%s_%s" % (hex(id(self))[2:], hex(_server_side_id())[2:])
        return self._dbapi_connection.cursor(ident)

    def get_result_proxy(self):
        self._log_notices(self.cursor)

        if self._is_server_side:
            return _result.BufferedRowResultProxy(self)
        else:
            return _result.ResultProxy(self)

    def _log_notices(self, cursor):
        # check also that notices is an iterable, after it's already
        # established that we will be iterating through it.  This is to get
        # around test suites such as SQLAlchemy's using a Mock object for
        # cursor
        if not cursor.connection.notices or not isinstance(
            cursor.connection.notices, collections_abc.Iterable
        ):
            return

        for notice in cursor.connection.notices:
            # NOTICE messages have a
            # newline character at the end
            logger.info(notice.rstrip())

        cursor.connection.notices[:] = []


class PGCompiler_psycopg2(PGCompiler):
    pass


class PGIdentifierPreparer_psycopg2(PGIdentifierPreparer):
    pass


EXECUTEMANY_DEFAULT = util.symbol("executemany_default")
EXECUTEMANY_BATCH = util.symbol("executemany_batch")
EXECUTEMANY_VALUES = util.symbol("executemany_values")


class PGDialect_psycopg2(PGDialect):
    driver = "psycopg2"
    if util.py2k:
        supports_unicode_statements = False

    supports_server_side_cursors = True

    default_paramstyle = "pyformat"
    # set to true based on psycopg2 version
    supports_sane_multi_rowcount = False
    execution_ctx_cls = PGExecutionContext_psycopg2
    statement_compiler = PGCompiler_psycopg2
    preparer = PGIdentifierPreparer_psycopg2
    psycopg2_version = (0, 0)

    FEATURE_VERSION_MAP = dict(
        native_json=(2, 5),
        native_jsonb=(2, 5, 4),
        sane_multi_rowcount=(2, 0, 9),
        array_oid=(2, 4, 3),
        hstore_adapter=(2, 4),
    )

    _has_native_hstore = False
    _has_native_json = False
    _has_native_jsonb = False

    engine_config_types = PGDialect.engine_config_types.union(
        [("use_native_unicode", util.asbool)]
    )

    colspecs = util.update_copy(
        PGDialect.colspecs,
        {
            sqltypes.Numeric: _PGNumeric,
            ENUM: _PGEnum,  # needs force_unicode
            sqltypes.Enum: _PGEnum,  # needs force_unicode
            HSTORE: _PGHStore,
            JSON: _PGJSON,
            sqltypes.JSON: _PGJSON,
            JSONB: _PGJSONB,
            UUID: _PGUUID,
        },
    )

    @util.deprecated_params(
        use_batch_mode=(
            "1.3.7",
            "The psycopg2 use_batch_mode flag is superseded by "
            "executemany_mode='batch'",
        )
    )
    def __init__(
        self,
        server_side_cursors=False,
        use_native_unicode=True,
        client_encoding=None,
        use_native_hstore=True,
        use_native_uuid=True,
        executemany_mode=None,
        executemany_batch_page_size=None,
        executemany_values_page_size=None,
        use_batch_mode=None,
        **kwargs
    ):
        PGDialect.__init__(self, **kwargs)
        self.server_side_cursors = server_side_cursors
        self.use_native_unicode = use_native_unicode
        self.use_native_hstore = use_native_hstore
        self.use_native_uuid = use_native_uuid
        self.supports_unicode_binds = use_native_unicode
        self.client_encoding = client_encoding

        # Parse executemany_mode argument, allowing it to be only one of the
        # symbol names
        self.executemany_mode = util.symbol.parse_user_argument(
            executemany_mode,
            {
                EXECUTEMANY_DEFAULT: [None],
                EXECUTEMANY_BATCH: ["batch"],
                EXECUTEMANY_VALUES: ["values"],
            },
            "executemany_mode",
        )
        if use_batch_mode:
            self.executemany_mode = EXECUTEMANY_BATCH

        self.executemany_batch_page_size = executemany_batch_page_size
        self.executemany_values_page_size = executemany_values_page_size

        if self.dbapi and hasattr(self.dbapi, "__version__"):
            m = re.match(r"(\d+)\.(\d+)(?:\.(\d+))?", self.dbapi.__version__)
            if m:
                self.psycopg2_version = tuple(
                    int(x) for x in m.group(1, 2, 3) if x is not None
                )

    def initialize(self, connection):
        super(PGDialect_psycopg2, self).initialize(connection)
        self._has_native_hstore = (
            self.use_native_hstore
            and self._hstore_oids(connection.connection) is not None
        )
        self._has_native_json = (
            self.psycopg2_version >= self.FEATURE_VERSION_MAP["native_json"]
        )
        self._has_native_jsonb = (
            self.psycopg2_version >= self.FEATURE_VERSION_MAP["native_jsonb"]
        )

        # http://initd.org/psycopg/docs/news.html#what-s-new-in-psycopg-2-0-9
        self.supports_sane_multi_rowcount = (
            self.psycopg2_version
            >= self.FEATURE_VERSION_MAP["sane_multi_rowcount"]
            and self.executemany_mode is EXECUTEMANY_DEFAULT
        )

    @classmethod
    def dbapi(cls):
        import psycopg2

        return psycopg2

    @classmethod
    def _psycopg2_extensions(cls):
        from psycopg2 import extensions

        return extensions

    @classmethod
    def _psycopg2_extras(cls):
        from psycopg2 import extras

        return extras

    @util.memoized_property
    def _isolation_lookup(self):
        extensions = self._psycopg2_extensions()
        return {
            "AUTOCOMMIT": extensions.ISOLATION_LEVEL_AUTOCOMMIT,
            "READ COMMITTED": extensions.ISOLATION_LEVEL_READ_COMMITTED,
            "READ UNCOMMITTED": extensions.ISOLATION_LEVEL_READ_UNCOMMITTED,
            "REPEATABLE READ": extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            "SERIALIZABLE": extensions.ISOLATION_LEVEL_SERIALIZABLE,
        }

    def set_isolation_level(self, connection, level):
        try:
            level = self._isolation_lookup[level.replace("_", " ")]
        except KeyError as err:
            util.raise_(
                exc.ArgumentError(
                    "Invalid value '%s' for isolation_level. "
                    "Valid isolation levels for %s are %s"
                    % (level, self.name, ", ".join(self._isolation_lookup))
                ),
                replace_context=err,
            )

        connection.set_isolation_level(level)

    def on_connect(self):
        extras = self._psycopg2_extras()
        extensions = self._psycopg2_extensions()

        fns = []
        if self.client_encoding is not None:

            def on_connect(conn):
                conn.set_client_encoding(self.client_encoding)

            fns.append(on_connect)

        if self.isolation_level is not None:

            def on_connect(conn):
                self.set_isolation_level(conn, self.isolation_level)

            fns.append(on_connect)

        if self.dbapi and self.use_native_uuid:

            def on_connect(conn):
                extras.register_uuid(None, conn)

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
                    kw = {"oid": oid}
                    if util.py2k:
                        kw["unicode"] = True
                    if (
                        self.psycopg2_version
                        >= self.FEATURE_VERSION_MAP["array_oid"]
                    ):
                        kw["array_oid"] = array_oid
                    extras.register_hstore(conn, **kw)

            fns.append(on_connect)

        if self.dbapi and self._json_deserializer:

            def on_connect(conn):
                if self._has_native_json:
                    extras.register_default_json(
                        conn, loads=self._json_deserializer
                    )
                if self._has_native_jsonb:
                    extras.register_default_jsonb(
                        conn, loads=self._json_deserializer
                    )

            fns.append(on_connect)

        if fns:

            def on_connect(conn):
                for fn in fns:
                    fn(conn)

            return on_connect
        else:
            return None

    def do_executemany(self, cursor, statement, parameters, context=None):
        if self.executemany_mode is EXECUTEMANY_DEFAULT:
            cursor.executemany(statement, parameters)
            return

        if (
            self.executemany_mode is EXECUTEMANY_VALUES
            and context
            and context.isinsert
            and context.compiled.insert_single_values_expr
        ):
            executemany_values = (
                "(%s)" % context.compiled.insert_single_values_expr
            )
            # guard for statement that was altered via event hook or similar
            if executemany_values not in statement:
                executemany_values = None
        else:
            executemany_values = None

        if executemany_values:
            # Currently, SQLAlchemy does not pass "RETURNING" statements
            # into executemany(), since no DBAPI has ever supported that
            # until the introduction of psycopg2's executemany_values, so
            # we are not yet using the fetch=True flag.
            statement = statement.replace(executemany_values, "%s")
            if self.executemany_values_page_size:
                kwargs = {"page_size": self.executemany_values_page_size}
            else:
                kwargs = {}
            self._psycopg2_extras().execute_values(
                cursor,
                statement,
                parameters,
                template=executemany_values,
                **kwargs
            )

        else:
            if self.executemany_batch_page_size:
                kwargs = {"page_size": self.executemany_batch_page_size}
            else:
                kwargs = {}
            self._psycopg2_extras().execute_batch(
                cursor, statement, parameters, **kwargs
            )

    @util.memoized_instancemethod
    def _hstore_oids(self, conn):
        if self.psycopg2_version >= self.FEATURE_VERSION_MAP["hstore_adapter"]:
            extras = self._psycopg2_extras()
            oids = extras.HstoreAdapter.get_oids(conn)
            if oids is not None and oids[0]:
                return oids[0:2]
        return None

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user")

        is_multihost = False
        if "host" in url.query:
            is_multihost = isinstance(url.query["host"], (list, tuple))

        if opts:
            if "port" in opts:
                opts["port"] = int(opts["port"])
            opts.update(url.query)
            if is_multihost:
                opts["host"] = ",".join(url.query["host"])
            # send individual dbname, user, password, host, port
            # parameters to psycopg2.connect()
            return ([], opts)
        elif url.query:
            # any other connection arguments, pass directly
            opts.update(url.query)
            if is_multihost:
                opts["host"] = ",".join(url.query["host"])
            return ([], opts)
        else:
            # no connection arguments whatsoever; psycopg2.connect()
            # requires that "dsn" be present as a blank string.
            return ([""], opts)

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.Error):
            # check the "closed" flag.  this might not be
            # present on old psycopg2 versions.   Also,
            # this flag doesn't actually help in a lot of disconnect
            # situations, so don't rely on it.
            if getattr(connection, "closed", False):
                return True

            # checks based on strings.  in the case that .closed
            # didn't cut it, fall back onto these.
            str_e = str(e).partition("\n")[0]
            for msg in [
                # these error messages from libpq: interfaces/libpq/fe-misc.c
                # and interfaces/libpq/fe-secure.c.
                "terminating connection",
                "closed the connection",
                "connection not open",
                "could not receive data from server",
                "could not send data to server",
                # psycopg2 client errors, psycopg2/conenction.h,
                # psycopg2/cursor.h
                "connection already closed",
                "cursor already closed",
                # not sure where this path is originally from, it may
                # be obsolete.   It really says "losed", not "closed".
                "losed the connection unexpectedly",
                # these can occur in newer SSL
                "connection has been closed unexpectedly",
                "SSL SYSCALL error: Bad file descriptor",
                "SSL SYSCALL error: EOF detected",
                "SSL error: decryption failed or bad record mac",
                "SSL SYSCALL error: Operation timed out",
            ]:
                idx = str_e.find(msg)
                if idx >= 0 and '"' not in str_e[:idx]:
                    return True
        return False


dialect = PGDialect_psycopg2
