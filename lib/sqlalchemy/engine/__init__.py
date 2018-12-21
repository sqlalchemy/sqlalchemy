# engine/__init__.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQL connections, SQL execution and high-level DB-API interface.

The engine package defines the basic components used to interface
DB-API modules with higher-level statement construction,
connection-management, execution and result contexts.  The primary
"entry point" class into this package is the Engine and its public
constructor ``create_engine()``.

This package includes:

base.py
    Defines interface classes and some implementation classes which
    comprise the basic components used to interface between a DB-API,
    constructed and plain-text statements, connections, transactions,
    and results.

default.py
    Contains default implementations of some of the components defined
    in base.py.  All current database dialects use the classes in
    default.py as base classes for their own database-specific
    implementations.

strategies.py
    The mechanics of constructing ``Engine`` objects are represented
    here.  Defines the ``EngineStrategy`` class which represents how
    to go from arguments specified to the ``create_engine()``
    function, to a fully constructed ``Engine``, including
    initialization of connection pooling, dialects, and specific
    subclasses of ``Engine``.

threadlocal.py
    The ``TLEngine`` class is defined here, which is a subclass of
    the generic ``Engine`` and tracks ``Connection`` and
    ``Transaction`` objects against the identity of the current
    thread.  This allows certain programming patterns based around
    the concept of a "thread-local connection" to be possible.
    The ``TLEngine`` is created by using the "threadlocal" engine
    strategy in conjunction with the ``create_engine()`` function.

url.py
    Defines the ``URL`` class which represents the individual
    components of a string URL passed to ``create_engine()``.  Also
    defines a basic module-loading strategy for the dialect specifier
    within a URL.
"""

from . import strategies
from . import util  # noqa
from .base import Connection  # noqa
from .base import Engine  # noqa
from .base import NestedTransaction  # noqa
from .base import RootTransaction  # noqa
from .base import Transaction  # noqa
from .base import TwoPhaseTransaction  # noqa
from .interfaces import Compiled  # noqa
from .interfaces import Connectable  # noqa
from .interfaces import CreateEnginePlugin  # noqa
from .interfaces import Dialect  # noqa
from .interfaces import ExceptionContext  # noqa
from .interfaces import ExecutionContext  # noqa
from .interfaces import TypeCompiler  # noqa
from .result import BaseRowProxy  # noqa
from .result import BufferedColumnResultProxy  # noqa
from .result import BufferedColumnRow  # noqa
from .result import BufferedRowResultProxy  # noqa
from .result import FullyBufferedResultProxy  # noqa
from .result import ResultProxy  # noqa
from .result import RowProxy  # noqa
from .util import connection_memoize  # noqa
from ..sql import ddl  # noqa


# backwards compat

default_strategy = "plain"


def create_engine(*args, **kwargs):
    """Create a new :class:`.Engine` instance.

    The standard calling form is to send the URL as the
    first positional argument, usually a string
    that indicates database dialect and connection arguments::


        engine = create_engine("postgresql://scott:tiger@localhost/test")

    Additional keyword arguments may then follow it which
    establish various options on the resulting :class:`.Engine`
    and its underlying :class:`.Dialect` and :class:`.Pool`
    constructs::

        engine = create_engine("mysql://scott:tiger@hostname/dbname",
                                    encoding='latin1', echo=True)

    The string form of the URL is
    ``dialect[+driver]://user:password@host/dbname[?key=value..]``, where
    ``dialect`` is a database name such as ``mysql``, ``oracle``,
    ``postgresql``, etc., and ``driver`` the name of a DBAPI, such as
    ``psycopg2``, ``pyodbc``, ``cx_oracle``, etc.  Alternatively,
    the URL can be an instance of :class:`~sqlalchemy.engine.url.URL`.

    ``**kwargs`` takes a wide variety of options which are routed
    towards their appropriate components.  Arguments may be specific to
    the :class:`.Engine`, the underlying :class:`.Dialect`, as well as the
    :class:`.Pool`.  Specific dialects also accept keyword arguments that
    are unique to that dialect.   Here, we describe the parameters
    that are common to most :func:`.create_engine()` usage.

    Once established, the newly resulting :class:`.Engine` will
    request a connection from the underlying :class:`.Pool` once
    :meth:`.Engine.connect` is called, or a method which depends on it
    such as :meth:`.Engine.execute` is invoked.   The :class:`.Pool` in turn
    will establish the first actual DBAPI connection when this request
    is received.   The :func:`.create_engine` call itself does **not**
    establish any actual DBAPI connections directly.

    .. seealso::

        :doc:`/core/engines`

        :doc:`/dialects/index`

        :ref:`connections_toplevel`

    :param case_sensitive=True: if False, result column names
       will match in a case-insensitive fashion, that is,
       ``row['SomeColumn']``.

    :param connect_args: a dictionary of options which will be
        passed directly to the DBAPI's ``connect()`` method as
        additional keyword arguments.  See the example
        at :ref:`custom_dbapi_args`.

    :param convert_unicode=False: if set to True, causes
        all :class:`.String` datatypes to act as though the
        :paramref:`.String.convert_unicode` flag has been set to ``True``,
        regardless of a setting of ``False`` on an individual :class:`.String`
        type.  This has the effect of causing all :class:`.String` -based
        columns to accommodate Python Unicode objects directly as though the
        datatype were the :class:`.Unicode` type.

          .. deprecated:: The :paramref:`.create_engine.convert_unicode` flag
             and related Unicode conversion features are legacy Python 2
             mechanisms which no longer have relevance under Python 3.
             As all modern DBAPIs now support Python Unicode fully even
             under Python 2, these flags will be removed in an upcoming
             release.

        .. note::

            SQLAlchemy's unicode-conversion flags and features only apply
            to Python 2; in Python 3, all string objects are Unicode objects.
            For this reason, as well as the fact that virtually all modern
            DBAPIs now support Unicode natively even under Python 2,
            the :paramref:`.Engine.convert_unicode` flag is inherently a
            legacy feature.

        .. note::

            This flag does **not** imply that SQLAlchemy's unicode-conversion
            services will be used, as all modern DBAPIs already handle
            unicode natively; in most cases it only indicates that the
            :class:`.String` datatype will return Python unicode objects,
            rather than plain strings.   The :class:`.String` datatype itself
            has additional options to force the usage of SQLAlchemy's unicode
            converters.

        .. note::

            This flag does **not** impact "raw" SQL statements that have no
            typing information set up; that is, if the :class:`.String`
            datatype is not used, no unicode behavior is implied.

        .. seealso::

            :paramref:`.String.convert_unicode` - the flag local to the
            :class:`.String` datatype has additional options
            which can force unicode handling on a per-type basis.


    :param creator: a callable which returns a DBAPI connection.
        This creation function will be passed to the underlying
        connection pool and will be used to create all new database
        connections. Usage of this function causes connection
        parameters specified in the URL argument to be bypassed.

    :param echo=False: if True, the Engine will log all statements
        as well as a repr() of their parameter lists to the engines
        logger, which defaults to sys.stdout. The ``echo`` attribute of
        ``Engine`` can be modified at any time to turn logging on and
        off. If set to the string ``"debug"``, result rows will be
        printed to the standard output as well. This flag ultimately
        controls a Python logger; see :ref:`dbengine_logging` for
        information on how to configure logging directly.

    :param echo_pool=False: if True, the connection pool will log
        all checkouts/checkins to the logging stream, which defaults to
        sys.stdout. This flag ultimately controls a Python logger; see
        :ref:`dbengine_logging` for information on how to configure logging
        directly.

    :param empty_in_strategy:  The SQL compilation strategy to use when
        rendering an IN or NOT IN expression for :meth:`.ColumnOperators.in_`
        where the right-hand side
        is an empty set.   This is a string value that may be one of
        ``static``, ``dynamic``, or ``dynamic_warn``.   The ``static``
        strategy is the default, and an IN comparison to an empty set
        will generate a simple false expression "1 != 1".   The ``dynamic``
        strategy behaves like that of SQLAlchemy 1.1 and earlier, emitting
        a false expression of the form "expr != expr", which has the effect
        of evaluting to NULL in the case of a null expression.
        ``dynamic_warn`` is the same as ``dynamic``, however also emits a
        warning when an empty set is encountered; this because the "dynamic"
        comparison is typically poorly performing on most databases.

        .. versionadded:: 1.2  Added the ``empty_in_strategy`` setting and
           additionally defaulted the behavior for empty-set IN comparisons
           to a static boolean expression.

    :param encoding: Defaults to ``utf-8``.  This is the string
        encoding used by SQLAlchemy for string encode/decode
        operations which occur within SQLAlchemy, **outside of
        the DBAPI.**  Most modern DBAPIs feature some degree of
        direct support for Python ``unicode`` objects,
        what you see in Python 2 as a string of the form
        ``u'some string'``.  For those scenarios where the
        DBAPI is detected as not supporting a Python ``unicode``
        object, this encoding is used to determine the
        source/destination encoding.  It is **not used**
        for those cases where the DBAPI handles unicode
        directly.

        To properly configure a system to accommodate Python
        ``unicode`` objects, the DBAPI should be
        configured to handle unicode to the greatest
        degree as is appropriate - see
        the notes on unicode pertaining to the specific
        target database in use at :ref:`dialect_toplevel`.

        Areas where string encoding may need to be accommodated
        outside of the DBAPI include zero or more of:

        * the values passed to bound parameters, corresponding to
          the :class:`.Unicode` type or the :class:`.String` type
          when ``convert_unicode`` is ``True``;
        * the values returned in result set columns corresponding
          to the :class:`.Unicode` type or the :class:`.String`
          type when ``convert_unicode`` is ``True``;
        * the string SQL statement passed to the DBAPI's
          ``cursor.execute()`` method;
        * the string names of the keys in the bound parameter
          dictionary passed to the DBAPI's ``cursor.execute()``
          as well as ``cursor.setinputsizes()`` methods;
        * the string column names retrieved from the DBAPI's
          ``cursor.description`` attribute.

        When using Python 3, the DBAPI is required to support
        *all* of the above values as Python ``unicode`` objects,
        which in Python 3 are just known as ``str``.  In Python 2,
        the DBAPI does not specify unicode behavior at all,
        so SQLAlchemy must make decisions for each of the above
        values on a per-DBAPI basis - implementations are
        completely inconsistent in their behavior.

    :param execution_options: Dictionary execution options which will
        be applied to all connections.  See
        :meth:`~sqlalchemy.engine.Connection.execution_options`

    :param implicit_returning=True: When ``True``, a RETURNING-
        compatible construct, if available, will be used to
        fetch newly generated primary key values when a single row
        INSERT statement is emitted with no existing returning()
        clause.  This applies to those backends which support RETURNING
        or a compatible construct, including PostgreSQL, Firebird, Oracle,
        Microsoft SQL Server.   Set this to ``False`` to disable
        the automatic usage of RETURNING.

    :param isolation_level: this string parameter is interpreted by various
        dialects in order to affect the transaction isolation level of the
        database connection.   The parameter essentially accepts some subset of
        these string arguments: ``"SERIALIZABLE"``, ``"REPEATABLE_READ"``,
        ``"READ_COMMITTED"``, ``"READ_UNCOMMITTED"`` and ``"AUTOCOMMIT"``.
        Behavior here varies per backend, and
        individual dialects should be consulted directly.

        Note that the isolation level can also be set on a
        per-:class:`.Connection` basis as well, using the
        :paramref:`.Connection.execution_options.isolation_level`
        feature.

        .. seealso::

            :attr:`.Connection.default_isolation_level` - view default level

            :paramref:`.Connection.execution_options.isolation_level`
            - set per :class:`.Connection` isolation level

            :ref:`SQLite Transaction Isolation <sqlite_isolation_level>`

            :ref:`PostgreSQL Transaction Isolation <postgresql_isolation_level>`

            :ref:`MySQL Transaction Isolation <mysql_isolation_level>`

            :ref:`session_transaction_isolation` - for the ORM

    :param label_length=None: optional integer value which limits
        the size of dynamically generated column labels to that many
        characters. If less than 6, labels are generated as
        "_(counter)". If ``None``, the value of
        ``dialect.max_identifier_length`` is used instead.

    :param listeners: A list of one or more
        :class:`~sqlalchemy.interfaces.PoolListener` objects which will
        receive connection pool events.

    :param logging_name:  String identifier which will be used within
        the "name" field of logging records generated within the
        "sqlalchemy.engine" logger. Defaults to a hexstring of the
        object's id.

    :param max_overflow=10: the number of connections to allow in
        connection pool "overflow", that is connections that can be
        opened above and beyond the pool_size setting, which defaults
        to five. this is only used with :class:`~sqlalchemy.pool.QueuePool`.

    :param module=None: reference to a Python module object (the module
        itself, not its string name).  Specifies an alternate DBAPI module to
        be used by the engine's dialect.  Each sub-dialect references a
        specific DBAPI which will be imported before first connect.  This
        parameter causes the import to be bypassed, and the given module to
        be used instead. Can be used for testing of DBAPIs as well as to
        inject "mock" DBAPI implementations into the :class:`.Engine`.

    :param paramstyle=None: The `paramstyle <http://legacy.python.org/dev/peps/pep-0249/#paramstyle>`_
        to use when rendering bound parameters.  This style defaults to the
        one recommended by the DBAPI itself, which is retrieved from the
        ``.paramstyle`` attribute of the DBAPI.  However, most DBAPIs accept
        more than one paramstyle, and in particular it may be desirable
        to change a "named" paramstyle into a "positional" one, or vice versa.
        When this attribute is passed, it should be one of the values
        ``"qmark"``, ``"numeric"``, ``"named"``, ``"format"`` or
        ``"pyformat"``, and should correspond to a parameter style known
        to be supported by the DBAPI in use.

    :param pool=None: an already-constructed instance of
        :class:`~sqlalchemy.pool.Pool`, such as a
        :class:`~sqlalchemy.pool.QueuePool` instance. If non-None, this
        pool will be used directly as the underlying connection pool
        for the engine, bypassing whatever connection parameters are
        present in the URL argument. For information on constructing
        connection pools manually, see :ref:`pooling_toplevel`.

    :param poolclass=None: a :class:`~sqlalchemy.pool.Pool`
        subclass, which will be used to create a connection pool
        instance using the connection parameters given in the URL. Note
        this differs from ``pool`` in that you don't actually
        instantiate the pool in this case, you just indicate what type
        of pool to be used.

    :param pool_logging_name:  String identifier which will be used within
       the "name" field of logging records generated within the
       "sqlalchemy.pool" logger. Defaults to a hexstring of the object's
       id.

    :param pool_pre_ping: boolean, if True will enable the connection pool
        "pre-ping" feature that tests connections for liveness upon
        each checkout.

        .. versionadded:: 1.2

        .. seealso::

            :ref:`pool_disconnects_pessimistic`

    :param pool_size=5: the number of connections to keep open
        inside the connection pool. This used with
        :class:`~sqlalchemy.pool.QueuePool` as
        well as :class:`~sqlalchemy.pool.SingletonThreadPool`.  With
        :class:`~sqlalchemy.pool.QueuePool`, a ``pool_size`` setting
        of 0 indicates no limit; to disable pooling, set ``poolclass`` to
        :class:`~sqlalchemy.pool.NullPool` instead.

    :param pool_recycle=-1: this setting causes the pool to recycle
        connections after the given number of seconds has passed. It
        defaults to -1, or no timeout. For example, setting to 3600
        means connections will be recycled after one hour. Note that
        MySQL in particular will disconnect automatically if no
        activity is detected on a connection for eight hours (although
        this is configurable with the MySQLDB connection itself and the
        server configuration as well).

        .. seealso::

            :ref:`pool_setting_recycle`

    :param pool_reset_on_return='rollback': set the
        :paramref:`.Pool.reset_on_return` parameter of the underlying
        :class:`.Pool` object, which can be set to the values
        ``"rollback"``, ``"commit"``, or ``None``.

        .. seealso::

            :paramref:`.Pool.reset_on_return`

    :param pool_timeout=30: number of seconds to wait before giving
        up on getting a connection from the pool. This is only used
        with :class:`~sqlalchemy.pool.QueuePool`.

    :param pool_use_lifo=False: use LIFO (last-in-first-out) when retrieving
        connections from :class:`.QueuePool` instead of FIFO
        (first-in-first-out). Using LIFO, a server-side timeout scheme can
        reduce the number of connections used during non- peak   periods of
        use.   When planning for server-side timeouts, ensure that a recycle or
        pre-ping strategy is in use to gracefully   handle stale connections.

          .. versionadded:: 1.3

          .. seealso::

            :ref:`pool_use_lifo`

            :ref:`pool_disconnects`

    :param plugins: string list of plugin names to load.  See
        :class:`.CreateEnginePlugin` for background.

        .. versionadded:: 1.2.3

    :param strategy='plain': selects alternate engine implementations.
        Currently available are:

        * the ``threadlocal`` strategy, which is described in
          :ref:`threadlocal_strategy`;
        * the ``mock`` strategy, which dispatches all statement
          execution to a function passed as the argument ``executor``.
          See `example in the FAQ
          <http://docs.sqlalchemy.org/en/latest/faq/metadata_schema.html#how-can-i-get-the-create-table-drop-table-output-as-a-string>`_.

    :param executor=None: a function taking arguments
        ``(sql, *multiparams, **params)``, to which the ``mock`` strategy will
        dispatch all statement execution. Used only by ``strategy='mock'``.

    """  # noqa

    strategy = kwargs.pop("strategy", default_strategy)
    strategy = strategies.strategies[strategy]
    return strategy.create(*args, **kwargs)


def engine_from_config(configuration, prefix="sqlalchemy.", **kwargs):
    """Create a new Engine instance using a configuration dictionary.

    The dictionary is typically produced from a config file.

    The keys of interest to ``engine_from_config()`` should be prefixed, e.g.
    ``sqlalchemy.url``, ``sqlalchemy.echo``, etc.  The 'prefix' argument
    indicates the prefix to be searched for.  Each matching key (after the
    prefix is stripped) is treated as though it were the corresponding keyword
    argument to a :func:`.create_engine` call.

    The only required key is (assuming the default prefix) ``sqlalchemy.url``,
    which provides the :ref:`database URL <database_urls>`.

    A select set of keyword arguments will be "coerced" to their
    expected type based on string values.    The set of arguments
    is extensible per-dialect using the ``engine_config_types`` accessor.

    :param configuration: A dictionary (typically produced from a config file,
        but this is not a requirement).  Items whose keys start with the value
        of 'prefix' will have that prefix stripped, and will then be passed to
        :ref:`create_engine`.

    :param prefix: Prefix to match and then strip from keys
        in 'configuration'.

    :param kwargs: Each keyword argument to ``engine_from_config()`` itself
        overrides the corresponding item taken from the 'configuration'
        dictionary.  Keyword arguments should *not* be prefixed.

    """

    options = dict(
        (key[len(prefix) :], configuration[key])
        for key in configuration
        if key.startswith(prefix)
    )
    options["_coerce_config"] = True
    options.update(kwargs)
    url = options.pop("url")
    return create_engine(url, **options)


__all__ = ("create_engine", "engine_from_config")
