# engine/__init__.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQL connections, SQL execution and high-level DB-API interface.

The engine package defines the basic components used to interface
DB-API modules with higher-level statement construction,
connection-management, execution and result contexts.  The primary
"entry point" class into this package is the Engine and it's public
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

import sqlalchemy.databases
from sqlalchemy.engine.base import (
    BufferedColumnResultProxy,
    BufferedColumnRow,
    BufferedRowResultProxy,
    Compiled,
    Connectable,
    Connection,
    DefaultRunner,
    Dialect,
    Engine,
    ExecutionContext,
    NestedTransaction,
    ResultProxy,
    RootTransaction,
    RowProxy,
    SchemaIterator,
    Transaction,
    TwoPhaseTransaction
    )
from sqlalchemy.engine import strategies
from sqlalchemy import util


__all__ = (
    'BufferedColumnResultProxy',
    'BufferedColumnRow',
    'BufferedRowResultProxy',
    'Compiled',
    'Connectable',
    'Connection',
    'DefaultRunner',
    'Dialect',
    'Engine',
    'ExecutionContext',
    'NestedTransaction',
    'ResultProxy',
    'RootTransaction',
    'RowProxy',
    'SchemaIterator',
    'Transaction',
    'TwoPhaseTransaction',
    'create_engine',
    'engine_from_config',
    )


default_strategy = 'plain'
def create_engine(*args, **kwargs):
    """Create a new Engine instance.

    The standard method of specifying the engine is via URL as the
    first positional argument, to indicate the appropriate database
    dialect and connection arguments, with additional keyword
    arguments sent as options to the dialect and resulting Engine.

    The URL is a string in the form
    ``dialect://user:password@host/dbname[?key=value..]``, where
    ``dialect`` is a name such as ``mysql``, ``oracle``, ``postgres``,
    etc.  Alternatively, the URL can be an instance of
    :class:`~sqlalchemy.engine.url.URL`.

    `**kwargs` takes a wide variety of options which are routed 
    towards their appropriate components.  Arguments may be 
    specific to the Engine, the underlying Dialect, as well as the 
    Pool.  Specific dialects also accept keyword arguments that
    are unique to that dialect.   Here, we describe the parameters
    that are common to most ``create_engine()`` usage.
    
    :param assert_unicode=False: When set to ``True`` alongside
        convert_unicode=``True``, asserts that incoming string bind
        parameters are instances of ``unicode``, otherwise raises an
        error. Only takes effect when ``convert_unicode==True``. This
        flag is also available on the ``String`` type and its
        descendants. New in 0.4.2.

    :param connect_args: a dictionary of options which will be
        passed directly to the DBAPI's ``connect()`` method as
        additional keyword arguments.

    :param convert_unicode=False: if set to True, all
        String/character based types will convert Unicode values to raw
        byte values going into the database, and all raw byte values to
        Python Unicode coming out in result sets. This is an
        engine-wide method to provide unicode conversion across the
        board. For unicode conversion on a column-by-column level, use
        the ``Unicode`` column type instead, described in `types`.

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
        controls a Python logger; see `dbengine_logging` at the end of
        this chapter for information on how to configure logging
        directly.

    :param echo_pool=False: if True, the connection pool will log
        all checkouts/checkins to the logging stream, which defaults to
        sys.stdout. This flag ultimately controls a Python logger; see
        `dbengine_logging` for information on how to configure logging
        directly.

    :param encoding='utf-8': the encoding to use for all Unicode
        translations, both by engine-wide unicode conversion as well as
        the ``Unicode`` type object.

    :param label_length=None: optional integer value which limits
        the size of dynamically generated column labels to that many
        characters. If less than 6, labels are generated as
        "_(counter)". If ``None``, the value of
        ``dialect.max_identifier_length`` is used instead.

    :param module=None: used by database implementations which
        support multiple DBAPI modules, this is a reference to a DBAPI2
        module to be used instead of the engine's default module. For
        PostgreSQL, the default is psycopg2. For Oracle, it's cx_Oracle.

    :param pool=None: an already-constructed instance of
        :class:`~sqlalchemy.pool.Pool`, such as a
        :class:`~sqlalchemy.pool.QueuePool` instance. If non-None, this
        pool will be used directly as the underlying connection pool
        for the engine, bypassing whatever connection parameters are
        present in the URL argument. For information on constructing
        connection pools manually, see `pooling`.

    :param poolclass=None: a :class:`~sqlalchemy.pool.Pool`
        subclass, which will be used to create a connection pool
        instance using the connection parameters given in the URL. Note
        this differs from ``pool`` in that you don't actually
        instantiate the pool in this case, you just indicate what type
        of pool to be used.

    :param max_overflow=10: the number of connections to allow in
        connection pool "overflow", that is connections that can be
        opened above and beyond the pool_size setting, which defaults
        to five. this is only used with :class:`~sqlalchemy.pool.QueuePool`.

    :param pool_size=5: the number of connections to keep open
        inside the connection pool. This used with :class:`~sqlalchemy.pool.QueuePool` as
        well as :class:`~sqlalchemy.pool.SingletonThreadPool`.

    :param pool_recycle=-1: this setting causes the pool to recycle
        connections after the given number of seconds has passed. It
        defaults to -1, or no timeout. For example, setting to 3600
        means connections will be recycled after one hour. Note that
        MySQL in particular will ``disconnect automatically`` if no
        activity is detected on a connection for eight hours (although
        this is configurable with the MySQLDB connection itself and the
        server configuration as well).

    :param pool_timeout=30: number of seconds to wait before giving
        up on getting a connection from the pool. This is only used
        with :class:`~sqlalchemy.pool.QueuePool`.

    :param strategy='plain': used to invoke alternate :class:`~sqlalchemy.engine.base.Engine.`
        implementations. Currently available is the ``threadlocal``
        strategy, which is described in :ref:`threadlocal_strategy`.
    
    """

    strategy = kwargs.pop('strategy', default_strategy)
    strategy = strategies.strategies[strategy]
    return strategy.create(*args, **kwargs)

def engine_from_config(configuration, prefix='sqlalchemy.', **kwargs):
    """Create a new Engine instance using a configuration dictionary.

    The dictionary is typically produced from a config file where keys
    are prefixed, such as sqlalchemy.url, sqlalchemy.echo, etc.  The
    'prefix' argument indicates the prefix to be searched for.

    A select set of keyword arguments will be "coerced" to their
    expected type based on string values.  In a future release, this
    functionality will be expanded and include dialect-specific
    arguments.
    """

    opts = _coerce_config(configuration, prefix)
    opts.update(kwargs)
    url = opts.pop('url')
    return create_engine(url, **opts)

def _coerce_config(configuration, prefix):
    """Convert configuration values to expected types."""

    options = dict((key[len(prefix):], configuration[key])
                   for key in configuration
                   if key.startswith(prefix))
    for option, type_ in (
        ('convert_unicode', bool),
        ('pool_timeout', int),
        ('echo', bool),
        ('echo_pool', bool),
        ('pool_recycle', int),
        ('pool_size', int),
        ('max_overflow', int),
        ('pool_threadlocal', bool),
    ):
        util.coerce_kw_type(options, option, type_)
    return options
