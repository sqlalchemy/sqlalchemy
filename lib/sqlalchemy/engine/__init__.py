# engine/__init__.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
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
from sqlalchemy.engine.base import Dialect, ExecutionContext, Compiled, \
     Connectable, Connection, Transaction, RootTransaction, \
     NestedTransaction, TwoPhaseTransaction, Engine, RowProxy, \
     BufferedColumnRow, ResultProxy, BufferedRowResultProxy, \
     BufferedColumnResultProxy, SchemaIterator, DefaultRunner
from sqlalchemy.engine import strategies
from sqlalchemy import util


__all__ = [
    'engine_descriptors', 'create_engine', 'engine_from_config',
    'Dialect', 'ExecutionContext', 'Compiled', 'Connectable',
    'Connection', 'Transaction', 'RootTransaction', 'NestedTransaction',
    'TwoPhaseTransaction', 'Engine', 'RowProxy', 'BufferedColumnRow',
    'ResultProxy', 'BufferedRowResultProxy', 'BufferedColumnResultProxy',
    'SchemaIterator', 'DefaultRunner',
    ]

def engine_descriptors():
    """Provide a listing of all the database implementations supported.
    
    This method will be removed in 0.5.

    """
    result = []
    for module in sqlalchemy.databases.__all__:
        module = getattr(
            __import__('sqlalchemy.databases.%s' % module).databases, module)
        result.append(module.descriptor())
    return result
engine_descriptors = util.deprecated()(engine_descriptors)


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
    ``sqlalchemy.engine.url.URL``.

    `**kwargs` represents options to be sent to the Engine itself as
    well as the components of the Engine, including the Dialect, the
    ConnectionProvider, and the Pool.  A list of common options is as
    follows:

    poolclass
      a subclass of ``sqlalchemy.pool.Pool`` which will be used to
      instantiate a connection pool.

    pool
      an instance of ``sqlalchemy.pool.DBProxy`` or
      ``sqlalchemy.pool.Pool`` to be used as the underlying source for
      connections (DBProxy/Pool is described in the previous section).
      This argument supercedes "poolclass".

    echo
      defaults to False: if True, the Engine will log all statements
      as well as a repr() of their parameter lists to the engines
      logger, which defaults to ``sys.stdout``.  A Engine instances'
      `echo` data member can be modified at any time to turn logging
      on and off.  If set to the string 'debug', result rows will be
      printed to the standard output as well.

    logger
      defaults to None: a file-like object where logging output can be
      sent, if `echo` is set to True.  This defaults to
      ``sys.stdout``.

    encoding
      defaults to 'utf-8': the encoding to be used when
      encoding/decoding Unicode strings.

    convert_unicode
      defaults to False: true if unicode conversion should be applied
      to all str types.

    module
      defaults to None: this is a reference to a DB-API 2.0 module to
      be used instead of the dialect's default module.

    strategy
      allows alternate Engine implementations to take effect.  Current
      implementations include ``plain`` and ``threadlocal``.  The
      default used by this function is ``plain``.

      ``plain`` provides support for a Connection object which can be
      used to execute SQL queries with a specific underlying DB-API
      connection.

      ``threadlocal`` is similar to ``plain`` except that it adds
      support for a thread-local connection and transaction context,
      which allows a group of engine operations to participate using
      the same underlying connection and transaction without the need
      for explicitly passing a single Connection.
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

    options = dict([(key[len(prefix):], configuration[key])
                 for key in configuration if key.startswith(prefix)])
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
