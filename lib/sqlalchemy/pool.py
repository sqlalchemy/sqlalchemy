# sqlalchemy/pool.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Connection pooling for DB-API connections.

Provides a number of connection pool implementations for a variety of
usage scenarios and thread behavior requirements imposed by the
application, DB-API or database itself.

Also provides a DB-API 2.0 connection proxying mechanism allowing
regular DB-API connect() methods to be transparently managed by a
SQLAlchemy connection pool.
"""

import time
import traceback
import weakref

from . import exc, log, event, interfaces, util
from .util import queue as sqla_queue
from .util import threading, memoized_property, \
    chop_traceback

from collections import deque
proxies = {}


def manage(module, **params):
    """Return a proxy for a DB-API module that automatically
    pools connections.

    Given a DB-API 2.0 module and pool management parameters, returns
    a proxy for the module that will automatically pool connections,
    creating new connection pools for each distinct set of connection
    arguments sent to the decorated module's connect() function.

    :param module: a DB-API 2.0 database module

    :param poolclass: the class used by the pool module to provide
      pooling.  Defaults to :class:`.QueuePool`.

    :param \*\*params: will be passed through to *poolclass*

    """
    try:
        return proxies[module]
    except KeyError:
        return proxies.setdefault(module, _DBProxy(module, **params))


def clear_managers():
    """Remove all current DB-API 2.0 managers.

    All pools and connections are disposed.
    """

    for manager in proxies.values():
        manager.close()
    proxies.clear()

reset_rollback = util.symbol('reset_rollback')
reset_commit = util.symbol('reset_commit')
reset_none = util.symbol('reset_none')


class _ConnDialect(object):

    """partial implementation of :class:`.Dialect`
    which provides DBAPI connection methods.

    When a :class:`.Pool` is combined with an :class:`.Engine`,
    the :class:`.Engine` replaces this with its own
    :class:`.Dialect`.

    """

    def do_rollback(self, dbapi_connection):
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def do_close(self, dbapi_connection):
        dbapi_connection.close()


class Pool(log.Identified):

    """Abstract base class for connection pools."""

    _dialect = _ConnDialect()

    def __init__(self,
                 creator, recycle=-1, echo=None,
                 use_threadlocal=False,
                 logging_name=None,
                 reset_on_return=True,
                 listeners=None,
                 events=None,
                 dialect=None,
                 _dispatch=None):
        """
        Construct a Pool.

        :param creator: a callable function that returns a DB-API
          connection object.  The function will be called with
          parameters.

        :param recycle: If set to non -1, number of seconds between
          connection recycling, which means upon checkout, if this
          timeout is surpassed the connection will be closed and
          replaced with a newly opened connection. Defaults to -1.

        :param logging_name:  String identifier which will be used within
          the "name" field of logging records generated within the
          "sqlalchemy.pool" logger. Defaults to a hexstring of the object's
          id.

        :param echo: If True, connections being pulled and retrieved
          from the pool will be logged to the standard output, as well
          as pool sizing information.  Echoing can also be achieved by
          enabling logging for the "sqlalchemy.pool"
          namespace. Defaults to False.

        :param use_threadlocal: If set to True, repeated calls to
          :meth:`connect` within the same application thread will be
          guaranteed to return the same connection object, if one has
          already been retrieved from the pool and has not been
          returned yet.  Offers a slight performance advantage at the
          cost of individual transactions by default.  The
          :meth:`.Pool.unique_connection` method is provided to return
          a consistently unique connection to bypass this behavior
          when the flag is set.

          .. warning::  The :paramref:`.Pool.use_threadlocal` flag
             **does not affect the behavior** of :meth:`.Engine.connect`.
             :meth:`.Engine.connect` makes use of the
             :meth:`.Pool.unique_connection` method which **does not use thread
             local context**.  To produce a :class:`.Connection` which refers
             to the :meth:`.Pool.connect` method, use
             :meth:`.Engine.contextual_connect`.

             Note that other SQLAlchemy connectivity systems such as
             :meth:`.Engine.execute` as well as the orm
             :class:`.Session` make use of
             :meth:`.Engine.contextual_connect` internally, so these functions
             are compatible with the :paramref:`.Pool.use_threadlocal` setting.

          .. seealso::

            :ref:`threadlocal_strategy` - contains detail on the
            "threadlocal" engine strategy, which provides a more comprehensive
            approach to "threadlocal" connectivity for the specific
            use case of using :class:`.Engine` and :class:`.Connection` objects
            directly.

        :param reset_on_return: Determine steps to take on
          connections as they are returned to the pool.
          reset_on_return can have any of these values:

          * ``"rollback"`` - call rollback() on the connection,
            to release locks and transaction resources.
            This is the default value.  The vast majority
            of use cases should leave this value set.
          * ``True`` - same as 'rollback', this is here for
            backwards compatibility.
          * ``"commit"`` - call commit() on the connection,
            to release locks and transaction resources.
            A commit here may be desirable for databases that
            cache query plans if a commit is emitted,
            such as Microsoft SQL Server.  However, this
            value is more dangerous than 'rollback' because
            any data changes present on the transaction
            are committed unconditionally.
          * ``None`` - don't do anything on the connection.
            This setting should only be made on a database
            that has no transaction support at all,
            namely MySQL MyISAM.   By not doing anything,
            performance can be improved.   This
            setting should **never be selected** for a
            database that supports transactions,
            as it will lead to deadlocks and stale
            state.
          * ``"none"`` - same as ``None``

            .. versionadded:: 0.9.10

          * ``False`` - same as None, this is here for
            backwards compatibility.

          .. versionchanged:: 0.7.6
              :paramref:`.Pool.reset_on_return` accepts ``"rollback"``
              and ``"commit"`` arguments.

        :param events: a list of 2-tuples, each of the form
         ``(callable, target)`` which will be passed to :func:`.event.listen`
         upon construction.   Provided here so that event listeners
         can be assigned via :func:`.create_engine` before dialect-level
         listeners are applied.

        :param listeners: Deprecated.  A list of
          :class:`~sqlalchemy.interfaces.PoolListener`-like objects or
          dictionaries of callables that receive events when DB-API
          connections are created, checked out and checked in to the
          pool.  This has been superseded by
          :func:`~sqlalchemy.event.listen`.

        :param dialect: a :class:`.Dialect` that will handle the job
         of calling rollback(), close(), or commit() on DBAPI connections.
         If omitted, a built-in "stub" dialect is used.   Applications that
         make use of :func:`~.create_engine` should not use this parameter
         as it is handled by the engine creation strategy.

         .. versionadded:: 1.1 - ``dialect`` is now a public parameter
            to the :class:`.Pool`.

        """
        if logging_name:
            self.logging_name = self._orig_logging_name = logging_name
        else:
            self._orig_logging_name = None

        log.instance_logger(self, echoflag=echo)
        self._threadconns = threading.local()
        self._creator = creator
        self._recycle = recycle
        self._invalidate_time = 0
        self._use_threadlocal = use_threadlocal
        if reset_on_return in ('rollback', True, reset_rollback):
            self._reset_on_return = reset_rollback
        elif reset_on_return in ('none', None, False, reset_none):
            self._reset_on_return = reset_none
        elif reset_on_return in ('commit', reset_commit):
            self._reset_on_return = reset_commit
        else:
            raise exc.ArgumentError(
                "Invalid value for 'reset_on_return': %r"
                % reset_on_return)

        self.echo = echo

        if _dispatch:
            self.dispatch._update(_dispatch, only_propagate=False)
        if dialect:
            self._dialect = dialect
        if events:
            for fn, target in events:
                event.listen(self, target, fn)
        if listeners:
            util.warn_deprecated(
                "The 'listeners' argument to Pool (and "
                "create_engine()) is deprecated.  Use event.listen().")
            for l in listeners:
                self.add_listener(l)

    @property
    def _creator(self):
        return self.__dict__['_creator']

    @_creator.setter
    def _creator(self, creator):
        self.__dict__['_creator'] = creator
        self._invoke_creator = self._should_wrap_creator(creator)

    def _should_wrap_creator(self, creator):
        """Detect if creator accepts a single argument, or is sent
        as a legacy style no-arg function.

        """

        try:
            argspec = util.get_callable_argspec(self._creator, no_self=True)
        except TypeError:
            return lambda crec: creator()

        defaulted = argspec[3] is not None and len(argspec[3]) or 0
        positionals = len(argspec[0]) - defaulted

        # look for the exact arg signature that DefaultStrategy
        # sends us
        if (argspec[0], argspec[3]) == (['connection_record'], (None,)):
            return creator
        # or just a single positional
        elif positionals == 1:
            return creator
        # all other cases, just wrap and assume legacy "creator" callable
        # thing
        else:
            return lambda crec: creator()

    def _close_connection(self, connection):
        self.logger.debug("Closing connection %r", connection)

        try:
            self._dialect.do_close(connection)
        except Exception:
            self.logger.error("Exception closing connection %r",
                              connection, exc_info=True)

    @util.deprecated(
        2.7, "Pool.add_listener is deprecated.  Use event.listen()")
    def add_listener(self, listener):
        """Add a :class:`.PoolListener`-like object to this pool.

        ``listener`` may be an object that implements some or all of
        PoolListener, or a dictionary of callables containing implementations
        of some or all of the named methods in PoolListener.

        """
        interfaces.PoolListener._adapt_listener(self, listener)

    def unique_connection(self):
        """Produce a DBAPI connection that is not referenced by any
        thread-local context.

        This method is equivalent to :meth:`.Pool.connect` when the
        :paramref:`.Pool.use_threadlocal` flag is not set to True.
        When :paramref:`.Pool.use_threadlocal` is True, the
        :meth:`.Pool.unique_connection` method provides a means of bypassing
        the threadlocal context.

        """
        return _ConnectionFairy._checkout(self)

    def _create_connection(self):
        """Called by subclasses to create a new ConnectionRecord."""

        return _ConnectionRecord(self)

    def _invalidate(self, connection, exception=None):
        """Mark all connections established within the generation
        of the given connection as invalidated.

        If this pool's last invalidate time is before when the given
        connection was created, update the timestamp til now.  Otherwise,
        no action is performed.

        Connections with a start time prior to this pool's invalidation
        time will be recycled upon next checkout.
        """

        rec = getattr(connection, "_connection_record", None)
        if not rec or self._invalidate_time < rec.starttime:
            self._invalidate_time = time.time()
        if getattr(connection, 'is_valid', False):
            connection.invalidate(exception)

    def recreate(self):
        """Return a new :class:`.Pool`, of the same class as this one
        and configured with identical creation arguments.

        This method is used in conjunction with :meth:`dispose`
        to close out an entire :class:`.Pool` and create a new one in
        its place.

        """

        raise NotImplementedError()

    def dispose(self):
        """Dispose of this pool.

        This method leaves the possibility of checked-out connections
        remaining open, as it only affects connections that are
        idle in the pool.

        See also the :meth:`Pool.recreate` method.

        """

        raise NotImplementedError()

    def connect(self):
        """Return a DBAPI connection from the pool.

        The connection is instrumented such that when its
        ``close()`` method is called, the connection will be returned to
        the pool.

        """
        if not self._use_threadlocal:
            return _ConnectionFairy._checkout(self)

        try:
            rec = self._threadconns.current()
        except AttributeError:
            pass
        else:
            if rec is not None:
                return rec._checkout_existing()

        return _ConnectionFairy._checkout(self, self._threadconns)

    def _return_conn(self, record):
        """Given a _ConnectionRecord, return it to the :class:`.Pool`.

        This method is called when an instrumented DBAPI connection
        has its ``close()`` method called.

        """
        if self._use_threadlocal:
            try:
                del self._threadconns.current
            except AttributeError:
                pass
        self._do_return_conn(record)

    def _do_get(self):
        """Implementation for :meth:`get`, supplied by subclasses."""

        raise NotImplementedError()

    def _do_return_conn(self, conn):
        """Implementation for :meth:`return_conn`, supplied by subclasses."""

        raise NotImplementedError()

    def status(self):
        raise NotImplementedError()


class _ConnectionRecord(object):

    """Internal object which maintains an individual DBAPI connection
    referenced by a :class:`.Pool`.

    The :class:`._ConnectionRecord` object always exists for any particular
    DBAPI connection whether or not that DBAPI connection has been
    "checked out".  This is in contrast to the :class:`._ConnectionFairy`
    which is only a public facade to the DBAPI connection while it is checked
    out.

    A :class:`._ConnectionRecord` may exist for a span longer than that
    of a single DBAPI connection.  For example, if the
    :meth:`._ConnectionRecord.invalidate`
    method is called, the DBAPI connection associated with this
    :class:`._ConnectionRecord`
    will be discarded, but the :class:`._ConnectionRecord` may be used again,
    in which case a new DBAPI connection is produced when the :class:`.Pool`
    next uses this record.

    The :class:`._ConnectionRecord` is delivered along with connection
    pool events, including :meth:`.PoolEvents.connect` and
    :meth:`.PoolEvents.checkout`, however :class:`._ConnectionRecord` still
    remains an internal object whose API and internals may change.

    .. seealso::

        :class:`._ConnectionFairy`

    """

    def __init__(self, pool, connect=True):
        self.__pool = pool
        if connect:
            self.__connect(first_connect_check=True)
        self.finalize_callback = deque()

    fairy_ref = None

    starttime = None

    connection = None
    """A reference to the actual DBAPI connection being tracked.

    May be ``None`` if this :class:`._ConnectionRecord` has been marked
    as invalidated; a new DBAPI connection may replace it if the owning
    pool calls upon this :class:`._ConnectionRecord` to reconnect.

    """

    _soft_invalidate_time = 0

    @util.memoized_property
    def info(self):
        """The ``.info`` dictionary associated with the DBAPI connection.

        This dictionary is shared among the :attr:`._ConnectionFairy.info`
        and :attr:`.Connection.info` accessors.

        .. note::

            The lifespan of this dictionary is linked to the
            DBAPI connection itself, meaning that it is **discarded** each time
            the DBAPI connection is closed and/or invalidated.   The
            :attr:`._ConnectionRecord.record_info` dictionary remains
            persistent throughout the lifespan of the
            :class:`._ConnectionRecord` container.

        """
        return {}

    @util.memoized_property
    def record_info(self):
        """An "info' dictionary associated with the connection record
        itself.

        Unlike the :attr:`._ConnectionRecord.info` dictionary, which is linked
        to the lifespan of the DBAPI connection, this dictionary is linked
        to the lifespan of the :class:`._ConnectionRecord` container itself
        and will remain persisent throughout the life of the
        :class:`._ConnectionRecord`.

        .. versionadded:: 1.1

        """
        return {}

    @classmethod
    def checkout(cls, pool):
        rec = pool._do_get()
        try:
            dbapi_connection = rec.get_connection()
        except:
            with util.safe_reraise():
                rec.checkin()
        echo = pool._should_log_debug()
        fairy = _ConnectionFairy(dbapi_connection, rec, echo)
        rec.fairy_ref = weakref.ref(
            fairy,
            lambda ref: _finalize_fairy and
            _finalize_fairy(
                dbapi_connection,
                rec, pool, ref, echo)
        )
        _refs.add(rec)
        if echo:
            pool.logger.debug("Connection %r checked out from pool",
                              dbapi_connection)
        return fairy

    def checkin(self):
        self.fairy_ref = None
        connection = self.connection
        pool = self.__pool
        while self.finalize_callback:
            finalizer = self.finalize_callback.pop()
            finalizer(connection)
        if pool.dispatch.checkin:
            pool.dispatch.checkin(connection, self)
        pool._return_conn(self)

    @property
    def in_use(self):
        return self.fairy_ref is not None

    @property
    def last_connect_time(self):
        return self.starttime

    def close(self):
        if self.connection is not None:
            self.__close()

    def invalidate(self, e=None, soft=False):
        """Invalidate the DBAPI connection held by this :class:`._ConnectionRecord`.

        This method is called for all connection invalidations, including
        when the :meth:`._ConnectionFairy.invalidate` or
        :meth:`.Connection.invalidate` methods are called, as well as when any
        so-called "automatic invalidation" condition occurs.

        :param e: an exception object indicating a reason for the invalidation.

        :param soft: if True, the connection isn't closed; instead, this
         connection will be recycled on next checkout.

         .. versionadded:: 1.0.3

        .. seealso::

            :ref:`pool_connection_invalidation`

        """
        # already invalidated
        if self.connection is None:
            return
        if soft:
            self.__pool.dispatch.soft_invalidate(self.connection, self, e)
        else:
            self.__pool.dispatch.invalidate(self.connection, self, e)
        if e is not None:
            self.__pool.logger.info(
                "%sInvalidate connection %r (reason: %s:%s)",
                "Soft " if soft else "",
                self.connection, e.__class__.__name__, e)
        else:
            self.__pool.logger.info(
                "%sInvalidate connection %r",
                "Soft " if soft else "",
                self.connection)
        if soft:
            self._soft_invalidate_time = time.time()
        else:
            self.__close()
            self.connection = None

    def get_connection(self):
        recycle = False
        if self.connection is None:
            self.info.clear()
            self.__connect()
        elif self.__pool._recycle > -1 and \
                time.time() - self.starttime > self.__pool._recycle:
            self.__pool.logger.info(
                "Connection %r exceeded timeout; recycling",
                self.connection)
            recycle = True
        elif self.__pool._invalidate_time > self.starttime:
            self.__pool.logger.info(
                "Connection %r invalidated due to pool invalidation; " +
                "recycling",
                self.connection
            )
            recycle = True
        elif self._soft_invalidate_time > self.starttime:
            self.__pool.logger.info(
                "Connection %r invalidated due to local soft invalidation; " +
                "recycling",
                self.connection
            )
            recycle = True

        if recycle:
            self.__close()
            self.info.clear()

            self.__connect()
        return self.connection

    def __close(self):
        self.finalize_callback.clear()
        if self.__pool.dispatch.close:
            self.__pool.dispatch.close(self.connection, self)
        self.__pool._close_connection(self.connection)
        self.connection = None

    def __connect(self, first_connect_check=False):
        pool = self.__pool

        # ensure any existing connection is removed, so that if
        # creator fails, this attribute stays None
        self.connection = None
        try:
            self.starttime = time.time()
            connection = pool._invoke_creator(self)
            pool.logger.debug("Created new connection %r", connection)
            self.connection = connection
        except Exception as e:
            pool.logger.debug("Error on connect(): %s", e)
            raise
        else:
            if first_connect_check:
                pool.dispatch.first_connect.\
                    for_modify(pool.dispatch).\
                    exec_once(self.connection, self)
            if pool.dispatch.connect:
                pool.dispatch.connect(self.connection, self)


def _finalize_fairy(connection, connection_record,
                    pool, ref, echo, fairy=None):
    """Cleanup for a :class:`._ConnectionFairy` whether or not it's already
    been garbage collected.

    """
    _refs.discard(connection_record)

    if ref is not None and \
            connection_record.fairy_ref is not ref:
        return

    if connection is not None:
        if connection_record and echo:
            pool.logger.debug("Connection %r being returned to pool",
                              connection)

        try:
            fairy = fairy or _ConnectionFairy(
                connection, connection_record, echo)
            assert fairy.connection is connection
            fairy._reset(pool)

            # Immediately close detached instances
            if not connection_record:
                if pool.dispatch.close_detached:
                    pool.dispatch.close_detached(connection)
                pool._close_connection(connection)
        except BaseException as e:
            pool.logger.error(
                "Exception during reset or similar", exc_info=True)
            if connection_record:
                connection_record.invalidate(e=e)
            if not isinstance(e, Exception):
                raise

    if connection_record:
        connection_record.checkin()


_refs = set()


class _ConnectionFairy(object):

    """Proxies a DBAPI connection and provides return-on-dereference
    support.

    This is an internal object used by the :class:`.Pool` implementation
    to provide context management to a DBAPI connection delivered by
    that :class:`.Pool`.

    The name "fairy" is inspired by the fact that the
    :class:`._ConnectionFairy` object's lifespan is transitory, as it lasts
    only for the length of a specific DBAPI connection being checked out from
    the pool, and additionally that as a transparent proxy, it is mostly
    invisible.

    .. seealso::

        :class:`._ConnectionRecord`

    """

    def __init__(self, dbapi_connection, connection_record, echo):
        self.connection = dbapi_connection
        self._connection_record = connection_record
        self._echo = echo

    connection = None
    """A reference to the actual DBAPI connection being tracked."""

    _connection_record = None
    """A reference to the :class:`._ConnectionRecord` object associated
    with the DBAPI connection.

    This is currently an internal accessor which is subject to change.

    """

    _reset_agent = None
    """Refer to an object with a ``.commit()`` and ``.rollback()`` method;
    if non-None, the "reset-on-return" feature will call upon this object
    rather than directly against the dialect-level do_rollback() and
    do_commit() methods.

    In practice, a :class:`.Connection` assigns a :class:`.Transaction` object
    to this variable when one is in scope so that the :class:`.Transaction`
    takes the job of committing or rolling back on return if
    :meth:`.Connection.close` is called while the :class:`.Transaction`
    still exists.

    This is essentially an "event handler" of sorts but is simplified as an
    instance variable both for performance/simplicity as well as that there
    can only be one "reset agent" at a time.
    """

    @classmethod
    def _checkout(cls, pool, threadconns=None, fairy=None):
        if not fairy:
            fairy = _ConnectionRecord.checkout(pool)

            fairy._pool = pool
            fairy._counter = 0

            if threadconns is not None:
                threadconns.current = weakref.ref(fairy)

        if fairy.connection is None:
            raise exc.InvalidRequestError("This connection is closed")
        fairy._counter += 1

        if not pool.dispatch.checkout or fairy._counter != 1:
            return fairy

        # Pool listeners can trigger a reconnection on checkout
        attempts = 2
        while attempts > 0:
            try:
                pool.dispatch.checkout(fairy.connection,
                                       fairy._connection_record,
                                       fairy)
                return fairy
            except exc.DisconnectionError as e:
                pool.logger.info(
                    "Disconnection detected on checkout: %s", e)
                fairy._connection_record.invalidate(e)
                try:
                    fairy.connection = \
                        fairy._connection_record.get_connection()
                except:
                    with util.safe_reraise():
                        fairy._connection_record.checkin()

                attempts -= 1

        pool.logger.info("Reconnection attempts exhausted on checkout")
        fairy.invalidate()
        raise exc.InvalidRequestError("This connection is closed")

    def _checkout_existing(self):
        return _ConnectionFairy._checkout(self._pool, fairy=self)

    def _checkin(self):
        _finalize_fairy(self.connection, self._connection_record,
                        self._pool, None, self._echo, fairy=self)
        self.connection = None
        self._connection_record = None

    _close = _checkin

    def _reset(self, pool):
        if pool.dispatch.reset:
            pool.dispatch.reset(self, self._connection_record)
        if pool._reset_on_return is reset_rollback:
            if self._echo:
                pool.logger.debug("Connection %s rollback-on-return%s",
                                  self.connection,
                                  ", via agent"
                                  if self._reset_agent else "")
            if self._reset_agent:
                self._reset_agent.rollback()
            else:
                pool._dialect.do_rollback(self)
        elif pool._reset_on_return is reset_commit:
            if self._echo:
                pool.logger.debug("Connection %s commit-on-return%s",
                                  self.connection,
                                  ", via agent"
                                  if self._reset_agent else "")
            if self._reset_agent:
                self._reset_agent.commit()
            else:
                pool._dialect.do_commit(self)

    @property
    def _logger(self):
        return self._pool.logger

    @property
    def is_valid(self):
        """Return True if this :class:`._ConnectionFairy` still refers
        to an active DBAPI connection."""

        return self.connection is not None

    @util.memoized_property
    def info(self):
        """Info dictionary associated with the underlying DBAPI connection
        referred to by this :class:`.ConnectionFairy`, allowing user-defined
        data to be associated with the connection.

        The data here will follow along with the DBAPI connection including
        after it is returned to the connection pool and used again
        in subsequent instances of :class:`._ConnectionFairy`.  It is shared
        with the :attr:`._ConnectionRecord.info` and :attr:`.Connection.info`
        accessors.

        The dictionary associated with a particular DBAPI connection is
        discarded when the connection itself is discarded.

        """
        return self._connection_record.info

    @property
    def record_info(self):
        """Info dictionary associated with the :class:`._ConnectionRecord
        container referred to by this :class:`.ConnectionFairy`.

        Unlike the :attr:`._ConnectionFairy.info` dictionary, the lifespan
        of this dictionary is persistent across connections that are
        disconnected and/or invalidated within the lifespan of a
        :class:`._ConnectionRecord`.

        .. versionadded:: 1.1

        """
        if self._connection_record:
            return self._connection_record.record_info
        else:
            return None

    def invalidate(self, e=None, soft=False):
        """Mark this connection as invalidated.

        This method can be called directly, and is also called as a result
        of the :meth:`.Connection.invalidate` method.   When invoked,
        the DBAPI connection is immediately closed and discarded from
        further use by the pool.  The invalidation mechanism proceeds
        via the :meth:`._ConnectionRecord.invalidate` internal method.

        :param e: an exception object indicating a reason for the invalidation.

        :param soft: if True, the connection isn't closed; instead, this
         connection will be recycled on next checkout.

         .. versionadded:: 1.0.3

        .. seealso::

            :ref:`pool_connection_invalidation`

        """

        if self.connection is None:
            util.warn("Can't invalidate an already-closed connection.")
            return
        if self._connection_record:
            self._connection_record.invalidate(e=e, soft=soft)
        if not soft:
            self.connection = None
            self._checkin()

    def cursor(self, *args, **kwargs):
        """Return a new DBAPI cursor for the underlying connection.

        This method is a proxy for the ``connection.cursor()`` DBAPI
        method.

        """
        return self.connection.cursor(*args, **kwargs)

    def __getattr__(self, key):
        return getattr(self.connection, key)

    def detach(self):
        """Separate this connection from its Pool.

        This means that the connection will no longer be returned to the
        pool when closed, and will instead be literally closed.  The
        containing ConnectionRecord is separated from the DB-API connection,
        and will create a new connection when next used.

        Note that any overall connection limiting constraints imposed by a
        Pool implementation may be violated after a detach, as the detached
        connection is removed from the pool's knowledge and control.
        """

        if self._connection_record is not None:
            rec = self._connection_record
            _refs.remove(rec)
            rec.fairy_ref = None
            rec.connection = None
            # TODO: should this be _return_conn?
            self._pool._do_return_conn(self._connection_record)
            self.info = self.info.copy()
            self._connection_record = None

            if self._pool.dispatch.detach:
                self._pool.dispatch.detach(self.connection, rec)

    def close(self):
        self._counter -= 1
        if self._counter == 0:
            self._checkin()


class SingletonThreadPool(Pool):

    """A Pool that maintains one connection per thread.

    Maintains one connection per each thread, never moving a connection to a
    thread other than the one which it was created in.

    .. warning::  the :class:`.SingletonThreadPool` will call ``.close()``
       on arbitrary connections that exist beyond the size setting of
       ``pool_size``, e.g. if more unique **thread identities**
       than what ``pool_size`` states are used.   This cleanup is
       non-deterministic and not sensitive to whether or not the connections
       linked to those thread identities are currently in use.

       :class:`.SingletonThreadPool` may be improved in a future release,
       however in its current status it is generally used only for test
       scenarios using a SQLite ``:memory:`` database and is not recommended
       for production use.


    Options are the same as those of :class:`.Pool`, as well as:

    :param pool_size: The number of threads in which to maintain connections
        at once.  Defaults to five.

    :class:`.SingletonThreadPool` is used by the SQLite dialect
    automatically when a memory-based database is used.
    See :ref:`sqlite_toplevel`.

    """

    def __init__(self, creator, pool_size=5, **kw):
        kw['use_threadlocal'] = True
        Pool.__init__(self, creator, **kw)
        self._conn = threading.local()
        self._all_conns = set()
        self.size = pool_size

    def recreate(self):
        self.logger.info("Pool recreating")
        return self.__class__(self._creator,
                              pool_size=self.size,
                              recycle=self._recycle,
                              echo=self.echo,
                              logging_name=self._orig_logging_name,
                              use_threadlocal=self._use_threadlocal,
                              reset_on_return=self._reset_on_return,
                              _dispatch=self.dispatch,
                              dialect=self._dialect)

    def dispose(self):
        """Dispose of this pool."""

        for conn in self._all_conns:
            try:
                conn.close()
            except Exception:
                # pysqlite won't even let you close a conn from a thread
                # that didn't create it
                pass

        self._all_conns.clear()

    def _cleanup(self):
        while len(self._all_conns) >= self.size:
            c = self._all_conns.pop()
            c.close()

    def status(self):
        return "SingletonThreadPool id:%d size: %d" % \
            (id(self), len(self._all_conns))

    def _do_return_conn(self, conn):
        pass

    def _do_get(self):
        try:
            c = self._conn.current()
            if c:
                return c
        except AttributeError:
            pass
        c = self._create_connection()
        self._conn.current = weakref.ref(c)
        if len(self._all_conns) >= self.size:
            self._cleanup()
        self._all_conns.add(c)
        return c


class QueuePool(Pool):

    """A :class:`.Pool` that imposes a limit on the number of open connections.

    :class:`.QueuePool` is the default pooling implementation used for
    all :class:`.Engine` objects, unless the SQLite dialect is in use.

    """

    def __init__(self, creator, pool_size=5, max_overflow=10, timeout=30,
                 **kw):
        """
        Construct a QueuePool.

        :param creator: a callable function that returns a DB-API
          connection object, same as that of :paramref:`.Pool.creator`.

        :param pool_size: The size of the pool to be maintained,
          defaults to 5. This is the largest number of connections that
          will be kept persistently in the pool. Note that the pool
          begins with no connections; once this number of connections
          is requested, that number of connections will remain.
          ``pool_size`` can be set to 0 to indicate no size limit; to
          disable pooling, use a :class:`~sqlalchemy.pool.NullPool`
          instead.

        :param max_overflow: The maximum overflow size of the
          pool. When the number of checked-out connections reaches the
          size set in pool_size, additional connections will be
          returned up to this limit. When those additional connections
          are returned to the pool, they are disconnected and
          discarded. It follows then that the total number of
          simultaneous connections the pool will allow is pool_size +
          `max_overflow`, and the total number of "sleeping"
          connections the pool will allow is pool_size. `max_overflow`
          can be set to -1 to indicate no overflow limit; no limit
          will be placed on the total number of concurrent
          connections. Defaults to 10.

        :param timeout: The number of seconds to wait before giving up
          on returning a connection. Defaults to 30.

        :param \**kw: Other keyword arguments including
          :paramref:`.Pool.recycle`, :paramref:`.Pool.echo`,
          :paramref:`.Pool.reset_on_return` and others are passed to the
          :class:`.Pool` constructor.

        """
        Pool.__init__(self, creator, **kw)
        self._pool = sqla_queue.Queue(pool_size)
        self._overflow = 0 - pool_size
        self._max_overflow = max_overflow
        self._timeout = timeout
        self._overflow_lock = threading.Lock()

    def _do_return_conn(self, conn):
        try:
            self._pool.put(conn, False)
        except sqla_queue.Full:
            try:
                conn.close()
            finally:
                self._dec_overflow()

    def _do_get(self):
        use_overflow = self._max_overflow > -1

        try:
            wait = use_overflow and self._overflow >= self._max_overflow
            return self._pool.get(wait, self._timeout)
        except sqla_queue.Empty:
            if use_overflow and self._overflow >= self._max_overflow:
                if not wait:
                    return self._do_get()
                else:
                    raise exc.TimeoutError(
                        "QueuePool limit of size %d overflow %d reached, "
                        "connection timed out, timeout %d" %
                        (self.size(), self.overflow(), self._timeout))

            if self._inc_overflow():
                try:
                    return self._create_connection()
                except:
                    with util.safe_reraise():
                        self._dec_overflow()
            else:
                return self._do_get()

    def _inc_overflow(self):
        if self._max_overflow == -1:
            self._overflow += 1
            return True
        with self._overflow_lock:
            if self._overflow < self._max_overflow:
                self._overflow += 1
                return True
            else:
                return False

    def _dec_overflow(self):
        if self._max_overflow == -1:
            self._overflow -= 1
            return True
        with self._overflow_lock:
            self._overflow -= 1
            return True

    def recreate(self):
        self.logger.info("Pool recreating")
        return self.__class__(self._creator, pool_size=self._pool.maxsize,
                              max_overflow=self._max_overflow,
                              timeout=self._timeout,
                              recycle=self._recycle, echo=self.echo,
                              logging_name=self._orig_logging_name,
                              use_threadlocal=self._use_threadlocal,
                              reset_on_return=self._reset_on_return,
                              _dispatch=self.dispatch,
                              dialect=self._dialect)

    def dispose(self):
        while True:
            try:
                conn = self._pool.get(False)
                conn.close()
            except sqla_queue.Empty:
                break

        self._overflow = 0 - self.size()
        self.logger.info("Pool disposed. %s", self.status())

    def status(self):
        return "Pool size: %d  Connections in pool: %d "\
            "Current Overflow: %d Current Checked out "\
            "connections: %d" % (self.size(),
                                 self.checkedin(),
                                 self.overflow(),
                                 self.checkedout())

    def size(self):
        return self._pool.maxsize

    def checkedin(self):
        return self._pool.qsize()

    def overflow(self):
        return self._overflow

    def checkedout(self):
        return self._pool.maxsize - self._pool.qsize() + self._overflow


class NullPool(Pool):

    """A Pool which does not pool connections.

    Instead it literally opens and closes the underlying DB-API connection
    per each connection open/close.

    Reconnect-related functions such as ``recycle`` and connection
    invalidation are not supported by this Pool implementation, since
    no connections are held persistently.

    .. versionchanged:: 0.7
        :class:`.NullPool` is used by the SQlite dialect automatically
        when a file-based database is used. See :ref:`sqlite_toplevel`.

    """

    def status(self):
        return "NullPool"

    def _do_return_conn(self, conn):
        conn.close()

    def _do_get(self):
        return self._create_connection()

    def recreate(self):
        self.logger.info("Pool recreating")

        return self.__class__(self._creator,
                              recycle=self._recycle,
                              echo=self.echo,
                              logging_name=self._orig_logging_name,
                              use_threadlocal=self._use_threadlocal,
                              reset_on_return=self._reset_on_return,
                              _dispatch=self.dispatch,
                              dialect=self._dialect)

    def dispose(self):
        pass


class StaticPool(Pool):

    """A Pool of exactly one connection, used for all requests.

    Reconnect-related functions such as ``recycle`` and connection
    invalidation (which is also used to support auto-reconnect) are not
    currently supported by this Pool implementation but may be implemented
    in a future release.

    """

    @memoized_property
    def _conn(self):
        return self._creator()

    @memoized_property
    def connection(self):
        return _ConnectionRecord(self)

    def status(self):
        return "StaticPool"

    def dispose(self):
        if '_conn' in self.__dict__:
            self._conn.close()
            self._conn = None

    def recreate(self):
        self.logger.info("Pool recreating")
        return self.__class__(creator=self._creator,
                              recycle=self._recycle,
                              use_threadlocal=self._use_threadlocal,
                              reset_on_return=self._reset_on_return,
                              echo=self.echo,
                              logging_name=self._orig_logging_name,
                              _dispatch=self.dispatch,
                              dialect=self._dialect)

    def _create_connection(self):
        return self._conn

    def _do_return_conn(self, conn):
        pass

    def _do_get(self):
        return self.connection


class AssertionPool(Pool):

    """A :class:`.Pool` that allows at most one checked out connection at
    any given time.

    This will raise an exception if more than one connection is checked out
    at a time.  Useful for debugging code that is using more connections
    than desired.

    .. versionchanged:: 0.7
        :class:`.AssertionPool` also logs a traceback of where
        the original connection was checked out, and reports
        this in the assertion error raised.

    """

    def __init__(self, *args, **kw):
        self._conn = None
        self._checked_out = False
        self._store_traceback = kw.pop('store_traceback', True)
        self._checkout_traceback = None
        Pool.__init__(self, *args, **kw)

    def status(self):
        return "AssertionPool"

    def _do_return_conn(self, conn):
        if not self._checked_out:
            raise AssertionError("connection is not checked out")
        self._checked_out = False
        assert conn is self._conn

    def dispose(self):
        self._checked_out = False
        if self._conn:
            self._conn.close()

    def recreate(self):
        self.logger.info("Pool recreating")
        return self.__class__(self._creator, echo=self.echo,
                              logging_name=self._orig_logging_name,
                              _dispatch=self.dispatch,
                              dialect=self._dialect)

    def _do_get(self):
        if self._checked_out:
            if self._checkout_traceback:
                suffix = ' at:\n%s' % ''.join(
                    chop_traceback(self._checkout_traceback))
            else:
                suffix = ''
            raise AssertionError("connection is already checked out" + suffix)

        if not self._conn:
            self._conn = self._create_connection()

        self._checked_out = True
        if self._store_traceback:
            self._checkout_traceback = traceback.format_stack()
        return self._conn


class _DBProxy(object):

    """Layers connection pooling behavior on top of a standard DB-API module.

    Proxies a DB-API 2.0 connect() call to a connection pool keyed to the
    specific connect parameters. Other functions and attributes are delegated
    to the underlying DB-API module.
    """

    def __init__(self, module, poolclass=QueuePool, **kw):
        """Initializes a new proxy.

        module
          a DB-API 2.0 module

        poolclass
          a Pool class, defaulting to QueuePool

        Other parameters are sent to the Pool object's constructor.

        """

        self.module = module
        self.kw = kw
        self.poolclass = poolclass
        self.pools = {}
        self._create_pool_mutex = threading.Lock()

    def close(self):
        for key in list(self.pools):
            del self.pools[key]

    def __del__(self):
        self.close()

    def __getattr__(self, key):
        return getattr(self.module, key)

    def get_pool(self, *args, **kw):
        key = self._serialize(*args, **kw)
        try:
            return self.pools[key]
        except KeyError:
            self._create_pool_mutex.acquire()
            try:
                if key not in self.pools:
                    kw.pop('sa_pool_key', None)
                    pool = self.poolclass(
                        lambda: self.module.connect(*args, **kw), **self.kw)
                    self.pools[key] = pool
                    return pool
                else:
                    return self.pools[key]
            finally:
                self._create_pool_mutex.release()

    def connect(self, *args, **kw):
        """Activate a connection to the database.

        Connect to the database using this DBProxy's module and the given
        connect arguments.  If the arguments match an existing pool, the
        connection will be returned from the pool's current thread-local
        connection instance, or if there is no thread-local connection
        instance it will be checked out from the set of pooled connections.

        If the pool has no available connections and allows new connections
        to be created, a new database connection will be made.

        """

        return self.get_pool(*args, **kw).connect()

    def dispose(self, *args, **kw):
        """Dispose the pool referenced by the given connect arguments."""

        key = self._serialize(*args, **kw)
        try:
            del self.pools[key]
        except KeyError:
            pass

    def _serialize(self, *args, **kw):
        if "sa_pool_key" in kw:
            return kw['sa_pool_key']

        return tuple(
            list(args) +
            [(k, kw[k]) for k in sorted(kw)]
        )
