# sqlalchemy/pool.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Base constructs for connection pools.

"""

from collections import deque
import time
import weakref

from .. import event
from .. import exc
from .. import interfaces
from .. import log
from .. import util
from ..util import threading


reset_rollback = util.symbol("reset_rollback")
reset_commit = util.symbol("reset_commit")
reset_none = util.symbol("reset_none")


class _ConnDialect(object):

    """partial implementation of :class:`.Dialect`
    which provides DBAPI connection methods.

    When a :class:`_pool.Pool` is combined with an :class:`_engine.Engine`,
    the :class:`_engine.Engine` replaces this with its own
    :class:`.Dialect`.

    """

    def do_rollback(self, dbapi_connection):
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def do_close(self, dbapi_connection):
        dbapi_connection.close()

    def do_ping(self, dbapi_connection):
        raise NotImplementedError(
            "The ping feature requires that a dialect is "
            "passed to the connection pool."
        )


class Pool(log.Identified):

    """Abstract base class for connection pools."""

    _dialect = _ConnDialect()

    @util.deprecated_params(
        use_threadlocal=(
            "1.3",
            "The :paramref:`_pool.Pool.use_threadlocal` parameter is "
            "deprecated and will be removed in a future release.",
        ),
        listeners=(
            "0.7",
            ":class:`.PoolListener` is deprecated in favor of the "
            ":class:`_events.PoolEvents` listener interface.  The "
            ":paramref:`_pool.Pool.listeners` parameter will be removed in a "
            "future release.",
        ),
    )
    def __init__(
        self,
        creator,
        recycle=-1,
        echo=None,
        use_threadlocal=False,
        logging_name=None,
        reset_on_return=True,
        listeners=None,
        events=None,
        dialect=None,
        pre_ping=False,
        _dispatch=None,
    ):
        """
        Construct a Pool.

        :param creator: a callable function that returns a DB-API
          connection object.  The function will be called with
          parameters.

        :param recycle: If set to a value other than -1, number of
          seconds between connection recycling, which means upon
          checkout, if this timeout is surpassed the connection will be
          closed and replaced with a newly opened connection. Defaults to -1.

        :param logging_name:  String identifier which will be used within
          the "name" field of logging records generated within the
          "sqlalchemy.pool" logger. Defaults to a hexstring of the object's
          id.

        :param echo: if True, the connection pool will log
         informational output such as when connections are invalidated
         as well as when connections are recycled to the default log handler,
         which defaults to ``sys.stdout`` for output..   If set to the string
         ``"debug"``, the logging will include pool checkouts and checkins.

         The :paramref:`_pool.Pool.echo` parameter can also be set from the
         :func:`_sa.create_engine` call by using the
         :paramref:`_sa.create_engine.echo_pool` parameter.

         .. seealso::

             :ref:`dbengine_logging` - further detail on how to configure
             logging.

        :param use_threadlocal: If set to True, repeated calls to
          :meth:`connect` within the same application thread will be
          guaranteed to return the same connection object that is already
          checked out.   This is a legacy use case and the flag has no
          effect when using the pool with a :class:`_engine.Engine` object.

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
            This setting should generally only be made on a database
            that has no transaction support at all,
            namely MySQL MyISAM; when used on this backend, performance
            can be improved as the "rollback" call is still expensive on
            MySQL.   It is **strongly recommended** that this setting not be
            used for transaction-supporting databases in conjunction with
            a persistent pool such as :class:`.QueuePool`, as it opens
            the possibility for connections still in a transaction to be
            idle in the pool.   The setting may be appropriate in the
            case of :class:`.NullPool` or special circumstances where
            the connection pool in use is not being used to maintain connection
            lifecycle.

          * ``False`` - same as None, this is here for
            backwards compatibility.

        :param events: a list of 2-tuples, each of the form
         ``(callable, target)`` which will be passed to :func:`.event.listen`
         upon construction.   Provided here so that event listeners
         can be assigned via :func:`_sa.create_engine` before dialect-level
         listeners are applied.

        :param listeners: A list of :class:`.PoolListener`-like objects or
          dictionaries of callables that receive events when DB-API
          connections are created, checked out and checked in to the
          pool.

        :param dialect: a :class:`.Dialect` that will handle the job
         of calling rollback(), close(), or commit() on DBAPI connections.
         If omitted, a built-in "stub" dialect is used.   Applications that
         make use of :func:`_sa.create_engine` should not use this parameter
         as it is handled by the engine creation strategy.

         .. versionadded:: 1.1 - ``dialect`` is now a public parameter
            to the :class:`_pool.Pool`.

        :param pre_ping: if True, the pool will emit a "ping" (typically
         "SELECT 1", but is dialect-specific) on the connection
         upon checkout, to test if the connection is alive or not.   If not,
         the connection is transparently re-connected and upon success, all
         other pooled connections established prior to that timestamp are
         invalidated.     Requires that a dialect is passed as well to
         interpret the disconnection error.

         .. versionadded:: 1.2

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
        self._pre_ping = pre_ping
        self._reset_on_return = util.symbol.parse_user_argument(
            reset_on_return,
            {
                reset_rollback: ["rollback", True],
                reset_none: ["none", None, False],
                reset_commit: ["commit"],
            },
            "reset_on_return",
            resolve_symbol_names=False,
        )

        self.echo = echo

        if _dispatch:
            self.dispatch._update(_dispatch, only_propagate=False)
        if dialect:
            self._dialect = dialect
        if events:
            for fn, target in events:
                event.listen(self, target, fn)
        if listeners:
            for l in listeners:
                self.add_listener(l)

    @property
    def _creator(self):
        return self.__dict__["_creator"]

    @_creator.setter
    def _creator(self, creator):
        self.__dict__["_creator"] = creator
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
        if (argspec[0], argspec[3]) == (["connection_record"], (None,)):
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
            self.logger.error(
                "Exception closing connection %r", connection, exc_info=True
            )

    @util.deprecated(
        "0.7",
        "The :meth:`_pool.Pool.add_listener` method is deprecated and "
        "will be removed in a future release.  Please use the "
        ":class:`_events.PoolEvents` listener interface.",
    )
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

        This method is equivalent to :meth:`_pool.Pool.connect` when the
        :paramref:`_pool.Pool.use_threadlocal` flag is not set to True.
        When :paramref:`_pool.Pool.use_threadlocal` is True, the
        :meth:`_pool.Pool.unique_connection`
        method provides a means of bypassing
        the threadlocal context.

        """
        return _ConnectionFairy._checkout(self)

    def _create_connection(self):
        """Called by subclasses to create a new ConnectionRecord."""

        return _ConnectionRecord(self)

    def _invalidate(self, connection, exception=None, _checkin=True):
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
        if _checkin and getattr(connection, "is_valid", False):
            connection.invalidate(exception)

    def recreate(self):
        """Return a new :class:`_pool.Pool`, of the same class as this one
        and configured with identical creation arguments.

        This method is used in conjunction with :meth:`dispose`
        to close out an entire :class:`_pool.Pool` and create a new one in
        its place.

        """

        raise NotImplementedError()

    def dispose(self):
        """Dispose of this pool.

        This method leaves the possibility of checked-out connections
        remaining open, as it only affects connections that are
        idle in the pool.

        .. seealso::

            :meth:`Pool.recreate`

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
        """Given a _ConnectionRecord, return it to the :class:`_pool.Pool`.

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
    referenced by a :class:`_pool.Pool`.

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
    in which case a new DBAPI connection is produced when the
    :class:`_pool.Pool`
    next uses this record.

    The :class:`._ConnectionRecord` is delivered along with connection
    pool events, including :meth:`_events.PoolEvents.connect` and
    :meth:`_events.PoolEvents.checkout`, however :class:`._ConnectionRecord`
    still
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
        and :attr:`_engine.Connection.info` accessors.

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
        and will remain persistent throughout the life of the
        :class:`._ConnectionRecord`.

        .. versionadded:: 1.1

        """
        return {}

    @classmethod
    def checkout(cls, pool):
        rec = pool._do_get()
        try:
            dbapi_connection = rec.get_connection()
        except Exception as err:
            with util.safe_reraise():
                rec._checkin_failed(err)
        echo = pool._should_log_debug()
        fairy = _ConnectionFairy(dbapi_connection, rec, echo)
        rec.fairy_ref = weakref.ref(
            fairy,
            lambda ref: _finalize_fairy
            and _finalize_fairy(None, rec, pool, ref, echo),
        )
        _refs.add(rec)
        if echo:
            pool.logger.debug(
                "Connection %r checked out from pool", dbapi_connection
            )
        return fairy

    def _checkin_failed(self, err):
        self.invalidate(e=err)
        self.checkin(_no_fairy_ref=True)

    def checkin(self, _no_fairy_ref=False):
        if self.fairy_ref is None and not _no_fairy_ref:
            util.warn("Double checkin attempted on %s" % self)
            return
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
        :meth:`_engine.Connection.invalidate` methods are called,
        as well as when any
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
                self.connection,
                e.__class__.__name__,
                e,
            )
        else:
            self.__pool.logger.info(
                "%sInvalidate connection %r",
                "Soft " if soft else "",
                self.connection,
            )
        if soft:
            self._soft_invalidate_time = time.time()
        else:
            self.__close()
            self.connection = None

    def get_connection(self):
        recycle = False

        # NOTE: the various comparisons here are assuming that measurable time
        # passes between these state changes.  however, time.time() is not
        # guaranteed to have sub-second precision.  comparisons of
        # "invalidation time" to "starttime" should perhaps use >= so that the
        # state change can take place assuming no measurable  time has passed,
        # however this does not guarantee correct behavior here as if time
        # continues to not pass, it will try to reconnect repeatedly until
        # these timestamps diverge, so in that sense using > is safer.  Per
        # https://stackoverflow.com/a/1938096/34549, Windows time.time() may be
        # within 16 milliseconds accuracy, so unit tests for connection
        # invalidation need a sleep of at least this long between initial start
        # time and invalidation for the logic below to work reliably.
        if self.connection is None:
            self.info.clear()
            self.__connect()
        elif (
            self.__pool._recycle > -1
            and time.time() - self.starttime > self.__pool._recycle
        ):
            self.__pool.logger.info(
                "Connection %r exceeded timeout; recycling", self.connection
            )
            recycle = True
        elif self.__pool._invalidate_time > self.starttime:
            self.__pool.logger.info(
                "Connection %r invalidated due to pool invalidation; "
                + "recycling",
                self.connection,
            )
            recycle = True
        elif self._soft_invalidate_time > self.starttime:
            self.__pool.logger.info(
                "Connection %r invalidated due to local soft invalidation; "
                + "recycling",
                self.connection,
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
            with util.safe_reraise():
                pool.logger.debug("Error on connect(): %s", e)
        else:
            if first_connect_check:
                pool.dispatch.first_connect.for_modify(
                    pool.dispatch
                ).exec_once_unless_exception(self.connection, self)
            if pool.dispatch.connect:
                pool.dispatch.connect(self.connection, self)


def _finalize_fairy(
    connection, connection_record, pool, ref, echo, fairy=None
):
    """Cleanup for a :class:`._ConnectionFairy` whether or not it's already
    been garbage collected.

    """
    _refs.discard(connection_record)

    if ref is not None:
        if connection_record.fairy_ref is not ref:
            return
        assert connection is None
        connection = connection_record.connection

    if connection is not None:
        if connection_record and echo:
            pool.logger.debug(
                "Connection %r being returned to pool", connection
            )

        try:
            fairy = fairy or _ConnectionFairy(
                connection, connection_record, echo
            )
            assert fairy.connection is connection
            fairy._reset(pool)

            # Immediately close detached instances
            if not connection_record:
                if pool.dispatch.close_detached:
                    pool.dispatch.close_detached(connection)
                pool._close_connection(connection)
        except BaseException as e:
            pool.logger.error(
                "Exception during reset or similar", exc_info=True
            )
            if connection_record:
                connection_record.invalidate(e=e)
            if not isinstance(e, Exception):
                raise

    if connection_record and connection_record.fairy_ref is not None:
        connection_record.checkin()


_refs = set()


class _ConnectionFairy(object):

    """Proxies a DBAPI connection and provides return-on-dereference
    support.

    This is an internal object used by the :class:`_pool.Pool` implementation
    to provide context management to a DBAPI connection delivered by
    that :class:`_pool.Pool`.

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

    In practice, a :class:`_engine.Connection` assigns a :class:`.Transaction`
    object
    to this variable when one is in scope so that the :class:`.Transaction`
    takes the job of committing or rolling back on return if
    :meth:`_engine.Connection.close` is called while the :class:`.Transaction`
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

        if (
            not pool.dispatch.checkout and not pool._pre_ping
        ) or fairy._counter != 1:
            return fairy

        # Pool listeners can trigger a reconnection on checkout, as well
        # as the pre-pinger.
        # there are three attempts made here, but note that if the database
        # is not accessible from a connection standpoint, those won't proceed
        # here.
        attempts = 2
        while attempts > 0:
            try:
                if pool._pre_ping:
                    if fairy._echo:
                        pool.logger.debug(
                            "Pool pre-ping on connection %s", fairy.connection
                        )

                    result = pool._dialect.do_ping(fairy.connection)
                    if not result:
                        if fairy._echo:
                            pool.logger.debug(
                                "Pool pre-ping on connection %s failed, "
                                "will invalidate pool",
                                fairy.connection,
                            )
                        raise exc.InvalidatePoolError()

                pool.dispatch.checkout(
                    fairy.connection, fairy._connection_record, fairy
                )
                return fairy
            except exc.DisconnectionError as e:
                if e.invalidate_pool:
                    pool.logger.info(
                        "Disconnection detected on checkout, "
                        "invalidating all pooled connections prior to "
                        "current timestamp (reason: %r)",
                        e,
                    )
                    fairy._connection_record.invalidate(e)
                    pool._invalidate(fairy, e, _checkin=False)
                else:
                    pool.logger.info(
                        "Disconnection detected on checkout, "
                        "invalidating individual connection %s (reason: %r)",
                        fairy.connection,
                        e,
                    )
                    fairy._connection_record.invalidate(e)
                try:
                    fairy.connection = (
                        fairy._connection_record.get_connection()
                    )
                except Exception as err:
                    with util.safe_reraise():
                        fairy._connection_record._checkin_failed(err)

                attempts -= 1

        pool.logger.info("Reconnection attempts exhausted on checkout")
        fairy.invalidate()
        raise exc.InvalidRequestError("This connection is closed")

    def _checkout_existing(self):
        return _ConnectionFairy._checkout(self._pool, fairy=self)

    def _checkin(self):
        _finalize_fairy(
            self.connection,
            self._connection_record,
            self._pool,
            None,
            self._echo,
            fairy=self,
        )
        self.connection = None
        self._connection_record = None

    _close = _checkin

    def _reset(self, pool):
        if pool.dispatch.reset:
            pool.dispatch.reset(self, self._connection_record)
        if pool._reset_on_return is reset_rollback:
            if self._echo:
                pool.logger.debug(
                    "Connection %s rollback-on-return%s",
                    self.connection,
                    ", via agent" if self._reset_agent else "",
                )
            if self._reset_agent:
                if not self._reset_agent.is_active:
                    util.warn(
                        "Reset agent is not active.  "
                        "This should not occur unless there was already "
                        "a connectivity error in progress."
                    )
                    pool._dialect.do_rollback(self)
                else:
                    self._reset_agent.rollback()
            else:
                pool._dialect.do_rollback(self)
        elif pool._reset_on_return is reset_commit:
            if self._echo:
                pool.logger.debug(
                    "Connection %s commit-on-return%s",
                    self.connection,
                    ", via agent" if self._reset_agent else "",
                )
            if self._reset_agent:
                if not self._reset_agent.is_active:
                    util.warn(
                        "Reset agent is not active.  "
                        "This should not occur unless there was already "
                        "a connectivity error in progress."
                    )
                    pool._dialect.do_commit(self)
                else:
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
        with the :attr:`._ConnectionRecord.info` and
        :attr:`_engine.Connection.info`
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
        of the :meth:`_engine.Connection.invalidate` method.   When invoked,
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
