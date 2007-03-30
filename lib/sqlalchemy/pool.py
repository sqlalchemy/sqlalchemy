# pool.py - Connection pooling for SQLAlchemy
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""Provide a connection pool implementation, which optionally manages
connections on a thread local basis.

Also provides a DBAPI2 transparency layer so that pools can be managed
automatically, based on module type and connect arguments, simply by
calling regular DBAPI connect() methods.
"""

import weakref, string, time, sys, traceback
try:
    import cPickle as pickle
except:
    import pickle

from sqlalchemy import exceptions, logging
from sqlalchemy import queue as Queue

try:
    import thread
except:
    import dummy_thread as thread

proxies = {}

def manage(module, **params):
    """Return a proxy for module that automatically pools connections.

    Given a DBAPI2 module and pool management parameters, returns a
    proxy for the module that will automatically pool connections,
    creating new connection pools for each distinct set of connection
    arguments sent to the decorated module's connect() function.

    Arguments:

    module
      A DBAPI2 database module.

    poolclass
      The class used by the pool module to provide pooling.
      Defaults to ``QueuePool``.

    See the ``Pool`` class for options.
    """
    try:
        return proxies[module]
    except KeyError:
        return proxies.setdefault(module, _DBProxy(module, **params))

def clear_managers():
    """Remove all current DBAPI2 managers.

    All pools and connections are disposed.
    """

    for manager in proxies.values():
        manager.close()
    proxies.clear()

class Pool(object):
    """Base Pool class.

    This is an abstract class, which is implemented by various
    subclasses including:

    QueuePool
      Pools multiple connections using ``Queue.Queue``.

    SingletonThreadPool
      Stores a single connection per execution thread.

    NullPool
      Doesn't do any pooling; opens and closes connections.

    AssertionPool
      Stores only one connection, and asserts that only one connection
      is checked out at a time.

    The main argument, `creator`, is a callable function that returns
    a newly connected DBAPI connection object.

    Options that are understood by Pool are:

    echo
      If set to True, connections being pulled and retrieved from/to
      the pool will be logged to the standard output, as well as pool
      sizing information.  Echoing can also be achieved by enabling
      logging for the "sqlalchemy.pool" namespace. Defaults to False.

    use_threadlocal
      If set to True, repeated calls to ``connect()`` within the same
      application thread will be guaranteed to return the same
      connection object, if one has already been retrieved from the
      pool and has not been returned yet. This allows code to retrieve
      a connection from the pool, and then while still holding on to
      that connection, to call other functions which also ask the pool
      for a connection of the same arguments; those functions will act
      upon the same connection that the calling method is using.
      Defaults to True.

    recycle
      If set to non -1, a number of seconds between connection
      recycling, which means upon checkout, if this timeout is
      surpassed the connection will be closed and replaced with a
      newly opened connection. Defaults to -1.

    auto_close_cursors
      Cursors, returned by ``connection.cursor()``, are tracked and
      are automatically closed when the connection is returned to the
      pool.  Some DBAPIs like MySQLDB become unstable if cursors
      remain open. Defaults to True.

    disallow_open_cursors
      If `auto_close_cursors` is False, and `disallow_open_cursors` is
      True, will raise an exception if an open cursor is detected upon
      connection checkin. Defaults to False.

    If `auto_close_cursors` and `disallow_open_cursors` are both
    False, then no cursor processing occurs upon checkin.
    """

    def __init__(self, creator, recycle=-1, echo=None, use_threadlocal=False, auto_close_cursors=True, disallow_open_cursors=False):
        self.logger = logging.instance_logger(self)
        self._threadconns = weakref.WeakValueDictionary()
        self._creator = creator
        self._recycle = recycle
        self._use_threadlocal = use_threadlocal
        self.auto_close_cursors = auto_close_cursors
        self.disallow_open_cursors = disallow_open_cursors
        self.echo = echo
    echo = logging.echo_property()

    def unique_connection(self):
        return _ConnectionFairy(self).checkout()

    def create_connection(self):
        return _ConnectionRecord(self)

    def connect(self):
        if not self._use_threadlocal:
            return _ConnectionFairy(self).checkout()

        try:
            return self._threadconns[thread.get_ident()].connfairy().checkout()
        except KeyError:
            agent = _ConnectionFairy(self).checkout()
            self._threadconns[thread.get_ident()] = agent._threadfairy
            return agent

    def return_conn(self, agent):
        self.do_return_conn(agent._connection_record)

    def get(self):
        return self.do_get()

    def do_get(self):
        raise NotImplementedError()

    def do_return_conn(self, conn):
        raise NotImplementedError()

    def status(self):
        raise NotImplementedError()

    def log(self, msg):
        self.logger.info(msg)

    def dispose(self):
        raise NotImplementedError()

class _ConnectionRecord(object):
    def __init__(self, pool):
        self.__pool = pool
        self.connection = self.__connect()

    def close(self):
        self.__pool.log("Closing connection %s" % repr(self.connection))
        self.connection.close()

    def invalidate(self):
        self.__pool.log("Invalidate connection %s" % repr(self.connection))
        self.__close()
        self.connection = None

    def get_connection(self):
        if self.connection is None:
            self.connection = self.__connect()
        elif (self.__pool._recycle > -1 and time.time() - self.starttime > self.__pool._recycle):
            self.__pool.log("Connection %s exceeded timeout; recycling" % repr(self.connection))
            self.__close()
            self.connection = self.__connect()
        return self.connection

    def __close(self):
        try:
            self.__pool.log("Closing connection %s" % (repr(self.connection)))
            self.connection.close()
        except Exception, e:
            self.__pool.log("Connection %s threw an error on close: %s" % (repr(self.connection), str(e)))

    def __connect(self):
        try:
            self.starttime = time.time()
            connection = self.__pool._creator()
            self.__pool.log("Created new connection %s" % repr(connection))
            return connection
        except Exception, e:
            self.__pool.log("Error on connect(): %s" % (str(e)))
            raise

class _ThreadFairy(object):
    """Mark a thread identifier as owning a connection, for a thread local pool."""

    def __init__(self, connfairy):
        self.connfairy = weakref.ref(connfairy)

class _ConnectionFairy(object):
    """Proxy a DBAPI connection object and provides return-on-dereference support."""

    def __init__(self, pool):
        self._threadfairy = _ThreadFairy(self)
        self._cursors = weakref.WeakKeyDictionary()
        self.__pool = pool
        self.__counter = 0
        try:
            self._connection_record = pool.get()
            self.connection = self._connection_record.get_connection()
        except:
            self.connection = None # helps with endless __getattr__ loops later on
            self._connection_record = None
            raise
        if self.__pool.echo:
            self.__pool.log("Connection %s checked out from pool" % repr(self.connection))

    def invalidate(self):
        if self.connection is None:
            raise exceptions.InvalidRequestError("This connection is closed")
        self._connection_record.invalidate()
        self.connection = None
        self._cursors = None
        self._close()

    def cursor(self, *args, **kwargs):
        try:
            return _CursorFairy(self, self.connection.cursor(*args, **kwargs))
        except Exception, e:
            self.invalidate()
            raise

    def __getattr__(self, key):
        return getattr(self.connection, key)

    def checkout(self):
        if self.connection is None:
            raise exceptions.InvalidRequestError("This connection is closed")
        self.__counter +=1
        return self

    def close_open_cursors(self):
        if self._cursors is not None:
            for c in list(self._cursors):
                c.close()

    def close(self):
        self.__counter -=1
        if self.__counter == 0:
            self._close()

    def __del__(self):
        self._close()

    def _close(self):
        if self._cursors is not None:
            # cursors should be closed before connection is returned to the pool.  some dbapis like
            # mysql have real issues if they are not.
            if self.__pool.auto_close_cursors:
                self.close_open_cursors()
            elif self.__pool.disallow_open_cursors:
                if len(self._cursors):
                    raise exceptions.InvalidRequestError("This connection still has %d open cursors" % len(self._cursors))
        if self.connection is not None:
            try:
                self.connection.rollback()
            except:
                if self._connection_record is not None:
                    self._connection_record.invalidate()
        if self._connection_record is not None:
            if self.__pool.echo:
                self.__pool.log("Connection %s being returned to pool" % repr(self.connection))
            self.__pool.return_conn(self)
        self.connection = None
        self._connection_record = None
        self._threadfairy = None
        self._cursors = None

class _CursorFairy(object):
    def __init__(self, parent, cursor):
        self.__parent = parent
        self.__parent._cursors[self]=True
        self.cursor = cursor

    def invalidate(self):
        self.__parent.invalidate()

    def close(self):
        if self in self.__parent._cursors:
            del self.__parent._cursors[self]
            self.cursor.close()

    def __getattr__(self, key):
        return getattr(self.cursor, key)

class SingletonThreadPool(Pool):
    """Maintain one connection per each thread, never moving a
    connection to a thread other than the one which it was created in.

    This is used for SQLite, which both does not handle multithreading
    by default, and also requires a singleton connection if a :memory:
    database is being used.

    Options are the same as those of Pool, as well as:

    pool_size : 5
      The number of threads in which to maintain connections at once.
    """

    def __init__(self, creator, pool_size=5, **params):
        params['use_threadlocal'] = True
        Pool.__init__(self, creator, **params)
        self._conns = {}
        self.size = pool_size

    def dispose(self):
        for key, conn in self._conns.items():
            try:
                conn.close()
            except:
                # sqlite won't even let you close a conn from a thread that didn't create it
                pass
            del self._conns[key]

    def dispose_local(self):
        try:
            del self._conns[thread.get_ident()]
        except KeyError:
            pass

    def cleanup(self):
        for key in self._conns.keys():
            try:
                del self._conns[key]
            except KeyError:
                pass
            if len(self._conns) <= self.size:
                return

    def status(self):
        return "SingletonThreadPool id:%d thread:%d size: %d" % (id(self), thread.get_ident(), len(self._conns))

    def do_return_conn(self, conn):
        pass

    def do_get(self):
        try:
            return self._conns[thread.get_ident()]
        except KeyError:
            c = self.create_connection()
            self._conns[thread.get_ident()] = c
            if len(self._conns) > self.size:
                self.cleanup()
            return c

class QueuePool(Pool):
    """Use ``Queue.Queue`` to maintain a fixed-size list of connections.

    Arguments include all those used by the base Pool class, as well
    as:

    pool_size
      The size of the pool to be maintained. This is the largest
      number of connections that will be kept persistently in the
      pool. Note that the pool begins with no connections; once this
      number of connections is requested, that number of connections
      will remain. Defaults to 5.

    max_overflow
      The maximum overflow size of the pool. When the number of
      checked-out connections reaches the size set in pool_size,
      additional connections will be returned up to this limit. When
      those additional connections are returned to the pool, they are
      disconnected and discarded. It follows then that the total
      number of simultaneous connections the pool will allow is
      pool_size + `max_overflow`, and the total number of "sleeping"
      connections the pool will allow is pool_size. `max_overflow` can
      be set to -1 to indicate no overflow limit; no limit will be
      placed on the total number of concurrent connections. Defaults
      to 10.

    timeout
      The number of seconds to wait before giving up on returning a
      connection. Defaults to 30.
    """

    def __init__(self, creator, pool_size = 5, max_overflow = 10, timeout=30, **params):
        Pool.__init__(self, creator, **params)
        self._pool = Queue.Queue(pool_size)
        self._overflow = 0 - pool_size
        self._max_overflow = max_overflow
        self._timeout = timeout

    def do_return_conn(self, conn):
        try:
            self._pool.put(conn, False)
        except Queue.Full:
            self._overflow -= 1

    def do_get(self):
        try:
            return self._pool.get(self._max_overflow > -1 and self._overflow >= self._max_overflow, self._timeout)
        except Queue.Empty:
            if self._max_overflow > -1 and self._overflow >= self._max_overflow:
                raise exceptions.TimeoutError("QueuePool limit of size %d overflow %d reached, connection timed out" % (self.size(), self.overflow()))
            con = self.create_connection()
            self._overflow += 1
            return con

    def dispose(self):
        while True:
            try:
                conn = self._pool.get(False)
                conn.close()
            except Queue.Empty:
                break

        self._overflow = 0 - self.size()
        self.log("Pool disposed. " + self.status())

    def status(self):
        tup = (self.size(), self.checkedin(), self.overflow(), self.checkedout())
        return "Pool size: %d  Connections in pool: %d Current Overflow: %d Current Checked out connections: %d" % tup

    def size(self):
        return self._pool.maxsize

    def checkedin(self):
        return self._pool.qsize()

    def overflow(self):
        return self._overflow

    def checkedout(self):
        return self._pool.maxsize - self._pool.qsize() + self._overflow

class NullPool(Pool):
    """A Pool implementation which does not pool connections.

    Instead it literally opens and closes the underlying DBAPI
    connection per each connection open/close.
    """

    def status(self):
        return "NullPool"

    def do_return_conn(self, conn):
       conn.close()

    def do_return_invalid(self, conn):
       pass

    def do_get(self):
        return self.create_connection()

class AssertionPool(Pool):
    """A Pool implementation that allows at most one checked out
    connection at a time.

    This will raise an exception if more than one connection is
    checked out at a time.  Useful for debugging code that is using
    more connections than desired.
    """

    ## TODO: modify this to handle an arbitrary connection count.

    def __init__(self, creator, **params):
        Pool.__init__(self, creator, **params)
        self.connection = _ConnectionRecord(self)
        self._conn = self.connection

    def status(self):
        return "AssertionPool"

    def create_connection(self):
        raise "Invalid"

    def do_return_conn(self, conn):
        assert conn is self._conn and self.connection is None
        self.connection = conn

    def do_return_invalid(self, conn):
        raise "Invalid"

    def do_get(self):
        assert self.connection is not None
        c = self.connection
        self.connection = None
        return c

class _DBProxy(object):
    """Proxy a DBAPI2 connect() call to a pooled connection keyed to
    the specific connect parameters. Other attributes are proxied
    through via __getattr__.
    """

    def __init__(self, module, poolclass = QueuePool, **params):
        """Initialize a new proxy.

        module
          a DBAPI2 module.

        poolclass
          a Pool class, defaulting to QueuePool.

        Other parameters are sent to the Pool object's constructor.
        """

        self.module = module
        self.params = params
        self.poolclass = poolclass
        self.pools = {}

    def close(self):
        for key in self.pools.keys():
            del self.pools[key]

    def __del__(self):
        self.close()

    def __getattr__(self, key):
        return getattr(self.module, key)

    def get_pool(self, *args, **params):
        key = self._serialize(*args, **params)
        try:
            return self.pools[key]
        except KeyError:
            pool = self.poolclass(lambda: self.module.connect(*args, **params), **self.params)
            self.pools[key] = pool
            return pool

    def connect(self, *args, **params):
        """Activate a connection to the database.

        Connect to the database using this DBProxy's module and the
        given connect arguments.  If the arguments match an existing
        pool, the connection will be returned from the pool's current
        thread-local connection instance, or if there is no
        thread-local connection instance it will be checked out from
        the set of pooled connections.

        If the pool has no available connections and allows new
        connections to be created, a new database connection will be
        made.
        """

        return self.get_pool(*args, **params).connect()

    def dispose(self, *args, **params):
        """Dispose the connection pool referenced by the given connect arguments."""

        key = self._serialize(*args, **params)
        try:
            del self.pools[key]
        except KeyError:
            pass

    def _serialize(self, *args, **params):
        return pickle.dumps([args, params])
