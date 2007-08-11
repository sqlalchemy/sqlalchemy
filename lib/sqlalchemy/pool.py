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

import weakref, time
try:
    import cPickle as pickle
except:
    import pickle

from sqlalchemy import exceptions, logging
from sqlalchemy import queue as Queue

try:
    import thread, threading
except:
    import dummy_thread as thread
    import dummy_threading as threading

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

    listeners
      A list of ``PoolListener``-like objects that receive events when
      DBAPI connections are created, checked out and checked in to the
      pool.

    """

    def __init__(self, creator, recycle=-1, echo=None, use_threadlocal=False,
                 listeners=None):
        self.logger = logging.instance_logger(self)
        self._threadconns = weakref.WeakValueDictionary()
        self._creator = creator
        self._recycle = recycle
        self._use_threadlocal = use_threadlocal
        self.echo = echo
        self.listeners = []
        self._on_connect = []
        self._on_checkout = []
        self._on_checkin = []
        if listeners:
            for l in listeners:
                self.add_listener(l)
    echo = logging.echo_property()

    def unique_connection(self):
        return _ConnectionFairy(self).checkout()

    def create_connection(self):
        return _ConnectionRecord(self)
    
    def recreate(self):
        """return a new instance of this Pool's class with identical creation arguments."""
        raise NotImplementedError()

    def dispose(self):
        """dispose of this pool.
        
        this method leaves the possibility of checked-out connections remaining opened,
        so it is advised to not reuse the pool once dispose() is called, and to instead
        use a new pool constructed by the recreate() method.
        """
        raise NotImplementedError()
        
    def connect(self):
        if not self._use_threadlocal:
            return _ConnectionFairy(self).checkout()

        try:
            return self._threadconns[thread.get_ident()].checkout()
        except KeyError:
            agent = _ConnectionFairy(self)
            self._threadconns[thread.get_ident()] = agent
            return agent.checkout()

    def return_conn(self, agent):
        if self._use_threadlocal and thread.get_ident() in self._threadconns:
            del self._threadconns[thread.get_ident()]
        self.do_return_conn(agent._connection_record)

    def get(self):
        return self.do_get()

    def do_get(self):
        raise NotImplementedError()

    def do_return_conn(self, conn):
        raise NotImplementedError()

    def status(self):
        raise NotImplementedError()

    def add_listener(self, listener):
        """Add a ``PoolListener``-like object to this pool."""

        self.listeners.append(listener)
        if hasattr(listener, 'connect'):
            self._on_connect.append(listener)
        if hasattr(listener, 'checkout'):
            self._on_checkout.append(listener)
        if hasattr(listener, 'checkin'):
            self._on_checkin.append(listener)

    def log(self, msg):
        self.logger.info(msg)

class _ConnectionRecord(object):
    def __init__(self, pool):
        self.__pool = pool
        self.connection = self.__connect()
        self.properties = {}
        if pool._on_connect:
            for l in pool._on_connect:
                l.connect(self.connection, self)

    def close(self):
        if self.connection is not None:
            self.__pool.log("Closing connection %s" % repr(self.connection))
            self.connection.close()

    def invalidate(self, e=None):
        if e is not None:
            self.__pool.log("Invalidate connection %s (reason: %s:%s)" % (repr(self.connection), e.__class__.__name__, str(e)))
        else:
            self.__pool.log("Invalidate connection %s" % repr(self.connection))
        self.__close()
        self.connection = None

    def get_connection(self):
        if self.connection is None:
            self.connection = self.__connect()
            self.properties.clear()
            if self.__pool._on_connect:
                for l in self.__pool._on_connect:
                    l.connect(self.connection, self)
        elif (self.__pool._recycle > -1 and time.time() - self.starttime > self.__pool._recycle):
            self.__pool.log("Connection %s exceeded timeout; recycling" % repr(self.connection))
            self.__close()
            self.connection = self.__connect()
            self.properties.clear()
            if self.__pool._on_connect:
                for l in self.__pool._on_connect:
                    l.connect(self.connection, self)
        return self.connection

    def __close(self):
        try:
            self.__pool.log("Closing connection %s" % (repr(self.connection)))
            self.connection.close()
        except Exception, e:
            self.__pool.log("Connection %s threw an error on close: %s" % (repr(self.connection), str(e)))
            if isinstance(e, (SystemExit, KeyboardInterrupt)):
                raise

    def __connect(self):
        try:
            self.starttime = time.time()
            connection = self.__pool._creator()
            self.__pool.log("Created new connection %s" % repr(connection))
            return connection
        except Exception, e:
            self.__pool.log("Error on connect(): %s" % (str(e)))
            raise

class _ConnectionFairy(object):
    """Proxy a DBAPI connection object and provides return-on-dereference support."""

    def __init__(self, pool):
        self._pool = pool
        self.__counter = 0
        try:
            self._connection_record = pool.get()
            self.connection = self._connection_record.get_connection()
        except:
            self.connection = None # helps with endless __getattr__ loops later on
            self._connection_record = None
            raise
        if self._pool.echo:
            self._pool.log("Connection %s checked out from pool" % repr(self.connection))
    
    _logger = property(lambda self: self._pool.logger)
    
    is_valid = property(lambda self:self.connection is not None)

    def _get_properties(self):
        """A property collection unique to this DBAPI connection."""
        
        try:
            return self._connection_record.properties
        except AttributeError:
            if self.connection is None:
                raise exceptions.InvalidRequestError("This connection is closed")
            try:
                return self._detatched_properties
            except AttributeError:
                self._detatched_properties = value = {}
                return value
    properties = property(_get_properties)
    
    def invalidate(self, e=None):
        """Mark this connection as invalidated.
        
        The connection will be immediately closed.  The 
        containing ConnectionRecord will create a new connection when next used.
        """
        if self.connection is None:
            raise exceptions.InvalidRequestError("This connection is closed")
        if self._connection_record is not None:
            self._connection_record.invalidate(e=e)
        self.connection = None
        self._close()

    def cursor(self, *args, **kwargs):
        try:
            c = self.connection.cursor(*args, **kwargs)
            return _CursorFairy(self, c)
        except Exception, e:
            self.invalidate(e=e)
            raise

    def __getattr__(self, key):
        return getattr(self.connection, key)

    def checkout(self):
        if self.connection is None:
            raise exceptions.InvalidRequestError("This connection is closed")
        self.__counter +=1

        if not self._pool._on_checkout or self.__counter != 1:
            return self

        # Pool listeners can trigger a reconnection on checkout
        attempts = 2
        while attempts > 0:
            try:
                for l in self._pool._on_checkout:
                    l.checkout(self.connection, self._connection_record)
                return self
            except exceptions.DisconnectionError, e:
                self._pool.log(
                    "Disconnection detected on checkout: %s" % (str(e)))
                self._connection_record.invalidate(e)
                self.connection = self._connection_record.get_connection()
                attempts -= 1

        self._pool.log("Reconnection attempts exhausted on checkout")
        self.invalidate()
        raise exceptions.InvalidRequestError("This connection is closed")

    def detach(self):
        """Separate this Connection from its Pool.
        
        This means that the connection will no longer be returned to the 
        pool when closed, and will instead be literally closed.  The 
        containing ConnectionRecord is separated from the DBAPI connection, and
        will create a new connection when next used.
        """
        
        if self._connection_record is not None:
            self._connection_record.connection = None        
            self._pool.do_return_conn(self._connection_record)
            self._detatched_properties = \
              self._connection_record.properties.copy()
            self._connection_record = None

    def close(self):
        self.__counter -=1
        if self.__counter == 0:
            self._close()

    def __del__(self):
        self._close()

    def _close(self):
        if self.connection is not None:
            try:
                self.connection.rollback()
                # Immediately close detached instances
                if self._connection_record is None:
                    self.connection.close()
            except Exception, e:
                if self._connection_record is not None:
                    self._connection_record.invalidate(e=e)
                if isinstance(e, (SystemExit, KeyboardInterrupt)):
                    raise
        if self._connection_record is not None:
            if self._pool.echo:
                self._pool.log("Connection %s being returned to pool" % repr(self.connection))
            if self._pool._on_checkin:
                for l in self._pool._on_checkin:
                    l.checkin(self.connection, self._connection_record)
            self._pool.return_conn(self)
        self.connection = None
        self._connection_record = None

class _CursorFairy(object):
    def __init__(self, parent, cursor):
        self.__parent = parent
        self.cursor = cursor

    def invalidate(self, e=None):
        self.__parent.invalidate(e=e)
    
    def close(self):
        try:
            self.cursor.close()
        except Exception, e:
            self.__parent._logger.warn("Error closing cursor: " + str(e))
            if isinstance(e, (SystemExit, KeyboardInterrupt)):
                raise

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

    def recreate(self):
        self.log("Pool recreating")
        return SingletonThreadPool(self._creator, pool_size=self.size, recycle=self._recycle, echo=self.echo, use_threadlocal=self._use_threadlocal)
        
    def dispose(self):
        """dispose of this pool.
        
        this method leaves the possibility of checked-out connections remaining opened,
        so it is advised to not reuse the pool once dispose() is called, and to instead
        use a new pool constructed by the recreate() method.
        """
        for key, conn in self._conns.items():
            try:
                conn.close()
            except (SystemExit, KeyboardInterrupt):
                raise
            except:
                # sqlite won't even let you close a conn from a thread 
                # that didn't create it
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
        self._overflow_lock = self._max_overflow > -1 and threading.Lock() or None

    def recreate(self):
        self.log("Pool recreating")
        return QueuePool(self._creator, pool_size=self._pool.maxsize, max_overflow=self._max_overflow, timeout=self._timeout, recycle=self._recycle, echo=self.echo, use_threadlocal=self._use_threadlocal)

    def do_return_conn(self, conn):
        try:
            self._pool.put(conn, False)
        except Queue.Full:
            if self._overflow_lock is None:
                self._overflow -= 1
            else:
                self._overflow_lock.acquire()
                try:
                    self._overflow -= 1
                finally:
                    self._overflow_lock.release()

    def do_get(self):
        try:
            wait = self._max_overflow > -1 and self._overflow >= self._max_overflow
            return self._pool.get(wait, self._timeout)
        except Queue.Empty:
            if self._max_overflow > -1 and self._overflow >= self._max_overflow:
                if not wait:
                    return self.do_get()
                else:
                    raise exceptions.TimeoutError("QueuePool limit of size %d overflow %d reached, connection timed out, timeout %d" % (self.size(), self.overflow(), self._timeout))

            if self._overflow_lock is not None:
                self._overflow_lock.acquire()

            if self._max_overflow > -1 and self._overflow >= self._max_overflow:
                if self._overflow_lock is not None:
                    self._overflow_lock.release()
                return self.do_get()

            try:
                con = self.create_connection()
                self._overflow += 1
            finally:
                if self._overflow_lock is not None:
                    self._overflow_lock.release()
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

class StaticPool(Pool):
    """A Pool implementation which stores exactly one connection that is 
    returned for all requests."""

    def __init__(self, creator, **params):
        Pool.__init__(self, creator, **params)
        self._conn = creator()
        self.connection = _ConnectionRecord(self)

    def status(self):
        return "StaticPool"

    def create_connection(self):
        return self._conn

    def do_return_conn(self, conn):
        pass

    def do_return_invalid(self, conn):
        pass

    def do_get(self):
        return self.connection
    
    
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
