# pool.py - Connection pooling for SQLAlchemy
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""provides a connection pool implementation, which optionally manages connections
on a thread local basis.  Also provides a DBAPI2 transparency layer so that pools can
be managed automatically, based on module type and connect arguments,
 simply by calling regular DBAPI connect() methods."""

import Queue, weakref, string, cPickle
import util

try:
    import thread
except:
    import dummythread as thread

proxies = {}

def manage(module, **params):
    """given a DBAPI2 module and pool management parameters, returns a proxy for the module
    that will automatically pool connections.  Options are delivered to an underlying DBProxy
    object.

    Arguments:
    module : a DBAPI2 database module.
    
    Options:
    echo=False : if set to True, connections being pulled and retrieved from/to the pool will
    be logged to the standard output, as well as pool sizing information.

    use_threadlocal=True : if set to True, repeated calls to connect() within the same
    application thread will be guaranteed to return the same connection object, if one has
    already been retrieved from the pool and has not been returned yet. This allows code to
    retrieve a connection from the pool, and then while still holding on to that connection,
    to call other functions which also ask the pool for a connection of the same arguments;
    those functions will act upon the same connection that the calling method is using.

    poolclass=QueuePool : the default class used by the pool module to provide pooling.
    QueuePool uses the Python Queue.Queue class to maintain a list of available connections.

    pool_size=5 : used by QueuePool - the size of the pool to be maintained. This is the
    largest number of connections that will be kept persistently in the pool. Note that the
    pool begins with no connections; once this number of connections is requested, that
    number of connections will remain.

    max_overflow=10 : the maximum overflow size of the pool. When the number of checked-out
    connections reaches the size set in pool_size, additional connections will be returned up
    to this limit. When those additional connections are returned to the pool, they are
    disconnected and discarded. It follows then that the total number of simultaneous
    connections the pool will allow is pool_size + max_overflow, and the total number of
    "sleeping" connections the pool will allow is pool_size. max_overflow can be set to -1 to
    indicate no overflow limit; no limit will be placed on the total number of concurrent
    connections.
    
    """
    try:
        return proxies[module]
    except KeyError:
        return proxies.setdefault(module, DBProxy(module, **params))    

def clear_managers():
    """removes all current DBAPI2 managers.  all pools and connections are disposed."""
    for manager in proxies.values():
        manager.close()
    proxies.clear()

    
class Pool(object):
    def __init__(self, echo = False, use_threadlocal = True, logger=None):
        self._threadconns = weakref.WeakValueDictionary()
        self._use_threadlocal = use_threadlocal
        self._echo = echo
        self._logger = logger or util.Logger(origin='pool')
        
    def connect(self):
        if not self._use_threadlocal:
            return ConnectionFairy(self)
            
        try:
            return self._threadconns[thread.get_ident()]
        except KeyError:
            agent = ConnectionFairy(self)
            self._threadconns[thread.get_ident()] = agent
            return agent

    def return_conn(self, conn):
        if self._echo:
            self.log("return connection to pool")
        self.do_return_conn(conn)
        
    def get(self):
        if self._echo:
            self.log("get connection from pool")
            self.log(self.status())
        return self.do_get()
    
    def return_invalid(self):
        if self._echo:
            self.log("return invalid connection to pool")
            self.log(self.status())
        self.do_return_invalid()
            
    def do_get(self):
        raise NotImplementedError()
        
    def do_return_conn(self, conn):
        raise NotImplementedError()
        
    def do_return_invalid(self):
        raise NotImplementedError()
        
    def status(self):
        raise NotImplementedError()

    def log(self, msg):
        self.logger.write(msg)

class ConnectionFairy(object):
    def __init__(self, pool):
        self.pool = pool
        try:
            self.connection = pool.get()
        except:
            self.connection = None
            self.pool.return_invalid()
            raise
    def cursor(self):
        return CursorFairy(self, self.connection.cursor())
    def __getattr__(self, key):
        return getattr(self.connection, key)
    def __del__(self):
        if self.connection is not None:
            self.pool.return_conn(self.connection)
            self.pool = None
            self.connection = None
            
class CursorFairy(object):
    def __init__(self, parent, cursor):
        self.parent = parent
        self.cursor = cursor
    def __getattr__(self, key):
        return getattr(self.cursor, key)

class SingletonThreadPool(Pool):
    """Maintains one connection per each thread, never moving to another thread.  this is
    used for SQLite and other databases with a similar restriction."""
    def __init__(self, creator, **params):
        params['use_threadlocal'] = False
        Pool.__init__(self, **params)
        self._conns = {}
        self._creator = creator

    def status(self):
        return "SingletonThreadPool size: %d" % len(self._conns)

    def do_return_conn(self, conn):
        pass
    def do_return_invalid(self):
        try:
            del self._conns[thread.get_ident()]
        except KeyError:
            pass
            
    def do_get(self):
        try:
            return self._conns[thread.get_ident()]
        except KeyError:
            return self._conns.setdefault(thread.get_ident(), self._creator())
    
class QueuePool(Pool):
    """uses Queue.Queue to maintain a fixed-size list of connections."""
    def __init__(self, creator, pool_size = 5, max_overflow = 10, **params):
        Pool.__init__(self, **params)
        self._creator = creator
        self._pool = Queue.Queue(pool_size)
        self._overflow = 0 - pool_size
        self._max_overflow = max_overflow
    
    def do_return_conn(self, conn):
        if self._echo:
            self.log("return connection to pool")
        try:
            self._pool.put(conn, False)
        except Queue.Full:
            self._overflow -= 1

    def do_return_invalid(self):
        if self._echo:
            self.log("return invalid connection")
        if self._pool.full():
            self._overflow -= 1
        
    def do_get(self):
        if self._echo:
            self.log("get connection from pool")
            self.log(self.status())
        try:
            return self._pool.get(self._max_overflow > -1 and self._overflow >= self._max_overflow)
        except Queue.Empty:
            self._overflow += 1
            return self._creator()

    def __del__(self):
        while True:
            try:
                conn = self._pool.get(False)
                conn.close()
            except Queue.Empty:
                break

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
        

class DBProxy(object):
    """proxies a DBAPI2 connect() call to a pooled connection keyed to the specific connect
    parameters."""
    
    def __init__(self, module, poolclass = QueuePool, **params):
        """
        module is a DBAPI2 module
        poolclass is a Pool class, defaulting to QueuePool.
        other parameters are sent to the Pool object's constructor.
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
            
    def get_pool(self, *args, **params):
        key = self._serialize(*args, **params)
        try:
            return self.pools[key]
        except KeyError:
            pool = self.poolclass(lambda: self.module.connect(*args, **params), **self.params)
            self.pools[key] = pool
            return pool
        
    def connect(self, *args, **params):
        """connects to a database using this DBProxy's module and the given connect
        arguments.  if the arguments match an existing pool, the connection will be returned
        from the pool's current thread-local connection instance, or if there is no
        thread-local connection instance it will be checked out from the set of pooled
        connections.  If the pool has no available connections and allows new connections to
        be created, a new database connection will be made."""
        return self.get_pool(*args, **params).connect()
    
    def dispose(self, *args, **params):
        """disposes the connection pool referenced by the given connect arguments."""
        key = self._serialize(*args, **params)
        try:
            del self.pools[key]
        except KeyError:
            pass
        
    def _serialize(self, *args, **params):
        return cPickle.dumps([args, params])

