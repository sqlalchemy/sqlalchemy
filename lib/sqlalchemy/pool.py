# pool.py - Connection pooling for SQLAlchemy
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.


"""provides a connection pool implementation, which optionally manages connections
on a thread local basis.  Also provides a DBAPI2 transparency layer so that pools can
be managed automatically, based on module type and connect arguments,
 simply by calling regular DBAPI connect() methods."""

import Queue, weakref, string

try:
    import thread
except:
    import dummythread as thread

proxies = {}

def manage(module, **params):
    """given a DBAPI2 module and pool management parameters, returns a proxy for the module that will
    automatically pool connections."""
    try:
        return proxies[module]
    except KeyError:
        return proxies.setdefault(module, DBProxy(module, **params))    

def clear_managers():
    """removes all current DBAPI2 managers.  all pools and connections are disposed."""
    proxies.clear()
    
class Pool(object):
    def __init__(self, echo = False, use_threadlocal = True):
        self._threadconns = weakref.WeakValueDictionary()
        self._use_threadlocal = use_threadlocal
        self._echo = echo
        
    def connect(self):
        if not self._use_threadlocal:
            return ConnectionFairy(self)
            
        try:
            return self._threadconns[thread.get_ident()]
        except KeyError:
            agent = ConnectionFairy(self)
            self._threadconns[thread.get_ident()] = agent
            return agent
            
    def get(self):
        raise NotImplementedError()
        
    def return_conn(self, conn):
        raise NotImplementedError()

    def log(self, msg):
        print msg

class ConnectionFairy:
    def __init__(self, pool):
        self.pool = pool
        self.connection = pool.get()
    def cursor(self):
        return CursorFairy(self, self.connection.cursor())
    def __getattr__(self, key):
        return getattr(self.connection, key)
    def __del__(self):
        self.pool.return_conn(self.connection)
            
class CursorFairy:
    def __init__(self, parent, cursor):
        self.parent = parent
        self.cursor = cursor
    def __getattr__(self, key):
        return getattr(self.cursor, key)
        
class QueuePool(Pool):
    def __init__(self, creator, pool_size = 5, max_overflow = 10, **params):
        Pool.__init__(self, **params)
        self._creator = creator
        self._pool = Queue.Queue(pool_size)
        self._overflow = 0 - pool_size
        self._max_overflow = max_overflow
    
    def return_conn(self, conn):
        if self._echo:
            self.log("return connection to pool")
        try:
            self._pool.put(conn, False)
        except Queue.Full:
            self._overflow -= 1

    def get(self):
        if self._echo:
            self.log("get connection from pool")
        try:
            return self._pool.get(self._max_overflow > -1 and self._overflow >= self._max_overflow)
        except Queue.Empty:
            self._overflow += 1
            return self._creator()

    def size(self):
        return self._pool.maxsize
    
    def checkedin(self):
        return self._pool.qsize()
    
    def overflow(self):
        return self._overflow
    
    def checkedout(self):
        return self._pool.maxsize - self._pool.qsize() + self._overflow
        

class DBProxy:
    """proxies a DBAPI2 connect() call to a pooled connection keyed to the specific connect parameters."""
    
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

    def get_pool(self, *args, **params):
        key = self._serialize(*args, **params)
        try:
            return self.pools[key]
        except KeyError:
            pool = self.poolclass(lambda: self.module.connect(*args, **params), **self.params)
            self.pools[key] = pool
            return pool
        
    def connect(self, *args, **params):
        """connects to a database using this DBProxy's module and the given connect arguments.  if the
        arguments match an existing pool, the connection will be returned from the pool's current 
        thread-local connection instance, or if there is no thread-local connection instance it will
        be checked out from the set of pooled connections.  If the pool has no available connections
        and allows new connections to be created, a new database connection will be made."""
        return self.get_pool(*args, **params).connect()
    
    def _serialize(self, *args, **params):
        return string.join([repr(a) for a in args], "&") + "&" + string.join(["%s=%s" % (key, repr(value)) for key, value in params.iteritems()], "&")    

