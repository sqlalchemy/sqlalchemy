try:
    from threading import local
except ImportError:
    from sqlalchemy.util import ThreadLocal as local

from sqlalchemy import sql
from sqlalchemy.engine import create_engine
from sqlalchemy.types import TypeEngine

import thread, weakref

class ProxyEngine(object):
    """
    SQLEngine proxy. Supports lazy and late initialization by
    delegating to a real engine (set with connect()), and using proxy
    classes for TableImpl, ColumnImpl and TypeEngine.
    """

    def __init__(self):
        # create the local storage for uri->engine map and current engine
        self.storage = local()
        self.storage.connection = {}
        self.storage.engine = None
        self.tables = {}
            
    def connect(self, uri, opts=None, **kwargs):
        """Establish connection to a real engine.
        """
        key = "%s(%s,%s)" % (uri, repr(opts), repr(kwargs))
        try:
            map = self.storage.connection
        except AttributeError:
            self.storage.connection = {}
            self.storage.engine = None
            map = self.storage.connection
        try:
            self.engine = map[key]
        except KeyError:
            map[key] = create_engine(uri, opts, **kwargs)
            self.storage.engine = map[key]
            
    def get_engine(self):
        if self.storage.engine is None:
            raise AttributeError('No connection established')
        return self.storage.engine

    def set_engine(self, engine):
        self.storage.engine = engine
        
    engine = property(get_engine, set_engine)
            
    def hash_key(self):
        return "%s(%s)" % (self.__class__.__name__, id(self))

    def oid_column_name(self):
        # NOTE: setting up mappers fails unless the proxy engine returns
        # something for oid column name, and the call happens too early
        # to proxy, so effecticely no oids are allowed when using
        # proxy engine
        if self.storage.engine is None:
            return None
        return self.get_engine().oid_column_name()
    
    def columnimpl(self, column):
        """Proxy point: return a ProxyColumnImpl
        """
        return ProxyColumnImpl(self, column)

    def tableimpl(self, table):
        """Proxy point: return a ProxyTableImpl
        """
        return ProxyTableImpl(self, table)
        
    def type_descriptor(self, typeobj):
        """Proxy point: return a ProxyTypeEngine 
        """
        return ProxyTypeEngine(self, typeobj)

    def __getattr__(self, attr):
        # call get_engine() to give subclasses a chance to change
        # connection establishment behavior
        if self.get_engine() is not None:
            return getattr(self.engine, attr)
        raise AttributeError('No connection established in ProxyEngine: '
                             ' no access to %s' % attr)

        
class ProxyColumnImpl(sql.ColumnImpl):
    """Proxy column; defers engine access to ProxyEngine
    """
    def __init__(self, engine, column):
        sql.ColumnImpl.__init__(self, column)
        self._engine = engine
        self.impls = weakref.WeakKeyDictionary()
    def _get_impl(self):
        e = self._engine.engine
        try:
            return self.impls[e]
        except KeyError:
            impl = e.columnimpl(self.column)
            self.impls[e] = impl
    def __getattr__(self, key):
        return getattr(self._get_impl(), key)
    engine = property(lambda self: self._engine.engine)

class ProxyTableImpl(sql.TableImpl):
    """Proxy table; defers engine access to ProxyEngine
    """
    def __init__(self, engine, table):
        sql.TableImpl.__init__(self, table)
        self._engine = engine
        self.impls = weakref.WeakKeyDictionary()
    def _get_impl(self):
        e = self._engine.engine
        try:
            return self.impls[e]
        except KeyError:
            impl = e.tableimpl(self.table)
            self.impls[e] = impl
            return impl
    def __getattr__(self, key):
        return getattr(self._get_impl(), key)

    engine = property(lambda self: self._engine.engine)

class ProxyType(object):
    """ProxyType base class; used by ProxyTypeEngine to construct proxying
    types    
    """
    def __init__(self, engine, typeobj):
        self._engine = engine
        self.typeobj = typeobj

    def __getattribute__(self, attr):
        if attr.startswith('__') and attr.endswith('__'):
            return object.__getattribute__(self, attr)
        
        engine = object.__getattribute__(self, '_engine').engine
        typeobj = object.__getattribute__(self, 'typeobj')        
        return getattr(engine.type_descriptor(typeobj), attr)

    def __repr__(self):
        return '<Proxy %s>' % (object.__getattribute__(self, 'typeobj'))
    
class ProxyTypeEngine(object):
    """Proxy type engine; creates dynamic proxy type subclass that is instance
    of actual type, but proxies engine-dependant operations through the proxy
    engine.    
    """
    def __new__(cls, engine, typeobj):
        """Create a new subclass of ProxyType and typeobj
        so that internal isinstance() calls will get the expected result.
        """
        if isinstance(typeobj, type):
            typeclass = typeobj
        else:
            typeclass = typeobj.__class__
        typed = type('ProxyTypeHelper', (ProxyType, typeclass), {})
        return typed(engine, typeobj)    
