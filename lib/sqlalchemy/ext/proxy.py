try:
    from threading import local
except ImportError:
    from sqlalchemy.util import ThreadLocal as local

from sqlalchemy import sql
from sqlalchemy.engine import create_engine, Engine

__all__ = ['BaseProxyEngine', 'AutoConnectEngine', 'ProxyEngine']

class BaseProxyEngine(sql.Executor):
    """Basis for all proxy engines."""
        
    def get_engine(self):
        raise NotImplementedError

    def set_engine(self, engine):
        raise NotImplementedError
        
    engine = property(lambda s:s.get_engine(), lambda s,e:s.set_engine(e))
    
    def execute_compiled(self, *args, **kwargs):
        """this method is required to be present as it overrides the execute_compiled present in sql.Engine"""
        return self.get_engine().execute_compiled(*args, **kwargs) 
    def compiler(self, *args, **kwargs): 
        """this method is required to be present as it overrides the compiler method present in sql.Engine"""
        return self.get_engine().compiler(*args, **kwargs) 

    def __getattr__(self, attr):
        """provides proxying for methods that are not otherwise present on this BaseProxyEngine.  Note 
        that methods which are present on the base class sql.Engine will *not* be proxied through this,
        and must be explicit on this class."""
        # call get_engine() to give subclasses a chance to change
        # connection establishment behavior
        e = self.get_engine()
        if e is not None:
            return getattr(e, attr)
        raise AttributeError("No connection established in ProxyEngine: "
                             " no access to %s" % attr)


class AutoConnectEngine(BaseProxyEngine):
    """An SQLEngine proxy that automatically connects when necessary."""
    
    def __init__(self, dburi, **kwargs):
        BaseProxyEngine.__init__(self)
        self.dburi = dburi
        self.kwargs = kwargs
        self._engine = None
        
    def get_engine(self):
        if self._engine is None:
            if callable(self.dburi):
                dburi = self.dburi()
            else:
                dburi = self.dburi
            self._engine = create_engine(dburi, **self.kwargs)
        return self._engine


class ProxyEngine(BaseProxyEngine):
    """Engine proxy for lazy and late initialization.
    
    This engine will delegate access to a real engine set with connect().
    """

    def __init__(self, **kwargs):
        BaseProxyEngine.__init__(self)
        # create the local storage for uri->engine map and current engine
        self.storage = local()
        self.kwargs = kwargs

    def connect(self, *args, **kwargs):
        """Establish connection to a real engine."""

        kwargs.update(self.kwargs)
        if not kwargs:
            key = repr(args)
        else:
            key = "%s, %s" % (repr(args), repr(sorted(kwargs.items())))
        try:
            map = self.storage.connection
        except AttributeError:
            self.storage.connection = {}
            self.storage.engine = None
            map = self.storage.connection
        try:
            self.storage.engine = map[key]
        except KeyError:
            map[key] = create_engine(*args, **kwargs)
            self.storage.engine = map[key]
            
    def get_engine(self):
        if not hasattr(self.storage, 'engine') or self.storage.engine is None:
            raise AttributeError("No connection established")
        return self.storage.engine

    def set_engine(self, engine):
        self.storage.engine = engine
