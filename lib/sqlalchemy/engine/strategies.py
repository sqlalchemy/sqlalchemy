"""defines different strategies for creating new instances of sql.Engine.  
by default there are two, one which is the "thread-local" strategy, one which is the "plain" strategy. 
new strategies can be added via constructing a new EngineStrategy object which will add itself to the
list of available strategies here, or replace one of the existing name.  
this can be accomplished via a mod; see the sqlalchemy/mods package for details."""


from sqlalchemy.engine import base, default, threadlocal, url

strategies = {}

class EngineStrategy(object):
    """defines a function that receives input arguments and produces an instance of sql.Engine, typically
    an instance sqlalchemy.engine.base.ComposedSQLEngine or a subclass."""
    def __init__(self, name):
        """constructs a new EngineStrategy object and sets it in the list of available strategies
        under this name."""
        self.name = name
        strategies[self.name] = self
    def create(self, *args, **kwargs):
        """given arguments, returns a new sql.Engine instance."""
        raise NotImplementedError()

class DefaultEngineStrategy(EngineStrategy):
    def create(self, name_or_url, **kwargs):    
        u = url.make_url(name_or_url)
        module = u.get_module()

        dialect = module.dialect(**kwargs)

        poolargs = {}
        for key in (('echo_pool', 'echo'), ('pool_size', 'pool_size'), ('max_overflow', 'max_overflow'), ('poolclass', 'poolclass'), ('pool_timeout','timeout'), ('pool', 'pool'), ('pool_recycle','recycle'),('connect_args', 'connect_args'), ('creator', 'creator')):
           if kwargs.has_key(key[0]):
               poolargs[key[1]] = kwargs[key[0]]
        poolclass = getattr(module, 'poolclass', None)
        if poolclass is not None:
           poolargs.setdefault('poolclass', poolclass)
        poolargs['use_threadlocal'] = self.pool_threadlocal()
        provider = self.get_pool_provider(dialect, u, **poolargs)

        return self.get_engine(provider, dialect, **kwargs)

    def pool_threadlocal(self):
        raise NotImplementedError()
    def get_pool_provider(self, dialect, url, **kwargs):
        raise NotImplementedError()
    def get_engine(self, provider, dialect, **kwargs):
        raise NotImplementedError()
           
class PlainEngineStrategy(DefaultEngineStrategy):
    def __init__(self):
        DefaultEngineStrategy.__init__(self, 'plain')
    def pool_threadlocal(self):
        return False
    def get_pool_provider(self, dialect, url, **poolargs):
        return default.PoolConnectionProvider(dialect, url, **poolargs)
    def get_engine(self, provider, dialect, **kwargs):
        return base.ComposedSQLEngine(provider, dialect, **kwargs)
PlainEngineStrategy()

class ThreadLocalEngineStrategy(DefaultEngineStrategy):
    def __init__(self):
        DefaultEngineStrategy.__init__(self, 'threadlocal')
    def pool_threadlocal(self):
        return True
    def get_pool_provider(self, dialect, url, **poolargs):
        return threadlocal.TLocalConnectionProvider(dialect, url, **poolargs)
    def get_engine(self, provider, dialect, **kwargs):
        return threadlocal.TLEngine(provider, dialect, **kwargs)
ThreadLocalEngineStrategy()


    
