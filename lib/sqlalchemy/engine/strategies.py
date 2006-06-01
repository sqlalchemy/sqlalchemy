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
    

class PlainEngineStrategy(EngineStrategy):
    def __init__(self):
        EngineStrategy.__init__(self, 'plain')
    def create(self, name_or_url, **kwargs):
        u = url.make_url(name_or_url)
        module = u.get_module()

        args = u.query.copy()
        args.update(kwargs)
        dialect = module.dialect(**args)

        poolargs = {}
        for key in (('echo_pool', 'echo'), ('pool_size', 'pool_size'), ('max_overflow', 'max_overflow'), ('poolclass', 'poolclass'), ('pool_timeout','timeout'), ('pool', 'pool')):
            if kwargs.has_key(key[0]):
                poolargs[key[1]] = kwargs[key[0]]
        poolclass = getattr(module, 'poolclass', None)
        if poolclass is not None:
            poolargs.setdefault('poolclass', poolclass)
        poolargs['use_threadlocal'] = False
        provider = default.PoolConnectionProvider(dialect, u, **poolargs)

        return base.ComposedSQLEngine(provider, dialect, **args)
PlainEngineStrategy()

class ThreadLocalEngineStrategy(EngineStrategy):
    def __init__(self):
        EngineStrategy.__init__(self, 'threadlocal')
    def create(self, name_or_url, **kwargs):
        u = url.make_url(name_or_url)
        module = u.get_module()

        args = u.query.copy()
        args.update(kwargs)
        dialect = module.dialect(**args)

        poolargs = {}
        for key in (('echo_pool', 'echo'), ('pool_size', 'pool_size'), ('max_overflow', 'max_overflow'), ('poolclass', 'poolclass'), ('pool_timeout','timeout'), ('pool', 'pool')):
            if kwargs.has_key(key[0]):
                poolargs[key[1]] = kwargs[key[0]]
        poolclass = getattr(module, 'poolclass', None)
        if poolclass is not None:
            poolargs.setdefault('poolclass', poolclass)
        poolargs['use_threadlocal'] = True
        provider = threadlocal.TLocalConnectionProvider(dialect, u, **poolargs)

        return threadlocal.TLEngine(provider, dialect, **args)
ThreadLocalEngineStrategy()


    
