from sqlalchemy.util import ScopedRegistry, to_list
from sqlalchemy.orm import MapperExtension, EXT_CONTINUE, object_session
from sqlalchemy.orm.session import Session
from sqlalchemy import exceptions
import types

__all__ = ['ScopedSession']


class ScopedSession(object):
    """Provides thread-local management of Sessions.

    Usage::

      Session = scoped_session(sessionmaker(autoflush=True))
      
      To map classes so that new instances are saved in the current
      Session automatically, as well as to provide session-aware
      class attributes such as "query":
      
      mapper = Session.mapper
      mapper(Class, table, ...)

    """

    def __init__(self, session_factory, scopefunc=None):
        self.session_factory = session_factory
        self.registry = ScopedRegistry(session_factory, scopefunc)
        self.extension = _ScopedExt(self)

    def __call__(self, **kwargs):
        if kwargs:
            scope = kwargs.pop('scope', False)
            if scope is not None:
                if self.registry.has():
                    raise exceptions.InvalidRequestError("Scoped session is already present; no new arguments may be specified.")
                else:
                    sess = self.session_factory(**kwargs)
                    self.registry.set(sess)
                    return sess
            else:
                return self.session_factory(**kwargs)
        else:
            return self.registry()
    
    def remove(self):
        self.registry.clear()
    
    def mapper(self, *args, **kwargs):
        """return a mapper() function which associates this ScopedSession with the Mapper."""
        
        from sqlalchemy.orm import mapper
        validate = kwargs.pop('validate', False)
        extension = to_list(kwargs.setdefault('extension', []))
        if validate:
            extension.append(self.extension.validating())
        else:
            extension.append(self.extension)
        return mapper(*args, **kwargs)
        
    def configure(self, **kwargs):
        """reconfigure the sessionmaker used by this ScopedSession."""
        
        self.session_factory.configure(**kwargs)

def instrument(name):
    def do(self, *args, **kwargs):
        return getattr(self.registry(), name)(*args, **kwargs)
    return do
for meth in ('get', 'load', 'close', 'save', 'commit', 'update', 'flush', 'query', 'delete', 'merge', 'clear', 'refresh', 'expire', 'expunge', 'rollback', 'begin', 'begin_nested', 'connection', 'execute', 'scalar', 'get_bind'):
    setattr(ScopedSession, meth, instrument(meth))

def makeprop(name):
    def set(self, attr):
        setattr(self.registry(), name, attr)
    def get(self):
        return getattr(self.registry(), name)
    return property(get, set)
for prop in ('bind', 'dirty', 'deleted', 'new', 'identity_map'):
    setattr(ScopedSession, prop, makeprop(prop))

def clslevel(name):
    def do(cls, *args,**kwargs):
        return getattr(Session, name)(*args, **kwargs)
    return classmethod(do)
for prop in ('close_all','object_session', 'identity_key'):
    setattr(ScopedSession, prop, clslevel(prop))
    
class _ScopedExt(MapperExtension):
    def __init__(self, context, validate=False):
        self.context = context
        self.validate = validate
    
    def validating(self):
        return _ScopedExt(self.context, validate=True)
        
    def get_session(self):
        return self.context.registry()

    def instrument_class(self, mapper, class_):
        class query(object):
            def __getattr__(s, key):
                return getattr(self.context.registry().query(class_), key)
            def __call__(s):
                return self.context.registry().query(class_)

        if not 'query' in class_.__dict__: 
            class_.query = query()
        
    def init_instance(self, mapper, class_, oldinit, instance, args, kwargs):
        session = kwargs.pop('_sa_session', self.context.registry())
        if not isinstance(oldinit, types.MethodType):
            for key, value in kwargs.items():
                if self.validate:
                    if not mapper.get_property(key, resolve_synonyms=False, raiseerr=False):
                        raise exceptions.ArgumentError("Invalid __init__ argument: '%s'" % key)
                setattr(instance, key, value)
        session._save_impl(instance, entity_name=kwargs.pop('_sa_entity_name', None))
        return EXT_CONTINUE

    def init_failed(self, mapper, class_, oldinit, instance, args, kwargs):
        object_session(instance).expunge(instance)
        return EXT_CONTINUE

    def dispose_class(self, mapper, class_):
        if hasattr(class_, '__init__') and hasattr(class_.__init__, '_oldinit'):
            if class_.__init__._oldinit is not None:
                class_.__init__ = class_.__init__._oldinit
            else:
                delattr(class_, '__init__')
        if hasattr(class_, 'query'):
            delattr(class_, 'query')
            

            
