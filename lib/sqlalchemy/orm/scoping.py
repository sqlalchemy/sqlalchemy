from sqlalchemy.util import ScopedRegistry, warn_deprecated
from sqlalchemy.orm import MapperExtension, EXT_CONTINUE
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.mapper import global_extensions
from sqlalchemy import exceptions
import types

__all__ = ['ScopedSession']


class ScopedSession(object):
    """Provides thread-local management of Sessions.

    Usage::

      Session = scoped_session(sessionmaker(autoflush=True), enhance_classes=True)

    """

    def __init__(self, session_factory, scopefunc=None, enhance_classes=False):
        self.session_factory = session_factory
        self.enhance_classes = enhance_classes
        self.registry = ScopedRegistry(session_factory, scopefunc)
        if self.enhance_classes:
            global_extensions.append(_ScopedExt(self))

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

    def configure(self, **kwargs):
        """reconfigure the sessionmaker used by this SessionContext"""
        self.session_factory.configure(**kwargs)

def instrument(name):
    def do(self, *args, **kwargs):
        return getattr(self.registry(), name)(*args, **kwargs)
    return do
for meth in ('get', 'close', 'save', 'commit', 'update', 'flush', 'query', 'delete'):
    setattr(ScopedSession, meth, instrument(meth))

def makeprop(name):
    def set(self, attr):
        setattr(self.registry(), name, attr)
    def get(self):
        return getattr(self.registry(), name)
    return property(get, set)
for prop in ('bind', 'dirty', 'identity_map'):
    setattr(ScopedSession, prop, makeprop(prop))

def clslevel(name):
    def do(cls, *args,**kwargs):
        return getattr(Session, name)(*args, **kwargs)
    return classmethod(do)
for prop in ('close_all',):
    setattr(ScopedSession, prop, clslevel(prop))
    
class _ScopedExt(MapperExtension):
    def __init__(self, context):
        self.context = context
        
    def get_session(self):
        return self.context.registry()

    def instrument_class(self, mapper, class_):
        class query(object):
            def __getattr__(self, key):
                return getattr(registry().query(class_), key)
            def __call__(self):
                return registry().query(class_)

        if not hasattr(class_, 'query'): 
            class_.query = query()
        
    def init_instance(self, mapper, class_, oldinit, instance, args, kwargs):
        session = kwargs.pop('_sa_session', self.context.registry())
        if not isinstance(oldinit, types.MethodType):
            for key, value in kwargs.items():
                #if validate:
                #    if not self.mapper.get_property(key, resolve_synonyms=False, raiseerr=False):
                #        raise exceptions.ArgumentError("Invalid __init__ argument: '%s'" % key)
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
            

            
