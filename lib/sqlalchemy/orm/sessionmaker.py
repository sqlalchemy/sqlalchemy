import types

from sqlalchemy import util, exceptions
from sqlalchemy.orm.session import Session
from sqlalchemy.orm import query, util as mapperutil, MapperExtension, EXT_CONTINUE
from sqlalchemy.orm.mapper import global_extensions

def sessionmaker(autoflush, transactional, bind=None, scope=None, enhance_classes=False, **kwargs):
    """Generate a Session configuration."""

    if enhance_classes and scope is None:
        raise exceptions.InvalidRequestError("enhance_classes requires a non-None 'scope' argument, so that mappers can automatically locate a Session already in progress.")
        
    class Sess(Session):
        def __init__(self, **local_kwargs):
            local_kwargs.setdefault('bind', bind)
            local_kwargs.setdefault('autoflush', autoflush)
            local_kwargs.setdefault('transactional', transactional)
            for k in kwargs:
                local_kwargs.setdefault(k, kwargs[k])
            super(Sess, self).__init__(**local_kwargs)
        
    if scope=="thread":
        registry = util.ScopedRegistry(Sess, scopefunc=None)

        if enhance_classes:
            class SessionContextExt(MapperExtension):
                def get_session(self):
                    return registry()

                def instrument_class(self, mapper, class_):
                    class query(object):
                        def __getattr__(self, key):
                            return getattr(registry().query(class_), key)
                        def __call__(self):
                            return registry().query(class_)

                    if not hasattr(class_, 'query'): 
                        class_.query = query()
                    
                def init_instance(self, mapper, class_, oldinit, instance, args, kwargs):
                    session = kwargs.pop('_sa_session', registry())
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
                        
            global_extensions.append(SessionContextExt())
            
        default_scope=scope
        
        class ScopedSess(Sess):
            def __call__(self, **kwargs):
                if len(kwargs):
                    scope = kwargs.pop('scope', default_scope)
                    if scope is not None:
                        if registry.has():
                            raise exceptions.InvalidRequestError("Scoped session is already present; no new arguments may be specified.")
                        else:
                            sess = Sess(**kwargs)
                            registry.set(sess)
                            return sess
                    else:
                        return Sess(**kwargs)
                else:
                    return registry()
                    
        def instrument(name):
            def do(cls, *args, **kwargs):
                return getattr(registry(), name)(*args, **kwargs)
            return do
        for meth in ('get', 'close', 'save', 'commit', 'update', 'flush', 'query', 'delete'):
            setattr(ScopedSess, meth, instrument(meth))
            
        def makeprop(name):
            def set(self, attr):
                setattr(registry(), name, attr)
            def get(self):
                return getattr(registry(), name)
            return property(get, set)
        for prop in ('bind', 'dirty', 'identity_map'):
            setattr(ScopedSess, prop, makeprop(prop))
            
        return ScopedSess()
    elif scope is not None:
        raise exceptions.ArgumentError("Unknown scope '%s'" % scope)
    else:
        return Sess
