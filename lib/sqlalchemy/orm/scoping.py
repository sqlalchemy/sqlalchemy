from sqlalchemy.util import ScopedRegistry, to_list, get_cls_kwargs
from sqlalchemy.orm import MapperExtension, EXT_CONTINUE, object_session, class_mapper
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
        if self.registry.has():
            self.registry().close()
        self.registry.clear()
    
    def mapper(self, *args, **kwargs):
        """return a mapper() function which associates this ScopedSession with the Mapper."""
        
        from sqlalchemy.orm import mapper
        
        extension_args = dict([(arg,kwargs.pop(arg))
                               for arg in get_cls_kwargs(_ScopedExt)
                               if arg in kwargs])
        
        kwargs['extension'] = extension = to_list(kwargs.get('extension', []))
        if extension_args:
            extension.append(self.extension.configure(**extension_args))
        else:
            extension.append(self.extension)
        return mapper(*args, **kwargs)
        
    def configure(self, **kwargs):
        """reconfigure the sessionmaker used by this ScopedSession."""
        
        self.session_factory.configure(**kwargs)

    def query_property(self):
        """return a class property which produces a `Query` object against the
        class when called.
        
        e.g.::
            Session = scoped_session(sessionmaker())
            
            class MyClass(object):
                query = Session.query_property()
                
            # after mappers are defined
            result = MyClass.query.filter(MyClass.name=='foo').all()
        
        """
        
        class query(object):
            def __get__(s, instance, owner):
                mapper = class_mapper(owner, raiseerror=False)
                if mapper:
                    return self.registry().query(mapper)
                else:
                    return None
        return query()
        
def instrument(name):
    def do(self, *args, **kwargs):
        return getattr(self.registry(), name)(*args, **kwargs)
    return do
for meth in ('add', 'add_all', 'get', 'load', 'close', 'save', 'commit', 'update', 'save_or_update', 'flush', 'query', 'delete', 'merge', 'clear', 'refresh', 'expire', 'expunge', 'rollback', 'begin', 'begin_nested', 'connection', 'execute', 'scalar', 'get_bind', 'is_modified', '__contains__', '__iter__'):
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
    def __init__(self, context, validate=False, save_on_init=True):
        self.context = context
        self.validate = validate
        self.save_on_init = save_on_init
    
    def validating(self):
        return _ScopedExt(self.context, validate=True)
    
    def configure(self, **kwargs):
        return _ScopedExt(self.context, **kwargs)
    
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
        if self.save_on_init:
            entity_name = kwargs.pop('_sa_entity_name', None)
            session = kwargs.pop('_sa_session', None)
        if not isinstance(oldinit, types.MethodType):
            for key, value in kwargs.items():
                if self.validate:
                    if not mapper.get_property(key, resolve_synonyms=False, raiseerr=False):
                        raise exceptions.ArgumentError("Invalid __init__ argument: '%s'" % key)
                setattr(instance, key, value)
            kwargs.clear()
        if self.save_on_init:
            session = session or self.context.registry()
            session._save_impl(instance, entity_name=entity_name)
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
            

            
