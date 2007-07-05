from sqlalchemy import util, exceptions
import types
from sqlalchemy.orm import mapper, Query

def monkeypatch_query_method(ctx, class_, name):
    def do(self, *args, **kwargs):
        query = Query(class_, session=ctx.current)
        return getattr(query, name)(*args, **kwargs)
    try:
        do.__name__ = name
    except:
        pass
    setattr(class_, name, classmethod(do))

def monkeypatch_objectstore_method(ctx, class_, name):
    def do(self, *args, **kwargs):
        session = ctx.current
        return getattr(session, name)(self, *args, **kwargs)
    try:
        do.__name__ = name
    except:
        pass
    setattr(class_, name, do)

    
def assign_mapper(ctx, class_, *args, **kwargs):
    validate = kwargs.pop('validate', False)
    if not isinstance(getattr(class_, '__init__'), types.MethodType):
        def __init__(self, **kwargs):
            if validate:
                keys = [p.key for p in self.mapper.iterate_properties]
            for key, value in kwargs.items():
                if validate and key not in keys:
                    raise exceptions.ArgumentError("Invalid __init__ argument: '%s'" % key)
                setattr(self, key, value)
        class_.__init__ = __init__
    extension = kwargs.pop('extension', None)
    if extension is not None:
        extension = util.to_list(extension)
        extension.append(ctx.mapper_extension)
    else:
        extension = ctx.mapper_extension
    m = mapper(class_, extension=extension, *args, **kwargs)
    class_.mapper = m
    class_.query = classmethod(lambda cls: Query(class_, session=ctx.current))
    for name in ['get', 'filter', 'filter_by', 'select', 'select_by', 'selectfirst', 'selectfirst_by', 'selectone', 'selectone_by', 'get_by', 'join', 'count', 'count_by', 'options', 'instances']:
        monkeypatch_query_method(ctx, class_, name)
    for name in ['delete', 'expire', 'refresh', 'expunge', 'save', 'update', 'save_or_update']:
        monkeypatch_objectstore_method(ctx, class_, name)
    return m

