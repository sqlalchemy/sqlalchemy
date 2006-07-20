from sqlalchemy import mapper, util
import types

def monkeypatch_query_method(ctx, class_, name):
    def do(self, *args, **kwargs):
        query = class_.mapper.query(session=ctx.current)
        return getattr(query, name)(*args, **kwargs)
    setattr(class_, name, classmethod(do))

def monkeypatch_objectstore_method(ctx, class_, name):
    def do(self, *args, **kwargs):
        session = ctx.current
        if name == "flush":
            # flush expects a list of objects
            self = [self]
        return getattr(session, name)(self, *args, **kwargs)
    setattr(class_, name, do)
    
def assign_mapper(ctx, class_, *args, **kwargs):
    if not isinstance(getattr(class_, '__init__'), types.MethodType):
        def __init__(self, **kwargs):
             for key, value in kwargs.items():
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
    for name in ['get', 'select', 'select_by', 'selectone', 'get_by', 'join_to', 'join_via', 'count', 'count_by']:
        monkeypatch_query_method(ctx, class_, name)
    for name in ['flush', 'delete', 'expire', 'refresh', 'expunge', 'merge', 'save', 'update', 'save_or_update']:
        monkeypatch_objectstore_method(ctx, class_, name)
