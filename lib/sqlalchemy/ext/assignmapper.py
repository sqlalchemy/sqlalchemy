from sqlalchemy import util, exceptions
import types
from sqlalchemy.orm import mapper, Query

def _monkeypatch_query_method(name, ctx, class_):
    def do(self, *args, **kwargs):
        query = Query(class_, session=ctx.current)
        util.warn_deprecated('Query methods on the class are deprecated; use %s.query.%s instead' % (class_.__name__, name))
        return getattr(query, name)(*args, **kwargs)
    try:
        do.__name__ = name
    except:
        pass
    if not hasattr(class_, name):
        setattr(class_, name, classmethod(do))

def _monkeypatch_session_method(name, ctx, class_):
    def do(self, *args, **kwargs):
        session = ctx.current
        return getattr(session, name)(self, *args, **kwargs)
    try:
        do.__name__ = name
    except:
        pass
    if not hasattr(class_, name):
        setattr(class_, name, do)

def assign_mapper(ctx, class_, *args, **kwargs):
    extension = kwargs.pop('extension', None)
    if extension is not None:
        extension = util.to_list(extension)
        extension.append(ctx.mapper_extension)
    else:
        extension = ctx.mapper_extension

    validate = kwargs.pop('validate', False)

    if not isinstance(getattr(class_, '__init__'), types.MethodType):
        def __init__(self, **kwargs):
             for key, value in kwargs.items():
                 if validate:
                     if not self.mapper.get_property(key,
                                                     resolve_synonyms=False,
                                                     raiseerr=False):
                         raise exceptions.ArgumentError(
                             "Invalid __init__ argument: '%s'" % key)
                 setattr(self, key, value)
        class_.__init__ = __init__

    class query(object):
        def __getattr__(self, key):
            return getattr(ctx.current.query(class_), key)
        def __call__(self):
            return ctx.current.query(class_)

    if not hasattr(class_, 'query'):
        class_.query = query()

    for name in ('get', 'filter', 'filter_by', 'select', 'select_by',
                 'selectfirst', 'selectfirst_by', 'selectone', 'selectone_by',
                 'get_by', 'join_to', 'join_via', 'count', 'count_by',
                 'options', 'instances'):
        _monkeypatch_query_method(name, ctx, class_)
    for name in ('refresh', 'expire', 'delete', 'expunge', 'update'):
        _monkeypatch_session_method(name, ctx, class_)

    m = mapper(class_, extension=extension, *args, **kwargs)
    class_.mapper = m
    return m

assign_mapper = util.deprecated(
    "assign_mapper is deprecated. Use scoped_session() instead.")(assign_mapper)
