import testbase
from testlib import config
import inspect
orm = None

__all__ = 'mapper',


def _make_blocker(method_name, fallback):
    def method(self, *args, **kw):
        frame_r = None
        try:
            frame_r = inspect.stack()[1]
            module = frame_r[0].f_globals.get('__name__', '')

            type_ = type(self)

            if not module.startswith('sqlalchemy'):
                supermeth = getattr(super(type_, self), method_name, None)
                if supermeth is None or supermeth.im_func is method:
                    return fallback(self, *args, **kw)
                else:
                    return supermeth(*args, **kw)
            else:
                raise AssertionError(
                    "%s.%s called in %s, line %s in %s" % (
                    type_.__name__, method_name, module, frame_r[2], frame_r[3]))
        finally:
            del frame_r
    method.__name__ = method_name
    return method

def mapper(type_, *args, **kw):
    global orm
    if orm is None:
        from sqlalchemy import orm

    forbidden = [
        ('__hash__', 'unhashable', None),
        ('__eq__', 'noncomparable', lambda s, x, y: x is y),
        ('__nonzero__', 'truthless', lambda s: 1), ]

    if type_.__bases__ == (object,):
        for method_name, option, fallback in forbidden:
            if (getattr(config.options, option, False) and
                method_name not in type_.__dict__):
                setattr(type_, method_name, _make_blocker(method_name, fallback))

    return orm.mapper(type_, *args, **kw)
