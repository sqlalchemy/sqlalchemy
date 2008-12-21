import types
import __builtin__

__all__ = '_function_named', 'callable'


def _function_named(fn, newname):
    try:
        fn.__name__ = newname
    except:
        fn = types.FunctionType(fn.func_code, fn.func_globals, newname,
                          fn.func_defaults, fn.func_closure)
    return fn

try:
    callable = __builtin__.callable
except NameError:
    def callable(fn): return hasattr(fn, '__call__')

