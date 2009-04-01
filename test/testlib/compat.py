import gc
import sys
import time
import types
import __builtin__


__all__ = '_function_named', 'callable', 'py3k', 'jython'

py3k = getattr(sys, 'py3kwarning', False) or sys.version_info >= (3, 0)

jython = sys.platform.startswith('java')

def _function_named(fn, name):
    """Return a function with a given __name__.

    Will assign to __name__ and return the original function if possible on
    the Python implementation, otherwise a new function will be constructed.

    """
    try:
        fn.__name__ = name
    except TypeError:
        fn = types.FunctionType(fn.func_code, fn.func_globals, name,
                          fn.func_defaults, fn.func_closure)
    return fn

if py3k:
    def callable(fn):
        return hasattr(fn, '__call__')
else:
    callable = __builtin__.callable

if sys.platform.startswith('java'):
    def gc_collect(*args):
        gc.collect()
        time.sleep(0.1)
        gc.collect()
        gc.collect()
        return 0
else:
    gc_collect = gc.collect
