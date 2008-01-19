import new

__all__ = 'set', 'sorted', '_function_named'

try:
    set = set
except NameError:
    from sets import Set as set

try:
    sorted = sorted
except NameError:
    def sorted(iterable):
        return list(iterable).sort()

def _function_named(fn, newname):
    try:
        fn.__name__ = newname
    except:
        fn = new.function(fn.func_code, fn.func_globals, newname,
                          fn.func_defaults, fn.func_closure)
    return fn
