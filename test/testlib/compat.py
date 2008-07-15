import new

__all__ = '_function_named',


def _function_named(fn, newname):
    try:
        fn.__name__ = newname
    except:
        fn = new.function(fn.func_code, fn.func_globals, newname,
                          fn.func_defaults, fn.func_closure)
    return fn

