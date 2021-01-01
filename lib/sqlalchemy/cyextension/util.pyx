from collections.abc import Mapping

from sqlalchemy import exc

cdef tuple _Empty_Tuple = ()

cdef inline bint _mapping_or_tuple(object value):
    return isinstance(value, dict) or isinstance(value, tuple) or isinstance(value, Mapping)

cdef inline bint _check_item(object params) except 0:
    cdef object item
    cdef bint ret = 1
    if params:
        item = params[0]
        if not _mapping_or_tuple(item):
            ret = 0
            raise exc.ArgumentError(
                "List argument must consist only of tuples or dictionaries"
            )
    return ret

def _distill_params_20(object params):
    if params is None:
        return _Empty_Tuple
    elif isinstance(params, list) or isinstance(params, tuple):
        _check_item(params)
        return params
    elif isinstance(params, dict) or isinstance(params, Mapping):
        return [params]
    else:
        raise exc.ArgumentError("mapping or list expected for parameters")


def _distill_raw_params(object params):
    if params is None:
        return _Empty_Tuple
    elif isinstance(params, list):
        _check_item(params)
        return params
    elif _mapping_or_tuple(params):
        return [params]
    else:
        raise exc.ArgumentError("mapping or sequence expected for parameters")
