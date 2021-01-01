from collections import abc as collections_abc

from .. import exc

_no_tuple = ()


def _distill_params_20(params):
    if params is None:
        return _no_tuple
    # Assume list is more likely than tuple
    elif isinstance(params, list) or isinstance(params, tuple):
        # collections_abc.MutableSequence): # avoid abc.__instancecheck__
        if params and not isinstance(
            params[0], (tuple, collections_abc.Mapping)
        ):
            raise exc.ArgumentError(
                "List argument must consist only of tuples or dictionaries"
            )

        return params
    elif isinstance(params, dict) or isinstance(
        # only do immutabledict or abc.__instancecheck__ for Mapping after
        # we've checked for plain dictionaries and would otherwise raise
        params,
        collections_abc.Mapping,
    ):
        return [params]
    else:
        raise exc.ArgumentError("mapping or list expected for parameters")


def _distill_raw_params(params):
    if params is None:
        return _no_tuple
    elif isinstance(params, list):
        # collections_abc.MutableSequence): # avoid abc.__instancecheck__
        if params and not isinstance(
            params[0], (tuple, collections_abc.Mapping)
        ):
            raise exc.ArgumentError(
                "List argument must consist only of tuples or dictionaries"
            )

        return params
    elif isinstance(params, (tuple, dict)) or isinstance(
        # only do abc.__instancecheck__ for Mapping after we've checked
        # for plain dictionaries and would otherwise raise
        params,
        collections_abc.Mapping,
    ):
        return [params]
    else:
        raise exc.ArgumentError("mapping or sequence expected for parameters")
