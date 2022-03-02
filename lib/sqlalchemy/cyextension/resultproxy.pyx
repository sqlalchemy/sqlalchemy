# TODO: this is mostly just copied over from the python implementation
# more improvements are likely possible
import operator

cdef int MD_INDEX = 0  # integer index in cursor.description

KEY_INTEGER_ONLY = 0
KEY_OBJECTS_ONLY = 1

cdef class BaseRow:
    cdef readonly object _parent
    cdef readonly tuple _data
    cdef readonly dict _keymap
    cdef readonly int _key_style

    def __init__(self, object parent, object processors, dict keymap, int key_style, object data):
        """Row objects are constructed by CursorResult objects."""

        self._parent = parent

        if processors:
            self._data = tuple(
                [
                    proc(value) if proc else value
                    for proc, value in zip(processors, data)
                ]
            )
        else:
            self._data = tuple(data)

        self._keymap = keymap

        self._key_style = key_style

    def __reduce__(self):
        return (
            rowproxy_reconstructor,
            (self.__class__, self.__getstate__()),
        )

    def __getstate__(self):
        return {
            "_parent": self._parent,
            "_data": self._data,
            "_key_style": self._key_style,
        }

    def __setstate__(self, dict state):
        self._parent = state["_parent"]
        self._data = state["_data"]
        self._keymap = self._parent._keymap
        self._key_style = state["_key_style"]

    def _values_impl(self):
        return list(self)

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __hash__(self):
        return hash(self._data)

    def __getitem__(self, index):
        return self._data[index]

    cpdef _get_by_key_impl_mapping(self, key):
        try:
            rec = self._keymap[key]
        except KeyError as ke:
            rec = self._parent._key_fallback(key, ke)

        mdindex = rec[MD_INDEX]
        if mdindex is None:
            self._parent._raise_for_ambiguous_column_name(rec)
        elif (
            self._key_style == KEY_OBJECTS_ONLY
            and isinstance(key, int)
        ):
            raise KeyError(key)

        return self._data[mdindex]

    def __getattr__(self, name):
        try:
            return self._get_by_key_impl_mapping(name)
        except KeyError as e:
           raise AttributeError(e.args[0]) from e


def rowproxy_reconstructor(cls, state):
    obj = cls.__new__(cls)
    obj.__setstate__(state)
    return obj


def tuplegetter(*indexes):
    it = operator.itemgetter(*indexes)

    if len(indexes) > 1:
        return it
    else:
        return lambda row: (it(row),)
