# TODO: this is mostly just copied over from the python implementation
# more improvements are likely possible
import operator

cdef object _MISSING_SENTINEL = object()

cdef class BaseRow:
    cdef readonly object _parent
    cdef readonly dict _keymap_by_str
    cdef readonly tuple _data

    def __init__(self, object parent, object processors, object data):
        """Row objects are constructed by CursorResult objects."""

        self._parent = parent

        self._keymap_by_str = parent._keymap_by_str

        if processors:
            self._data = tuple(
                [
                    proc(value) if proc else value
                    for proc, value in zip(processors, data)
                ]
            )
        else:
            self._data = tuple(data)

    def __reduce__(self):
        return (
            rowproxy_reconstructor,
            (self.__class__, self.__getstate__()),
        )

    def __getstate__(self):
        return {
            "_parent": self._parent,
            "_data": self._data,
        }

    def __setstate__(self, dict state):
        parent = state["_parent"]
        self._parent = parent
        self._data = state["_data"]
        self._keymap_by_str = parent._keymap_by_str

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
        cached_index = self._keymap_by_str.get(key, _MISSING_SENTINEL)
        if cached_index is not _MISSING_SENTINEL and cached_index is not None:
            return self._data[cached_index]        
        if cached_index is _MISSING_SENTINEL:
            self._parent._key_fallback(key, KeyError(key))
        self._parent._raise_for_ambiguous_column_name(self._parent._keymap[key])

    def __getattr__(self, name):
        cached_index = self._keymap_by_str.get(name, _MISSING_SENTINEL)
        if cached_index is not _MISSING_SENTINEL and cached_index is not None:
            return self._data[cached_index]
        if cached_index is _MISSING_SENTINEL:
            try:
                self._parent._key_fallback(name, KeyError(name))
            except KeyError as e:
                raise AttributeError(e.args[0]) from e
        self._parent._raise_for_ambiguous_column_name(self._parent._keymap[name])


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
