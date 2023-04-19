# TODO: this is mostly just copied over from the python implementation
# more improvements are likely possible
import operator

cdef class BaseRow:
    cdef readonly object _parent
    cdef readonly dict _key_to_index
    cdef readonly tuple _data

    def __init__(self, object parent, object processors, object data):
        """Row objects are constructed by CursorResult objects."""

        self._parent = parent

        self._key_to_index = parent._key_to_index

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
        self._key_to_index = parent._key_to_index

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

    def _get_by_key_impl_mapping(self, key):
        return self._get_by_key_impl(key, 0)

    cdef _get_by_key_impl(self, object key, int attr_err):
        index = self._key_to_index.get(key)
        if index is not None:
            return self._data[<int>index]
        self._parent._key_not_found(key, attr_err != 0)

    def __getattr__(self, name):
        return self._get_by_key_impl(name, 1)


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
