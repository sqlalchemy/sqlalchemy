import operator

MD_INDEX = 0  # integer index in cursor.description

KEY_INTEGER_ONLY = 0
"""__getitem__ only allows integer values and slices, raises TypeError
   otherwise"""

KEY_OBJECTS_ONLY = 1
"""__getitem__ only allows string/object values, raises TypeError otherwise"""

sqlalchemy_engine_row = None


class BaseRow:
    Row = None
    __slots__ = ("_parent", "_data", "_keymap", "_key_style")

    def __init__(self, parent, processors, keymap, key_style, data):
        """Row objects are constructed by CursorResult objects."""

        object.__setattr__(self, "_parent", parent)

        if processors:
            object.__setattr__(
                self,
                "_data",
                tuple(
                    [
                        proc(value) if proc else value
                        for proc, value in zip(processors, data)
                    ]
                ),
            )
        else:
            object.__setattr__(self, "_data", tuple(data))

        object.__setattr__(self, "_keymap", keymap)

        object.__setattr__(self, "_key_style", key_style)

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

    def __setstate__(self, state):
        parent = state["_parent"]
        object.__setattr__(self, "_parent", parent)
        object.__setattr__(self, "_data", state["_data"])
        object.__setattr__(self, "_keymap", parent._keymap)
        object.__setattr__(self, "_key_style", state["_key_style"])

    def _filter_on_values(self, filters):
        global sqlalchemy_engine_row
        if sqlalchemy_engine_row is None:
            from sqlalchemy.engine.row import Row as sqlalchemy_engine_row

        return sqlalchemy_engine_row(
            self._parent,
            filters,
            self._keymap,
            self._key_style,
            self._data,
        )

    def _values_impl(self):
        return list(self)

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __hash__(self):
        return hash(self._data)

    def _get_by_int_impl(self, key):
        return self._data[key]

    def _get_by_key_impl(self, key):
        # keep two isinstance since it's noticeably faster in the int case
        if isinstance(key, int) or isinstance(key, slice):
            return self._data[key]

        self._parent._raise_for_nonint(key)

    # The original 1.4 plan was that Row would not allow row["str"]
    # access, however as the C extensions were inadvertently allowing
    # this coupled with the fact that orm Session sets future=True,
    # this allows a softer upgrade path.  see #6218
    __getitem__ = _get_by_key_impl

    def _get_by_key_impl_mapping(self, key):
        try:
            rec = self._keymap[key]
        except KeyError as ke:
            rec = self._parent._key_fallback(key, ke)

        mdindex = rec[MD_INDEX]
        if mdindex is None:
            self._parent._raise_for_ambiguous_column_name(rec)
        elif self._key_style == KEY_OBJECTS_ONLY and isinstance(key, int):
            raise KeyError(key)

        return self._data[mdindex]

    def __getattr__(self, name):
        try:
            return self._get_by_key_impl_mapping(name)
        except KeyError as e:
            raise AttributeError(e.args[0]) from e


# This reconstructor is necessary so that pickles with the Cy extension or
# without use the same Binary format.
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
