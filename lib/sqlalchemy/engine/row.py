# engine/row.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Define row constructs including :class:`.Row`."""


import collections.abc as collections_abc
import operator

from .. import util
from ..sql import util as sql_util

MD_INDEX = 0  # integer index in cursor.description

# This reconstructor is necessary so that pickles with the C extension or
# without use the same Binary format.
try:
    # We need a different reconstructor on the C extension so that we can
    # add extra checks that fields have correctly been initialized by
    # __setstate__.
    from sqlalchemy.cresultproxy import safe_rowproxy_reconstructor

    # The extra function embedding is needed so that the
    # reconstructor function has the same signature whether or not
    # the extension is present.
    def rowproxy_reconstructor(cls, state):
        return safe_rowproxy_reconstructor(cls, state)


except ImportError:

    def rowproxy_reconstructor(cls, state):
        obj = cls.__new__(cls)
        obj.__setstate__(state)
        return obj


KEY_INTEGER_ONLY = 0
"""__getitem__ only allows integer values and slices, raises TypeError
   otherwise"""

KEY_OBJECTS_ONLY = 1
"""__getitem__ only allows string/object values, raises TypeError otherwise"""

try:
    from sqlalchemy.cresultproxy import BaseRow

    _baserow_usecext = True
except ImportError:
    _baserow_usecext = False

    class BaseRow:
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

        def _filter_on_values(self, filters):
            return Row(
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
            if int in key.__class__.__mro__:
                return self._data[key]

            assert self._key_style == KEY_INTEGER_ONLY

            if isinstance(key, slice):
                return tuple(self._data[key])

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
            elif (
                self._key_style == KEY_OBJECTS_ONLY
                and int in key.__class__.__mro__
            ):
                raise KeyError(key)

            return self._data[mdindex]

        def __getattr__(self, name):
            try:
                return self._get_by_key_impl_mapping(name)
            except KeyError as e:
                util.raise_(AttributeError(e.args[0]), replace_context=e)


class Row(BaseRow, collections_abc.Sequence):
    """Represent a single result row.

    The :class:`.Row` object represents a row of a database result.  It is
    typically associated in the 1.x series of SQLAlchemy with the
    :class:`_engine.CursorResult` object, however is also used by the ORM for
    tuple-like results as of SQLAlchemy 1.4.

    The :class:`.Row` object seeks to act as much like a Python named
    tuple as possible.   For mapping (i.e. dictionary) behavior on a row,
    such as testing for containment of keys, refer to the :attr:`.Row._mapping`
    attribute.

    .. seealso::

        :ref:`coretutorial_selecting` - includes examples of selecting
        rows from SELECT statements.

    .. versionchanged:: 1.4

        Renamed ``RowProxy`` to :class:`.Row`. :class:`.Row` is no longer a
        "proxy" object in that it contains the final form of data within it,
        and now acts mostly like a named tuple. Mapping-like functionality is
        moved to the :attr:`.Row._mapping` attribute. See
        :ref:`change_4710_core` for background on this change.

    """

    __slots__ = ()

    _default_key_style = KEY_INTEGER_ONLY

    def __setattr__(self, name, value):
        raise AttributeError("can't set attribute")

    def __delattr__(self, name):
        raise AttributeError("can't delete attribute")

    @property
    def _mapping(self):
        """Return a :class:`.RowMapping` for this :class:`.Row`.

        This object provides a consistent Python mapping (i.e. dictionary)
        interface for the data contained within the row.   The :class:`.Row`
        by itself behaves like a named tuple.

        .. seealso::

            :attr:`.Row._fields`

        .. versionadded:: 1.4

        """
        return RowMapping(
            self._parent,
            None,
            self._keymap,
            RowMapping._default_key_style,
            self._data,
        )

    def _special_name_accessor(name):
        """Handle ambiguous names such as "count" and "index" """

        @property
        def go(self):
            if self._parent._has_key(name):
                return self.__getattr__(name)
            else:

                def meth(*arg, **kw):
                    return getattr(collections_abc.Sequence, name)(
                        self, *arg, **kw
                    )

                return meth

        return go

    count = _special_name_accessor("count")
    index = _special_name_accessor("index")

    def __contains__(self, key):
        return key in self._data

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

    def _op(self, other, op):
        return (
            op(tuple(self), tuple(other))
            if isinstance(other, Row)
            else op(tuple(self), other)
        )

    __hash__ = BaseRow.__hash__

    def __lt__(self, other):
        return self._op(other, operator.lt)

    def __le__(self, other):
        return self._op(other, operator.le)

    def __ge__(self, other):
        return self._op(other, operator.ge)

    def __gt__(self, other):
        return self._op(other, operator.gt)

    def __eq__(self, other):
        return self._op(other, operator.eq)

    def __ne__(self, other):
        return self._op(other, operator.ne)

    def __repr__(self):
        return repr(sql_util._repr_row(self))

    @property
    def _fields(self):
        """Return a tuple of string keys as represented by this
        :class:`.Row`.

        The keys can represent the labels of the columns returned by a core
        statement or the names of the orm classes returned by an orm
        execution.

        This attribute is analogous to the Python named tuple ``._fields``
        attribute.

        .. versionadded:: 1.4

        .. seealso::

            :attr:`.Row._mapping`

        """
        return tuple([k for k in self._parent.keys if k is not None])

    def _asdict(self):
        """Return a new dict which maps field names to their corresponding
        values.

        This method is analogous to the Python named tuple ``._asdict()``
        method, and works by applying the ``dict()`` constructor to the
        :attr:`.Row._mapping` attribute.

        .. versionadded:: 1.4

        .. seealso::

            :attr:`.Row._mapping`

        """
        return dict(self._mapping)

    def _replace(self):
        raise NotImplementedError()

    @property
    def _field_defaults(self):
        raise NotImplementedError()


BaseRowProxy = BaseRow
RowProxy = Row


class ROMappingView(
    collections_abc.KeysView,
    collections_abc.ValuesView,
    collections_abc.ItemsView,
):
    __slots__ = (
        "_mapping",
        "_items",
    )

    def __init__(self, mapping, items):
        self._mapping = mapping
        self._items = items

    def __len__(self):
        return len(self._items)

    def __repr__(self):
        return "{0.__class__.__name__}({0._mapping!r})".format(self)

    def __iter__(self):
        return iter(self._items)

    def __contains__(self, item):
        return item in self._items

    def __eq__(self, other):
        return list(other) == list(self)

    def __ne__(self, other):
        return list(other) != list(self)


class RowMapping(BaseRow, collections_abc.Mapping):
    """A ``Mapping`` that maps column names and objects to :class:`.Row` values.

    The :class:`.RowMapping` is available from a :class:`.Row` via the
    :attr:`.Row._mapping` attribute, as well as from the iterable interface
    provided by the :class:`.MappingResult` object returned by the
    :meth:`_engine.Result.mappings` method.

    :class:`.RowMapping` supplies Python mapping (i.e. dictionary) access to
    the  contents of the row.   This includes support for testing of
    containment of specific keys (string column names or objects), as well
    as iteration of keys, values, and items::

        for row in result:
            if 'a' in row._mapping:
                print("Column 'a': %s" % row._mapping['a'])

            print("Column b: %s" % row._mapping[table.c.b])


    .. versionadded:: 1.4 The :class:`.RowMapping` object replaces the
       mapping-like access previously provided by a database result row,
       which now seeks to behave mostly like a named tuple.

    """

    __slots__ = ()

    _default_key_style = KEY_OBJECTS_ONLY

    if not _baserow_usecext:

        __getitem__ = BaseRow._get_by_key_impl_mapping

        def _values_impl(self):
            return list(self._data)

    def __iter__(self):
        return (k for k in self._parent.keys if k is not None)

    def __len__(self):
        return len(self._data)

    def __contains__(self, key):
        return self._parent._has_key(key)

    def __repr__(self):
        return repr(dict(self))

    def items(self):
        """Return a view of key/value tuples for the elements in the
        underlying :class:`.Row`.

        """
        return ROMappingView(self, [(key, self[key]) for key in self.keys()])

    def keys(self):
        """Return a view of 'keys' for string column names represented
        by the underlying :class:`.Row`.

        """

        return self._parent.keys

    def values(self):
        """Return a view of values for the values represented in the
        underlying :class:`.Row`.

        """
        return ROMappingView(self, self._values_impl())
