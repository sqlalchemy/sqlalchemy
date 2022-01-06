# engine/row.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Define row constructs including :class:`.Row`."""


import collections.abc as collections_abc
import operator

from ..sql import util as sql_util


try:
    from sqlalchemy.cyextension.resultproxy import BaseRow
    from sqlalchemy.cyextension.resultproxy import KEY_INTEGER_ONLY
    from sqlalchemy.cyextension.resultproxy import KEY_OBJECTS_ONLY
except ImportError:
    from ._py_row import BaseRow
    from ._py_row import KEY_INTEGER_ONLY
    from ._py_row import KEY_OBJECTS_ONLY


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
