# orm/collections.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

from __future__ import annotations

import operator
from typing import Any
from typing import Callable
from typing import Dict
from typing import Type
from typing import TypeVar

from . import base
from .collections import collection
from .. import exc as sa_exc
from .. import util
from ..sql import coercions
from ..sql import expression
from ..sql import roles

_KT = TypeVar("_KT", bound=Any)
_VT = TypeVar("_VT", bound=Any)


class _PlainColumnGetter:
    """Plain column getter, stores collection of Column objects
    directly.

    Serializes to a :class:`._SerializableColumnGetterV2`
    which has more expensive __call__() performance
    and some rare caveats.

    """

    __slots__ = ("cols", "composite")

    def __init__(self, cols):
        self.cols = cols
        self.composite = len(cols) > 1

    def __reduce__(self):
        return _SerializableColumnGetterV2._reduce_from_cols(self.cols)

    def _cols(self, mapper):
        return self.cols

    def __call__(self, value):
        state = base.instance_state(value)
        m = base._state_mapper(state)

        key = [
            m._get_state_attr_by_column(state, state.dict, col)
            for col in self._cols(m)
        ]

        if self.composite:
            return tuple(key)
        else:
            return key[0]


class _SerializableColumnGetterV2(_PlainColumnGetter):
    """Updated serializable getter which deals with
    multi-table mapped classes.

    Two extremely unusual cases are not supported.
    Mappings which have tables across multiple metadata
    objects, or which are mapped to non-Table selectables
    linked across inheriting mappers may fail to function
    here.

    """

    __slots__ = ("colkeys",)

    def __init__(self, colkeys):
        self.colkeys = colkeys
        self.composite = len(colkeys) > 1

    def __reduce__(self):
        return self.__class__, (self.colkeys,)

    @classmethod
    def _reduce_from_cols(cls, cols):
        def _table_key(c):
            if not isinstance(c.table, expression.TableClause):
                return None
            else:
                return c.table.key

        colkeys = [(c.key, _table_key(c)) for c in cols]
        return _SerializableColumnGetterV2, (colkeys,)

    def _cols(self, mapper):
        cols = []
        metadata = getattr(mapper.local_table, "metadata", None)
        for (ckey, tkey) in self.colkeys:
            if tkey is None or metadata is None or tkey not in metadata:
                cols.append(mapper.local_table.c[ckey])
            else:
                cols.append(metadata.tables[tkey].c[ckey])
        return cols


def column_mapped_collection(mapping_spec):
    """A dictionary-based collection type with column-based keying.

    Returns a :class:`.MappedCollection` factory with a keying function
    generated from mapping_spec, which may be a Column or a sequence
    of Columns.

    The key value must be immutable for the lifetime of the object.  You
    can not, for example, map on foreign key values if those key values will
    change during the session, i.e. from None to a database-assigned integer
    after a session flush.

    """
    cols = [
        coercions.expect(roles.ColumnArgumentRole, q, argname="mapping_spec")
        for q in util.to_list(mapping_spec)
    ]
    keyfunc = _PlainColumnGetter(cols)
    return _mapped_collection_cls(keyfunc)


def attribute_mapped_collection(attr_name: str) -> Type["MappedCollection"]:
    """A dictionary-based collection type with attribute-based keying.

    Returns a :class:`.MappedCollection` factory with a keying based on the
    'attr_name' attribute of entities in the collection, where ``attr_name``
    is the string name of the attribute.

    .. warning:: the key value must be assigned to its final value
       **before** it is accessed by the attribute mapped collection.
       Additionally, changes to the key attribute are **not tracked**
       automatically, which means the key in the dictionary is not
       automatically synchronized with the key value on the target object
       itself.  See the section :ref:`key_collections_mutations`
       for an example.

    """
    getter = operator.attrgetter(attr_name)
    return _mapped_collection_cls(getter)


def mapped_collection(
    keyfunc: Callable[[Any], _KT]
) -> Type["MappedCollection[_KT, Any]"]:
    """A dictionary-based collection type with arbitrary keying.

    Returns a :class:`.MappedCollection` factory with a keying function
    generated from keyfunc, a callable that takes an entity and returns a
    key value.

    The key value must be immutable for the lifetime of the object.  You
    can not, for example, map on foreign key values if those key values will
    change during the session, i.e. from None to a database-assigned integer
    after a session flush.

    """
    return _mapped_collection_cls(keyfunc)


class MappedCollection(Dict[_KT, _VT]):
    """Base for ORM mapped dictionary classes.

    Extends the ``dict`` type with additional methods needed by SQLAlchemy ORM
    collection classes. Use of :class:`_orm.MappedCollection` is most directly
    by using the :func:`.attribute_mapped_collection` or
    :func:`.column_mapped_collection` class factories.
    :class:`_orm.MappedCollection` may also serve as the base for user-defined
    custom dictionary classes.

    .. seealso::

        :ref:`orm_dictionary_collection`

        :ref:`orm_custom_collection`


    """

    def __init__(self, keyfunc):
        """Create a new collection with keying provided by keyfunc.

        keyfunc may be any callable that takes an object and returns an object
        for use as a dictionary key.

        The keyfunc will be called every time the ORM needs to add a member by
        value-only (such as when loading instances from the database) or
        remove a member.  The usual cautions about dictionary keying apply-
        ``keyfunc(object)`` should return the same output for the life of the
        collection.  Keying based on mutable properties can result in
        unreachable instances "lost" in the collection.

        """
        self.keyfunc = keyfunc

    @classmethod
    def _unreduce(cls, keyfunc, values):
        mp = MappedCollection(keyfunc)
        mp.update(values)
        return mp

    def __reduce__(self):
        return (MappedCollection._unreduce, (self.keyfunc, dict(self)))

    @collection.appender
    @collection.internally_instrumented
    def set(self, value, _sa_initiator=None):
        """Add an item by value, consulting the keyfunc for the key."""

        key = self.keyfunc(value)
        self.__setitem__(key, value, _sa_initiator)

    @collection.remover
    @collection.internally_instrumented
    def remove(self, value, _sa_initiator=None):
        """Remove an item by value, consulting the keyfunc for the key."""

        key = self.keyfunc(value)
        # Let self[key] raise if key is not in this collection
        # testlib.pragma exempt:__ne__
        if self[key] != value:
            raise sa_exc.InvalidRequestError(
                "Can not remove '%s': collection holds '%s' for key '%s'. "
                "Possible cause: is the MappedCollection key function "
                "based on mutable properties or properties that only obtain "
                "values after flush?" % (value, self[key], key)
            )
        self.__delitem__(key, _sa_initiator)


def _mapped_collection_cls(keyfunc):
    class _MKeyfuncMapped(MappedCollection):
        def __init__(self):
            super().__init__(keyfunc)

    return _MKeyfuncMapped
