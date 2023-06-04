# orm/writeonly.py
# Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors


"""Write-only collection API.

This is an alternate mapped attribute style that only supports single-item
collection mutation operations.   To read the collection, a select()
object must be executed each time.

.. versionadded:: 2.0


"""

from __future__ import annotations

from typing import Any
from typing import Generic
from typing import Iterable
from typing import NoReturn
from typing import Optional
from typing import overload
from typing import Tuple
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union

from sqlalchemy.sql import bindparam
from . import attributes
from . import interfaces
from . import relationships
from . import strategies
from .base import object_mapper
from .base import PassiveFlag
from .relationships import RelationshipDirection
from .. import exc
from .. import inspect
from .. import log
from .. import util
from ..sql import delete
from ..sql import insert
from ..sql import select
from ..sql import update
from ..sql.dml import Delete
from ..sql.dml import Insert
from ..sql.dml import Update
from ..util.typing import Literal

if TYPE_CHECKING:
    from ._typing import _InstanceDict
    from .attributes import _AdaptedCollectionProtocol
    from .attributes import AttributeEventToken
    from .attributes import CollectionAdapter
    from .base import LoaderCallableStatus
    from .state import InstanceState
    from ..sql.selectable import Select


_T = TypeVar("_T", bound=Any)


class WriteOnlyHistory:
    """Overrides AttributeHistory to receive append/remove events directly."""

    def __init__(self, attr, state, passive, apply_to=None):
        if apply_to:
            if passive & PassiveFlag.SQL_OK:
                raise exc.InvalidRequestError(
                    f"Attribute {attr} can't load the existing state from the "
                    "database for this operation; full iteration is not "
                    "permitted.  If this is a delete operation, configure "
                    f"passive_deletes=True on the {attr} relationship in "
                    "order to resolve this error."
                )

            self.unchanged_items = apply_to.unchanged_items
            self.added_items = apply_to.added_items
            self.deleted_items = apply_to.deleted_items
            self._reconcile_collection = apply_to._reconcile_collection
        else:
            self.deleted_items = util.OrderedIdentitySet()
            self.added_items = util.OrderedIdentitySet()
            self.unchanged_items = util.OrderedIdentitySet()
            self._reconcile_collection = False

    @property
    def added_plus_unchanged(self):
        return list(self.added_items.union(self.unchanged_items))

    @property
    def all_items(self):
        return list(
            self.added_items.union(self.unchanged_items).union(
                self.deleted_items
            )
        )

    def as_history(self):
        if self._reconcile_collection:
            added = self.added_items.difference(self.unchanged_items)
            deleted = self.deleted_items.intersection(self.unchanged_items)
            unchanged = self.unchanged_items.difference(deleted)
        else:
            added, unchanged, deleted = (
                self.added_items,
                self.unchanged_items,
                self.deleted_items,
            )
        return attributes.History(list(added), list(unchanged), list(deleted))

    def indexed(self, index):
        return list(self.added_items)[index]

    def add_added(self, value):
        self.added_items.add(value)

    def add_removed(self, value):
        if value in self.added_items:
            self.added_items.remove(value)
        else:
            self.deleted_items.add(value)


class WriteOnlyAttributeImpl(
    attributes.HasCollectionAdapter, attributes.AttributeImpl
):
    uses_objects = True
    default_accepts_scalar_loader = False
    supports_population = False
    _supports_dynamic_iteration = False
    collection = False
    dynamic = True
    order_by = ()
    collection_history_cls = WriteOnlyHistory

    def __init__(
        self,
        class_,
        key,
        typecallable,
        dispatch,
        target_mapper,
        order_by,
        **kw,
    ):
        super().__init__(class_, key, typecallable, dispatch, **kw)
        self.target_mapper = target_mapper
        self.query_class = WriteOnlyCollection
        if order_by:
            self.order_by = tuple(order_by)

    def get(self, state, dict_, passive=attributes.PASSIVE_OFF):
        if not passive & attributes.SQL_OK:
            return self._get_collection_history(
                state, attributes.PASSIVE_NO_INITIALIZE
            ).added_items
        else:
            return self.query_class(self, state)

    @overload
    def get_collection(
        self,
        state: InstanceState[Any],
        dict_: _InstanceDict,
        user_data: Literal[None] = ...,
        passive: Literal[PassiveFlag.PASSIVE_OFF] = ...,
    ) -> CollectionAdapter:
        ...

    @overload
    def get_collection(
        self,
        state: InstanceState[Any],
        dict_: _InstanceDict,
        user_data: _AdaptedCollectionProtocol = ...,
        passive: PassiveFlag = ...,
    ) -> CollectionAdapter:
        ...

    @overload
    def get_collection(
        self,
        state: InstanceState[Any],
        dict_: _InstanceDict,
        user_data: Optional[_AdaptedCollectionProtocol] = ...,
        passive: PassiveFlag = ...,
    ) -> Union[
        Literal[LoaderCallableStatus.PASSIVE_NO_RESULT], CollectionAdapter
    ]:
        ...

    def get_collection(
        self,
        state: InstanceState[Any],
        dict_: _InstanceDict,
        user_data: Optional[_AdaptedCollectionProtocol] = None,
        passive: PassiveFlag = PassiveFlag.PASSIVE_OFF,
    ) -> Union[
        Literal[LoaderCallableStatus.PASSIVE_NO_RESULT], CollectionAdapter
    ]:
        if not passive & attributes.SQL_OK:
            data = self._get_collection_history(state, passive).added_items
        else:
            history = self._get_collection_history(state, passive)
            data = history.added_plus_unchanged
        return DynamicCollectionAdapter(data)  # type: ignore

    @util.memoized_property
    def _append_token(self):
        return attributes.AttributeEventToken(self, attributes.OP_APPEND)

    @util.memoized_property
    def _remove_token(self):
        return attributes.AttributeEventToken(self, attributes.OP_REMOVE)

    def fire_append_event(
        self, state, dict_, value, initiator, collection_history=None
    ):
        if collection_history is None:
            collection_history = self._modified_event(state, dict_)

        collection_history.add_added(value)

        for fn in self.dispatch.append:
            value = fn(state, value, initiator or self._append_token)

        if self.trackparent and value is not None:
            self.sethasparent(attributes.instance_state(value), state, True)

    def fire_remove_event(
        self, state, dict_, value, initiator, collection_history=None
    ):
        if collection_history is None:
            collection_history = self._modified_event(state, dict_)

        collection_history.add_removed(value)

        if self.trackparent and value is not None:
            self.sethasparent(attributes.instance_state(value), state, False)

        for fn in self.dispatch.remove:
            fn(state, value, initiator or self._remove_token)

    def _modified_event(self, state, dict_):
        if self.key not in state.committed_state:
            state.committed_state[self.key] = self.collection_history_cls(
                self, state, PassiveFlag.PASSIVE_NO_FETCH
            )

        state._modified_event(dict_, self, attributes.NEVER_SET)

        # this is a hack to allow the fixtures.ComparableEntity fixture
        # to work
        dict_[self.key] = True
        return state.committed_state[self.key]

    def set(
        self,
        state: InstanceState[Any],
        dict_: _InstanceDict,
        value: Any,
        initiator: Optional[AttributeEventToken] = None,
        passive: PassiveFlag = PassiveFlag.PASSIVE_OFF,
        check_old: Any = None,
        pop: bool = False,
        _adapt: bool = True,
    ) -> None:
        if initiator and initiator.parent_token is self.parent_token:
            return

        if pop and value is None:
            return

        iterable = value
        new_values = list(iterable)
        if state.has_identity:
            if not self._supports_dynamic_iteration:
                raise exc.InvalidRequestError(
                    f'Collection "{self}" does not support implicit '
                    "iteration; collection replacement operations "
                    "can't be used"
                )
            old_collection = util.IdentitySet(
                self.get(state, dict_, passive=passive)
            )

        collection_history = self._modified_event(state, dict_)
        if not state.has_identity:
            old_collection = collection_history.added_items
        else:
            old_collection = old_collection.union(
                collection_history.added_items
            )

        constants = old_collection.intersection(new_values)
        additions = util.IdentitySet(new_values).difference(constants)
        removals = old_collection.difference(constants)

        for member in new_values:
            if member in additions:
                self.fire_append_event(
                    state,
                    dict_,
                    member,
                    None,
                    collection_history=collection_history,
                )

        for member in removals:
            self.fire_remove_event(
                state,
                dict_,
                member,
                None,
                collection_history=collection_history,
            )

    def delete(self, *args, **kwargs):
        raise NotImplementedError()

    def set_committed_value(self, state, dict_, value):
        raise NotImplementedError(
            "Dynamic attributes don't support collection population."
        )

    def get_history(self, state, dict_, passive=attributes.PASSIVE_NO_FETCH):
        c = self._get_collection_history(state, passive)
        return c.as_history()

    def get_all_pending(
        self, state, dict_, passive=attributes.PASSIVE_NO_INITIALIZE
    ):
        c = self._get_collection_history(state, passive)
        return [(attributes.instance_state(x), x) for x in c.all_items]

    def _get_collection_history(self, state, passive):
        if self.key in state.committed_state:
            c = state.committed_state[self.key]
        else:
            c = self.collection_history_cls(
                self, state, PassiveFlag.PASSIVE_NO_FETCH
            )

        if state.has_identity and (passive & attributes.INIT_OK):
            return self.collection_history_cls(
                self, state, passive, apply_to=c
            )
        else:
            return c

    def append(
        self,
        state,
        dict_,
        value,
        initiator,
        passive=attributes.PASSIVE_NO_FETCH,
    ):
        if initiator is not self:
            self.fire_append_event(state, dict_, value, initiator)

    def remove(
        self,
        state,
        dict_,
        value,
        initiator,
        passive=attributes.PASSIVE_NO_FETCH,
    ):
        if initiator is not self:
            self.fire_remove_event(state, dict_, value, initiator)

    def pop(
        self,
        state,
        dict_,
        value,
        initiator,
        passive=attributes.PASSIVE_NO_FETCH,
    ):
        self.remove(state, dict_, value, initiator, passive=passive)


@log.class_logger
@relationships.RelationshipProperty.strategy_for(lazy="write_only")
class WriteOnlyLoader(strategies.AbstractRelationshipLoader, log.Identified):
    impl_class = WriteOnlyAttributeImpl

    def init_class_attribute(self, mapper):
        self.is_class_level = True
        if not self.uselist or self.parent_property.direction not in (
            interfaces.ONETOMANY,
            interfaces.MANYTOMANY,
        ):
            raise exc.InvalidRequestError(
                "On relationship %s, 'dynamic' loaders cannot be used with "
                "many-to-one/one-to-one relationships and/or "
                "uselist=False." % self.parent_property
            )

        strategies._register_attribute(
            self.parent_property,
            mapper,
            useobject=True,
            impl_class=self.impl_class,
            target_mapper=self.parent_property.mapper,
            order_by=self.parent_property.order_by,
            query_class=self.parent_property.query_class,
        )


class DynamicCollectionAdapter:
    """simplified CollectionAdapter for internal API consistency"""

    def __init__(self, data):
        self.data = data

    def __iter__(self):
        return iter(self.data)

    def _reset_empty(self):
        pass

    def __len__(self):
        return len(self.data)

    def __bool__(self):
        return True


class AbstractCollectionWriter(Generic[_T]):
    """Virtual collection which includes append/remove methods that synchronize
    into the attribute event system.

    """

    if not TYPE_CHECKING:
        __slots__ = ()

    def __init__(self, attr, state):
        self.instance = instance = state.obj()
        self.attr = attr

        mapper = object_mapper(instance)
        prop = mapper._props[self.attr.key]

        if prop.secondary is not None:
            # this is a hack right now.  The Query only knows how to
            # make subsequent joins() without a given left-hand side
            # from self._from_obj[0].  We need to ensure prop.secondary
            # is in the FROM.  So we purposely put the mapper selectable
            # in _from_obj[0] to ensure a user-defined join() later on
            # doesn't fail, and secondary is then in _from_obj[1].

            # note also, we are using the official ORM-annotated selectable
            # from __clause_element__(), see #7868
            self._from_obj = (prop.mapper.__clause_element__(), prop.secondary)
        else:
            self._from_obj = ()

        self._where_criteria = (
            prop._with_parent(instance, alias_secondary=False),
        )

        if self.attr.order_by:
            self._order_by_clauses = self.attr.order_by
        else:
            self._order_by_clauses = ()

    def _add_all_impl(self, iterator: Iterable[_T]) -> None:
        for item in iterator:
            self.attr.append(
                attributes.instance_state(self.instance),
                attributes.instance_dict(self.instance),
                item,
                None,
            )

    def _remove_impl(self, item: _T) -> None:
        self.attr.remove(
            attributes.instance_state(self.instance),
            attributes.instance_dict(self.instance),
            item,
            None,
        )


class WriteOnlyCollection(AbstractCollectionWriter[_T]):
    """Write-only collection which can synchronize changes into the
    attribute event system.

    The :class:`.WriteOnlyCollection` is used in a mapping by
    using the ``"write_only"`` lazy loading strategy with
    :func:`_orm.relationship`.     For background on this configuration,
    see :ref:`write_only_relationship`.

    .. versionadded:: 2.0

    .. seealso::

        :ref:`write_only_relationship`

    """

    __slots__ = (
        "instance",
        "attr",
        "_where_criteria",
        "_from_obj",
        "_order_by_clauses",
    )

    def __iter__(self) -> NoReturn:
        raise TypeError(
            "WriteOnly collections don't support iteration in-place; "
            "to query for collection items, use the select() method to "
            "produce a SQL statement and execute it with session.scalars()."
        )

    def select(self) -> Select[Tuple[_T]]:
        """Produce a :class:`_sql.Select` construct that represents the
        rows within this instance-local :class:`_orm.WriteOnlyCollection`.

        """
        stmt = select(self.attr.target_mapper).where(*self._where_criteria)
        if self._from_obj:
            stmt = stmt.select_from(*self._from_obj)
        if self._order_by_clauses:
            stmt = stmt.order_by(*self._order_by_clauses)
        return stmt

    def insert(self) -> Insert[_T]:
        """For one-to-many collections, produce a :class:`_dml.Insert` which
        will insert new rows in terms of this this instance-local
        :class:`_orm.WriteOnlyCollection`.

        This construct is only supported for a :class:`_orm.Relationship`
        that does **not** include the :paramref:`_orm.relationship.secondary`
        parameter.  For relationships that refer to a many-to-many table,
        use ordinary bulk insert techniques to produce new objects, then
        use :meth:`_orm.AbstractCollectionWriter.add_all` to associate them
        with the collection.


        """

        state = inspect(self.instance)
        mapper = state.mapper
        prop = mapper._props[self.attr.key]

        if prop.direction is not RelationshipDirection.ONETOMANY:
            raise exc.InvalidRequestError(
                "Write only bulk INSERT only supported for one-to-many "
                "collections; for many-to-many, use a separate bulk "
                "INSERT along with add_all()."
            )

        dict_ = {}

        for l, r in prop.synchronize_pairs:
            fn = prop._get_attr_w_warn_on_none(
                mapper,
                state,
                state.dict,
                l,
            )

            dict_[r.key] = bindparam(None, callable_=fn)

        return insert(self.attr.target_mapper).values(**dict_)

    def update(self) -> Update[_T]:
        """Produce a :class:`_dml.Update` which will refer to rows in terms
        of this instance-local :class:`_orm.WriteOnlyCollection`.

        """
        return update(self.attr.target_mapper).where(*self._where_criteria)

    def delete(self) -> Delete[_T]:
        """Produce a :class:`_dml.Delete` which will refer to rows in terms
        of this instance-local :class:`_orm.WriteOnlyCollection`.

        """
        return delete(self.attr.target_mapper).where(*self._where_criteria)

    def add_all(self, iterator: Iterable[_T]) -> None:
        """Add an iterable of items to this :class:`_orm.WriteOnlyCollection`.

        The given items will be persisted to the database in terms of
        the parent instance's collection on the next flush.

        """
        self._add_all_impl(iterator)

    def add(self, item: _T) -> None:
        """Add an item to this :class:`_orm.WriteOnlyCollection`.

        The given item will be persisted to the database in terms of
        the parent instance's collection on the next flush.

        """
        self._add_all_impl([item])

    def remove(self, item: _T) -> None:
        """Remove an item from this :class:`_orm.WriteOnlyCollection`.

        The given item will be removed from the parent instance's collection on
        the next flush.

        """
        self._remove_impl(item)
