# orm/dynamic.py
# Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors


"""Dynamic collection API.

Dynamic collections act like Query() objects for read operations and support
basic add/delete mutation.

.. legacy:: the "dynamic" loader is a legacy feature, superseded by the
 "write_only" loader.


"""

from __future__ import annotations

from typing import Any
from typing import Iterable
from typing import Iterator
from typing import TYPE_CHECKING
from typing import TypeVar

from . import attributes
from . import exc as orm_exc
from . import relationships
from . import util as orm_util
from .query import Query
from .session import object_session
from .writeonly import AbstractCollectionWriter
from .writeonly import WriteOnlyAttributeImpl
from .writeonly import WriteOnlyHistory
from .writeonly import WriteOnlyLoader
from .. import util
from ..engine import result

if TYPE_CHECKING:
    from .session import Session


_T = TypeVar("_T", bound=Any)


class DynamicCollectionHistory(WriteOnlyHistory):
    def __init__(self, attr, state, passive, apply_to=None):
        if apply_to:
            coll = AppenderQuery(attr, state).autoflush(False)
            self.unchanged_items = util.OrderedIdentitySet(coll)
            self.added_items = apply_to.added_items
            self.deleted_items = apply_to.deleted_items
            self._reconcile_collection = True
        else:
            self.deleted_items = util.OrderedIdentitySet()
            self.added_items = util.OrderedIdentitySet()
            self.unchanged_items = util.OrderedIdentitySet()
            self._reconcile_collection = False


class DynamicAttributeImpl(WriteOnlyAttributeImpl):
    _supports_dynamic_iteration = True
    collection_history_cls = DynamicCollectionHistory

    def __init__(
        self,
        class_,
        key,
        typecallable,
        dispatch,
        target_mapper,
        order_by,
        query_class=None,
        **kw,
    ):
        attributes.AttributeImpl.__init__(
            self, class_, key, typecallable, dispatch, **kw
        )
        self.target_mapper = target_mapper
        if order_by:
            self.order_by = tuple(order_by)
        if not query_class:
            self.query_class = AppenderQuery
        elif AppenderMixin in query_class.mro():
            self.query_class = query_class
        else:
            self.query_class = mixin_user_query(query_class)


@relationships.RelationshipProperty.strategy_for(lazy="dynamic")
class DynaLoader(WriteOnlyLoader):
    impl_class = DynamicAttributeImpl


class AppenderMixin(AbstractCollectionWriter[_T]):
    """A mixin that expects to be mixing in a Query class with
    AbstractAppender.


    """

    query_class = None

    def __init__(self, attr, state):
        Query.__init__(self, attr.target_mapper, None)
        super().__init__(attr, state)

    @property
    def session(self) -> Session:
        sess = object_session(self.instance)
        if (
            sess is not None
            and self.autoflush
            and sess.autoflush
            and self.instance in sess
        ):
            sess.flush()
        if not orm_util.has_identity(self.instance):
            return None
        else:
            return sess

    @session.setter
    def session(self, session: Session) -> None:
        self.sess = session

    def _iter(self):
        sess = self.session
        if sess is None:
            state = attributes.instance_state(self.instance)
            if state.detached:
                util.warn(
                    "Instance %s is detached, dynamic relationship cannot "
                    "return a correct result.   This warning will become "
                    "a DetachedInstanceError in a future release."
                    % (orm_util.state_str(state))
                )

            return result.IteratorResult(
                result.SimpleResultMetaData([self.attr.class_.__name__]),
                self.attr._get_collection_history(
                    attributes.instance_state(self.instance),
                    attributes.PASSIVE_NO_INITIALIZE,
                ).added_items,
                _source_supports_scalars=True,
            ).scalars()
        else:
            return self._generate(sess)._iter()

    if TYPE_CHECKING:

        def __iter__(self) -> Iterator[_T]:
            ...

    def __getitem__(self, index: Any) -> _T:
        sess = self.session
        if sess is None:
            return self.attr._get_collection_history(
                attributes.instance_state(self.instance),
                attributes.PASSIVE_NO_INITIALIZE,
            ).indexed(index)
        else:
            return self._generate(sess).__getitem__(index)

    def count(self) -> int:
        sess = self.session
        if sess is None:
            return len(
                self.attr._get_collection_history(
                    attributes.instance_state(self.instance),
                    attributes.PASSIVE_NO_INITIALIZE,
                ).added_items
            )
        else:
            return self._generate(sess).count()

    def _generate(self, sess=None):
        # note we're returning an entirely new Query class instance
        # here without any assignment capabilities; the class of this
        # query is determined by the session.
        instance = self.instance
        if sess is None:
            sess = object_session(instance)
            if sess is None:
                raise orm_exc.DetachedInstanceError(
                    "Parent instance %s is not bound to a Session, and no "
                    "contextual session is established; lazy load operation "
                    "of attribute '%s' cannot proceed"
                    % (orm_util.instance_str(instance), self.attr.key)
                )

        if self.query_class:
            query = self.query_class(self.attr.target_mapper, session=sess)
        else:
            query = sess.query(self.attr.target_mapper)

        query._where_criteria = self._where_criteria
        query._from_obj = self._from_obj
        query._order_by_clauses = self._order_by_clauses

        return query

    def add_all(self, iterator: Iterable[_T]) -> None:
        """Add an iterable of items to this :class:`_orm.AppenderQuery`.

        The given items will be persisted to the database in terms of
        the parent instance's collection on the next flush.

        This method is provided to assist in delivering forwards-compatibility
        with the :class:`_orm.WriteOnlyCollection` collection class.

        .. versionadded:: 2.0

        """
        self._add_all_impl(iterator)

    def add(self, item: _T) -> None:
        """Add an item to this :class:`_orm.AppenderQuery`.

        The given item will be persisted to the database in terms of
        the parent instance's collection on the next flush.

        This method is provided to assist in delivering forwards-compatibility
        with the :class:`_orm.WriteOnlyCollection` collection class.

        .. versionadded:: 2.0

        """
        self._add_all_impl([item])

    def extend(self, iterator: Iterable[_T]) -> None:
        """Add an iterable of items to this :class:`_orm.AppenderQuery`.

        The given items will be persisted to the database in terms of
        the parent instance's collection on the next flush.

        """
        self._add_all_impl(iterator)

    def append(self, item: _T) -> None:
        """Append an item to this :class:`_orm.AppenderQuery`.

        The given item will be persisted to the database in terms of
        the parent instance's collection on the next flush.

        """
        self._add_all_impl([item])

    def remove(self, item: _T) -> None:
        """Remove an item from this :class:`_orm.AppenderQuery`.

        The given item will be removed from the parent instance's collection on
        the next flush.

        """
        self._remove_impl(item)


class AppenderQuery(AppenderMixin[_T], Query[_T]):
    """A dynamic query that supports basic collection storage operations.

    Methods on :class:`.AppenderQuery` include all methods of
    :class:`_orm.Query`, plus additional methods used for collection
    persistence.


    """


def mixin_user_query(cls):
    """Return a new class with AppenderQuery functionality layered over."""
    name = "Appender" + cls.__name__
    return type(name, (AppenderMixin, cls), {"query_class": cls})
