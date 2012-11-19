# orm/dynamic.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Dynamic collection API.

Dynamic collections act like Query() objects for read operations and support
basic add/delete mutation.

"""

from .. import log, util, exc
from ..sql import operators
from . import (
    attributes, object_session, util as orm_util, strategies,
    object_mapper, exc as orm_exc, collections
    )
from .query import Query


class DynaLoader(strategies.AbstractRelationshipLoader):
    def init_class_attribute(self, mapper):
        self.is_class_level = True
        if not self.uselist:
            raise exc.InvalidRequestError(
                "On relationship %s, 'dynamic' loaders cannot be used with "
                "many-to-one/one-to-one relationships and/or "
                "uselist=False." % self.parent_property)
        strategies._register_attribute(self,
            mapper,
            useobject=True,
            impl_class=DynamicAttributeImpl,
            target_mapper=self.parent_property.mapper,
            order_by=self.parent_property.order_by,
            query_class=self.parent_property.query_class
        )

log.class_logger(DynaLoader)


class DynamicAttributeImpl(attributes.AttributeImpl):
    uses_objects = True
    accepts_scalar_loader = False
    supports_population = False
    collection = False

    def __init__(self, class_, key, typecallable,
                     dispatch,
                     target_mapper, order_by, query_class=None, **kw):
        super(DynamicAttributeImpl, self).\
                    __init__(class_, key, typecallable, dispatch, **kw)
        self.target_mapper = target_mapper
        self.order_by = order_by
        if not query_class:
            self.query_class = AppenderQuery
        elif AppenderMixin in query_class.mro():
            self.query_class = query_class
        else:
            self.query_class = mixin_user_query(query_class)

    def get(self, state, dict_, passive=attributes.PASSIVE_OFF):
        if not passive & attributes.SQL_OK:
            return self._get_collection_history(state,
                    attributes.PASSIVE_NO_INITIALIZE).added_items
        else:
            return self.query_class(self, state)

    def get_collection(self, state, dict_, user_data=None,
                            passive=attributes.PASSIVE_NO_INITIALIZE):
        if not passive & attributes.SQL_OK:
            return self._get_collection_history(state,
                    passive).added_items
        else:
            history = self._get_collection_history(state, passive)
            return history.added_items + history.unchanged_items

    def fire_append_event(self, state, dict_, value, initiator):
        collection_history = self._modified_event(state, dict_)
        collection_history.added_items.append(value)

        for fn in self.dispatch.append:
            value = fn(state, value, initiator or self)

        if self.trackparent and value is not None:
            self.sethasparent(attributes.instance_state(value), state, True)

    def fire_remove_event(self, state, dict_, value, initiator):
        collection_history = self._modified_event(state, dict_)
        collection_history.deleted_items.append(value)

        if self.trackparent and value is not None:
            self.sethasparent(attributes.instance_state(value), state, False)

        for fn in self.dispatch.remove:
            fn(state, value, initiator or self)

    def _modified_event(self, state, dict_):

        if self.key not in state.committed_state:
            state.committed_state[self.key] = CollectionHistory(self, state)

        state._modified_event(dict_,
                                self,
                                attributes.NEVER_SET)

        # this is a hack to allow the fixtures.ComparableEntity fixture
        # to work
        dict_[self.key] = True
        return state.committed_state[self.key]

    def set(self, state, dict_, value, initiator,
                        passive=attributes.PASSIVE_OFF,
                        check_old=None, pop=False):
        if initiator and initiator.parent_token is self.parent_token:
            return

        if pop and value is None:
            return
        self._set_iterable(state, dict_, value)

    def _set_iterable(self, state, dict_, iterable, adapter=None):
        collection_history = self._modified_event(state, dict_)
        new_values = list(iterable)
        if state.has_identity:
            old_collection = list(self.get(state, dict_))
        else:
            old_collection = []
        collections.bulk_replace(new_values, DynCollectionAdapter(self,
                                 state, old_collection),
                                 DynCollectionAdapter(self, state,
                                 new_values))

    def delete(self, *args, **kwargs):
        raise NotImplementedError()

    def set_committed_value(self, state, dict_, value):
        raise NotImplementedError("Dynamic attributes don't support "
                                  "collection population.")

    def get_history(self, state, dict_, passive=attributes.PASSIVE_OFF):
        c = self._get_collection_history(state, passive)
        return attributes.History(c.added_items, c.unchanged_items,
                                  c.deleted_items)

    def get_all_pending(self, state, dict_):
        c = self._get_collection_history(
            state, attributes.PASSIVE_NO_INITIALIZE)
        return [
                (attributes.instance_state(x), x)
                for x in
                c.added_items + c.unchanged_items + c.deleted_items
            ]

    def _get_collection_history(self, state, passive=attributes.PASSIVE_OFF):
        if self.key in state.committed_state:
            c = state.committed_state[self.key]
        else:
            c = CollectionHistory(self, state)

        # TODO: consider using a different flag here, possibly
        # one local to dynamic
        if passive & attributes.INIT_OK:
            return CollectionHistory(self, state, apply_to=c)
        else:
            return c

    def append(self, state, dict_, value, initiator,
                            passive=attributes.PASSIVE_OFF):
        if initiator is not self:
            self.fire_append_event(state, dict_, value, initiator)

    def remove(self, state, dict_, value, initiator,
                            passive=attributes.PASSIVE_OFF):
        if initiator is not self:
            self.fire_remove_event(state, dict_, value, initiator)


class DynCollectionAdapter(object):
    """the dynamic analogue to orm.collections.CollectionAdapter"""

    def __init__(self, attr, owner_state, data):
        self.attr = attr
        self.state = owner_state
        self.data = data

    def __iter__(self):
        return iter(self.data)

    def append_with_event(self, item, initiator=None):
        self.attr.append(self.state, self.state.dict, item, initiator)

    def remove_with_event(self, item, initiator=None):
        self.attr.remove(self.state, self.state.dict, item, initiator)

    def append_without_event(self, item):
        pass

    def remove_without_event(self, item):
        pass


class AppenderMixin(object):
    query_class = None

    def __init__(self, attr, state):
        super(AppenderMixin, self).__init__(attr.target_mapper, None)
        self.instance = instance = state.obj()
        self.attr = attr

        mapper = object_mapper(instance)
        prop = mapper._props[self.attr.key]
        self._criterion = prop.compare(
                            operators.eq,
                            instance,
                            value_is_parent=True,
                            alias_secondary=False)

        if self.attr.order_by:
            self._order_by = self.attr.order_by

    def __session(self):
        sess = object_session(self.instance)
        if sess is not None and self.autoflush and sess.autoflush \
            and self.instance in sess:
            sess.flush()
        if not orm_util.has_identity(self.instance):
            return None
        else:
            return sess

    def session(self):
        return self.__session()
    session = property(session, lambda s, x: None)

    def __iter__(self):
        sess = self.__session()
        if sess is None:
            return iter(self.attr._get_collection_history(
                attributes.instance_state(self.instance),
                attributes.PASSIVE_NO_INITIALIZE).added_items)
        else:
            return iter(self._clone(sess))

    def __getitem__(self, index):
        sess = self.__session()
        if sess is None:
            return self.attr._get_collection_history(
                attributes.instance_state(self.instance),
                attributes.PASSIVE_NO_INITIALIZE).added_items.\
                    __getitem__(index)
        else:
            return self._clone(sess).__getitem__(index)

    def count(self):
        sess = self.__session()
        if sess is None:
            return len(self.attr._get_collection_history(
                attributes.instance_state(self.instance),
                attributes.PASSIVE_NO_INITIALIZE).added_items)
        else:
            return self._clone(sess).count()

    def _clone(self, sess=None):
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
                    "of attribute '%s' cannot proceed" % (
                        orm_util.instance_str(instance), self.attr.key))

        if self.query_class:
            query = self.query_class(self.attr.target_mapper, session=sess)
        else:
            query = sess.query(self.attr.target_mapper)

        query._criterion = self._criterion
        query._order_by = self._order_by

        return query

    def append(self, item):
        self.attr.append(
            attributes.instance_state(self.instance),
            attributes.instance_dict(self.instance), item, None)

    def remove(self, item):
        self.attr.remove(
            attributes.instance_state(self.instance),
            attributes.instance_dict(self.instance), item, None)


class AppenderQuery(AppenderMixin, Query):
    """A dynamic query that supports basic collection storage operations."""


def mixin_user_query(cls):
    """Return a new class with AppenderQuery functionality layered over."""
    name = 'Appender' + cls.__name__
    return type(name, (AppenderMixin, cls), {'query_class': cls})


class CollectionHistory(object):
    """Overrides AttributeHistory to receive append/remove events directly."""

    def __init__(self, attr, state, apply_to=None):
        if apply_to:
            deleted = util.IdentitySet(apply_to.deleted_items)
            added = apply_to.added_items
            coll = AppenderQuery(attr, state).autoflush(False)
            self.unchanged_items = [o for o in util.IdentitySet(coll)
                                    if o not in deleted]
            self.added_items = apply_to.added_items
            self.deleted_items = apply_to.deleted_items
        else:
            self.deleted_items = []
            self.added_items = []
            self.unchanged_items = []
