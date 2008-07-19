"""'dynamic' collection API.  returns Query() objects on the 'read' side, alters
a special AttributeHistory on the 'write' side."""

from sqlalchemy import exceptions, util, logging
from sqlalchemy.orm import attributes, object_session, util as mapperutil, strategies
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.mapper import has_identity, object_mapper
from sqlalchemy.orm.util import _state_has_identity

class DynaLoader(strategies.AbstractRelationLoader):
    def init_class_attribute(self):
        self.is_class_level = True
        self._register_attribute(self.parent.class_, impl_class=DynamicAttributeImpl, target_mapper=self.parent_property.mapper, order_by=self.parent_property.order_by)

    def create_row_processor(self, selectcontext, mapper, row):
        return (None, None, None)

DynaLoader.logger = logging.class_logger(DynaLoader)

class DynamicAttributeImpl(attributes.AttributeImpl):
    def __init__(self, class_, key, typecallable, target_mapper, order_by, **kwargs):
        super(DynamicAttributeImpl, self).__init__(class_, key, typecallable, **kwargs)
        self.target_mapper = target_mapper
        self.order_by=order_by
        self.query_class = AppenderQuery

    def get(self, state, passive=False):
        if passive:
            return self._get_collection_history(state, passive=True).added_items
        else:
            return self.query_class(self, state)

    def get_collection(self, state, user_data=None, passive=True):
        if passive:
            return self._get_collection_history(state, passive=passive).added_items
        else:
            history = self._get_collection_history(state, passive=passive)
            return history.added_items + history.unchanged_items

    def fire_append_event(self, state, value, initiator):
        collection_history = self._modified_event(state)
        collection_history.added_items.append(value)

        if self.trackparent and value is not None:
            self.sethasparent(value._state, True)
        instance = state.obj()
        for ext in self.extensions:
            ext.append(instance, value, initiator or self)

    def fire_remove_event(self, state, value, initiator):
        collection_history = self._modified_event(state)
        collection_history.deleted_items.append(value)

        if self.trackparent and value is not None:
            self.sethasparent(value._state, False)

        instance = state.obj()
        for ext in self.extensions:
            ext.remove(instance, value, initiator or self)
    
    def _modified_event(self, state):
        state.modified = True
        if self.key not in state.committed_state:
            state.committed_state[self.key] = CollectionHistory(self, state)

        # this is a hack to allow the _base.ComparableEntity fixture
        # to work
        state.dict[self.key] = True
        
        return state.committed_state[self.key]
        
    def set(self, state, value, initiator):
        if initiator is self:
            return
        
        collection_history = self._modified_event(state)
        if _state_has_identity(state):
            old_collection = list(self.get(state))
        else:
            old_collection = []
        collection_history.replace(old_collection, value)

    def delete(self, *args, **kwargs):
        raise NotImplementedError()
        
    def get_history(self, state, passive=False):
        c = self._get_collection_history(state, passive)
        return (c.added_items, c.unchanged_items, c.deleted_items)
        
    def _get_collection_history(self, state, passive=False):
        if self.key in state.committed_state:
            c = state.committed_state[self.key]
        else:
            c = CollectionHistory(self, state)
            
        if not passive:
            return CollectionHistory(self, state, apply_to=c)
        else:
            return c
        
    def append(self, state, value, initiator, passive=False):
        if initiator is not self:
            self.fire_append_event(state, value, initiator)
    
    def remove(self, state, value, initiator, passive=False):
        if initiator is not self:
            self.fire_remove_event(state, value, initiator)

        
class AppenderQuery(Query):
    def __init__(self, attr, state):
        super(AppenderQuery, self).__init__(attr.target_mapper, None)
        self.instance = state.obj()
        self.attr = attr
    
    def __session(self):
        sess = object_session(self.instance)
        if sess is not None and self.autoflush and sess.autoflush and self.instance in sess:
            sess.flush()
        if not has_identity(self.instance):
            return None
        else:
            return sess
    
    def session(self):
        return self.__session()
    session = property(session)
    
    def __iter__(self):
        sess = self.__session()
        if sess is None:
            return iter(self.attr._get_collection_history(self.instance._state, passive=True).added_items)
        else:
            return iter(self._clone(sess))

    def __getitem__(self, index):
        sess = self.__session()
        if sess is None:
            return self.attr._get_collection_history(self.instance._state, passive=True).added_items.__getitem__(index)
        else:
            return self._clone(sess).__getitem__(index)
    
    def count(self):
        sess = self.__session()
        if sess is None:
            return len(self.attr._get_collection_history(self.instance._state, passive=True).added_items)
        else:
            return self._clone(sess).count()
    
    def _clone(self, sess=None):
        # note we're returning an entirely new Query class instance here
        # without any assignment capabilities;
        # the class of this query is determined by the session.
        instance = self.instance
        if sess is None:
            sess = object_session(instance)
            if sess is None:
                try:
                    sess = object_mapper(instance).get_session()
                except exceptions.InvalidRequestError:
                    raise exceptions.UnboundExecutionError("Parent instance %s is not bound to a Session, and no contextual session is established; lazy load operation of attribute '%s' cannot proceed" % (mapperutil.instance_str(instance), self.attr.key))

        q = sess.query(self.attr.target_mapper).with_parent(instance, self.attr.key)
        if self.attr.order_by:
            q = q.order_by(self.attr.order_by)
        return q

    def append(self, item):
        self.attr.append(self.instance._state, item, None)

    def remove(self, item):
        self.attr.remove(self.instance._state, item, None)

            
class CollectionHistory(object): 
    """Overrides AttributeHistory to receive append/remove events directly."""

    def __init__(self, attr, state, apply_to=None):
        if apply_to:
            deleted = util.IdentitySet(apply_to.deleted_items)
            added = apply_to.added_items
            coll = AppenderQuery(attr, state).autoflush(False)
            self.unchanged_items = [o for o in util.IdentitySet(coll) if o not in deleted]
            self.added_items = apply_to.added_items
            self.deleted_items = apply_to.deleted_items
        else:
            self.deleted_items = []
            self.added_items = []
            self.unchanged_items = []
            
    def replace(self, olditems, newitems):
        self.added_items = newitems
        self.deleted_items = olditems
        
