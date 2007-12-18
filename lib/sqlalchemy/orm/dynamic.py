"""'dynamic' collection API.  returns Query() objects on the 'read' side, alters
a special AttributeHistory on the 'write' side."""

from sqlalchemy import exceptions, util
from sqlalchemy.orm import attributes, object_session, util as mapperutil
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.mapper import has_identity, object_mapper

class DynamicAttributeImpl(attributes.AttributeImpl):
    def __init__(self, class_, key, typecallable, target_mapper, **kwargs):
        super(DynamicAttributeImpl, self).__init__(class_, key, typecallable, **kwargs)
        self.target_mapper = target_mapper

    def get(self, state, passive=False):
        if passive:
            return self._get_collection(state, passive=True).added_items
        else:
            return AppenderQuery(self, state)

    def get_collection(self, state, user_data=None):
        return self._get_collection(state, passive=True).added_items

    def fire_append_event(self, state, value, initiator):
        state.modified = True

        if self.trackparent and value is not None:
            self.sethasparent(value._state, True)
        instance = state.obj()
        for ext in self.extensions:
            ext.append(instance, value, initiator or self)

    def fire_remove_event(self, state, value, initiator):
        state.modified = True

        if self.trackparent and value is not None:
            self.sethasparent(value._state, False)

        instance = state.obj()
        for ext in self.extensions:
            ext.remove(instance, value, initiator or self)
        
    def set(self, state, value, initiator):
        if initiator is self:
            return

        old_collection = self.get(state).assign(value)

        # TODO: emit events ???
        state.modified = True

    def delete(self, *args, **kwargs):
        raise NotImplementedError()
        
    def get_history(self, state, passive=False):
        c = self._get_collection(state, passive)
        return (c.added_items, c.unchanged_items, c.deleted_items)
        
    def _get_collection(self, state, passive=False):
        try:
            c = state.dict[self.key]
        except KeyError:
            state.dict[self.key] = c = CollectionHistory(self, state)

        if not passive:
            return CollectionHistory(self, state, apply_to=c)
        else:
            return c
        
    def append(self, state, value, initiator, passive=False):
        if initiator is not self:
            self._get_collection(state, passive=True).added_items.append(value)
            self.fire_append_event(state, value, initiator)
    
    def remove(self, state, value, initiator, passive=False):
        if initiator is not self:
            self._get_collection(state, passive=True).deleted_items.append(value)
            self.fire_remove_event(state, value, initiator)

            
class AppenderQuery(Query):
    def __init__(self, attr, state):
        super(AppenderQuery, self).__init__(attr.target_mapper, None)
        self.state = state
        self.attr = attr
    
    def __session(self):
        instance = self.state.obj()
        sess = object_session(instance)
        if sess is not None and self.autoflush and sess.autoflush and instance in sess:
            sess.flush()
        if not has_identity(instance):
            return None
        else:
            return sess
    
    def session(self):
        return self.__session()
    session = property(session)
    
    def __iter__(self):
        sess = self.__session()
        if sess is None:
            return iter(self.attr._get_collection(self.state, passive=True).added_items)
        else:
            return iter(self._clone(sess))

    def __getitem__(self, index):
        sess = self.__session()
        if sess is None:
            return self.attr._get_collection(self.state, passive=True).added_items.__getitem__(index)
        else:
            return self._clone(sess).__getitem__(index)
    
    def count(self):
        sess = self.__session()
        if sess is None:
            return len(self.attr._get_collection(self.state, passive=True).added_items)
        else:
            return self._clone(sess).count()
    
    def _clone(self, sess=None):
        # note we're returning an entirely new Query class instance here
        # without any assignment capabilities;
        # the class of this query is determined by the session.
        instance = self.state.obj()
        if sess is None:
            sess = object_session(instance)
            if sess is None:
                try:
                    sess = object_mapper(instance).get_session()
                except exceptions.InvalidRequestError:
                    raise exceptions.InvalidRequestError("Parent instance %s is not bound to a Session, and no contextual session is established; lazy load operation of attribute '%s' cannot proceed" % (mapperutil.instance_str(instance), self.attr.key))

        return sess.query(self.attr.target_mapper).with_parent(instance, self.attr.key)

    def assign(self, collection):
        instance = self.state.obj()
        if has_identity(instance):
            oldlist = list(self)
        else:
            oldlist = []
        self.attr._get_collection(self.state, passive=True).replace(oldlist, collection)
        return oldlist
        
    def append(self, item):
        self.attr.append(self.state, item, None)

    def remove(self, item):
        self.attr.remove(self.state, item, None)

            
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
        
