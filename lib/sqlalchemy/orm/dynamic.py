"""'dynamic' collection API.  returns Query() objects on the 'read' side, alters
a special AttributeHistory on the 'write' side."""

from sqlalchemy import exceptions
from sqlalchemy.orm import attributes, object_session
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.mapper import has_identity, object_mapper

class DynamicCollectionAttribute(attributes.InstrumentedAttribute):
    def __init__(self, class_, attribute_manager, key, typecallable, target_mapper, **kwargs):
        super(DynamicCollectionAttribute, self).__init__(class_, attribute_manager, key, typecallable, **kwargs)
        self.target_mapper = target_mapper

    def get(self, obj, passive=False):
        if passive:
            return self.get_history(obj, passive=True).added_items()
        else:
            return AppenderQuery(self, obj)

    def commit_to_state(self, state, obj, value=attributes.NO_VALUE):
        # we have our own AttributeHistory therefore dont need CommittedState
        # instead, we reset the history stored on the attribute
        obj.__dict__[self.key] = CollectionHistory(self, obj)
    
    def set(self, obj, value, initiator):
        if initiator is self:
            return

        state = obj._state

        old_collection = self.get(obj).assign(value)

        # TODO: emit events ???
        state['modified'] = True

    def delete(self, *args, **kwargs):
        raise NotImplementedError()
        
    def get_history(self, obj, passive=False):
        try:
            return obj.__dict__[self.key]
        except KeyError:
            obj.__dict__[self.key] = c = CollectionHistory(self, obj)
            return c

    def append(self, obj, value, initiator):
        if initiator is not self:
            self.get_history(obj)._added_items.append(value)
            self.fire_append_event(obj, value, self)
    
    def remove(self, obj, value, initiator):
        if initiator is not self:
            self.get_history(obj)._deleted_items.append(value)
            self.fire_remove_event(obj, value, self)

            
class AppenderQuery(Query):
    def __init__(self, attr, instance):
        super(AppenderQuery, self).__init__(attr.target_mapper, None)
        self.instance = instance
        self.attr = attr
    
    def __session(self):
        sess = object_session(self.instance)
        if sess is not None and self.instance in sess and sess.autoflush:
            sess.flush()
        if not has_identity(self.instance):
            return None
        else:
            return sess
            
    def __len__(self):
        sess = self.__session()
        if sess is None:
            return len(self.attr.get_history(self.instance)._added_items)
        else:
            return self._clone(sess).count()
        
    def __iter__(self):
        sess = self.__session()
        if sess is None:
            return iter(self.attr.get_history(self.instance)._added_items)
        else:
            return iter(self._clone(sess))

    def __getitem__(self, index):
        sess = self.__session()
        if sess is None:
            return self.attr.get_history(self.instance)._added_items.__getitem__(index)
        else:
            return self._clone(sess).__getitem__(index)

    def _clone(self, sess=None):
        # note we're returning an entirely new Query class instance here
        # without any assignment capabilities;
        # the class of this query is determined by the session.
        if sess is None:
            sess = object_session(self.instance)
            if sess is None:
                try:
                    sess = object_mapper(self.instance).get_session()
                except exceptions.InvalidRequestError:
                    raise exceptions.InvalidRequestError("Parent instance %s is not bound to a Session, and no contextual session is established; lazy load operation of attribute '%s' cannot proceed" % (self.instance.__class__, self.key))

        return sess.query(self.attr.target_mapper).with_parent(self.instance)

    def assign(self, collection):
        if has_identity(self.instance):
            oldlist = list(self)
        else:
            oldlist = []
        self.attr.get_history(self.instance).replace(oldlist, collection)
        return oldlist
        
    def append(self, item):
        self.attr.append(self.instance, item, None)

    def remove(self, item):
        self.attr.remove(self.instance, item, None)

            
class CollectionHistory(attributes.AttributeHistory): 
    """Overrides AttributeHistory to receive append/remove events directly."""

    def __init__(self, attr, obj):
        self._deleted_items = []
        self._added_items = []
        self._unchanged_items = []
        self._obj = obj
        
    def replace(self, olditems, newitems):
        self._added_items = newitems
        self._deleted_items = olditems
        
    def is_modified(self):
        return len(self._deleted_items) > 0 or len(self._added_items) > 0

    def added_items(self):
        return self._added_items

    def unchanged_items(self):
        return self._unchanged_items

    def deleted_items(self):
        return self._deleted_items
    
