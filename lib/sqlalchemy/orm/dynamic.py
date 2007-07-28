"""'dynamic' collection API.  returns Query() objects on the 'read' side, alters
a special AttributeHistory on the 'write' side."""

from sqlalchemy import exceptions
from sqlalchemy.orm import attributes, Query, object_session
from sqlalchemy.orm.mapper import has_identity

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
        pass
    
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
    
    def __len__(self):
        if not has_identity(self.instance):
            # TODO: all these various calls to _added_items should be more
            # intelligently calculated from the CollectionHistory object 
            # (i.e. account for deletes too)
            return len(self.attr.get_history(self.instance)._added_items)
        else:
            return self._clone().count()
        
    def __iter__(self):
        if not has_identity(self.instance):
            return iter(self.attr.get_history(self.instance)._added_items)
        else:
            return iter(self._clone())

    def __getitem__(self, index):
        if not has_identity(self.instance):
            # TODO: hmm
            return self.attr.get_history(self.instance)._added_items.__getitem__(index)
        else:
            return self._clone().__getitem__(index)
        
    def _clone(self):
        # note we're returning an entirely new Query class instance here
        # without any assignment capabilities;
        # the class of this query is determined by the session.
        sess = object_session(self.instance)
        if sess is None:
            try:
                sess = mapper.object_mapper(instance).get_session()
            except exceptions.InvalidRequestError:
                raise exceptions.InvalidRequestError("Parent instance %s is not bound to a Session, and no contextual session is established; lazy load operation of attribute '%s' cannot proceed" % (instance.__class__, self.key))

        return sess.query(self.attr.target_mapper).with_parent(self.instance)

    def assign(self, collection):
        if has_identity(self.instance):
            oldlist = list(self)
        else:
            oldlist = []
        self.attr.get_history(self.instance).replace(oldlist, collection)
        return oldlist
        
    def append(self, item):
        self.attr.append(self.instance, item, self.attr)

    # TODO:jek: I think this should probably be axed, time will tell.
    def remove(self, item):
        self.attr.remove(self.instance, item, self.attr)
            
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
    
