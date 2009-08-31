from sqlalchemy.util import EMPTY_SET
import weakref
from sqlalchemy import util
from sqlalchemy.orm.attributes import PASSIVE_NORESULT, PASSIVE_OFF, NEVER_SET, NO_VALUE, manager_of_class, ATTR_WAS_SET
from sqlalchemy.orm import attributes
from sqlalchemy.orm import interfaces

class InstanceState(object):
    """tracks state information at the instance level."""

    session_id = None
    key = None
    runid = None
    expired_attributes = EMPTY_SET
    load_options = EMPTY_SET
    load_path = ()
    insert_order = None
    mutable_dict = None
    _strong_obj = None
    
    def __init__(self, obj, manager):
        self.class_ = obj.__class__
        self.manager = manager
        self.obj = weakref.ref(obj, self._cleanup)
        self.modified = False
        self.callables = {}
        self.expired = False
        self.committed_state = {}
        self.pending = {}
        self.parents = {}
        
    def detach(self):
        if self.session_id:
            del self.session_id

    def dispose(self):
        if self.session_id:
            del self.session_id
        del self.obj
    
    def _cleanup(self, ref):
        instance_dict = self._instance_dict()
        if instance_dict:
            instance_dict.remove(self)
        self.dispose()
    
    def obj(self):
        return None
    
    @property
    def dict(self):
        o = self.obj()
        if o is not None:
            return attributes.instance_dict(o)
        else:
            return {}
        
    @property
    def sort_key(self):
        return self.key and self.key[1] or (self.insert_order, )

    def check_modified(self):
        # TODO: deprecate
        return self.modified

    def initialize_instance(*mixed, **kwargs):
        self, instance, args = mixed[0], mixed[1], mixed[2:]
        manager = self.manager

        for fn in manager.events.on_init:
            fn(self, instance, args, kwargs)
            
        # LESSTHANIDEAL:
        # adjust for the case where the InstanceState was created before
        # mapper compilation, and this actually needs to be a MutableAttrInstanceState
        if manager.mutable_attributes and self.__class__ is not MutableAttrInstanceState:
            self.__class__ = MutableAttrInstanceState
            self.obj = weakref.ref(self.obj(), self._cleanup)
            self.mutable_dict = {}
            
        try:
            return manager.events.original_init(*mixed[1:], **kwargs)
        except:
            for fn in manager.events.on_init_failure:
                fn(self, instance, args, kwargs)
            raise

    def get_history(self, key, **kwargs):
        return self.manager.get_impl(key).get_history(self, self.dict, **kwargs)

    def get_impl(self, key):
        return self.manager.get_impl(key)

    def get_pending(self, key):
        if key not in self.pending:
            self.pending[key] = PendingCollection()
        return self.pending[key]

    def value_as_iterable(self, key, passive=PASSIVE_OFF):
        """return an InstanceState attribute as a list,
        regardless of it being a scalar or collection-based
        attribute.

        returns None if passive is not PASSIVE_OFF and the getter returns
        PASSIVE_NORESULT.
        """

        impl = self.get_impl(key)
        dict_ = self.dict
        x = impl.get(self, dict_, passive=passive)
        if x is PASSIVE_NORESULT:
            return None
        elif hasattr(impl, 'get_collection'):
            return impl.get_collection(self, dict_, x, passive=passive)
        else:
            return [x]

    def _run_on_load(self, instance):
        self.manager.events.run('on_load', instance)

    def __getstate__(self):
        d = {
            'instance':self.obj(),
        }

        d.update(
            (k, self.__dict__[k]) for k in (
                'committed_state', 'pending', 'parents', 'modified', 'expired', 
                'callables'
            ) if self.__dict__[k]
        )
        
        d.update(
            (k, self.__dict__[k]) for k in (
                'key', 'load_options', 'expired_attributes', 'mutable_dict'
            ) if k in self.__dict__ 
        )
        if self.load_path:
            d['load_path'] = interfaces.serialize_path(self.load_path)
        return d
        
    def __setstate__(self, state):
        self.obj = weakref.ref(state['instance'], self._cleanup)
        self.class_ = state['instance'].__class__
        self.manager = manager_of_class(self.class_)

        self.committed_state = state.get('committed_state', {})
        self.pending = state.get('pending', {})
        self.parents = state.get('parents', {})
        self.modified = state.get('modified', False)
        self.expired = state.get('expired', False)
        self.callables = state.get('callables', {})
        
        if self.modified:
            self._strong_obj = state['instance']
            
        self.__dict__.update(
            (k, state[k]) for k in (
                'key', 'load_options', 'expired_attributes', 'mutable_dict'
            ) if k in state 
        )

        if 'load_path' in state:
            self.load_path = interfaces.deserialize_path(state['load_path'])

    def initialize(self, key):
        self.manager.get_impl(key).initialize(self, self.dict)

    def set_callable(self, key, callable_):
        self.dict.pop(key, None)
        self.callables[key] = callable_

    def __call__(self):
        """__call__ allows the InstanceState to act as a deferred
        callable for loading expired attributes, which is also
        serializable (picklable).

        """
        unmodified = self.unmodified
        class_manager = self.manager
        class_manager.deferred_scalar_loader(self, [
            attr.impl.key for attr in class_manager.attributes if
                attr.impl.accepts_scalar_loader and
                attr.impl.key in self.expired_attributes and
                attr.impl.key in unmodified
            ])
        for k in self.expired_attributes:
            self.callables.pop(k, None)
        del self.expired_attributes
        return ATTR_WAS_SET

    @property
    def unmodified(self):
        """a set of keys which have no uncommitted changes"""
        
        return set(self.manager).difference(self.committed_state)

    @property
    def unloaded(self):
        """a set of keys which do not have a loaded value.

        This includes expired attributes and any other attribute that
        was never populated or modified.

        """
        return set(
            key for key in self.manager.iterkeys()
            if key not in self.committed_state and key not in self.dict)

    def expire_attributes(self, attribute_names, instance_dict=None):
        self.expired_attributes = set(self.expired_attributes)

        if attribute_names is None:
            attribute_names = self.manager.keys()
            self.expired = True
            if self.modified:
                if not instance_dict:
                    instance_dict = self._instance_dict()
                    if instance_dict:
                        instance_dict._modified.discard(self)
                else:
                    instance_dict._modified.discard(self)
                    
            self.modified = False
            filter_deferred = True
        else:
            filter_deferred = False
        dict_ = self.dict
        
        for key in attribute_names:
            impl = self.manager[key].impl
            if not filter_deferred or \
                not impl.dont_expire_missing or \
                key in dict_:
                self.expired_attributes.add(key)
                if impl.accepts_scalar_loader:
                    self.callables[key] = self
            dict_.pop(key, None)
            self.pending.pop(key, None)
            self.committed_state.pop(key, None)
            if self.mutable_dict:
                self.mutable_dict.pop(key, None)
                
    def reset(self, key, dict_):
        """remove the given attribute and any callables associated with it."""

        dict_.pop(key, None)
        self.callables.pop(key, None)

    def _instance_dict(self):
        return None

    def _is_really_none(self):
        return self.obj()
        
    def modified_event(self, dict_, attr, should_copy, previous, passive=PASSIVE_OFF):
        needs_committed = attr.key not in self.committed_state

        if needs_committed:
            if previous is NEVER_SET:
                if passive:
                    if attr.key in dict_:
                        previous = dict_[attr.key]
                else:
                    previous = attr.get(self, dict_)

            if should_copy and previous not in (None, NO_VALUE, NEVER_SET):
                previous = attr.copy(previous)

            if needs_committed:
                self.committed_state[attr.key] = previous

        if not self.modified:
            instance_dict = self._instance_dict()
            if instance_dict:
                instance_dict._modified.add(self)

        self.modified = True
        if self._strong_obj is None:
            self._strong_obj = self.obj()

    def commit(self, dict_, keys):
        """Commit attributes.

        This is used by a partial-attribute load operation to mark committed
        those attributes which were refreshed from the database.

        Attributes marked as "expired" can potentially remain "expired" after
        this step if a value was not populated in state.dict.

        """
        class_manager = self.manager
        for key in keys:
            if key in dict_ and key in class_manager.mutable_attributes:
                class_manager[key].impl.commit_to_state(self, dict_, self.committed_state)
            else:
                self.committed_state.pop(key, None)

        self.expired = False
        # unexpire attributes which have loaded
        for key in self.expired_attributes.intersection(keys):
            if key in dict_:
                self.expired_attributes.remove(key)
                self.callables.pop(key, None)

    def commit_all(self, dict_, instance_dict=None):
        """commit all attributes unconditionally.

        This is used after a flush() or a full load/refresh
        to remove all pending state from the instance.

         - all attributes are marked as "committed"
         - the "strong dirty reference" is removed
         - the "modified" flag is set to False
         - any "expired" markers/callables are removed.

        Attributes marked as "expired" can potentially remain "expired" after this step
        if a value was not populated in state.dict.

        """
        
        self.committed_state = {}
        self.pending = {}
        
        # unexpire attributes which have loaded
        if self.expired_attributes:
            for key in self.expired_attributes.intersection(dict_):
                self.callables.pop(key, None)
            self.expired_attributes.difference_update(dict_)

        for key in self.manager.mutable_attributes:
            if key in dict_:
                self.manager[key].impl.commit_to_state(self, dict_, self.committed_state)

        if instance_dict and self.modified:
            instance_dict._modified.discard(self)

        self.modified = self.expired = False
        self._strong_obj = None

class MutableAttrInstanceState(InstanceState):
    def __init__(self, obj, manager):
        self.mutable_dict = {}
        InstanceState.__init__(self, obj, manager)
        
    def _get_modified(self, dict_=None):
        if self.__dict__.get('modified', False):
            return True
        else:
            if dict_ is None:
                dict_ = self.dict
            for key in self.manager.mutable_attributes:
                if self.manager[key].impl.check_mutable_modified(self, dict_):
                    return True
            else:
                return False
    
    def _set_modified(self, value):
        self.__dict__['modified'] = value
        
    modified = property(_get_modified, _set_modified)
    
    @property
    def unmodified(self):
        """a set of keys which have no uncommitted changes"""

        dict_ = self.dict
        return set(
            key for key in self.manager.iterkeys()
            if (key not in self.committed_state or
                (key in self.manager.mutable_attributes and
                 not self.manager[key].impl.check_mutable_modified(self, dict_))))

    def _is_really_none(self):
        """do a check modified/resurrect.
        
        This would be called in the extremely rare
        race condition that the weakref returned None but
        the cleanup handler had not yet established the 
        __resurrect callable as its replacement.
        
        """
        if self.modified:
            self.obj = self.__resurrect
            return self.obj()
        else:
            return None

    def reset(self, key, dict_):
        self.mutable_dict.pop(key, None)
        InstanceState.reset(self, key, dict_)
    
    def _cleanup(self, ref):
        """weakref callback.
        
        This method may be called by an asynchronous
        gc.
        
        If the state shows pending changes, the weakref
        is replaced by the __resurrect callable which will
        re-establish an object reference on next access,
        else removes this InstanceState from the owning
        identity map, if any.
        
        """
        if self._get_modified(self.mutable_dict):
            self.obj = self.__resurrect
        else:
            instance_dict = self._instance_dict()
            if instance_dict:
                instance_dict.remove(self)
            self.dispose()
            
    def __resurrect(self):
        """A substitute for the obj() weakref function which resurrects."""
        
        # store strong ref'ed version of the object; will revert
        # to weakref when changes are persisted
        
        obj = self.manager.new_instance(state=self)
        self.obj = weakref.ref(obj, self._cleanup)
        self._strong_obj = obj
        obj.__dict__.update(self.mutable_dict)

        # re-establishes identity attributes from the key
        self.manager.events.run('on_resurrect', self, obj)
        
        # TODO: don't really think we should run this here.
        # resurrect is only meant to preserve the minimal state needed to
        # do an UPDATE, not to produce a fully usable object
        self._run_on_load(obj)
        
        return obj

class PendingCollection(object):
    """A writable placeholder for an unloaded collection.

    Stores items appended to and removed from a collection that has not yet
    been loaded. When the collection is loaded, the changes stored in
    PendingCollection are applied to it to produce the final result.

    """
    def __init__(self):
        self.deleted_items = util.IdentitySet()
        self.added_items = util.OrderedIdentitySet()

    def append(self, value):
        if value in self.deleted_items:
            self.deleted_items.remove(value)
        self.added_items.add(value)

    def remove(self, value):
        if value in self.added_items:
            self.added_items.remove(value)
        self.deleted_items.add(value)

