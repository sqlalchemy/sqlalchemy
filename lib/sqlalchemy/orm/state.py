# orm/state.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines instrumentation of instances.

This module is usually not directly visible to user applications, but
defines a large part of the ORM's interactivity.

"""

from sqlalchemy.util import EMPTY_SET
import weakref
from sqlalchemy import util

from sqlalchemy.orm import exc as orm_exc, attributes, interfaces,\
        util as orm_util
from sqlalchemy.orm.attributes import PASSIVE_OFF, PASSIVE_NO_RESULT, \
    PASSIVE_NO_FETCH, NEVER_SET, ATTR_WAS_SET, NO_VALUE

mapperlib = util.importlater("sqlalchemy.orm", "mapperlib")

import sys

class InstanceState(object):
    """tracks state information at the instance level."""

    session_id = None
    key = None
    runid = None
    load_options = EMPTY_SET
    load_path = ()
    insert_order = None
    mutable_dict = None
    _strong_obj = None
    modified = False
    expired = False
    deleted = False

    def __init__(self, obj, manager):
        self.class_ = obj.__class__
        self.manager = manager
        self.obj = weakref.ref(obj, self._cleanup)
        self.callables = {}
        self.committed_state = {}

    @util.memoized_property
    def parents(self):
        return {}

    @util.memoized_property
    def pending(self):
        return {}

    @property
    def has_identity(self):
        return bool(self.key)

    def detach(self):
        self.session_id = None

    def dispose(self):
        self.detach()
        del self.obj

    def _cleanup(self, ref):
        instance_dict = self._instance_dict()
        if instance_dict:
            instance_dict.discard(self)

        self.callables = {}
        self.session_id = None
        del self.obj

    def obj(self):
        return None

    @property
    def dict(self):
        o = self.obj()
        if o is not None:
            return attributes.instance_dict(o)
        else:
            return {}

    def initialize_instance(*mixed, **kwargs):
        self, instance, args = mixed[0], mixed[1], mixed[2:]
        manager = self.manager

        manager.dispatch.init(self, args, kwargs)

        #if manager.mutable_attributes:
        #    assert self.__class__ is MutableAttrInstanceState

        try:
            return manager.original_init(*mixed[1:], **kwargs)
        except:
            manager.dispatch.init_failure(self, args, kwargs)
            raise

    def get_history(self, key, passive):
        return self.manager[key].impl.get_history(self, self.dict, passive)

    def get_impl(self, key):
        return self.manager[key].impl

    def get_pending(self, key):
        if key not in self.pending:
            self.pending[key] = PendingCollection()
        return self.pending[key]

    def value_as_iterable(self, dict_, key, passive=PASSIVE_OFF):
        """Return a list of tuples (state, obj) for the given
        key.

        returns an empty list if the value is None/empty/PASSIVE_NO_RESULT
        """

        impl = self.manager[key].impl
        x = impl.get(self, dict_, passive=passive)
        if x is PASSIVE_NO_RESULT or x is None:
            return []
        elif hasattr(impl, 'get_collection'):
            return [
                (attributes.instance_state(o), o) for o in 
                impl.get_collection(self, dict_, x, passive=passive)
            ]
        else:
            return [(attributes.instance_state(x), x)]

    def __getstate__(self):
        d = {'instance':self.obj()}
        d.update(
            (k, self.__dict__[k]) for k in (
                'committed_state', 'pending', 'modified', 'expired', 
                'callables', 'key', 'parents', 'load_options', 'mutable_dict',
                'class_',
            ) if k in self.__dict__ 
        )
        if self.load_path:
            d['load_path'] = interfaces.serialize_path(self.load_path)

        self.manager.dispatch.pickle(self, d)

        return d

    def __setstate__(self, state):
        from sqlalchemy.orm import instrumentation
        inst = state['instance']
        if inst is not None:
            self.obj = weakref.ref(inst, self._cleanup)
            self.class_ = inst.__class__
        else:
            # None being possible here generally new as of 0.7.4
            # due to storage of state in "parents".  "class_"
            # also new.
            self.obj = None
            self.class_ = state['class_']
        self.manager = manager = instrumentation.manager_of_class(self.class_)
        if manager is None:
            raise orm_exc.UnmappedInstanceError(
                        inst,
                        "Cannot deserialize object of type %r - no mapper() has"
                        " been configured for this class within the current Python process!" %
                        self.class_)
        elif manager.is_mapped and not manager.mapper.configured:
            mapperlib.configure_mappers()

        self.committed_state = state.get('committed_state', {})
        self.pending = state.get('pending', {})
        self.parents = state.get('parents', {})
        self.modified = state.get('modified', False)
        self.expired = state.get('expired', False)
        self.callables = state.get('callables', {})

        if self.modified:
            self._strong_obj = inst

        self.__dict__.update([
            (k, state[k]) for k in (
                'key', 'load_options', 'mutable_dict'
            ) if k in state 
        ])

        if 'load_path' in state:
            self.load_path = interfaces.deserialize_path(state['load_path'])

        # setup _sa_instance_state ahead of time so that 
        # unpickle events can access the object normally.
        # see [ticket:2362]
        manager.setup_instance(inst, self)
        manager.dispatch.unpickle(self, state)

    def initialize(self, key):
        """Set this attribute to an empty value or collection, 
           based on the AttributeImpl in use."""

        self.manager.get_impl(key).initialize(self, self.dict)

    def reset(self, dict_, key):
        """Remove the given attribute and any 
           callables associated with it."""

        dict_.pop(key, None)
        self.callables.pop(key, None)

    def expire_attribute_pre_commit(self, dict_, key):
        """a fast expire that can be called by column loaders during a load.

        The additional bookkeeping is finished up in commit_all().

        This method is actually called a lot with joined-table
        loading, when the second table isn't present in the result.

        """
        dict_.pop(key, None)
        self.callables[key] = self

    def set_callable(self, dict_, key, callable_):
        """Remove the given attribute and set the given callable
           as a loader."""

        dict_.pop(key, None)
        self.callables[key] = callable_

    def expire(self, dict_, modified_set):
        self.expired = True
        if self.modified:
            modified_set.discard(self)

        self.modified = False

        self.committed_state.clear()

        self.__dict__.pop('pending', None)
        self.__dict__.pop('mutable_dict', None)

        # clear out 'parents' collection.  not
        # entirely clear how we can best determine
        # which to remove, or not.
        self.__dict__.pop('parents', None)

        for key in self.manager:
            impl = self.manager[key].impl
            if impl.accepts_scalar_loader and \
                (impl.expire_missing or key in dict_):
                self.callables[key] = self
            dict_.pop(key, None)

        self.manager.dispatch.expire(self, None)

    def expire_attributes(self, dict_, attribute_names):
        pending = self.__dict__.get('pending', None)
        mutable_dict = self.mutable_dict

        for key in attribute_names:
            impl = self.manager[key].impl
            if impl.accepts_scalar_loader:
                self.callables[key] = self
            dict_.pop(key, None)

            self.committed_state.pop(key, None)
            if mutable_dict:
                mutable_dict.pop(key, None)
            if pending:
                pending.pop(key, None)

        self.manager.dispatch.expire(self, attribute_names)

    def __call__(self, passive):
        """__call__ allows the InstanceState to act as a deferred
        callable for loading expired attributes, which is also
        serializable (picklable).

        """

        if passive is PASSIVE_NO_FETCH:
            return PASSIVE_NO_RESULT

        toload = self.expired_attributes.\
                        intersection(self.unmodified)

        self.manager.deferred_scalar_loader(self, toload)

        # if the loader failed, or this 
        # instance state didn't have an identity,
        # the attributes still might be in the callables
        # dict.  ensure they are removed.
        for k in toload.intersection(self.callables):
            del self.callables[k]

        return ATTR_WAS_SET

    @property
    def unmodified(self):
        """Return the set of keys which have no uncommitted changes"""

        return set(self.manager).difference(self.committed_state)

    def unmodified_intersection(self, keys):
        """Return self.unmodified.intersection(keys)."""

        return set(keys).intersection(self.manager).\
                    difference(self.committed_state)


    @property
    def unloaded(self):
        """Return the set of keys which do not have a loaded value.

        This includes expired attributes and any other attribute that
        was never populated or modified.

        """
        return set(self.manager).\
                    difference(self.committed_state).\
                    difference(self.dict)

    @property
    def expired_attributes(self):
        """Return the set of keys which are 'expired' to be loaded by
           the manager's deferred scalar loader, assuming no pending 
           changes.

           see also the ``unmodified`` collection which is intersected
           against this set when a refresh operation occurs.

        """
        return set([k for k, v in self.callables.items() if v is self])

    def _instance_dict(self):
        return None

    def _is_really_none(self):
        return self.obj()

    def modified_event(self, dict_, attr, previous, collection=False):
        if attr.key not in self.committed_state:
            if collection:
                if previous is NEVER_SET:
                    if attr.key in dict_:
                        previous = dict_[attr.key]

                if previous not in (None, NO_VALUE, NEVER_SET):
                    previous = attr.copy(previous)

            self.committed_state[attr.key] = previous

        # the "or not self.modified" is defensive at 
        # this point.  The assertion below is expected
        # to be True:
        # assert self._strong_obj is None or self.modified

        if self._strong_obj is None or not self.modified:
            instance_dict = self._instance_dict()
            if instance_dict:
                instance_dict._modified.add(self)

            self._strong_obj = self.obj()
            if self._strong_obj is None:
                raise orm_exc.ObjectDereferencedError(
                        "Can't emit change event for attribute '%s' - "
                        "parent object of type %s has been garbage "
                        "collected." 
                        % (
                            self.manager[attr.key], 
                            orm_util.state_class_str(self)
                        ))
            self.modified = True

    def commit(self, dict_, keys):
        """Commit attributes.

        This is used by a partial-attribute load operation to mark committed
        those attributes which were refreshed from the database.

        Attributes marked as "expired" can potentially remain "expired" after
        this step if a value was not populated in state.dict.

        """
        class_manager = self.manager
        if class_manager.mutable_attributes:
            for key in keys:
                if key in dict_ and key in class_manager.mutable_attributes:
                    self.committed_state[key] = self.manager[key].impl.copy(dict_[key])
                else:
                    self.committed_state.pop(key, None)
        else:
            for key in keys:
                self.committed_state.pop(key, None)

        self.expired = False

        for key in set(self.callables).\
                            intersection(keys).\
                            intersection(dict_):
            del self.callables[key]

    def commit_all(self, dict_, instance_dict=None):
        """commit all attributes unconditionally.

        This is used after a flush() or a full load/refresh
        to remove all pending state from the instance.

         - all attributes are marked as "committed"
         - the "strong dirty reference" is removed
         - the "modified" flag is set to False
         - any "expired" markers/callables for attributes loaded are removed.

        Attributes marked as "expired" can potentially remain "expired" after this step
        if a value was not populated in state.dict.

        """

        self.committed_state.clear()
        self.__dict__.pop('pending', None)

        callables = self.callables
        for key in list(callables):
            if key in dict_ and callables[key] is self:
                del callables[key]

        for key in self.manager.mutable_attributes:
            if key in dict_:
                self.committed_state[key] = self.manager[key].impl.copy(dict_[key])

        if instance_dict and self.modified:
            instance_dict._modified.discard(self)

        self.modified = self.expired = False
        self._strong_obj = None

class MutableAttrInstanceState(InstanceState):
    """InstanceState implementation for objects that reference 'mutable' 
    attributes.

    Has a more involved "cleanup" handler that checks mutable attributes
    for changes upon dereference, resurrecting if needed.

    """

    @util.memoized_property
    def mutable_dict(self):
        return {}

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

        return set([
            key for key in self.manager
            if (key not in self.committed_state or
                (key in self.manager.mutable_attributes and
                 not self.manager[key].impl.check_mutable_modified(self, dict_)))])

    def unmodified_intersection(self, keys):
        """Return self.unmodified.intersection(keys)."""

        dict_ = self.dict

        return set([
            key for key in keys
            if (key not in self.committed_state or
                (key in self.manager.mutable_attributes and
                 not self.manager[key].impl.check_mutable_modified(self, dict_)))])


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

    def reset(self, dict_, key):
        self.mutable_dict.pop(key, None)
        InstanceState.reset(self, dict_, key)

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
                instance_dict.discard(self)
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
        self.manager.dispatch.resurrect(self)

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
        else:
            self.added_items.add(value)

    def remove(self, value):
        if value in self.added_items:
            self.added_items.remove(value)
        else:
            self.deleted_items.add(value)

