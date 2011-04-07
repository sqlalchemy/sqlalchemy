# orm/state.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.util import EMPTY_SET
import weakref
from sqlalchemy import util
from sqlalchemy.orm.attributes import PASSIVE_NO_RESULT, PASSIVE_OFF, \
                        NEVER_SET, NO_VALUE, manager_of_class, ATTR_WAS_SET
from sqlalchemy.orm import attributes, exc as orm_exc, interfaces, \
                        util as orm_util

import sys
attributes.state = sys.modules['sqlalchemy.orm.state']

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

    @util.memoized_property
    def committed_state(self):
        return {}

    @util.memoized_property
    def parents(self):
        return {}

    @util.memoized_property
    def pending(self):
        return {}

    @util.memoized_property
    def callables(self):
        return {}

    @property
    def has_identity(self):
        return bool(self.key)

    def detach(self):
        if self.session_id:
            try:
                del self.session_id
            except AttributeError:
                pass

    def dispose(self):
        self.detach()
        del self.obj

    def _cleanup(self, ref):
        instance_dict = self._instance_dict()
        if instance_dict:
            try:
                instance_dict.remove(self)
            except AssertionError:
                pass
        # remove possible cycles
        self.__dict__.pop('callables', None)
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
        PASSIVE_NO_RESULT.
        """

        impl = self.get_impl(key)
        dict_ = self.dict
        x = impl.get(self, dict_, passive=passive)
        if x is PASSIVE_NO_RESULT:
            return None
        elif hasattr(impl, 'get_collection'):
            return impl.get_collection(self, dict_, x, passive=passive)
        else:
            return [x]

    def _run_on_load(self, instance):
        self.manager.events.run('on_load', instance)

    def __getstate__(self):
        d = {'instance':self.obj()}

        d.update(
            (k, self.__dict__[k]) for k in (
                'committed_state', 'pending', 'parents', 'modified', 'expired', 
                'callables', 'key', 'load_options', 'mutable_dict'
            ) if k in self.__dict__ 
        )
        if self.load_path:
            d['load_path'] = interfaces.serialize_path(self.load_path)
        return d

    def __setstate__(self, state):
        self.obj = weakref.ref(state['instance'], self._cleanup)
        self.class_ = state['instance'].__class__
        self.manager = manager = manager_of_class(self.class_)
        if manager is None:
            raise orm_exc.UnmappedInstanceError(
                        state['instance'],
                        "Cannot deserialize object of type %r - no mapper() has"
                        " been configured for this class within the current Python process!" %
                        self.class_)
        elif manager.is_mapped and not manager.mapper.compiled:
            manager.mapper.compile()

        self.committed_state = state.get('committed_state', {})
        self.pending = state.get('pending', {})
        self.parents = state.get('parents', {})
        self.modified = state.get('modified', False)
        self.expired = state.get('expired', False)
        self.callables = state.get('callables', {})

        if self.modified:
            self._strong_obj = state['instance']

        self.__dict__.update([
            (k, state[k]) for k in (
                'key', 'load_options', 'mutable_dict'
            ) if k in state 
        ])

        if 'load_path' in state:
            self.load_path = interfaces.deserialize_path(state['load_path'])

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

    def expire_attributes(self, dict_, attribute_names, instance_dict=None):
        """Expire all or a group of attributes.

        If all attributes are expired, the "expired" flag is set to True.

        """
        # we would like to assert that 'self.key is not None' here, 
        # but there are many cases where the mapper will expire
        # a newly persisted instance within the flush, before the
        # key is assigned, and even cases where the attribute refresh
        # occurs fully, within the flush(), before this key is assigned.
        # the key is assigned late within the flush() to assist in
        # "key switch" bookkeeping scenarios.

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

        to_clear = (
            self.__dict__.get('pending', None),
            self.__dict__.get('committed_state', None),
            self.mutable_dict
        )

        for key in attribute_names:
            impl = self.manager[key].impl
            if impl.accepts_scalar_loader and \
                (not filter_deferred or impl.expire_missing or key in dict_):
                self.callables[key] = self
            dict_.pop(key, None)

            for d in to_clear:
                if d is not None:
                    d.pop(key, None)

    def __call__(self, **kw):
        """__call__ allows the InstanceState to act as a deferred
        callable for loading expired attributes, which is also
        serializable (picklable).

        """

        if kw.get('passive') is attributes.PASSIVE_NO_FETCH:
            return attributes.PASSIVE_NO_RESULT

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

    def modified_event(self, dict_, attr, should_copy, previous, passive=PASSIVE_OFF):
        if attr.key not in self.committed_state:
            if previous is NEVER_SET:
                if passive:
                    if attr.key in dict_:
                        previous = dict_[attr.key]
                else:
                    previous = attr.get(self, dict_)

            if should_copy and previous not in (None, NO_VALUE, NEVER_SET):
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
                util.warn(
                        "Can't emit change event for attribute '%s.%s' "
                        "- parent object of type %s has been garbage "
                        "collected." 
                        % (
                            self.class_.__name__, 
                            attr.key, 
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
        for key in keys:
            if key in dict_ and key in class_manager.mutable_attributes:
                self.committed_state[key] = self.manager[key].impl.copy(dict_[key])
            else:
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

        self.__dict__.pop('committed_state', None)
        self.__dict__.pop('pending', None)

        if 'callables' in self.__dict__:
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
                try:
                    instance_dict.remove(self)
                except AssertionError:
                    pass
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
        else:
            self.added_items.add(value)

    def remove(self, value):
        if value in self.added_items:
            self.added_items.remove(value)
        else:
            self.deleted_items.add(value)

