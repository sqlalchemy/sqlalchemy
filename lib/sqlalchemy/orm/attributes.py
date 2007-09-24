# attributes.py - manages object attributes
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import weakref, threading
import UserDict
from sqlalchemy import util
from sqlalchemy.orm import util as orm_util, interfaces, collections
from sqlalchemy.orm.mapper import class_mapper
from sqlalchemy import logging, exceptions


PASSIVE_NORESULT = object()
ATTR_WAS_SET = object()
NO_VALUE = object()

class InstrumentedAttribute(interfaces.PropComparator):
    """public-facing instrumented attribute."""
    
    def __init__(self, impl, comparator=None):
        """Construct an InstrumentedAttribute.
        comparator
          a sql.Comparator to which class-level compare/math events will be sent
        """
        
        self.impl = impl
        self.comparator = comparator

    def __set__(self, obj, value):
        self.impl.set(obj._state, value, None)

    def __delete__(self, obj):
        self.impl.delete(obj._state)

    def __get__(self, obj, owner):
        if obj is None:
            return self
        return self.impl.get(obj._state)

    def get_history(self, obj, **kwargs):
        return self.impl.get_history(obj._state, **kwargs)
        
    def clause_element(self):
        return self.comparator.clause_element()

    def expression_element(self):
        return self.comparator.expression_element()

    def operate(self, op, *other, **kwargs):
        return op(self.comparator, *other, **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        return op(other, self.comparator, **kwargs)

    def hasparent(self, instance, optimistic=False):
        return self.impl.hasparent(instance._state, optimistic=optimistic)

    property = property(lambda s: class_mapper(s.impl.class_).get_property(s.impl.key),
                        doc="the MapperProperty object associated with this attribute")
        
class AttributeImpl(object):
    """internal implementation for instrumented attributes."""
    
    def __init__(self, class_, manager, key, callable_, trackparent=False, extension=None, compare_function=None, mutable_scalars=False, **kwargs):
        """Construct an AttributeImpl.
        
            class_
              the class to be instrumented.
                
            manager
              AttributeManager managing this class
              
            key
              string name of the attribute
              
            callable_
              optional function which generates a callable based on a parent 
              instance, which produces the "default" values for a scalar or 
              collection attribute when it's first accessed, if not present already.
              
            trackparent
              if True, attempt to track if an instance has a parent attached to it 
              via this attribute
              
            extension
              an AttributeExtension object which will receive 
              set/delete/append/remove/etc. events 
              
            compare_function
              a function that compares two values which are normally assignable to this 
              attribute
              
            mutable_scalars
              if True, the values which are normally assignable to this attribute can mutate, 
              and need to be compared against a copy of their original contents in order to 
              detect changes on the parent instance
              
              
        """
        
        self.class_ = class_
        self.manager = manager
        self.key = key
        self.callable_ = callable_
        self.trackparent = trackparent
        self.mutable_scalars = mutable_scalars
        self.copy = None
        if compare_function is None:
            self.is_equal = lambda x,y: x == y
        else:
            self.is_equal = compare_function
        self.extensions = util.to_list(extension or [])
        
    def commit_to_state(self, state, value=NO_VALUE):
        """commit the object's current state to its 'committed' state."""
        
        if value is NO_VALUE:
            if self.key in state.dict:
                value = state.dict[self.key]
        if value is not NO_VALUE:
            state.committed_state[self.key] = self.copy(value)

    def hasparent(self, state, optimistic=False):
        """Return the boolean value of a `hasparent` flag attached to the given item.

        The `optimistic` flag determines what the default return value
        should be if no `hasparent` flag can be located.

        As this function is used to determine if an instance is an
        *orphan*, instances that were loaded from storage should be
        assumed to not be orphans, until a True/False value for this
        flag is set.

        An instance attribute that is loaded by a callable function
        will also not have a `hasparent` flag.
        """

        return state.parents.get(id(self), optimistic)

    def sethasparent(self, state, value):
        """Set a boolean flag on the given item corresponding to
        whether or not it is attached to a parent object via the
        attribute represented by this ``InstrumentedAttribute``.
        """

        state.parents[id(self)] = value

    def get_history(self, state, passive=False):
        current = self.get(state, passive=passive)
        if current is PASSIVE_NORESULT:
            return None
        return AttributeHistory(self, state, current, passive=passive)
        
    def set_callable(self, state, callable_, clear=False):
        """Set a callable function for this attribute on the given object.

        This callable will be executed when the attribute is next
        accessed, and is assumed to construct part of the instances
        previously stored state. When its value or values are loaded,
        they will be established as part of the instance's *committed
        state*.  While *trackparent* information will be assembled for
        these instances, attribute-level event handlers will not be
        fired.

        The callable overrides the class level callable set in the
        ``InstrumentedAttribute` constructor.
        """

        if clear:
            self.clear(state)
            
        if callable_ is None:
            self.initialize(state)
        else:
            state.callables[self] = callable_

    def _get_callable(self, state):
        if self in state.callables:
            return state.callables[self]
        elif self.callable_ is not None:
            return self.callable_(state.obj())
        else:
            return None

    def reset(self, state):
        """Remove any per-instance callable functions corresponding to
        this ``InstrumentedAttribute``'s attribute from the given
        object, and remove this ``InstrumentedAttribute``'s attribute
        from the given object's dictionary.
        """

        try:
            del state.callables[self]
        except KeyError:
            pass
        self.clear(state)

    def clear(self, state):
        """Remove this ``InstrumentedAttribute``'s attribute from the given object's dictionary.

        Subsequent calls to ``getattr(obj, key)`` will raise an
        ``AttributeError`` by default.
        """

        try:
            del state.dict[self.key]
        except KeyError:
            pass

    def check_mutable_modified(self, state):
        return False

    def initialize(self, state):
        """Initialize this attribute on the given object instance with an empty value."""

        state.dict[self.key] = None
        return None

    def get(self, state, passive=False):
        """Retrieve a value from the given object.

        If a callable is assembled on this object's attribute, and
        passive is False, the callable will be executed and the
        resulting value will be set as the new value for this attribute.
        """

        try:
            return state.dict[self.key]
        except KeyError:
            # if an instance-wide "trigger" was set, call that
            # and start again
            if state.trigger:
                state.call_trigger()
                return self.get(state, passive=passive)

            callable_ = self._get_callable(state)
            if callable_ is not None:
                if passive:
                    return PASSIVE_NORESULT
                value = callable_()
                if value is not ATTR_WAS_SET:
                    return self.set_committed_value(state, value)
                else:
                    return state.dict[self.key]
            else:
                # Return a new, empty value
                return self.initialize(state)

    def append(self, state, value, initiator):
        self.set(state, value, initiator)

    def remove(self, state, value, initiator):
        self.set(state, None, initiator)

    def set(self, state, value, initiator):
        raise NotImplementedError()

    def set_committed_value(self, state, value):
        """set an attribute value on the given instance and 'commit' it.
        
        this indicates that the given value is the "persisted" value,
        and history will be logged only if a newly set value is not
        equal to this value.
        
        this is typically used by deferred/lazy attribute loaders
        to set object attributes after the initial load.
        """

        if state.committed_state is not None:
            self.commit_to_state(state, value)
        # remove per-instance callable, if any
        state.callables.pop(self, None)
        state.dict[self.key] = value
        return value

    def set_raw_value(self, state, value):
        state.dict[self.key] = value
        return value

    def fire_append_event(self, state, value, initiator):
        state.modified = True
        if self.trackparent and value is not None:
            self.sethasparent(value._state, True)
        obj = state.obj()
        for ext in self.extensions:
            ext.append(obj, value, initiator or self)

    def fire_remove_event(self, state, value, initiator):
        state.modified = True
        if self.trackparent and value is not None:
            self.sethasparent(value._state, False)
        obj = state.obj()
        for ext in self.extensions:
            ext.remove(obj, value, initiator or self)

    def fire_replace_event(self, state, value, previous, initiator):
        state.modified = True
        if self.trackparent:
            if value is not None:
                self.sethasparent(value._state, True)
            if previous is not None:
                self.sethasparent(previous._state, False)
        obj = state.obj()
        for ext in self.extensions:
            ext.set(obj, value, previous, initiator or self)


        
class ScalarAttributeImpl(AttributeImpl):
    """represents a scalar-holding InstrumentedAttribute."""
    
    def __init__(self, class_, manager, key, callable_, trackparent=False, extension=None, copy_function=None, compare_function=None, mutable_scalars=False, **kwargs):
        super(ScalarAttributeImpl, self).__init__(class_, manager, key,
          callable_, trackparent=trackparent, extension=extension,
          compare_function=compare_function, **kwargs)
        self.mutable_scalars = mutable_scalars

        if copy_function is None:
            copy_function = self.__copy
        self.copy = copy_function

    def __copy(self, item):
        # scalar values are assumed to be immutable unless a copy function
        # is passed
        return item

    def delete(self, state):
        old = self.get(state)
        del state.dict[self.key]
        self.fire_remove_event(state, old, self)

    def check_mutable_modified(self, state):
        if self.mutable_scalars:
            h = self.get_history(state, passive=True)
            if h is not None and h.is_modified():
                state.modified = True
                return True
            else:
                return False
        else:
            return False

    def set(self, state, value, initiator):
        """Set a value on the given object.

        `initiator` is the ``InstrumentedAttribute`` that initiated the
        ``set()` operation and is used to control the depth of a circular
        setter operation.
        """

        if initiator is self:
            return

        # if an instance-wide "trigger" was set, call that
        if state.trigger:
            state.call_trigger()

        old = self.get(state)
        state.dict[self.key] = value
        self.fire_replace_event(state, value, old, initiator)

    type = property(lambda self: self.property.columns[0].type)

        
class CollectionAttributeImpl(AttributeImpl):
    """A collection-holding attribute that instruments changes in membership.

    InstrumentedCollectionAttribute holds an arbitrary, user-specified
    container object (defaulting to a list) and brokers access to the
    CollectionAdapter, a "view" onto that object that presents consistent
    bag semantics to the orm layer independent of the user data implementation.
    """
    
    def __init__(self, class_, manager, key, callable_, typecallable=None, trackparent=False, extension=None, copy_function=None, compare_function=None, **kwargs):
        super(CollectionAttributeImpl, self).__init__(class_, manager,
          key, callable_, trackparent=trackparent, extension=extension,
          compare_function=compare_function, **kwargs)

        if copy_function is None:
            copy_function = self.__copy
        self.copy = copy_function

        if typecallable is None:
            typecallable = list
        self.collection_factory = \
          collections._prepare_instrumentation(typecallable)
        self.collection_interface = \
          util.duck_type_collection(self.collection_factory())

    def __copy(self, item):
        return [y for y in list(collections.collection_adapter(item))]

    def delete(self, state):
        if self.key not in state.dict:
            return

        state.modified = True

        collection = self.get_collection(state)
        collection.clear_with_event()
        del state.dict[self.key]

    def initialize(self, state):
        """Initialize this attribute on the given object instance with an empty collection."""

        _, user_data = self._build_collection(state)
        state.dict[self.key] = user_data
        return user_data

    def append(self, state, value, initiator):
        if initiator is self:
            return
        collection = self.get_collection(state)
        collection.append_with_event(value, initiator)

    def remove(self, state, value, initiator):
        if initiator is self:
            return
        collection = self.get_collection(state)
        collection.remove_with_event(value, initiator)

    def set(self, state, value, initiator):
        """Set a value on the given object.

        `initiator` is the ``InstrumentedAttribute`` that initiated the
        ``set()` operation and is used to control the depth of a circular
        setter operation.
        """

        if initiator is self:
            return

        setting_type = util.duck_type_collection(value)

        if value is None or setting_type != self.collection_interface:
            raise exceptions.ArgumentError(
                "Incompatible collection type on assignment: %s is not %s-like" %
                (type(value).__name__, self.collection_interface.__name__))

        if hasattr(value, '_sa_adapter'):
            value = list(getattr(value, '_sa_adapter'))
        elif setting_type == dict:
            value = value.values()

        # if an instance-wide "trigger" was set, call that
        if state.trigger:
            state.call_trigger()

        old = self.get(state)
        old_collection = self.get_collection(state, old)

        new_collection, user_data = self._build_collection(state)
        self._load_collection(state, value or [], emit_events=True,
                              collection=new_collection)

        state.dict[self.key] = user_data
        state.modified = True

        # mark all the old elements as detached from the parent
        if old_collection:
            old_collection.clear_with_event()
            old_collection.unlink(old)

    def set_committed_value(self, state, value):
        """Set an attribute value on the given instance and 'commit' it."""

        collection, user_data = self._build_collection(state)
        self._load_collection(state, value or [], emit_events=False,
                              collection=collection)
        value = user_data

        if state.committed_state is not None:
            self.commit_to_state(state, value)
        # remove per-instance callable, if any
        state.callables.pop(self, None)
        state.dict[self.key] = value
        return value

    def _build_collection(self, state):
        user_data = self.collection_factory()
        collection = collections.CollectionAdapter(self, state, user_data)
        return collection, user_data

    def _load_collection(self, state, values, emit_events=True, collection=None):
        collection = collection or self.get_collection(state)
        if values is None:
            return
        elif emit_events:
            for item in values:
                collection.append_with_event(item)
        else:
            for item in values:
                collection.append_without_event(item)
            
    def get_collection(self, state, user_data=None):
        if user_data is None:
            user_data = self.get(state)
        try:
            return getattr(user_data, '_sa_adapter')
        except AttributeError:
            collections.CollectionAdapter(self, state, user_data)
            return getattr(user_data, '_sa_adapter')


class GenericBackrefExtension(interfaces.AttributeExtension):
    """An extension which synchronizes a two-way relationship.

    A typical two-way relationship is a parent object containing a
    list of child objects, where each child object references the
    parent.  The other are two objects which contain scalar references
    to each other.
    """

    def __init__(self, key):
        self.key = key

    def set(self, obj, child, oldchild, initiator):
        if oldchild is child:
            return
        if oldchild is not None:
            getattr(oldchild.__class__, self.key).impl.remove(oldchild._state, obj, initiator)
        if child is not None:
            getattr(child.__class__, self.key).impl.append(child._state, obj, initiator)

    def append(self, obj, child, initiator):
        getattr(child.__class__, self.key).impl.append(child._state, obj, initiator)

    def remove(self, obj, child, initiator):
        getattr(child.__class__, self.key).impl.remove(child._state, obj, initiator)

class InstanceState(object):
    """tracks state information at the instance level."""

    __slots__ = 'class_', 'obj', 'dict', 'committed_state', 'modified', 'trigger', 'callables', 'parents', 'instance_dict', '_strong_obj'
    
    def __init__(self, obj):
        self.class_ = obj.__class__
        self.obj = weakref.ref(obj, self.__cleanup)
        self.dict = obj.__dict__
        self.committed_state = None
        self.modified = False
        self.trigger = None
        self.callables = {}
        self.parents = {}
        self.instance_dict = None
        
    def __cleanup(self, ref):
        if self.instance_dict is None or self.instance_dict() is None:
            return
            
        instance_dict = self.instance_dict()
        
        # the mutexing here is based on the assumption that gc.collect()
        # may be firing off cleanup handlers in a different thread than that
        # which is normally operating upon the instance dict.
        instance_dict._mutex.acquire()
        try:
            # if instance_dict de-refed us, or it called our
            # _resurrect, return
            if self.instance_dict is None or self.instance_dict() is None or self.obj() is not None:
                return
                
            self.__resurrect(instance_dict)
        finally:
            instance_dict._mutex.release()
    
    def _check_resurrect(self, instance_dict):
        instance_dict._mutex.acquire()
        try:
            return self.obj() or self.__resurrect(instance_dict)
        finally:
            instance_dict._mutex.release()
        
    def __resurrect(self, instance_dict):
        if self.modified or self.class_._sa_attribute_manager._is_modified(self):
            # store strong ref'ed version of the object; will revert
            # to weakref when changes are persisted
            obj = self.class_._sa_attribute_manager.new_instance(self.class_, state=self)
            self.obj = weakref.ref(obj, self.__cleanup)
            self._strong_obj = obj
            obj.__dict__.update(self.dict)
            self.dict = obj.__dict__
            return obj
        else:
            del instance_dict[self.dict['_instance_key']]
            return None
            
    def __getstate__(self):
        return {'committed_state':self.committed_state, 'parents':self.parents, 'modified':self.modified, 'instance':self.obj()}
    
    def __setstate__(self, state):
        self.committed_state = state['committed_state']
        self.parents = state['parents']
        self.modified = state['modified']
        self.obj = weakref.ref(state['instance'])
        self.class_ = self.obj().__class__
        self.dict = self.obj().__dict__
        self.callables = {}
        self.trigger = None
        
    def call_trigger(self):
        trig = self.trigger
        self.trigger = None
        trig()
        
    def commit(self, manager, obj):
        self.committed_state = {}
        self.modified = False
        for attr in manager.managed_attributes(obj.__class__):
            attr.impl.commit_to_state(self)
        # remove strong ref
        self._strong_obj = None
        
    def rollback(self, manager, obj):
        if not self.committed_state:
            manager._clear(obj)
        else:
            for attr in manager.managed_attributes(obj.__class__):
                if attr.impl.key in self.committed_state:
                    if not hasattr(attr.impl, 'get_collection'):
                        obj.__dict__[attr.impl.key] = self.committed_state[attr.impl.key]
                    else:
                        collection = attr.impl.get_collection(self)
                        collection.clear_without_event()
                        for item in self.committed_state[attr.impl.key]:
                            collection.append_without_event(item)
                else:
                    if attr.impl.key in self.dict:
                        del self.dict[attr.impl.key]

class InstanceDict(UserDict.UserDict):
    """similar to WeakValueDictionary, but wired towards 'state' objects."""
    
    def __init__(self, *args, **kw):
        self._wr = weakref.ref(self)
        # RLock because the mutex is used by a cleanup 
        # handler, which can be called at any time (including within an already mutexed block)
        self._mutex = threading.RLock()
        UserDict.UserDict.__init__(self, *args, **kw)
        
    def __getitem__(self, key):
        state = self.data[key]
        o = state.obj() or state._check_resurrect(self)
        if o is None:
            raise KeyError, key
        return o
                
    def __contains__(self, key):
        try:
            state = self.data[key]
            o = state.obj() or state._check_resurrect(self)
        except KeyError:
            return False
        return o is not None

    def has_key(self, key):
        return key in self

    def __repr__(self):
        return "<InstanceDict at %s>" % id(self)

    def __setitem__(self, key, value):
        if key in self.data:
            self._mutex.acquire()
            try:
                if key in self.data:
                    self.data[key].instance_dict = None
            finally:
                self._mutex.release()
        self.data[key] = value._state
        value._state.instance_dict = self._wr

    def __delitem__(self, key):
        state = self.data[key]
        state.instance_dict = None
        del self.data[key]
        
    def get(self, key, default=None):
        try:
            state = self.data[key]
        except KeyError:
            return default
        else:
            o = state.obj()
            if o is None:
                # This should only happen
                return default
            else:
                return o

    def items(self):
        L = []
        for key, state in self.data.items():
            o = state.obj()
            if o is not None:
                L.append((key, o))
        return L

    def iteritems(self):
        for state in self.data.itervalues():
            value = state.obj()
            if value is not None:
                yield value._instance_key, value

    def iterkeys(self):
        return self.data.iterkeys()

    def __iter__(self):
        return self.data.iterkeys()
    
    def __len__(self):
        return len(self.values())
        
    def itervalues(self):
        for state in self.data.itervalues():
            obj = state.obj()
            if obj is not None:
                yield obj

    def values(self):
        L = []
        for state in self.data.values():
            o = state.obj()
            if o is not None:
                L.append(o)
        return L

    def popitem(self):
        raise NotImplementedError()

    def pop(self, key, *args):
        raise NotImplementedError()

    def setdefault(self, key, default=None):
        raise NotImplementedError()

    def update(self, dict=None, **kwargs):
        raise NotImplementedError()

    def copy(self):
        raise NotImplementedError()

    
    
class AttributeHistory(object):
    """Calculate the *history* of a particular attribute on a
    particular instance.
    """

    def __init__(self, attr, state, current, passive=False):
        self.attr = attr

        # get the "original" value.  if a lazy load was fired when we got
        # the 'current' value, this "original" was also populated just
        # now as well (therefore we have to get it second)
        if state.committed_state:
            original = state.committed_state.get(attr.key, None)
        else:
            original = None

        if hasattr(attr, 'get_collection'):
            self._current = current
            s = util.Set(original or [])
            self._added_items = []
            self._unchanged_items = []
            self._deleted_items = []
            if current:
                collection = attr.get_collection(state, current)
                for a in collection:
                    if a in s:
                        self._unchanged_items.append(a)
                    else:
                        self._added_items.append(a)
            for a in s:
                if a not in self._unchanged_items:
                    self._deleted_items.append(a)
        else:
            self._current = [current]
            if attr.is_equal(current, original) is True:
                self._unchanged_items = [current]
                self._added_items = []
                self._deleted_items = []
            else:
                self._added_items = [current]
                if original is not None:
                    self._deleted_items = [original]
                else:
                    self._deleted_items = []
                self._unchanged_items = []

    def __iter__(self):
        return iter(self._current)

    def is_modified(self):
        return len(self._deleted_items) > 0 or len(self._added_items) > 0

    def added_items(self):
        return self._added_items

    def unchanged_items(self):
        return self._unchanged_items

    def deleted_items(self):
        return self._deleted_items

class AttributeManager(object):
    """Allow the instrumentation of object attributes."""

    def __init__(self):
        # will cache attributes, indexed by class objects
        self._inherited_attribute_cache = weakref.WeakKeyDictionary()
        self._noninherited_attribute_cache = weakref.WeakKeyDictionary()

    def clear_attribute_cache(self):
        self._attribute_cache.clear()

    def rollback(self, *obj):
        """Retrieve the committed history for each object in the given
        list, and rolls back the attributes each instance to their
        original value.
        """

        for o in obj:
            o._state.rollback(self, o)

    def _clear(self, obj):
        for attr in self.managed_attributes(obj.__class__):
            try:
                del obj.__dict__[attr.impl.key]
            except KeyError:
                pass
    
    def commit(self, *obj):
        """Establish the "committed state" for each object in the given list."""

        for o in obj:
            o._state.commit(self, o)

    def managed_attributes(self, class_):
        """Return a list of all ``InstrumentedAttribute`` objects
        associated with the given class.
        """

        try:
            # TODO: move this collection onto the class itself?
            return self._inherited_attribute_cache[class_]
        except KeyError:
            if not isinstance(class_, type):
                raise TypeError(repr(class_) + " is not a type")
            inherited = [v for v in [getattr(class_, key, None) for key in dir(class_)] if isinstance(v, InstrumentedAttribute)]
            self._inherited_attribute_cache[class_] = inherited
            return inherited

    def noninherited_managed_attributes(self, class_):
        try:
            # TODO: move this collection onto the class itself?
            return self._noninherited_attribute_cache[class_]
        except KeyError:
            if not isinstance(class_, type):
                raise TypeError(repr(class_) + " is not a type")
            noninherited = [v for v in [getattr(class_, key, None) for key in list(class_.__dict__)] if isinstance(v, InstrumentedAttribute)]
            self._noninherited_attribute_cache[class_] = noninherited
            return noninherited

    def is_modified(self, obj):
        return self._is_modified(obj._state)
    
    def _is_modified(self, state):
        if state.modified:
            return True
        else:
            for attr in self.managed_attributes(state.class_):
                if attr.impl.check_mutable_modified(state):
                    return True
            else:
                return False
        
    def get_history(self, obj, key, **kwargs):
        """Return a new ``AttributeHistory`` object for the given
        attribute on the given object.
        """

        return getattr(obj.__class__, key).impl.get_history(obj._state, **kwargs)

    def get_as_list(self, obj, key, passive=False):
        """Return an attribute of the given name from the given object.

        If the attribute is a scalar, return it as a single-item list,
        otherwise return a collection based attribute.

        If the attribute's value is to be produced by an unexecuted
        callable, the callable will only be executed if the given
        `passive` flag is False.
        """
        attr = getattr(obj.__class__, key).impl
        state = obj._state
        x = attr.get(state, passive=passive)
        if x is PASSIVE_NORESULT:
            return []
        elif hasattr(attr, 'get_collection'):
            return list(attr.get_collection(state, x))
        elif isinstance(x, list):
            return x
        else:
            return [x]

    def trigger_history(self, obj, callable):
        """Clear all managed object attributes and places the given
        `callable` as an attribute-wide *trigger*, which will execute
        upon the next attribute access, after which the trigger is
        removed.
        """

        s = obj._state
        self._clear(obj)
        s.committed_state = None
        s.trigger = callable

    def untrigger_history(self, obj):
        """Remove a trigger function set by trigger_history.

        Does not restore the previous state of the object.
        """

        obj._state.trigger = None

    def has_trigger(self, obj):
        """Return True if the given object has a trigger function set
        by ``trigger_history()``.
        """

        return obj._state.trigger is not None

    def reset_instance_attribute(self, obj, key):
        """Remove any per-instance callable functions corresponding to
        given attribute `key` from the given object, and remove this
        attribute from the given object's dictionary.
        """

        attr = getattr(obj.__class__, key)
        attr.impl.reset(obj._state)

    def is_class_managed(self, class_, key):
        """Return True if the given `key` correponds to an
        instrumented property on the given class.
        """
        return hasattr(class_, key) and isinstance(getattr(class_, key), InstrumentedAttribute)

    def has_parent(self, class_, obj, key, optimistic=False):
        return getattr(class_, key).impl.hasparent(obj._state, optimistic=optimistic)

    def init_instance_attribute(self, obj, key, callable_=None, clear=False):
        """Initialize an attribute on an instance to either a blank
        value, cancelling out any class- or instance-level callables
        that were present, or if a `callable` is supplied set the
        callable to be invoked when the attribute is next accessed.
        """

        getattr(obj.__class__, key).impl.set_callable(obj._state, callable_, clear=clear)

    def _create_prop(self, class_, key, uselist, callable_, typecallable, **kwargs):
        """Create a scalar property object, defaulting to
        ``InstrumentedAttribute``, which will communicate change
        events back to this ``AttributeManager``.
        """
        
        if kwargs.pop('dynamic', False):
            from sqlalchemy.orm import dynamic
            return dynamic.DynamicAttributeImpl(class_, self, key, typecallable, **kwargs)
        elif uselist:
            return CollectionAttributeImpl(class_, self, key,
                                                   callable_,
                                                   typecallable,
                                                   **kwargs)
        else:
            return ScalarAttributeImpl(class_, self, key, callable_,
                                               **kwargs)

    def manage(self, obj):
        if not hasattr(obj, '_state'):
            obj._state = InstanceState(obj)
            
    def new_instance(self, class_, state=None):
        """create a new instance of class_ without its __init__() method being called."""
        
        s = class_.__new__(class_)
        if state:
            s._state = state
        else:
            s._state = InstanceState(s)
        return s
        
    def register_class(self, class_, extra_init=None, on_exception=None):
        """decorate the constructor of the given class to establish attribute
        management on new instances."""

        # do a sweep first, this also helps some attribute extensions
        # (like associationproxy) become aware of themselves at the 
        # class level
        self.unregister_class(class_)
        
        oldinit = None
        doinit = False
        class_._sa_attribute_manager = self

        def init(instance, *args, **kwargs):
            instance._state = InstanceState(instance)

            if extra_init:
                extra_init(class_, oldinit, instance, args, kwargs)

            if doinit:
                try:
                    oldinit(instance, *args, **kwargs)
                except:
                    if on_exception:
                        on_exception(class_, oldinit, instance, args, kwargs)
                    raise
        
        # override oldinit
        oldinit = class_.__init__
        if oldinit is None or not hasattr(oldinit, '_oldinit'):
            init._oldinit = oldinit
            class_.__init__ = init
        # if oldinit is already one of our 'init' methods, replace it
        elif hasattr(oldinit, '_oldinit'):
            init._oldinit = oldinit._oldinit
            class_.__init = init
            oldinit = oldinit._oldinit
            
        if oldinit is not None:
            doinit = oldinit is not object.__init__
            try:
                init.__name__ = oldinit.__name__
                init.__doc__ = oldinit.__doc__
            except:
                # cant set __name__ in py 2.3 !
                pass
            
    def unregister_class(self, class_):
        if hasattr(class_, '__init__') and hasattr(class_.__init__, '_oldinit'):
            if class_.__init__._oldinit is not None:
                class_.__init__ = class_.__init__._oldinit
            else:
                delattr(class_, '__init__')
                
        for attr in self.noninherited_managed_attributes(class_):
            delattr(class_, attr.impl.key)
        self._inherited_attribute_cache.pop(class_,None)
        self._noninherited_attribute_cache.pop(class_,None)
        
    def register_attribute(self, class_, key, uselist, callable_=None, **kwargs):
        """Register an attribute at the class level to be instrumented
        for all instances of the class.
        """

        # firt invalidate the cache for the given class
        # (will be reconstituted as needed, while getting managed attributes)
        self._inherited_attribute_cache.pop(class_, None)
        self._noninherited_attribute_cache.pop(class_, None)

        typecallable = kwargs.pop('typecallable', None)
        if isinstance(typecallable, InstrumentedAttribute):
            typecallable = None
        comparator = kwargs.pop('comparator', None)
        setattr(class_, key, InstrumentedAttribute(self._create_prop(class_, key, uselist, callable_,
                                           typecallable=typecallable, **kwargs), comparator=comparator))

    def set_raw_value(self, instance, key, value):
        getattr(instance.__class__, key).impl.set_raw_value(instance._state, value)

    def set_committed_value(self, instance, key, value):
        getattr(instance.__class__, key).impl.set_committed_value(instance._state, value)

    def init_collection(self, instance, key):
        """Initialize a collection attribute and return the collection adapter."""
        attr = getattr(instance.__class__, key).impl
        state = instance._state
        user_data = attr.initialize(state)
        return attr.get_collection(state, user_data)
