# attributes.py - manages object attributes
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Defines SQLAlchemy's system of class instrumentation..

This module is usually not directly visible to user applications, but
defines a large part of the ORM's interactivity.

SQLA's instrumentation system is completely customizable, in which
case an understanding of the general mechanics of this module is helpful.
An example of full customization is in /examples/custom_attributes.

"""

import operator
from operator import attrgetter, itemgetter
import types
import weakref

from sqlalchemy import util
from sqlalchemy.orm import interfaces, collections, exc
import sqlalchemy.exceptions as sa_exc

# lazy imports
_entity_info = None
identity_equal = None
state = None

PASSIVE_NORESULT = util.symbol('PASSIVE_NORESULT')
ATTR_WAS_SET = util.symbol('ATTR_WAS_SET')
NO_VALUE = util.symbol('NO_VALUE')
NEVER_SET = util.symbol('NEVER_SET')

# "passive" get settings
# TODO: the True/False values need to be factored out
# of the rest of ORM code
# don't fire off any callables, and don't initialize the attribute to
# an empty value
PASSIVE_NO_INITIALIZE = True #util.symbol('PASSIVE_NO_INITIALIZE')

# don't fire off any callables, but if no callables present
# then initialize to an empty value/collection
# this is used by backrefs.
PASSIVE_NO_CALLABLES = util.symbol('PASSIVE_NO_CALLABLES')

# fire callables/initialize as needed
PASSIVE_OFF = False #util.symbol('PASSIVE_OFF')

INSTRUMENTATION_MANAGER = '__sa_instrumentation_manager__'
"""Attribute, elects custom instrumentation when present on a mapped class.

Allows a class to specify a slightly or wildly different technique for
tracking changes made to mapped attributes and collections.

Only one instrumentation implementation is allowed in a given object
inheritance hierarchy.

The value of this attribute must be a callable and will be passed a class
object.  The callable must return one of:

  - An instance of an interfaces.InstrumentationManager or subclass
  - An object implementing all or some of InstrumentationManager (TODO)
  - A dictionary of callables, implementing all or some of the above (TODO)
  - An instance of a ClassManager or subclass

interfaces.InstrumentationManager is public API and will remain stable
between releases.  ClassManager is not public and no guarantees are made
about stability.  Caveat emptor.

This attribute is consulted by the default SQLAlchemy instrumentation
resolution code.  If custom finders are installed in the global
instrumentation_finders list, they may or may not choose to honor this
attribute.

"""

instrumentation_finders = []
"""An extensible sequence of instrumentation implementation finding callables.

Finders callables will be passed a class object.  If None is returned, the
next finder in the sequence is consulted.  Otherwise the return must be an
instrumentation factory that follows the same guidelines as
INSTRUMENTATION_MANAGER.

By default, the only finder is find_native_user_instrumentation_hook, which
searches for INSTRUMENTATION_MANAGER.  If all finders return None, standard
ClassManager instrumentation is used.

"""

class QueryableAttribute(interfaces.PropComparator):

    def __init__(self, key, impl=None, comparator=None, parententity=None):
        """Construct an InstrumentedAttribute.

          comparator
            a sql.Comparator to which class-level compare/math events will be sent
        """
        self.key = key
        self.impl = impl
        self.comparator = comparator
        self.parententity = parententity

    def get_history(self, instance, **kwargs):
        return self.impl.get_history(instance_state(instance), instance_dict(instance), **kwargs)

    def __selectable__(self):
        # TODO: conditionally attach this method based on clause_element ?
        return self

    def __clause_element__(self):
        return self.comparator.__clause_element__()

    def label(self, name):
        return self.__clause_element__().label(name)

    def operate(self, op, *other, **kwargs):
        return op(self.comparator, *other, **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        return op(other, self.comparator, **kwargs)

    def hasparent(self, state, optimistic=False):
        return self.impl.hasparent(state, optimistic=optimistic)
    
    def __getattr__(self, key):
        try:
            return getattr(self.comparator, key)
        except AttributeError:
            raise AttributeError('Neither %r object nor %r object has an attribute %r' % (
                    type(self).__name__, 
                    type(self.comparator).__name__, 
                    key)
            )
        
    def __str__(self):
        return repr(self.parententity) + "." + self.property.key

    @property
    def property(self):
        return self.comparator.property


class InstrumentedAttribute(QueryableAttribute):
    """Public-facing descriptor, placed in the mapped class dictionary."""

    def __set__(self, instance, value):
        self.impl.set(instance_state(instance), instance_dict(instance), value, None)

    def __delete__(self, instance):
        self.impl.delete(instance_state(instance), instance_dict(instance))

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.impl.get(instance_state(instance), instance_dict(instance))

class _ProxyImpl(object):
    accepts_scalar_loader = False
    dont_expire_missing = False
    
    def __init__(self, key):
        self.key = key

def proxied_attribute_factory(descriptor):
    """Create an InstrumentedAttribute / user descriptor hybrid.

    Returns a new InstrumentedAttribute type that delegates descriptor
    behavior and getattr() to the given descriptor.
    """

    class Proxy(InstrumentedAttribute):
        """A combination of InsturmentedAttribute and a regular descriptor."""

        def __init__(self, key, descriptor, comparator, parententity):
            self.key = key
            # maintain ProxiedAttribute.user_prop compatability.
            self.descriptor = self.user_prop = descriptor
            self._comparator = comparator
            self._parententity = parententity
            self.impl = _ProxyImpl(key)

        @util.memoized_property
        def comparator(self):
            if util.callable(self._comparator):
                self._comparator = self._comparator()
            return self._comparator

        def __get__(self, instance, owner):
            """Delegate __get__ to the original descriptor."""
            if instance is None:
                descriptor.__get__(instance, owner)
                return self
            return descriptor.__get__(instance, owner)

        def __set__(self, instance, value):
            """Delegate __set__ to the original descriptor."""
            return descriptor.__set__(instance, value)

        def __delete__(self, instance):
            """Delegate __delete__ to the original descriptor."""
            return descriptor.__delete__(instance)

        def __getattr__(self, attribute):
            """Delegate __getattr__ to the original descriptor and/or comparator."""
            
            try:
                return getattr(descriptor, attribute)
            except AttributeError:
                try:
                    return getattr(self._comparator, attribute)
                except AttributeError:
                    raise AttributeError('Neither %r object nor %r object has an attribute %r' % (
                            type(descriptor).__name__, 
                            type(self._comparator).__name__, 
                            attribute)
                    )

    Proxy.__name__ = type(descriptor).__name__ + 'Proxy'

    util.monkeypatch_proxied_specials(Proxy, type(descriptor),
                                      name='descriptor',
                                      from_instance=descriptor)
    return Proxy

class AttributeImpl(object):
    """internal implementation for instrumented attributes."""

    def __init__(self, class_, key,
                    callable_, trackparent=False, extension=None,
                    compare_function=None, active_history=False, parent_token=None, 
                    dont_expire_missing=False,
                    **kwargs):
        """Construct an AttributeImpl.

        \class_
          associated class
          
        key
          string name of the attribute

        \callable_
          optional function which generates a callable based on a parent
          instance, which produces the "default" values for a scalar or
          collection attribute when it's first accessed, if not present
          already.

        trackparent
          if True, attempt to track if an instance has a parent attached
          to it via this attribute.

        extension
          a single or list of AttributeExtension object(s) which will
          receive set/delete/append/remove/etc. events.

        compare_function
          a function that compares two values which are normally
          assignable to this attribute.

        active_history
          indicates that get_history() should always return the "old" value,
          even if it means executing a lazy callable upon attribute change.

        parent_token
          Usually references the MapperProperty, used as a key for
          the hasparent() function to identify an "owning" attribute.
          Allows multiple AttributeImpls to all match a single 
          owner attribute.
          
        dont_expire_missing
          if True, don't add an "expiry" callable to this attribute
          during state.expire_attributes(None), if no value is present 
          for this key.
          
        """
        self.class_ = class_
        self.key = key
        self.callable_ = callable_
        self.trackparent = trackparent
        self.parent_token = parent_token or self
        if compare_function is None:
            self.is_equal = operator.eq
        else:
            self.is_equal = compare_function
        self.extensions = util.to_list(extension or [])
        for e in self.extensions:
            if e.active_history:
                active_history = True
                break
        self.active_history = active_history
        self.dont_expire_missing = dont_expire_missing
        
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
        return state.parents.get(id(self.parent_token), optimistic)

    def sethasparent(self, state, value):
        """Set a boolean flag on the given item corresponding to
        whether or not it is attached to a parent object via the
        attribute represented by this ``InstrumentedAttribute``.

        """
        state.parents[id(self.parent_token)] = value

    def set_callable(self, state, callable_):
        """Set a callable function for this attribute on the given object.

        This callable will be executed when the attribute is next
        accessed, and is assumed to construct part of the instances
        previously stored state. When its value or values are loaded,
        they will be established as part of the instance's *committed
        state*.  While *trackparent* information will be assembled for
        these instances, attribute-level event handlers will not be
        fired.

        The callable overrides the class level callable set in the
        ``InstrumentedAttribute`` constructor.

        """
        if callable_ is None:
            self.initialize(state)
        else:
            state.callables[self.key] = callable_

    def get_history(self, state, dict_, passive=PASSIVE_OFF):
        raise NotImplementedError()

    def _get_callable(self, state):
        if self.key in state.callables:
            return state.callables[self.key]
        elif self.callable_ is not None:
            return self.callable_(state)
        else:
            return None

    def initialize(self, state, dict_):
        """Initialize this attribute on the given object instance with an empty value."""

        dict_[self.key] = None
        return None

    def get(self, state, dict_, passive=PASSIVE_OFF):
        """Retrieve a value from the given object.

        If a callable is assembled on this object's attribute, and
        passive is False, the callable will be executed and the
        resulting value will be set as the new value for this attribute.
        """

        try:
            return dict_[self.key]
        except KeyError:
            # if no history, check for lazy callables, etc.
            if state.committed_state.get(self.key, NEVER_SET) is NEVER_SET:
                if passive is PASSIVE_NO_INITIALIZE:
                    return PASSIVE_NORESULT
                    
                callable_ = self._get_callable(state)
                if callable_ is not None:
                    if passive is not PASSIVE_OFF:
                        return PASSIVE_NORESULT
                    value = callable_()
                    if value is not ATTR_WAS_SET:
                        return self.set_committed_value(state, dict_, value)
                    else:
                        if self.key not in dict_:
                            return self.get(state, dict_, passive=passive)
                        return dict_[self.key]

            # Return a new, empty value
            return self.initialize(state, dict_)

    def append(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        self.set(state, dict_, value, initiator, passive=passive)

    def remove(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        self.set(state, dict_, None, initiator, passive=passive)

    def set(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        raise NotImplementedError()

    def get_committed_value(self, state, dict_, passive=PASSIVE_OFF):
        """return the unchanged value of this attribute"""

        if self.key in state.committed_state:
            if state.committed_state[self.key] is NO_VALUE:
                return None
            else:
                return state.committed_state.get(self.key)
        else:
            return self.get(state, dict_, passive=passive)

    def set_committed_value(self, state, dict_, value):
        """set an attribute value on the given instance and 'commit' it."""

        state.commit(dict_, [self.key])

        state.callables.pop(self.key, None)
        state.dict[self.key] = value

        return value

class ScalarAttributeImpl(AttributeImpl):
    """represents a scalar value-holding InstrumentedAttribute."""

    accepts_scalar_loader = True
    uses_objects = False

    def delete(self, state, dict_):

        # TODO: catch key errors, convert to attributeerror?
        if self.active_history:
            old = self.get(state, dict_)
        else:
            old = dict_.get(self.key, NO_VALUE)

        state.modified_event(dict_, self, False, old)

        if self.extensions:
            self.fire_remove_event(state, dict_, old, None)
        del dict_[self.key]

    def get_history(self, state, dict_, passive=PASSIVE_OFF):
        return History.from_attribute(
            self, state, dict_.get(self.key, NO_VALUE))

    def set(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        if initiator is self:
            return

        if self.active_history:
            old = self.get(state, dict_)
        else:
            old = dict_.get(self.key, NO_VALUE)

        state.modified_event(dict_, self, False, old)

        if self.extensions:
            value = self.fire_replace_event(state, dict_, value, old, initiator)
        dict_[self.key] = value

    def fire_replace_event(self, state, dict_, value, previous, initiator):
        for ext in self.extensions:
            value = ext.set(state, value, previous, initiator or self)
        return value

    def fire_remove_event(self, state, dict_, value, initiator):
        for ext in self.extensions:
            ext.remove(state, value, initiator or self)

    @property
    def type(self):
        self.property.columns[0].type


class MutableScalarAttributeImpl(ScalarAttributeImpl):
    """represents a scalar value-holding InstrumentedAttribute, which can detect
    changes within the value itself.
    """

    uses_objects = False

    def __init__(self, class_, key, callable_,
                    class_manager, copy_function=None,
                    compare_function=None, **kwargs):
        super(ScalarAttributeImpl, self).__init__(class_, key, callable_,
                                compare_function=compare_function, **kwargs)
        class_manager.mutable_attributes.add(key)
        if copy_function is None:
            raise sa_exc.ArgumentError("MutableScalarAttributeImpl requires a copy function")
        self.copy = copy_function

    def get_history(self, state, dict_, passive=PASSIVE_OFF):
        if not dict_:
            v = state.committed_state.get(self.key, NO_VALUE)
        else:
            v = dict_.get(self.key, NO_VALUE)
            
        return History.from_attribute(
            self, state, v)

    def commit_to_state(self, state, dict_, dest):
        dest[self.key] = self.copy(dict_[self.key])

    def check_mutable_modified(self, state, dict_):
        (added, unchanged, deleted) = self.get_history(state, dict_, passive=PASSIVE_NO_INITIALIZE)
        return bool(added or deleted)

    def get(self, state, dict_, passive=PASSIVE_OFF):
        if self.key not in state.mutable_dict:
            ret = ScalarAttributeImpl.get(self, state, dict_, passive=passive)
            if ret is not PASSIVE_NORESULT:
                state.mutable_dict[self.key] = ret
            return ret
        else:
            return state.mutable_dict[self.key]

    def delete(self, state, dict_):
        ScalarAttributeImpl.delete(self, state, dict_)
        state.mutable_dict.pop(self.key)

    def set(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        if initiator is self:
            return

        state.modified_event(dict_, self, True, NEVER_SET)
        
        if self.extensions:
            old = self.get(state, dict_)
            value = self.fire_replace_event(state, dict_, value, old, initiator)
            dict_[self.key] = value
        else:
            dict_[self.key] = value
        state.mutable_dict[self.key] = value


class ScalarObjectAttributeImpl(ScalarAttributeImpl):
    """represents a scalar-holding InstrumentedAttribute, where the target object is also instrumented.

    Adds events to delete/set operations.
    """

    accepts_scalar_loader = False
    uses_objects = True

    def __init__(self, class_, key, callable_, 
                    trackparent=False, extension=None, copy_function=None,
                    compare_function=None, **kwargs):
        super(ScalarObjectAttributeImpl, self).__init__(class_, key,
          callable_, trackparent=trackparent, extension=extension,
          compare_function=compare_function, **kwargs)
        if compare_function is None:
            self.is_equal = identity_equal

    def delete(self, state, dict_):
        old = self.get(state, dict_)
        self.fire_remove_event(state, dict_, old, self)
        del dict_[self.key]

    def get_history(self, state, dict_, passive=PASSIVE_OFF):
        if self.key in dict_:
            return History.from_attribute(self, state, dict_[self.key])
        else:
            current = self.get(state, dict_, passive=passive)
            if current is PASSIVE_NORESULT:
                return HISTORY_BLANK
            else:
                return History.from_attribute(self, state, current)

    def set(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        """Set a value on the given InstanceState.

        `initiator` is the ``InstrumentedAttribute`` that initiated the
        ``set()`` operation and is used to control the depth of a circular
        setter operation.

        """
        if initiator is self:
            return
        
        if self.active_history:
            old = self.get(state, dict_)
        else:
            # this would be the "laziest" approach,
            # however it breaks currently expected backref
            # behavior
            #old = dict_.get(self.key, None)
            # instead, use the "passive" setting, which
            # is only going to be PASSIVE_NOCALLABLES if it
            # came from a backref
            old = self.get(state, dict_, passive=passive)
            if old is PASSIVE_NORESULT:
                old = None
             
        value = self.fire_replace_event(state, dict_, value, old, initiator)
        dict_[self.key] = value

    def fire_remove_event(self, state, dict_, value, initiator):
        state.modified_event(dict_, self, False, value)

        if self.trackparent and value is not None:
            self.sethasparent(instance_state(value), False)

        for ext in self.extensions:
            ext.remove(state, value, initiator or self)

    def fire_replace_event(self, state, dict_, value, previous, initiator):
        state.modified_event(dict_, self, False, previous)

        if self.trackparent:
            if previous is not value and previous is not None:
                self.sethasparent(instance_state(previous), False)

        for ext in self.extensions:
            value = ext.set(state, value, previous, initiator or self)

        if self.trackparent:
            if value is not None:
                self.sethasparent(instance_state(value), True)

        return value


class CollectionAttributeImpl(AttributeImpl):
    """A collection-holding attribute that instruments changes in membership.

    Only handles collections of instrumented objects.

    InstrumentedCollectionAttribute holds an arbitrary, user-specified
    container object (defaulting to a list) and brokers access to the
    CollectionAdapter, a "view" onto that object that presents consistent
    bag semantics to the orm layer independent of the user data implementation.

    """
    accepts_scalar_loader = False
    uses_objects = True

    def __init__(self, class_, key, callable_, 
                    typecallable=None, trackparent=False, extension=None,
                    copy_function=None, compare_function=None, **kwargs):
        super(CollectionAttributeImpl, self).__init__(class_, key, callable_, trackparent=trackparent,
          extension=extension, compare_function=compare_function, **kwargs)

        if copy_function is None:
            copy_function = self.__copy
        self.copy = copy_function

        self.collection_factory = typecallable
        # may be removed in 0.5:
        self.collection_interface = \
          util.duck_type_collection(self.collection_factory())

    def __copy(self, item):
        return [y for y in list(collections.collection_adapter(item))]

    def get_history(self, state, dict_, passive=PASSIVE_OFF):
        current = self.get(state, dict_, passive=passive)
        if current is PASSIVE_NORESULT:
            return HISTORY_BLANK
        else:
            return History.from_attribute(self, state, current)

    def fire_append_event(self, state, dict_, value, initiator):
        state.modified_event(dict_, self, True, NEVER_SET, passive=PASSIVE_NO_INITIALIZE)

        for ext in self.extensions:
            value = ext.append(state, value, initiator or self)

        if self.trackparent and value is not None:
            self.sethasparent(instance_state(value), True)

        return value

    def fire_pre_remove_event(self, state, dict_, initiator):
        state.modified_event(dict_, self, True, NEVER_SET, passive=PASSIVE_NO_INITIALIZE)

    def fire_remove_event(self, state, dict_, value, initiator):
        state.modified_event(dict_, self, True, NEVER_SET, passive=PASSIVE_NO_INITIALIZE)

        if self.trackparent and value is not None:
            self.sethasparent(instance_state(value), False)

        for ext in self.extensions:
            ext.remove(state, value, initiator or self)

    def delete(self, state, dict_):
        if self.key not in dict_:
            return

        state.modified_event(dict_, self, True, NEVER_SET)

        collection = self.get_collection(state, state.dict)
        collection.clear_with_event()
        # TODO: catch key errors, convert to attributeerror?
        del dict_[self.key]

    def initialize(self, state, dict_):
        """Initialize this attribute with an empty collection."""

        _, user_data = self._initialize_collection(state)
        dict_[self.key] = user_data
        return user_data

    def _initialize_collection(self, state):
        return state.manager.initialize_collection(
            self.key, state, self.collection_factory)

    def append(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        if initiator is self:
            return

        collection = self.get_collection(state, dict_, passive=passive)
        if collection is PASSIVE_NORESULT:
            value = self.fire_append_event(state, dict_, value, initiator)
            assert self.key not in dict_, "Collection was loaded during event handling."
            state.get_pending(self.key).append(value)
        else:
            collection.append_with_event(value, initiator)

    def remove(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        if initiator is self:
            return

        collection = self.get_collection(state, state.dict, passive=passive)
        if collection is PASSIVE_NORESULT:
            self.fire_remove_event(state, dict_, value, initiator)
            assert self.key not in dict_, "Collection was loaded during event handling."
            state.get_pending(self.key).remove(value)
        else:
            collection.remove_with_event(value, initiator)

    def set(self, state, dict_, value, initiator, passive=PASSIVE_OFF):
        """Set a value on the given object.

        `initiator` is the ``InstrumentedAttribute`` that initiated the
        ``set()`` operation and is used to control the depth of a circular
        setter operation.
        """

        if initiator is self:
            return

        self._set_iterable(
            state, dict_, value,
            lambda adapter, i: adapter.adapt_like_to_iterable(i))

    def _set_iterable(self, state, dict_, iterable, adapter=None):
        """Set a collection value from an iterable of state-bearers.

        ``adapter`` is an optional callable invoked with a CollectionAdapter
        and the iterable.  Should return an iterable of state-bearing
        instances suitable for appending via a CollectionAdapter.  Can be used
        for, e.g., adapting an incoming dictionary into an iterator of values
        rather than keys.

        """
        # pulling a new collection first so that an adaptation exception does
        # not trigger a lazy load of the old collection.
        new_collection, user_data = self._initialize_collection(state)
        if adapter:
            new_values = list(adapter(new_collection, iterable))
        else:
            new_values = list(iterable)

        old = self.get(state, dict_)

        # ignore re-assignment of the current collection, as happens
        # implicitly with in-place operators (foo.collection |= other)
        if old is iterable:
            return

        state.modified_event(dict_, self, True, old)

        old_collection = self.get_collection(state, dict_, old)

        dict_[self.key] = user_data

        collections.bulk_replace(new_values, old_collection, new_collection)
        old_collection.unlink(old)


    def set_committed_value(self, state, dict_, value):
        """Set an attribute value on the given instance and 'commit' it."""

        collection, user_data = self._initialize_collection(state)

        if value:
            for item in value:
                collection.append_without_event(item)

        state.callables.pop(self.key, None)
        state.dict[self.key] = user_data

        state.commit(dict_, [self.key])

        if self.key in state.pending:
            
            # pending items exist.  issue a modified event,
            # add/remove new items.
            state.modified_event(dict_, self, True, user_data)

            pending = state.pending.pop(self.key)
            added = pending.added_items
            removed = pending.deleted_items
            for item in added:
                collection.append_without_event(item)
            for item in removed:
                collection.remove_without_event(item)

        return user_data

    def get_collection(self, state, dict_, user_data=None, passive=PASSIVE_OFF):
        """Retrieve the CollectionAdapter associated with the given state.

        Creates a new CollectionAdapter if one does not exist.

        """
        if user_data is None:
            user_data = self.get(state, dict_, passive=passive)
            if user_data is PASSIVE_NORESULT:
                return user_data

        return getattr(user_data, '_sa_adapter')

class GenericBackrefExtension(interfaces.AttributeExtension):
    """An extension which synchronizes a two-way relationship.

    A typical two-way relationship is a parent object containing a list of
    child objects, where each child object references the parent.  The other
    are two objects which contain scalar references to each other.

    """
    
    active_history = False
    
    def __init__(self, key):
        self.key = key

    def set(self, state, child, oldchild, initiator):
        if oldchild is child:
            return child
        if oldchild is not None:
            # With lazy=None, there's no guarantee that the full collection is
            # present when updating via a backref.
            old_state, old_dict = instance_state(oldchild), instance_dict(oldchild)
            impl = old_state.get_impl(self.key)
            try:
                impl.remove(old_state, old_dict, state.obj(), initiator, passive=PASSIVE_NO_CALLABLES)
            except (ValueError, KeyError, IndexError):
                pass
        if child is not None:
            new_state,  new_dict = instance_state(child), instance_dict(child)
            new_state.get_impl(self.key).append(new_state, new_dict, state.obj(), initiator, passive=PASSIVE_NO_CALLABLES)
        return child

    def append(self, state, child, initiator):
        child_state, child_dict = instance_state(child), instance_dict(child)
        child_state.get_impl(self.key).append(child_state, child_dict, state.obj(), initiator, passive=PASSIVE_NO_CALLABLES)
        return child

    def remove(self, state, child, initiator):
        if child is not None:
            child_state, child_dict = instance_state(child), instance_dict(child)
            child_state.get_impl(self.key).remove(child_state, child_dict, state.obj(), initiator, passive=PASSIVE_NO_CALLABLES)


class Events(object):
    def __init__(self):
        self.original_init = object.__init__
        self.on_init = ()
        self.on_init_failure = ()
        self.on_load = ()
        self.on_resurrect = ()

    def run(self, event, *args, **kwargs):
        for fn in getattr(self, event):
            fn(*args, **kwargs)

    def add_listener(self, event, listener):
        # not thread safe... problem?  mb: nope
        bucket = getattr(self, event)
        if bucket == ():
            setattr(self, event, [listener])
        else:
            bucket.append(listener)

    def remove_listener(self, event, listener):
        bucket = getattr(self, event)
        bucket.remove(listener)


class ClassManager(dict):
    """tracks state information at the class level."""

    MANAGER_ATTR = '_sa_class_manager'
    STATE_ATTR = '_sa_instance_state'

    event_registry_factory = Events
    deferred_scalar_loader = None
    
    def __init__(self, class_):
        self.class_ = class_
        self.factory = None  # where we came from, for inheritance bookkeeping
        self.info = {}
        self.mapper = None
        self.new_init = None
        self.mutable_attributes = set()
        self.local_attrs = {}
        self.originals = {}
        for base in class_.__mro__[-2:0:-1]:   # reverse, skipping 1st and last
            if not isinstance(base, type):
                continue
            cls_state = manager_of_class(base)
            if cls_state:
                self.update(cls_state)
        self.events = self.event_registry_factory()
        self.manage()
        self._instrument_init()
    
    def _configure_create_arguments(self, 
                            _source=None, 
                            deferred_scalar_loader=None):
        """Accept extra **kw arguments passed to create_manager_for_cls.
        
        The current contract of ClassManager and other managers is that they
        take a single "cls" argument in their constructor (as per 
        test/orm/instrumentation.py InstrumentationCollisionTest).  This
        is to provide consistency with the current API of "class manager"
        callables and such which may return various ClassManager and 
        ClassManager-like instances.   So create_manager_for_cls sends
        in ClassManager-specific arguments via this method once the 
        non-proxied ClassManager is available.
        
        """
        if _source:
            deferred_scalar_loader = _source.deferred_scalar_loader

        if deferred_scalar_loader:
            self.deferred_scalar_loader = deferred_scalar_loader
    
    def _subclass_manager(self, cls):
        """Create a new ClassManager for a subclass of this ClassManager's class.
        
        This is called automatically when attributes are instrumented so that
        the attributes can be propagated to subclasses against their own
        class-local manager, without the need for mappers etc. to have already
        pre-configured managers for the full class hierarchy.   Mappers
        can post-configure the auto-generated ClassManager when needed.
        
        """
        manager = manager_of_class(cls)
        if manager is None:
            manager = _create_manager_for_cls(cls, _source=self)
        return manager
        
    def _instrument_init(self):
        # TODO: self.class_.__init__ is often the already-instrumented
        # __init__ from an instrumented superclass.  We still need to make 
        # our own wrapper, but it would
        # be nice to wrap the original __init__ and not our existing wrapper
        # of such, since this adds method overhead.
        self.events.original_init = self.class_.__init__
        self.new_init = _generate_init(self.class_, self)
        self.install_member('__init__', self.new_init)
        
    def _uninstrument_init(self):
        if self.new_init:
            self.uninstall_member('__init__')
            self.new_init = None
    
    def _create_instance_state(self, instance):
        global state
        if state is None:
            from sqlalchemy.orm import state
        if self.mutable_attributes:
            return state.MutableAttrInstanceState(instance, self)
        else:
            return state.InstanceState(instance, self)
        
    def manage(self):
        """Mark this instance as the manager for its class."""
        
        setattr(self.class_, self.MANAGER_ATTR, self)

    def dispose(self):
        """Dissasociate this manager from its class."""
        
        delattr(self.class_, self.MANAGER_ATTR)

    def manager_getter(self):
        return attrgetter(self.MANAGER_ATTR)

    def instrument_attribute(self, key, inst, propagated=False):
        if propagated:
            if key in self.local_attrs:
                return  # don't override local attr with inherited attr
        else:
            self.local_attrs[key] = inst
            self.install_descriptor(key, inst)
        self[key] = inst
        for cls in self.class_.__subclasses__():
            if isinstance(cls, types.ClassType):
                continue
            manager = self._subclass_manager(cls)
            manager.instrument_attribute(key, inst, True)

    def post_configure_attribute(self, key):
        pass
        
    def uninstrument_attribute(self, key, propagated=False):
        if key not in self:
            return
        if propagated:
            if key in self.local_attrs:
                return  # don't get rid of local attr
        else:
            del self.local_attrs[key]
            self.uninstall_descriptor(key)
        del self[key]
        if key in self.mutable_attributes:
            self.mutable_attributes.remove(key)
        for cls in self.class_.__subclasses__():
            if isinstance(cls, types.ClassType):
                continue
            manager = self._subclass_manager(cls)
            manager.uninstrument_attribute(key, True)

    def unregister(self):
        """remove all instrumentation established by this ClassManager."""
        
        self._uninstrument_init()

        self.mapper = self.events = None
        self.info.clear()
        
        for key in list(self):
            if key in self.local_attrs:
                self.uninstrument_attribute(key)

    def install_descriptor(self, key, inst):
        if key in (self.STATE_ATTR, self.MANAGER_ATTR):
            raise KeyError("%r: requested attribute name conflicts with "
                           "instrumentation attribute of the same name." % key)
        setattr(self.class_, key, inst)

    def uninstall_descriptor(self, key):
        delattr(self.class_, key)

    def install_member(self, key, implementation):
        if key in (self.STATE_ATTR, self.MANAGER_ATTR):
            raise KeyError("%r: requested attribute name conflicts with "
                           "instrumentation attribute of the same name." % key)
        self.originals.setdefault(key, getattr(self.class_, key, None))
        setattr(self.class_, key, implementation)

    def uninstall_member(self, key):
        original = self.originals.pop(key, None)
        if original is not None:
            setattr(self.class_, key, original)

    def instrument_collection_class(self, key, collection_class):
        return collections.prepare_instrumentation(collection_class)

    def initialize_collection(self, key, state, factory):
        user_data = factory()
        adapter = collections.CollectionAdapter(
            self.get_impl(key), state, user_data)
        return adapter, user_data

    def is_instrumented(self, key, search=False):
        if search:
            return key in self
        else:
            return key in self.local_attrs

    def get_impl(self, key):
        return self[key].impl

    @property
    def attributes(self):
        return self.itervalues()

    ## InstanceState management

    def new_instance(self, state=None):
        instance = self.class_.__new__(self.class_)
        setattr(instance, self.STATE_ATTR, state or self._create_instance_state(instance))
        return instance

    def setup_instance(self, instance, state=None):
        setattr(instance, self.STATE_ATTR, state or self._create_instance_state(instance))
    
    def teardown_instance(self, instance):
        delattr(instance, self.STATE_ATTR)
        
    def _new_state_if_none(self, instance):
        """Install a default InstanceState if none is present.

        A private convenience method used by the __init__ decorator.
        
        """
        if hasattr(instance, self.STATE_ATTR):
            return False
        else:
            state = self._create_instance_state(instance)
            setattr(instance, self.STATE_ATTR, state)
            return state
    
    def state_getter(self):
        """Return a (instance) -> InstanceState callable.

        "state getter" callables should raise either KeyError or
        AttributeError if no InstanceState could be found for the
        instance.
        """

        return attrgetter(self.STATE_ATTR)
    
    def dict_getter(self):
        return attrgetter('__dict__')
        
    def has_state(self, instance):
        return hasattr(instance, self.STATE_ATTR)
        
    def has_parent(self, state, key, optimistic=False):
        """TODO"""
        return self.get_impl(key).hasparent(state, optimistic=optimistic)

    def __nonzero__(self):
        """All ClassManagers are non-zero regardless of attribute state."""
        return True

    def __repr__(self):
        return '<%s of %r at %x>' % (
            self.__class__.__name__, self.class_, id(self))

class _ClassInstrumentationAdapter(ClassManager):
    """Adapts a user-defined InstrumentationManager to a ClassManager."""

    def __init__(self, class_, override, **kw):
        self._adapted = override
        self._get_state = self._adapted.state_getter(class_)
        self._get_dict = self._adapted.dict_getter(class_)
        
        ClassManager.__init__(self, class_, **kw)

    def manage(self):
        self._adapted.manage(self.class_, self)

    def dispose(self):
        self._adapted.dispose(self.class_)

    def manager_getter(self):
        return self._adapted.manager_getter(self.class_)

    def instrument_attribute(self, key, inst, propagated=False):
        ClassManager.instrument_attribute(self, key, inst, propagated)
        if not propagated:
            self._adapted.instrument_attribute(self.class_, key, inst)

    def post_configure_attribute(self, key):
        self._adapted.post_configure_attribute(self.class_, key, self[key])

    def install_descriptor(self, key, inst):
        self._adapted.install_descriptor(self.class_, key, inst)

    def uninstall_descriptor(self, key):
        self._adapted.uninstall_descriptor(self.class_, key)

    def install_member(self, key, implementation):
        self._adapted.install_member(self.class_, key, implementation)

    def uninstall_member(self, key):
        self._adapted.uninstall_member(self.class_, key)

    def instrument_collection_class(self, key, collection_class):
        return self._adapted.instrument_collection_class(
            self.class_, key, collection_class)

    def initialize_collection(self, key, state, factory):
        delegate = getattr(self._adapted, 'initialize_collection', None)
        if delegate:
            return delegate(key, state, factory)
        else:
            return ClassManager.initialize_collection(self, key, state, factory)

    def new_instance(self, state=None):
        instance = self.class_.__new__(self.class_)
        self.setup_instance(instance, state)
        return instance

    def _new_state_if_none(self, instance):
        """Install a default InstanceState if none is present.

        A private convenience method used by the __init__ decorator.
        """
        if self.has_state(instance):
            return False
        else:
            return self.setup_instance(instance)

    def setup_instance(self, instance, state=None):
        self._adapted.initialize_instance_dict(self.class_, instance)
        
        if state is None:
            state = self._create_instance_state(instance)
            
        # the given instance is assumed to have no state
        self._adapted.install_state(self.class_, instance, state)
        return state

    def teardown_instance(self, instance):
        self._adapted.remove_state(self.class_, instance)

    def has_state(self, instance):
        try:
            state = self._get_state(instance)
            return True
        except exc.NO_STATE:
            return False

    def state_getter(self):
        return self._get_state

    def dict_getter(self):
        return self._get_dict

class History(tuple):
    """A 3-tuple of added, unchanged and deleted values.

    Each tuple member is an iterable sequence.

    """

    __slots__ = ()

    added = property(itemgetter(0))
    unchanged = property(itemgetter(1))
    deleted = property(itemgetter(2))

    def __new__(cls, added, unchanged, deleted):
        return tuple.__new__(cls, (added, unchanged, deleted))
    
    def __nonzero__(self):
        return self != HISTORY_BLANK
    
    def sum(self):
        return self.added + self.unchanged + self.deleted
    
    def non_deleted(self):
        return self.added + self.unchanged
    
    def non_added(self):
        return self.unchanged + self.deleted
    
    def has_changes(self):
        return bool(self.added or self.deleted)
        
    def as_state(self):
        return History(
            [c is not None and instance_state(c) or None for c in self.added],
            [c is not None and instance_state(c) or None for c in self.unchanged],
            [c is not None and instance_state(c) or None for c in self.deleted],
        )
    
    @classmethod
    def from_attribute(cls, attribute, state, current):
        original = state.committed_state.get(attribute.key, NEVER_SET)

        if hasattr(attribute, 'get_collection'):
            current = attribute.get_collection(state, state.dict, current)
            if original is NO_VALUE:
                return cls(list(current), (), ())
            elif original is NEVER_SET:
                return cls((), list(current), ())
            else:
                current_set = util.IdentitySet(current)
                original_set = util.IdentitySet(original)

                # ensure duplicates are maintained
                return cls(
                    [x for x in current if x not in original_set],
                    [x for x in current if x in original_set],
                    [x for x in original if x not in current_set]
                )
        else:
            if current is NO_VALUE:
                if original not in [None, NEVER_SET, NO_VALUE]:
                    deleted = [original]
                else:
                    deleted = ()
                return cls((), (), deleted)
            elif original is NO_VALUE:
                return cls([current], (), ())
            elif (original is NEVER_SET or
                  attribute.is_equal(current, original) is True):
                # dont let ClauseElement expressions here trip things up
                return cls((), [current], ())
            else:
                if original is not None:
                    deleted = [original]
                else:
                    deleted = ()
                return cls([current], (), deleted)

HISTORY_BLANK = History(None, None, None)

def _conditional_instance_state(obj):
    if not isinstance(obj, state.InstanceState):
        obj = instance_state(obj)
    return obj
        
def get_history(obj, key, **kwargs):
    """Return a History record for the given object and attribute key.
    
    obj is an instrumented object instance.  An InstanceState
    is accepted directly for backwards compatibility but 
    this usage is deprecated.
    
    """
    return get_state_history(_conditional_instance_state(obj), key, **kwargs)

def get_state_history(state, key, **kwargs):
    return state.get_history(key, **kwargs)

def has_parent(cls, obj, key, optimistic=False):
    """TODO"""
    manager = manager_of_class(cls)
    state = instance_state(obj)
    return manager.has_parent(state, key, optimistic)

def register_class(class_, **kw):
    """Register class instrumentation.
    
    Returns the existing or newly created class manager.
    """

    manager = manager_of_class(class_)
    if manager is None:
        manager = _create_manager_for_cls(class_, **kw)
    return manager
    
def unregister_class(class_):
    """Unregister class instrumentation."""
    
    instrumentation_registry.unregister(class_)

def register_attribute(class_, key, **kw):

    proxy_property = kw.pop('proxy_property', None)
    
    comparator = kw.pop('comparator', None)
    parententity = kw.pop('parententity', None)
    register_descriptor(class_, key, proxy_property, comparator, parententity)
    if not proxy_property:
        register_attribute_impl(class_, key, **kw)
    
def register_attribute_impl(class_, key,         
        uselist=False, callable_=None, 
        useobject=False, mutable_scalars=False, 
        impl_class=None, **kw):
    
    manager = manager_of_class(class_)
    if uselist:
        factory = kw.pop('typecallable', None)
        typecallable = manager.instrument_collection_class(
            key, factory or list)
    else:
        typecallable = kw.pop('typecallable', None)

    if impl_class:
        impl = impl_class(class_, key, typecallable, **kw)
    elif uselist:
        impl = CollectionAttributeImpl(class_, key, callable_,
                                       typecallable=typecallable, **kw)
    elif useobject:
        impl = ScalarObjectAttributeImpl(class_, key, callable_, **kw)
    elif mutable_scalars:
        impl = MutableScalarAttributeImpl(class_, key, callable_,
                                          class_manager=manager, **kw)
    else:
        impl = ScalarAttributeImpl(class_, key, callable_, **kw)

    manager[key].impl = impl
    
    manager.post_configure_attribute(key)
    
def register_descriptor(class_, key, proxy_property=None, comparator=None, parententity=None, property_=None):
    manager = manager_of_class(class_)

    if proxy_property:
        proxy_type = proxied_attribute_factory(proxy_property)
        descriptor = proxy_type(key, proxy_property, comparator, parententity)
    else:
        descriptor = InstrumentedAttribute(key, comparator=comparator, parententity=parententity)

    manager.instrument_attribute(key, descriptor)

def unregister_attribute(class_, key):
    manager_of_class(class_).uninstrument_attribute(key)

def init_collection(obj, key):
    """Initialize a collection attribute and return the collection adapter.
    
    This function is used to provide direct access to collection internals
    for a previously unloaded attribute.  e.g.::
        
        collection_adapter = init_collection(someobject, 'elements')
        for elem in values:
            collection_adapter.append_without_event(elem)
    
    For an easier way to do the above, see :func:`~sqlalchemy.orm.attributes.set_committed_value`.
    
    obj is an instrumented object instance.  An InstanceState
    is accepted directly for backwards compatibility but 
    this usage is deprecated.
    
    """
    state = _conditional_instance_state(obj)
    dict_ = state.dict
    return init_state_collection(state, dict_, key)
    
def init_state_collection(state, dict_, key):
    """Initialize a collection attribute and return the collection adapter."""
    
    attr = state.get_impl(key)
    user_data = attr.initialize(state, dict_)
    return attr.get_collection(state, dict_, user_data)

def set_committed_value(instance, key, value):
    """Set the value of an attribute with no history events.
    
    Cancels any previous history present.  The value should be 
    a scalar value for scalar-holding attributes, or
    an iterable for any collection-holding attribute.

    This is the same underlying method used when a lazy loader
    fires off and loads additional data from the database.
    In particular, this method can be used by application code
    which has loaded additional attributes or collections through
    separate queries, which can then be attached to an instance
    as though it were part of its original loaded state.
    
    """
    state, dict_ = instance_state(instance), instance_dict(instance)
    state.get_impl(key).set_committed_value(state, dict_, value)
    
def set_attribute(instance, key, value):
    """Set the value of an attribute, firing history events.
    
    This function may be used regardless of instrumentation
    applied directly to the class, i.e. no descriptors are required.
    Custom attribute management schemes will need to make usage
    of this method to establish attribute state as understood
    by SQLAlchemy.
    
    """
    state, dict_ = instance_state(instance), instance_dict(instance)
    state.get_impl(key).set(state, dict_, value, None)

def get_attribute(instance, key):
    """Get the value of an attribute, firing any callables required.

    This function may be used regardless of instrumentation
    applied directly to the class, i.e. no descriptors are required.
    Custom attribute management schemes will need to make usage
    of this method to make usage of attribute state as understood
    by SQLAlchemy.
    
    """
    state, dict_ = instance_state(instance), instance_dict(instance)
    return state.get_impl(key).get(state, dict_)

def del_attribute(instance, key):
    """Delete the value of an attribute, firing history events.

    This function may be used regardless of instrumentation
    applied directly to the class, i.e. no descriptors are required.
    Custom attribute management schemes will need to make usage
    of this method to establish attribute state as understood
    by SQLAlchemy.
    
    """
    state, dict_ = instance_state(instance), instance_dict(instance)
    state.get_impl(key).delete(state, dict_)

def is_instrumented(instance, key):
    """Return True if the given attribute on the given instance is instrumented
    by the attributes package.
    
    This function may be used regardless of instrumentation
    applied directly to the class, i.e. no descriptors are required.
    
    """
    return manager_of_class(instance.__class__).is_instrumented(key, search=True)

class InstrumentationRegistry(object):
    """Private instrumentation registration singleton."""

    _manager_finders = weakref.WeakKeyDictionary()
    _state_finders = util.WeakIdentityMapping()
    _dict_finders = util.WeakIdentityMapping()
    _extended = False

    def create_manager_for_cls(self, class_, **kw):
        assert class_ is not None
        assert manager_of_class(class_) is None

        for finder in instrumentation_finders:
            factory = finder(class_)
            if factory is not None:
                break
        else:
            factory = ClassManager

        existing_factories = self._collect_management_factories_for(class_).\
                                difference([factory])
        if existing_factories:
            raise TypeError(
                "multiple instrumentation implementations specified "
                "in %s inheritance hierarchy: %r" % (
                    class_.__name__, list(existing_factories)))

        manager = factory(class_)
        if not isinstance(manager, ClassManager):
            manager = _ClassInstrumentationAdapter(class_, manager)
            
        if factory != ClassManager and not self._extended:
            self._extended = True
            _install_lookup_strategy(self)
        
        manager._configure_create_arguments(**kw)

        manager.factory = factory
        self._manager_finders[class_] = manager.manager_getter()
        self._state_finders[class_] = manager.state_getter()
        self._dict_finders[class_] = manager.dict_getter()
        return manager

    def _collect_management_factories_for(self, cls):
        """Return a collection of factories in play or specified for a hierarchy.

        Traverses the entire inheritance graph of a cls and returns a collection
        of instrumentation factories for those classes.  Factories are extracted
        from active ClassManagers, if available, otherwise
        instrumentation_finders is consulted.

        """
        hierarchy = util.class_hierarchy(cls)
        factories = set()
        for member in hierarchy:
            manager = manager_of_class(member)
            if manager is not None:
                factories.add(manager.factory)
            else:
                for finder in instrumentation_finders:
                    factory = finder(member)
                    if factory is not None:
                        break
                else:
                    factory = None
                factories.add(factory)
        factories.discard(None)
        return factories

    def manager_of_class(self, cls):
        if cls is None:
            return None
        try:
            finder = self._manager_finders[cls]
        except KeyError:
            return None
        else:
            return finder(cls)

    def state_of(self, instance):
        # this is only called when alternate instrumentation has been established
        if instance is None:
            raise AttributeError("None has no persistent state.")
        try:
            return self._state_finders[instance.__class__](instance)
        except KeyError:
            raise AttributeError("%r is not instrumented" % instance.__class__)

    def dict_of(self, instance):
        # this is only called when alternate instrumentation has been established
        if instance is None:
            raise AttributeError("None has no persistent state.")
        try:
            return self._dict_finders[instance.__class__](instance)
        except KeyError:
            raise AttributeError("%r is not instrumented" % instance.__class__)
        
    def unregister(self, class_):
        if class_ in self._manager_finders:
            manager = self.manager_of_class(class_)
            manager.unregister()
            manager.dispose()
            del self._manager_finders[class_]
            del self._state_finders[class_]
            del self._dict_finders[class_]

instrumentation_registry = InstrumentationRegistry()

def _install_lookup_strategy(implementation):
    """Replace global class/object management functions
    with either faster or more comprehensive implementations,
    based on whether or not extended class instrumentation
    has been detected.
    
    This function is called only by InstrumentationRegistry()
    and unit tests specific to this behavior.
    
    """
    global instance_state, instance_dict
    if implementation is util.symbol('native'):
        instance_state = attrgetter(ClassManager.STATE_ATTR)
        instance_dict = attrgetter("__dict__")
    else:
        instance_state = instrumentation_registry.state_of
        instance_dict = instrumentation_registry.dict_of
        
manager_of_class = instrumentation_registry.manager_of_class
_create_manager_for_cls = instrumentation_registry.create_manager_for_cls
_install_lookup_strategy(util.symbol('native'))

def find_native_user_instrumentation_hook(cls):
    """Find user-specified instrumentation management for a class."""
    return getattr(cls, INSTRUMENTATION_MANAGER, None)
instrumentation_finders.append(find_native_user_instrumentation_hook)

def _generate_init(class_, class_manager):
    """Build an __init__ decorator that triggers ClassManager events."""

    # TODO: we should use the ClassManager's notion of the 
    # original '__init__' method, once ClassManager is fixed
    # to always reference that.
    original__init__ = class_.__init__
    assert original__init__

    # Go through some effort here and don't change the user's __init__
    # calling signature.
    # FIXME: need to juggle local names to avoid constructor argument
    # clashes.
    func_body = """\
def __init__(%(apply_pos)s):
    new_state = class_manager._new_state_if_none(%(self_arg)s)
    if new_state:
        return new_state.initialize_instance(%(apply_kw)s)
    else:
        return original__init__(%(apply_kw)s)
"""
    func_vars = util.format_argspec_init(original__init__, grouped=False)
    func_text = func_body % func_vars

    func = getattr(original__init__, 'im_func', original__init__)
    func_defaults = getattr(func, 'func_defaults', None)

    env = locals().copy()
    exec func_text in env
    __init__ = env['__init__']
    __init__.__doc__ = original__init__.__doc__
    if func_defaults:
        __init__.func_defaults = func_defaults
    return __init__
