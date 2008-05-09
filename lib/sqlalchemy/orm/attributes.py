# attributes.py - manages object attributes
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import operator, weakref
from itertools import chain
import UserDict
from sqlalchemy import util
from sqlalchemy.orm import interfaces, collections
from sqlalchemy.orm.util import identity_equal
from sqlalchemy import exceptions

PASSIVE_NORESULT = util.symbol('PASSIVE_NORESULT')
ATTR_WAS_SET = util.symbol('ATTR_WAS_SET')
NO_VALUE = util.symbol('NO_VALUE')
NEVER_SET = util.symbol('NEVER_SET')

class InstrumentedAttribute(interfaces.PropComparator):
    """public-facing instrumented attribute, placed in the
    class dictionary.

    """

    def __init__(self, impl, comparator=None):
        """Construct an InstrumentedAttribute.
        comparator
          a sql.Comparator to which class-level compare/math events will be sent
        """

        self.impl = impl
        self.comparator = comparator

    def __set__(self, instance, value):
        self.impl.set(instance._state, value, None)

    def __delete__(self, instance):
        self.impl.delete(instance._state)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.impl.get(instance._state)

    def get_history(self, instance, **kwargs):
        return self.impl.get_history(instance._state, **kwargs)

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

    def _property(self):
        from sqlalchemy.orm.mapper import class_mapper
        return class_mapper(self.impl.class_).get_property(self.impl.key)
    property = property(_property, doc="the MapperProperty object associated with this attribute")

class ProxiedAttribute(InstrumentedAttribute):
    """Adds InstrumentedAttribute class-level behavior to a regular descriptor.

    Obsoleted by proxied_attribute_factory.
    """

    class ProxyImpl(object):
        accepts_scalar_loader = False

        def __init__(self, key):
            self.key = key

    def __init__(self, key, user_prop, comparator=None):
        self.user_prop = user_prop
        self._comparator = comparator
        self.key = key
        self.impl = ProxiedAttribute.ProxyImpl(key)

    def comparator(self):
        if callable(self._comparator):
            self._comparator = self._comparator()
        return self._comparator
    comparator = property(comparator)

    def __get__(self, instance, owner):
        if instance is None:
            self.user_prop.__get__(instance, owner)
            return self
        return self.user_prop.__get__(instance, owner)

    def __set__(self, instance, value):
        return self.user_prop.__set__(instance, value)

    def __delete__(self, instance):
        return self.user_prop.__delete__(instance)

def proxied_attribute_factory(descriptor):
    """Create an InstrumentedAttribute / user descriptor hybrid.

    Returns a new InstrumentedAttribute type that delegates descriptor
    behavior and getattr() to the given descriptor.
    """

    class ProxyImpl(object):
        accepts_scalar_loader = False
        def __init__(self, key):
            self.key = key

    class Proxy(InstrumentedAttribute):
        """A combination of InsturmentedAttribute and a regular descriptor."""

        def __init__(self, key, descriptor, comparator):
            self.key = key
            # maintain ProxiedAttribute.user_prop compatability.
            self.descriptor = self.user_prop = descriptor
            self._comparator = comparator
            self.impl = ProxyImpl(key)

        def comparator(self):
            if callable(self._comparator):
                self._comparator = self._comparator()
            return self._comparator
        comparator = property(comparator)

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
            """Delegate __getattr__ to the original descriptor."""
            return getattr(descriptor, attribute)
    Proxy.__name__ = type(descriptor).__name__ + 'Proxy'

    util.monkeypatch_proxied_specials(Proxy, type(descriptor),
                                      name='descriptor',
                                      from_instance=descriptor)
    return Proxy

class AttributeImpl(object):
    """internal implementation for instrumented attributes."""

    def __init__(self, class_, key, callable_, trackparent=False, extension=None, compare_function=None, **kwargs):
        """Construct an AttributeImpl.

        class_
          the class to be instrumented.

        key
          string name of the attribute

        callable_
          optional function which generates a callable based on a parent
          instance, which produces the "default" values for a scalar or
          collection attribute when it's first accessed, if not present
          already.

        trackparent
          if True, attempt to track if an instance has a parent attached
          to it via this attribute.

        extension
          an AttributeExtension object which will receive
          set/delete/append/remove/etc. events.

        compare_function
          a function that compares two values which are normally
          assignable to this attribute.

        """

        self.class_ = class_
        self.key = key
        self.callable_ = callable_
        self.trackparent = trackparent
        if compare_function is None:
            self.is_equal = operator.eq
        else:
            self.is_equal = compare_function
        self.extensions = util.to_list(extension or [])

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
        ``InstrumentedAttribute` constructor.
        """

        if callable_ is None:
            self.initialize(state)
        else:
            state.callables[self.key] = callable_

    def get_history(self, state, passive=False):
        raise NotImplementedError()

    def _get_callable(self, state):
        if self.key in state.callables:
            return state.callables[self.key]
        elif self.callable_ is not None:
            return self.callable_(state.obj())
        else:
            return None

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
            # if no history, check for lazy callables, etc.
            if self.key not in state.committed_state:
                callable_ = self._get_callable(state)
                if callable_ is not None:
                    if passive:
                        return PASSIVE_NORESULT
                    value = callable_()
                    if value is not ATTR_WAS_SET:
                        return self.set_committed_value(state, value)
                    else:
                        if self.key not in state.dict:
                            return self.get(state, passive=passive)
                        return state.dict[self.key]

            # Return a new, empty value
            return self.initialize(state)

    def append(self, state, value, initiator, passive=False):
        self.set(state, value, initiator)

    def remove(self, state, value, initiator, passive=False):
        self.set(state, None, initiator)

    def set(self, state, value, initiator):
        raise NotImplementedError()

    def get_committed_value(self, state):
        """return the unchanged value of this attribute"""

        if self.key in state.committed_state:
            if state.committed_state[self.key] is NO_VALUE:
                return None
            else:
                return state.committed_state.get(self.key)
        else:
            return self.get(state)

    def set_committed_value(self, state, value):
        """set an attribute value on the given instance and 'commit' it."""

        state.commit_attr(self, value)
        return value

class ScalarAttributeImpl(AttributeImpl):
    """represents a scalar value-holding InstrumentedAttribute."""

    accepts_scalar_loader = True

    def delete(self, state):
        if self.key not in state.committed_state:
            state.committed_state[self.key] = state.dict.get(self.key, NO_VALUE)

        # TODO: catch key errors, convert to attributeerror?
        del state.dict[self.key]
        state.modified=True

    def get_history(self, state, passive=False):
        return _create_history(self, state, state.dict.get(self.key, NO_VALUE))

    def set(self, state, value, initiator):
        if initiator is self:
            return

        if self.key not in state.committed_state:
            state.committed_state[self.key] = state.dict.get(self.key, NO_VALUE)

        state.dict[self.key] = value
        state.modified=True

    def type(self):
        self.property.columns[0].type
    type = property(type)

class MutableScalarAttributeImpl(ScalarAttributeImpl):
    """represents a scalar value-holding InstrumentedAttribute, which can detect
    changes within the value itself.
    """

    def __init__(self, class_, key, callable_, copy_function=None, compare_function=None, **kwargs):
        super(ScalarAttributeImpl, self).__init__(class_, key, callable_, compare_function=compare_function, **kwargs)
        class_._class_state.has_mutable_scalars = True
        if copy_function is None:
            raise exceptions.ArgumentError("MutableScalarAttributeImpl requires a copy function")
        self.copy = copy_function

    def get_history(self, state, passive=False):
        return _create_history(self, state, state.dict.get(self.key, NO_VALUE))

    def commit_to_state(self, state, value):
        state.committed_state[self.key] = self.copy(value)

    def check_mutable_modified(self, state):
        (added, unchanged, deleted) = self.get_history(state, passive=True)
        if added or deleted:
            state.modified = True
            return True
        else:
            return False

    def set(self, state, value, initiator):
        if initiator is self:
            return

        if self.key not in state.committed_state:
            if self.key in state.dict:
                state.committed_state[self.key] = self.copy(state.dict[self.key])
            else:
                state.committed_state[self.key] = NO_VALUE

        state.dict[self.key] = value
        state.modified=True


class ScalarObjectAttributeImpl(ScalarAttributeImpl):
    """represents a scalar-holding InstrumentedAttribute, where the target object is also instrumented.

    Adds events to delete/set operations.
    """

    accepts_scalar_loader = False

    def __init__(self, class_, key, callable_, trackparent=False, extension=None, copy_function=None, compare_function=None, **kwargs):
        super(ScalarObjectAttributeImpl, self).__init__(class_, key,
          callable_, trackparent=trackparent, extension=extension,
          compare_function=compare_function, **kwargs)
        if compare_function is None:
            self.is_equal = identity_equal

    def delete(self, state):
        old = self.get(state)
        # TODO: catch key errors, convert to attributeerror?
        del state.dict[self.key]
        self.fire_remove_event(state, old, self)

    def get_history(self, state, passive=False):
        if self.key in state.dict:
            return _create_history(self, state, state.dict[self.key])
        else:
            current = self.get(state, passive=passive)
            if current is PASSIVE_NORESULT:
                return (None, None, None)
            else:
                return _create_history(self, state, current)

    def set(self, state, value, initiator):
        """Set a value on the given InstanceState.

        `initiator` is the ``InstrumentedAttribute`` that initiated the
        ``set()` operation and is used to control the depth of a circular
        setter operation.
        """

        if initiator is self:
            return

        if value is not None and not hasattr(value, '_state'):
            raise TypeError("Can not assign %s instance to %s's %r attribute, "
                            "a mapped instance was expected." % (
                type(value).__name__, type(state.obj()).__name__, self.key))

        # TODO: add options to allow the get() to be passive
        old = self.get(state)
        state.dict[self.key] = value
        self.fire_replace_event(state, value, old, initiator)

    def fire_remove_event(self, state, value, initiator):
        if self.key not in state.committed_state:
            state.committed_state[self.key] = value
        state.modified = True

        if self.trackparent and value is not None:
            self.sethasparent(value._state, False)

        instance = state.obj()
        for ext in self.extensions:
            ext.remove(instance, value, initiator or self)

    def fire_replace_event(self, state, value, previous, initiator):
        if self.key not in state.committed_state:
            state.committed_state[self.key] = previous
        state.modified = True

        if self.trackparent:
            if value is not None:
                self.sethasparent(value._state, True)
            if previous is not value and previous is not None:
                self.sethasparent(previous._state, False)

        instance = state.obj()
        for ext in self.extensions:
            ext.set(instance, value, previous, initiator or self)

class CollectionAttributeImpl(AttributeImpl):
    """A collection-holding attribute that instruments changes in membership.

    Only handles collections of instrumented objects.

    InstrumentedCollectionAttribute holds an arbitrary, user-specified
    container object (defaulting to a list) and brokers access to the
    CollectionAdapter, a "view" onto that object that presents consistent
    bag semantics to the orm layer independent of the user data implementation.
    """
    accepts_scalar_loader = False

    def __init__(self, class_, key, callable_, typecallable=None, trackparent=False, extension=None, copy_function=None, compare_function=None, **kwargs):
        super(CollectionAttributeImpl, self).__init__(class_,
          key, callable_, trackparent=trackparent, extension=extension,
          compare_function=compare_function, **kwargs)

        if copy_function is None:
            copy_function = self.__copy
        self.copy = copy_function

        if typecallable is None:
            typecallable = list
        self.collection_factory = \
          collections._prepare_instrumentation(typecallable)
        # may be removed in 0.5:
        self.collection_interface = \
          util.duck_type_collection(self.collection_factory())

    def __copy(self, item):
        return [y for y in list(collections.collection_adapter(item))]

    def get_history(self, state, passive=False):
        current = self.get(state, passive=passive)
        if current is PASSIVE_NORESULT:
            return (None, None, None)
        else:
            return _create_history(self, state, current)

    def fire_append_event(self, state, value, initiator):
        if self.key not in state.committed_state and self.key in state.dict:
            state.committed_state[self.key] = self.copy(state.dict[self.key])

        state.modified = True

        if self.trackparent and value is not None:
            self.sethasparent(value._state, True)
        instance = state.obj()
        for ext in self.extensions:
            ext.append(instance, value, initiator or self)

    def fire_pre_remove_event(self, state, initiator):
        if self.key not in state.committed_state and self.key in state.dict:
            state.committed_state[self.key] = self.copy(state.dict[self.key])

    def fire_remove_event(self, state, value, initiator):
        if self.key not in state.committed_state and self.key in state.dict:
            state.committed_state[self.key] = self.copy(state.dict[self.key])

        state.modified = True

        if self.trackparent and value is not None:
            self.sethasparent(value._state, False)

        instance = state.obj()
        for ext in self.extensions:
            ext.remove(instance, value, initiator or self)

    def delete(self, state):
        if self.key not in state.dict:
            return

        state.modified = True

        collection = self.get_collection(state)
        collection.clear_with_event()
        # TODO: catch key errors, convert to attributeerror?
        del state.dict[self.key]

    def initialize(self, state):
        """Initialize this attribute on the given object instance with an empty collection."""

        _, user_data = self._build_collection(state)
        state.dict[self.key] = user_data
        return user_data

    def append(self, state, value, initiator, passive=False):
        if initiator is self:
            return

        collection = self.get_collection(state, passive=passive)
        if collection is PASSIVE_NORESULT:
            state.get_pending(self.key).append(value)
            self.fire_append_event(state, value, initiator)
        else:
            collection.append_with_event(value, initiator)

    def remove(self, state, value, initiator, passive=False):
        if initiator is self:
            return

        collection = self.get_collection(state, passive=passive)
        if collection is PASSIVE_NORESULT:
            state.get_pending(self.key).remove(value)
            self.fire_remove_event(state, value, initiator)
        else:
            collection.remove_with_event(value, initiator)
    
    def set(self, state, value, initiator):
        """Set a value on the given object.

        `initiator` is the ``InstrumentedAttribute`` that initiated the
        ``set()` operation and is used to control the depth of a circular
        setter operation.
        """

        if initiator is self:
            return

        self._set_iterable(
            state, value,
            lambda adapter, i: adapter.adapt_like_to_iterable(i))

    def _set_iterable(self, state, iterable, adapter=None):
        """Set a collection value from an iterable of state-bearers.

        ``adapter`` is an optional callable invoked with a CollectionAdapter
        and the iterable.  Should return an iterable of state-bearing
        instances suitable for appending via a CollectionAdapter.  Can be used
        for, e.g., adapting an incoming dictionary into an iterator of values
        rather than keys.

        """
        # pulling a new collection first so that an adaptation exception does
        # not trigger a lazy load of the old collection.
        new_collection, user_data = self._build_collection(state)
        if adapter:
            new_values = list(adapter(new_collection, iterable))
        else:
            new_values = list(iterable)

        old = self.get(state)

        # ignore re-assignment of the current collection, as happens
        # implicitly with in-place operators (foo.collection |= other)
        if old is iterable:
            return

        if self.key not in state.committed_state:
            state.committed_state[self.key] = self.copy(old)

        old_collection = self.get_collection(state, old)

        state.dict[self.key] = user_data
        state.modified = True

        collections.bulk_replace(new_values, old_collection, new_collection)
        old_collection.unlink(old)


    def set_committed_value(self, state, value):
        """Set an attribute value on the given instance and 'commit' it.

        Loads the existing collection from lazy callables in all cases.
        """

        collection, user_data = self._build_collection(state)

        if value:
            for item in value:
                collection.append_without_event(item)

        state.callables.pop(self.key, None)
        state.dict[self.key] = user_data

        if self.key in state.pending:
            # pending items.  commit loaded data, add/remove new data
            state.committed_state[self.key] = list(value or [])
            added = state.pending[self.key].added_items
            removed = state.pending[self.key].deleted_items
            for item in added:
                collection.append_without_event(item)
            for item in removed:
                collection.remove_without_event(item)
            del state.pending[self.key]
        elif self.key in state.committed_state:
            # no pending items.  remove committed state if any.
            # (this can occur with an expired attribute)
            del state.committed_state[self.key]

        return user_data

    def _build_collection(self, state):
        """build a new, blank collection and return it wrapped in a CollectionAdapter."""

        user_data = self.collection_factory()
        collection = collections.CollectionAdapter(self, state, user_data)
        return collection, user_data

    def get_collection(self, state, user_data=None, passive=False):
        """retrieve the CollectionAdapter associated with the given state.

        Creates a new CollectionAdapter if one does not exist.

        """

        if user_data is None:
            user_data = self.get(state, passive=passive)
            if user_data is PASSIVE_NORESULT:
                return user_data
        try:
            return getattr(user_data, '_sa_adapter')
        except AttributeError:
            # TODO: this codepath never occurs, and this
            # except/initialize should be removed
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

    def set(self, instance, child, oldchild, initiator):
        if oldchild is child:
            return
        if oldchild is not None:
            # With lazy=None, there's no guarantee that the full collection is
            # present when updating via a backref.
            impl = getattr(oldchild.__class__, self.key).impl
            try:
                impl.remove(oldchild._state, instance, initiator, passive=True)
            except (ValueError, KeyError, IndexError):
                pass
        if child is not None:
            getattr(child.__class__, self.key).impl.append(child._state, instance, initiator, passive=True)

    def append(self, instance, child, initiator):
        getattr(child.__class__, self.key).impl.append(child._state, instance, initiator, passive=True)

    def remove(self, instance, child, initiator):
        if child is not None:
            getattr(child.__class__, self.key).impl.remove(child._state, instance, initiator, passive=True)

class ClassState(object):
    """tracks state information at the class level."""
    def __init__(self):
        self.mappers = {}
        self.attrs = {}
        self.has_mutable_scalars = False

import sets
_empty_set = sets.ImmutableSet()

class InstanceState(object):
    """tracks state information at the instance level."""

    def __init__(self, obj):
        self.class_ = obj.__class__
        self.obj = weakref.ref(obj, self.__cleanup)
        self.dict = obj.__dict__
        self.committed_state = {}
        self.modified = False
        self.callables = {}
        self.parents = {}
        self.pending = {}
        self.appenders = {}
        self.instance_dict = None
        self.runid = None
        self.expired_attributes = _empty_set

    def __cleanup(self, ref):
        # tiptoe around Python GC unpredictableness
        instance_dict = self.instance_dict
        if instance_dict is None:
            return

        instance_dict = instance_dict()
        if instance_dict is None or instance_dict._mutex is None:
            return

        # the mutexing here is based on the assumption that gc.collect()
        # may be firing off cleanup handlers in a different thread than that
        # which is normally operating upon the instance dict.
        instance_dict._mutex.acquire()
        try:
            try:
                self.__resurrect(instance_dict)
            except:
                # catch app cleanup exceptions.  no other way around this
                # without warnings being produced
                pass
        finally:
            instance_dict._mutex.release()

    def _check_resurrect(self, instance_dict):
        instance_dict._mutex.acquire()
        try:
            return self.obj() or self.__resurrect(instance_dict)
        finally:
            instance_dict._mutex.release()

    def get_pending(self, key):
        if key not in self.pending:
            self.pending[key] = PendingCollection()
        return self.pending[key]

    def is_modified(self):
        if self.modified:
            return True
        elif self.class_._class_state.has_mutable_scalars:
            for attr in _managed_attributes(self.class_):
                if hasattr(attr.impl, 'check_mutable_modified') and attr.impl.check_mutable_modified(self):
                    return True
            else:
                return False
        else:
            return False

    def __resurrect(self, instance_dict):
        if self.is_modified():
            # store strong ref'ed version of the object; will revert
            # to weakref when changes are persisted
            obj = new_instance(self.class_, state=self)
            self.obj = weakref.ref(obj, self.__cleanup)
            self._strong_obj = obj
            obj.__dict__.update(self.dict)
            self.dict = obj.__dict__
            return obj
        else:
            del instance_dict[self.dict['_instance_key']]
            return None

    def __getstate__(self):
        return {'committed_state':self.committed_state, 'pending':self.pending, 'parents':self.parents, 'modified':self.modified, 'instance':self.obj(), 'expired_attributes':self.expired_attributes, 'callables':self.callables}

    def __setstate__(self, state):
        self.committed_state = state['committed_state']
        self.parents = state['parents']
        self.pending = state['pending']
        self.modified = state['modified']
        self.obj = weakref.ref(state['instance'])
        self.class_ = self.obj().__class__
        self.dict = self.obj().__dict__
        self.callables = state['callables']
        self.runid = None
        self.appenders = {}
        self.expired_attributes = state['expired_attributes']

    def initialize(self, key):
        getattr(self.class_, key).impl.initialize(self)

    def set_callable(self, key, callable_):
        self.dict.pop(key, None)
        self.callables[key] = callable_

    def __call__(self):
        """__call__ allows the InstanceState to act as a deferred
        callable for loading expired attributes, which is also
        serializable.
        """
        instance = self.obj()
        unmodified = self.unmodified
        self.class_._class_state.deferred_scalar_loader(instance, [
            attr.impl.key for attr in _managed_attributes(self.class_) if
                attr.impl.accepts_scalar_loader and
                attr.impl.key in self.expired_attributes and
                attr.impl.key in unmodified
            ])
        for k in self.expired_attributes:
            self.callables.pop(k, None)
        self.expired_attributes.clear()
        return ATTR_WAS_SET

    def unmodified(self):
        """a set of keys which have no uncommitted changes"""

        return util.Set([
            attr.impl.key for attr in _managed_attributes(self.class_) if
            attr.impl.key not in self.committed_state
            and (not hasattr(attr.impl, 'commit_to_state') or not attr.impl.check_mutable_modified(self))
        ])
    unmodified = property(unmodified)

    def expire_attributes(self, attribute_names):
        self.expired_attributes = util.Set(self.expired_attributes)

        if attribute_names is None:
            for attr in _managed_attributes(self.class_):
                self.dict.pop(attr.impl.key, None)
                self.expired_attributes.add(attr.impl.key)
                if attr.impl.accepts_scalar_loader:
                    self.callables[attr.impl.key] = self

            self.committed_state = {}
        else:
            for key in attribute_names:
                self.dict.pop(key, None)
                self.committed_state.pop(key, None)
                self.expired_attributes.add(key)
                if getattr(self.class_, key).impl.accepts_scalar_loader:
                    self.callables[key] = self

    def reset(self, key):
        """remove the given attribute and any callables associated with it."""
        self.dict.pop(key, None)
        self.callables.pop(key, None)

    def commit_attr(self, attr, value):
        """set the value of an attribute and mark it 'committed'."""

        if hasattr(attr, 'commit_to_state'):
            attr.commit_to_state(self, value)
        else:
            self.committed_state.pop(attr.key, None)
        self.dict[attr.key] = value
        self.pending.pop(attr.key, None)
        self.appenders.pop(attr.key, None)

        # we have a value so we can also unexpire it
        self.callables.pop(attr.key, None)
        if attr.key in self.expired_attributes:
            self.expired_attributes.remove(attr.key)

    def commit(self, keys):
        """commit all attributes named in the given list of key names.

        This is used by a partial-attribute load operation to mark committed those attributes
        which were refreshed from the database.

        Attributes marked as "expired" can potentially remain "expired" after this step
        if a value was not populated in state.dict.
        """

        if self.class_._class_state.has_mutable_scalars:
            for key in keys:
                attr = getattr(self.class_, key).impl
                if hasattr(attr, 'commit_to_state') and attr.key in self.dict:
                    attr.commit_to_state(self, self.dict[attr.key])
                else:
                    self.committed_state.pop(attr.key, None)
                self.pending.pop(key, None)
                self.appenders.pop(key, None)
        else:
            for key in keys:
                self.committed_state.pop(key, None)
                self.pending.pop(key, None)
                self.appenders.pop(key, None)

        # unexpire attributes which have loaded
        for key in self.expired_attributes.intersection(keys):
            if key in self.dict:
                self.expired_attributes.remove(key)
                self.callables.pop(key, None)


    def commit_all(self):
        """commit all attributes unconditionally.

        This is used after a flush() or a regular instance load or refresh operation
        to mark committed all populated attributes.

        Attributes marked as "expired" can potentially remain "expired" after this step
        if a value was not populated in state.dict.
        """

        self.committed_state = {}
        self.modified = False
        self.pending = {}
        self.appenders = {}

        # unexpire attributes which have loaded
        for key in list(self.expired_attributes):
            if key in self.dict:
                self.expired_attributes.remove(key)
                self.callables.pop(key, None)

        if self.class_._class_state.has_mutable_scalars:
            for attr in _managed_attributes(self.class_):
                if hasattr(attr.impl, 'commit_to_state') and attr.impl.key in self.dict:
                    attr.impl.commit_to_state(self, self.dict[attr.impl.key])

        # remove strong ref
        self._strong_obj = None


class WeakInstanceDict(UserDict.UserDict):
    """similar to WeakValueDictionary, but wired towards 'state' objects."""

    def __init__(self, *args, **kw):
        self._wr = weakref.ref(self)
        # RLock because the mutex is used by a cleanup handler, which can be
        # called at any time (including within an already mutexed block)
        self._mutex = util.threading.RLock()
        UserDict.UserDict.__init__(self, *args, **kw)

    def __getitem__(self, key):
        state = self.data[key]
        o = state.obj()
        if o is None:
            o = state._check_resurrect(self)
        if o is None:
            raise KeyError, key
        return o

    def __contains__(self, key):
        try:
            state = self.data[key]
            o = state.obj()
            if o is None:
                o = state._check_resurrect(self)
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
            instance = state.obj()
            if instance is not None:
                yield instance

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

    def all_states(self):
        return self.data.values()

class StrongInstanceDict(dict):
    def all_states(self):
        return [o._state for o in self.values()]

def _create_history(attr, state, current):
    original = state.committed_state.get(attr.key, NEVER_SET)

    if hasattr(attr, 'get_collection'):
        current = attr.get_collection(state, current)
        if original is NO_VALUE:
            return (list(current), [], [])
        elif original is NEVER_SET:
            return ([], list(current), [])
        else:
            collection = util.OrderedIdentitySet(current)
            s = util.OrderedIdentitySet(original)
            return (list(collection.difference(s)), list(collection.intersection(s)), list(s.difference(collection)))
    else:
        if current is NO_VALUE:
            if original not in [None, NEVER_SET, NO_VALUE]:
                deleted = [original]
            else:
                deleted = []
            return ([], [], deleted)
        elif original is NO_VALUE:
            return ([current], [], [])
        elif original is NEVER_SET or attr.is_equal(current, original) is True:   # dont let ClauseElement expressions here trip things up
            return ([], [current], [])
        else:
            if original is not None:
                deleted = [original]
            else:
                deleted = []
            return ([current], [], deleted)

class PendingCollection(object):
    """stores items appended and removed from a collection that has not been loaded yet.

    When the collection is loaded, the changes present in PendingCollection are applied
    to produce the final result.
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

def _managed_attributes(class_):
    """return all InstrumentedAttributes associated with the given class_ and its superclasses."""

    return chain(*[cl._class_state.attrs.values() for cl in class_.__mro__[:-1] if hasattr(cl, '_class_state')])

def get_history(state, key, **kwargs):
    return getattr(state.class_, key).impl.get_history(state, **kwargs)

def get_as_list(state, key, passive=False):
    """return an InstanceState attribute as a list,
    regardless of it being a scalar or collection-based
    attribute.

    returns None if passive=True and the getter returns
    PASSIVE_NORESULT.
    """

    attr = getattr(state.class_, key).impl
    x = attr.get(state, passive=passive)
    if x is PASSIVE_NORESULT:
        return None
    elif hasattr(attr, 'get_collection'):
        return attr.get_collection(state, x, passive=passive)
    elif isinstance(x, list):
        return x
    else:
        return [x]

def has_parent(class_, instance, key, optimistic=False):
    return getattr(class_, key).impl.hasparent(instance._state, optimistic=optimistic)

def _create_prop(class_, key, uselist, callable_, typecallable, useobject, mutable_scalars, impl_class, **kwargs):
    if impl_class:
        return impl_class(class_, key, typecallable, **kwargs)
    elif uselist:
        return CollectionAttributeImpl(class_, key, callable_, typecallable, **kwargs)
    elif useobject:
        return ScalarObjectAttributeImpl(class_, key, callable_,**kwargs)
    elif mutable_scalars:
        return MutableScalarAttributeImpl(class_, key, callable_, **kwargs)
    else:
        return ScalarAttributeImpl(class_, key, callable_, **kwargs)

def manage(instance):
    """initialize an InstanceState on the given instance."""

    if not hasattr(instance, '_state'):
        instance._state = InstanceState(instance)

def new_instance(class_, state=None):
    """create a new instance of class_ without its __init__() method being called.

    Also initializes an InstanceState on the new instance.
    """

    s = class_.__new__(class_)
    if state:
        s._state = state
    else:
        s._state = InstanceState(s)
    return s

def _init_class_state(class_):
    if not '_class_state' in class_.__dict__:
        class_._class_state = ClassState()

def register_class(class_, extra_init=None, on_exception=None, deferred_scalar_loader=None):
    _init_class_state(class_)
    class_._class_state.deferred_scalar_loader=deferred_scalar_loader

    oldinit = None
    doinit = False

    def init(instance, *args, **kwargs):
        if not hasattr(instance, '_state'):
            instance._state = InstanceState(instance)

        if extra_init:
            extra_init(class_, oldinit, instance, args, kwargs)

        try:
            if doinit:
                oldinit(instance, *args, **kwargs)
            elif args or kwargs:
                # simulate error message raised by object(), but don't copy
                # the text verbatim
                raise TypeError("default constructor for object() takes no parameters")
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

def unregister_class(class_):
    if hasattr(class_, '__init__') and hasattr(class_.__init__, '_oldinit'):
        if class_.__init__._oldinit is not None:
            class_.__init__ = class_.__init__._oldinit
        else:
            delattr(class_, '__init__')

    if '_class_state' in class_.__dict__:
        _class_state = class_.__dict__['_class_state']
        for key, attr in _class_state.attrs.iteritems():
            if key in class_.__dict__:
                delattr(class_, attr.impl.key)
        delattr(class_, '_class_state')

def register_attribute(class_, key, uselist, useobject, callable_=None, proxy_property=None, mutable_scalars=False, impl_class=None, **kwargs):
    _init_class_state(class_)

    typecallable = kwargs.pop('typecallable', None)
    if isinstance(typecallable, InstrumentedAttribute):
        typecallable = None
    comparator = kwargs.pop('comparator', None)

    if key in class_.__dict__ and isinstance(class_.__dict__[key], InstrumentedAttribute):
        # this currently only occurs if two primary mappers are made for the same class.
        # TODO:  possibly have InstrumentedAttribute check "entity_name" when searching for impl.
        # raise an error if two attrs attached simultaneously otherwise
        return

    if proxy_property:
        proxy_type = proxied_attribute_factory(proxy_property)
        inst = proxy_type(key, proxy_property, comparator)
    else:
        inst = InstrumentedAttribute(_create_prop(class_, key, uselist, callable_, useobject=useobject,
                                       typecallable=typecallable, mutable_scalars=mutable_scalars, impl_class=impl_class, **kwargs), comparator=comparator)

    setattr(class_, key, inst)
    class_._class_state.attrs[key] = inst

def unregister_attribute(class_, key):
    class_state = class_._class_state
    if key in class_state.attrs:
        del class_._class_state.attrs[key]
        delattr(class_, key)

def init_collection(instance, key):
    """Initialize a collection attribute and return the collection adapter."""
    attr = getattr(instance.__class__, key).impl
    state = instance._state
    user_data = attr.initialize(state)
    return attr.get_collection(state, user_data)
