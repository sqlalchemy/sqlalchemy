# attributes.py - manages object attributes
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import weakref

from sqlalchemy import util
from sqlalchemy.orm import util as orm_util, interfaces, collections
from sqlalchemy.orm.mapper import class_mapper
from sqlalchemy import logging, exceptions


PASSIVE_NORESULT = object()
ATTR_WAS_SET = object()
NO_VALUE = object()

class InstrumentedAttribute(interfaces.PropComparator):
    """attribute access for instrumented classes."""
    
    def __init__(self, class_, manager, key, callable_, trackparent=False, extension=None, compare_function=None, mutable_scalars=False, comparator=None, **kwargs):
        """Construct an InstrumentedAttribute.
        
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
              
            comparator
              a sql.Comparator to which class-level compare/math events will be sent
              
        """
        
        self.class_ = class_
        self.manager = manager
        self.key = key
        self.callable_ = callable_
        self.trackparent = trackparent
        self.mutable_scalars = mutable_scalars
        self.comparator = comparator
        self.copy = None
        if compare_function is None:
            self.is_equal = lambda x,y: x == y
        else:
            self.is_equal = compare_function
        self.extensions = util.to_list(extension or [])

    def __set__(self, obj, value):
        self.set(obj, value, None)

    def __delete__(self, obj):
        self.delete(None, obj)

    def __get__(self, obj, owner):
        if obj is None:
            return self
        return self.get(obj)

    def commit_to_state(self, state, obj, value=NO_VALUE):
        """commit the a copy of thte value of 'obj' to the given CommittedState"""

        if value is NO_VALUE:
            if self.key in obj.__dict__:
                value = obj.__dict__[self.key]
        if value is not NO_VALUE:
            state.data[self.key] = self.copy(value)

    def clause_element(self):
        return self.comparator.clause_element()

    def expression_element(self):
        return self.comparator.expression_element()
        
    def operate(self, op, *other, **kwargs):
        return op(self.comparator, *other, **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        return op(other, self.comparator, **kwargs)
        
    def hasparent(self, item, optimistic=False):
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

        return item._state.get(('hasparent', id(self)), optimistic)

    def sethasparent(self, item, value):
        """Set a boolean flag on the given item corresponding to
        whether or not it is attached to a parent object via the
        attribute represented by this ``InstrumentedAttribute``.
        """

        item._state[('hasparent', id(self))] = value

    def get_history(self, obj, passive=False):
        """Return a new ``AttributeHistory`` object for the given object/this attribute's key.

        If `passive` is True, then don't execute any callables; if the
        attribute's value can only be achieved via executing a
        callable, then return None.
        """

        # get the current state.  this may trigger a lazy load if
        # passive is False.
        current = self.get(obj, passive=passive)
        if current is PASSIVE_NORESULT:
            return None
        return AttributeHistory(self, obj, current, passive=passive)

    def set_callable(self, obj, callable_, clear=False):
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
            self.clear(obj)
            
        if callable_ is None:
            self.initialize(obj)
        else:
            obj._state[('callable', self)] = callable_

    def _get_callable(self, obj):
        if ('callable', self) in obj._state:
            return obj._state[('callable', self)]
        elif self.callable_ is not None:
            return self.callable_(obj)
        else:
            return None

    def reset(self, obj):
        """Remove any per-instance callable functions corresponding to
        this ``InstrumentedAttribute``'s attribute from the given
        object, and remove this ``InstrumentedAttribute``'s attribute
        from the given object's dictionary.
        """

        try:
            del obj._state[('callable', self)]
        except KeyError:
            pass
        self.clear(obj)

    def clear(self, obj):
        """Remove this ``InstrumentedAttribute``'s attribute from the given object's dictionary.

        Subsequent calls to ``getattr(obj, key)`` will raise an
        ``AttributeError`` by default.
        """

        try:
            del obj.__dict__[self.key]
        except KeyError:
            pass

    def check_mutable_modified(self, obj):
        return False

    def initialize(self, obj):
        """Initialize this attribute on the given object instance with an empty value."""

        obj.__dict__[self.key] = None
        return None

    def get(self, obj, passive=False):
        """Retrieve a value from the given object.

        If a callable is assembled on this object's attribute, and
        passive is False, the callable will be executed and the
        resulting value will be set as the new value for this attribute.
        """

        try:
            return obj.__dict__[self.key]
        except KeyError:
            state = obj._state
            # if an instance-wide "trigger" was set, call that
            # and start again
            if 'trigger' in state:
                trig = state['trigger']
                del state['trigger']
                trig()
                return self.get(obj, passive=passive)

            callable_ = self._get_callable(obj)
            if callable_ is not None:
                if passive:
                    return PASSIVE_NORESULT
                self.logger.debug("Executing lazy callable on %s.%s" %
                                  (orm_util.instance_str(obj), self.key))
                value = callable_()
                if value is not ATTR_WAS_SET:
                    return self.set_committed_value(obj, value)
                else:
                    return obj.__dict__[self.key]
            else:
                # Return a new, empty value
                return self.initialize(obj)

    def append(self, obj, value, initiator):
        self.set(obj, value, initiator)

    def remove(self, obj, value, initiator):
        self.set(obj, None, initiator)

    def set(self, obj, value, initiator):
        raise NotImplementedError()

    def set_committed_value(self, obj, value):
        """set an attribute value on the given instance and 'commit' it.
        
        this indicates that the given value is the "persisted" value,
        and history will be logged only if a newly set value is not
        equal to this value.
        
        this is typically used by deferred/lazy attribute loaders
        to set object attributes after the initial load.
        """

        state = obj._state
        orig = state.get('original', None)
        if orig is not None:
            self.commit_to_state(orig, obj, value)
        # remove per-instance callable, if any
        state.pop(('callable', self), None)
        obj.__dict__[self.key] = value
        return value

    def set_raw_value(self, obj, value):
        obj.__dict__[self.key] = value
        return value

    def fire_append_event(self, obj, value, initiator):
        obj._state['modified'] = True
        if self.trackparent and value is not None:
            self.sethasparent(value, True)
        for ext in self.extensions:
            ext.append(obj, value, initiator or self)

    def fire_remove_event(self, obj, value, initiator):
        obj._state['modified'] = True
        if self.trackparent and value is not None:
            self.sethasparent(value, False)
        for ext in self.extensions:
            ext.remove(obj, value, initiator or self)

    def fire_replace_event(self, obj, value, previous, initiator):
        obj._state['modified'] = True
        if self.trackparent:
            if value is not None:
                self.sethasparent(value, True)
            if previous is not None:
                self.sethasparent(previous, False)
        for ext in self.extensions:
            ext.set(obj, value, previous, initiator or self)

    property = property(lambda s: class_mapper(s.class_).get_property(s.key),
                        doc="the MapperProperty object associated with this attribute")

InstrumentedAttribute.logger = logging.class_logger(InstrumentedAttribute)

        
class InstrumentedScalarAttribute(InstrumentedAttribute):
    """represents a scalar-holding InstrumentedAttribute."""
    
    def __init__(self, class_, manager, key, callable_, trackparent=False, extension=None, copy_function=None, compare_function=None, mutable_scalars=False, **kwargs):
        super(InstrumentedScalarAttribute, self).__init__(class_, manager, key,
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

    def __delete__(self, obj):
        old = self.get(obj)
        del obj.__dict__[self.key]
        self.fire_remove_event(obj, old, self)

    def check_mutable_modified(self, obj):
        if self.mutable_scalars:
            h = self.get_history(obj, passive=True)
            if h is not None and h.is_modified():
                obj._state['modified'] = True
                return True
            else:
                return False
        else:
            return False

    def set(self, obj, value, initiator):
        """Set a value on the given object.

        `initiator` is the ``InstrumentedAttribute`` that initiated the
        ``set()` operation and is used to control the depth of a circular
        setter operation.
        """

        if initiator is self:
            return

        state = obj._state
        # if an instance-wide "trigger" was set, call that
        if 'trigger' in state:
            trig = state['trigger']
            del state['trigger']
            trig()

        old = self.get(obj)
        obj.__dict__[self.key] = value
        self.fire_replace_event(obj, value, old, initiator)

    type = property(lambda self: self.property.columns[0].type)

        
class InstrumentedCollectionAttribute(InstrumentedAttribute):
    """A collection-holding attribute that instruments changes in membership.

    InstrumentedCollectionAttribute holds an arbitrary, user-specified
    container object (defaulting to a list) and brokers access to the
    CollectionAdapter, a "view" onto that object that presents consistent
    bag semantics to the orm layer independent of the user data implementation.
    """
    
    def __init__(self, class_, manager, key, callable_, typecallable=None, trackparent=False, extension=None, copy_function=None, compare_function=None, **kwargs):
        super(InstrumentedCollectionAttribute, self).__init__(class_, manager,
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

    def __set__(self, obj, value):
        """Replace the current collection with a new one."""

        setting_type = util.duck_type_collection(value)

        if value is None or setting_type != self.collection_interface:
            raise exceptions.ArgumentError(
                "Incompatible collection type on assignment: %s is not %s-like" %
                (type(value).__name__, self.collection_interface.__name__))

        if hasattr(value, '_sa_adapter'):
            self.set(obj, list(getattr(value, '_sa_adapter')), None)
        elif setting_type == dict:
            self.set(obj, value.values(), None)
        else:
            self.set(obj, value, None)

    def __delete__(self, obj):
        if self.key not in obj.__dict__:
            return

        obj._state['modified'] = True

        collection = self._get_collection(obj)
        collection.clear_with_event()
        del obj.__dict__[self.key]

    def initialize(self, obj):
        """Initialize this attribute on the given object instance with an empty collection."""

        _, user_data = self._build_collection(obj)
        obj.__dict__[self.key] = user_data
        return user_data

    def append(self, obj, value, initiator):
        if initiator is self:
            return
        collection = self._get_collection(obj)
        collection.append_with_event(value, initiator)

    def remove(self, obj, value, initiator):
        if initiator is self:
            return
        collection = self._get_collection(obj)
        collection.remove_with_event(value, initiator)

    def set(self, obj, value, initiator):
        """Set a value on the given object.

        `initiator` is the ``InstrumentedAttribute`` that initiated the
        ``set()` operation and is used to control the depth of a circular
        setter operation.
        """

        if initiator is self:
            return

        state = obj._state
        # if an instance-wide "trigger" was set, call that
        if 'trigger' in state:
            trig = state['trigger']
            del state['trigger']
            trig()

        old = self.get(obj)
        old_collection = self._get_collection(obj, old)

        new_collection, user_data = self._build_collection(obj)
        self._load_collection(obj, value or [], emit_events=True,
                              collection=new_collection)

        obj.__dict__[self.key] = user_data
        state['modified'] = True

        # mark all the old elements as detached from the parent
        if old_collection:
            old_collection.clear_with_event()
            old_collection.unlink(old)

    def set_committed_value(self, obj, value):
        """Set an attribute value on the given instance and 'commit' it."""
        
        state = obj._state
        orig = state.get('original', None)

        collection, user_data = self._build_collection(obj)
        self._load_collection(obj, value or [], emit_events=False,
                              collection=collection)
        value = user_data

        if orig is not None:
            self.commit_to_state(orig, obj, value)
        # remove per-instance callable, if any
        state.pop(('callable', self), None)
        obj.__dict__[self.key] = value
        return value

    def _build_collection(self, obj):
        user_data = self.collection_factory()
        collection = collections.CollectionAdapter(self, obj, user_data)
        return collection, user_data

    def _load_collection(self, obj, values, emit_events=True, collection=None):
        collection = collection or self._get_collection(obj)
        if values is None:
            return
        elif emit_events:
            for item in values:
                collection.append_with_event(item)
        else:
            for item in values:
                collection.append_without_event(item)
            
    def _get_collection(self, obj, user_data=None):
        if user_data is None:
            user_data = self.get(obj)
        try:
            return getattr(user_data, '_sa_adapter')
        except AttributeError:
            collections.CollectionAdapter(self, obj, user_data)
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
            getattr(oldchild.__class__, self.key).remove(oldchild, obj, initiator)
        if child is not None:
            getattr(child.__class__, self.key).append(child, obj, initiator)

    def append(self, obj, child, initiator):
        getattr(child.__class__, self.key).append(child, obj, initiator)

    def remove(self, obj, child, initiator):
        getattr(child.__class__, self.key).remove(child, obj, initiator)

class CommittedState(object):
    """Store the original state of an object when the ``commit()`
    method on the attribute manager is called.
    """


    def __init__(self, manager, obj):
        self.data = {}
        for attr in manager.managed_attributes(obj.__class__):
            attr.commit_to_state(self, obj)

    def rollback(self, manager, obj):
        for attr in manager.managed_attributes(obj.__class__):
            if attr.key in self.data:
                if not isinstance(attr, InstrumentedCollectionAttribute):
                    obj.__dict__[attr.key] = self.data[attr.key]
                else:
                    collection = attr._get_collection(obj)
                    collection.clear_without_event()
                    for item in self.data[attr.key]:
                        collection.append_without_event(item)
            else:
                if attr.key in obj.__dict__:
                    del obj.__dict__[attr.key]

    def __repr__(self):
        return "CommittedState: %s" % repr(self.data)

class AttributeHistory(object):
    """Calculate the *history* of a particular attribute on a
    particular instance, based on the ``CommittedState`` associated
    with the instance, if any.
    """

    def __init__(self, attr, obj, current, passive=False):
        self.attr = attr

        # get the "original" value.  if a lazy load was fired when we got
        # the 'current' value, this "original" was also populated just
        # now as well (therefore we have to get it second)
        orig = obj._state.get('original', None)
        if orig is not None:
            original = orig.data.get(attr.key)
        else:
            original = None

        if isinstance(attr, InstrumentedCollectionAttribute):
            self._current = current
            s = util.Set(original or [])
            self._added_items = []
            self._unchanged_items = []
            self._deleted_items = []
            if current:
                collection = attr._get_collection(obj, current)
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
            orig = o._state.get('original')
            if orig is not None:
                orig.rollback(self, o)
            else:
                self._clear(o)

    def _clear(self, obj):
        for attr in self.managed_attributes(obj.__class__):
            try:
                del obj.__dict__[attr.key]
            except KeyError:
                pass

    def commit(self, *obj):
        """Create a ``CommittedState`` instance for each object in the given list, representing
        its *unchanged* state, and associates it with the instance.

        ``AttributeHistory`` objects will indicate the modified state of
        instance attributes as compared to its value in this
        ``CommittedState`` object.
        """

        for o in obj:
            o._state['original'] = CommittedState(self, o)
            o._state['modified'] = False

    def managed_attributes(self, class_):
        """Return a list of all ``InstrumentedAttribute`` objects
        associated with the given class.
        """

        try:
            return self._inherited_attribute_cache[class_]
        except KeyError:
            if not isinstance(class_, type):
                raise TypeError(repr(class_) + " is not a type")
            inherited = [v for v in [getattr(class_, key, None) for key in dir(class_)] if isinstance(v, InstrumentedAttribute)]
            self._inherited_attribute_cache[class_] = inherited
            return inherited

    def noninherited_managed_attributes(self, class_):
        try:
            return self._noninherited_attribute_cache[class_]
        except KeyError:
            if not isinstance(class_, type):
                raise TypeError(repr(class_) + " is not a type")
            noninherited = [v for v in [getattr(class_, key, None) for key in list(class_.__dict__)] if isinstance(v, InstrumentedAttribute)]
            self._noninherited_attribute_cache[class_] = noninherited
            return noninherited

    def is_modified(self, object):
        for attr in self.managed_attributes(object.__class__):
            if attr.check_mutable_modified(object):
                return True
        return object._state.get('modified', False)

    def get_history(self, obj, key, **kwargs):
        """Return a new ``AttributeHistory`` object for the given
        attribute on the given object.
        """

        return getattr(obj.__class__, key).get_history(obj, **kwargs)

    def get_as_list(self, obj, key, passive=False):
        """Return an attribute of the given name from the given object.

        If the attribute is a scalar, return it as a single-item list,
        otherwise return a collection based attribute.

        If the attribute's value is to be produced by an unexecuted
        callable, the callable will only be executed if the given
        `passive` flag is False.
        """

        attr = getattr(obj.__class__, key)
        x = attr.get(obj, passive=passive)
        if x is PASSIVE_NORESULT:
            return []
        elif isinstance(attr, InstrumentedCollectionAttribute):
            return list(attr._get_collection(obj, x))
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

        self._clear(obj)
        try:
            del obj._state['original']
        except KeyError:
            pass
        obj._state['trigger'] = callable

    def untrigger_history(self, obj):
        """Remove a trigger function set by trigger_history.

        Does not restore the previous state of the object.
        """

        del obj._state['trigger']

    def has_trigger(self, obj):
        """Return True if the given object has a trigger function set
        by ``trigger_history()``.
        """

        return 'trigger' in obj._state

    def reset_instance_attribute(self, obj, key):
        """Remove any per-instance callable functions corresponding to
        given attribute `key` from the given object, and remove this
        attribute from the given object's dictionary.
        """

        attr = getattr(obj.__class__, key)
        attr.reset(obj)

    def reset_class_managed(self, class_):
        """Remove all ``InstrumentedAttribute`` property objects from
        the given class.
        """

        for attr in self.noninherited_managed_attributes(class_):
            delattr(class_, attr.key)
        self._inherited_attribute_cache.pop(class_,None)
        self._noninherited_attribute_cache.pop(class_,None)

    def is_class_managed(self, class_, key):
        """Return True if the given `key` correponds to an
        instrumented property on the given class.
        """
        return hasattr(class_, key) and isinstance(getattr(class_, key), InstrumentedAttribute)

    def init_instance_attribute(self, obj, key, callable_=None, clear=False):
        """Initialize an attribute on an instance to either a blank
        value, cancelling out any class- or instance-level callables
        that were present, or if a `callable` is supplied set the
        callable to be invoked when the attribute is next accessed.
        """

        getattr(obj.__class__, key).set_callable(obj, callable_, clear=clear)

    def create_prop(self, class_, key, uselist, callable_, typecallable, **kwargs):
        """Create a scalar property object, defaulting to
        ``InstrumentedAttribute``, which will communicate change
        events back to this ``AttributeManager``.
        """
        
        if kwargs.pop('dynamic', False):
            from sqlalchemy.orm import dynamic
            return dynamic.DynamicCollectionAttribute(class_, self, key, typecallable, **kwargs)
        elif uselist:
            return InstrumentedCollectionAttribute(class_, self, key,
                                                   callable_,
                                                   typecallable,
                                                   **kwargs)
        else:
            return InstrumentedScalarAttribute(class_, self, key, callable_,
                                               **kwargs)

    def get_attribute(self, obj_or_cls, key):
        """Register an attribute at the class level to be instrumented
        for all instances of the class.
        """

        if isinstance(obj_or_cls, type):
            return getattr(obj_or_cls, key)
        else:
            return getattr(obj_or_cls.__class__, key)

    def register_attribute(self, class_, key, uselist, callable_=None, **kwargs):
        """Register an attribute at the class level to be instrumented
        for all instances of the class.
        """

        # firt invalidate the cache for the given class
        # (will be reconstituted as needed, while getting managed attributes)
        self._inherited_attribute_cache.pop(class_, None)
        self._noninherited_attribute_cache.pop(class_, None)

        if not hasattr(class_, '_state'):
            def _get_state(self):
                if not hasattr(self, '_sa_attr_state'):
                    self._sa_attr_state = {}
                return self._sa_attr_state
            class_._state = property(_get_state)

        typecallable = kwargs.pop('typecallable', None)
        if isinstance(typecallable, InstrumentedAttribute):
            typecallable = None
        setattr(class_, key, self.create_prop(class_, key, uselist, callable_,
                                           typecallable=typecallable, **kwargs))

    def init_collection(self, instance, key):
        """Initialize a collection attribute and return the collection adapter."""

        attr = self.get_attribute(instance, key)
        user_data = attr.initialize(instance)
        return attr._get_collection(instance, user_data)
