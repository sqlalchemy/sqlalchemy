# attributes.py - manages object attributes
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import util
from sqlalchemy.orm import util as orm_util
from sqlalchemy import logging, exceptions
import weakref

class InstrumentedAttribute(object):
    """A property object that instruments attribute access on object instances.

    All methods correspond to a single attribute on a particular
    class.
    """

    PASSIVE_NORESULT = object()

    def __init__(self, manager, key, uselist, callable_, typecallable, trackparent=False, extension=None, copy_function=None, compare_function=None, mutable_scalars=False, **kwargs):
        self.manager = manager
        self.key = key
        self.uselist = uselist
        self.callable_ = callable_
        self.typecallable= typecallable
        self.trackparent = trackparent
        self.mutable_scalars = mutable_scalars
        if copy_function is None:
            if uselist:
                self.copy = lambda x:[y for y in x]
            else:
                # scalar values are assumed to be immutable unless a copy function
                # is passed
                self.copy = lambda x:x
        else:
            self.copy = lambda x:copy_function(x)
        if compare_function is None:
            self.is_equal = lambda x,y: x == y
        else:
            self.is_equal = compare_function
        self.extensions = util.to_list(extension or [])

    def __set__(self, obj, value):
        self.set(None, obj, value)

    def __delete__(self, obj):
        self.delete(None, obj)

    def __get__(self, obj, owner):
        if obj is None:
            return self
        return self.get(obj)

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
        current = self.get(obj, passive=passive, raiseerr=False)
        if current is InstrumentedAttribute.PASSIVE_NORESULT:
            return None
        return AttributeHistory(self, obj, current, passive=passive)

    def set_callable(self, obj, callable_):
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
            self.initialize(obj)
        else:
            obj._state[('callable', self)] = callable_

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

    def _get_callable(self, obj):
        if obj._state.has_key(('callable', self)):
            return obj._state[('callable', self)]
        elif self.callable_ is not None:
            return self.callable_(obj)
        else:
            return None

    def _blank_list(self):
        if self.typecallable is not None:
            return self.typecallable()
        else:
            return []

    def initialize(self, obj):
        """Initialize this attribute on the given object instance.

        If this is a list-based attribute, a new, blank list will be
        created.  if a scalar attribute, the value will be initialized
        to None.
        """

        if self.uselist:
            l = InstrumentedList(self, obj, self._blank_list())
            obj.__dict__[self.key] = l
            return l
        else:
            obj.__dict__[self.key] = None
            return None

    def get(self, obj, passive=False, raiseerr=True):
        """Retrieve a value from the given object.

        If a callable is assembled on this object's attribute, and
        passive is False, the callable will be executed and the
        resulting value will be set as the new value for this
        attribute.
        """

        try:
            return obj.__dict__[self.key]
        except KeyError:
            state = obj._state
            # if an instance-wide "trigger" was set, call that
            # and start again
            if state.has_key('trigger'):
                trig = state['trigger']
                del state['trigger']
                trig()
                return self.get(obj, passive=passive, raiseerr=raiseerr)

            if self.uselist:
                callable_ = self._get_callable(obj)
                if callable_ is not None:
                    if passive:
                        return InstrumentedAttribute.PASSIVE_NORESULT
                    self.logger.debug("Executing lazy callable on %s.%s" % (orm_util.instance_str(obj), self.key))
                    values = callable_()
                    l = InstrumentedList(self, obj, values, init=False)

                    # if a callable was executed, then its part of the "committed state"
                    # if any, so commit the newly loaded data
                    orig = state.get('original', None)
                    if orig is not None:
                        orig.commit_attribute(self, obj, l)

                else:
                    # note that we arent raising AttributeErrors, just creating a new
                    # blank list and setting it.
                    # this might be a good thing to be changeable by options.
                    l = InstrumentedList(self, obj, self._blank_list(), init=False)
                obj.__dict__[self.key] = l
                return l
            else:
                callable_ = self._get_callable(obj)
                if callable_ is not None:
                    if passive:
                        return InstrumentedAttribute.PASSIVE_NORESULT
                    self.logger.debug("Executing lazy callable on %s.%s" % (orm_util.instance_str(obj), self.key))
                    value = callable_()
                    obj.__dict__[self.key] = value

                    # if a callable was executed, then its part of the "committed state"
                    # if any, so commit the newly loaded data
                    orig = state.get('original', None)
                    if orig is not None:
                        orig.commit_attribute(self, obj)
                    return value
                else:
                    # note that we arent raising AttributeErrors, just returning None.
                    # this might be a good thing to be changeable by options.
                    return None

    def set(self, event, obj, value):
        """Set a value on the given object.

        `event` is the ``InstrumentedAttribute`` that initiated the
        ``set()` operation and is used to control the depth of a
        circular setter operation.
        """

        if event is not self:
            state = obj._state
            # if an instance-wide "trigger" was set, call that
            if state.has_key('trigger'):
                trig = state['trigger']
                del state['trigger']
                trig()
            if self.uselist:
                value = InstrumentedList(self, obj, value)
            old = self.get(obj)
            obj.__dict__[self.key] = value
            state['modified'] = True
            if not self.uselist:
                if self.trackparent:
                    if value is not None:
                        self.sethasparent(value, True)
                    if old is not None:
                        self.sethasparent(old, False)
                for ext in self.extensions:
                    ext.set(event or self, obj, value, old)
            else:
                # mark all the old elements as detached from the parent
                old.list_replaced()

    def delete(self, event, obj):
        """Delete a value from the given object.

        `event` is the ``InstrumentedAttribute`` that initiated the
        ``delete()`` operation and is used to control the depth of a
        circular delete operation.
        """

        if event is not self:
            try:
                if not self.uselist and (self.trackparent or len(self.extensions)):
                    old = self.get(obj)
                del obj.__dict__[self.key]
            except KeyError:
                # TODO: raise this?  not consistent with get() ?
                raise AttributeError(self.key)
            obj._state['modified'] = True
            if not self.uselist:
                if self.trackparent:
                    if old is not None:
                        self.sethasparent(old, False)
                for ext in self.extensions:
                    ext.delete(event or self, obj, old)

    def append(self, event, obj, value):
        """Append an element to a list based element or sets a scalar
        based element to the given value.

        Used by ``GenericBackrefExtension`` to *append* an item
        independent of list/scalar semantics.

        `event` is the ``InstrumentedAttribute`` that initiated the
        ``append()`` operation and is used to control the depth of a
        circular append operation.
        """

        if self.uselist:
            if event is not self:
                self.get(obj).append_with_event(value, event)
        else:
            self.set(event, obj, value)

    def remove(self, event, obj, value):
        """Remove an element from a list based element or sets a
        scalar based element to None.

        Used by ``GenericBackrefExtension`` to *remove* an item
        independent of list/scalar semantics.

        `event` is the ``InstrumentedAttribute`` that initiated the
        ``remove()`` operation and is used to control the depth of a
        circular remove operation.
        """

        if self.uselist:
            if event is not self:
                self.get(obj).remove_with_event(value, event)
        else:
            self.set(event, obj, None)

    def append_event(self, event, obj, value):
        """Called by ``InstrumentedList`` when an item is appended."""

        obj._state['modified'] = True
        if self.trackparent and value is not None:
            self.sethasparent(value, True)
        for ext in self.extensions:
            ext.append(event or self, obj, value)

    def remove_event(self, event, obj, value):
        """Called by ``InstrumentedList`` when an item is removed."""

        obj._state['modified'] = True
        if self.trackparent and value is not None:
            self.sethasparent(value, False)
        for ext in self.extensions:
            ext.delete(event or self, obj, value)

InstrumentedAttribute.logger = logging.class_logger(InstrumentedAttribute)

    
class InstrumentedList(object):
    """Instrument a list-based attribute.

    All mutator operations (i.e. append, remove, etc.) will fire off
    events to the ``InstrumentedAttribute`` that manages the object's
    attribute.  Those events in turn trigger things like backref
    operations and whatever is implemented by
    ``do_list_value_changed`` on ``InstrumentedAttribute``.

    Note that this list does a lot less than earlier versions of SA
    list-based attributes, which used ``HistoryArraySet``.  This list
    wrapper does **not** maintain setlike semantics, meaning you can add
    as many duplicates as you want (which can break a lot of SQL), and
    also does not do anything related to history tracking.

    Please see ticket #213 for information on the future of this
    class, where it will be broken out into more collection-specific
    subtypes.
    """

    def __init__(self, attr, obj, data, init=True):
        self.attr = attr
        # this weakref is to prevent circular references between the parent object
        # and the list attribute, which interferes with immediate garbage collection.
        self.__obj = weakref.ref(obj)
        self.key = attr.key

        # adapt to lists or sets
        # TODO: make three subclasses of InstrumentedList that come off from a
        # metaclass, based on the type of data sent in
        if attr.typecallable is not None:
            self.data = attr.typecallable()
        else:
            self.data = data or attr._blank_list()
        
        if isinstance(self.data, list):
            self._data_appender = self.data.append
            self._clear_data = self._clear_list
        elif isinstance(self.data, util.Set):
            self._data_appender = self.data.add
            self._clear_data = self._clear_set
        elif isinstance(self.data, dict):
            if hasattr(self.data, 'append'):
                self._data_appender = self.data.append
            else:
                raise exceptions.ArgumentError("Dictionary collection class '%s' must implement an append() method" % type(self.data).__name__)
            self._clear_data = self._clear_dict
        else:
            if hasattr(self.data, 'append'):
                self._data_appender = self.data.append
            elif hasattr(self.data, 'add'):
                self._data_appender = self.data.add
            else:
                raise exceptions.ArgumentError("Collection class '%s' is not of type 'list', 'set', or 'dict' and has no append() or add() method" % type(self.data).__name__)

            if hasattr(self.data, 'clear'):
                self._clear_data = self._clear_set
            else:
                raise exceptions.ArgumentError("Collection class '%s' is not of type 'list', 'set', or 'dict' and has no clear() method" % type(self.data).__name__)
            
        if data is not None and data is not self.data:
            for elem in data:
                self._data_appender(elem)
                

        if init:
            for x in self.data:
                self.__setrecord(x)

    def list_replaced(self):
        """Fire off delete event handlers for each item in the list
        but doesnt affect the original data list.
        """

        [self.__delrecord(x) for x in self.data]

    def clear(self):
        """Clear all items in this InstrumentedList and fires off
        delete event handlers for each item.
        """

        self._clear_data()

    def _clear_dict(self):
        [self.__delrecord(x) for x in self.data.values()]
        self.data.clear()

    def _clear_set(self):
        [self.__delrecord(x) for x in self.data]
        self.data.clear()

    def _clear_list(self):
        self[:] = []

    def __getstate__(self):
        """Implemented to allow pickling, since `__obj` is a weakref,
        also the ``InstrumentedAttribute`` has callables attached to
        it.
        """

        return {'key':self.key, 'obj':self.obj, 'data':self.data}

    def __setstate__(self, d):
        """Implemented to allow pickling, since `__obj` is a weakref,
        also the ``InstrumentedAttribute`` has callables attached to it.
        """

        self.key = d['key']
        self.__obj = weakref.ref(d['obj'])
        self.data = d['data']
        self.attr = getattr(d['obj'].__class__, self.key)

    obj = property(lambda s:s.__obj())

    def unchanged_items(self):
        """Deprecated."""

        return self.attr.get_history(self.obj).unchanged_items

    def added_items(self):
        """Deprecated."""

        return self.attr.get_history(self.obj).added_items

    def deleted_items(self):
        """Deprecated."""

        return self.attr.get_history(self.obj).deleted_items

    def __iter__(self):
        return iter(self.data)

    def __repr__(self):
        return repr(self.data)

    def __getattr__(self, attr):
        """Proxy unknown methods and attributes to the underlying
        data array.  This allows custom list classes to be used.
        """

        return getattr(self.data, attr)

    def __setrecord(self, item, event=None):
        self.attr.append_event(event, self.obj, item)
        return True

    def __delrecord(self, item, event=None):
        self.attr.remove_event(event, self.obj, item)
        return True

    def append_with_event(self, item, event):
        self.__setrecord(item, event)
        self._data_appender(item)

    def append_without_event(self, item):
        self._data_appender(item)

    def remove_with_event(self, item, event):
        self.__delrecord(item, event)
        self.data.remove(item)

    def append(self, item, _mapper_nohistory=False):
        """Fire off dependent events, and appends the given item to the underlying list.

        `_mapper_nohistory` is a backwards compatibility hack; call
        ``append_without_event`` instead.
        """

        if _mapper_nohistory:
            self.append_without_event(item)
        else:
            self.__setrecord(item)
            self._data_appender(item)

    def __getitem__(self, i):
        return self.data[i]

    def __setitem__(self, i, item):
        if isinstance(i, slice):
            self.__setslice__(i.start, i.stop, item)
        else:
            self.__setrecord(item)
            self.data[i] = item

    def __delitem__(self, i):
        if isinstance(i, slice):
            self.__delslice__(i.start, i.stop)
        else:
            self.__delrecord(self.data[i], None)
            del self.data[i]

    def __lt__(self, other): return self.data <  self.__cast(other)

    def __le__(self, other): return self.data <= self.__cast(other)

    def __eq__(self, other): return self.data == self.__cast(other)

    def __ne__(self, other): return self.data != self.__cast(other)

    def __gt__(self, other): return self.data >  self.__cast(other)

    def __ge__(self, other): return self.data >= self.__cast(other)

    def __cast(self, other):
       if isinstance(other, InstrumentedList): return other.data
       else: return other

    def __cmp__(self, other):
       return cmp(self.data, self.__cast(other))

    def __contains__(self, item): return item in self.data

    def __len__(self):
        try:
            return len(self.data)
        except TypeError:
            return len(list(self.data))

    def __setslice__(self, i, j, other):
        [self.__delrecord(x) for x in self.data[i:j]]
        g = [a for a in list(other) if self.__setrecord(a)]
        self.data[i:j] = g

    def __delslice__(self, i, j):
        for a in self.data[i:j]:
            self.__delrecord(a)
        del self.data[i:j]

    def insert(self, i, item):
        if self.__setrecord(item):
            self.data.insert(i, item)

    def pop(self, i=-1):
        item = self.data[i]
        self.__delrecord(item)
        return self.data.pop(i)

    def remove(self, item):
        self.__delrecord(item)
        self.data.remove(item)

    def discard(self, item):
        if item in self.data:
            self.__delrecord(item)
            self.data.remove(item)

    def extend(self, item_list):
        for item in item_list:
            self.append(item)

    def __add__(self, other):
        raise NotImplementedError()

    def __radd__(self, other):
        raise NotImplementedError()

    def __iadd__(self, other):
        raise NotImplementedError()

class AttributeExtension(object):
    """An abstract class which specifies `append`, `delete`, and `set`
    event handlers to be attached to an object property.
    """

    def append(self, event, obj, child):
        pass

    def delete(self, event, obj, child):
        pass

    def set(self, event, obj, child, oldchild):
        pass

class GenericBackrefExtension(AttributeExtension):
    """An extension which synchronizes a two-way relationship.

    A typical two-way relationship is a parent object containing a
    list of child objects, where each child object references the
    parent.  The other are two objects which contain scalar references
    to each other.
    """

    def __init__(self, key):
        self.key = key

    def set(self, event, obj, child, oldchild):
        if oldchild is child:
            return
        if oldchild is not None:
            getattr(oldchild.__class__, self.key).remove(event, oldchild, obj)
        if child is not None:
            getattr(child.__class__, self.key).append(event, child, obj)

    def append(self, event, obj, child):
        getattr(child.__class__, self.key).append(event, child, obj)

    def delete(self, event, obj, child):
        getattr(child.__class__, self.key).remove(event, child, obj)

class CommittedState(object):
    """Store the original state of an object when the ``commit()`
    method on the attribute manager is called.
    """

    NO_VALUE = object()

    def __init__(self, manager, obj):
        self.data = {}
        for attr in manager.managed_attributes(obj.__class__):
            self.commit_attribute(attr, obj)

    def commit_attribute(self, attr, obj, value=NO_VALUE):
        """Establish the value of attribute `attr` on instance `obj`
        as *committed*.

        This corresponds to a previously saved state being restored.
        """

        if value is CommittedState.NO_VALUE:
            if obj.__dict__.has_key(attr.key):
                value = obj.__dict__[attr.key]
        if value is not CommittedState.NO_VALUE:
            self.data[attr.key] = attr.copy(value)

            # not tracking parent on lazy-loaded instances at the moment.
            # its not needed since they will be "optimistically" tested
            #if attr.uselist:
                #if attr.trackparent:
                #    [attr.sethasparent(x, True) for x in self.data[attr.key] if x is not None]
            #else:
                #if attr.trackparent and value is not None:
                #    attr.sethasparent(value, True)

    def rollback(self, manager, obj):
        for attr in manager.managed_attributes(obj.__class__):
            if self.data.has_key(attr.key):
                if attr.uselist:
                    obj.__dict__[attr.key][:] = self.data[attr.key]
                else:
                    obj.__dict__[attr.key] = self.data[attr.key]
            else:
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

        if attr.uselist:
            self._current = current
        else:
            self._current = [current]
        if attr.uselist:
            s = util.Set(original or [])
            self._added_items = []
            self._unchanged_items = []
            self._deleted_items = []
            if current:
                for a in current:
                    if a in s:
                        self._unchanged_items.append(a)
                    else:
                        self._added_items.append(a)
            for a in s:
                if a not in self._unchanged_items:
                    self._deleted_items.append(a)
        else:
            if attr.is_equal(current, original):
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
        #print "key", attr.key, "orig", original, "current", current, "added", self._added_items, "unchanged", self._unchanged_items, "deleted", self._deleted_items

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

    def hasparent(self, obj):
        """Deprecated.  This should be called directly from the
        appropriate ``InstrumentedAttribute`` object.
        """

        return self.attr.hasparent(obj)

class AttributeManager(object):
    """Allow the instrumentation of object attributes.

    ``AttributeManager`` is stateless, but can be overridden by
    subclasses to redefine some of its factory operations. Also be
    aware ``AttributeManager`` will cache attributes for a given
    class, allowing not to determine those for each objects (used in
    ``managed_attributes()`` and
    ``noninherited_managed_attributes()``). This cache is cleared for
    a given class while calling ``register_attribute()``, and can be
    cleared using ``clear_attribute_cache()``.
    """

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
        """Return an iterator of all ``InstrumentedAttribute`` objects
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

    def init_attr(self, obj):
        """Sets up the __sa_attr_state dictionary on the given instance.

        This dictionary is automatically created when the `_state`
        attribute of the class is first accessed, but calling it here
        will save a single throw of an ``AttributeError`` that occurs
        in that creation step.
        """

        setattr(obj, '_%s__sa_attr_state' % obj.__class__.__name__, {})

    def get_history(self, obj, key, **kwargs):
        """Return a new ``AttributeHistory`` object for the given
        attribute on the given object.
        """

        return getattr(obj.__class__, key).get_history(obj, **kwargs)

    def get_as_list(self, obj, key, passive=False):
        """Return an attribute of the given name from the given object.

        If the attribute is a scalar, return it as a single-item list,
        otherwise return the list based attribute.

        If the attribute's value is to be produced by an unexecuted
        callable, the callable will only be executed if the given
        `passive` flag is False.
        """

        attr = getattr(obj.__class__, key)
        x = attr.get(obj, passive=passive)
        if x is InstrumentedAttribute.PASSIVE_NORESULT:
            return []
        elif attr.uselist:
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

        return obj._state.has_key('trigger')

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

    def init_instance_attribute(self, obj, key, uselist, callable_=None, **kwargs):
        """Initialize an attribute on an instance to either a blank
        value, cancelling out any class- or instance-level callables
        that were present, or if a `callable` is supplied set the
        callable to be invoked when the attribute is next accessed.
        """

        getattr(obj.__class__, key).set_callable(obj, callable_)

    def create_prop(self, class_, key, uselist, callable_, typecallable, **kwargs):
        """Create a scalar property object, defaulting to
        ``InstrumentedAttribute``, which will communicate change
        events back to this ``AttributeManager``.
        """

        return InstrumentedAttribute(self, key, uselist, callable_, typecallable, **kwargs)

    def register_attribute(self, class_, key, uselist, callable_=None, **kwargs):
        """Register an attribute at the class level to be instrumented
        for all instances of the class.
        """

        # firt invalidate the cache for the given class
        # (will be reconstituted as needed, while getting managed attributes)
        self._inherited_attribute_cache.pop(class_,None)
        self._noninherited_attribute_cache.pop(class_,None)

        #print self, "register attribute", key, "for class", class_
        if not hasattr(class_, '_state'):
            def _get_state(self):
                if not hasattr(self, '_sa_attr_state'):
                    self._sa_attr_state = {}
                return self._sa_attr_state
            class_._state = property(_get_state)

        typecallable = kwargs.pop('typecallable', None)
        if isinstance(typecallable, InstrumentedAttribute):
            typecallable = None
        setattr(class_, key, self.create_prop(class_, key, uselist, callable_, typecallable=typecallable, **kwargs))
