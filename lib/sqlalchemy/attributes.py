# attributes.py - manages object attributes
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""provides a class called AttributeManager that can attach history-aware attributes to object
instances.  AttributeManager-enabled object attributes can be scalar or lists.  In both cases, the "change
history" of each attribute is available via the AttributeManager in a unit called a "history
container".  Via the history container, which can be a scalar or list based container, 
the attribute can be "committed", meaning whatever changes it has are registered as the current value, 
or "rolled back", which means the original "committed" value is restored; in both cases
the accumulated history is removed.

The change history is represented as three lists, the "added items", the "deleted items", 
and the "unchanged items".  In the case of a scalar attribute, these lists would be zero or 
one element in length.  for a list based attribute, these lists are of arbitrary length.  
"unchanged items" represents the assigned value or appended values on the attribute either 
with "history tracking" disabled, or have been "committed".  "added items" represent new
values that have been assigned or appended to the attribute.  "deleted items" represents the 
the value that was previously "unchanged", but has been de-assigned or removed from the attribute.

AttributeManager can also assign a "callable" history container to an object's attribute, 
which is invoked when first accessed, to provide the object's "committed" value.  

The package includes functions for managing "bi-directional" object relationships as well
via the GenericBackrefExtension object.
"""

import util
import weakref
from exceptions import *

class SmartProperty(object):
    """Provides a property object that will communicate set/get/delete operations
    to an AttributeManager.  SmartProperty objects are constructed by the 
    create_prop method on AttributeManger, which can be overridden to provide
    subclasses of SmartProperty.
    """
    def __init__(self, manager, key, uselist, callable_, **kwargs):
        self.manager = manager
        self.key = key
        self.uselist = uselist
        self.callable_ = callable_
        self.kwargs = kwargs
    def init(self, obj, attrhist=None):
        """creates an appropriate ManagedAttribute for the given object and establishes
        it with the object's list of managed attributes."""
        if self.callable_ is not None:
            func = self.callable_(obj)
        else:
            func = None
        return self.manager.create_managed_attribute(obj, self.key, self.uselist, callable_=func, attrdict=attrhist, **self.kwargs)
    def __set__(self, obj, value):
        self.manager.set_attribute(obj, self.key, value)
    def __delete__(self, obj):
        self.manager.delete_attribute(obj, self.key)
    def __get__(self, obj, owner):
        if obj is None:
            return self
        if self.uselist:
            return self.manager.get_list_attribute(obj, self.key)
        else:
            return self.manager.get_attribute(obj, self.key)
    def setattr_clean(self, obj, value):
        """sets an attribute on an object without triggering a history event"""
        h = self.manager.get_history(obj, self.key)
        h.setattr_clean(value)
    def append_clean(self, obj, value):
        """appends a value to a list-based attribute without triggering a history event."""
        h = self.manager.get_history(obj, self.key)
        h.append_nohistory(value)

class ManagedAttribute(object):
    """base class for a "managed attribute", which is attached to individual instances
    of a class mapped to the keyname of the property, inside of a dictionary which is
    attached to the object via the propertyname "_managed_attributes".  Attribute access
    which occurs through the SmartProperty property object ultimately calls upon 
    ManagedAttribute objects associated with the instance via this dictionary."""
    def __init__(self, obj, key):
        #self.__obj = weakref.ref(obj)
        self.obj = obj
        self.key = key
    #obj = property(lambda s:s.__obj())
    def history(self, **kwargs):
        return self
    def plain_init(self, *args, **kwargs):
        pass
        
class ScalarAttribute(ManagedAttribute):
    """Used by AttributeManager to track the history of a scalar attribute
    on an object instance.  This is the "scalar history container" object.
    Has an interface similar to util.HistoryList
    so that the two objects can be called upon largely interchangeably."""
    # make our own NONE to distinguish from "None"
    NONE = object()
    def __init__(self, obj, key, extension=None, **kwargs):
        ManagedAttribute.__init__(self, obj, key)
        self.orig = ScalarAttribute.NONE
        self.extension = extension
    def clear(self):
        del self.obj.__dict__[self.key]
    def history_contains(self, obj):
        return self.orig is obj or self.obj.__dict__[self.key] is obj
    def setattr_clean(self, value):
        self.obj.__dict__[self.key] = value
    def delattr_clean(self):
        del self.obj.__dict__[self.key]
    def getattr(self, **kwargs):
        return self.obj.__dict__[self.key]
    def setattr(self, value, **kwargs):
        #if isinstance(value, list):
        #    raise InvalidRequestError("assigning a list to scalar property '%s' on '%s' instance %d" % (self.key, self.obj.__class__.__name__, id(self.obj)))
        orig = self.obj.__dict__.get(self.key, None)
        if orig is value:
            return
        if self.orig is ScalarAttribute.NONE:
            self.orig = orig
        self.obj.__dict__[self.key] = value
        if self.extension is not None:
            self.extension.set(self.obj, value, orig)
    def delattr(self, **kwargs):
        orig = self.obj.__dict__.get(self.key, None)
        if self.orig is ScalarAttribute.NONE:
            self.orig = orig
        self.obj.__dict__[self.key] = None
        if self.extension is not None:
            self.extension.set(self.obj, None, orig)
    def append(self, obj):
        self.setattr(obj)
    def remove(self, obj):
        self.delattr()
    def rollback(self):
        if self.orig is not ScalarAttribute.NONE:
            self.obj.__dict__[self.key] = self.orig
            self.orig = ScalarAttribute.NONE
    def commit(self):
        self.orig = ScalarAttribute.NONE
    def added_items(self):
        if self.orig is not ScalarAttribute.NONE:
            return [self.obj.__dict__[self.key]]
        else:
            return []
    def deleted_items(self):
        if self.orig is not ScalarAttribute.NONE and self.orig is not None:
            return [self.orig]
        else:
            return []
    def unchanged_items(self):
        if self.orig is ScalarAttribute.NONE:
            return [self.obj.__dict__[self.key]]
        else:
            return []

class ListAttribute(util.HistoryArraySet, ManagedAttribute):
    """Used by AttributeManager to track the history of a list-based object attribute.
    This is the "list history container" object.
    Subclasses util.HistoryArraySet to provide "onchange" event handling as well
    as a plugin point for BackrefExtension objects."""
    def __init__(self, obj, key, data=None, extension=None, **kwargs):
        ManagedAttribute.__init__(self, obj, key)
        self.extension = extension
        # if we are given a list, try to behave nicely with an existing
        # list that might be set on the object already
        try:
            list_ = obj.__dict__[key]
            if list_ is data:
                raise InvalidArgumentError("Creating a list element passing the object's list as an argument")
            if data is not None:
                for d in data:
                    list_.append(d)
        except KeyError:
            if data is not None:
                list_ = data
            else:
                list_ = []
            obj.__dict__[key] = []
            
        util.HistoryArraySet.__init__(self, list_, readonly=kwargs.get('readonly', False))
    def list_value_changed(self, obj, key, item, listval, isdelete):
        pass    
    def setattr(self, value, **kwargs):
        self.obj.__dict__[self.key] = value
        self.set_data(value)
    def delattr(self, value, **kwargs):
        pass
    def _setrecord(self, item):
        res = util.HistoryArraySet._setrecord(self, item)
        if res:
            self.list_value_changed(self.obj, self.key, item, self, False)
            if self.extension is not None:
                self.extension.append(self.obj, item)
        return res
    def _delrecord(self, item):
        res = util.HistoryArraySet._delrecord(self, item)
        if res:
            self.list_value_changed(self.obj, self.key, item, self, True)
            if self.extension is not None:
                self.extension.delete(self.obj, item)
        return res
        
# deprecated
class ListElement(ListAttribute):pass
    
class TriggeredAttribute(ManagedAttribute):
    """Used by AttributeManager to allow the attaching of a callable item, representing the future value
    of a particular attribute on a particular object instance, as the current attribute on an object. 
    When accessed normally, its history() method is invoked to run the underlying callable, which
    is then used to create a new ScalarAttribute or ListAttribute.  This new attribute object 
    is then registered with the attribute manager to replace this TriggeredAttribute as the 
    current ManagedAttribute."""
    def __init__(self, manager, callable_, obj, key, uselist = False, live = False, **kwargs):
        ManagedAttribute.__init__(self, obj, key)
        self.manager = manager
        self.callable_ = callable_
        self.uselist = uselist
        self.kwargs = kwargs

    def clear(self):
        self.plain_init(self.manager.attribute_history(self.obj))
        
    def plain_init(self, attrhist):
        if not self.uselist:
            p = ScalarAttribute(self.obj, self.key, **self.kwargs)
            self.obj.__dict__[self.key] = None
        else:
            p = self.manager.create_list(self.obj, self.key, None, **self.kwargs)
        attrhist[self.key] = p
    
    def __getattr__(self, key):
        def callit(*args, **kwargs):
            passive = kwargs.pop('passive', False)
            return getattr(self.history(passive=passive), key)(*args, **kwargs)
        return callit
    
    def history(self, passive=False):
        if not self.uselist:
            if self.obj.__dict__.get(self.key, None) is None:
                if passive:
                    value = None
                else:
                    try:
                        value = self.callable_()
                    except AttributeError, e:
                        # this catch/raise is because this call is frequently within an 
                        # AttributeError-sensitive callstack
                        raise AssertionError("AttributeError caught in callable prop:" + str(e.args))
                self.obj.__dict__[self.key] = value

            p = ScalarAttribute(self.obj, self.key, **self.kwargs)
        else:
            if not self.obj.__dict__.has_key(self.key) or len(self.obj.__dict__[self.key]) == 0:
                if passive:
                    value =  None
                else:
                    try:
                        value = self.callable_()
                    except AttributeError, e:
                        # this catch/raise is because this call is frequently within an 
                        # AttributeError-sensitive callstack
                        raise AssertionError("AttributeError caught in callable prop:" + str(e.args))
            else:
                value = None
            p = self.manager.create_list(self.obj, self.key, value, **self.kwargs)
        if not passive:
            # set the new history list as the new attribute, discards ourself
            self.manager.attribute_history(self.obj)[self.key] = p
            self.manager = None
        return p

    def commit(self):
        pass
    def rollback(self):
        pass

class AttributeExtension(object):
    """an abstract class which specifies an "onadd" or "ondelete" operation
    to be attached to an object property."""
    def append(self, obj, child):
        pass
    def delete(self, obj, child):
        pass
    def set(self, obj, child, oldchild):
        pass
        
class GenericBackrefExtension(AttributeExtension):
    """an attachment to a ScalarAttribute or ListAttribute which receives change events,
    and upon such an event synchronizes a two-way relationship.  A typical two-way
    relationship is a parent object containing a list of child objects, where each
    child object references the parent.  The other are two objects which contain 
    scalar references to each other."""
    def __init__(self, key):
        self.key = key
    def set(self, obj, child, oldchild):
        if oldchild is not None:
            prop = oldchild.__class__._attribute_manager.get_history(oldchild, self.key)
            prop.remove(obj)
        if child is not None:
            prop = child.__class__._attribute_manager.get_history(child, self.key)
            prop.append(obj)
    def append(self, obj, child):
        prop = child.__class__._attribute_manager.get_history(child, self.key)
        prop.append(obj)
    def delete(self, obj, child):
        prop = child.__class__._attribute_manager.get_history(child, self.key)
        prop.remove(obj)

            
class AttributeManager(object):
    """maintains a set of per-attribute history container objects for a set of objects."""
    def __init__(self):
        pass

    def value_changed(self, obj, key, value):
        """subclasses override this method to provide functionality that is triggered 
        upon an attribute change of value."""
        pass
        
    def create_prop(self, class_, key, uselist, callable_, **kwargs):
        """creates a scalar property object, defaulting to SmartProperty, which 
        will communicate change events back to this AttributeManager."""
        return SmartProperty(self, key, uselist, callable_, **kwargs)
        
    def create_list(self, obj, key, list_, **kwargs):
        """creates a history-aware list property, defaulting to a ListAttribute which
        is a subclass of HistoryArrayList."""
        return ListAttribute(obj, key, list_, **kwargs)
    def create_callable(self, obj, key, func, uselist, **kwargs):
        """creates a callable container that will invoke a function the first
        time an object property is accessed.  The return value of the function
        will become the object property's new value."""
        return TriggeredAttribute(self, func, obj, key, uselist, **kwargs)
        
    def get_attribute(self, obj, key, **kwargs):
        """returns the value of an object's scalar attribute, or None if
        its not defined on the object (since we are a property accessor, this
        is considered more appropriate than raising AttributeError)."""
        h = self.get_unexec_history(obj, key)
        try:
            return h.getattr(**kwargs)
        except KeyError:
            return None

    def get_list_attribute(self, obj, key, **kwargs):
        """returns the value of an object's list-based attribute."""
        return self.get_history(obj, key, **kwargs)
        
    def set_attribute(self, obj, key, value, **kwargs):
        """sets the value of an object's attribute."""
        self.get_unexec_history(obj, key).setattr(value, **kwargs)
        self.value_changed(obj, key, value)
    
    def delete_attribute(self, obj, key, **kwargs):
        """deletes the value from an object's attribute."""
        self.get_unexec_history(obj, key).delattr(**kwargs)
        self.value_changed(obj, key, None)
        
    def rollback(self, *obj):
        """rolls back all attribute changes on the given list of objects, 
        and removes all history."""
        for o in obj:
            try:
                attributes = self.attribute_history(o)
                for hist in attributes.values():
                    hist.rollback()
            except KeyError:
                pass

    def commit(self, *obj):
        """commits all attribute changes on the given list of objects, 
        and removes all history."""
        for o in obj:
            try:
                attributes = self.attribute_history(o)
                for hist in attributes.values():
                    hist.commit()
            except KeyError:
                pass
                
    def remove(self, obj):
        """called when an object is totally being removed from memory"""
        # currently a no-op since the state of the object is attached to the object itself
        pass


    def init_attr(self, obj):
        """sets up the _managed_attributes dictionary on an object.  this happens anyway 
        when a particular attribute is first accessed on the object regardless
        of this method being called, however calling this first will result in an elimination of 
        AttributeError/KeyErrors that are thrown when get_unexec_history is called for the first
        time for a particular key."""
        d = {}
        obj._managed_attributes = d
        for value in obj.__class__.__dict__.values():
            if not isinstance(value, SmartProperty):
                continue
            value.init(obj, attrhist=d).plain_init(d)

    def get_unexec_history(self, obj, key):
        """returns the "history" container for the given attribute on the given object.
        If the container does not exist, it will be created based on the class-level
        history container definition."""
        try:
            return obj._managed_attributes[key]
        except AttributeError, ae:
            return getattr(obj.__class__, key).init(obj)
        except KeyError, e:
            return getattr(obj.__class__, key).init(obj)

    def get_history(self, obj, key, **kwargs):
        """accesses the appropriate ManagedAttribute container and calls its history() method.
        For a TriggeredAttribute this will execute the underlying callable and return the
        resulting ScalarAttribute or ListAttribute object.  For an existing ScalarAttribute
        or ListAttribute, just returns the container."""
        return self.get_unexec_history(obj, key).history(**kwargs)

    def attribute_history(self, obj):
        """returns a dictionary of ManagedAttribute containers corresponding to the given object.
        this dictionary is attached to the object via the attribute '_managed_attributes'.
        If the dictionary does not exist, it will be created.  If a 'trigger' has been placed on 
        this object via the trigger_history() method, it will first be executed."""
        try:
            return obj._managed_attributes
        except AttributeError:
            obj._managed_attributes = {}
            trigger = obj.__dict__.pop('_managed_trigger', None)
            if trigger:
                trigger()
            return obj._managed_attributes

    def trigger_history(self, obj, callable):
        """removes all ManagedAttribute instances from the given object and places the given callable
        as an attribute-wide "trigger", which will execute upon the next attribute access, after
        which the trigger is removed and the object re-initialized to receive new ManagedAttributes. """
        try:
            del obj._managed_attributes
        except KeyError:
            pass
        obj._managed_trigger = callable

    def untrigger_history(self, obj):
        del obj._managed_trigger
        
    def has_trigger(self, obj):
        return hasattr(obj, '_managed_trigger')
            
    def reset_history(self, obj, key):
        """removes the history object for the given attribute on the given object.
        When the attribute is next accessed, a new container will be created via the
        class-level history container definition."""
        try:
            x = self.attribute_history(obj)[key]
            x.clear()
            del self.attribute_history(obj)[key]
        except KeyError:
            try:
                del obj.__dict__[key]
            except KeyError:
                pass
        
    def reset_class_managed(self, class_):
        for value in class_.__dict__.values():
            if not isinstance(value, SmartProperty):
                continue
            delattr(class_, value.key)

    def is_class_managed(self, class_, key):
        return hasattr(class_, key) and isinstance(getattr(class_, key), SmartProperty)

    def create_managed_attribute(self, obj, key, uselist, callable_=None, attrdict=None, **kwargs):
        """creates a new ManagedAttribute corresponding to the given attribute key on the 
        given object instance, and installs it in the attribute dictionary attached to the object."""
        if callable_ is not None:
            prop = self.create_callable(obj, key, callable_, uselist=uselist, **kwargs)
        elif not uselist:
            prop = ScalarAttribute(obj, key, **kwargs)
        else:
            prop = self.create_list(obj, key, None, **kwargs)
        if attrdict is None:
            attrdict = self.attribute_history(obj)
        attrdict[key] = prop
        return prop
    
    # deprecated
    create_history=create_managed_attribute
    
    def register_attribute(self, class_, key, uselist, callable_=None, **kwargs):
        """registers an attribute's behavior at the class level.  This attribute
        can be scalar or list based, and also may have a callable unit that will be
        used to create the initial value (i.e. a lazy loader).  The definition for this attribute is 
        wrapped up into a callable which is then stored in the corresponding
        SmartProperty object attached to the class.  When instances of the class 
        are created and the attribute first referenced, the callable is invoked with
        the new object instance as an argument to create the new ManagedAttribute.  
        Extra keyword arguments can be sent which
        will be passed along to newly created ManagedAttribute."""
        if not hasattr(class_, '_attribute_manager'):
            class_._attribute_manager = self
            class_._managed_attributes = ObjectAttributeGateway()
        setattr(class_, key, self.create_prop(class_, key, uselist, callable_, **kwargs))

managed_attributes = weakref.WeakKeyDictionary()

class ObjectAttributeGateway(object):
    """handles the dictionary of ManagedAttributes for instances.  this level of indirection
    is to prevent circular references upon objects, as well as keeping them Pickle-compatible."""
    def __set__(self, obj, value):
        managed_attributes[obj] = value
    def __delete__(self, obj):
        try:
            del managed_attributes[obj]
        except KeyError:
            raise AttributeError()
    def __get__(self, obj, owner):
        if obj is None:
            return self
        try:
            return managed_attributes[obj]
        except KeyError:
            raise AttributeError()