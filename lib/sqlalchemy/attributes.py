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
from exceptions import *

class SmartProperty(object):
    """Provides a property object that will communicate set/get/delete operations
    to an AttributeManager.  SmartProperty objects are constructed by the 
    create_prop method on AttributeManger, which can be overridden to provide
    subclasses of SmartProperty.
    """
    def __init__(self, manager, key, uselist):
        self.manager = manager
        self.key = key
        self.uselist = uselist
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
        
class PropHistory(object):
    """Used by AttributeManager to track the history of a scalar attribute
    on an object instance.  This is the "scalar history container" object.
    Has an interface similar to util.HistoryList
    so that the two objects can be called upon largely interchangeably."""
    # make our own NONE to distinguish from "None"
    NONE = object()
    def __init__(self, obj, key, extension=None, **kwargs):
        self.obj = obj
        self.key = key
        self.orig = PropHistory.NONE
        self.extension = extension
    def gethistory(self, *args, **kwargs):
        return self
    def clear(self):
        del self.obj.__dict__[self.key]
    def history_contains(self, obj):
        return self.orig is obj or self.obj.__dict__[self.key] is obj
    def setattr_clean(self, value):
        self.obj.__dict__[self.key] = value
    def delattr_clean(self):
        del self.obj.__dict__[self.key]
    def getattr(self):
        return self.obj.__dict__[self.key]
    def setattr(self, value):
        if isinstance(value, list):
            raise InvalidRequestError("assigning a list to scalar property '%s' on '%s' instance %d" % (self.key, self.obj.__class__.__name__, id(self.obj)))
        orig = self.obj.__dict__.get(self.key, None)
        if orig is value:
            return
        if self.orig is PropHistory.NONE:
            self.orig = orig
        self.obj.__dict__[self.key] = value
        if self.extension is not None:
            self.extension.set(self.obj, value, orig)
    def delattr(self):
        orig = self.obj.__dict__.get(self.key, None)
        if self.orig is PropHistory.NONE:
            self.orig = orig
        self.obj.__dict__[self.key] = None
        if self.extension is not None:
            self.extension.set(self.obj, None, orig)
    def append(self, obj):
        self.setattr(obj)
    def remove(self, obj):
        self.delattr()
    def rollback(self):
        if self.orig is not PropHistory.NONE:
            self.obj.__dict__[self.key] = self.orig
            self.orig = PropHistory.NONE
    def commit(self):
        self.orig = PropHistory.NONE
    def added_items(self):
        if self.orig is not PropHistory.NONE:
            return [self.obj.__dict__[self.key]]
        else:
            return []
    def deleted_items(self):
        if self.orig is not PropHistory.NONE and self.orig is not None:
            return [self.orig]
        else:
            return []
    def unchanged_items(self):
        if self.orig is PropHistory.NONE:
            return [self.obj.__dict__[self.key]]
        else:
            return []

class ListElement(util.HistoryArraySet):
    """Used by AttributeManager to track the history of a list-based object attribute.
    This is the "list history container" object.
    Subclasses util.HistoryArraySet to provide "onchange" event handling as well
    as a plugin point for BackrefExtension objects."""
    def __init__(self, obj, key, data=None, extension=None, **kwargs):
        self.obj = obj
        self.key = key
        self.extension = extension
        # if we are given a list, try to behave nicely with an existing
        # list that might be set on the object already
        try:
            list_ = obj.__dict__[key]
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

    def gethistory(self, *args, **kwargs):
        return self
    def list_value_changed(self, obj, key, item, listval, isdelete):
        pass    
    def setattr(self, value):
        self.obj.__dict__[self.key] = value
        self.set_data(value)
    def delattr(self, value):
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

class CallableProp(object):
    """Used by AttributeManager to allow the attaching of a callable item, representing the future value
    of a particular attribute on a particular object instance, to an attribute on an object. 
    This is the "callable history container" object.
    When the attributemanager first accesses the object attribute, either to get its history or 
    its real value, the __call__ method
    is invoked which runs the underlying callable_ and sets the new value to the object attribute, 
    at which point the CallableProp itself is dereferenced."""
    def __init__(self, manager, callable_, obj, key, uselist = False, live = False, **kwargs):
        self.manager = manager
        self.callable_ = callable_
        self.obj = obj
        self.key = key
        self.uselist = uselist
        self.live = live
        self.kwargs = kwargs

    def gethistory(self, passive=False, *args, **kwargs):
        if not self.uselist:
            if self.obj.__dict__.get(self.key, None) is None:
                if passive:
                    value = None
                else:
                    value = self.callable_()
                self.obj.__dict__[self.key] = value

            p = PropHistory(self.obj, self.key, **self.kwargs)
        else:
            if self.live or not self.obj.__dict__.has_key(self.key) or len(self.obj.__dict__[self.key]) == 0:
                if passive:
                    value =  None
                else:
                    value = self.callable_()
            else:
                value = None
            p = self.manager.create_list(self.obj, self.key, value, readonly=self.live, **self.kwargs)
        
        if not self.live:
            # set the new history list as the new attribute, discards ourself
            self.manager.attribute_history(self.obj)[self.key] = p
            self.manager = None
            # unless we are "live", in which case we stay around to execute again
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
        
    def create_prop(self, class_, key, uselist, **kwargs):
        """creates a scalar property object, defaulting to SmartProperty, which 
        will communicate change events back to this AttributeManager."""
        return SmartProperty(self, key, uselist)
    def create_list(self, obj, key, list_, **kwargs):
        """creates a history-aware list property, defaulting to a ListElement which
        is a subclass of HistoryArrayList."""
        return ListElement(obj, key, list_, **kwargs)
    def create_callable(self, obj, key, func, uselist, **kwargs):
        """creates a callable container that will invoke a function the first
        time an object property is accessed.  The return value of the function
        will become the object property's new value."""
        return CallableProp(self, func, obj, key, uselist, **kwargs)
        
    def get_attribute(self, obj, key, **kwargs):
        """returns the value of an object's scalar attribute, or None if
        its not defined on the object (since we are a property accessor, this
        is considered more appropriate than raising AttributeError)."""
        h = self.get_history(obj, key, **kwargs)
        try:
            return h.getattr()
        except KeyError:
            return None

    def get_list_attribute(self, obj, key, **kwargs):
        """returns the value of an object's list-based attribute."""
        return self.get_history(obj, key, **kwargs)
        
    def set_attribute(self, obj, key, value, **kwargs):
        """sets the value of an object's attribute."""
        self.get_history(obj, key, **kwargs).setattr(value)
        self.value_changed(obj, key, value)
    
    def delete_attribute(self, obj, key, **kwargs):
        """deletes the value from an object's attribute."""
        self.get_history(obj, key, **kwargs).delattr()
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

    def create_history(self, obj, key, uselist, callable_=None, **kwargs):
        """creates a new "history" container for a specific attribute on the given object.  
        this can be used to override a class-level attribute with something different,
        such as a callable. """
        p = self.create_history_container(obj, key, uselist, callable_=callable_, **kwargs)
        self.attribute_history(obj)[key] = p
        return p

    def get_history(self, obj, key, passive=False, **kwargs):
        """returns the "history" container for the given attribute on the given object.
        If the container does not exist, it will be created based on the class-level
        history container definition."""
        try:
            return self.attribute_history(obj)[key].gethistory(passive=passive, **kwargs)
        except KeyError, e:
            return self.class_managed(obj.__class__)[key](obj, **kwargs).gethistory(passive=passive, **kwargs)

    def attribute_history(self, obj):
        """returns a dictionary of "history" containers corresponding to the given object.
        this dictionary is attached to the object via the attribute '_managed_attributes'.
        If the dictionary does not exist, it will be created."""
        try:
            attr = obj.__dict__['_managed_attributes']
        except KeyError:
            trigger = obj.__dict__.pop('_managed_trigger', None)
            if trigger:
                trigger()
            attr = {}
            obj.__dict__['_managed_attributes'] = attr
        return attr

    def trigger_history(self, obj, callable):
        try:
            del obj.__dict__['_managed_attributes']
        except KeyError:
            pass
        obj.__dict__['_managed_trigger'] = callable
        
    def reset_history(self, obj, key):
        """removes the history object for the given attribute on the given object.
        When the attribute is next accessed, a new container will be created via the
        class-level history container definition."""
        try:
            x = self.attribute_history(obj)[key]
            x.clear()
            del self.attribute_history(obj)[key]
        except KeyError:
            pass
        
    def class_managed(self, class_):
        """returns a dictionary of "history container definitions", which is attached to a 
        class.  creates the dictionary if it doesnt exist."""
        try:
            attr = getattr(class_, '_class_managed_attributes')
        except AttributeError:
            attr = {}
            class_._class_managed_attributes = attr
            class_._attribute_manager = self
        return attr

    def reset_class_managed(self, class_):
        try:
            attr = getattr(class_, '_class_managed_attributes')
            for key in attr.keys():
                delattr(class_, key)
            delattr(class_, '_class_managed_attributes')
        except AttributeError:
            pass

    def is_class_managed(self, class_, key):
        try:
            return class_._class_managed_attributes.has_key(key)
        except AttributeError:
            return False
            
    def create_history_container(self, obj, key, uselist, callable_ = None, **kwargs):
        """creates a new history container for the given attribute on the given object."""
        if callable_ is not None:
            return self.create_callable(obj, key, callable_, uselist=uselist, **kwargs)
        elif not uselist:
            return PropHistory(obj, key, **kwargs)
        else:
            list_ = obj.__dict__.get(key, None)
            return self.create_list(obj, key, list_, **kwargs)
        
    def register_attribute(self, class_, key, uselist, callable_=None, **kwargs):
        """registers an attribute's behavior at the class level.  This attribute
        can be scalar or list based, and also may have a callable unit that will be
        used to create the initial value.  The definition for this attribute is 
        wrapped up into a callable which is then stored in the classes' 
        dictionary of "class managed" attributes.  When instances of the class 
        are created and the attribute first referenced, the callable is invoked with
        the new object instance as an argument to create the new history container.  
        Extra keyword arguments can be sent which
        will be passed along to newly created history containers."""
        def createprop(obj):
            if callable_ is not None: 
                func = callable_(obj)
            else:
                func = None
            p = self.create_history_container(obj, key, uselist, callable_=func, **kwargs)
            self.attribute_history(obj)[key] = p
            return p
        
        self.class_managed(class_)[key] = createprop
        setattr(class_, key, self.create_prop(class_, key, uselist))

