# attributes.py - manages object attributes
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.


import sqlalchemy.util as util
import weakref

class SmartProperty(object):
    """attaches AttributeManager functionality to the property accessors of a class.  all instances
    of the class will retrieve and modify their properties via an AttributeManager."""
    def __init__(self, manager):
        self.manager = manager
    def attribute_registry(self):
        return self.manager
    def property(self, key, uselist, **kwargs):
        def set_prop(obj, value):
            if uselist:
                self.attribute_registry().set_list_attribute(obj, key, value, **kwargs)
            else:
                self.attribute_registry().set_attribute(obj, key, value, **kwargs)
        def del_prop(obj):
            if uselist:
                # TODO: this probably doesnt work right, deleting the list off an item
                self.attribute_registry().delete_list_attribute(obj, key, **kwargs)
            else:
                self.attribute_registry().delete_attribute(obj, key, **kwargs)
        def get_prop(obj):
            if uselist:
                return self.attribute_registry().get_list_attribute(obj, key, **kwargs)
            else:
                return self.attribute_registry().get_attribute(obj, key, **kwargs)
                
        return property(get_prop, set_prop, del_prop)

class PropHistory(object):
    """manages the value of a particular scalar attribute on a particular object instance."""
    # make our own NONE to distinguish from "None"
    NONE = object()
    def __init__(self, obj, key, **kwargs):
        self.obj = obj
        self.key = key
        self.orig = PropHistory.NONE
    def gethistory(self, *args, **kwargs):
        return self
    def __call__(self, *args, **kwargs):
        return self.obj.__dict__[self.key]
    def history_contains(self, obj):
        return self.orig is obj or self.obj.__dict__[self.key] is obj
    def setattr_clean(self, value):
        self.obj.__dict__[self.key] = value
    def setattr(self, value):
        if isinstance(value, list):
            raise ("assigning a list to scalar property '%s' on '%s' instance %d" % (self.key, self.obj.__class__.__name__, id(self.obj)))
        self.orig = self.obj.__dict__.get(self.key, None)
        self.obj.__dict__[self.key] = value
    def delattr(self):
        self.orig = self.obj.__dict__.get(self.key, None)
        self.obj.__dict__[self.key] = None
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
    """manages the value of a particular list-based attribute on a particular object instance."""
    def __init__(self, obj, key, data=None):
        self.obj = obj
        self.key = key
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
            
        util.HistoryArraySet.__init__(self, list_)

    def gethistory(self, *args, **kwargs):
        return self
    def __call__(self, *args, **kwargs):
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
        return res
    def _delrecord(self, item):
        res = util.HistoryArraySet._delrecord(self, item)
        if res:
            self.list_value_changed(self.obj, self.key, item, self, True)
        return res

class CallableProp(object):
    """allows the attaching of a callable item, representing the future value
    of a particular attribute on a particular object instance, to 
    the AttributeManager.  When the attributemanager
    accesses the object attribute, either to get its history or its real value, the __call__ method
    is invoked which runs the underlying callable_ and sets the new value to the object attribute
    via the manager, at which point the CallableProp itself is dereferenced."""
    def __init__(self, callable_, obj, key, uselist = False, **kwargs):
        self.callable_ = callable_
        self.obj = obj
        self.key = key
        self.uselist = uselist
        self.kwargs = kwargs
    def gethistory(self, manager, *args, **kwargs):
        self.__call__(manager, *args, **kwargs)
        return manager.attribute_history[self.obj][self.key]
    def __call__(self, manager, passive=False, **kwargs):
        if passive:
            return None
        if self.uselist:
            if not self.obj.__dict__.has_key(self.key) or len(self.obj.__dict__[self.key]) == 0:
                value = self.callable_()
            else:
                value = None
            p = manager.create_list(self.obj, self.key, value, **self.kwargs)
            manager.attribute_history[self.obj][self.key] = p
            return p
        else:
            if not self.obj.__dict__.has_key(self.key):
                value = self.callable_()
                self.obj.__dict__[self.key] = value
            p = PropHistory(self.obj, self.key, **self.kwargs)
            manager.attribute_history[self.obj][self.key] = p
            return p
            
class AttributeManager(object):
    """maintains a set of per-attribute callable/history manager objects for a set of objects."""
    def __init__(self):
        self.attribute_history = {}

    def value_changed(self, obj, key, value):
        pass
    def create_prop(self, key, uselist, **kwargs):
        return SmartProperty(self).property(key, uselist, **kwargs)
    def create_list(self, obj, key, list_, **kwargs):
        return ListElement(obj, key, list_)
        
    def get_attribute(self, obj, key, **kwargs):
        try:
            return self.get_history(obj, key, **kwargs)(self)
        except KeyError:
            pass
        try:
            return obj.__dict__[key]
        except KeyError:
            raise AttributeError(key)

    def get_list_attribute(self, obj, key, **kwargs):
        return self.get_list_history(obj, key, **kwargs)
        
    def set_attribute(self, obj, key, value, **kwargs):
        self.get_history(obj, key, **kwargs).setattr(value)
        self.value_changed(obj, key, value)
    
    def set_list_attribute(self, obj, key, value, **kwargs):
        self.get_list_history(obj, key, **kwargs).setattr(value)
        
    def delete_attribute(self, obj, key, **kwargs):
        self.get_history(obj, key, **kwargs).delattr()
        self.value_changed(obj, key, value)

    def set_callable(self, obj, key, func, uselist, **kwargs):
        try:
            d = self.attribute_history[obj]
        except KeyError, e:
            d = {}
            self.attribute_history[obj] = d
        d[key] = CallableProp(func, obj, key, uselist, **kwargs)
        
    def delete_list_attribute(self, obj, key, **kwargs):
        pass
        
    def rollback(self, obj = None):
        if obj is None:
            for attr in self.attribute_history.values():
                for hist in attr.values():
                    hist.rollback()
        else:
            try:
                attributes = self.attribute_history[obj]
                for hist in attributes.values():
                    hist.rollback()
            except KeyError:
                pass

    def commit(self, obj = None):
        if obj is None:
            for attr in self.attribute_history.values():
                for hist in attr.values():
                    hist.commit()
        else:
            try:
                attributes = self.attribute_history[obj]
                for hist in attributes.values():
                    hist.commit()
            except KeyError:
                pass
                
    def remove(self, obj):
        try:
            del self.attribute_history[obj]
        except KeyError:
            pass
            
    def get_history(self, obj, key, **kwargs):
        try:
            return self.attribute_history[obj][key].gethistory(self, **kwargs)
        except KeyError, e:
            if e.args[0] is obj:
                d = {}
                self.attribute_history[obj] = d
                p = PropHistory(obj, key, **kwargs)
                d[key] = p
                return p
            else:
                p = PropHistory(obj, key, **kwargs)
                self.attribute_history[obj][key] = p
                return p

    def get_list_history(self, obj, key, passive = False, **kwargs):
        try:
            return self.attribute_history[obj][key].gethistory(self, passive)
        except KeyError, e:
            # TODO: when an callable is re-set on an existing list element
            list_ = obj.__dict__.get(key, None)
            if e.args[0] is obj:
                d = {}
                self.attribute_history[obj] = d
                p = self.create_list(obj, key, list_, **kwargs)
                d[key] = p
                return p
            else:
                p = self.create_list(obj, key, list_, **kwargs)
                self.attribute_history[obj][key] = p
                return p

    def register_attribute(self, class_, key, uselist, **kwargs):
        setattr(class_, key, self.create_prop(key, uselist, **kwargs))
