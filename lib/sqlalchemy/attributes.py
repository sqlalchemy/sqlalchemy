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
    def property(self, key, uselist):
        def set_prop(obj, value):
            if uselist:
                self.attribute_registry().set_list_attribute(obj, key, value)
            else:
                self.attribute_registry().set_attribute(obj, key, value)
        def del_prop(obj):
            if uselist:
                # TODO: this probably doesnt work right, deleting the list off an item
                self.attribute_registry().delete_list_attribute(obj, key)
            else:
                self.attribute_registry().delete_attribute(obj, key)
        def get_prop(obj):
            if uselist:
                return self.attribute_registry().get_list_attribute(obj, key)
            else:
                return self.attribute_registry().get_attribute(obj, key)
                
        return property(get_prop, set_prop, del_prop)

class PropHistory(object):
    """manages the value of a particular scalar attribute on a particular object instance."""
    # make our own NONE to distinguish from "None"
    NONE = object()
    def __init__(self, obj, key):
        self.obj = obj
        self.key = key
        self.orig = PropHistory.NONE
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
    def __init__(self, obj, key, items = None):
        self.obj = obj
        self.key = key
        util.HistoryArraySet.__init__(self, items)
        obj.__dict__[key] = self.data

    def list_value_changed(self, obj, key, listval):
        pass    

    def setattr(self, value):
        self.obj.__dict__[self.key] = value
        self.set_data(value)
    def delattr(self, value):
        pass    
    def _setrecord(self, item):
        res = util.HistoryArraySet._setrecord(self, item)
        if res:
            self.list_value_changed(self.obj, self.key, self)
        return res
    def _delrecord(self, item):
        res = util.HistoryArraySet._delrecord(self, item)
        if res:
            self.list_value_changed(self.obj, self.key, self)
        return res


class AttributeManager(object):
    """maintains a set of per-attribute history objects for a set of objects."""
    def __init__(self):
        self.attribute_history = {}

    def value_changed(self, obj, key, value):
        pass
    def create_prop(self, key, uselist):
        return SmartProperty(self).property(key, uselist)
    def create_list(self, obj, key, list_):
        return ListElement(obj, key, list_)
        
    def get_attribute(self, obj, key):
        try:
            v = obj.__dict__[key]
        except KeyError:
            raise AttributeError(key)
        if (callable(v)):
            v = v()
            obj.__dict__[key] = v
        return v

    def get_list_attribute(self, obj, key):
        return self.get_list_history(obj, key)
        
    def set_attribute(self, obj, key, value):
        self.get_history(obj, key).setattr(value)
        self.value_changed(obj, key, value)
    
    def set_list_attribute(self, obj, key, value):
        self.get_list_history(obj, key).setattr(value)
        
    def delete_attribute(self, obj, key):
        self.get_history(obj, key).delattr()
        self.value_changed(obj, key, value)

        
    def delete_list_attribute(self, obj, key):
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
            
    def get_history(self, obj, key):
        try:
            return self.attribute_history[obj][key]
        except KeyError, e:
            if e.args[0] is obj:
                d = {}
                self.attribute_history[obj] = d
                p = PropHistory(obj, key)
                d[key] = p
                return p
            else:
                p = PropHistory(obj, key)
                self.attribute_history[obj][key] = p
                return p

    def get_list_history(self, obj, key, passive = False):
        try:
            return self.attribute_history[obj][key]
        except KeyError, e:
            list_ = obj.__dict__.get(key, None)
            if callable(list_):
                if passive:
                    return None
                list_ = list_()
            if e.args[0] is obj:
                d = {}
                self.attribute_history[obj] = d
                p = self.create_list(obj, key, list_)
                d[key] = p
                return p
            else:
                p = self.create_list(obj, key, list_)
                self.attribute_history[obj][key] = p
                return p

    def register_attribute(self, class_, key, uselist):
        setattr(class_, key, self.create_prop(key, uselist))
