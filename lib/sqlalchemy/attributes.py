import sqlalchemy.util as util
import weakref

class SmartProperty(object):
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

class ListElement(util.HistoryArraySet):
    """overrides HistoryArraySet to mark the parent object as dirty when changes occur"""

    def __init__(self, obj, key, items = None):
        self.obj = obj
        self.key = key
        util.HistoryArraySet.__init__(self, items)
        print "listelement init"

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

class PropHistory(object):
    # make our own NONE to distinguish from "None"
    NONE = object()
    def __init__(self, obj, key):
        self.obj = obj
        self.key = key
        self.orig = PropHistory.NONE
    def setattr_clean(self, value):
        self.obj.__dict__[self.key] = value
    def setattr(self, value):
        self.orig = self.obj.__dict__.get(self.key, None)
        self.obj.__dict__[self.key] = value
    def delattr(self):
        self.orig = self.obj.__dict__.get(self.key, None)
        self.obj.__dict__[self.key] = None
    def rollback(self):
        if self.orig is not PropHistory.NONE:
            self.obj.__dict__[self.key] = self.orig
            self.orig = PropHistory.NONE
    def clear_history(self):
        self.orig = PropHistory.NONE
    def added_items(self):
        if self.orig is not PropHistory.NONE:
            return [self.obj.__dict__[self.key]]
        else:
            return []
    def deleted_items(self):
        if self.orig is not PropHistory.NONE:
            return [self.orig]
        else:
            return []
    def unchanged_items(self):
        if self.orig is PropHistory.NONE:
            return [self.obj.__dict__[self.key]]
        else:
            return []

class AttributeManager(object):
    def __init__(self):
        self.attribute_history = {}
    def value_changed(self, obj, key, value):
        pass
#        if hasattr(obj, '_instance_key'):
#            self.register_dirty(obj)
#        else:
#            self.register_new(obj)

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
        
    def rollback(self, obj):
        try:
            attributes = self.attribute_history[obj]
            for hist in attributes.values():
                hist.rollback()
        except KeyError:
            pass

    def clear_history(self, obj):
        try:
            attributes = self.attribute_history[obj]
            for hist in attributes.values():
                hist.clear_history()
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

    def get_list_history(self, obj, key):
        try:
            return self.attribute_history[obj][key]
        except KeyError, e:
            list_ = obj.__dict__.get(key, None)
            if callable(list_):
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
