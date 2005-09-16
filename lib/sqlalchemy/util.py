# util.py
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

__ALL__ = ['OrderedProperties', 'OrderedDict']
import thread, weakref, UserList

class OrderedProperties(object):

    def __init__(self):
        self.__dict__['_list'] = []
    
    def keys(self):
        return self._list
        
    def __iter__(self):
        return iter([self[x] for x in self._list])
    
    def __setitem__(self, key, object):
        setattr(self, key, object)
        
    def __getitem__(self, key):
        return getattr(self, key)
        
    def __setattr__(self, key, object):
        if not hasattr(self, key):
            self._list.append(key)
    
        self.__dict__[key] = object
    

class OrderedDict(dict):
    """A Dictionary that keeps its own internal ordering"""
    def __init__(self, values = None):
        self.list = []
        if values is not None:
            for val in values:
                self.update(val)

    def keys(self):
        return self.list

    def update(self, dict):
        for key in dict.keys():
            self.__setitem__(key, dict[key])

    def setdefault(self, key, value):
        if not self.has_key(key):
            self.__setitem__(key, value)
            return value
        else:
            return self.__getitem__(key)

    def values(self):
        return map(lambda key: self[key], self.list)
        
    def __iter__(self):
        return iter(self.list)

    def itervalues(self):
        return iter([self[key] for key in self.list])
        
    def iterkeys(self):return self.__iter__()
    
    def iteritems(self):
        return iter([(key, self[key]) for key in self.keys()])
    
    def __setitem__(self, key, object):
        if not self.has_key(key):
            self.list.append(key)
        dict.__setitem__(self, key, object)
        
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

class ThreadLocal(object):
    def __init__(self):
        object.__setattr__(self, 'tdict', {})
    def __getattribute__(self, key):
        try:
            return object.__getattribute__(self, 'tdict')["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            raise AttributeError(key)
    def __setattr__(self, key, value):
        object.__getattribute__(self, 'tdict')["%d_%s" % (thread.get_ident(), key)] = value

class HashSet(object):
    def __init__(self, iter = None):
        self.map  = {}
        if iter is not None:
            for i in iter:
                self.append(i)
    def __iter__(self):
        return iter(self.map.values())
 
    def contains(self, item):
        return self.map.has_key(item)

    def clear(self):
        self.map.clear()
        
    def append(self, item):
        self.map[item] = item
 
    def __len__(self):
        return len(self.map)
        
    def __delitem__(self, key):
        del self.map[key]
 
    def __getitem__(self, key):
        return self.map[key]
        
class HistoryArraySet(UserList.UserList):
    def __init__(self, items = None, data = None):
        UserList.UserList.__init__(self, items)
        # stores the array's items as keys, and a value of True, False or None indicating
        # added, deleted, or unchanged for that item
        if data is not None:
            self.data = data
        self.records = {}
        for i in self.data:
            self.records[i] = True

    def set_data(self, data):
        # first mark everything current as "deleted"
        for i in self.data:
            self.records[i] = False
            
        # switch array
        self.data = data

        # TODO: fix this up, remove items from array while iterating
        for i in range(0, len(self.data)):
            if not _setrecord(self, self.data[i]):
               del self.data[i]
               i -= 1

    def hash(self):
        return id(self)
    def _setrecord(self, item):
        try:
            val = self.records[item]
            if val is True or val is None:
                return False
            else:
                self.records[item] = None
                return True
        except KeyError:
            self.records[item] = True
            return True
    def _delrecord(self, item):
        try:
            val = self.records[item]
            if val is None:
                self.records[item] = False
            elif val is True:
                del self.records[item]
        except KeyError:
            pass
    def clear_history(self):
        for key in self.records.keys():
            value = self.records[key]
            if value is False:
                del self.records[key]
            else:
                self.records[key] = None
    def added_items(self):
        return [key for key in self.data if self.records[key] is True]
    def deleted_items(self):
        return [key for key, value in self.records.iteritems() if value is False]
    def unchanged_items(self):
        return [key for key in self.data if self.records[key] is None]
    def append_nohistory(self, item):
        if not self.records.has_key(item):
            self.records[item] = None
            self.data.append(item)
    def has_item(self, item):
        return self.records.has_key(item)
    def __setitem__(self, i, item): 
        if self._setrecord(a):
            self.data[i] = item
    def __delitem__(self, i):
        self._delrecord(self.data[i])
        del self.data[i]
    def __setslice__(self, i, j, other):
        i = max(i, 0); j = max(j, 0)
        if isinstance(other, UserList.UserList):
            l = other.data
        elif isinstance(other, type(self.data)):
            l = other
        else:
            l = list(other)
        g = [a for a in l if self._setrecord(a)]
        self.data[i:] = g
    def __delslice__(self, i, j):
        i = max(i, 0); j = max(j, 0)
        for a in self.data[i:j]:
            self._delrecord(a)
        del self.data[i:j]
    def append(self, item): 
        if self._setrecord(item):
            self.data.append(item)
    def insert(self, i, item): 
        if self._setrecord(item):
            self.data.insert(i, item)
    def pop(self, i=-1):
        item = self.data[i]
        self._delrecord(item) 
        return self.data.pop(i)
    def remove(self, item): 
        self._delrecord(item)
        self.data.remove(item)
    def __add__(self, other):
        raise NotImplementedError()
    def __radd__(self, other):
        raise NotImplementedError()
    def __iadd__(self, other):
        raise NotImplementedError()

class PropHistory(object):
    def __init__(self, current):
        self.added = None
        self.current = current
        self.deleted = None
    def setattr_clean(self, value):
        self.current = value
    def setattr(self, value):
        self.deleted = self.current
        self.current = None
        self.added = value
    def delattr(self):
        self.deleted = self.current
        self.current = None
    def clear_history(self):
        if self.added is not None:
            self.current = self.added
            self.added = None
        if self.deleted is not None:
            self.deleted = None
    def added_items(self):
        if self.added is not None:
            return [self.added]
        else:
            return []
    def deleted_items(self):
        if self.deleted is not None:
            return [self.deleted]
        else:
            return []
    def unchanged_items(self):
        if self.current is not None:
            return [self.current]
        else:
            return []
        
class ScopedRegistry(object):
    def __init__(self, createfunc, defaultscope):
        self.createfunc = createfunc
        self.defaultscope = defaultscope
        self.application = createfunc()
        self.threadlocal = {}
        self.scopes = {
                    'application' : {'call' : self._call_application, 'clear' : self._clear_application, 'set':self._set_application}, 
                    'thread' : {'call' : self._call_thread, 'clear':self._clear_thread, 'set':self._set_thread}
                    }

    def __call__(self, scope = None):
        if scope is None:
            scope = self.defaultscope
        return self.scopes[scope]['call']()

    def set(self, obj, scope = None):
        if scope is None:
            scope = self.defaultscope
        return self.scopes[scope]['set'](obj)
        
    def clear(self, scope = None):
        if scope is None:
            scope = self.defaultscope
        return self.scopes[scope]['clear']()

    def _set_thread(self, obj):
        self.threadlocal[thread.get_ident()] = obj
    
    def _call_thread(self):
        try:
            return self.threadlocal[thread.get_ident()]
        except KeyError:
            return self.threadlocal.setdefault(thread.get_ident(), self.createfunc())

    def _clear_thread(self):
        try:
            del self.threadlocal[thread.get_ident()]
        except KeyError:
            pass

    def _set_application(self, obj):
        self.application = obj
        
    def _call_application(self):
        return self.application

    def _clear_application(self):
        self.application = createfunc()
                
            