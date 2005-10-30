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
import thread, weakref, UserList,string

class OrderedProperties(object):
    """an object that maintains the order in which attributes are set upon it.
    also provides an iterator and a very basic dictionary interface to those attributes.
    """
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
    def __delitem__(self, key):
        delattr(self, key)
        del self._list[self._list.index(key)]
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

    def clear(self):
        self.list = []
        dict.clear(self)
        
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
    
    def __delitem__(self, key):
        try:
            del self.list[self.list.index(key)]
        except ValueError:
            raise KeyError(key)
        dict.__delitem__(self, key)
        
    def __setitem__(self, key, object):
        if not self.has_key(key):
            self.list.append(key)
        dict.__setitem__(self, key, object)
        
    def __getitem__(self, key):
        return dict.__getitem__(self, key)

class ThreadLocal(object):
    """an object in which attribute access occurs only within the context of the current thread"""
    def __init__(self, raiseerror = True):
        self.__dict__['_tdict'] = {}
        self.__dict__['_raiseerror'] = raiseerror
    def __getattr__(self, key):
        try:
            return self._tdict["%d_%s" % (thread.get_ident(), key)]
        except KeyError:
            if self._raiseerror:
                raise AttributeError(key)
            else:
                return None
    def __setattr__(self, key, value):
        self._tdict["%d_%s" % (thread.get_ident(), key)] = value

class HashSet(object):
    """implements a Set."""
    def __init__(self, iter = None, ordered = False):
        if ordered:
            self.map = OrderedDict()
        else:
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
    def __add__(self, other):
        return HashSet(self.map.values() + [i for i in other])
    def __len__(self):
        return len(self.map)
    def __delitem__(self, key):
        del self.map[key]
    def __getitem__(self, key):
        return self.map[key]
        
class HistoryArraySet(UserList.UserList):
    """extends a UserList to provide unique-set functionality as well as history-aware 
    functionality, including information about what list elements were modified, 
    as well as rollback capability."""
    def __init__(self, data = None):
        # stores the array's items as keys, and a value of True, False or None indicating
        # added, deleted, or unchanged for that item
        self.records = OrderedDict()
        if data is not None:
            self.data = data
            for item in data:
                # add items without triggering any change events
                # *ASSUME* the list is unique already.  might want to change this.
                self.records[item] = None
        else:
            self.data = []
    def __getattr__(self, attr):
        """proxies unknown HistoryArraySet methods and attributes to the underlying
        data array.  this allows custom list classes to be used."""
        return getattr(self.data, attr)
    def set_data(self, data):
        # first mark everything current as "deleted"
        for i in self.data:
            self.records[i] = False
            
        # switch array
        self.data = data

        # TODO: fix this up, remove items from array while iterating
        for i in range(0, len(self.data)):
            if not self._setrecord(self.data[i]):
               del self.data[i]
               i -= 1
    def history_contains(self, obj):
        return self.records.has_key(obj)
    def __hash__(self):
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
        return True
    def commit(self):
        for key in self.records.keys():
            value = self.records[key]
            if value is False:
                del self.records[key]
            else:
                self.records[key] = None
    def rollback(self):
        # TODO: speed this up
        list = []
        for key, status in self.records.iteritems():
            if status is False or status is None:
                list.append(key)
        self.data[:] = []
        self.records = {}
        for l in list:
            self.append_nohistory(l)
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

        
class ScopedRegistry(object):
    """a Registry that can store one or multiple instances of a single class 
    on a per-application or per-thread scoped basis"""
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
                
class DependencySorter(object):
    """creates a "dependency tree" across a list of objects, using a series of 'dependency relationships'
    expressed as a list of tuples to determine its shape.  the tuples are of the form (x,y) which indicate 
    'y is dependent on x'.
    a list of additional elements who may or may not be expressed within the tuples is also supplied.
    
    If a tuple contains the same value dependent on itself, the corresponding node
    is marked as "circular", indicating the node is dependent on itself.
    """
    class Node:
        def __init__(self, item):
            #print "new node on " + str(item)
            self.item = item
            self.children = HashSet()
            self.parent = None
            self.circular = False
        def append(self, node):
            if node.parent is not None:
                del node.parent.children[node]
            self.children.append(node)
            node.parent = self
        def __str__(self):
            return self.safestr({})
        def safestr(self, hash, indent = 0):
            if hash.has_key(self):
                return (' ' * indent) + "RECURSIVE:%s(%s, %s)" % (str(self.item), repr(id(self)), self.parent and repr(id(self.parent)) or 'None')
            hash[self] = True
            return (' ' * indent) + "%s  (idself=%s, idparent=%s)" % (str(self.item), repr(id(self)), self.parent and repr(id(self.parent)) or "None") + "\n" + string.join([n.safestr(hash, indent + 1) for n in self.children], '')

    def __init__(self, tuples, allitems):
        self.tuples = tuples
        self.allitems = allitems
    def sort(self):
        (tuples, allitems) = (self.tuples, self.allitems)
        
        nodes = {}
        # make nodes for all the items and store in the hash
        for item in allitems + [t[0] for t in tuples] + [t[1] for t in tuples]:
            if not nodes.has_key(item):
                nodes[item] = DependencySorter.Node(item)

        # loop through tuples
        for tup in tuples:
            (parent, child) = (tup[0], tup[1])
            # get parent node
            parentnode = nodes[parent]

            # if parent is child, mark "circular" attribute on the node
            if parent is child:
                parentnode.circular = True
                # and just continue
                continue

            # get child node
            childnode = nodes[child]
                    
            # now see, if the parent is an ancestor of the child
            c = childnode
            while c is not None and c is not parentnode:
                root = c
                c = c.parent

            # nope, so we have to move the child down from whereever
            # it currently is to a child of the parent
            if c is None:
                for c in parentnode.children:
                    c.parent = root
                    root.children.append(c)
                    del parentnode.children[c]
                root.parent = parentnode
                parentnode.children.append(root)
                print str(parentnode)
        
        # now we have a collection of subtrees which represent dependencies.
        # go through the collection root nodes wire them together into one tree        
        head = None
        for node in nodes.values():
            print "hi1:" + str(node)
            if node.parent is None:
                if head is not None:
                    head.append(node)
                else:
                    head = node

        return head
            