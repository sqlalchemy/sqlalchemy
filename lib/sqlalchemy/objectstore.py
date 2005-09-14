# objectstore.py
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


"""maintains all currently loaded objects in memory,
using the "identity map" pattern.  Also provides a "unit of work" object which tracks changes
to objects so that they may be properly persisted within a transactional scope."""

import thread
import sqlalchemy.util as util
import weakref

def get_id_key(ident, class_, table, selectable):
    """returns an identity-map key for use in storing/retrieving an item from the identity map, given
    a tuple of the object's primary key values.
    
    ident - a tuple of primary key values corresponding to the object to be stored.  these values
    should be in the same order as the primary keys of the table
    class_ - a reference to the object's class
    table - a Table object where the object's primary fields are stored.
    selectable - a Selectable object which represents all the object's column-based fields.  this Selectable
    may be synonymous with the table argument or can be a larger construct containing that table.
    return value: a tuple object which is used as an identity key.
    """
    return (class_, table, tuple(ident))
def get_instance_key(object, class_, table, selectable):
    """returns an identity-map key for use in storing/retrieving an item from the identity map, given
    the object instance itself.
    
    object - the object to be stored.  it is assumed that the object's primary key attributes are
    populated.
    class_ - a reference to the object's class
    table - a Table object where the object's primary fields are stored.
    selectable - a Selectable object which represents all the object's column-based fields.  this Selectable
    may be synonymous with the table argument or can be a larger construct containing that table.
    return value: a tuple object which is used as an identity key.
    """
    return (class_, table, tuple([getattr(object, column.key, None) for column in selectable.primary_keys]))
def get_row_key(row, class_, table, selectable):
    """returns an identity-map key for use in storing/retrieving an item from the identity map, given
    a result set row.
    
    row - a sqlalchemy.dbengine.RowProxy instance or other map corresponding result-set column
    names to their values within a row.
    class_ - a reference to the object's class
    table - a Table object where the object's primary fields are stored.
    selectable - a Selectable object which represents all the object's column-based fields.  this Selectable
    may be synonymous with the table argument or can be a larger construct containing that table.
    return value: a tuple object which is used as an identity key.
    """
    return (class_, table, tuple([row[column.label] for column in selectable.primary_keys]))

identity_map = {}

def get(key):
    val = identity_map[key]
    if isinstance(val, dict):
        return val[thread.get_ident()]
    else:
        return val
    
def put(key, obj, scope='thread'):

    if isinstance(obj, dict):
        raise "cant put a dict in the object store"
    
    obj._instance_key = key
        
    if scope == 'thread':
        try:
            d = identity_map[key]
        except KeyError:
            d = identity_map.setdefault(key, {})
        d[thread.get_ident()] = obj
    else:
        identity_map[key] = obj

def clear(scope='thread'):
    if scope == 'thread':
        for k in identity_map.keys():
            if isinstance(identity_map[k], dict):
                identity_map[k].clear()
    else:
        for k in identity_map.keys():
            if not isinstance(identity_map[k], dict):
                del identity_map[k]
            
def has_key(key):
    if identity_map.has_key(key):
        d = identity_map[key]
        if isinstance(d, dict):
            return d.has_key(thread.get_ident())
        else:
            return True
    else:
        return False

class UOWListElement(util.HistoryArraySet):
    """overrides HistoryArraySet to mark the parent object as dirty when changes occur"""
    class listpointer(object): pass
        
    def __init__(self, obj, items = None):
        util.HistoryArraySet.__init__(self, items)
        self.obj = weakref.ref(obj)
        
        # cant hash a UserList, so make a bullshit pointer to us
        self.listpointer = UOWListElement.listpointer()
        self.listpointer.list = self
        
    def _setrecord(self, item):
        res = util.HistoryArraySet._setrecord(self, item)
        if res:
            uow().modified_lists.append(self.listpointer)
        return res
    def _delrecord(self, item):
        res = util.HistoryArraySet._delrecord(self, item)
        if res:
            uow().modified_lists.append(self.listpointer)
        return res
    
class UnitOfWork(object):
    def __init__(self):
        self.new = util.HashSet()
        self.dirty = util.HashSet()
        self.modified_lists = util.HashSet()
        self.deleted = util.HashSet()
        self.attribute_history = weakref.WeakKeyDictionary()
        
    def attribute_set_callable(self, obj, key, func):
        obj.__dict__[key] = func

    def get_attribute(self, obj, key):
        try:
            v = obj.__dict__[key]
        except KeyError:
            raise AttributeError(key)
        if (callable(v)):
            v = v()
            obj.__dict__[key] = v
            self.register_attribute(obj, key).setattr_clean(v)
        return v
        
    def set_attribute(self, obj, key, value, usehistory = False):
        if usehistory:
            self.register_attribute(obj, key).setattr(value)
        obj.__dict__[key] = value
        if hasattr(obj, '_instance_key'):
            self.register_dirty(obj)
        else:
            self.register_new(obj)
        
    def delete_attribute(self, obj, key, value, usehistory = False):
        if usehistory:
            self.register_attribute(obj, key).delattr(value)    
        del obj.__dict__[key]
        if hasattr(obj, '_instance_key'):
            self.register_dirty(obj)
        else:
            self.register_new(obj)
        
    def register_attribute(self, obj, key):
        try:
            attributes = self.attribute_history[obj]
        except KeyError:
            attributes = self.attribute_history.setdefault(obj, {})
        try:
            return attributes[key]
        except KeyError:
            return attributes.setdefault(key, util.PropHistory(obj.__dict__.get(key, None)))

    def register_list_attribute(self, obj, key, data = None):
        try:
            childlist = obj.__dict__[key]
        except KeyError:
            childlist = UOWListElement(obj)
            obj.__dict__[key] = childlist
        
        if callable(childlist):
            childlist = childlist()
            
        if not isinstance(childlist, util.HistoryArraySet):
            childlist = UOWListElement(obj, childlist)
            obj.__dict__[key] = childlist
        if data is not None and childlist.data != data:
            try:
                childlist.set_data(data)
            except TypeError:
                raise "object " + repr(data) + " is not an iterable object"
        return childlist
        
    def register_clean(self, obj):
        try:
            del self.dirty[obj]
        except KeyError:
            pass

    def register_new(self, obj):
        self.new.append(obj)
        
    def register_dirty(self, obj):
        self.dirty.append(obj)

    def is_dirty(self, obj):
        if not self.dirty.contains(obj):
            return False
        else:
            return True
        
    def register_deleted(self, obj):
        pass   

    def commit(self):
        import sqlalchemy.mapper
        
        self.dependencies = {}
        self.tasks = {}
        
        for obj in [n for n in self.new] + [d for d in self.dirty]:
            mapper = sqlalchemy.mapper.object_mapper(obj)
            task = self.get_task_by_mapper(mapper)
            task.objects.append(obj)

        for item in self.modified_lists:
            item = item.list
            obj = item.obj()
            mapper = sqlalchemy.mapper.object_mapper(obj)
            task = self.get_task_by_mapper(mapper)
            task.lists.append(obj)
            
        for task in self.tasks.values():
            task.mapper.register_dependencies(util.HashSet(task.objects + task.lists), self)
            
        mapperlist = self.tasks.values()
        def compare(a, b):
            if self.dependencies.has_key((a.mapper, b.mapper)):
                return -1
            elif self.dependencies.has_key((b.mapper, a.mapper)):
                return 1
            else:
                return 0
        mapperlist.sort(compare)
        
        # TODO: break save_obj into a list of tasks that are more SQL-specific
        for task in mapperlist:
            obj_list = task.objects
            for obj in obj_list:
                task.mapper.save_obj(obj)
            for dep in task.dependencies:
                (processor, stuff_to_process) = dep
                processor.process_dependencies(stuff_to_process, self)

        for obj in self.new:
            mapper = sqlalchemy.mapper.object_mapper(obj)
            mapper.put(obj)
        self.new.clear()
        self.dirty.clear()
        for item in self.modified_lists:
            item = item.list
            item.clear_history()
        self.modified_lists.clear()

        self.tasks.clear()
        self.dependencies.clear()
        # TODO: deleted stuff

    # TODO: better interface for tasks with no object save, or multiple dependencies
    def register_dependency(self, mapper, dependency, processor, stuff_to_process):
        self.dependencies[(mapper, dependency)] = True
        task = self.get_task_by_mapper(mapper)
        if processor is not None:
            task.dependencies.append((processor, stuff_to_process))
        
    def get_task_by_mapper(self, mapper):
        try:
            return self.tasks[mapper]
        except KeyError:
            return self.tasks.setdefault(mapper, UOWTask(mapper))
    
class UOWTask(object):
    def __init__(self, mapper):
        self.mapper = mapper
        self.objects = []
        self.lists = []
        self.dependencies = []
        
uow = util.ScopedRegistry(lambda: UnitOfWork(), "thread")