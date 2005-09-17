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

def get_id_key(ident, class_, table):
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
def get_row_key(row, class_, table, primary_keys):
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
    return (class_, table, tuple([row[column.label] for column in primary_keys]))

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
        uow.set(UnitOfWork())
    else:
        for k in identity_map.keys():
            if not isinstance(identity_map[k], dict):
                del identity_map[k]
        uow.set(UnitOfWork(), scope="application")
            
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
        
    def __init__(self, obj, items = None):
        util.HistoryArraySet.__init__(self, items)
        self.obj = weakref.ref(obj)
        
    def _setrecord(self, item):
        res = util.HistoryArraySet._setrecord(self, item)
        if res:
            uow().modified_lists.append(self)
        return res
    def _delrecord(self, item):
        res = util.HistoryArraySet._delrecord(self, item)
        if res:
            uow().modified_lists.append(self)
        return res
    
class UnitOfWork(object):
    def __init__(self, parent = None):
        self.new = util.HashSet()
        self.dirty = util.HashSet()
        self.modified_lists = util.HashSet()
        self.deleted = util.HashSet()
        self.attribute_history = weakref.WeakKeyDictionary()
        self.parent = parent
        
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
            childlist = UOWListElement(obj, childlist())
            obj.__dict__[key] = childlist                        
        elif not isinstance(childlist, util.HistoryArraySet):
            childlist = UOWListElement(obj, childlist)
            obj.__dict__[key] = childlist
        if data is not None and childlist.data != data:
            try:
                childlist.set_data(data)
            except TypeError:
                raise "object " + repr(data) + " is not an iterable object"
        return childlist
        
    def register_clean(self, obj, scope="thread"):
        try:
            del self.dirty[obj]
        except KeyError:
            pass
        try:
            del self.new[obj]
        except KeyError:
            pass
        # TODO: figure scope out from what scope of this UOW is
        put(obj._instance_key, obj, scope=scope)
        # TODO: get lists off the object and make sure theyre clean too ?
        
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

    # TODO: tie in register_new/register_dirty with table transaction begins ?
    def begin(self):
        u = UnitOfWork(self)
        uow.set(u)
        
    def commit(self, *objects):
        import sqlalchemy.mapper

        commit_context = UOWTransaction(self)
        
        if len(objects):
            for obj in objects:
                commit_context.append_task(obj)
        else:
            for obj in [n for n in self.new] + [d for d in self.dirty]:
                commit_context.append_task(obj)
            for item in self.modified_lists:
                obj = item.obj()
                commit_context.append_task(obj)

        commit_context.execute()                   

        # TODO: deleted stuff
        
        if self.parent:
            uow.set(self.parent)
            
class UOWTransaction(object):
    def __init__(self, uow):
        self.uow = uow
        self.mappers = {}
        self.dependencies = {}
        self.tasks = {}
        self.saved_objects = util.HashSet()
        self.saved_lists = util.HashSet()

    def append_task(self, obj):
        mapper = self.object_mapper(obj)
        task = self.get_task_by_mapper(mapper)
        task.objects.append(obj)

    def get_task_by_mapper(self, mapper):
        try:
            return self.tasks[mapper]
        except KeyError:
            return self.tasks.setdefault(mapper, UOWTask(mapper))

    # TODO: better interface for tasks with no object save, or multiple dependencies
    def register_dependency(self, mapper, dependency, processor, stuff_to_process):
        self.dependencies[(mapper, dependency)] = True
        task = self.get_task_by_mapper(mapper)
        if processor is not None:
            task.dependencies.append((processor, stuff_to_process))

    def register_saved_object(self, obj):
        self.saved_objects.append(obj)

    def register_saved_list(self, listobj):
        self.saved_lists.append(listobj)

    def object_mapper(self, obj):
        import sqlalchemy.mapper
        try:
            return self.mappers[obj]
        except KeyError:
            mapper = sqlalchemy.mapper.object_mapper(obj)
            self.mappers[obj] = mapper
            return mapper
            
    def execute(self):
        for task in self.tasks.values():
            task.mapper.register_dependencies(task.objects, self)
            
        mapperlist = self.tasks.values()
        def compare(a, b):
            if self.dependencies.has_key((a.mapper, b.mapper)):
                return -1
            elif self.dependencies.has_key((b.mapper, a.mapper)):
                return 1
            else:
                return 0
        mapperlist.sort(compare)

        try:
            # TODO: db tranasction boundary
            for task in mapperlist:
                obj_list = task.objects
                task.mapper.save_obj(obj_list, self)
                for dep in task.dependencies:
                    (processor, stuff_to_process) = dep
                    processor.process_dependencies(stuff_to_process, self)
        except:
            raise

        for obj in self.saved_objects:
            mapper = self.object_mapper(obj)
            obj._instance_key = mapper.identity_key(obj)
            self.uow.register_clean(obj)

        for obj in self.saved_lists:
            try:
                del self.uow.modified_lists[obj]
            except KeyError:
                pass

        
class UOWTask(object):
    def __init__(self, mapper):
        self.mapper = mapper
        self.objects = util.HashSet()
        self.dependencies = []
        
uow = util.ScopedRegistry(lambda: UnitOfWork(), "thread")