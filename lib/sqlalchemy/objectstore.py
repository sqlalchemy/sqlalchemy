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
import sqlalchemy.attributes as attributes
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

def clear():
    uow.set(UnitOfWork())
            
def has_key(key):
    return uow().identity_map.has_key(key)

class UOWSmartProperty(attributes.SmartProperty):
    def attribute_registry(self):
        return uow().attributes
    
class UOWListElement(attributes.ListElement):
    def list_value_changed(self, obj, key, listval):
        uow().modified_lists.append(self)

class UOWAttributeManager(attributes.AttributeManager):
    def __init__(self, uow):
        attributes.AttributeManager.__init__(self)
        self.uow = uow
        
    def value_changed(self, obj, key, value):
        if hasattr(obj, '_instance_key'):
            self.uow.register_dirty(obj)
        else:
            self.uow.register_new(obj)

    def create_prop(self, key, uselist):
        return UOWSmartProperty(self).property(key, uselist)

    def create_list(self, obj, key, list_):
        return UOWListElement(obj, key, list_)
        
class UnitOfWork(object):
    def __init__(self, parent = None, is_begun = False):
        self.is_begun = is_begun
        if parent is not None:
            self.identity_map = parent.identity_map
        else:
            self.identity_map = {}
        self.attributes = UOWAttributeManager(self)
        self.new = util.HashSet()
        self.dirty = util.HashSet()
        self.modified_lists = util.HashSet()
        self.deleted = util.HashSet()
        self.parent = parent

    def get(self, class_, *id):
        return sqlalchemy.mapper.object_mapper(class_).get(*id)

    def _get(self, key):
        return self.identity_map[key]
        
    def _put(self, key, obj):
        self.identity_map[key] = obj
    
    def _remove_deleted(self, obj):
        if hasattr(obj, "_instancekey"):
            del self.identity_map[obj._instancekey]
        del self.deleted[obj]
        self.attributes.remove(obj)
        
    def update(self, obj):
        """called to add an object to this UnitOfWork as though it were loaded from the DB, but is
        actually coming from somewhere else, like a web session or similar."""
        self._put(obj._instance_key, obj)
        self.register_dirty(obj)
        
    def register_attribute(self, class_, key, uselist):
        self.attributes.register_attribute(class_, key, uselist)
        
    def attribute_set_callable(self, obj, key, func):
        obj.__dict__[key] = func

    
    def register_clean(self, obj):
        try:
            del self.dirty[obj]
        except KeyError:
            pass
        try:
            del self.new[obj]
        except KeyError:
            pass
        self._put(obj._instance_key, obj)
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
        self.deleted.append(obj)  

    # TODO: tie in register_new/register_dirty with table transaction begins ?
    def begin(self):
        u = UnitOfWork(self, True)
        uow.set(u)
        
    def commit(self, *objects):
        import sqlalchemy.mapper

        commit_context = UOWTransaction(self)

        if len(objects):
            for obj in objects:
                if self.deleted.contains(obj):
                    commit_context.add_item_to_delete(obj)
                elif self.new.contains(obj) or self.dirty.contains(obj):
                    commit_context.append_task(obj)
        else:
            for obj in [n for n in self.new] + [d for d in self.dirty]:
                commit_context.append_task(obj)
            for item in self.modified_lists:
                obj = item.obj
                commit_context.append_task(obj)
            for obj in self.deleted:
                commit_context.add_item_to_delete(obj)
                
        engines = util.HashSet()
        for mapper in commit_context.mappers:
            for e in mapper.engines:
                engines.append(e)
                
        for e in engines:
            e.begin()
        try:
            commit_context.execute()
        except:
            for e in engines:
                e.rollback()
            if self.parent:
                self.rollback()
            raise
        for e in engines:
            e.commit()
            
        commit_context.post_exec()
        self.attributes.commit()
        
        if self.parent:
            uow.set(self.parent)

    def rollback_object(self, obj):
        self.attributes.rollback(obj)

    def rollback(self):
        if not self.is_begun:
            raise "UOW transaction is not begun"
        self.attributes.rollback()
        uow.set(self.parent)
            
class UOWTransaction(object):
    def __init__(self, uow):
        self.uow = uow
        # links objects to their mappers
        self.object_mappers = {}
        #  unique list of all the mappers we come across
        self.mappers = util.HashSet()
        self.dependencies = {}
        self.tasks = {}
        self.saved_objects = util.HashSet()
        self.saved_lists = util.HashSet()
        self.deleted_objects = util.HashSet()
        self.deleted_lists = util.HashSet()

    def append_task(self, obj):
        mapper = self.object_mapper(obj)
        task = self.get_task_by_mapper(mapper)
        task.objects.append(obj)

    def add_item_to_delete(self, obj):
        mapper = self.object_mapper(obj)
        task = self.get_task_by_mapper(mapper, True)
        task.objects.append(obj)

    def get_task_by_mapper(self, mapper, isdelete = False):
        try:
            return self.tasks[(mapper, isdelete)]
        except KeyError:
            return self.tasks.setdefault((mapper, isdelete), UOWTask(mapper, isdelete))

    def get_objects(self, mapper, isdelete = False):
        try:
            task = self.tasks[(mapper, isdelete)]
        except KeyError:
            return []
            
        return task.objects
            
    def register_dependency(self, mapper, dependency):
        self.dependencies[(mapper, dependency)] = True

    def register_task(self, mapper, isdelete, processor, mapperfrom, isdeletefrom):
        task = self.get_task_by_mapper(mapper, isdelete)
        targettask = self.get_task_by_mapper(mapperfrom, isdeletefrom)
        task.dependencies.append((processor, targettask))

    def register_saved_object(self, obj):
        self.saved_objects.append(obj)

    def register_saved_list(self, listobj):
        self.saved_lists.append(listobj)

    def register_deleted_list(self, listobj):
        self.deleted_lists.append(listobj)
        
    def register_deleted_object(self, obj):
        self.deleted_objects.append(obj)
        
        
    def object_mapper(self, obj):
        import sqlalchemy.mapper
        try:
            return self.object_mappers[obj]
        except KeyError:
            mapper = sqlalchemy.mapper.object_mapper(obj)
            self.object_mappers[obj] = mapper
            self.mappers.append(mapper)
            return mapper
            
    def execute(self):
        for task in self.tasks.values():
            task.mapper.register_dependencies(self)
            
        tasklist = self.tasks.values()
        def compare(a, b):
            if a.mapper is b.mapper:
                return a.isdelete and 1 or -1
            elif self.dependencies.has_key((a.mapper, b.mapper)):
                if a.isdelete is not b.isdelete:
                    return a.isdelete and 1 or -1
                else:
                    return -1
            elif self.dependencies.has_key((b.mapper, a.mapper)):
                if a.isdelete is not b.isdelete:
                    return a.isdelete and 1 or -1
                else:
                    return 1
            else:
                return 0
            return c
        tasklist.sort(compare)

        import string
        print string.join([str(t) for t in tasklist], ',')

        for task in tasklist:
            obj_list = task.objects
            if not task.isdelete:
                task.mapper.save_obj(obj_list, self)
            for dep in task.dependencies:
                (processor, targettask) = dep
                processor.process_dependencies(targettask.objects, self, delete = task.isdelete)
            if task.isdelete:
                task.mapper.delete_obj(obj_list, self)
            
    def post_exec(self):
        for obj in self.saved_objects:
            mapper = self.object_mapper(obj)
            obj._instance_key = mapper.identity_key(obj)
            self.uow.register_clean(obj)

        for obj in self.saved_lists:
            try:
                del self.uow.modified_lists[obj]
            except KeyError:
                pass

        for obj in self.deleted_objects:
            self.uow._remove_deleted(obj)
        
        for obj in self.deleted_lists:
            try:
                del self.uow.modified_lists[obj]
            except KeyError:
                pass
            
class UOWTask(object):
    def __init__(self, mapper, isdelete = False):
        self.mapper = mapper
        self.isdelete = isdelete
        self.objects = util.HashSet()
        self.dependencies = []
    
    def __str__(self):
        if self.isdelete:
            return self.mapper.table.name + " deletes"
        else:
            return self.mapper.table.name + " saves"
            
uow = util.ScopedRegistry(lambda: UnitOfWork(), "thread")