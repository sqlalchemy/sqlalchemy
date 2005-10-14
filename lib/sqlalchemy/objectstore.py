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
import string

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

def mapper(*args, **params):
    import sqlalchemy.mapper
    return sqlalchemy.mapper.mapper(*args, **params)
    
def commit(*obj):
    uow().commit(*obj)
    
def clear():
    uow.set(UnitOfWork())

def delete(*obj):
    uw = uow()
    for o in obj:
        uw.register_deleted(o)
    
def has_key(key):
    return uow().identity_map.has_key(key)

class UOWSmartProperty(attributes.SmartProperty):
    def attribute_registry(self):
        return uow().attributes
    
class UOWListElement(attributes.ListElement):
    def __init__(self, obj, key, data=None, deleteremoved=False):
        attributes.ListElement.__init__(self, obj, key, data=data)
        self.deleteremoved = deleteremoved
    def list_value_changed(self, obj, key, item, listval, isdelete):
        uow().modified_lists.append(self)
        if isdelete and self.deleteremoved:
            uow().register_deleted(item)
    def append(self, item, _mapper_nohistory = False):
        if _mapper_nohistory:
            self.append_nohistory(item)
        else:
            attributes.ListElement.append(self, item)
            
class UOWAttributeManager(attributes.AttributeManager):
    def __init__(self, uow):
        attributes.AttributeManager.__init__(self)
        self.uow = uow
        
    def value_changed(self, obj, key, value):
        if hasattr(obj, '_instance_key'):
            self.uow.register_dirty(obj)
        else:
            self.uow.register_new(obj)

    def create_prop(self, key, uselist, **kwargs):
        return UOWSmartProperty(self).property(key, uselist, **kwargs)

    def create_list(self, obj, key, list_, **kwargs):
        return UOWListElement(obj, key, list_, **kwargs)
        
class UnitOfWork(object):
    def __init__(self, parent = None, is_begun = False):
        self.is_begun = is_begun
        if parent is not None:
            self.identity_map = parent.identity_map
        else:
            self.identity_map = {}
        self.attributes = UOWAttributeManager(self)
        self.new = util.HashSet(ordered = True)
        self.dirty = util.HashSet()
        self.modified_lists = util.HashSet()
        # the delete list is ordered mostly so the unit tests can predict the argument list ordering.
        # TODO: need stronger unit test fixtures....
        self.deleted = util.HashSet(ordered = True)
        self.parent = parent

    def get(self, class_, *id):
        return sqlalchemy.mapper.object_mapper(class_).get(*id)

    def _get(self, key):
        return self.identity_map[key]
        
    def _put(self, key, obj):
        self.identity_map[key] = obj

    def has_key(self, key):
        return self.identity_map.has_key(key)
        
    def _remove_deleted(self, obj):
        if hasattr(obj, "_instance_key"):
            del self.identity_map[obj._instance_key]
        del self.deleted[obj]
        self.attributes.remove(obj)
        
    def update(self, obj):
        """called to add an object to this UnitOfWork as though it were loaded from the DB, but is
        actually coming from somewhere else, like a web session or similar."""
        self._put(obj._instance_key, obj)
        self.register_dirty(obj)
        
    def register_attribute(self, class_, key, uselist, **kwargs):
        self.attributes.register_attribute(class_, key, uselist, **kwargs)

    def register_callable(self, obj, key, func, uselist, **kwargs):
        self.attributes.set_callable(obj, key, func, uselist, **kwargs)
        
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
        mapper = object_mapper(obj)
        # TODO: should the cascading delete dependency thing
        # happen wtihin PropertyLoader.process_dependencies ?
        mapper.register_deleted(obj, self)

    # TODO: tie in register_new/register_dirty with table transaction begins ?
    def begin(self):
        u = UnitOfWork(self, True)
        uow.set(u)
        
    def commit(self, *objects):
        commit_context = UOWTransaction(self)

        if len(objects):
            for obj in objects:
                if self.deleted.contains(obj):
                    commit_context.register_object(obj, isdelete=True)
                elif self.new.contains(obj) or self.dirty.contains(obj):
                    commit_context.register_object(obj)
        else:
            for obj in [n for n in self.new] + [d for d in self.dirty]:
                if self.deleted.contains(obj):
                    continue
                commit_context.register_object(obj)
            for item in self.modified_lists:
                obj = item.obj
                if self.deleted.contains(obj):
                    continue
                commit_context.register_object(obj, listonly = True)
                for o in item.added_items() + item.deleted_items():
                    if self.deleted.contains(o):
                        continue
                    # TODO: why is listonly = False ?  shouldnt we set it
                    # True and have the PropertyLoader determine if it needs update?
                    commit_context.register_object(o, listonly = False)
            for obj in self.deleted:
                commit_context.register_object(obj, isdelete=True)
                
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

        #  unique list of all the mappers we come across
        self.mappers = util.HashSet()
        self.dependencies = {}
        self.tasks = {}
        self.saved_objects = util.HashSet()
        self.saved_lists = util.HashSet()
        self.deleted_objects = util.HashSet()
        self.deleted_lists = util.HashSet()

    def register_object(self, obj, isdelete = False, listonly = False):
        """adds an object to this UOWTransaction to be updated in the database.
        'isdelete' indicates whether the object is to be deleted or saved (update/inserted).
        'listonly', which should be specified with "isdelete=False", indicates that 
        only this object's dependency relationships should be 
        refreshed/updated to reflect a recent save/upcoming delete operation, but not a full
        save/delete operation on the object itself, unless an additional save/delete registration 
        is entered for the object."""
        mapper = object_mapper(obj)
        self.mappers.append(mapper)
        task = self.get_task_by_mapper(mapper, isdelete)
        task.append(obj, listonly)

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

    def register_processor(self, mapper, isdelete, processor, mapperfrom, isdeletefrom):
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
        
    def execute(self):
        for task in self.tasks.values():
            task.mapper.register_dependencies(self)
        
        for task in self._sort_dependencies():
            task.execute(self)
            
    def post_exec(self):
        """after an execute/commit is completed, all of the objects and lists that have
        been committed are updated in the parent UnitOfWork object to mark them as clean."""
        for obj in self.saved_objects:
            mapper = object_mapper(obj)
            obj._instance_key = mapper.instance_key(obj)
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

    def _sort_dependencies(self):
        bymapper = {}
        
        def sort(node, isdel, res):
            #print "Sort: " + (node and str(node.item) or 'None')
            if node is None:
                return res
            task = bymapper.get((node.item, isdel), None)
            if task is not None:
                res.append(task)
                if node.circular:
                    task.iscircular = True
            for child in node.children:
                sort(child, isdel, res)
            return res
            
        mappers = util.HashSet()
        for task in self.tasks.values():
            mappers.append(task.mapper)
            bymapper[(task.mapper, task.isdelete)] = task
    
        head = util.DependencySorter(self.dependencies, mappers).sort()
        res = []
        tasklist = sort(head, False, res)

        res = []
        sort(head, True, res)
        res.reverse()
        tasklist += res

        assert(len(self.tasks.values()) == len(tasklist)) # "sorted task list not the same size as original task list"

        return tasklist
            
class UOWTask(object):
    def __init__(self, mapper, isdelete = False):
        self.mapper = mapper
        self.isdelete = isdelete
        self.objects = util.OrderedDict()
        self.dependencies = []
        self.iscircular = False

    def append(self, obj, listonly = False, childtask = None):
        """appends an object to this task, to be either saved or deleted
        depending on the 'isdelete' attribute of this UOWTask.  'listonly' indicates
        that the object should only be processed as a dependency and not actually saved/deleted.
        if the object already exists with a 'listonly' flag of False, it is kept as is.
        'childtask' is used internally when creating a hierarchical list of self-referential
        tasks, to assign dependent operations at the per-object instead of per-task level."""
        try:
            rec = self.objects[obj]
        except KeyError:
            rec = {'listonly': True, 'childtask': None}
            self.objects[obj] = rec
        if not listonly:
            rec['listonly'] = False
        if childtask:
            rec['childtask'] = childtask
        #print "Task " + str(self) + " append object " + obj.__class__.__name__ + "/" + repr(id(obj)) + " listonly " + repr(listonly) + "/" + repr(self.objects[obj]['listonly'])        
        
    def execute(self, trans):
        """executes this UOWTask.  saves objects to be saved, processes all dependencies
        that have been registered, and deletes objects to be deleted.  If the UOWTask
        has been marked as "circular", performs a circular dependency sort which creates 
        a subtree of UOWTasks which are then executed hierarchically."""
        if self.iscircular:
            #print "creating circular task for " + str(self)
            task = self._sort_circular_dependencies(trans)
            if task is not None:
                task.execute_circular(trans)
            return
            
        obj_list = [o for o, rec in self.objects.iteritems() if not rec['listonly']]
        if not self.isdelete:
            self.mapper.save_obj(obj_list, trans)
        for dep in self.dependencies:
            (processor, targettask) = dep
            processor.process_dependencies(targettask, targettask.objects.keys(), trans, delete = self.isdelete)
        if self.isdelete:
            self.mapper.delete_obj(obj_list, trans)

    def execute_circular(self, trans):
        if not self.isdelete:
            self.execute(trans)
        for obj in self.objects.keys():
            childtask = self.objects[obj]['childtask']
            childtask.execute_circular(trans)
        if self.isdelete:
            self.execute(trans)
            
    def _sort_circular_dependencies(self, trans):
        """for a single task, creates a hierarchical tree of "subtasks" which associate
        specific dependency actions with individual objects.  This is used for a
        "circular" task, or a task where elements
        of its object list contain dependencies on each other."""
        
        allobjects = self.objects.keys()
        tuples = []
        
        objecttotask = {}
        def get_task(obj):
            try:
                return objecttotask[obj]
            except KeyError:
                t = UOWTask(self.mapper, self.isdelete)
                objecttotask[obj] = t
                return t

        dependencies = {}
        def get_dependency_task(obj, processor):
            try:
                dp = dependencies[obj]
            except KeyError:
                dp = {}
                dependencies[obj] = dp
            try:
                l = dp[processor]
            except KeyError:
                l = UOWTask(None, None)
                dp[processor] = l
            return l
            
        for obj in allobjects:
            parenttask = get_task(obj)
            # TODO: we are doing this dependency sort which uses a lot of the 
            # concepts in mapper.PropertyLoader's more coarse-grained version.
            # should consolidate the concept of "childlist/added/deleted/unchanged" "left/right"
            # in one place
            for dep in self.dependencies:
                (processor, targettask) = dep
                childlist = processor.get_object_dependencies(obj, trans, passive = True)
                if self.isdelete:
                    childlist = childlist.unchanged_items() + childlist.deleted_items()
                else:
                    #childlist = childlist.added_items() + childlist.deleted_items()
                    childlist = childlist.added_items()
                for o in childlist:
                    whosdep = processor.whose_dependent_on_who(obj, o, trans)
                    if whosdep is not None:
                        tuples.append(whosdep)
                        if whosdep[0] is obj:
                            get_dependency_task(whosdep[0], processor).append(whosdep[0])
                        else:
                            get_dependency_task(whosdep[0], processor).append(whosdep[1])
        
        head = util.DependencySorter(tuples, allobjects).sort()
        if head is None:
            return None
        
        def make_task_tree(node, parenttask):
            parenttask.append(node.item, self.objects[node.item]['listonly'], objecttotask[node.item])
            if dependencies.has_key(node.item):
                for processor, deptask in dependencies[node.item].iteritems():
                    parenttask.dependencies.append((processor, deptask))
            t = get_task(node.item)
            for n in node.children:
                t2 = make_task_tree(n, t)
            return t
            
        t = UOWTask(self.mapper, self.isdelete)
        make_task_tree(head, t)
        #t._print_circular()        
        return t

    def _print_circular(t):
        print "-----------------------------"
        print "task objects: " + repr([str(v) + " listonly: " + repr(l['listonly']) for v, l in t.objects.iteritems()])
        print "task depends: " + repr([(dt[0].key, [str(o) for o in dt[1].objects.keys()]) for dt in t.dependencies])
        for rec in t.objects.values():
            rec['childtask']._print_circular()
        
    def __str__(self):
        if self.mapper is not None:
            mapperstr = self.mapper.primarytable.name
        else:
            mapperstr = "(no mapper)"
        if self.isdelete:
            return mapperstr + "/deletes/" + repr(id(self))
        else:
            return mapperstr + "/saves/" + repr(id(self))

                    
uow = util.ScopedRegistry(lambda: UnitOfWork(), "thread")


def object_mapper(obj):
    import sqlalchemy.mapper
    return sqlalchemy.mapper.object_mapper(obj)
