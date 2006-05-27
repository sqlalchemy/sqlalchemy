# orm/unitofwork.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""the internals for the Unit Of Work system.  includes hooks into the attributes package
enabling the routing of change events to Unit Of Work objects, as well as the flush() mechanism
which creates a dependency structure that executes change operations.  

a Unit of Work is essentially a system of maintaining a graph of in-memory objects and their
modified state.  Objects are maintained as unique against their primary key identity using
an "identity map" pattern.  The Unit of Work then maintains lists of objects that are new, 
dirty, or deleted and provides the capability to flush all those changes at once.
"""

from sqlalchemy import attributes
from sqlalchemy import util
import sqlalchemy
from sqlalchemy.exceptions import *
import StringIO
import weakref
import topological
from sets import *

# a global indicating if all flush() operations should have their plan
# printed to standard output.  also can be affected by creating an engine
# with the "echo_uow=True" keyword argument.
LOG = False

class UOWProperty(attributes.SmartProperty):
    """overrides SmartProperty to provide ORM-specific accessors"""
    def __init__(self, class_, *args, **kwargs):
        super(UOWProperty, self).__init__(*args, **kwargs)
        self.class_ = class_
    property = property(lambda s:class_mapper(s.class_).props[s.key], doc="returns the MapperProperty object associated with this property")

                
class UOWListElement(attributes.ListAttribute):
    """overrides ListElement to provide unit-of-work "dirty" hooks when list attributes are modified,
    plus specialzed append() method."""
    def __init__(self, obj, key, data=None, cascade=None, **kwargs):
        attributes.ListAttribute.__init__(self, obj, key, data=data, **kwargs)
        self.cascade = cascade
    def do_value_changed(self, obj, key, item, listval, isdelete):
        sess = object_session(obj)
        if sess is not None:
            sess._register_changed(obj)
            if self.cascade is not None:
                if not isdelete:
                    if self.cascade.save_update:
                        sess.save_or_update(item)
    def append(self, item, _mapper_nohistory = False):
        if _mapper_nohistory:
            self.append_nohistory(item)
        else:
            attributes.ListAttribute.append(self, item)

class UOWScalarElement(attributes.ScalarAttribute):
    def __init__(self, obj, key, cascade=None, **kwargs):
        attributes.ScalarAttribute.__init__(self, obj, key, **kwargs)
        self.cascade=cascade
    def do_value_changed(self, oldvalue, newvalue):
        obj = self.obj
        sess = object_session(obj)
        if sess is not None:
            sess._register_changed(obj)
            if newvalue is not None and self.cascade is not None:
                if self.cascade.save_update:
                    sess.save_or_update(newvalue)
            
class UOWAttributeManager(attributes.AttributeManager):
    """overrides AttributeManager to provide unit-of-work "dirty" hooks when scalar attribues are modified, plus factory methods for UOWProperrty/UOWListElement."""
    def __init__(self):
        attributes.AttributeManager.__init__(self)
        
    def create_prop(self, class_, key, uselist, callable_, **kwargs):
        return UOWProperty(class_, self, key, uselist, callable_, **kwargs)

    def create_scalar(self, obj, key, **kwargs):
        return UOWScalarElement(obj, key, **kwargs)
        
    def create_list(self, obj, key, list_, **kwargs):
        return UOWListElement(obj, key, list_, **kwargs)
        
class UnitOfWork(object):
    """main UOW object which stores lists of dirty/new/deleted objects.  provides top-level "flush" functionality as well as the transaction boundaries with the SQLEngine(s) involved in a write operation."""
    def __init__(self, identity_map=None):
        if identity_map is not None:
            self.identity_map = identity_map
        else:
            self.identity_map = weakref.WeakValueDictionary()
            
        self.attributes = global_attributes
        self.new = util.HashSet(ordered = True)
        self.dirty = util.HashSet()
        
        self.deleted = util.HashSet()

    def get(self, class_, *id):
        """given a class and a list of primary key values in their table-order, locates the mapper 
        for this class and calls get with the given primary key values."""
        return object_mapper(class_).get(*id)

    def _get(self, key):
        return self.identity_map[key]
        
    def _put(self, key, obj):
        self.identity_map[key] = obj

    def refresh(self, sess, obj):
        self.rollback_object(obj)
        sess.query(obj.__class__)._get(obj._instance_key, reload=True)

    def expire(self, sess, obj):
        self.rollback_object(obj)
        def exp():
            sess.query(obj.__class__)._get(obj._instance_key, reload=True)
        global_attributes.trigger_history(obj, exp)
    
    def is_expired(self, obj, unexpire=False):
        ret = global_attributes.has_trigger(obj)
        if ret and unexpire:
            global_attributes.untrigger_history(obj)
        return ret
            
    def has_key(self, key):
        """returns True if the given key is present in this UnitOfWork's identity map."""
        return self.identity_map.has_key(key)
    
    def expunge(self, obj):
        """removes this object completely from the UnitOfWork, including the identity map,
        and the "new", "dirty" and "deleted" lists."""
        self._remove_deleted(obj)
        
    def _remove_deleted(self, obj):
        if hasattr(obj, "_instance_key"):
            del self.identity_map[obj._instance_key]
        try:            
            del self.deleted[obj]
        except KeyError:
            pass
        try:
            del self.dirty[obj]
        except KeyError:
            pass
        try:
            del self.new[obj]
        except KeyError:
            pass
        #self.attributes.commit(obj)
        self.attributes.remove(obj)

    def _validate_obj(self, obj):
        """validates that dirty/delete/flush operations can occur upon the given object, by checking
        if it has an instance key and that the instance key is present in the identity map."""
        if hasattr(obj, '_instance_key') and not self.identity_map.has_key(obj._instance_key):
            raise InvalidRequestError("Detected a mapped object not present in the current thread's Identity Map: '%s'.  Use objectstore.import_instance() to place deserialized instances or instances from other threads" % repr(obj._instance_key))
        
    def update(self, obj):
        """called to add an object to this UnitOfWork as though it were loaded from the DB,
        but is actually coming from somewhere else, like a web session or similar."""
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
        if not hasattr(obj, '_instance_key'):
            mapper = object_mapper(obj)
            obj._instance_key = mapper.instance_key(obj)
        self._put(obj._instance_key, obj)
        self.attributes.commit(obj)
        
    def register_new(self, obj):
        if hasattr(obj, '_instance_key'):
            raise InvalidRequestError("Object '%s' already has an identity - it cant be registered as new" % repr(obj))
        if not self.new.contains(obj):
            self.new.append(obj)
        self.unregister_deleted(obj)
        
    def register_dirty(self, obj):
        if not self.dirty.contains(obj):
            self._validate_obj(obj)
            self.dirty.append(obj)
        self.unregister_deleted(obj)
        
    def is_dirty(self, obj):
        if not self.dirty.contains(obj):
            return False
        else:
            return True
        
    def register_deleted(self, obj):
        if not self.deleted.contains(obj):
            self._validate_obj(obj)
            self.deleted.append(obj)  

    def unregister_deleted(self, obj):
        try:
            self.deleted.remove(obj)
        except KeyError:
            pass
            
    def flush(self, session, objects=None, echo=False):
        flush_context = UOWTransaction(self, session)

        if objects is not None:
            objset = util.HashSet(iter=objects)
        else:
            objset = None

        for obj in [n for n in self.new] + [d for d in self.dirty]:
            if objset is not None and not objset.contains(obj):
                continue
            if self.deleted.contains(obj):
                continue
            flush_context.register_object(obj)
            
        for obj in self.deleted:
            if objset is not None and not objset.contains(obj):
                continue
            flush_context.register_object(obj, isdelete=True)
        
        trans = session.create_transaction(autoflush=False)
        flush_context.transaction = trans
        try:
            flush_context.execute(echo=echo)
            trans.commit()
        except:
            trans.rollback()
            raise
            
        flush_context.post_exec()
        

    def rollback_object(self, obj):
        """'rolls back' the attributes that have been changed on an object instance."""
        self.attributes.rollback(obj)
        try:
            del self.dirty[obj]
        except KeyError:
            pass
        try:
            del self.deleted[obj]
        except KeyError:
            pass
            
class UOWTransaction(object):
    """handles the details of organizing and executing transaction tasks 
    during a UnitOfWork object's flush() operation."""
    def __init__(self, uow, session):
        self.uow = uow
        self.session = session
        #  unique list of all the mappers we come across
        self.mappers = util.HashSet()
        self.dependencies = {}
        self.tasks = {}
        self.__modified = False
        self.__is_executing = False
        
    def register_object(self, obj, isdelete = False, listonly = False, postupdate=False, **kwargs):
        """adds an object to this UOWTransaction to be updated in the database.

        'isdelete' indicates whether the object is to be deleted or saved (update/inserted).

        'listonly', indicates that only this object's dependency relationships should be
        refreshed/updated to reflect a recent save/upcoming delete operation, but not a full
        save/delete operation on the object itself, unless an additional save/delete
        registration is entered for the object."""
        #print "REGISTER", repr(obj), repr(getattr(obj, '_instance_key', None)), str(isdelete), str(listonly)
        
        # things can get really confusing if theres duplicate instances floating around,
        # so make sure everything is OK
        self.uow._validate_obj(obj)
            
        mapper = object_mapper(obj)
        self.mappers.append(mapper)
        task = self.get_task_by_mapper(mapper)

        if postupdate:
            mod = task.append_postupdate(obj)
            if mod: self._mark_modified()
            return
                
        # for a cyclical task, things need to be sorted out already,
        # so this object should have already been added to the appropriate sub-task
        # can put an assertion here to make sure....
        if task.circular:
            return
        
        mod = task.append(obj, listonly, isdelete=isdelete, **kwargs)
        if mod: self._mark_modified()

    def unregister_object(self, obj):
        mapper = object_mapper(obj)
        task = self.get_task_by_mapper(mapper)
        if obj in task.objects:
            task.delete(obj)
            self._mark_modified()
    
    def _mark_modified(self):
        #if self.__is_executing:
        #    raise "test assertion failed"
        self.__modified = True
        
    def get_task_by_mapper(self, mapper):
        """every individual mapper involved in the transaction has a single
        corresponding UOWTask object, which stores all the operations involved
        with that mapper as well as operations dependent on those operations.
        this method returns or creates the single per-transaction instance of
        UOWTask that exists for that mapper."""
        try:
            return self.tasks[mapper]
        except KeyError:
            task = UOWTask(self, mapper)
            task.mapper.register_dependencies(self)
            return task
            
    def register_dependency(self, mapper, dependency):
        """called by mapper.PropertyLoader to register the objects handled by
        one mapper being dependent on the objects handled by another."""
        # correct for primary mapper (the mapper offcially associated with the class)
        mapper = mapper._primary_mapper().base_mapper()
        dependency = dependency._primary_mapper().base_mapper()
        
        self.dependencies[(mapper, dependency)] = True
        self._mark_modified()

    def register_processor(self, mapper, processor, mapperfrom):
        """called by mapper.PropertyLoader to register itself as a "processor", which
        will be associated with a particular UOWTask, and be given a list of "dependent"
        objects corresponding to another UOWTask to be processed, either after that secondary
        task saves its objects or before it deletes its objects."""
        # when the task from "mapper" executes, take the objects from the task corresponding
        # to "mapperfrom"'s list of save/delete objects, and send them to "processor"
        # for dependency processing
        #print "registerprocessor", str(mapper), repr(processor.key), str(mapperfrom)
        
        # correct for primary mapper (the mapper offcially associated with the class)
        mapper = mapper._primary_mapper()
        mapperfrom = mapperfrom._primary_mapper()
        
        task = self.get_task_by_mapper(mapper)
        targettask = self.get_task_by_mapper(mapperfrom)
        up = UOWDependencyProcessor(processor, targettask)
        task.dependencies.append(up)
        self._mark_modified()

    def execute(self, echo=False):
        # pre-execute dependency processors.  this process may 
        # result in new tasks, objects and/or dependency processors being added,
        # particularly with 'delete-orphan' cascade rules.
        # keep running through the full list of tasks until all
        # objects have been processed.
        while True:
            ret = False
            for task in self.tasks.values():
                for up in task.dependencies:
                    if up.preexecute(self):
                        ret = True
            if not ret:
                break
        
        # flip the execution flag on.  in some test cases
        # we like to check this flag against any new objects being added, since everything
        # should be registered by now.  there is a slight exception in the case of 
        # post_update requests; this should be fixed.
        self.__is_executing = True
        
        head = self._sort_dependencies()
        self.__modified = False
        if LOG or echo:
            if head is None:
                print "Task dump: None"
            else:
                print "Task dump:\n" + head.dump()
        if head is not None:
            head.execute(self)
        #if self.__modified and head is not None:
        #    raise "Assertion failed ! new pre-execute dependency step should eliminate post-execute changes (except post_update stuff)."
        if LOG or echo:
            print "\nExecute complete\n"
            
    def post_exec(self):
        """after an execute/flush is completed, all of the objects and lists that have
        been flushed are updated in the parent UnitOfWork object to mark them as clean."""
        
        for task in self.tasks.values():
            for elem in task.objects.values():
                if elem.isdelete:
                    self.uow._remove_deleted(elem.obj)
                else:
                    self.uow.register_clean(elem.obj)

    def _sort_dependencies(self):
        """creates a hierarchical tree of dependent tasks.  the root node is returned.
        when the root node is executed, it also executes its child tasks recursively."""
        def sort_hier(node):
            if node is None:
                return None
            task = self.get_task_by_mapper(node.item)
            if node.cycles is not None:
                tasks = []
                for n in node.cycles:
                    tasks.append(self.get_task_by_mapper(n.item))
                task.circular = task._sort_circular_dependencies(self, tasks)
            for child in node.children:
                t = sort_hier(child)
                if t is not None:
                    task.childtasks.append(t)
            return task
            
        mappers = util.HashSet()
        for task in self.tasks.values():
            mappers.append(task.mapper)

        def inheriting_tasks(task):
            if task.mapper not in mappers:
                return
            for mapper in task.mapper._inheriting_mappers:
                inherit_task = self.tasks.get(mapper, None)
                if inherit_task is None:
                    continue
                inheriting_tasks(inherit_task)
                task.inheriting_tasks.append(inherit_task)
                del mappers[mapper]
                
        for task in self.tasks.values():
            inheriting_tasks(task)
                
        head = DependencySorter(self.dependencies, mappers).sort(allow_all_cycles=True)
        #print str(head)
        task = sort_hier(head)
        return task


class UOWTaskElement(object):
    """an element within a UOWTask.  corresponds to a single object instance
    to be saved, deleted, or just part of the transaction as a placeholder for 
    further dependencies (i.e. 'listonly').
    in the case of self-referential mappers, may also store a list of childtasks,
    further UOWTasks containing objects dependent on this element's object instance."""
    def __init__(self, obj):
        self.obj = obj
        self.__listonly = True
        self.childtasks = []
        self.__isdelete = False
        self.__preprocessed = {}
    def _get_listonly(self):
        return self.__listonly
    def _set_listonly(self, value):
        """set_listonly is a one-way setter, will only go from True to False."""
        if not value and self.__listonly:
            self.__listonly = False
            self.clear_preprocessed()
    def _get_isdelete(self):
        return self.__isdelete
    def _set_isdelete(self, value):
        if self.__isdelete is not value:
            self.__isdelete = value
            self.clear_preprocessed()
    listonly = property(_get_listonly, _set_listonly)
    isdelete = property(_get_isdelete, _set_isdelete)
    
    def mark_preprocessed(self, processor):
        """marks this element as "preprocessed" by a particular UOWDependencyProcessor.  preprocessing is the step
        which sweeps through all the relationships on all the objects in the flush transaction and adds other objects
        which are also affected,  In some cases it can switch an object from "tosave" to "todelete".  changes to the state 
        of this UOWTaskElement will reset all "preprocessed" flags, causing it to be preprocessed again.  When all UOWTaskElements
        have been fully preprocessed by all UOWDependencyProcessors, then the topological sort can be done."""
        self.__preprocessed[processor] = True
    def is_preprocessed(self, processor):
        return self.__preprocessed.get(processor, False)
    def clear_preprocessed(self):
        self.__preprocessed.clear()
    def __repr__(self):
        return "UOWTaskElement/%d: %s/%d %s" % (id(self), self.obj.__class__.__name__, id(self.obj), (self.listonly and 'listonly' or (self.isdelete and 'delete' or 'save')) )

class UOWDependencyProcessor(object):
    """in between the saving and deleting of objects, process "dependent" data, such as filling in 
    a foreign key on a child item from a new primary key, or deleting association rows before a 
    delete.  This object acts as a proxy to a DependencyProcessor."""
    def __init__(self, processor, targettask):
        self.processor = processor
        self.targettask = targettask
        
    def preexecute(self, trans):
        """traverses all objects handled by this dependency processor and locates additional objects which should be 
        part of the transaction, such as those affected deletes, orphans to be deleted, etc. Returns True if any
        objects were preprocessed, or False if no objects were preprocessed."""
        def getobj(elem):
            elem.mark_preprocessed(self)
            return elem.obj
            
        ret = False
        elements = [getobj(elem) for elem in self.targettask.polymorphic_tosave_elements if elem.obj is not None and not elem.is_preprocessed(self)]
        if len(elements):
            ret = True
            self.processor.preprocess_dependencies(self.targettask, elements, trans, delete=False)

        elements = [getobj(elem) for elem in self.targettask.polymorphic_todelete_elements if elem.obj is not None and not elem.is_preprocessed(self)]
        if len(elements):
            ret = True
            self.processor.preprocess_dependencies(self.targettask, elements, trans, delete=True)
        return ret
        
    def execute(self, trans, delete):
        if not delete:
            self.processor.process_dependencies(self.targettask, [elem.obj for elem in self.targettask.polymorphic_tosave_elements if elem.obj is not None], trans, delete=False)
        else:            
            self.processor.process_dependencies(self.targettask, [elem.obj for elem in self.targettask.polymorphic_todelete_elements if elem.obj is not None], trans, delete=True)

    def get_object_dependencies(self, obj, trans, passive):
        return self.processor.get_object_dependencies(obj, trans, passive=passive)

    def whose_dependent_on_who(self, obj, o):        
        return self.processor.whose_dependent_on_who(obj, o)

    def branch(self, task):
        return UOWDependencyProcessor(self.processor, task)

class UOWTask(object):
    """represents the full list of objects that are to be saved/deleted by a specific Mapper."""
    def __init__(self, uowtransaction, mapper):
        if uowtransaction is not None:
            uowtransaction.tasks[mapper] = self
        self.uowtransaction = uowtransaction
        self.mapper = mapper
        self.objects = util.OrderedDict()
        self.dependencies = []
        self.cyclical_dependencies = []
        self.circular = None
        self.childtasks = []
        self.inheriting_tasks = []
        
    def is_empty(self):
        return len(self.objects) == 0 and len(self.dependencies) == 0 and len(self.childtasks) == 0
    
    def append(self, obj, listonly = False, childtask = None, isdelete = False):
        """appends an object to this task, to be either saved or deleted depending on the
        'isdelete' attribute of this UOWTask.  'listonly' indicates that the object should
        only be processed as a dependency and not actually saved/deleted. if the object
        already exists with a 'listonly' flag of False, it is kept as is. 'childtask' is used
        internally when creating a hierarchical list of self-referential tasks, to assign
        dependent operations at the per-object instead of per-task level. """
        try:
            rec = self.objects[obj]
            retval = False
        except KeyError:
            rec = UOWTaskElement(obj)
            self.objects[obj] = rec
            retval = True
        if not listonly:
            rec.listonly = False
        if childtask:
            rec.childtasks.append(childtask)
        if isdelete:
            rec.isdelete = True
        #if not childtask:
        #    rec.preprocessed = False
        return retval
    
    def append_postupdate(self, obj):
        # postupdates are UPDATED immeditely (for now)
        self.mapper.save_obj([obj], self.uowtransaction, postupdate=True)
        return True
            
    def delete(self, obj):
        try:
            del self.objects[obj]
        except KeyError:
            pass

    def _save_objects(self, trans):
        self.mapper.save_obj(self.tosave_objects, trans)
        for task in self.inheriting_tasks:
            task._save_objects(trans)
    def _delete_objects(self, trans):
        self.mapper.delete_obj(self.todelete_objects, trans)
        for task in self.inheriting_tasks:
            task._delete_objects(trans)
    def _execute_dependencies(self, trans):
        for dep in self.dependencies:
            dep.execute(trans, False)
        for task in self.inheriting_tasks:
            task._execute_dependencies(trans)
        for dep in self.dependencies:
            dep.execute(trans, True)
    def _execute_childtasks(self, trans):
        for child in self.childtasks:
            child.execute(trans)
        for task in self.inheriting_tasks:
            task._execute_childtasks(trans)
    def _execute_cyclical_dependencies(self, trans, isdelete):
        for dep in self.cyclical_dependencies:
            dep.execute(trans, isdelete)
        for task in self.inheriting_tasks:
            task._execute_cyclical_dependencies(trans, isdelete)
    def _execute_per_element_childtasks(self, trans, isdelete):
        if isdelete:
            for element in self.todelete_elements:
                for task in element.childtasks:
                    task.execute(trans)
        else:
            for element in self.tosave_elements:
                for task in element.childtasks:
                    task.execute(trans)
        for task in self.inheriting_tasks:
            task._execute_per_element_childtasks(trans, isdelete)
            
    def execute(self, trans):
        """executes this UOWTask.  saves objects to be saved, processes all dependencies
        that have been registered, and deletes objects to be deleted. """
        
        # a "circular" task is a circularly-sorted collection of UOWTask/UOWTaskElements
        # derived from the components of this UOWTask, which accounts for inter-row dependencies.  
        # if one was created for this UOWTask, it replaces the execution for this UOWTask.
        if self.circular is not None:
            self.circular.execute(trans)
            return

        # TODO: add a visitation system to the UOW classes and have this execution called
        # from a separate executor object ? (would also handle dumping)
        
        self._save_objects(trans)
        self._execute_cyclical_dependencies(trans, False)
        self._execute_per_element_childtasks(trans, False)
        self._execute_dependencies(trans)
        self._execute_cyclical_dependencies(trans, True)
        self._execute_childtasks(trans)
        self._execute_per_element_childtasks(trans, True)
        self._delete_objects(trans)

    def _polymorphic_elements(self):
        for rec in self.objects.values():
            yield rec
            for task in self.inheriting_tasks:
                for rec in task._polymorphic_elements():
                    yield rec
    
    polymorphic_tosave_elements = property(lambda self: [rec for rec in self._polymorphic_elements() if not rec.isdelete])
    polymorphic_todelete_elements = property(lambda self: [rec for rec in self._polymorphic_elements() if rec.isdelete])
    
    tosave_elements = property(lambda self: [rec for rec in self.objects.values() if not rec.isdelete])
    todelete_elements = property(lambda self:[rec for rec in self.objects.values() if rec.isdelete])
    tosave_objects = property(lambda self:[rec.obj for rec in self.objects.values() if rec.obj is not None and not rec.listonly and rec.isdelete is False])
    todelete_objects = property(lambda self:[rec.obj for rec in self.objects.values() if rec.obj is not None and not rec.listonly and rec.isdelete is True])
        
    def _sort_circular_dependencies(self, trans, cycles):
        """for a single task, creates a hierarchical tree of "subtasks" which associate
        specific dependency actions with individual objects.  This is used for a
        "cyclical" task, or a task where elements
        of its object list contain dependencies on each other.
        
        this is not the normal case; this logic only kicks in when something like 
        a hierarchical tree is being represented.

        """

        allobjects = []
        for task in cycles:
            allobjects += task.objects.keys()
        tuples = []
        
        cycles = Set(cycles)
        
        #print "BEGIN CIRC SORT-------"
        #print "PRE-CIRC:"
        #print list(cycles)[0].dump()
        
        # dependency processors that arent part of the cyclical thing
        # get put here
        extradeplist = []

        object_to_original_task = {}
        
        # organizes a set of new UOWTasks that will be assembled into
        # the final tree, for the purposes of holding new UOWDependencyProcessors
        # which process small sub-sections of dependent parent/child operations
        dependencies = {}
        def get_dependency_task(obj, depprocessor):
            try:
                dp = dependencies[obj]
            except KeyError:
                dp = {}
                dependencies[obj] = dp
            try:
                l = dp[depprocessor]
            except KeyError:
                l = UOWTask(None, depprocessor.targettask.mapper)
                dp[depprocessor] = l
            return l

        # organize all original UOWDependencyProcessors by their target task
        deps_by_targettask = {}
        for task in cycles:
            for dep in task.dependencies:
                if dep.targettask not in cycles or trans.get_task_by_mapper(dep.processor.mapper) not in cycles:
                    extradeplist.append(dep)
                l = deps_by_targettask.setdefault(dep.targettask, [])
                l.append(dep)

        for task in cycles:
            for taskelement in task.objects.values():
                obj = taskelement.obj
                object_to_original_task[obj] = task
                #print "OBJ", repr(obj), "TASK", repr(task)
                
                for dep in deps_by_targettask.get(task, []):
                    # is this dependency involved in one of the cycles ?
                    cyclicaldep = dep.targettask in cycles and trans.get_task_by_mapper(dep.processor.mapper) in cycles
                    if not cyclicaldep:
                        continue
                        
                    (processor, targettask) = (dep.processor, dep.targettask)
                    isdelete = taskelement.isdelete
                    
                    # list of dependent objects from this object
                    childlist = dep.get_object_dependencies(obj, trans, passive = True)
                    # the task corresponding to the processor's objects
                    childtask = trans.get_task_by_mapper(processor.mapper)
                    
#                    if isdelete:
#                        childlist = childlist.unchanged_items() + childlist.deleted_items()
#                    else:
#                        childlist = childlist.added_items() 
                        
                    childlist = childlist.added_items() + childlist.unchanged_items() + childlist.deleted_items()
                        
                    for o in childlist:
                        if o is None or o not in childtask.objects:
                            continue
                        whosdep = dep.whose_dependent_on_who(obj, o)
                        if whosdep is not None:
                            tuples.append(whosdep)
                            # create a UOWDependencyProcessor representing this pair of objects.
                            # append it to a UOWTask
                            if whosdep[0] is obj:
                                get_dependency_task(whosdep[0], dep).append(whosdep[0], isdelete=isdelete)
                            else:
                                get_dependency_task(whosdep[0], dep).append(whosdep[1], isdelete=isdelete)
                        else:
                            get_dependency_task(obj, dep).append(obj, isdelete=isdelete)
                        
        head = DependencySorter(tuples, allobjects).sort()
        if head is None:
            return None

        #print str(head)

        hierarchical_tasks = {}
        def get_object_task(obj):
            try:
                return hierarchical_tasks[obj]
            except KeyError:
                originating_task = object_to_original_task[obj]
                return hierarchical_tasks.setdefault(obj, UOWTask(None, originating_task.mapper))

        def make_task_tree(node, parenttask):
            """takes a dependency-sorted tree of objects and creates a tree of UOWTasks"""
            #print "MAKETASKTREE", node.item

            t = get_object_task(node.item)
            for n in node.children:
                t2 = make_task_tree(n, t)
                    
            can_add_to_parent = t.mapper is parenttask.mapper
            original_task = object_to_original_task[node.item]
            if original_task.objects.has_key(node.item):
                if can_add_to_parent:
                    parenttask.append(node.item, original_task.objects[node.item].listonly, isdelete=original_task.objects[node.item].isdelete, childtask=t)
                else:
                    t.append(node.item, original_task.objects[node.item].listonly, isdelete=original_task.objects[node.item].isdelete)
                    parenttask.append(None, listonly=False, isdelete=original_task.objects[node.item].isdelete, childtask=t)
            #else:
            #    parenttask.append(None, listonly=False, isdelete=original_task.objects[node.item].isdelete, childtask=t)
            if dependencies.has_key(node.item):
                for depprocessor, deptask in dependencies[node.item].iteritems():
                    if can_add_to_parent:
                        parenttask.cyclical_dependencies.append(depprocessor.branch(deptask))
                    else:
                        t.cyclical_dependencies.append(depprocessor.branch(deptask))
            return t

        # this is the new "circular" UOWTask which will execute in place of "self"
        t = UOWTask(None, self.mapper)

        # stick the non-circular dependencies and child tasks onto the new
        # circular UOWTask
        t.dependencies += [d for d in extradeplist]
        t.childtasks = self.childtasks
        make_task_tree(head, t)
        #print t.dump()
        return t

    def dump(self):
        buf = StringIO.StringIO()
        self._dump(buf)
        return buf.getvalue()
        
    def _dump(self, buf, indent=0, circularparent=None):
        # TODO: the dumper should go into a separate module/class, allow different dumper implementations
        def _indent():
            return "  | " * indent

        headers = {}
        def header(buf, text):
            """writes a given header just once"""
            try:
                headers[text]
            except KeyError:
                buf.write(_indent() + "  |\n")
                buf.write(text)
                headers[text] = True
            
        def _dump_processor(proc, deletes):
            if deletes:
                val = proc.targettask.polymorphic_todelete_elements
            else:
                val = proc.targettask.polymorphic_tosave_elements

            buf.write(_indent() + "  |- %s attribute on %s (UOWDependencyProcessor(%d) processing %s)\n" % (
                repr(proc.processor.key), 
                    ("%s's to be %s" % (_repr_task_class(proc.targettask), deletes and "deleted" or "saved")),
                id(proc), 
                _repr_task(proc.targettask))
            )
            
            if len(val) == 0:
                buf.write(_indent() + "  |       |-" + "(no objects)\n")
            for v in val:
                buf.write(_indent() + "  |       |-" + _repr_task_element(v, proc.processor.key) + "\n")
        
        def _repr_task_element(te, attribute=None):
            if te.obj is None:
                objid = "(placeholder)"
            else:
                if attribute is not None:
                    objid = "%s(%d).%s" % (te.obj.__class__.__name__, id(te.obj), attribute)
                else:
                    objid = "%s(%d)" % (te.obj.__class__.__name__, id(te.obj))
            return "%s (UOWTaskElement(%d, %s))" % (objid, id(te), (te.listonly and 'listonly' or (te.isdelete and 'delete' or 'save')))
                
        def _repr_task(task):
            if task.mapper is not None:
                if task.mapper.__class__.__name__ == 'Mapper':
                    name = task.mapper.class_.__name__ + "/" + task.mapper.local_table.name + "/" + str(task.mapper.entity_name)
                else:
                    name = repr(task.mapper)
            else:
                name = '(none)'
            return ("UOWTask(%d, %s)" % (id(task), name))
        def _repr_task_class(task):
            if task.mapper is not None and task.mapper.__class__.__name__ == 'Mapper':
                return task.mapper.class_.__name__
            else:
                return '(none)'

        def _repr(obj):
            return "%s(%d)" % (obj.__class__.__name__, id(obj))

        def _inheritance_tag(task):
            if task is not self:
                return (" (inheriting task %s)" % _repr_task(task))
            else:
                return ""

        def _dump_saveelements(task):
            for rec in task.tosave_elements:
                if rec.listonly:
                    continue
                header(buf, _indent() + "  |- Save elements"+ _inheritance_tag(task) + "\n")
                buf.write(_indent() + "  |- " + _repr_task_element(rec)  + "\n")
            for t in task.inheriting_tasks:
                _dump_saveelements(t)

        def _dump_deleteelements(task):
            for rec in task.todelete_elements:
                if rec.listonly:
                    continue
                header(buf, _indent() + "  |- Delete elements"+ _inheritance_tag(task) + "\n")
                buf.write(_indent() + "  |- " + _repr_task_element(rec) + "\n")
            for t in task.inheriting_tasks:
                _dump_deleteelements(t)

        def _dump_dependencies(task):
            for dep in task.dependencies:
                header(buf, _indent() + "  |- Save dependencies" + _inheritance_tag(task) + "\n")
                _dump_processor(dep, False)
            for t in task.inheriting_tasks:
                _dump_dependencies(t)
            for dep in task.dependencies:
                header(buf, _indent() + "  |- Delete dependencies" + _inheritance_tag(task) + "\n")
                _dump_processor(dep, True)
        
        def _dump_childtasks(task):
            for child in task.childtasks:
                header(buf, _indent() + "  |- Child tasks" + _inheritance_tag(task) + "\n")
                child._dump(buf, indent + 1)
            for t in task.inheriting_tasks:
                _dump_childtasks(t)
            
        if self.circular is not None:
            self.circular._dump(buf, indent, self)
            return

        i = _indent()
        if len(i):
            i = i[0:-1] + "-"
        if circularparent is not None:
            buf.write(i + " " + _repr_task(circularparent))
            buf.write("->circular->" + _repr_task(self))
        else:
            buf.write(i + " " + _repr_task(self))
            
        buf.write("\n")
        _dump_saveelements(self)
        for dep in self.cyclical_dependencies:
            header(buf, _indent() + "  |- Cyclical Save dependencies\n")
            _dump_processor(dep, False)
        for element in self.tosave_elements:
            for task in element.childtasks:
                header(buf, _indent() + "  |- Save subelements of UOWTaskElement(%s)\n" % id(element))
                task._dump(buf, indent + 1)
        _dump_dependencies(self)
        for dep in self.cyclical_dependencies:
            header(buf, _indent() + "  |- Cyclical Delete dependencies\n")
            _dump_processor(dep, True)
        _dump_childtasks(self)
        for element in self.todelete_elements:
            for task in element.childtasks:
                header(buf, _indent() + "  |- Delete subelements of UOWTaskElement(%s)\n" % id(element))
                task._dump(buf, indent + 1)
        _dump_deleteelements(self)

        if self.is_empty():   
            buf.write(_indent() + "  |- (empty task)\n")
        else:
            buf.write(_indent() + "  |----\n")
            
        buf.write(_indent() + "\n")           
        
    def __repr__(self):
        if self.mapper is not None:
            if self.mapper.__class__.__name__ == 'Mapper':
                name = self.mapper.class_.__name__ + "/" + self.mapper.local_table.name
            else:
                name = repr(self.mapper)
        else:
            name = '(none)'
        return ("UOWTask(%d) Mapper: '%s'" % (id(self), name))

class DependencySorter(topological.QueueDependencySorter):
    pass

def mapper(*args, **params):
    return sqlalchemy.mapper(*args, **params)

def object_mapper(obj):
    return sqlalchemy.object_mapper(obj)

def class_mapper(class_):
    return sqlalchemy.class_mapper(class_)

global_attributes = UOWAttributeManager()

