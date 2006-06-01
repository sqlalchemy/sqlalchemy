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
import sets

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
            objset = sets.Set(objects)
        else:
            objset = None

        for obj in [n for n in self.new] + [d for d in self.dirty]:
            if objset is not None and not obj in objset:
                continue
            if self.deleted.contains(obj):
                continue
            flush_context.register_object(obj)
            
        for obj in self.deleted:
            if objset is not None and not obj in objset:
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
        self.mappers = sets.Set()
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
        self.mappers.add(mapper)
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
        #print "UNREGISTER", obj
        mapper = object_mapper(obj)
        task = self.get_task_by_mapper(mapper)
        if obj in task.objects:
            task.delete(obj)
            self._mark_modified()
    
    def _mark_modified(self):
        #if self.__is_executing:
        #    raise "test assertion failed"
        self.__modified = True
        
    def get_task_by_mapper(self, mapper, dontcreate=False):
        """every individual mapper involved in the transaction has a single
        corresponding UOWTask object, which stores all the operations involved
        with that mapper as well as operations dependent on those operations.
        this method returns or creates the single per-transaction instance of
        UOWTask that exists for that mapper."""
        try:
            if isinstance(mapper, UOWTask):
                raise "wha"
            return self.tasks[mapper]
        except KeyError:
            if dontcreate:
                return None
            task = UOWTask(self, mapper)
            task.mapper.register_dependencies(self)
            return task
            
    def register_dependency(self, mapper, dependency):
        """called by mapper.PropertyLoader to register the objects handled by
        one mapper being dependent on the objects handled by another."""
        # correct for primary mapper (the mapper offcially associated with the class)
        # also convert to the "base mapper", the parentmost task at the top of an inheritance chain
        # dependency sorting is done via non-inheriting mappers only, dependencies between mappers
        # in the same inheritance chain is done at the per-object level
        mapper = mapper.primary_mapper().base_mapper()
        dependency = dependency.primary_mapper().base_mapper()
        
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
        mapper = mapper.primary_mapper()
        mapperfrom = mapperfrom.primary_mapper()
        
        task = self.get_task_by_mapper(mapper)
        targettask = self.get_task_by_mapper(mapperfrom)
        up = UOWDependencyProcessor(processor, targettask)
        task.dependencies.add(up)
        self._mark_modified()

    def execute(self, echo=False):
        #print "\n------------------\nEXECUTE"
        #for task in self.tasks.values():
        #    print "\nTASK:", task
        #    for obj in task.objects:
        #        print "TASK OBJ:", obj
        #    for elem in task.get_elements(polymorphic=True):
        #        print "POLYMORPHIC TASK OBJ:", elem.obj
        #print "-----------------------------"
        # pre-execute dependency processors.  this process may 
        # result in new tasks, objects and/or dependency processors being added,
        # particularly with 'delete-orphan' cascade rules.
        # keep running through the full list of tasks until all
        # objects have been processed.
        while True:
            ret = False
            for task in self.tasks.values():
                for up in list(task.dependencies):
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
            
        mappers = self._get_noninheriting_mappers()
        head = DependencySorter(self.dependencies, list(mappers)).sort(allow_all_cycles=True)
        #print "-------------------------"
        #print str(head)
        #print "---------------------------"
        task = sort_hier(head)
        return task

    def _get_noninheriting_mappers(self):
        """returns a list of UOWTasks whose mappers are not inheriting from the mapper of another UOWTask.
        i.e., this returns the root UOWTasks for all the inheritance hierarchies represented in this UOWTransaction."""
        mappers = sets.Set()
        for task in self.tasks.values():
            base = task.mapper.base_mapper()
            mappers.add(base)
        return mappers
        
        
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
    def __eq__(self, other):
        return other.processor is self.processor and other.targettask is self.targettask
    def __hash__(self):
        return hash((self.processor, self.targettask))
        
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
    def __init__(self, uowtransaction, mapper, circular_parent=None):
        if not circular_parent:
            uowtransaction.tasks[mapper] = self
        
        # the transaction owning this UOWTask
        self.uowtransaction = uowtransaction
        
        # the Mapper which this UOWTask corresponds to
        self.mapper = mapper
        
        # a dictionary mapping object instances to a corresponding UOWTaskElement.
        # Each UOWTaskElement represents one instance which is to be saved or 
        # deleted by this UOWTask's Mapper.
        # in the case of the row-based "circular sort", the UOWTaskElement may
        # also reference further UOWTasks which are dependent on that UOWTaskElement.
        self.objects = util.OrderedDict()
        
        # a list of UOWDependencyProcessors which are executed after saves and
        # before deletes, to synchronize data to dependent objects
        self.dependencies = sets.Set()

        # a list of UOWTasks that are dependent on this UOWTask, which 
        # are to be executed after this UOWTask performs saves and post-save
        # dependency processing, and before pre-delete processing and deletes
        self.childtasks = []
        
        # a list of UOWTasks that correspond to Mappers which are inheriting
        # mappers of this UOWTask's Mapper
        #self.inheriting_tasks = sets.Set()

        # whether this UOWTask is circular, meaning it holds a second
        # UOWTask that contains a special row-based dependency structure.
        self.circular = None

        # for a task thats part of that row-based dependency structure, points
        # back to the "public facing" task.
        self.circular_parent = circular_parent
        
        # a list of UOWDependencyProcessors are derived from the main
        # set of dependencies, referencing sub-UOWTasks attached to this
        # one which represent portions of the total list of objects.
        # this is used for the row-based "circular sort"
        self.cyclical_dependencies = sets.Set()
        
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

    def _inheriting_tasks(self):
        """returns a collection of UOWTasks whos mappers are immediate descendants of this UOWTask's mapper,
        *or* are descendants of this UOWTask's mapper where the intervening anscestor mappers do not have
        corresponding UOWTasks in the current UOWTransaction.
        
        Consider mapper A, which has descendant mappers B1 and B2.  B1 has descendant mapper C1, B2 has descendant 
        mapper C2.  UOWTasks are present for mappers A, B1, C1 and C2.
        
            A->
                B1->C1
                (B2)->C2
                
        calling inheriting_tasks for A's UOWTask yields B1, C2.  calling inheriting_tasks for B1's UOWTask yields C1.        
        """
        if self.circular_parent is not None:
            return
        def _tasks_by_mapper(mapper):
            for m in mapper._inheriting_mappers:
                inherit_task = self.uowtransaction.tasks.get(m, None)
                if inherit_task is not None:
                    yield inherit_task
                else:
                    for t in _tasks_by_mapper(m):
                        yield t
        for t in _tasks_by_mapper(self.mapper):
            yield t
    inheriting_tasks = property(_inheriting_tasks)
    
    def polymorphic_tasks(self):
        """returns a collection of all UOWTasks whos mappers are descendants of this UOWTask's mapper."""
        yield self
        for task in self.inheriting_tasks:
            for t in task.polymorphic_tasks():
                yield t
                
    def contains_object(self, obj, polymorphic=False):
        if obj in self.objects:
            return True
        if polymorphic:
            for task in self.inheriting_tasks:
                if task.contains_object(obj, polymorphic=True):
                    return True
        return False
        
    def get_elements(self, polymorphic=False):
        for rec in self.objects.values():
            yield rec
        if polymorphic:
            for task in self.inheriting_tasks:
                for rec in task.get_elements(polymorphic=True):
                    yield rec
    
    polymorphic_tosave_elements = property(lambda self: [rec for rec in self.get_elements(polymorphic=True) if not rec.isdelete])
    polymorphic_todelete_elements = property(lambda self: [rec for rec in self.get_elements(polymorphic=True) if rec.isdelete])
    tosave_elements = property(lambda self: [rec for rec in self.get_elements(polymorphic=False) if not rec.isdelete])
    todelete_elements = property(lambda self:[rec for rec in self.get_elements(polymorphic=False) if rec.isdelete])
    tosave_objects = property(lambda self:[rec.obj for rec in self.get_elements(polymorphic=False) if rec.obj is not None and not rec.listonly and rec.isdelete is False])
    todelete_objects = property(lambda self:[rec.obj for rec in self.get_elements(polymorphic=False) if rec.obj is not None and not rec.listonly and rec.isdelete is True])
        
    def _sort_circular_dependencies(self, trans, cycles):
        """for a single task, creates a hierarchical tree of "subtasks" which associate
        specific dependency actions with individual objects.  This is used for a
        "cyclical" task, or a task where elements
        of its object list contain dependencies on each other.
        
        this is not the normal case; this logic only kicks in when something like 
        a hierarchical tree is being represented."""
        allobjects = []
        for task in cycles:
            allobjects += [e.obj for e in task.get_elements(polymorphic=True)]
        tuples = []
        
        cycles = sets.Set(cycles)
        
        #print "BEGIN CIRC SORT-------"
        #print "PRE-CIRC:"
        #print list(cycles)[0].dump()
        
        # dependency processors that arent part of the cyclical thing
        # get put here
        extradeplist = []
        
        # organizes a set of new UOWTasks that will be assembled into
        # the final tree, for the purposes of holding new UOWDependencyProcessors
        # which process small sub-sections of dependent parent/child operations
        dependencies = {}
        def get_dependency_task(obj, depprocessor):
            try:
                dp = dependencies[obj]
            except KeyError:
                dp = dependencies.setdefault(obj, {})
            try:
                l = dp[depprocessor]
            except KeyError:
                l = UOWTask(self.uowtransaction, depprocessor.targettask.mapper, circular_parent=self)
                dp[depprocessor] = l
            return l

        def dependency_in_cycles(dep):
            # TODO: make a simpler way to get at the "root inheritance" mapper
            proctask = trans.get_task_by_mapper(dep.processor.mapper.primary_mapper().base_mapper(), True)
            targettask = trans.get_task_by_mapper(dep.targettask.mapper.base_mapper(), True)
            return targettask in cycles and (proctask is not None and proctask in cycles)
            
        # organize all original UOWDependencyProcessors by their target task
        deps_by_targettask = {}
        for t in cycles:
            for task in t.polymorphic_tasks():
                for dep in task.dependencies:
                    if not dependency_in_cycles(dep):
                        extradeplist.append(dep)
                    for t in dep.targettask.polymorphic_tasks():
                        l = deps_by_targettask.setdefault(t, [])
                        l.append(dep)

        object_to_original_task = {}
        
        for t in cycles:
            for task in t.polymorphic_tasks():
                for taskelement in task.get_elements(polymorphic=False):
                    obj = taskelement.obj
                    object_to_original_task[obj] = task
                    #print "OBJ", repr(obj), "TASK", repr(task)
                    
                    for dep in deps_by_targettask.get(task, []):
                        # is this dependency involved in one of the cycles ?
                        #print "DEP iterate", dep.processor.key, dep.processor.parent, dep.processor.mapper
                        if not dependency_in_cycles(dep):
                            #print "NOT IN CYCLE"
                            continue
                        #print "DEP", dep.processor.key    
                        (processor, targettask) = (dep.processor, dep.targettask)
                        isdelete = taskelement.isdelete
                    
                        # list of dependent objects from this object
                        childlist = dep.get_object_dependencies(obj, trans, passive = True)
                    
                        # the task corresponding to saving/deleting of those dependent objects
                        childtask = trans.get_task_by_mapper(processor.mapper.primary_mapper())
                    
                        childlist = childlist.added_items() + childlist.unchanged_items() + childlist.deleted_items()
                        
                        for o in childlist:
                            if o is None or not childtask.contains_object(o, polymorphic=True):
                                continue
                            #print "parent/child", obj, o
                            whosdep = dep.whose_dependent_on_who(obj, o)
                            #print "WHOSEDEP", dep.processor.key, dep.processor.direction, whosdep
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
        
        #print "TUPLES", tuples
        head = DependencySorter(tuples, allobjects).sort()
        if head is None:
            return None

        #print str(head)

        # create a tree of UOWTasks corresponding to the tree of object instances
        # created by the DependencySorter
        def make_task_tree(node, parenttask, nexttasks):
            #print "MAKETASKTREE", node.item, parenttask
            originating_task = object_to_original_task[node.item]
            t = nexttasks.get(originating_task, None)
            if t is None:
                t = UOWTask(self.uowtransaction, originating_task.mapper, circular_parent=self)
                nexttasks[originating_task] = t
                parenttask.append(None, listonly=False, isdelete=originating_task.objects[node.item].isdelete, childtask=t)
            t.append(node.item, originating_task.objects[node.item].listonly, isdelete=originating_task.objects[node.item].isdelete)
                
            if dependencies.has_key(node.item):
                for depprocessor, deptask in dependencies[node.item].iteritems():
                    t.cyclical_dependencies.add(depprocessor.branch(deptask))
            nd = {}
            for n in node.children:
                t2 = make_task_tree(n, t, nd)
            return t

        # this is the new "circular" UOWTask which will execute in place of "self"
        t = UOWTask(self.uowtransaction, self.mapper, circular_parent=self)

        # stick the non-circular dependencies and child tasks onto the new
        # circular UOWTask
        [t.dependencies.add(d) for d in extradeplist]
        t.childtasks = self.childtasks
        make_task_tree(head, t, {})
        #print t.dump()
        return t

    def dump(self):
        buf = StringIO.StringIO()
        import uowdumper
        uowdumper.UOWDumper(self, buf)
        return buf.getvalue()
        
        
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

