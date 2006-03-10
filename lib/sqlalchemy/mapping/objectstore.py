# objectstore.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php


"""maintains all currently loaded objects in memory, using the "identity map" pattern.  Also
provides a "unit of work" object which tracks changes to objects so that they may be properly
persisted within a transactional scope."""

import thread
import sqlalchemy
import sqlalchemy.util as util
import sqlalchemy.attributes as attributes
from sqlalchemy.exceptions import *
import topological
import weakref
import string
import StringIO
from sets import *

__all__ = ['get_id_key', 'get_row_key', 'is_dirty', 'import_instance', 'commit', 'update', 'clear', 'delete', 'instance_key', 'begin', 'has_key', 'has_instance', 'UnitOfWork']

# a global indicating if all commit() operations should have their plan
# printed to standard output.  also can be affected by creating an engine
# with the "echo_uow=True" keyword argument.
LOG = False
    
class Session(object):
    """Maintains a UnitOfWork instance, including transaction state."""
    
    def __init__(self, nest_transactions=False, hash_key=None):
        """Initialize the objectstore with a UnitOfWork registry.  If called
        with no arguments, creates a single UnitOfWork for all operations.
        
        nest_transactions - indicates begin/commit statements can be executed in a
        "nested", defaults to False which indicates "only commit on the outermost begin/commit"
        hash_key - the hash_key used to identify objects against this session, which 
        defaults to the id of the Session instance.
        """
        self.uow = UnitOfWork()
        self.parent_uow = None
        self.begin_count = 0
        self.nest_transactions = nest_transactions
        if hash_key is None:
            self.hash_key = id(self)
        else:
            self.hash_key = hash_key
        _sessions[self.hash_key] = self
        
    def get_id_key(ident, class_):
        """returns an identity-map key for use in storing/retrieving an item from the identity
        map, given a tuple of the object's primary key values.

        ident - a tuple of primary key values corresponding to the object to be stored.  these
        values should be in the same order as the primary keys of the table 

        class_ - a reference to the object's class

        table - a Table object where the object's primary fields are stored.

        selectable - a Selectable object which represents all the object's column-based fields.
        this Selectable may be synonymous with the table argument or can be a larger construct
        containing that table. return value: a tuple object which is used as an identity key. """
        return (class_, tuple(ident))
    get_id_key = staticmethod(get_id_key)

    def get_row_key(row, class_, primary_key):
        """returns an identity-map key for use in storing/retrieving an item from the identity
        map, given a result set row.

        row - a sqlalchemy.dbengine.RowProxy instance or other map corresponding result-set
        column names to their values within a row.

        class_ - a reference to the object's class

        table - a Table object where the object's primary fields are stored.

        selectable - a Selectable object which represents all the object's column-based fields.
        this Selectable may be synonymous with the table argument or can be a larger construct
        containing that table. return value: a tuple object which is used as an identity key.
        """
        return (class_, tuple([row[column] for column in primary_key]))
    get_row_key = staticmethod(get_row_key)

    class SessionTrans(object):
        """returned by Session.begin(), denotes a transactionalized UnitOfWork instance.
        call commit() on this to commit the transaction."""
        def __init__(self, parent, uow, isactive):
            self.__parent = parent
            self.__isactive = isactive
            self.__uow = uow
        isactive = property(lambda s:s.__isactive, doc="True if this SessionTrans is the 'active' transaction marker, else its a no-op.")
        parent = property(lambda s:s.__parent, doc="returns the parent Session of this SessionTrans object.")
        uow = property(lambda s:s.__uow, doc="returns the parent UnitOfWork corresponding to this transaction.")
        def begin(self):
            """calls begin() on the underlying Session object, returning a new no-op SessionTrans object."""
            return self.parent.begin()
        def commit(self):
            """commits the transaction noted by this SessionTrans object."""
            self.__parent._trans_commit(self)
            self.__isactive = False
        def rollback(self):
            """rolls back the current UnitOfWork transaction, in the case that begin()
            has been called.  The changes logged since the begin() call are discarded."""
            self.__parent._trans_rollback(self)
            self.__isactive = False

    def begin(self):
        """begins a new UnitOfWork transaction and returns a tranasaction-holding
        object.  commit() or rollback() should be called on the returned object.
        commit() on the Session will do nothing while a transaction is pending, and further
        calls to begin() will return no-op transactional objects."""
        if self.parent_uow is not None:
            return Session.SessionTrans(self, self.uow, False)
        self.parent_uow = self.uow
        self.uow = UnitOfWork(identity_map = self.uow.identity_map)
        return Session.SessionTrans(self, self.uow, True)
    
    def _trans_commit(self, trans):
        if trans.uow is self.uow and trans.isactive:
            try:
                self.uow.commit()
            finally:
                self.uow = self.parent_uow
                self.parent_uow = None
    def _trans_rollback(self, trans):
        if trans.uow is self.uow:
            self.uow = self.parent_uow
            self.parent_uow = None
                        
    def commit(self, *objects):
        """commits the current UnitOfWork transaction.  called with
        no arguments, this is only used
        for "implicit" transactions when there was no begin().
        if individual objects are submitted, then only those objects are committed, and the 
        begin/commit cycle is not affected."""
        # if an object list is given, commit just those but dont
        # change begin/commit status
        if len(objects):
            self.uow.commit(*objects)
            return
        if self.parent_uow is None:
            self.uow.commit()

    def refresh(self, *obj):
        """reloads the attributes for the given objects from the database, clears
        any changes made."""
        for o in obj:
            self.uow.refresh(o)

    def expire(self, *obj):
        """invalidates the data in the given objects and sets them to refresh themselves
        the next time they are requested."""
        for o in obj:
            global_attributes.trigger_history(o, lambda: refresh(o))

    def register_clean(self, obj):
        self._bind_to(obj)
        self.uow.register_clean(obj)
        
    def register_new(self, obj):
        self._bind_to(obj)
        self.uow.register_new(obj)

    def _bind_to(self, obj):
        """given an object, binds it to this session.  changes on the object will affect
        the currently scoped UnitOfWork maintained by this session."""
        obj._sa_session_id = self.hash_key

    def __getattr__(self, key):
        """proxy other methods to our underlying UnitOfWork"""
        return getattr(self.uow, key)

    def clear(self):
        self.uow = UnitOfWork()

    def delete(self, *obj):
        """registers the given objects as to be deleted upon the next commit"""
        for o in obj:
            self.uow.register_deleted(o)
        
    def import_instance(self, instance):
        """places the given instance in the current thread's unit of work context,
        either in the current IdentityMap or marked as "new".  Returns either the object
        or the current corresponding version in the Identity Map.

        this method should be used for any object instance that is coming from a serialized
        storage, from another thread (assuming the regular threaded unit of work model), or any
        case where the instance was loaded/created corresponding to a different base unitofwork
        than the current one."""
        if instance is None:
            return None
        key = getattr(instance, '_instance_key', None)
        mapper = object_mapper(instance)
        u = self.uow
        if key is not None:
            if u.identity_map.has_key(key):
                return u.identity_map[key]
            else:
                instance._instance_key = key
                u.identity_map[key] = instance
                self._bind_to(instance)
        else:
            u.register_new(instance)
        return instance

def get_id_key(ident, class_):
    return Session.get_id_key(ident, class_)

def get_row_key(row, class_, primary_key):
    return Session.get_row_key(row, class_, primary_key)

def begin():
    """begins a new UnitOfWork transaction.  the next commit will affect only
    objects that are created, modified, or deleted following the begin statement."""
    return get_session().begin()

def commit(*obj):
    """commits the current UnitOfWork transaction.  if a transaction was begun 
    via begin(), commits only those objects that were created, modified, or deleted
    since that begin statement.  otherwise commits all objects that have been
    changed.
    
    if individual objects are submitted, then only those objects are committed, and the 
    begin/commit cycle is not affected."""
    get_session().commit(*obj)

def clear():
    """removes all current UnitOfWorks and IdentityMaps for this thread and 
    establishes a new one.  It is probably a good idea to discard all
    current mapped object instances, as they are no longer in the Identity Map."""
    get_session().clear()

def refresh(*obj):
    """reloads the state of this object from the database, and cancels any in-memory
    changes."""
    get_session().refresh(*obj)

def expire(*obj):
    """invalidates the data in the given objects and sets them to refresh themselves
    the next time they are requested."""
    get_session().expire(*obj)
    
def delete(*obj):
    """registers the given objects as to be deleted upon the next commit"""
    s = get_session().delete(*obj)

def has_key(key):
    """returns True if the current thread-local IdentityMap contains the given instance key"""
    return get_session().has_key(key)

def has_instance(instance):
    """returns True if the current thread-local IdentityMap contains the given instance"""
    return get_session().has_instance(instance)

def is_dirty(obj):
    """returns True if the given object is in the current UnitOfWork's new or dirty list,
    or if its a modified list attribute on an object."""
    return get_session().is_dirty(obj)

def instance_key(instance):
    """returns the IdentityMap key for the given instance"""
    return get_session().instance_key(instance)

def import_instance(instance):
    return get_session().import_instance(instance)

class UOWProperty(attributes.SmartProperty):
    def __init__(self, class_, *args, **kwargs):
        super(UOWProperty, self).__init__(*args, **kwargs)
        self.class_ = class_
    property = property(lambda s:class_mapper(s.class_).props[s.key], doc="returns the MapperProperty object associated with this property")
    
class UOWListElement(attributes.ListElement):
    def __init__(self, obj, key, data=None, deleteremoved=False, **kwargs):
        attributes.ListElement.__init__(self, obj, key, data=data, **kwargs)
        self.deleteremoved = deleteremoved
    def list_value_changed(self, obj, key, item, listval, isdelete):
        sess = get_session(obj)
        if not isdelete and sess.deleted.contains(item):
            raise InvalidRequestError("re-inserting a deleted value into a list")
        sess.modified_lists.append(self)
        if self.deleteremoved and isdelete:
            sess.register_deleted(item)
    def append(self, item, _mapper_nohistory = False):
        if _mapper_nohistory:
            self.append_nohistory(item)
        else:
            attributes.ListElement.append(self, item)
            
class UOWAttributeManager(attributes.AttributeManager):
    def __init__(self):
        attributes.AttributeManager.__init__(self)
        
    def value_changed(self, obj, key, value):
        if hasattr(obj, '_instance_key'):
            get_session(obj).register_dirty(obj)
        else:
            get_session(obj).register_new(obj)
            
    def create_prop(self, class_, key, uselist, **kwargs):
        return UOWProperty(class_, self, key, uselist)

    def create_list(self, obj, key, list_, **kwargs):
        return UOWListElement(obj, key, list_, **kwargs)
        
class UnitOfWork(object):
    def __init__(self, identity_map=None):
        if identity_map is not None:
            self.identity_map = identity_map
        else:
            self.identity_map = weakref.WeakValueDictionary()
            
        self.attributes = global_attributes
        self.new = util.HashSet(ordered = True)
        self.dirty = util.HashSet()
        self.modified_lists = util.HashSet()
        self.deleted = util.HashSet()

    def get(self, class_, *id):
        """given a class and a list of primary key values in their table-order, locates the mapper 
        for this class and calls get with the given primary key values."""
        return object_mapper(class_).get(*id)

    def _get(self, key):
        return self.identity_map[key]
        
    def _put(self, key, obj):
        self.identity_map[key] = obj

    def refresh(self, obj):
        self.rollback_object(obj)
        object_mapper(obj)._get(obj._instance_key, reload=True)

    def has_key(self, key):
        """returns True if the given key is present in this UnitOfWork's identity map."""
        return self.identity_map.has_key(key)
    
    def is_dirty(self, obj):
        """returns True if the given object is in the dirty, new, modified_lists, or deleted lists of 
        this UnitOfWork."""
        return obj in self.dirty or obj in self.new or obj in self.modified_lists or obj in self.deleted
            
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
        self.attributes.commit(obj)
        self.attributes.remove(obj)

    def _validate_obj(self, obj):
        """validates that dirty/delete/commit operations can occur upon the given object, by checking
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
        if not self.new.contains(obj):
            self.new.append(obj)
        
    def register_dirty(self, obj):
        if not self.dirty.contains(obj):
            self._validate_obj(obj)
            self.dirty.append(obj)
            
    def is_dirty(self, obj):
        if not self.dirty.contains(obj):
            return False
        else:
            return True
        
    def register_deleted(self, obj):
        if not self.deleted.contains(obj):
            self._validate_obj(obj)
            self.deleted.append(obj)  
            mapper = object_mapper(obj)
            # TODO: should the cascading delete dependency thing
            # happen wtihin PropertyLoader.process_dependencies ?
            mapper.register_deleted(obj, self)

    def unregister_deleted(self, obj):
        try:
            self.deleted.remove(obj)
        except KeyError:
            pass
            
    def commit(self, *objects):
        commit_context = UOWTransaction(self)

        if len(objects):
            objset = util.HashSet(iter=objects)
        else:
            objset = None

        for obj in [n for n in self.new] + [d for d in self.dirty]:
            if objset is not None and not objset.contains(obj):
                continue
            if self.deleted.contains(obj):
                continue
            commit_context.register_object(obj)
        for item in self.modified_lists:
            obj = item.obj
            if objset is not None and not objset.contains(obj):
                continue
            if self.deleted.contains(obj):
                continue
            commit_context.register_object(obj, listonly = True)
            commit_context.register_saved_history(item)

#            for o in item.added_items() + item.deleted_items():
#                if self.deleted.contains(o):
#                    continue
#                commit_context.register_object(o, listonly=True)
                     
        for obj in self.deleted:
            if objset is not None and not objset.contains(obj):
                continue
            commit_context.register_object(obj, isdelete=True)
                
        engines = util.HashSet()
        for mapper in commit_context.mappers:
            for e in mapper.engines:
                engines.append(e)
        
        echo_commit = False        
        for e in engines:
            echo_commit = echo_commit or e.echo_uow
            e.begin()
        try:
            commit_context.execute(echo=echo_commit)
        except:
            for e in engines:
                e.rollback()
            raise
        for e in engines:
            e.commit()
            
        commit_context.post_exec()
        

    def rollback_object(self, obj):
        """'rolls back' the attributes that have been changed on an object instance."""
        self.attributes.rollback(obj)
            
class UOWTransaction(object):
    """handles the details of organizing and executing transaction tasks 
    during a UnitOfWork object's commit() operation."""
    def __init__(self, uow):
        self.uow = uow

        #  unique list of all the mappers we come across
        self.mappers = util.HashSet()
        self.dependencies = {}
        self.tasks = {}
        self.saved_histories = util.HashSet()
        self.__modified = False
        
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
            self.__modified = self.__modified or mod
            return
            
        # for a cyclical task, things need to be sorted out already,
        # so this object should have already been added to the appropriate sub-task
        # can put an assertion here to make sure....
        if task.circular:
            return
            
        mod = task.append(obj, listonly, isdelete=isdelete, **kwargs)
        self.__modified = self.__modified or mod

    def unregister_object(self, obj):
        mapper = object_mapper(obj)
        task = self.get_task_by_mapper(mapper)
        task.delete(obj)
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
            return UOWTask(self, mapper)
            
    def register_dependency(self, mapper, dependency):
        """called by mapper.PropertyLoader to register the objects handled by
        one mapper being dependent on the objects handled by another."""
        # correct for primary mapper (the mapper offcially associated with the class)
        self.dependencies[(mapper._primary_mapper(), dependency._primary_mapper())] = True
        self.__modified = True

    def register_processor(self, mapper, processor, mapperfrom, isdeletefrom):
        """called by mapper.PropertyLoader to register itself as a "processor", which
        will be associated with a particular UOWTask, and be given a list of "dependent"
        objects corresponding to another UOWTask to be processed, either after that secondary
        task saves its objects or before it deletes its objects."""
        # when the task from "mapper" executes, take the objects from the task corresponding
        # to "mapperfrom"'s list of save/delete objects, and send them to "processor"
        # for dependency processing
        #print "registerprocessor", str(mapper), repr(processor.key), str(mapperfrom), repr(isdeletefrom)
        
        # correct for primary mapper (the mapper offcially associated with the class)
        mapper = mapper._primary_mapper()
        mapperfrom = mapperfrom._primary_mapper()
        task = self.get_task_by_mapper(mapper)
        targettask = self.get_task_by_mapper(mapperfrom)
        task.dependencies.append(UOWDependencyProcessor(processor, targettask, isdeletefrom))
        self.__modified = True

    def register_saved_history(self, listobj):
        self.saved_histories.append(listobj)

    def execute(self, echo=False):
        for task in self.tasks.values():
            task.mapper.register_dependencies(self)

        head = self._sort_dependencies()
        self.__modified = False
        if LOG or echo:
            if head is None:
                print "Task dump: None"
            else:
                print "Task dump:\n" + head.dump()
        if head is not None:
            head.execute(self)
        if LOG or echo:
            if self.__modified and head is not None:
                print "\nAfter Execute:\n" + head.dump()
            else:
                print "\nExecute complete (no post-exec changes)\n"
            
    def post_exec(self):
        """after an execute/commit is completed, all of the objects and lists that have
        been committed are updated in the parent UnitOfWork object to mark them as clean."""
        
        for task in self.tasks.values():
            for elem in task.objects.values():
                if elem.isdelete:
                    self.uow._remove_deleted(elem.obj)
                else:
                    self.uow.register_clean(elem.obj)
                
        for obj in self.saved_histories:
            try:
                obj.commit()
                del self.uow.modified_lists[obj]
            except KeyError:
                pass

    # this assertion only applies to a full commit(), not a
    # partial one
        #if len(self.uow.new) > 0 or len(self.uow.dirty) >0 or len(self.uow.modified_lists) > 0:
        #    raise "assertion failed"

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
    
        head = DependencySorter(self.dependencies, mappers).sort(allow_all_cycles=True)
        #print str(head)
        task = sort_hier(head)
        return task


class UOWTaskElement(object):
    """an element within a UOWTask.  corresponds to a single object instance
    to be saved, deleted, or just part of the transaction as a placeholder for 
    further dependencies (i.e. 'listonly').
    in the case of self-referential mappers, may also store a "childtask", which is a
    UOWTask containing objects dependent on this element's object instance."""
    def __init__(self, obj):
        self.obj = obj
        self.listonly = True
        self.childtasks = []
        self.isdelete = False
        self.mapper = None
    def __repr__(self):
        return "UOWTaskElement/%d: %s/%d %s" % (id(self), self.obj.__class__.__name__, id(self.obj), (self.listonly and 'listonly' or (self.isdelete and 'delete' or 'save')) )

class UOWDependencyProcessor(object):
    """in between the saving and deleting of objects, process "dependent" data, such as filling in 
    a foreign key on a child item from a new primary key, or deleting association rows before a 
    delete."""
    def __init__(self, processor, targettask, isdeletefrom):
        self.processor = processor
        self.targettask = targettask
        self.isdeletefrom = isdeletefrom
    
    def execute(self, trans, delete):
        if not delete:
            self.processor.process_dependencies(self.targettask, [elem.obj for elem in self.targettask.tosave_elements() if elem.obj is not None], trans, delete = delete)
        else:            
            self.processor.process_dependencies(self.targettask, [elem.obj for elem in self.targettask.todelete_elements() if elem.obj is not None], trans, delete = delete)

    def get_object_dependencies(self, obj, trans, passive):
        return self.processor.get_object_dependencies(obj, trans, passive=passive)

    def whose_dependent_on_who(self, obj, o):        
        return self.processor.whose_dependent_on_who(obj, o)

    def branch(self, task):
        return UOWDependencyProcessor(self.processor, task, self.isdeletefrom)

class UOWTask(object):
    def __init__(self, uowtransaction, mapper):
        if uowtransaction is not None:
            uowtransaction.tasks[mapper] = self
        self.uowtransaction = uowtransaction
        self.mapper = mapper
        self.objects = util.OrderedDict()
        self.dependencies = []
        self.cyclical_dependencies = []
        self.circular = None
        self.postcircular = None
        self.childtasks = []
#        print "NEW TASK", repr(self)
        
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
        
    def execute(self, trans):
        """executes this UOWTask.  saves objects to be saved, processes all dependencies
        that have been registered, and deletes objects to be deleted. """
        if self.circular is not None:
            self.circular.execute(trans)
            return

        self.mapper.save_obj(self.tosave_objects(), trans)
        for dep in self.cyclical_save_dependencies():
            dep.execute(trans, delete=False)
        for element in self.tosave_elements():
            for task in element.childtasks:
                task.execute(trans)
        for dep in self.save_dependencies():
            dep.execute(trans, delete=False)
        for dep in self.delete_dependencies():
            dep.execute(trans, delete=True)
        for dep in self.cyclical_delete_dependencies():
            dep.execute(trans, delete=True)
        for child in self.childtasks:
            child.execute(trans)
        for element in self.todelete_elements():
            for task in element.childtasks:
                task.execute(trans)
        self.mapper.delete_obj(self.todelete_objects(), trans)

    def tosave_elements(self):
        return [rec for rec in self.objects.values() if not rec.isdelete]
    def todelete_elements(self):
        return [rec for rec in self.objects.values() if rec.isdelete]
    def tosave_objects(self):
        return [rec.obj for rec in self.objects.values() if rec.obj is not None and not rec.listonly and rec.isdelete is False]
    def todelete_objects(self):
        return [rec.obj for rec in self.objects.values() if rec.obj is not None and not rec.listonly and rec.isdelete is True]
    def save_dependencies(self):
        return [dep for dep in self.dependencies if not dep.isdeletefrom]
    def cyclical_save_dependencies(self):
        return [dep for dep in self.cyclical_dependencies if not dep.isdeletefrom]
    def delete_dependencies(self):
        return [dep for dep in self.dependencies if dep.isdeletefrom]
    def cyclical_delete_dependencies(self):
        return [dep for dep in self.cyclical_dependencies if dep.isdeletefrom]
        
    def _sort_circular_dependencies(self, trans, cycles):
        """for a single task, creates a hierarchical tree of "subtasks" which associate
        specific dependency actions with individual objects.  This is used for a
        "cyclical" task, or a task where elements
        of its object list contain dependencies on each other.
        
        this is not the normal case; this logic only kicks in when something like 
        a hierarchical tree is being represented."""

        allobjects = []
        for task in cycles:
            allobjects += task.objects.keys()
        tuples = []
        
        objecttotask = {}

        cycles = Set(cycles)
        
        # dependency processors that arent part of the cyclical thing
        # get put here
        extradeplist = []

        def get_object_task(parent, obj):
            try:
                return objecttotask[obj]
            except KeyError:
                t = UOWTask(None, parent.mapper)
                t.parent = parent
                objecttotask[obj] = t
                return t

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

        # work out a list of all the "dependency processors" that 
        # represent objects that have to be dependency sorted at the 
        # per-object level.  all other dependency processors go in
        # "extradep."
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
                #print "OBJ", repr(obj), "TASK", repr(task)
                
                # create a placeholder UOWTask that may be built into the final
                # task tree
                get_object_task(task, obj)
                for dep in deps_by_targettask.get(task, []):
                    (processor, targettask, isdelete) = (dep.processor, dep.targettask, dep.isdeletefrom)
                    if taskelement.isdelete is not dep.isdeletefrom:
                        continue
                    #print "GETING LIST OFF PROC", processor.key, "OBJ", repr(obj)

                    # traverse through the modified child items of each object.  normally this
                    # is done via PropertyLoader in properties.py, but we need all the info
                    # up front here to do the object-level topological sort.
                    
                    # list of dependent objects from this object
                    childlist = dep.get_object_dependencies(obj, trans, passive = True)
                    # the task corresponding to the processor's objects
                    childtask = trans.get_task_by_mapper(processor.mapper)
                    # is this dependency involved in one of the cycles ?
                    cyclicaldep = dep.targettask in cycles and trans.get_task_by_mapper(dep.processor.mapper) in cycles
                    if isdelete:
                        childlist = childlist.unchanged_items() + childlist.deleted_items()
                    else:
                        childlist = childlist.added_items()
                        
                    for o in childlist:
                        if not o in childtask.objects:
                            # item needs to be saved since its added, or attached to a deleted object
                            childtask.append(o, isdelete=isdelete and dep.processor.private)
                            if cyclicaldep:
                                # cyclical, so create a placeholder UOWTask that may be built into the
                                # final task tree
                                t = get_object_task(childtask, o)
                        if not cyclicaldep:
                            # not cyclical, so we are done with this
                            continue
                        # cyclical, so create an ordered pair for the dependency sort
                        whosdep = dep.whose_dependent_on_who(obj, o)
                        if whosdep is not None:
                            tuples.append(whosdep)
                            # then locate a UOWDependencyProcessor to add the object onto, which
                            # will handle the modifications between saves/deletes
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

        def make_task_tree(node, parenttask):
            """takes a dependency-sorted tree of objects and creates a tree of UOWTasks"""
            t = objecttotask[node.item]
            can_add_to_parent = t.mapper is parenttask.mapper
            if can_add_to_parent:
                parenttask.append(node.item, t.parent.objects[node.item].listonly, isdelete=t.parent.objects[node.item].isdelete, childtask=t)
            else:
                t.append(node.item, t.parent.objects[node.item].listonly, isdelete=t.parent.objects[node.item].isdelete)
                parenttask.append(None, listonly=False, isdelete=t.parent.objects[node.item].isdelete, childtask=t)
            if dependencies.has_key(node.item):
                for depprocessor, deptask in dependencies[node.item].iteritems():
                    if can_add_to_parent:
                        parenttask.cyclical_dependencies.append(depprocessor.branch(deptask))
                    else:
                        t.cyclical_dependencies.append(depprocessor.branch(deptask))
            for n in node.children:
                t2 = make_task_tree(n, t)
            return t

        # this is the new "circular" UOWTask which will execute in place of "self"
        t = UOWTask(None, self.mapper)

        # stick the non-circular dependencies and child tasks onto the new
        # circular UOWTask
        t.dependencies += [d for d in extradeplist]
        t.childtasks = self.childtasks
        make_task_tree(head, t)
        return t

    def dump(self):
        buf = StringIO.StringIO()
        self._dump(buf)
        return buf.getvalue()
        
    def _dump(self, buf, indent=0, circularparent=None):

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
            
        def _dump_processor(proc):
            if proc.isdeletefrom:
                val = [t for t in proc.targettask.objects.values() if t.isdelete]
            else:
                val = [t for t in proc.targettask.objects.values() if not t.isdelete]

            buf.write(_indent() + "  |- UOWDependencyProcessor(%d) %s attribute on %s (%s)\n" % (
                id(proc), 
                repr(proc.processor.key), 
                    (proc.isdeletefrom and 
                        "%s's to be deleted" % _repr_task_class(proc.targettask) 
                        or "saved %s's" % _repr_task_class(proc.targettask)), 
                _repr_task(proc.targettask))
            )
            
            if len(val) == 0:
                buf.write(_indent() + "  |       |-" + "(no objects)\n")
            for v in val:
                buf.write(_indent() + "  |       |-" + _repr_task_element(v) + "\n")
        
        def _repr_task_element(te):
            if te.obj is None:
                objid = "(placeholder)"
            else:
                objid = "%s(%d)" % (te.obj.__class__.__name__, id(te.obj))
            return "UOWTaskElement(%d): %s %s%s" % (id(te), objid, (te.listonly and '(listonly)' or (te.isdelete and '(delete' or '(save')),
            (te.mapper is not None and " w/ " + str(te.mapper) + ")" or ")")
            )
                
        def _repr_task(task):
            if task.mapper is not None:
                if task.mapper.__class__.__name__ == 'Mapper':
                    name = task.mapper.class_.__name__ + "/" + task.mapper.primarytable.id + "/" + str(id(task.mapper))
                else:
                    name = repr(task.mapper)
            else:
                name = '(none)'
            return ("UOWTask(%d) '%s'" % (id(task), name))
        def _repr_task_class(task):
            if task.mapper is not None and task.mapper.__class__.__name__ == 'Mapper':
                return task.mapper.class_.__name__
            else:
                return '(none)'

        def _repr(obj):
            return "%s(%d)" % (obj.__class__.__name__, id(obj))

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
        for rec in self.tosave_elements():
            if rec.listonly:
                continue
            header(buf, _indent() + "  |- Save elements\n")
            buf.write(_indent() + "  |- Save: " + _repr_task_element(rec) + "\n")
        for dep in self.cyclical_save_dependencies():
            header(buf, _indent() + "  |- Cyclical Save dependencies\n")
            _dump_processor(dep)
        for element in self.tosave_elements():
            for task in element.childtasks:
                header(buf, _indent() + "  |- Save subelements of UOWTaskElement(%s)\n" % id(element))
                task._dump(buf, indent + 1)
        for dep in self.save_dependencies():
            header(buf, _indent() + "  |- Save dependencies\n")
            _dump_processor(dep)
        for dep in self.delete_dependencies():
            header(buf, _indent() + "  |- Delete dependencies\n")
            _dump_processor(dep)
        for dep in self.cyclical_delete_dependencies():
            header(buf, _indent() + "  |- Cyclical Delete dependencies\n")
            _dump_processor(dep)
        for child in self.childtasks:
            header(buf, _indent() + "  |- Child tasks\n")
            child._dump(buf, indent + 1)
#        for obj in self.postupdate:
#            header(buf, _indent() + "  |- Post Update objects\n")
#            buf.write(_repr(obj) + "\n")
        for element in self.todelete_elements():
            for task in element.childtasks:
                header(buf, _indent() + "  |- Delete subelements of UOWTaskElement(%s)\n" % id(element))
                task._dump(buf, indent + 1)

        for rec in self.todelete_elements():
            if rec.listonly:
                continue
            header(buf, _indent() + "  |- Delete elements\n")
            buf.write(_indent() + "  |- Delete: " + _repr_task_element(rec) + "\n")

        buf.write(_indent() + "  |----\n")
        buf.write(_indent() + "\n")           
        
    def __repr__(self):
        if self.mapper is not None:
            if self.mapper.__class__.__name__ == 'Mapper':
                name = self.mapper.class_.__name__ + "/" + self.mapper.primarytable.name
            else:
                name = repr(self.mapper)
        else:
            name = '(none)'
        return ("UOWTask(%d) Mapper: '%s'" % (id(self), name))

class DependencySorter(topological.QueueDependencySorter):
    pass

def mapper(*args, **params):
    return sqlalchemy.mapperlib.mapper(*args, **params)

def object_mapper(obj):
    return sqlalchemy.mapperlib.object_mapper(obj)

def class_mapper(class_):
    return sqlalchemy.mapperlib.class_mapper(class_)

global_attributes = UOWAttributeManager()

session_registry = util.ScopedRegistry(Session) # Default session registry
_sessions = weakref.WeakValueDictionary() # all referenced sessions (including user-created)

def get_session(obj=None):
    # object-specific session ?
    if obj is not None:
        # does it have a hash key ?
        hashkey = getattr(obj, '_sa_session_id', None)
        if hashkey is not None:
            # ok, return that
            try:
                return _sessions[hashkey]
            except KeyError:
                raise InvalidRequestError("Session '%s' referenced by object '%s' no longer exists" % (hashkey, repr(obj)))

    return session_registry()

uow = get_session # deprecated

def push_session(sess):
    old = get_session()
    sess._previous = old
    session_registry.set(sess)
    
def pop_session():
    sess = get_session()
    old = sess._previous
    sess._previous = None
    session_registry.set(old)
    return old
    
def using_session(sess, func):
    push_session(sess)
    try:
        return func()
    finally:
        pop_session()

