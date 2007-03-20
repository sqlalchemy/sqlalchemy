# orm/unitofwork.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The internals for the Unit Of Work system.

Includes hooks into the attributes package enabling the routing of
change events to Unit Of Work objects, as well as the flush()
mechanism which creates a dependency structure that executes change
operations.

A Unit of Work is essentially a system of maintaining a graph of
in-memory objects and their modified state.  Objects are maintained as
unique against their primary key identity using an *identity map*
pattern.  The Unit of Work then maintains lists of objects that are
new, dirty, or deleted and provides the capability to flush all those
changes at once.
"""

from sqlalchemy import util, logging, topological
from sqlalchemy.orm import attributes
from sqlalchemy.orm.mapper import object_mapper, class_mapper
from sqlalchemy.exceptions import *
import StringIO
import weakref
import sets

class UOWEventHandler(attributes.AttributeExtension):
    """An event handler added to all class attributes which handles
    session operations.
    """

    def __init__(self, key, class_, cascade=None):
        self.key = key
        self.class_ = class_
        self.cascade = cascade

    def append(self, event, obj, item):
        # process "save_update" cascade rules for when an instance is appended to the list of another instance
        sess = object_session(obj)
        if sess is not None:
            if self.cascade is not None and self.cascade.save_update and item not in sess:
                mapper = object_mapper(obj)
                prop = mapper.props[self.key]
                ename = prop.mapper.entity_name
                sess.save_or_update(item, entity_name=ename)

    def delete(self, event, obj, item):
        # currently no cascade rules for removing an item from a list
        # (i.e. it stays in the Session)
        pass

    def set(self, event, obj, newvalue, oldvalue):
        # process "save_update" cascade rules for when an instance is attached to another instance
        sess = object_session(obj)
        if sess is not None:
            if newvalue is not None and self.cascade is not None and self.cascade.save_update and newvalue not in sess:
                mapper = object_mapper(obj)
                prop = mapper.props[self.key]
                ename = prop.mapper.entity_name
                sess.save_or_update(newvalue, entity_name=ename)

class UOWProperty(attributes.InstrumentedAttribute):
    """Override ``InstrumentedAttribute`` to provide an extra
    ``AttributeExtension`` to all managed attributes as well as the
    `property` property.
    """

    def __init__(self, manager, class_, key, uselist, callable_, typecallable, cascade=None, extension=None, **kwargs):
        extension = util.to_list(extension or [])
        extension.insert(0, UOWEventHandler(key, class_, cascade=cascade))
        super(UOWProperty, self).__init__(manager, key, uselist, callable_, typecallable, extension=extension,**kwargs)
        self.class_ = class_

    property = property(lambda s:class_mapper(s.class_).props[s.key], doc="returns the MapperProperty object associated with this property")

class UOWAttributeManager(attributes.AttributeManager):
    """Override ``AttributeManager`` to provide the ``UOWProperty``
    instance for all ``InstrumentedAttributes``.
    """

    def create_prop(self, class_, key, uselist, callable_, typecallable, **kwargs):
        return UOWProperty(self, class_, key, uselist, callable_, typecallable, **kwargs)

class UnitOfWork(object):
    """Main UOW object which stores lists of dirty/new/deleted objects.

    Provides top-level *flush* functionality as well as the
    transaction boundaries with the SQLEngine(s) involved in a write
    operation.
    """

    def __init__(self, identity_map=None, weak_identity_map=False):
        if identity_map is not None:
            self.identity_map = identity_map
        else:
            if weak_identity_map:
                self.identity_map = weakref.WeakValueDictionary()
            else:
                self.identity_map = {}

        self.new = util.Set() #OrderedSet()
        self.deleted = util.Set()
        self.logger = logging.instance_logger(self)

    echo = logging.echo_property()

    def _remove_deleted(self, obj):
        if hasattr(obj, "_instance_key"):
            del self.identity_map[obj._instance_key]
        try:
            self.deleted.remove(obj)
        except KeyError:
            pass
        try:
            self.new.remove(obj)
        except KeyError:
            pass

    def _validate_obj(self, obj):
        if (hasattr(obj, '_instance_key') and not self.identity_map.has_key(obj._instance_key)) or \
            (not hasattr(obj, '_instance_key') and obj not in self.new):
            raise InvalidRequestError("Instance '%s' is not attached or pending within this session" % repr(obj))

    def _is_valid(self, obj):
        if (hasattr(obj, '_instance_key') and not self.identity_map.has_key(obj._instance_key)) or \
            (not hasattr(obj, '_instance_key') and obj not in self.new):
            return False
        else:
            return True

    def register_attribute(self, class_, key, uselist, **kwargs):
        attribute_manager.register_attribute(class_, key, uselist, **kwargs)

    def register_callable(self, obj, key, func, uselist, **kwargs):
        attribute_manager.set_callable(obj, key, func, uselist, **kwargs)

    def register_clean(self, obj):
        if obj in self.new:
            self.new.remove(obj)
        if not hasattr(obj, '_instance_key'):
            mapper = object_mapper(obj)
            obj._instance_key = mapper.instance_key(obj)
        if hasattr(obj, '_sa_insert_order'):
            delattr(obj, '_sa_insert_order')
        self.identity_map[obj._instance_key] = obj
        attribute_manager.commit(obj)

    def register_new(self, obj):
        if hasattr(obj, '_instance_key'):
            raise InvalidRequestError("Object '%s' already has an identity - it cant be registered as new" % repr(obj))
        if obj not in self.new:
            self.new.add(obj)
            obj._sa_insert_order = len(self.new)

    def register_deleted(self, obj):
        if obj not in self.deleted:
            self._validate_obj(obj)
            self.deleted.add(obj)

    def locate_dirty(self):
        return util.Set([x for x in self.identity_map.values() if x not in self.deleted and attribute_manager.is_modified(x)])

    def flush(self, session, objects=None):
        # this context will track all the objects we want to save/update/delete,
        # and organize a hierarchical dependency structure.  it also handles
        # communication with the mappers and relationships to fire off SQL
        # and synchronize attributes between related objects.
        echo = logging.is_info_enabled(self.logger)

        flush_context = UOWTransaction(self, session)

        # create the set of all objects we want to operate upon
        if objects is not None:
            # specific list passed in
            objset = util.Set(objects)
        else:
            # or just everything
            objset = util.Set(self.identity_map.values()).union(self.new)

        # detect persistent objects that have changes
        dirty = self.locate_dirty()

        # store objects whose fate has been decided
        processed = util.Set()


        # put all saves/updates into the flush context.  detect orphans and throw them into deleted.
        for obj in self.new.union(dirty).intersection(objset).difference(self.deleted):
            if obj in processed:
                continue
            if object_mapper(obj)._is_orphan(obj):
                for c in [obj] + list(object_mapper(obj).cascade_iterator('delete', obj)):
                    if c in processed:
                        continue
                    flush_context.register_object(c, isdelete=True)
                    processed.add(c)
            else:
                flush_context.register_object(obj)
                processed.add(obj)

        # put all remaining deletes into the flush context.
        for obj in self.deleted:
            if (objset is not None and not obj in objset) or obj in processed:
                continue
            flush_context.register_object(obj, isdelete=True)

        trans = session.create_transaction(autoflush=False)
        flush_context.transaction = trans
        try:
            flush_context.execute()
        except:
            trans.rollback()
            raise
        trans.commit()

        flush_context.post_exec()

class UOWTransaction(object):
    """Handles the details of organizing and executing transaction
    tasks during a UnitOfWork object's flush() operation.
    """

    def __init__(self, uow, session):
        self.uow = uow
        self.session = session
        #  unique list of all the mappers we come across
        self.mappers = util.Set()
        self.dependencies = {}
        self.tasks = {}
        self.logger = logging.instance_logger(self)
        self.echo = uow.echo
        self.attributes = {}
        
    echo = logging.echo_property()

    def register_object(self, obj, isdelete = False, listonly = False, postupdate=False, post_update_cols=None, **kwargs):
        """Add an object to this UOWTransaction to be updated in the database.

        `isdelete` indicates whether the object is to be deleted or
        saved (update/inserted).

        `listonly` indicates that only this object's dependency
        relationships should be refreshed/updated to reflect a recent
        save/upcoming delete operation, but not a full save/delete
        operation on the object itself, unless an additional
        save/delete registration is entered for the object.
        """

        #print "REGISTER", repr(obj), repr(getattr(obj, '_instance_key', None)), str(isdelete), str(listonly)

        # if object is not in the overall session, do nothing
        if not self.uow._is_valid(obj):
            return

        mapper = object_mapper(obj)
        self.mappers.add(mapper)
        task = self.get_task_by_mapper(mapper)
        if postupdate:
            task.append_postupdate(obj, post_update_cols)
            return

        # for a cyclical task, things need to be sorted out already,
        # so this object should have already been added to the appropriate sub-task
        # can put an assertion here to make sure....
        if task.circular:
            return

        task.append(obj, listonly, isdelete=isdelete, **kwargs)

    def unregister_object(self, obj):
        #print "UNREGISTER", obj
        mapper = object_mapper(obj)
        task = self.get_task_by_mapper(mapper)
        if obj in task.objects:
            task.delete(obj)

    def is_deleted(self, obj):
        mapper = object_mapper(obj)
        task = self.get_task_by_mapper(mapper)
        return task.is_deleted(obj)

    def get_task_by_mapper(self, mapper, dontcreate=False):
        """Every individual mapper involved in the transaction has a
        single corresponding UOWTask object, which stores all the
        operations involved with that mapper as well as operations
        dependent on those operations.  this method returns or creates
        the single per-transaction instance of UOWTask that exists for
        that mapper.
        """

        try:
            return self.tasks[mapper]
        except KeyError:
            if dontcreate:
                return None
            task = UOWTask(self, mapper)
            task.mapper.register_dependencies(self)
            return task

    def register_dependency(self, mapper, dependency):
        """Called by ``mapper.PropertyLoader`` to register the objects
        handled by one mapper being dependent on the objects handled
        by another.
        """

        # correct for primary mapper (the mapper offcially associated with the class)
        # also convert to the "base mapper", the parentmost task at the top of an inheritance chain
        # dependency sorting is done via non-inheriting mappers only, dependencies between mappers
        # in the same inheritance chain is done at the per-object level
        mapper = mapper.primary_mapper().base_mapper()
        dependency = dependency.primary_mapper().base_mapper()

        self.dependencies[(mapper, dependency)] = True

    def register_processor(self, mapper, processor, mapperfrom):
        """Called by ``mapper.PropertyLoader`` to register itself as a
        *processor*, which will be associated with a particular
        UOWTask, and be given a list of *dependent* objects
        corresponding to another UOWTask to be processed, either after
        that secondary task saves its objects or before it deletes its
        objects.
        """

        # when the task from "mapper" executes, take the objects from the task corresponding
        # to "mapperfrom"'s list of save/delete objects, and send them to "processor"
        # for dependency processing

        #print "registerprocessor", str(mapper), repr(processor), repr(processor.key), str(mapperfrom)

        # correct for primary mapper (the mapper offcially associated with the class)
        mapper = mapper.primary_mapper()
        mapperfrom = mapperfrom.primary_mapper()

        task = self.get_task_by_mapper(mapper)
        targettask = self.get_task_by_mapper(mapperfrom)
        up = UOWDependencyProcessor(processor, targettask)
        task.dependencies.add(up)
        
    def execute(self):
        # ensure that we have a UOWTask for every mapper that will be involved
        # in the topological sort
        [self.get_task_by_mapper(m) for m in self._get_noninheriting_mappers()]

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

        head = self._sort_dependencies()
        if self.echo:
            if head is None:
                self.logger.info("Task dump: None")
            else:
                self.logger.info("Task dump:\n" + head.dump())
        if head is not None:
            head.execute(self)
        self.logger.info("Execute Complete")

    def post_exec(self):
        """After an execute/flush is completed, all of the objects and
        lists that have been flushed are updated in the parent
        UnitOfWork object to mark them as clean.
        """

        for task in self.tasks.values():
            for elem in task.objects.values():
                if elem.isdelete:
                    self.uow._remove_deleted(elem.obj)
                else:
                    self.uow.register_clean(elem.obj)

    def _sort_dependencies(self):
        """Create a hierarchical tree of dependent tasks.

        The root node is returned.

        When the root node is executed, it also executes its child
        tasks recursively.
        """

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
        if logging.is_debug_enabled(self.logger):
            self.logger.debug("Dependent tuples:\n" + "\n".join(["(%s->%s)" % (d[0].class_.__name__, d[1].class_.__name__) for d in self.dependencies]))
            self.logger.debug("Dependency sort:\n"+ str(head))
        task = sort_hier(head)
        return task

    def _get_noninheriting_mappers(self):
        """Return a list of UOWTasks whose mappers are not inheriting
        from the mapper of another UOWTask.

        I.e., this returns the root UOWTasks for all the inheritance
        hierarchies represented in this UOWTransaction.
        """

        mappers = util.Set()
        for task in self.tasks.values():
            base = task.mapper.base_mapper()
            mappers.add(base)
        return mappers

class UOWTask(object):
    """Represents the full list of objects that are to be
    saved/deleted by a specific Mapper.
    """

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
        self.objects = {} #util.OrderedDict()

        # a list of UOWDependencyProcessors which are executed after saves and
        # before deletes, to synchronize data to dependent objects
        self.dependencies = util.Set()

        # a list of UOWTasks that are dependent on this UOWTask, which
        # are to be executed after this UOWTask performs saves and post-save
        # dependency processing, and before pre-delete processing and deletes
        self.childtasks = []

        # holds a second UOWTask that contains a special row-based dependency 
        # structure. this is generated when cycles are detected between mapper-
        # level UOWTasks, and breaks up the mapper-level UOWTasks into individual
        # object-based tasks.  
        self.circular = None

        # for a task thats part of that row-based dependency structure, points
        # back to the "public facing" task.
        self.circular_parent = circular_parent

        # a list of UOWDependencyProcessors are derived from the main
        # set of dependencies, referencing sub-UOWTasks attached to this
        # one which represent portions of the total list of objects.
        # this is used for the row-based "circular sort"
        self.cyclical_dependencies = util.Set()

    def is_empty(self):
        return len(self.objects) == 0 and len(self.dependencies) == 0 and len(self.childtasks) == 0

    def append(self, obj, listonly = False, childtask = None, isdelete = False):
        """Append an object to this task, to be either saved or deleted depending on the
        'isdelete' attribute of this UOWTask.

        `listonly` indicates that the object should only be processed
        as a dependency and not actually saved/deleted. if the object
        already exists with a `listonly` flag of False, it is kept as
        is.

        `childtask` is used internally when creating a hierarchical
        list of self-referential tasks, to assign dependent operations
        at the per-object instead of per-task level.
        """

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

    def append_postupdate(self, obj, post_update_cols):
        # postupdates are UPDATED immeditely (for now)
        # convert post_update_cols list to a Set so that __hashcode__ is used to compare columns
        # instead of __eq__
        self.mapper.save_obj([obj], self.uowtransaction, postupdate=True, post_update_cols=util.Set(post_update_cols))
        return True

    def delete(self, obj):
        try:
            del self.objects[obj]
        except KeyError:
            pass

    def _save_objects(self, trans):
        self.mapper.save_obj(self.polymorphic_tosave_objects, trans)
    def _delete_objects(self, trans):
        for task in self.polymorphic_tasks():
            task.mapper.delete_obj(task.todelete_objects, trans)

    def execute(self, trans):
        """Execute this UOWTask.

        Save objects to be saved, process all dependencies that have
        been registered, and delete objects to be deleted.
        """

        UOWExecutor().execute(trans, self)

    def polymorphic_tasks(self):
        """Return an iteration consisting of this UOWTask, and all
        UOWTasks whose mappers are inheriting descendants of this
        UOWTask's mapper.

        UOWTasks are returned in order of their hierarchy to each
        other, meaning if UOWTask B's mapper inherits from UOWTask A's
        mapper, then UOWTask B will appear after UOWTask A in the
        iteration.
        """

        # first us
        yield self

        # "circular dependency" tasks aren't polymorphic, since they break down into many 
        # sub-tasks which encompass a subset of the objects that their "non-circular" parent task would.
        if self.circular_parent is not None:
            return

        # closure to locate the "next level" of inherited mapper UOWTasks
        def _tasks_by_mapper(mapper):
            for m in mapper._inheriting_mappers:
                inherit_task = self.uowtransaction.tasks.get(m, None)
                if inherit_task is not None:
                    yield inherit_task
                else:
                    for t in _tasks_by_mapper(m):
                        yield t

        # main yield loop
        for task in _tasks_by_mapper(self.mapper):
            for t in task.polymorphic_tasks():
                yield t

    def contains_object(self, obj, polymorphic=False):
        if polymorphic:
            for task in self.polymorphic_tasks():
                if obj in task.objects:
                    return True
        else:
            if obj in self.objects:
                return True
        return False

    def is_inserted(self, obj):
        return not hasattr(obj, '_instance_key')

    def is_deleted(self, obj):
        try:
            return self.objects[obj].isdelete
        except KeyError:
            return False

    def get_elements(self, polymorphic=False):
        if polymorphic:
            for task in self.polymorphic_tasks():
                for rec in task.objects.values():
                    yield rec
        else:
            for rec in self.objects.values():
                yield rec

    polymorphic_tosave_elements = property(lambda self: [rec for rec in self.get_elements(polymorphic=True)
                                                         if not rec.isdelete])

    polymorphic_todelete_elements = property(lambda self: [rec for rec in self.get_elements(polymorphic=True)
                                                           if rec.isdelete])

    tosave_elements = property(lambda self: [rec for rec in self.get_elements(polymorphic=False)
                                             if not rec.isdelete])

    todelete_elements = property(lambda self:[rec for rec in self.get_elements(polymorphic=False)
                                              if rec.isdelete])

    tosave_objects = property(lambda self:[rec.obj for rec in self.get_elements(polymorphic=False)
                                           if rec.obj is not None and not rec.listonly and rec.isdelete is False])

    todelete_objects = property(lambda self:[rec.obj for rec in self.get_elements(polymorphic=False)
                                             if rec.obj is not None and not rec.listonly and rec.isdelete is True])

    polymorphic_tosave_objects = property(lambda self:[rec.obj for rec in self.get_elements(polymorphic=True)
                                                       if rec.obj is not None and not rec.listonly and rec.isdelete is False])

    def _sort_circular_dependencies(self, trans, cycles):
        """For a single task, create a hierarchical tree of *subtasks*
        which associate specific dependency actions with individual
        objects.  This is used for a *cyclical* task, or a task where
        elements of its object list contain dependencies on each
        other.

        This is not the normal case; this logic only kicks in when
        something like a hierarchical tree is being represented.
        """

        allobjects = []
        for task in cycles:
            allobjects += [e.obj for e in task.get_elements(polymorphic=True)]
        tuples = []

        cycles = util.Set(cycles)

        #print "BEGIN CIRC SORT-------"
        #print "PRE-CIRC:"
        #print list(cycles) #[0].dump()

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

                    for dep in deps_by_targettask.get(task, []):
                        # is this dependency involved in one of the cycles ?
                        if not dependency_in_cycles(dep):
                            continue
                        (processor, targettask) = (dep.processor, dep.targettask)
                        isdelete = taskelement.isdelete

                        # list of dependent objects from this object
                        childlist = dep.get_object_dependencies(obj, trans, passive=True)
                        if childlist is None:
                            continue
                        # the task corresponding to saving/deleting of those dependent objects
                        childtask = trans.get_task_by_mapper(processor.mapper.primary_mapper())

                        childlist = childlist.added_items() + childlist.unchanged_items() + childlist.deleted_items()

                        for o in childlist:

                            # other object is None.  this can occur if the relationship is many-to-one
                            # or one-to-one, and None was set.  the "removed" object will be picked
                            # up in this iteration via the deleted_items() part of the collection.
                            if o is None:
                                continue

                            # the other object is not in the UOWTransaction !  but if we are many-to-one,
                            # we need a task in order to attach dependency operations, so establish a "listonly"
                            # task
                            if not childtask.contains_object(o, polymorphic=True):
                                childtask.append(o, listonly=True)
                                object_to_original_task[o] = childtask

                            # create a tuple representing the "parent/child"
                            whosdep = dep.whose_dependent_on_who(obj, o)
                            if whosdep is not None:
                                # append the tuple to the partial ordering.
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
        #print "ALLOBJECTS", allobjects
        head = DependencySorter(tuples, allobjects).sort()

        # create a tree of UOWTasks corresponding to the tree of object instances
        # created by the DependencySorter

        used_tasks = util.Set()
        def make_task_tree(node, parenttask, nexttasks):
            originating_task = object_to_original_task[node.item]
            used_tasks.add(originating_task)
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

        # stick the non-circular dependencies onto the new UOWTask
        for d in extradeplist:
            t.dependencies.add(d)
        
        # share the "childtasks" list with the new UOWTask.  more elements
        # may be appended to this "childtasks" list in the enclosing
        # _sort_dependencies() operation that is calling us.
        t.childtasks = self.childtasks
        
        # if we have a head from the dependency sort, assemble child nodes
        # onto the tree.  note this only occurs if there were actual objects
        # to be saved/deleted.
        if head is not None:
            make_task_tree(head, t, {})
            
        for t2 in cycles:
            # tasks that were in the cycle but did not get assembled
            # into the tree, add them as child tasks.  these tasks
            # will have no "save" or "delete" members, but may have dependency
            # processors that operate upon other tasks outside of the cycle.
            if t2 not in used_tasks and t2 is not self:
                # the task must be copied into a "circular" task, so that polymorphic
                # rules dont fire off.  this ensures that the task will have no "save"
                # or "delete" members due to inheriting mappers which contain tasks
                localtask = UOWTask(self.uowtransaction, t2.mapper, circular_parent=self)
                for obj in t2.get_elements(polymorphic=False):
                    localtask.append(obj, t2.listonly, isdelete=t2.objects[obj].isdelete)
                for dep in t2.dependencies:
                    localtask.dependencies.add(dep)
                t.childtasks.insert(0, localtask)

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

class UOWTaskElement(object):
    """An element within a UOWTask.

    Corresponds to a single object instance to be saved, deleted, or
    just part of the transaction as a placeholder for further
    dependencies (i.e. 'listonly').

    In the case of self-referential mappers, may also store a list of
    childtasks, further UOWTasks containing objects dependent on this
    element's object instance.
    """

    def __init__(self, obj):
        self.obj = obj
        self.__listonly = True
        self.childtasks = []
        self.__isdelete = False
        self.__preprocessed = {}

    def _get_listonly(self):
        return self.__listonly

    def _set_listonly(self, value):
        """Set_listonly is a one-way setter, will only go from True to False."""

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
        """Mark this element as *preprocessed* by a particular UOWDependencyProcessor.

        Preprocessing is the step which sweeps through all the
        relationships on all the objects in the flush transaction and
        adds other objects which are also affected, In some cases it
        can switch an object from *tosave* to *todelete*.

        Changes to the state of this UOWTaskElement will reset all
        *preprocessed* flags, causing it to be preprocessed again.
        When all UOWTaskElements have been fully preprocessed by all
        UOWDependencyProcessors, then the topological sort can be
        done.
        """

        self.__preprocessed[processor] = True

    def is_preprocessed(self, processor):
        return self.__preprocessed.get(processor, False)

    def clear_preprocessed(self):
        self.__preprocessed.clear()

    def __repr__(self):
        return "UOWTaskElement/%d: %s/%d %s" % (id(self), self.obj.__class__.__name__, id(self.obj), (self.listonly and 'listonly' or (self.isdelete and 'delete' or 'save')) )

class UOWDependencyProcessor(object):
    """In between the saving and deleting of objects, process
    *dependent* data, such as filling in a foreign key on a child item
    from a new primary key, or deleting association rows before a
    delete.  This object acts as a proxy to a DependencyProcessor.
    """

    def __init__(self, processor, targettask):
        self.processor = processor
        self.targettask = targettask

    def __eq__(self, other):
        return other.processor is self.processor and other.targettask is self.targettask

    def __hash__(self):
        return hash((self.processor, self.targettask))

    def preexecute(self, trans):
        """Traverse all objects handled by this dependency processor
        and locate additional objects which should be part of the
        transaction, such as those affected deletes, orphans to be
        deleted, etc.

        Return True if any objects were preprocessed, or False if no
        objects were preprocessed.
        """

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

class UOWExecutor(object):
    """Encapsulates the execution traversal of a UOWTransaction structure."""

    def execute(self, trans, task, isdelete=None):
        if isdelete is not True:
            self.execute_save_steps(trans, task)
        if isdelete is not False:
            self.execute_delete_steps(trans, task)

    def save_objects(self, trans, task):
        task._save_objects(trans)

    def delete_objects(self, trans, task):
        task._delete_objects(trans)

    def execute_dependency(self, trans, dep, isdelete):
        dep.execute(trans, isdelete)

    def execute_save_steps(self, trans, task):
        if task.circular is not None:
            self.execute_save_steps(trans, task.circular)
        else:
            self.save_objects(trans, task)
            self.execute_cyclical_dependencies(trans, task, False)
            self.execute_per_element_childtasks(trans, task, False)
            self.execute_dependencies(trans, task, False)
            self.execute_dependencies(trans, task, True)
            self.execute_childtasks(trans, task, False)

    def execute_delete_steps(self, trans, task):
        if task.circular is not None:
            self.execute_delete_steps(trans, task.circular)
        else:
            self.execute_cyclical_dependencies(trans, task, True)
            self.execute_childtasks(trans, task, True)
            self.execute_per_element_childtasks(trans, task, True)
            self.delete_objects(trans, task)

    def execute_dependencies(self, trans, task, isdelete=None):
        alltasks = list(task.polymorphic_tasks())
        if isdelete is not True:
            for task in alltasks:
                for dep in task.dependencies:
                    self.execute_dependency(trans, dep, False)
        if isdelete is not False:
            alltasks.reverse()
            for task in alltasks:
                for dep in task.dependencies:
                    self.execute_dependency(trans, dep, True)

    def execute_childtasks(self, trans, task, isdelete=None):
        for polytask in task.polymorphic_tasks():
            for child in polytask.childtasks:
                self.execute(trans, child, isdelete)

    def execute_cyclical_dependencies(self, trans, task, isdelete):
        for polytask in task.polymorphic_tasks():
            for dep in polytask.cyclical_dependencies:
                self.execute_dependency(trans, dep, isdelete)

    def execute_per_element_childtasks(self, trans, task, isdelete):
        for polytask in task.polymorphic_tasks():
            for element in polytask.tosave_elements + polytask.todelete_elements:
                self.execute_element_childtasks(trans, element, isdelete)

    def execute_element_childtasks(self, trans, element, isdelete):
        for child in element.childtasks:
            self.execute(trans, child, isdelete)


class DependencySorter(topological.QueueDependencySorter):
    pass

attribute_manager = UOWAttributeManager()
