# orm/unitofwork.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
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

import StringIO, weakref
from sqlalchemy import util, logging, topological, exceptions
from sqlalchemy.orm import attributes, interfaces
from sqlalchemy.orm import util as mapperutil
from sqlalchemy.orm.mapper import object_mapper, _state_mapper, has_identity

# Load lazily
object_session = None

class UOWEventHandler(interfaces.AttributeExtension):
    """An event handler added to all relation attributes which handles
    session cascade operations.
    """

    def __init__(self, key, class_, cascade):
        self.key = key
        self.class_ = class_
        self.cascade = cascade
    
    def _target_mapper(self, obj):
        prop = object_mapper(obj).get_property(self.key)
        return prop.mapper

    def append(self, obj, item, initiator):
        # process "save_update" cascade rules for when an instance is appended to the list of another instance
        sess = object_session(obj)
        if sess:
            if self.cascade.save_update and item not in sess:
                sess.save_or_update(item, entity_name=self._target_mapper(obj).entity_name)

    def remove(self, obj, item, initiator):
        sess = object_session(obj)
        if sess:
            # expunge pending orphans
            if self.cascade.delete_orphan and item in sess.new:
                if self._target_mapper(obj)._is_orphan(item):
                    sess.expunge(item)

    def set(self, obj, newvalue, oldvalue, initiator):
        # process "save_update" cascade rules for when an instance is attached to another instance
        if oldvalue is newvalue:
            return
        sess = object_session(obj)
        if sess:
            if newvalue is not None and self.cascade.save_update and newvalue not in sess:
                sess.save_or_update(newvalue, entity_name=self._target_mapper(obj).entity_name)
            if self.cascade.delete_orphan and oldvalue in sess.new:
                sess.expunge(oldvalue)


def register_attribute(class_, key, *args, **kwargs):
    """overrides attributes.register_attribute() to add UOW event handlers
    to new InstrumentedAttributes.
    """
    
    cascade = kwargs.pop('cascade', None)
    useobject = kwargs.get('useobject', False)
    if useobject:
        # for object-holding attributes, instrument UOWEventHandler
        # to process per-attribute cascades
        extension = util.to_list(kwargs.pop('extension', None) or [])
        extension.insert(0, UOWEventHandler(key, class_, cascade=cascade))
        kwargs['extension'] = extension
    return attributes.register_attribute(class_, key, *args, **kwargs)
    


class UnitOfWork(object):
    """Main UOW object which stores lists of dirty/new/deleted objects.

    Provides top-level *flush* functionality as well as the
    default transaction boundaries involved in a write
    operation.
    """

    def __init__(self, session):
        if session.weak_identity_map:
            self.identity_map = attributes.WeakInstanceDict()
        else:
            self.identity_map = attributes.StrongInstanceDict()

        self.new = {}   # InstanceState->object, strong refs object
        self.deleted = {}  # same
        self.logger = logging.instance_logger(self, echoflag=session.echo_uow)

    def _remove_deleted(self, state):
        if '_instance_key' in state.dict:
            del self.identity_map[state.dict['_instance_key']]
        self.deleted.pop(state, None)
        self.new.pop(state, None)

    def _is_valid(self, state):
        if '_instance_key' in state.dict:
            return state.dict['_instance_key'] in self.identity_map
        else:
            return state in self.new

    def _register_clean(self, state):
        """register the given object as 'clean' (i.e. persistent) within this unit of work, after
        a save operation has taken place."""

        mapper = _state_mapper(state)
        instance_key = mapper._identity_key_from_state(state)
        
        if '_instance_key' not in state.dict:
            state.dict['_instance_key'] = instance_key
            
        elif state.dict['_instance_key'] != instance_key:
            # primary key switch
            del self.identity_map[state.dict['_instance_key']]
            state.dict['_instance_key'] = instance_key
            
        if hasattr(state, 'insert_order'):
            delattr(state, 'insert_order')
        
        o = state.obj()
        # prevent against last minute dereferences of the object
        # TODO: identify a code path where state.obj() is None
        if o is not None:
            self.identity_map[state.dict['_instance_key']] = o
            state.commit_all()
        
        # remove from new last, might be the last strong ref
        self.new.pop(state, None)

    def register_new(self, obj):
        """register the given object as 'new' (i.e. unsaved) within this unit of work."""

        if hasattr(obj, '_instance_key'):
            raise exceptions.InvalidRequestError("Object '%s' already has an identity - it can't be registered as new" % repr(obj))
        if obj._state not in self.new:
            self.new[obj._state] = obj
            obj._state.insert_order = len(self.new)

    def register_deleted(self, obj):
        """register the given persistent object as 'to be deleted' within this unit of work."""
        
        self.deleted[obj._state] = obj

    def locate_dirty(self):
        """return a set of all persistent instances within this unit of work which 
        either contain changes or are marked as deleted.
        """
        
        # a little bit of inlining for speed
        return util.IdentitySet([x for x in self.identity_map.values() 
            if x._state not in self.deleted 
            and (
                x._state.modified
                or (x.__class__._class_state.has_mutable_scalars and x._state.is_modified())
            )
            ])

    def flush(self, session, objects=None):
        """create a dependency tree of all pending SQL operations within this unit of work and execute."""

        dirty = [x for x in self.identity_map.all_states()
            if x.modified
            or (x.class_._class_state.has_mutable_scalars and x.is_modified())
        ]
        
        if not dirty and not self.deleted and not self.new:
            return
        
        deleted = util.Set(self.deleted)
        new = util.Set(self.new)
        
        dirty = util.Set(dirty).difference(deleted)
        
        flush_context = UOWTransaction(self, session)

        if session.extension is not None:
            session.extension.before_flush(session, flush_context, objects)

        # create the set of all objects we want to operate upon
        if objects:
            # specific list passed in
            objset = util.Set([o._state for o in objects])
        else:
            # or just everything
            objset = util.Set(self.identity_map.all_states()).union(new)
            
        # store objects whose fate has been decided
        processed = util.Set()

        # put all saves/updates into the flush context.  detect top-level orphans and throw them into deleted.
        for state in new.union(dirty).intersection(objset).difference(deleted):
            if state in processed:
                continue

            obj = state.obj()
            is_orphan = _state_mapper(state)._is_orphan(obj)
            if is_orphan and not has_identity(obj):
                raise exceptions.FlushError("instance %s is an unsaved, pending instance and is an orphan (is not attached to %s)" %
                    (
                        obj,
                        ", nor ".join(["any parent '%s' instance via that classes' '%s' attribute" % (klass.__name__, key) for (key,klass) in _state_mapper(state).delete_orphans])
                    ))
            flush_context.register_object(state, isdelete=is_orphan)
            processed.add(state)

        # put all remaining deletes into the flush context.
        for state in deleted.intersection(objset).difference(processed):
            flush_context.register_object(state, isdelete=True)

        if len(flush_context.tasks) == 0:
            return
            
        session.create_transaction(autoflush=False)
        flush_context.transaction = session.transaction
        try:
            flush_context.execute()
            
            if session.extension is not None:
                session.extension.after_flush(session, flush_context)
            session.commit()
        except:
            session.rollback()
            raise

        flush_context.post_exec()

        if session.extension is not None:
            session.extension.after_flush_postexec(session, flush_context)

    def prune_identity_map(self):
        """Removes unreferenced instances cached in a strong-referencing identity map.

        Note that this method is only meaningful if "weak_identity_map"
        on the parent Session is set to False and therefore this UnitOfWork's
        identity map is a regular dictionary
        
        Removes any object in the identity map that is not referenced
        in user code or scheduled for a unit of work operation.  Returns
        the number of objects pruned.
        """

        if isinstance(self.identity_map, attributes.WeakInstanceDict):
            return 0
        ref_count = len(self.identity_map)
        dirty = self.locate_dirty()
        keepers = weakref.WeakValueDictionary(self.identity_map)
        self.identity_map.clear()
        self.identity_map.update(keepers)
        return ref_count - len(self.identity_map)

class UOWTransaction(object):
    """Handles the details of organizing and executing transaction
    tasks during a UnitOfWork object's flush() operation.  
    
    The central operation is to form a graph of nodes represented by the
    ``UOWTask`` class, which is then traversed by a ``UOWExecutor`` object
    that issues SQL and instance-synchronizing operations via the related
    packages.
    """

    def __init__(self, uow, session):
        self.uow = uow
        self.session = session
        self.mapper_flush_opts = session._mapper_flush_opts
        
        # stores tuples of mapper/dependent mapper pairs,
        # representing a partial ordering fed into topological sort
        self.dependencies = util.Set()
        
        # dictionary of mappers to UOWTasks
        self.tasks = {}
        
        # dictionary used by external actors to store arbitrary state
        # information. 
        self.attributes = {}

        self.logger = logging.instance_logger(self, echoflag=session.echo_uow)

    def get_attribute_history(self, state, key, passive=True):
        hashkey = ("history", state, key)

        # cache the objects, not the states; the strong reference here
        # prevents newly loaded objects from being dereferenced during the 
        # flush process
        if hashkey in self.attributes:
            (added, unchanged, deleted, cached_passive) = self.attributes[hashkey]
            # if the cached lookup was "passive" and now we want non-passive, do a non-passive
            # lookup and re-cache
            if cached_passive and not passive:
                (added, unchanged, deleted) = attributes.get_history(state, key, passive=False)
                self.attributes[hashkey] = (added, unchanged, deleted, passive)
        else:
            (added, unchanged, deleted) = attributes.get_history(state, key, passive=passive)
            self.attributes[hashkey] = (added, unchanged, deleted, passive)

        if added is None:
            return (added, unchanged, deleted)
        else:
            return (
                [getattr(c, '_state', c) for c in added],
                [getattr(c, '_state', c) for c in unchanged],
                [getattr(c, '_state', c) for c in deleted],
                )

        
    def register_object(self, state, isdelete = False, listonly = False, postupdate=False, post_update_cols=None, **kwargs):
        # if object is not in the overall session, do nothing
        if not self.uow._is_valid(state):
            if self._should_log_debug:
                self.logger.debug("object %s not part of session, not registering for flush" % (mapperutil.state_str(state)))
            return

        if self._should_log_debug:
            self.logger.debug("register object for flush: %s isdelete=%s listonly=%s postupdate=%s" % (mapperutil.state_str(state), isdelete, listonly, postupdate))

        mapper = _state_mapper(state)
        
        task = self.get_task_by_mapper(mapper)
        if postupdate:
            task.append_postupdate(state, post_update_cols)
        else:
            task.append(state, listonly, isdelete=isdelete, **kwargs)

    def set_row_switch(self, state):
        """mark a deleted object as a 'row switch'.
        
        this indicates that an INSERT statement elsewhere corresponds to this DELETE;
        the INSERT is converted to an UPDATE and the DELETE does not occur.
        """
        mapper = _state_mapper(state)
        task = self.get_task_by_mapper(mapper)
        taskelement = task._objects[state]
        taskelement.isdelete = "rowswitch"
        
    def is_deleted(self, state):
        """return true if the given state is marked as deleted within this UOWTransaction."""
        
        mapper = _state_mapper(state)
        task = self.get_task_by_mapper(mapper)
        return task.is_deleted(state)
        
    def get_task_by_mapper(self, mapper, dontcreate=False):
        """return UOWTask element corresponding to the given mapper.

        Will create a new UOWTask, including a UOWTask corresponding to the 
        "base" inherited mapper, if needed, unless the dontcreate flag is True.
        """
        try:
            return self.tasks[mapper]
        except KeyError:
            if dontcreate:
                return None
                
            base_mapper = mapper.base_mapper
            if base_mapper in self.tasks:
                base_task = self.tasks[base_mapper]
            else:
                self.tasks[base_mapper] = base_task = UOWTask(self, base_mapper)
                base_mapper._register_dependencies(self)

            if mapper not in self.tasks:
                self.tasks[mapper] = task = UOWTask(self, mapper, base_task=base_task)
                mapper._register_dependencies(self)
            else:
                task = self.tasks[mapper]
                
            return task

    def register_dependency(self, mapper, dependency):
        """register a dependency between two mappers.

        Called by ``mapper.PropertyLoader`` to register the objects
        handled by one mapper being dependent on the objects handled
        by another.        
        """

        # correct for primary mapper
        # also convert to the "base mapper", the parentmost task at the top of an inheritance chain
        # dependency sorting is done via non-inheriting mappers only, dependencies between mappers
        # in the same inheritance chain is done at the per-object level
        mapper = mapper.primary_mapper().base_mapper
        dependency = dependency.primary_mapper().base_mapper

        self.dependencies.add((mapper, dependency))

    def register_processor(self, mapper, processor, mapperfrom):
        """register a dependency processor, corresponding to dependencies between
        the two given mappers.
        
        """

        # correct for primary mapper
        mapper = mapper.primary_mapper()
        mapperfrom = mapperfrom.primary_mapper()

        task = self.get_task_by_mapper(mapper)
        targettask = self.get_task_by_mapper(mapperfrom)
        up = UOWDependencyProcessor(processor, targettask)
        task.dependencies.add(up)
        
    def execute(self):
        """Execute this UOWTransaction.
        
        This will organize all collected UOWTasks into a dependency-sorted
        list which is then traversed using the traversal scheme
        encoded in the UOWExecutor class.  Operations to mappers and dependency
        processors are fired off in order to issue SQL to the database and 
        synchronize instance attributes with database values and related
        foreign key values."""

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

        tasks = self._sort_dependencies()
        if self._should_log_info:
            self.logger.info("Task dump:\n" + self._dump(tasks))
        UOWExecutor().execute(self, tasks)
        if self._should_log_info:
            self.logger.info("Execute Complete")

    def _dump(self, tasks):
        buf = StringIO.StringIO()
        import uowdumper
        uowdumper.UOWDumper(tasks, buf)
        return buf.getvalue()
        
    def post_exec(self):
        """mark processed objects as clean / deleted after a successful flush().
        
        this method is called within the flush() method after the 
        execute() method has succeeded and the transaction has been committed.
        """

        for task in self.tasks.values():
            for elem in task.elements:
                if elem.state is None:
                    continue
                if elem.isdelete:
                    self.uow._remove_deleted(elem.state)
                else:
                    self.uow._register_clean(elem.state)

    def _sort_dependencies(self):
        nodes = topological.sort_with_cycles(self.dependencies, 
            [t.mapper for t in self.tasks.values() if t.base_task is t]
        )

        ret = []
        for item, cycles in nodes:
            task = self.get_task_by_mapper(item)
            if cycles:
                for t in task._sort_circular_dependencies(self, [self.get_task_by_mapper(i) for i in cycles]):
                    ret.append(t)
            else:
                ret.append(task)

        if self._should_log_debug:
            self.logger.debug("Dependent tuples:\n" + "\n".join(["(%s->%s)" % (d[0].class_.__name__, d[1].class_.__name__) for d in self.dependencies]))
            self.logger.debug("Dependency sort:\n"+ str(ret))
        return ret

class UOWTask(object):
    """Represents all of the objects in the UOWTransaction which correspond to
    a particular mapper.  This is the primary class of three classes used to generate
    the elements of the dependency graph.
    """

    def __init__(self, uowtransaction, mapper, base_task=None):
        self.uowtransaction = uowtransaction

        # base_task is the UOWTask which represents the "base mapper"
        # in our mapper's inheritance chain.  if the mapper does not
        # inherit from any other mapper, the base_task is self.
        # the _inheriting_tasks dictionary is a dictionary present only
        # on the "base_task"-holding UOWTask, which maps all mappers within
        # an inheritance hierarchy to their corresponding UOWTask instances.
        if base_task is None:
            self.base_task = self
            self._inheriting_tasks = {mapper:self}
        else:
            self.base_task = base_task
            base_task._inheriting_tasks[mapper] = self
        
        # the Mapper which this UOWTask corresponds to
        self.mapper = mapper

        # mapping of InstanceState -> UOWTaskElement
        self._objects = {} 

        self.dependencies = util.Set()
        self.cyclical_dependencies = util.Set()

    def polymorphic_tasks(self):
        """return an iterator of UOWTask objects corresponding to the inheritance sequence
        of this UOWTask's mapper.
        
            e.g. if mapper B and mapper C inherit from mapper A, and mapper D inherits from B:
            
                mapperA -> mapperB -> mapperD
                        -> mapperC 
                                   
            the inheritance sequence starting at mapper A is a depth-first traversal:
            
                [mapperA, mapperB, mapperD, mapperC]
                
            this method will therefore return
            
                [UOWTask(mapperA), UOWTask(mapperB), UOWTask(mapperD), UOWTask(mapperC)]
                
        The concept of "polymporphic iteration" is adapted into several property-based 
        iterators which return object instances, UOWTaskElements and UOWDependencyProcessors
        in an order corresponding to this sequence of parent UOWTasks.  This is used to issue
        operations related to inheritance-chains of mappers in the proper order based on 
        dependencies between those mappers.
        
        """
        
        for mapper in self.mapper.polymorphic_iterator():
            t = self.base_task._inheriting_tasks.get(mapper, None)
            if t is not None:
                yield t

    def is_empty(self):
        """return True if this UOWTask is 'empty', meaning it has no child items.

        used only for debugging output.
        """

        return not self._objects and not self.dependencies
            
    def append(self, state, listonly=False, isdelete=False):
        if state not in self._objects:
            self._objects[state] = rec = UOWTaskElement(state)
        else:
            rec = self._objects[state]
        
        rec.update(listonly, isdelete)
    
    def _append_cyclical_childtask(self, task):
        if "cyclical" not in self._objects:
            self._objects["cyclical"] = UOWTaskElement(None)
        self._objects["cyclical"].childtasks.append(task)

    def append_postupdate(self, state, post_update_cols):
        """issue a 'post update' UPDATE statement via this object's mapper immediately.  
        
        this operation is used only with relations that specify the `post_update=True`
        flag.
        """

        # postupdates are UPDATED immeditely (for now)
        # convert post_update_cols list to a Set so that __hashcode__ is used to compare columns
        # instead of __eq__
        self.mapper._save_obj([state], self.uowtransaction, postupdate=True, post_update_cols=util.Set(post_update_cols))

    def __contains__(self, state):
        """return True if the given object is contained within this UOWTask or inheriting tasks."""
        
        for task in self.polymorphic_tasks():
            if state in task._objects:
                return True
        else:
            return False

    def is_deleted(self, state):
        """return True if the given object is marked as to be deleted within this UOWTask."""
        
        try:
            return self._objects[state].isdelete
        except KeyError:
            return False

    def _polymorphic_collection(callable):
        """return a property that will adapt the collection returned by the
        given callable into a polymorphic traversal."""
        
        def collection(self):
            for task in self.polymorphic_tasks():
                for rec in callable(task):
                    yield rec
        return property(collection)
        
    elements = property(lambda self:self._objects.values())
    
    polymorphic_elements = _polymorphic_collection(lambda task:task.elements)

    polymorphic_tosave_elements = property(lambda self: [rec for rec in self.polymorphic_elements
                                             if not rec.isdelete])
                                             
    polymorphic_todelete_elements = property(lambda self:[rec for rec in self.polymorphic_elements
                                               if rec.isdelete])

    polymorphic_tosave_objects = property(lambda self:[rec.state for rec in self.polymorphic_elements
                                          if rec.state is not None and not rec.listonly and rec.isdelete is False])

    polymorphic_todelete_objects = property(lambda self:[rec.state for rec in self.polymorphic_elements
                                          if rec.state is not None and not rec.listonly and rec.isdelete is True])

    polymorphic_dependencies = _polymorphic_collection(lambda task:task.dependencies)
    
    polymorphic_cyclical_dependencies = _polymorphic_collection(lambda task:task.cyclical_dependencies)
    
    def _sort_circular_dependencies(self, trans, cycles):
        """Create a hierarchical tree of *subtasks*
        which associate specific dependency actions with individual
        objects.  This is used for a *cyclical* task, or a task where
        elements of its object list contain dependencies on each
        other.

        This is not the normal case; this logic only kicks in when
        something like a hierarchical tree is being represented.
        """
        allobjects = []
        for task in cycles:
            allobjects += [e.state for e in task.polymorphic_elements]
        tuples = []

        cycles = util.Set(cycles)

        extradeplist = []
        dependencies = {}

        def get_dependency_task(state, depprocessor):
            try:
                dp = dependencies[state]
            except KeyError:
                dp = dependencies.setdefault(state, {})
            try:
                l = dp[depprocessor]
            except KeyError:
                l = UOWTask(self.uowtransaction, depprocessor.targettask.mapper)
                dp[depprocessor] = l
            return l

        def dependency_in_cycles(dep):
            proctask = trans.get_task_by_mapper(dep.processor.mapper.base_mapper, True)
            targettask = trans.get_task_by_mapper(dep.targettask.mapper.base_mapper, True)
            return targettask in cycles and (proctask is not None and proctask in cycles)

        # organize all original UOWDependencyProcessors by their target task
        deps_by_targettask = {}
        for task in cycles:
            for dep in task.polymorphic_dependencies:
                if not dependency_in_cycles(dep):
                    extradeplist.append(dep)
                for t in dep.targettask.polymorphic_tasks():
                    l = deps_by_targettask.setdefault(t, [])
                    l.append(dep)

        object_to_original_task = {}

        for task in cycles:
            for subtask in task.polymorphic_tasks():
                for taskelement in subtask.elements:
                    state = taskelement.state
                    object_to_original_task[state] = subtask
                    for dep in deps_by_targettask.get(subtask, []):
                        # is this dependency involved in one of the cycles ?
                        # (don't count the DetectKeySwitch prop)
                        if dep.processor.no_dependencies or not dependency_in_cycles(dep):
                            continue
                        (processor, targettask) = (dep.processor, dep.targettask)
                        isdelete = taskelement.isdelete

                        # list of dependent objects from this object
                        (added, unchanged, deleted) = dep.get_object_dependencies(state, trans, passive=True)
                        if not added and not unchanged and not deleted:
                            continue
                            
                        # the task corresponding to saving/deleting of those dependent objects
                        childtask = trans.get_task_by_mapper(processor.mapper)

                        childlist = added + unchanged + deleted

                        for o in childlist:
                            # other object is None.  this can occur if the relationship is many-to-one
                            # or one-to-one, and None was set.  the "removed" object will be picked
                            # up in this iteration via the deleted_items() part of the collection.
                            if o is None:
                                continue

                            # the other object is not in the UOWTransaction !  but if we are many-to-one,
                            # we need a task in order to attach dependency operations, so establish a "listonly"
                            # task
                            if o not in childtask:
                                childtask.append(o, listonly=True)
                                object_to_original_task[o] = childtask

                            # create a tuple representing the "parent/child"
                            whosdep = dep.whose_dependent_on_who(state, o)
                            if whosdep is not None:
                                # append the tuple to the partial ordering.
                                tuples.append(whosdep)

                                # create a UOWDependencyProcessor representing this pair of objects.
                                # append it to a UOWTask
                                if whosdep[0] is state:
                                    get_dependency_task(whosdep[0], dep).append(whosdep[0], isdelete=isdelete)
                                else:
                                    get_dependency_task(whosdep[0], dep).append(whosdep[1], isdelete=isdelete)
                            else:
                                # TODO: no test coverage here
                                get_dependency_task(state, dep).append(state, isdelete=isdelete)

        head = topological.sort_as_tree(tuples, allobjects)
        
        used_tasks = util.Set()
        def make_task_tree(node, parenttask, nexttasks):
            (state, cycles, children) = node
            originating_task = object_to_original_task[state]
            used_tasks.add(originating_task)
            t = nexttasks.get(originating_task, None)
            if t is None:
                t = UOWTask(self.uowtransaction, originating_task.mapper)
                nexttasks[originating_task] = t
                parenttask._append_cyclical_childtask(t)
            t.append(state, originating_task._objects[state].listonly, isdelete=originating_task._objects[state].isdelete)

            if state in dependencies:
                for depprocessor, deptask in dependencies[state].iteritems():
                    t.cyclical_dependencies.add(depprocessor.branch(deptask))
            nd = {}
            for n in children:
                t2 = make_task_tree(n, t, nd)
            return t

        t = UOWTask(self.uowtransaction, self.mapper)
        
        # stick the non-circular dependencies onto the new UOWTask
        for d in extradeplist:
            t.dependencies.add(d)
        
        if head is not None:
            make_task_tree(head, t, {})

        ret = [t]

        # add tasks that were in the cycle, but didnt get assembled
        # into the cyclical tree, to the start of the list
        for t2 in cycles:
            if t2 not in used_tasks and t2 is not self:
                localtask = UOWTask(self.uowtransaction, t2.mapper)
                for state in t2.elements:
                    localtask.append(state, t2.listonly, isdelete=t2._objects[state].isdelete)
                for dep in t2.dependencies:
                    localtask.dependencies.add(dep)
                ret.insert(0, localtask)
        
        return ret

    def __repr__(self):
        if self.mapper is not None:
            if self.mapper.__class__.__name__ == 'Mapper':
                name = self.mapper.class_.__name__ + "/" + self.mapper.local_table.description
            else:
                name = repr(self.mapper)
        else:
            name = '(none)'
        return ("UOWTask(%s) Mapper: '%s'" % (hex(id(self)), name))

class UOWTaskElement(object):
    """An element within a UOWTask.

    Corresponds to a single object instance to be saved, deleted, or
    just part of the transaction as a placeholder for further
    dependencies (i.e. 'listonly').

    may also store additional sub-UOWTasks.
    """

    def __init__(self, state):
        self.state = state
        self.listonly = True
        self.childtasks = []
        self.isdelete = False
        self.__preprocessed = {}

    def update(self, listonly, isdelete):
        if not listonly and self.listonly:
            self.listonly = False
            self.__preprocessed.clear()
        if isdelete and not self.isdelete:
            self.isdelete = True
            self.__preprocessed.clear()

    def mark_preprocessed(self, processor):
        """Mark this element as *preprocessed* by a particular ``UOWDependencyProcessor``.

        Preprocessing is used by dependency.py to apply
        flush-time cascade rules to relations and bring all
        required objects into the flush context.

        each processor as marked as "processed" when complete, however
        changes to the state of this UOWTaskElement will reset
        the list of completed processors, so that they 
        execute again, until no new objects or state changes
        are brought in.
        """

        self.__preprocessed[processor] = True

    def is_preprocessed(self, processor):
        return self.__preprocessed.get(processor, False)

    def __repr__(self):
        return "UOWTaskElement/%d: %s/%d %s" % (id(self), self.state.class_.__name__, id(self.state.obj()), (self.listonly and 'listonly' or (self.isdelete and 'delete' or 'save')) )

class UOWDependencyProcessor(object):
    """In between the saving and deleting of objects, process
    *dependent* data, such as filling in a foreign key on a child item
    from a new primary key, or deleting association rows before a
    delete.  This object acts as a proxy to a DependencyProcessor.
    """

    def __init__(self, processor, targettask):
        self.processor = processor
        self.targettask = targettask

    def __repr__(self):
        return "UOWDependencyProcessor(%s, %s)" % (str(self.processor), str(self.targettask))
    
    def __str__(self):
        return repr(self)
            
    def __eq__(self, other):
        return other.processor is self.processor and other.targettask is self.targettask

    def __hash__(self):
        return hash((self.processor, self.targettask))

    def preexecute(self, trans):
        """preprocess all objects contained within this ``UOWDependencyProcessor``s target task.

        This may locate additional objects which should be part of the
        transaction, such as those affected deletes, orphans to be
        deleted, etc.
        
        Once an object is preprocessed, its ``UOWTaskElement`` is marked as processed.  If subsequent 
        changes occur to the ``UOWTaskElement``, its processed flag is reset, and will require processing
        again.

        Return True if any objects were preprocessed, or False if no
        objects were preprocessed.  If True is returned, the parent ``UOWTransaction`` will
        ultimately call ``preexecute()`` again on all processors until no new objects are processed.
        """

        def getobj(elem):
            elem.mark_preprocessed(self)
            return elem.state

        ret = False
        elements = [getobj(elem) for elem in self.targettask.polymorphic_tosave_elements if elem.state is not None and not elem.is_preprocessed(self)]
        if elements:
            ret = True
            self.processor.preprocess_dependencies(self.targettask, elements, trans, delete=False)

        elements = [getobj(elem) for elem in self.targettask.polymorphic_todelete_elements if elem.state is not None and not elem.is_preprocessed(self)]
        if elements:
            ret = True
            self.processor.preprocess_dependencies(self.targettask, elements, trans, delete=True)
        return ret

    def execute(self, trans, delete):
        """process all objects contained within this ``UOWDependencyProcessor``s target task."""
        
        if not delete:
            self.processor.process_dependencies(self.targettask, [elem.state for elem in self.targettask.polymorphic_tosave_elements if elem.state is not None], trans, delete=False)
        else:
            self.processor.process_dependencies(self.targettask, [elem.state for elem in self.targettask.polymorphic_todelete_elements if elem.state is not None], trans, delete=True)

    def get_object_dependencies(self, state, trans, passive):
        return trans.get_attribute_history(state, self.processor.key, passive=passive)

    def whose_dependent_on_who(self, state1, state2):
        """establish which object is operationally dependent amongst a parent/child 
        using the semantics stated by the dependency processor.
        
        This method is used to establish a partial ordering (set of dependency tuples)
        when toplogically sorting on a per-instance basis.
        
        """
        
        return self.processor.whose_dependent_on_who(state1, state2)

    def branch(self, task):
        """create a copy of this ``UOWDependencyProcessor`` against a new ``UOWTask`` object.
        
        this is used within the instance-level sorting operation when a single ``UOWTask``
        is broken up into many individual ``UOWTask`` objects.
        
        """
        
        return UOWDependencyProcessor(self.processor, task)
    
        
class UOWExecutor(object):
    """Encapsulates the execution traversal of a UOWTransaction structure."""

    def execute(self, trans, tasks, isdelete=None):
        if isdelete is not True:
            for task in tasks:
                self.execute_save_steps(trans, task)
        if isdelete is not False:
            for task in util.reversed(tasks):
                self.execute_delete_steps(trans, task)

    def save_objects(self, trans, task):
        task.mapper._save_obj(task.polymorphic_tosave_objects, trans)

    def delete_objects(self, trans, task):
        task.mapper._delete_obj(task.polymorphic_todelete_objects, trans)

    def execute_dependency(self, trans, dep, isdelete):
        dep.execute(trans, isdelete)

    def execute_save_steps(self, trans, task):
        self.save_objects(trans, task)
        self.execute_cyclical_dependencies(trans, task, False)
        self.execute_per_element_childtasks(trans, task, False)
        self.execute_dependencies(trans, task, False)
        self.execute_dependencies(trans, task, True)
        
    def execute_delete_steps(self, trans, task):
        self.execute_cyclical_dependencies(trans, task, True)
        self.execute_per_element_childtasks(trans, task, True)
        self.delete_objects(trans, task)

    def execute_dependencies(self, trans, task, isdelete=None):
        if isdelete is not True:
            for dep in task.polymorphic_dependencies:
                self.execute_dependency(trans, dep, False)
        if isdelete is not False:
            for dep in util.reversed(list(task.polymorphic_dependencies)):
                self.execute_dependency(trans, dep, True)

    def execute_cyclical_dependencies(self, trans, task, isdelete):
        for dep in task.polymorphic_cyclical_dependencies:
            self.execute_dependency(trans, dep, isdelete)

    def execute_per_element_childtasks(self, trans, task, isdelete):
        for element in task.polymorphic_tosave_elements + task.polymorphic_todelete_elements:
            self.execute_element_childtasks(trans, element, isdelete)

    def execute_element_childtasks(self, trans, element, isdelete):
        for child in element.childtasks:
            self.execute(trans, [child], isdelete)

