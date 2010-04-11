# orm/unitofwork.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The internals for the unit of work system.

The session's flush() process passes objects to a contextual object
here, which assembles flush tasks based on mappers and their properties,
organizes them in order of dependency, and executes.

Most of the code in this module is obsolete, and will 
be replaced by a much simpler and more efficient
system in an upcoming release.  See [ticket:1742] for 
details.


"""

from sqlalchemy import util, log, topological
from sqlalchemy.orm import attributes, interfaces
from sqlalchemy.orm import util as mapperutil
from sqlalchemy.orm.mapper import _state_mapper

# Load lazily
object_session = None
_state_session = None

class UOWEventHandler(interfaces.AttributeExtension):
    """An event handler added to all relationship attributes which handles
    session cascade operations.
    """
    
    active_history = False
    
    def __init__(self, key):
        self.key = key

    def append(self, state, item, initiator):
        # process "save_update" cascade rules for when 
        # an instance is appended to the list of another instance
        sess = _state_session(state)
        if sess:
            prop = _state_mapper(state).get_property(self.key)
            if prop.cascade.save_update and item not in sess:
                sess.add(item)
        return item
        
    def remove(self, state, item, initiator):
        sess = _state_session(state)
        if sess:
            prop = _state_mapper(state).get_property(self.key)
            # expunge pending orphans
            if prop.cascade.delete_orphan and \
                item in sess.new and \
                prop.mapper._is_orphan(attributes.instance_state(item)):
                    sess.expunge(item)

    def set(self, state, newvalue, oldvalue, initiator):
        # process "save_update" cascade rules for when an instance is attached to another instance
        if oldvalue is newvalue:
            return newvalue
        sess = _state_session(state)
        if sess:
            prop = _state_mapper(state).get_property(self.key)
            if newvalue is not None and prop.cascade.save_update and newvalue not in sess:
                sess.add(newvalue)
            if prop.cascade.delete_orphan and oldvalue in sess.new and \
                prop.mapper._is_orphan(attributes.instance_state(oldvalue)):
                sess.expunge(oldvalue)
        return newvalue


class UOWTransaction(object):
    """Represent the state of a flush operation in progress."""
    
    def __init__(self, session):
        self.session = session
        self.mapper_flush_opts = session._mapper_flush_opts

        # stores tuples of mapper/dependent mapper pairs,
        # representing a partial ordering fed into topological sort
        self.dependencies = set()

        # dictionary of mappers to UOWTasks
        self.tasks = {}

        # dictionary used by external actors 
        # to store arbitrary state
        # information.
        self.attributes = {}
        
        self.processors = set()
    
    def get_attribute_history(self, state, key, passive=True):
        hashkey = ("history", state, key)

        # cache the objects, not the states; the strong reference here
        # prevents newly loaded objects from being dereferenced during the
        # flush process
        if hashkey in self.attributes:
            (history, cached_passive) = self.attributes[hashkey]
            # if the cached lookup was "passive" and now we want non-passive, do a non-passive
            # lookup and re-cache
            if cached_passive and not passive:
                history = attributes.get_state_history(state, key, passive=False)
                self.attributes[hashkey] = (history, passive)
        else:
            history = attributes.get_state_history(state, key, passive=passive)
            self.attributes[hashkey] = (history, passive)

        if not history or not state.get_impl(key).uses_objects:
            return history
        else:
            return history.as_state()

    def register_object(self, state, isdelete=False, 
                            listonly=False, postupdate=False, post_update_cols=None):
        
        # if object is not in the overall session, do nothing
        if not self.session._contains_state(state):
            return

        mapper = _state_mapper(state)

        task = self.get_task_by_mapper(mapper)
        if postupdate:
            task.append_postupdate(state, post_update_cols)
        else:
            task.append(state, listonly=listonly, isdelete=isdelete)

        # ensure the mapper for this object has had its 
        # DependencyProcessors added.
        if mapper not in self.processors:
            mapper._register_processors(self)
            self.processors.add(mapper)

            if mapper.base_mapper not in self.processors:
                mapper.base_mapper._register_processors(self)
                self.processors.add(mapper.base_mapper)
            
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
        """register a dependency between two mappers."""
        
        # correct for primary mapper
        # also convert to the "base mapper", the parentmost 
        # task at the top of an inheritance chain
        # dependency sorting is done via non-inheriting 
        # mappers only, dependencies between mappers
        # in the same inheritance chain is done at the per-object level
        mapper = mapper.primary_mapper().base_mapper
        dependency = dependency.primary_mapper().base_mapper

        self.dependencies.add((mapper, dependency))

    def register_processor(self, mapper, processor, mapperfrom):
        """register a dependency processor, corresponding to 
        operations which occur between two mappers.
        
        """
        # correct for primary mapper
        mapper = mapper.primary_mapper()
        mapperfrom = mapperfrom.primary_mapper()

        task = self.get_task_by_mapper(mapper)
        targettask = self.get_task_by_mapper(mapperfrom)
        up = UOWDependencyProcessor(processor, targettask)
        task.dependencies.add(up)

    def execute(self):
        """Execute this steps assembled into this UOWTransaction."""

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
        if self._should_log_info():
            self.logger.info("Task dump:\n%s", self._dump(tasks))
        UOWExecutor().execute(self, tasks)
        self.logger.info("Execute Complete")

    def _dump(self, tasks):
        from uowdumper import UOWDumper
        return UOWDumper.dump(tasks)

    @property
    def elements(self):
        """Iterate UOWTaskElements."""
        
        for task in self.tasks.itervalues():
            for elem in task.elements:
                yield elem

    def finalize_flush_changes(self):
        """mark processed objects as clean / deleted after a successful flush().

        this method is called within the flush() method after the
        execute() method has succeeded and the transaction has been committed.
        """

        for elem in self.elements:
            if elem.isdelete:
                self.session._remove_newly_deleted(elem.state)
            elif not elem.listonly:
                self.session._register_newly_persistent(elem.state)

    def _sort_dependencies(self):
        nodes = topological._sort_with_cycles(self.dependencies,
            [t.mapper for t in self.tasks.itervalues() if t.base_task is t]
        )

        ret = []
        for item, cycles in nodes:
            task = self.get_task_by_mapper(item)
            if cycles:
                for t in task._sort_circular_dependencies(
                                        self, 
                                        [self.get_task_by_mapper(i) for i in cycles]
                                        ):
                    ret.append(t)
            else:
                ret.append(task)

        return ret

log.class_logger(UOWTransaction)

class UOWTask(object):
    """A collection of mapped states corresponding to a particular mapper.
    
    This object is deprecated.
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

        self.dependent_tasks = []
        self.dependencies = set()
        self.cyclical_dependencies = set()

    @util.memoized_property
    def inheriting_mappers(self):
        return list(self.mapper.polymorphic_iterator())

    @property
    def polymorphic_tasks(self):
        """Return an iterator of UOWTask objects corresponding to the
        inheritance sequence of this UOWTask's mapper.

        """
        for mapper in self.inheriting_mappers:
            t = self.base_task._inheriting_tasks.get(mapper, None)
            if t is not None:
                yield t

    def is_empty(self):
        """return True if this UOWTask is 'empty', 
        meaning it has no child items.

        """

        return not self._objects and not self.dependencies

    def append(self, state, listonly=False, isdelete=False):
        if state not in self._objects:
            self._objects[state] = rec = UOWTaskElement(state)
        else:
            rec = self._objects[state]

        rec.update(listonly, isdelete)

    def append_postupdate(self, state, post_update_cols):
        """issue a 'post update' UPDATE statement via 
        this object's mapper immediately.

        """

        self.mapper._save_obj([state], self.uowtransaction, 
                            postupdate=True, post_update_cols=set(post_update_cols))

    def __contains__(self, state):
        """return True if the given object is contained 
        within this UOWTask or inheriting tasks."""

        for task in self.polymorphic_tasks:
            if state in task._objects:
                return True
        else:
            return False

    def is_deleted(self, state):
        """return True if the given object is marked 
        as to be deleted within this UOWTask."""

        try:
            return self._objects[state].isdelete
        except KeyError:
            return False

    def _polymorphic_collection(fn):
        """return a property that will adapt the collection returned by the
        given callable into a polymorphic traversal."""

        @property
        def collection(self):
            for task in self.polymorphic_tasks:
                for rec in fn(task):
                    yield rec
        return collection

    def _polymorphic_collection_filtered(fn):

        def collection(self, mappers):
            for task in self.polymorphic_tasks:
                if task.mapper in mappers:
                    for rec in fn(task):
                        yield rec
        return collection

    @property
    def elements(self):
        return self._objects.values()

    @_polymorphic_collection
    def polymorphic_elements(self):
        return self.elements

    @_polymorphic_collection_filtered
    def filter_polymorphic_elements(self):
        return self.elements

    @property
    def polymorphic_tosave_elements(self):
        return [rec for rec in self.polymorphic_elements if not rec.isdelete]

    @property
    def polymorphic_todelete_elements(self):
        return [rec for rec in self.polymorphic_elements if rec.isdelete]

    @property
    def polymorphic_tosave_objects(self):
        return [
            rec.state for rec in self.polymorphic_elements
            if rec.state is not None and not rec.listonly and rec.isdelete is False
        ]

    @property
    def polymorphic_todelete_objects(self):
        return [
            rec.state for rec in self.polymorphic_elements
            if rec.state is not None and not rec.listonly and rec.isdelete is True
        ]

    @_polymorphic_collection
    def polymorphic_dependencies(self):
        return self.dependencies

    @_polymorphic_collection
    def polymorphic_cyclical_dependencies(self):
        return self.cyclical_dependencies

    def _sort_circular_dependencies(self, trans, cycles):
        """sort row-level dependencies.
        
        Note that this method is a total disaster, as it was 
        bolted onto the originally simple unit-of-work
        system after more complex mappings revealed 
        the presence of inter-row rependencies - this occured
        well within version 0.1 and despite many fixes 
        has remained the most legacy code within SQLAlchemy.  
        It is gone without a trace after [ticket:1742].
        
        """

        dependencies = {}
        def set_processor_for_state(state, depprocessor, target_state, isdelete):
            if state not in dependencies:
                dependencies[state] = {}
            tasks = dependencies[state]
            if depprocessor not in tasks:
                tasks[depprocessor] = UOWDependencyProcessor(
                                            depprocessor.processor, 
                                            UOWTask(self.uowtransaction,
                                             depprocessor.targettask.mapper)
                                      )
            tasks[depprocessor].targettask.append(target_state, isdelete=isdelete)
            
        cycles = set(cycles)
        def dependency_in_cycles(dep):
            proctask = trans.get_task_by_mapper(dep.processor.mapper.base_mapper, True)
            targettask = trans.get_task_by_mapper(dep.targettask.mapper.base_mapper, True)
            return targettask in cycles and (proctask is not None and proctask in cycles)

        deps_by_targettask = {}
        extradeplist = []
        for task in cycles:
            for dep in task.polymorphic_dependencies:
                if not dependency_in_cycles(dep):
                    extradeplist.append(dep)
                for t in dep.targettask.polymorphic_tasks:
                    l = deps_by_targettask.setdefault(t, [])
                    l.append(dep)

        object_to_original_task = {}
        tuples = []

        for task in cycles:
            for subtask in task.polymorphic_tasks:
                for taskelement in subtask.elements:
                    state = taskelement.state
                    object_to_original_task[state] = subtask
                    if subtask not in deps_by_targettask:
                        continue
                    for dep in deps_by_targettask[subtask]:
                        if not dep.processor.has_dependencies or not dependency_in_cycles(dep):
                            continue
                        (processor, targettask) = (dep.processor, dep.targettask)
                        isdelete = taskelement.isdelete

                        # list of dependent objects from this object
                        (added, unchanged, deleted) = dep.get_object_dependencies(
                                                    state, trans, passive=True)
                        if not added and not unchanged and not deleted:
                            continue

                        # the task corresponding to saving/deleting of those dependent objects
                        childtask = trans.get_task_by_mapper(processor.mapper)

                        childlist = added + unchanged + deleted

                        for o in childlist:
                            if o is None:
                                continue

                            if o not in childtask:
                                childtask.append(o, listonly=True)
                                object_to_original_task[o] = childtask

                            whosdep = dep.whose_dependent_on_who(state, o)
                            if whosdep is not None:
                                tuples.append(whosdep)

                                if whosdep[0] is state:
                                    set_processor_for_state(whosdep[0], dep, whosdep[0],
                                                            isdelete=isdelete)
                                else:
                                    set_processor_for_state(whosdep[0], dep, whosdep[1],
                                                            isdelete=isdelete)
                            else:
                                # TODO: no test coverage here
                                set_processor_for_state(state, dep, state, isdelete=isdelete)

        t = UOWTask(self.uowtransaction, self.mapper)
        t.dependencies.update(extradeplist)

        used_tasks = set()

        # rationale for "tree" sort as opposed to a straight
        # dependency - keep non-dependent objects
        # grouped together, so that insert ordering as determined
        # by session.add() is maintained.
        head = topological._sort_as_tree(tuples, object_to_original_task.iterkeys())
        if head is not None:
            original_to_tasks = {}
            stack = [(head, t)]
            while stack:
                ((state, cycles, children), parenttask) = stack.pop()

                originating_task = object_to_original_task[state]
                used_tasks.add(originating_task)

                if (parenttask, originating_task) not in original_to_tasks:
                    task = UOWTask(self.uowtransaction, originating_task.mapper)
                    original_to_tasks[(parenttask, originating_task)] = task
                    parenttask.dependent_tasks.append(task)
                else:
                    task = original_to_tasks[(parenttask, originating_task)]

                task.append(state, originating_task._objects[state].listonly,
                            isdelete=originating_task._objects[state].isdelete)

                if state in dependencies:
                    task.cyclical_dependencies.update(dependencies[state].itervalues())

                stack += [(n, task) for n in children]

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
        return ("UOWTask(%s) Mapper: '%r'" % (hex(id(self)), self.mapper))

class UOWTaskElement(object):
    """Represent a single state to be saved.
    
    This object is deprecated.
    """
    def __init__(self, state):
        self.state = state
        self.listonly = True
        self.isdelete = False
        self.preprocessed = set()

    def update(self, listonly, isdelete):
        if not listonly and self.listonly:
            self.listonly = False
            self.preprocessed.clear()
        if isdelete and not self.isdelete:
            self.isdelete = True
            self.preprocessed.clear()

    def __repr__(self):
        return "UOWTaskElement/%d: %s/%d %s" % (
            id(self), 
            self.state.class_.__name__, 
            id(self.state.obj()), 
            (self.listonly and 'listonly' or (self.isdelete and 'delete' or 'save')) 
        )

class UOWDependencyProcessor(object):
    """Represent tasks in between inserts/updates/deletes.

    This object is deprecated.
    """
    def __init__(self, processor, targettask):
        self.processor = processor
        self.targettask = targettask
        prop = processor.prop
        
        # define a set of mappers which
        # will filter the lists of entities
        # this UOWDP processes.  this allows
        # MapperProperties to be overridden
        # at least for concrete mappers.
        self._mappers = set([
            m
            for m in self.processor.parent.polymorphic_iterator()
            if m._props[prop.key] is prop
        ]).union(self.processor.mapper.polymorphic_iterator())
            
    def __repr__(self):
        return "UOWDependencyProcessor(%s, %s)" % (str(self.processor), str(self.targettask))

    def __eq__(self, other):
        return other.processor is self.processor and other.targettask is self.targettask

    def __hash__(self):
        return hash((self.processor, self.targettask))

    def preexecute(self, trans):
        """preprocess all objects contained within 
        this ``UOWDependencyProcessor``s target task.
        """

        def getobj(elem):
            elem.preprocessed.add(self)
            return elem.state

        ret = False
        elements = [getobj(elem) for elem in 
                        self.targettask.filter_polymorphic_elements(self._mappers)
                        if self not in elem.preprocessed and not elem.isdelete]
        if elements:
            ret = True
            self.processor.preprocess_dependencies(self.targettask, elements, trans, delete=False)

        elements = [getobj(elem) for elem in 
                        self.targettask.filter_polymorphic_elements(self._mappers)
                        if self not in elem.preprocessed and elem.isdelete]
        if elements:
            ret = True
            self.processor.preprocess_dependencies(self.targettask, elements, trans, delete=True)
        return ret

    def execute(self, trans, delete):
        """process all objects contained within this 
        ``UOWDependencyProcessor``s target task."""


        elements = [e for e in 
                    self.targettask.filter_polymorphic_elements(self._mappers) 
                    if bool(e.isdelete)==delete]

        self.processor.process_dependencies(
            self.targettask, 
            [elem.state for elem in elements], 
            trans, 
            delete=delete)

    def get_object_dependencies(self, state, trans, passive):
        return trans.get_attribute_history(state, self.processor.key, passive=passive)

    def whose_dependent_on_who(self, state1, state2):
        """establish which object is operationally dependent amongst a parent/child
        using the semantics stated by the dependency processor.
        """
        return self.processor.whose_dependent_on_who(state1, state2)

class UOWExecutor(object):
    """Encapsulates the execution traversal 
    of a UOWTransaction structure.
    
    This part of the approach is the core flaw that's 
    being removed with [ticket:1742], as it necessitates
    deep levels of recursion.
    
    """

    def execute(self, trans, tasks, isdelete=None):
        if isdelete is not True:
            for task in tasks:
                self.execute_save_steps(trans, task)
        if isdelete is not False:
            for task in reversed(tasks):
                self.execute_delete_steps(trans, task)

    def save_objects(self, trans, task):
        task.mapper._save_obj(task.polymorphic_tosave_objects, trans)

    def delete_objects(self, trans, task):
        task.mapper._delete_obj(task.polymorphic_todelete_objects, trans)

    def execute_dependency(self, trans, dep, isdelete):
        dep.execute(trans, isdelete)

    def execute_save_steps(self, trans, task):
        self.save_objects(trans, task)
        for dep in task.polymorphic_cyclical_dependencies:
            self.execute_dependency(trans, dep, False)
        for dep in task.polymorphic_cyclical_dependencies:
            self.execute_dependency(trans, dep, True)
        self.execute_cyclical_dependencies(trans, task, False)
        self.execute_dependencies(trans, task)

    def execute_delete_steps(self, trans, task):
        self.execute_cyclical_dependencies(trans, task, True)
        self.delete_objects(trans, task)

    def execute_dependencies(self, trans, task):
        polymorphic_dependencies = list(task.polymorphic_dependencies)
        for dep in polymorphic_dependencies:
            self.execute_dependency(trans, dep, False)
        for dep in reversed(polymorphic_dependencies):
            self.execute_dependency(trans, dep, True)

    def execute_cyclical_dependencies(self, trans, task, isdelete):
        for t in task.dependent_tasks:
            self.execute(trans, [t], isdelete)
