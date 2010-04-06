# orm/unitofwork.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
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

from sqlalchemy import util, log, topological
from sqlalchemy.orm import attributes, interfaces
from sqlalchemy.orm import util as mapperutil
from sqlalchemy.orm.util import _state_mapper

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
        # process "save_update" cascade rules for when an instance 
        # is attached to another instance
        if oldvalue is newvalue:
            return newvalue
        sess = _state_session(state)
        if sess:
            prop = _state_mapper(state).get_property(self.key)
            if newvalue is not None and \
                prop.cascade.save_update and \
                newvalue not in sess:
                sess.add(newvalue)
            if prop.cascade.delete_orphan and \
                oldvalue in sess.new and \
                prop.mapper._is_orphan(attributes.instance_state(oldvalue)):
                sess.expunge(oldvalue)
        return newvalue


class UOWTransaction(object):
    """Handles the details of organizing and executing transaction
    tasks during a UnitOfWork object's flush() operation.

    """

    def __init__(self, session):
        self.session = session
        self.mapper_flush_opts = session._mapper_flush_opts

        # dictionary used by external actors to store arbitrary state
        # information.
        self.attributes = {}
        
        self.mappers = util.defaultdict(set)
        self.presort_actions = {}
        self.postsort_actions = {}
        self.states = {}
        self.dependencies = set()
    
    @property
    def has_work(self):
        return bool(self.states)

    def is_deleted(self, state):
        """return true if the given state is marked as deleted 
        within this UOWTransaction."""
        return state in self.states and self.states[state][0]
    
    def remove_state_actions(self, state):
        """remove pending actions for a state from the uowtransaction."""
        
        self.states[state] = (False, True)
        
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

    def register_object(self, state, isdelete=False, listonly=False):
        if not self.session._contains_state(state):
            return

        if state not in self.states:
            mapper = _state_mapper(state)
            
            if mapper not in self.mappers:
                mapper._per_mapper_flush_actions(self)
            
            self.mappers[mapper].add(state)
            self.states[state] = (isdelete, listonly)
        else:
            existing_isdelete, existing_listonly = self.states[state]
            self.states[state] = (
                existing_isdelete or isdelete,
                existing_listonly and listonly
            )
    
    def issue_post_update(self, state, post_update_cols):
        mapper = state.manager.mapper.base_mapper
        mapper._save_obj([state], self, \
                    postupdate=True, \
                    post_update_cols=set(post_update_cols))
    
    def states_for_mapper(self, mapper, isdelete, listonly):
        checktup = (isdelete, listonly)
        for state in self.mappers[mapper]:
            if self.states[state] == checktup:
                yield state
        
    def states_for_mapper_hierarchy(self, mapper, isdelete, listonly):
        checktup = (isdelete, listonly)
        for mapper in mapper.base_mapper.polymorphic_iterator():
            for state in self.mappers[mapper]:
                if self.states[state] == checktup:
                    yield state
                
    def execute(self):
        
        # execute presort_actions, until all states
        # have been processed.   a presort_action might
        # add new states to the uow.
        while True:
            ret = False
            for action in self.presort_actions.values():
                if action.execute(self):
                    ret = True
            if not ret:
                break
        
        # see if the graph of mapper dependencies has cycles.
        self.cycles = cycles = topological.find_cycles(
                                        self.dependencies, 
                                        self.postsort_actions.values())
        
        if cycles:
            # if yes, break the per-mapper actions into
            # per-state actions
            convert = dict(
                (rec, set(rec.per_state_flush_actions(self)))
                for rec in cycles
            )

            # rewrite the existing dependencies to point to
            # the per-state actions for those per-mapper actions
            # that were broken up.
            for edge in list(self.dependencies):
                if None in edge or \
                    edge[0].disabled or edge[1].disabled or \
                    cycles.issuperset(edge):
                    self.dependencies.remove(edge)
                elif edge[0] in cycles:
                    self.dependencies.remove(edge)
                    for dep in convert[edge[0]]:
                        self.dependencies.add((dep, edge[1]))
                elif edge[1] in cycles:
                    self.dependencies.remove(edge)
                    for dep in convert[edge[1]]:
                        self.dependencies.add((edge[0], dep))
        
        postsort_actions = set(
                                [a for a in self.postsort_actions.values()
                                if not a.disabled
                                ]
                            ).difference(cycles)
        
        #sort = topological.sort(self.dependencies, postsort_actions)
        #print "--------------"
        #print self.dependencies
        #print postsort_actions
        
        # execute
        if cycles:
            for set_ in topological.sort_as_subsets(
                                            self.dependencies, 
                                            postsort_actions):
                while set_:
                    n = set_.pop()
                    n.execute_aggregate(self, set_)
        else:
            for rec in topological.sort(
                                    self.dependencies, 
                                    postsort_actions):
                rec.execute(self)
            

    def finalize_flush_changes(self):
        """mark processed objects as clean / deleted after a successful flush().

        this method is called within the flush() method after the
        execute() method has succeeded and the transaction has been committed.
        
        """
        for state, (isdelete, listonly) in self.states.iteritems():
            if isdelete:
                self.session._remove_newly_deleted(state)
            else:
                # if listonly:
                #   debug... would like to see how many do this
                self.session._register_newly_persistent(state)

log.class_logger(UOWTransaction)

class PreSortRec(object):
    def __new__(cls, uow, *args):
        key = (cls, ) + args
        if key in uow.presort_actions:
            return uow.presort_actions[key]
        else:
            uow.presort_actions[key] = \
                                    ret = \
                                    object.__new__(cls)
            return ret

class PostSortRec(object):
    disabled = False
    
    def __new__(cls, uow, *args):
        key = (cls, ) + args
        if key in uow.postsort_actions:
            return uow.postsort_actions[key]
        else:
            uow.postsort_actions[key] = \
                                    ret = \
                                    object.__new__(cls)
            return ret
    
    def execute_aggregate(self, uow, recs):
        self.execute(uow)
        
    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            ",".join(str(x) for x in self.__dict__.values())
        )

class PropertyRecMixin(object):
    def __init__(self, uow, dependency_processor, delete, fromparent):
        self.dependency_processor = dependency_processor
        self.delete = delete
        self.fromparent = fromparent
        
        prop = dependency_processor.prop
        if fromparent:
            self._mappers = set(
                m for m in dependency_processor.parent.polymorphic_iterator()
                if m._props[prop.key] is prop
            )
        else:
            self._mappers = set(
                dependency_processor.mapper.polymorphic_iterator()
            )

    def __repr__(self):
        return "%s(%s, delete=%s)" % (
            self.__class__.__name__,
            self.dependency_processor,
            self.delete
        )
    
    processed = ()
    def _elements(self, uow):
        for mapper in self._mappers:
            for state in uow.mappers[mapper]:
                if state in self.processed:
                    continue
                (isdelete, listonly) = uow.states[state]
                if isdelete == self.delete and not listonly:
                    yield state
    
class GetDependentObjects(PropertyRecMixin, PreSortRec):
    def __init__(self, *args):
        super(GetDependentObjects, self).__init__(*args)
        self.processed = set()
        
    def execute(self, uow):
        states = list(self._elements(uow))
        if states:
            if self.delete:
                self.dependency_processor.presort_deletes(uow, states)
            else:
                self.dependency_processor.presort_saves(uow, states)
            self.processed.update(states)
            return True
        else:
            return False

class ProcessAll(PropertyRecMixin, PostSortRec):
    def execute(self, uow):
        states = list(self._elements(uow))
        if self.delete:
            self.dependency_processor.process_deletes(uow, states)
        else:
            self.dependency_processor.process_saves(uow, states)

    def per_state_flush_actions(self, uow):
        return iter([])
        
class SaveUpdateAll(PostSortRec):
    def __init__(self, uow, mapper):
        self.mapper = mapper
        assert mapper is mapper.base_mapper
        
    def execute(self, uow):
        self.mapper._save_obj(
            uow.states_for_mapper_hierarchy(self.mapper, False, False),
            uow
        )
    
    def per_state_flush_actions(self, uow):
        for rec in self.mapper._per_state_flush_actions(
                            uow, 
                            uow.states_for_mapper_hierarchy(self.mapper, False, False), 
                            False):
            yield rec
        
class DeleteAll(PostSortRec):
    def __init__(self, uow, mapper):
        self.mapper = mapper
        assert mapper is mapper.base_mapper

    def execute(self, uow):
        self.mapper._delete_obj(
            uow.states_for_mapper_hierarchy(self.mapper, True, False),
            uow
        )

    def per_state_flush_actions(self, uow):
        for rec in self.mapper._per_state_flush_actions(
                            uow, 
                            uow.states_for_mapper_hierarchy(self.mapper, True, False), 
                            True):
            yield rec

class ProcessState(PostSortRec):
    def __init__(self, uow, dependency_processor, delete, state):
        self.dependency_processor = dependency_processor
        self.delete = delete
        self.state = state

    def execute_aggregate(self, uow, recs):
        cls_ = self.__class__
        dependency_processor = self.dependency_processor
        delete = self.delete
        our_recs = [r for r in recs 
                        if r.__class__ is cls_ and 
                        r.dependency_processor is dependency_processor and
                        r.delete is delete]
        recs.difference_update(our_recs)
        states = [self.state] + [r.state for r in our_recs]
        if delete:
            dependency_processor.process_deletes(uow, states)
        else:
            dependency_processor.process_saves(uow, states)

    def __repr__(self):
        return "%s(%s, %s, delete=%s)" % (
            self.__class__.__name__,
            self.dependency_processor,
            mapperutil.state_str(self.state),
            self.delete
        )
        
class SaveUpdateState(PostSortRec):
    def __init__(self, uow, state, mapper):
        self.state = state
        self.mapper = mapper
        
    def execute_aggregate(self, uow, recs):
        cls_ = self.__class__
        mapper = self.mapper
        our_recs = [r for r in recs 
                        if r.__class__ is cls_ and 
                        r.mapper is mapper]
        recs.difference_update(our_recs)
        mapper._save_obj(
                        [self.state] + 
                        [r.state for r in our_recs], 
                        uow)

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            mapperutil.state_str(self.state)
        )

class DeleteState(PostSortRec):
    def __init__(self, uow, state, mapper):
        self.state = state
        self.mapper = mapper
        
    def execute_aggregate(self, uow, recs):
        cls_ = self.__class__
        mapper = self.mapper
        our_recs = [r for r in recs 
                        if r.__class__ is cls_ and 
                        r.mapper is mapper]
        recs.difference_update(our_recs)
        states = [self.state] + [r.state for r in our_recs]
        mapper._delete_obj(
                        [s for s in states if uow.states[s][0]], 
                        uow)

    def __repr__(self):
        return "%s(%s)" % (
            self.__class__.__name__,
            mapperutil.state_str(self.state)
        )

