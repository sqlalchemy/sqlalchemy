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
    """Handles the details of organizing and executing transaction
    tasks during a UnitOfWork object's flush() operation.

    """

    def __init__(self, session):
        self.session = session
        self.mapper_flush_opts = session._mapper_flush_opts

        # dictionary used by external actors to store arbitrary state
        # information.
        self.attributes = {}
        
        self.recs = []
        self.states = set()
        self.dependencies = []
        
    def _dependency(self, rec1, rec2):
        self.dependencies.append((rec1, rec2))
        
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
                            listonly=False, postupdate=False, 
                            post_update_cols=None):
        
        # if object is not in the overall session, do nothing
        if not self.session._contains_state(state):
            return
        
        if state in self.states:
            return
            
        mapper = _state_mapper(state)
        
        self.states.add(state)
        
        self.recs.extend(
            mapper.get_flush_actions(self, state)
        )
        
            
    def execute(self):
        # so here, thinking we could figure out a way to get
        # consecutive, "compatible" records to collapse together,
        # i.e. a bunch of updates become an executemany(), etc.
        # even though we usually need individual executes.
        for rec in topological.sort(self.dependencies, self.recs):
            rec.execute()

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

log.class_logger(UOWTransaction)

# TODO: don't know what these should be.
# its very hard not to use subclasses to define behavior here.
class Rec(object):
    pass

