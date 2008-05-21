# identity.py
# Copyright (C) the SQLAlchemy authors and contributors
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import weakref

from sqlalchemy import util as base_util
from sqlalchemy.orm import attributes


class IdentityMap(dict):
    def __init__(self):
        self._mutable_attrs = weakref.WeakKeyDictionary()
        self.modified = False
        
    def add(self, state):
        raise NotImplementedError()
    
    def remove(self, state):
        raise NotImplementedError()
    
    def update(self, dict):
        raise NotImplementedError("IdentityMap uses add() to insert data")
    
    def clear(self):
        raise NotImplementedError("IdentityMap uses remove() to remove data")
        
    def _manage_incoming_state(self, state):
        if state.modified:  
            self.modified = True
        if state.manager.mutable_attributes:
            self._mutable_attrs[state] = True
    
    def _manage_removed_state(self, state):
        if state in self._mutable_attrs:
            del self._mutable_attrs[state]
            
    def check_modified(self):
        """return True if any InstanceStates present have been marked as 'modified'."""
        
        if not self.modified:
            for state in self._mutable_attrs:
                if state.check_modified():
                    return True
            else:
                return False
        else:
            return True
            
    def has_key(self, key):
        return key in self
        
    def popitem(self):
        raise NotImplementedError("IdentityMap uses remove() to remove data")

    def pop(self, key, *args):
        raise NotImplementedError("IdentityMap uses remove() to remove data")

    def setdefault(self, key, default=None):
        raise NotImplementedError("IdentityMap uses add() to insert data")

    def copy(self):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        raise NotImplementedError("IdentityMap uses add() to insert data")

    def __delitem__(self, key):
        raise NotImplementedError("IdentityMap uses remove() to remove data")
        
class WeakInstanceDict(IdentityMap):

    def __init__(self):
        IdentityMap.__init__(self)
        self._wr = weakref.ref(self)
        # RLock because the mutex is used by a cleanup
        # handler, which can be called at any time (including within an already mutexed block)
        self._mutex = base_util.threading.RLock()

    def __getitem__(self, key):
        state = dict.__getitem__(self, key)
        o = state.obj()
        if o is None:
            o = state._check_resurrect(self)
        if o is None:
            raise KeyError, key
        return o

    def __contains__(self, key):
        try:
            state = dict.__getitem__(self, key)
            o = state.obj()
            if o is None:
                o = state._check_resurrect(self)
        except KeyError:
            return False
        return o is not None
    
    def contains_state(self, state):
        return dict.get(self, state.key) is state
        
    def add(self, state):
        if state.key in self:
            if dict.__getitem__(self, state.key) is not state:
                raise AssertionError("A conflicting state is already present in the identity map for key %r" % state.key)
        else:
            dict.__setitem__(self, state.key, state)
            state._instance_dict = self._wr
            self._manage_incoming_state(state)
    
    def remove_key(self, key):
        state = dict.__getitem__(self, key)
        self.remove(state)
        
    def remove(self, state):
        if not self.contains_state(state):
            raise AssertionError("State %s is not present in this identity map" % state)
        dict.__delitem__(self, state.key)
        del state._instance_dict
        self._manage_removed_state(state)
    
    def discard(self, state):
        if self.contains_state(state):
            dict.__delitem__(self, state.key)
            del state._instance_dict
            self._manage_removed_state(state)
        
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
            
    def items(self):
        return list(self.iteritems())

    def iteritems(self):
        for state in dict.itervalues(self):
            value = state.obj()
            if value is not None:
                yield state.key, value

    def itervalues(self):
        for state in dict.itervalues(self):
            instance = state.obj()
            if instance is not None:
                yield instance

    def values(self):
        return list(self.itervalues())

    def all_states(self):
        return dict.values(self)
    
    def prune(self):
        return 0
        
class StrongInstanceDict(IdentityMap):
    def all_states(self):
        return [attributes.instance_state(o) for o in self.values()]
    
    def contains_state(self, state):
        return state.key in self and attributes.instance_state(self[state.key]) is state
    
    def add(self, state):
        dict.__setitem__(self, state.key, state.obj())
        self._manage_incoming_state(state)
    
    def remove(self, state):
        if not self.contains_state(state):
            raise AssertionError("State %s is not present in this identity map" % state)
        dict.__delitem__(self, state.key)
        self._manage_removed_state(state)
    
    def discard(self, state):
        if self.contains_state(state):
            dict.__delitem__(self, state.key)
            self._manage_removed_state(state)
            
    def remove_key(self, key):
        state = dict.__getitem__(self, key)
        self.remove(state)

    def prune(self):
        """prune unreferenced, non-dirty states."""
        
        ref_count = len(self)
        dirty = [s.obj() for s in self.all_states() if s.check_modified()]
        keepers = weakref.WeakValueDictionary(self)
        dict.clear(self)
        dict.update(self, keepers)
        self.modified = bool(dirty)
        return ref_count - len(self)
        
class IdentityManagedState(attributes.InstanceState):
    def _instance_dict(self):
        return None
    
    def _check_resurrect(self, instance_dict):
        instance_dict._mutex.acquire()
        try:
            return self.obj() or self.__resurrect(instance_dict)
        finally:
            instance_dict._mutex.release()
    
    def modified_event(self, attr, should_copy, previous, passive=False):
        attributes.InstanceState.modified_event(self, attr, should_copy, previous, passive)
        
        instance_dict = self._instance_dict()
        if instance_dict:
            instance_dict.modified = True
        
    def _cleanup(self, ref):
        # tiptoe around Python GC unpredictableness
        try:
            instance_dict = self._instance_dict()
            instance_dict._mutex.acquire()
        except:
            return
        # the mutexing here is based on the assumption that gc.collect()
        # may be firing off cleanup handlers in a different thread than that
        # which is normally operating upon the instance dict.
        try:
            try:
                self.__resurrect(instance_dict)
            except:
                # catch app cleanup exceptions.  no other way around this
                # without warnings being produced
                pass
        finally:
            instance_dict._mutex.release()

    def __resurrect(self, instance_dict):
        if self.check_modified():
            # store strong ref'ed version of the object; will revert
            # to weakref when changes are persisted
            obj = self.manager.new_instance(state=self)
            self.obj = weakref.ref(obj, self._cleanup)
            self._strong_obj = obj
            obj.__dict__.update(self.dict)
            self.dict = obj.__dict__
            self._run_on_load(obj)
            return obj
        else:
            instance_dict.remove(self)
            self.dispose()
            return None
