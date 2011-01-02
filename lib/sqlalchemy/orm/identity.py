# orm/identity.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import weakref

from sqlalchemy import util as base_util
from sqlalchemy.orm import attributes


class IdentityMap(dict):
    def __init__(self):
        self._mutable_attrs = set()
        self._modified = set()
        self._wr = weakref.ref(self)

    def replace(self, state):
        raise NotImplementedError()

    def add(self, state):
        raise NotImplementedError()

    def remove(self, state):
        raise NotImplementedError()

    def update(self, dict):
        raise NotImplementedError("IdentityMap uses add() to insert data")

    def clear(self):
        raise NotImplementedError("IdentityMap uses remove() to remove data")

    def _manage_incoming_state(self, state):
        state._instance_dict = self._wr

        if state.modified:
            self._modified.add(state)
        if state.manager.mutable_attributes:
            self._mutable_attrs.add(state)

    def _manage_removed_state(self, state):
        del state._instance_dict
        self._mutable_attrs.discard(state)
        self._modified.discard(state)

    def _dirty_states(self):
        return self._modified.union(s for s in self._mutable_attrs.copy()
                                    if s.modified)

    def check_modified(self):
        """return True if any InstanceStates present have been marked as 'modified'."""

        if self._modified:
            return True
        else:
            for state in self._mutable_attrs.copy():
                if state.modified:
                    return True
        return False

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
        self._remove_mutex = base_util.threading.Lock()

    def __getitem__(self, key):
        state = dict.__getitem__(self, key)
        o = state.obj()
        if o is None:
            o = state._is_really_none()
        if o is None:
            raise KeyError, key
        return o

    def __contains__(self, key):
        try:
            if dict.__contains__(self, key):
                state = dict.__getitem__(self, key)
                o = state.obj()
                if o is None:
                    o = state._is_really_none()
            else:
                return False
        except KeyError:
            return False
        else:
            return o is not None

    def contains_state(self, state):
        return dict.get(self, state.key) is state

    def replace(self, state):
        if dict.__contains__(self, state.key):
            existing = dict.__getitem__(self, state.key)
            if existing is not state:
                self._manage_removed_state(existing)
            else:
                return

        dict.__setitem__(self, state.key, state)
        self._manage_incoming_state(state)

    def add(self, state):
        if state.key in self:
            if dict.__getitem__(self, state.key) is not state:
                raise AssertionError("A conflicting state is already "
                                    "present in the identity map for key %r" 
                                    % (state.key, ))
        else:
            dict.__setitem__(self, state.key, state)
            self._manage_incoming_state(state)

    def remove_key(self, key):
        state = dict.__getitem__(self, key)
        self.remove(state)

    def remove(self, state):
        self._remove_mutex.acquire()
        try:
            if dict.pop(self, state.key) is not state:
                raise AssertionError("State %s is not present in this identity map" % state)
        finally:
            self._remove_mutex.release()

        self._manage_removed_state(state)

    def discard(self, state):
        if self.contains_state(state):
            dict.__delitem__(self, state.key)
            self._manage_removed_state(state)

    def get(self, key, default=None):
        state = dict.get(self, key, default)
        if state is default:
            return default
        o = state.obj()
        if o is None:
            o = state._is_really_none()
        if o is None:
            return default
        return o


    def items(self):
    # Py2K
        return list(self.iteritems())

    def iteritems(self):
    # end Py2K
        self._remove_mutex.acquire()
        try:
            result = []
            for state in dict.values(self):
                value = state.obj()
                if value is not None:
                    result.append((state.key, value))

            return iter(result)
        finally:
            self._remove_mutex.release()

    def values(self):
    # Py2K
        return list(self.itervalues())

    def itervalues(self):
    # end Py2K
        self._remove_mutex.acquire()
        try:
            result = []
            for state in dict.values(self):
                value = state.obj()
                if value is not None:
                    result.append(value)

            return iter(result)
        finally:
            self._remove_mutex.release()

    def all_states(self):
        self._remove_mutex.acquire()
        try:
            # Py3K
            # return list(dict.values(self))

            # Py2K
            return dict.values(self)
            # end Py2K
        finally:
            self._remove_mutex.release()

    def prune(self):
        return 0

class StrongInstanceDict(IdentityMap):
    def all_states(self):
        return [attributes.instance_state(o) for o in self.itervalues()]

    def contains_state(self, state):
        return state.key in self and attributes.instance_state(self[state.key]) is state

    def replace(self, state):
        if dict.__contains__(self, state.key):
            existing = dict.__getitem__(self, state.key)
            existing = attributes.instance_state(existing)
            if existing is not state:
                self._manage_removed_state(existing)
            else:
                return

        dict.__setitem__(self, state.key, state.obj())
        self._manage_incoming_state(state)

    def add(self, state):
        if state.key in self:
            if attributes.instance_state(dict.__getitem__(self, state.key)) is not state:
                raise AssertionError("A conflicting state is already present in the identity map for key %r" % (state.key, ))
        else:
            dict.__setitem__(self, state.key, state.obj())
            self._manage_incoming_state(state)

    def remove(self, state):
        if attributes.instance_state(dict.pop(self, state.key)) is not state:
            raise AssertionError("State %s is not present in this identity map" % state)
        self._manage_removed_state(state)

    def discard(self, state):
        if self.contains_state(state):
            dict.__delitem__(self, state.key)
            self._manage_removed_state(state)

    def remove_key(self, key):
        state = attributes.instance_state(dict.__getitem__(self, key))
        self.remove(state)

    def prune(self):
        """prune unreferenced, non-dirty states."""

        ref_count = len(self)
        dirty = [s.obj() for s in self.all_states() if s.modified]

        # work around http://bugs.python.org/issue6149
        keepers = weakref.WeakValueDictionary()
        keepers.update(self)

        dict.clear(self)
        dict.update(self, keepers)
        self.modified = bool(dirty)
        return ref_count - len(self)

