# orm/identity.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import weakref
from . import attributes
from .. import util


class IdentityMap(object):
    def __init__(self):
        self._dict = {}
        self._modified = set()
        self._wr = weakref.ref(self)

    def keys(self):
        return self._dict.keys()

    def replace(self, state):
        raise NotImplementedError()

    def add(self, state):
        raise NotImplementedError()

    def _add_unpresent(self, state, key):
        """optional inlined form of add() which can assume item isn't present
        in the map"""
        self.add(state)

    def update(self, dict):
        raise NotImplementedError("IdentityMap uses add() to insert data")

    def clear(self):
        raise NotImplementedError("IdentityMap uses remove() to remove data")

    def _manage_incoming_state(self, state):
        state._instance_dict = self._wr

        if state.modified:
            self._modified.add(state)

    def _manage_removed_state(self, state):
        del state._instance_dict
        if state.modified:
            self._modified.discard(state)

    def _dirty_states(self):
        return self._modified

    def check_modified(self):
        """return True if any InstanceStates present have been marked
        as 'modified'.

        """
        return bool(self._modified)

    def has_key(self, key):
        return key in self

    def popitem(self):
        raise NotImplementedError("IdentityMap uses remove() to remove data")

    def pop(self, key, *args):
        raise NotImplementedError("IdentityMap uses remove() to remove data")

    def setdefault(self, key, default=None):
        raise NotImplementedError("IdentityMap uses add() to insert data")

    def __len__(self):
        return len(self._dict)

    def copy(self):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        raise NotImplementedError("IdentityMap uses add() to insert data")

    def __delitem__(self, key):
        raise NotImplementedError("IdentityMap uses remove() to remove data")


class WeakInstanceDict(IdentityMap):

    def __getitem__(self, key):
        state = self._dict[key]
        o = state.obj()
        if o is None:
            raise KeyError(key)
        return o

    def __contains__(self, key):
        try:
            if key in self._dict:
                state = self._dict[key]
                o = state.obj()
            else:
                return False
        except KeyError:
            return False
        else:
            return o is not None

    def contains_state(self, state):
        return state.key in self._dict and self._dict[state.key] is state

    def replace(self, state):
        if state.key in self._dict:
            existing = self._dict[state.key]
            if existing is not state:
                self._manage_removed_state(existing)
            else:
                return

        self._dict[state.key] = state
        self._manage_incoming_state(state)

    def add(self, state):
        key = state.key
        # inline of self.__contains__
        if key in self._dict:
            try:
                existing_state = self._dict[key]
                if existing_state is not state:
                    o = existing_state.obj()
                    if o is not None:
                        raise AssertionError(
                            "A conflicting state is already "
                            "present in the identity map for key %r"
                            % (key, ))
                else:
                    return
            except KeyError:
                pass
        self._dict[key] = state
        self._manage_incoming_state(state)

    def _add_unpresent(self, state, key):
        # inlined form of add() called by loading.py
        self._dict[key] = state
        state._instance_dict = self._wr

    def get(self, key, default=None):
        if key not in self._dict:
            return default
        state = self._dict[key]
        o = state.obj()
        if o is None:
            return default
        return o

    def items(self):
        values = self.all_states()
        result = []
        for state in values:
            value = state.obj()
            if value is not None:
                result.append((state.key, value))
        return result

    def values(self):
        values = self.all_states()
        result = []
        for state in values:
            value = state.obj()
            if value is not None:
                result.append(value)

        return result

    def __iter__(self):
        return iter(self.keys())

    if util.py2k:

        def iteritems(self):
            return iter(self.items())

        def itervalues(self):
            return iter(self.values())

    def all_states(self):
        if util.py2k:
            return self._dict.values()
        else:
            return list(self._dict.values())

    def _fast_discard(self, state):
        self._dict.pop(state.key, None)

    def discard(self, state):
        st = self._dict.pop(state.key, None)
        if st:
            assert st is state
            self._manage_removed_state(state)

    def safe_discard(self, state):
        if state.key in self._dict:
            st = self._dict[state.key]
            if st is state:
                self._dict.pop(state.key, None)
                self._manage_removed_state(state)

    def prune(self):
        return 0


class StrongInstanceDict(IdentityMap):
    """A 'strong-referencing' version of the identity map.

    .. deprecated:: this object is present in order to fulfill
       the ``weak_identity_map=False`` option of the Session.
       This option is present to allow compatibility with older applications,
       but it is recommended that strong references to objects
       be maintained by the calling application
       externally to the :class:`.Session` itself, to the degree
       that is needed by the application.

    """

    if util.py2k:
        def itervalues(self):
            return self._dict.itervalues()

        def iteritems(self):
            return self._dict.iteritems()

    def __iter__(self):
        return iter(self.dict_)

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def get(self, key, default=None):
        return self._dict.get(key, default)

    def values(self):
        return self._dict.values()

    def items(self):
        return self._dict.items()

    def all_states(self):
        return [attributes.instance_state(o) for o in self.values()]

    def contains_state(self, state):
        return (
            state.key in self and
            attributes.instance_state(self[state.key]) is state)

    def replace(self, state):
        if state.key in self._dict:
            existing = self._dict[state.key]
            existing = attributes.instance_state(existing)
            if existing is not state:
                self._manage_removed_state(existing)
            else:
                return

        self._dict[state.key] = state.obj()
        self._manage_incoming_state(state)

    def add(self, state):
        if state.key in self:
            if attributes.instance_state(self._dict[state.key]) is not state:
                raise AssertionError('A conflicting state is already '
                                     'present in the identity map for key %r'
                                     % (state.key, ))
        else:
            self._dict[state.key] = state.obj()
            self._manage_incoming_state(state)

    def _add_unpresent(self, state, key):
        # inlined form of add() called by loading.py
        self._dict[key] = state.obj()
        state._instance_dict = self._wr

    def _fast_discard(self, state):
        self._dict.pop(state.key, None)

    def discard(self, state):
        obj = self._dict.pop(state.key, None)
        if obj is not None:
            self._manage_removed_state(state)
            st = attributes.instance_state(obj)
            assert st is state

    def safe_discard(self, state):
        if state.key in self._dict:
            obj = self._dict[state.key]
            st = attributes.instance_state(obj)
            if st is state:
                self._dict.pop(state.key, None)
                self._manage_removed_state(state)

    def prune(self):
        """prune unreferenced, non-dirty states."""

        ref_count = len(self)
        dirty = [s.obj() for s in self.all_states() if s.modified]

        # work around http://bugs.python.org/issue6149
        keepers = weakref.WeakValueDictionary()
        keepers.update(self)

        self._dict.clear()
        self._dict.update(keepers)
        self.modified = bool(dirty)
        return ref_count - len(self)
