# event/registry.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Provides managed registration services on behalf of :func:`.listen`
arguments.

By "managed registration", we mean that event listening functions and
other objects can be added to various collections in such a way that their
membership in all those collections can be revoked at once, based on
an equivalent :class:`._EventKey`.

"""

from __future__ import absolute_import

import weakref
import collections
import types
from .. import exc, util


_key_to_collection = collections.defaultdict(dict)
"""
Given an original listen() argument, can locate all
listener collections and the listener fn contained

(target, identifier, fn) -> {
                            ref(listenercollection) -> ref(listener_fn)
                            ref(listenercollection) -> ref(listener_fn)
                            ref(listenercollection) -> ref(listener_fn)
                        }
"""

_collection_to_key = collections.defaultdict(dict)
"""
Given a _ListenerCollection or _DispatchDescriptor, can locate
all the original listen() arguments and the listener fn contained

ref(listenercollection) -> {
                            ref(listener_fn) -> (target, identifier, fn),
                            ref(listener_fn) -> (target, identifier, fn),
                            ref(listener_fn) -> (target, identifier, fn),
                        }
"""


def _collection_gced(ref):
    # defaultdict, so can't get a KeyError
    if not _collection_to_key or ref not in _collection_to_key:
        return
    listener_to_key = _collection_to_key.pop(ref)
    for key in listener_to_key.values():
        if key in _key_to_collection:
            # defaultdict, so can't get a KeyError
            dispatch_reg = _key_to_collection[key]
            dispatch_reg.pop(ref)
            if not dispatch_reg:
                _key_to_collection.pop(key)


def _stored_in_collection(event_key, owner):
    key = event_key._key

    dispatch_reg = _key_to_collection[key]

    owner_ref = owner.ref
    listen_ref = weakref.ref(event_key._listen_fn)

    if owner_ref in dispatch_reg:
        assert dispatch_reg[owner_ref] == listen_ref
    else:
        dispatch_reg[owner_ref] = listen_ref

    listener_to_key = _collection_to_key[owner_ref]
    listener_to_key[listen_ref] = key


def _removed_from_collection(event_key, owner):
    key = event_key._key

    dispatch_reg = _key_to_collection[key]

    listen_ref = weakref.ref(event_key._listen_fn)

    owner_ref = owner.ref
    dispatch_reg.pop(owner_ref, None)
    if not dispatch_reg:
        del _key_to_collection[key]

    if owner_ref in _collection_to_key:
        listener_to_key = _collection_to_key[owner_ref]
        listener_to_key.pop(listen_ref)


def _stored_in_collection_multi(newowner, oldowner, elements):
    if not elements:
        return

    oldowner = oldowner.ref
    newowner = newowner.ref

    old_listener_to_key = _collection_to_key[oldowner]
    new_listener_to_key = _collection_to_key[newowner]

    for listen_fn in elements:
        listen_ref = weakref.ref(listen_fn)
        key = old_listener_to_key[listen_ref]
        dispatch_reg = _key_to_collection[key]
        if newowner in dispatch_reg:
            assert dispatch_reg[newowner] == listen_ref
        else:
            dispatch_reg[newowner] = listen_ref

        new_listener_to_key[listen_ref] = key


def _clear(owner, elements):
    if not elements:
        return

    owner = owner.ref
    listener_to_key = _collection_to_key[owner]
    for listen_fn in elements:
        listen_ref = weakref.ref(listen_fn)
        key = listener_to_key[listen_ref]
        dispatch_reg = _key_to_collection[key]
        dispatch_reg.pop(owner, None)

        if not dispatch_reg:
            del _key_to_collection[key]


class _EventKey(object):
    """Represent :func:`.listen` arguments.
    """

    def __init__(self, target, identifier,
                 fn, dispatch_target, _fn_wrap=None):
        self.target = target
        self.identifier = identifier
        self.fn = fn
        if isinstance(fn, types.MethodType):
            self.fn_key = id(fn.__func__), id(fn.__self__)
        else:
            self.fn_key = id(fn)
        self.fn_wrap = _fn_wrap
        self.dispatch_target = dispatch_target

    @property
    def _key(self):
        return (id(self.target), self.identifier, self.fn_key)

    def with_wrapper(self, fn_wrap):
        if fn_wrap is self._listen_fn:
            return self
        else:
            return _EventKey(
                self.target,
                self.identifier,
                self.fn,
                self.dispatch_target,
                _fn_wrap=fn_wrap
            )

    def with_dispatch_target(self, dispatch_target):
        if dispatch_target is self.dispatch_target:
            return self
        else:
            return _EventKey(
                self.target,
                self.identifier,
                self.fn,
                dispatch_target,
                _fn_wrap=self.fn_wrap
            )

    def listen(self, *args, **kw):
        once = kw.pop("once", False)
        if once:
            self.with_wrapper(
                util.only_once(self._listen_fn)).listen(*args, **kw)
        else:
            self.dispatch_target.dispatch._listen(self, *args, **kw)

    def remove(self):
        key = self._key

        if key not in _key_to_collection:
            raise exc.InvalidRequestError(
                "No listeners found for event %s / %r / %s " %
                (self.target, self.identifier, self.fn)
            )
        dispatch_reg = _key_to_collection.pop(key)

        for collection_ref, listener_ref in dispatch_reg.items():
            collection = collection_ref()
            listener_fn = listener_ref()
            if collection is not None and listener_fn is not None:
                collection.remove(self.with_wrapper(listener_fn))

    def contains(self):
        """Return True if this event key is registered to listen.
        """
        return self._key in _key_to_collection

    def base_listen(self, propagate=False, insert=False,
                    named=False):

        target, identifier, fn = \
            self.dispatch_target, self.identifier, self._listen_fn

        dispatch_descriptor = getattr(target.dispatch, identifier)

        fn = dispatch_descriptor._adjust_fn_spec(fn, named)
        self = self.with_wrapper(fn)

        if insert:
            dispatch_descriptor.\
                for_modify(target.dispatch).insert(self, propagate)
        else:
            dispatch_descriptor.\
                for_modify(target.dispatch).append(self, propagate)

    @property
    def _listen_fn(self):
        return self.fn_wrap or self.fn

    def append_value_to_list(self, owner, list_, value):
        _stored_in_collection(self, owner)
        list_.append(value)

    def append_to_list(self, owner, list_):
        _stored_in_collection(self, owner)
        list_.append(self._listen_fn)

    def remove_from_list(self, owner, list_):
        _removed_from_collection(self, owner)
        list_.remove(self._listen_fn)

    def prepend_to_list(self, owner, list_):
        _stored_in_collection(self, owner)
        list_.insert(0, self._listen_fn)
