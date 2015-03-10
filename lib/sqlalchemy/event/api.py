# event/api.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Public API functions for the event system.

"""
from __future__ import absolute_import

from .. import util, exc
from .base import _registrars
from .registry import _EventKey

CANCEL = util.symbol('CANCEL')
NO_RETVAL = util.symbol('NO_RETVAL')


def _event_key(target, identifier, fn):
    for evt_cls in _registrars[identifier]:
        tgt = evt_cls._accept_with(target)
        if tgt is not None:
            return _EventKey(target, identifier, fn, tgt)
    else:
        raise exc.InvalidRequestError("No such event '%s' for target '%s'" %
                                      (identifier, target))


def listen(target, identifier, fn, *args, **kw):
    """Register a listener function for the given target.

    e.g.::

        from sqlalchemy import event
        from sqlalchemy.schema import UniqueConstraint

        def unique_constraint_name(const, table):
            const.name = "uq_%s_%s" % (
                table.name,
                list(const.columns)[0].name
            )
        event.listen(
                UniqueConstraint,
                "after_parent_attach",
                unique_constraint_name)


    A given function can also be invoked for only the first invocation
    of the event using the ``once`` argument::

        def on_config():
            do_config()

        event.listen(Mapper, "before_configure", on_config, once=True)

    .. versionadded:: 0.9.4 Added ``once=True`` to :func:`.event.listen`
       and :func:`.event.listens_for`.

    """

    _event_key(target, identifier, fn).listen(*args, **kw)


def listens_for(target, identifier, *args, **kw):
    """Decorate a function as a listener for the given target + identifier.

    e.g.::

        from sqlalchemy import event
        from sqlalchemy.schema import UniqueConstraint

        @event.listens_for(UniqueConstraint, "after_parent_attach")
        def unique_constraint_name(const, table):
            const.name = "uq_%s_%s" % (
                table.name,
                list(const.columns)[0].name
            )

    A given function can also be invoked for only the first invocation
    of the event using the ``once`` argument::

        @event.listens_for(Mapper, "before_configure", once=True)
        def on_config():
            do_config()


    .. versionadded:: 0.9.4 Added ``once=True`` to :func:`.event.listen`
       and :func:`.event.listens_for`.

    """
    def decorate(fn):
        listen(target, identifier, fn, *args, **kw)
        return fn
    return decorate


def remove(target, identifier, fn):
    """Remove an event listener.

    The arguments here should match exactly those which were sent to
    :func:`.listen`; all the event registration which proceeded as a result
    of this call will be reverted by calling :func:`.remove` with the same
    arguments.

    e.g.::

        # if a function was registered like this...
        @event.listens_for(SomeMappedClass, "before_insert", propagate=True)
        def my_listener_function(*arg):
            pass

        # ... it's removed like this
        event.remove(SomeMappedClass, "before_insert", my_listener_function)

    Above, the listener function associated with ``SomeMappedClass`` was also
    propagated to subclasses of ``SomeMappedClass``; the :func:`.remove`
    function will revert all of these operations.

    .. versionadded:: 0.9.0

    """
    _event_key(target, identifier, fn).remove()


def contains(target, identifier, fn):
    """Return True if the given target/ident/fn is set up to listen.

    .. versionadded:: 0.9.0

    """

    return _event_key(target, identifier, fn).contains()
