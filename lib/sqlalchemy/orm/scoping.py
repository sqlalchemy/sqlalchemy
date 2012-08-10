# orm/scoping.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import exc as sa_exc
from sqlalchemy.util import ScopedRegistry, ThreadLocalRegistry, warn
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm.session import Session


__all__ = ['ScopedSession']


class ScopedSession(object):
    """Provides thread-local management of Sessions.

    Typical invocation is via the :func:`.scoped_session`
    function::

      Session = scoped_session(sessionmaker())

    The internal registry is accessible,
    and by default is an instance of :class:`.ThreadLocalRegistry`.

    See also: :ref:`unitofwork_contextual`.

    """

    def __init__(self, session_factory, scopefunc=None):
        self.session_factory = session_factory
        if scopefunc:
            self.registry = ScopedRegistry(session_factory, scopefunc)
        else:
            self.registry = ThreadLocalRegistry(session_factory)

    def __call__(self, **kwargs):
        if kwargs:
            scope = kwargs.pop('scope', False)
            if scope is not None:
                if self.registry.has():
                    raise sa_exc.InvalidRequestError(
                            "Scoped session is already present; "
                            "no new arguments may be specified.")
                else:
                    sess = self.session_factory(**kwargs)
                    self.registry.set(sess)
                    return sess
            else:
                return self.session_factory(**kwargs)
        else:
            return self.registry()

    def remove(self):
        """Dispose of the current contextual session."""

        if self.registry.has():
            self.registry().close()
        self.registry.clear()

    def configure(self, **kwargs):
        """reconfigure the sessionmaker used by this ScopedSession."""

        if self.registry.has():
            warn('At least one scoped session is already present. '
                      ' configure() can not affect sessions that have '
                      'already been created.')

        self.session_factory.configure(**kwargs)

    def query_property(self, query_cls=None):
        """return a class property which produces a `Query` object
        against the class when called.

        e.g.::

            Session = scoped_session(sessionmaker())

            class MyClass(object):
                query = Session.query_property()

            # after mappers are defined
            result = MyClass.query.filter(MyClass.name=='foo').all()

        Produces instances of the session's configured query class by
        default.  To override and use a custom implementation, provide
        a ``query_cls`` callable.  The callable will be invoked with
        the class's mapper as a positional argument and a session
        keyword argument.

        There is no limit to the number of query properties placed on
        a class.

        """
        class query(object):
            def __get__(s, instance, owner):
                try:
                    mapper = class_mapper(owner)
                    if mapper:
                        if query_cls:
                            # custom query class
                            return query_cls(mapper, session=self.registry())
                        else:
                            # session's configured query class
                            return self.registry().query(mapper)
                except orm_exc.UnmappedClassError:
                    return None
        return query()

def instrument(name):
    def do(self, *args, **kwargs):
        return getattr(self.registry(), name)(*args, **kwargs)
    return do
for meth in Session.public_methods:
    setattr(ScopedSession, meth, instrument(meth))

def makeprop(name):
    def set(self, attr):
        setattr(self.registry(), name, attr)
    def get(self):
        return getattr(self.registry(), name)
    return property(get, set)
for prop in ('bind', 'dirty', 'deleted', 'new', 'identity_map',
                'is_active', 'autoflush', 'no_autoflush'):
    setattr(ScopedSession, prop, makeprop(prop))

def clslevel(name):
    def do(cls, *args, **kwargs):
        return getattr(Session, name)(*args, **kwargs)
    return classmethod(do)
for prop in ('close_all', 'object_session', 'identity_key'):
    setattr(ScopedSession, prop, clslevel(prop))

