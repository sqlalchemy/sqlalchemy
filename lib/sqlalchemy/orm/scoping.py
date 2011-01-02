# orm/scoping.py
# Copyright (C) 2005-2011 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import sqlalchemy.exceptions as sa_exc
from sqlalchemy.util import ScopedRegistry, ThreadLocalRegistry, \
                            to_list, get_cls_kwargs, deprecated,\
                            warn
from sqlalchemy.orm import (
    EXT_CONTINUE, MapperExtension, class_mapper, object_session
    )
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm.session import Session


__all__ = ['ScopedSession']


class ScopedSession(object):
    """Provides thread-local management of Sessions.

    Usage::

      Session = scoped_session(sessionmaker())

    ... use Session normally.

    The internal registry is accessible as well,
    and by default is an instance of :class:`.ThreadLocalRegistry`.


    """

    def __init__(self, session_factory, scopefunc=None):
        self.session_factory = session_factory
        if scopefunc:
            self.registry = ScopedRegistry(session_factory, scopefunc)
        else:
            self.registry = ThreadLocalRegistry(session_factory)
        self.extension = _ScopedExt(self)

    def __call__(self, **kwargs):
        if kwargs:
            scope = kwargs.pop('scope', False)
            if scope is not None:
                if self.registry.has():
                    raise sa_exc.InvalidRequestError("Scoped session is already present; "
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

    @deprecated("0.5", ":meth:`.ScopedSession.mapper` is deprecated.  "
        "Please see http://www.sqlalchemy.org/trac/wiki/UsageRecipes/SessionAwareMapper "
        "for information on how to replicate its behavior.")
    def mapper(self, *args, **kwargs):
        """return a :func:`.mapper` function which associates this ScopedSession with the Mapper.

        """

        from sqlalchemy.orm import mapper

        extension_args = dict((arg, kwargs.pop(arg))
                              for arg in get_cls_kwargs(_ScopedExt)
                              if arg in kwargs)

        kwargs['extension'] = extension = to_list(kwargs.get('extension', []))
        if extension_args:
            extension.append(self.extension.configure(**extension_args))
        else:
            extension.append(self.extension)
        return mapper(*args, **kwargs)

    def configure(self, **kwargs):
        """reconfigure the sessionmaker used by this ScopedSession."""

        if self.registry.has():
            warn('At least one scoped session is already present. '
                      ' configure() can not affect sessions that have '
                      'already been created.')

        self.session_factory.configure(**kwargs)

    def query_property(self, query_cls=None):
        """return a class property which produces a `Query` object against the
        class when called.

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
for prop in ('bind', 'dirty', 'deleted', 'new', 'identity_map', 'is_active', 'autoflush'):
    setattr(ScopedSession, prop, makeprop(prop))

def clslevel(name):
    def do(cls, *args, **kwargs):
        return getattr(Session, name)(*args, **kwargs)
    return classmethod(do)
for prop in ('close_all', 'object_session', 'identity_key'):
    setattr(ScopedSession, prop, clslevel(prop))

class _ScopedExt(MapperExtension):
    def __init__(self, context, validate=False, save_on_init=True):
        self.context = context
        self.validate = validate
        self.save_on_init = save_on_init
        self.set_kwargs_on_init = True

    def validating(self):
        return _ScopedExt(self.context, validate=True)

    def configure(self, **kwargs):
        return _ScopedExt(self.context, **kwargs)

    def instrument_class(self, mapper, class_):
        class query(object):
            def __getattr__(s, key):
                return getattr(self.context.registry().query(class_), key)
            def __call__(s):
                return self.context.registry().query(class_)
            def __get__(self, instance, cls):
                return self

        if not 'query' in class_.__dict__:
            class_.query = query()

        if self.set_kwargs_on_init and class_.__init__ is object.__init__:
            class_.__init__ = self._default__init__(mapper)

    def _default__init__(ext, mapper):
        def __init__(self, **kwargs):
            for key, value in kwargs.iteritems():
                if ext.validate:
                    if not mapper.get_property(key, resolve_synonyms=False,
                                               raiseerr=False):
                        raise sa_exc.ArgumentError(
                            "Invalid __init__ argument: '%s'" % key)
                setattr(self, key, value)
        return __init__

    def init_instance(self, mapper, class_, oldinit, instance, args, kwargs):
        if self.save_on_init:
            session = kwargs.pop('_sa_session', None)
            if session is None:
                session = self.context.registry()
            session._save_without_cascade(instance)
        return EXT_CONTINUE

    def init_failed(self, mapper, class_, oldinit, instance, args, kwargs):
        sess = object_session(instance)
        if sess:
            sess.expunge(instance)
        return EXT_CONTINUE

    def dispose_class(self, mapper, class_):
        if hasattr(class_, 'query'):
            delattr(class_, 'query')
