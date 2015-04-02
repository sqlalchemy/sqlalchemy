# orm/__init__.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Functional constructs for ORM configuration.

See the SQLAlchemy object relational tutorial and mapper configuration
documentation for an overview of how this module is used.

"""

from . import exc
from .mapper import (
    Mapper,
    _mapper_registry,
    class_mapper,
    configure_mappers,
    reconstructor,
    validates
)
from .interfaces import (
    EXT_CONTINUE,
    EXT_STOP,
    PropComparator,
)
from .deprecated_interfaces import (
    MapperExtension,
    SessionExtension,
    AttributeExtension,
)
from .util import (
    aliased,
    join,
    object_mapper,
    outerjoin,
    polymorphic_union,
    was_deleted,
    with_parent,
    with_polymorphic,
)
from .properties import ColumnProperty
from .relationships import RelationshipProperty
from .descriptor_props import (
    ComparableProperty,
    CompositeProperty,
    SynonymProperty,
)
from .relationships import (
    foreign,
    remote,
)
from .session import (
    Session,
    object_session,
    sessionmaker,
    make_transient,
    make_transient_to_detached
)
from .scoping import (
    scoped_session
)
from . import mapper as mapperlib
from .query import AliasOption, Query, Bundle
from ..util.langhelpers import public_factory
from .. import util as _sa_util
from . import strategies as _strategies


def create_session(bind=None, **kwargs):
    """Create a new :class:`.Session`
    with no automation enabled by default.

    This function is used primarily for testing.   The usual
    route to :class:`.Session` creation is via its constructor
    or the :func:`.sessionmaker` function.

    :param bind: optional, a single Connectable to use for all
      database access in the created
      :class:`~sqlalchemy.orm.session.Session`.

    :param \*\*kwargs: optional, passed through to the
      :class:`.Session` constructor.

    :returns: an :class:`~sqlalchemy.orm.session.Session` instance

    The defaults of create_session() are the opposite of that of
    :func:`sessionmaker`; ``autoflush`` and ``expire_on_commit`` are
    False, ``autocommit`` is True.  In this sense the session acts
    more like the "classic" SQLAlchemy 0.3 session with these.

    Usage::

      >>> from sqlalchemy.orm import create_session
      >>> session = create_session()

    It is recommended to use :func:`sessionmaker` instead of
    create_session().

    """
    kwargs.setdefault('autoflush', False)
    kwargs.setdefault('autocommit', True)
    kwargs.setdefault('expire_on_commit', False)
    return Session(bind=bind, **kwargs)

relationship = public_factory(RelationshipProperty, ".orm.relationship")


def relation(*arg, **kw):
    """A synonym for :func:`relationship`."""

    return relationship(*arg, **kw)


def dynamic_loader(argument, **kw):
    """Construct a dynamically-loading mapper property.

    This is essentially the same as
    using the ``lazy='dynamic'`` argument with :func:`relationship`::

        dynamic_loader(SomeClass)

        # is the same as

        relationship(SomeClass, lazy="dynamic")

    See the section :ref:`dynamic_relationship` for more details
    on dynamic loading.

    """
    kw['lazy'] = 'dynamic'
    return relationship(argument, **kw)


column_property = public_factory(ColumnProperty, ".orm.column_property")
composite = public_factory(CompositeProperty, ".orm.composite")


def backref(name, **kwargs):
    """Create a back reference with explicit keyword arguments, which are the
    same arguments one can send to :func:`relationship`.

    Used with the ``backref`` keyword argument to :func:`relationship` in
    place of a string argument, e.g.::

        'items':relationship(
            SomeItem, backref=backref('parent', lazy='subquery'))

    """
    return (name, kwargs)


def deferred(*columns, **kw):
    """Indicate a column-based mapped attribute that by default will
    not load unless accessed.

    :param \*columns: columns to be mapped.  This is typically a single
     :class:`.Column` object, however a collection is supported in order
     to support multiple columns mapped under the same attribute.

    :param \**kw: additional keyword arguments passed to
     :class:`.ColumnProperty`.

    .. seealso::

        :ref:`deferred`

    """
    return ColumnProperty(deferred=True, *columns, **kw)


mapper = public_factory(Mapper, ".orm.mapper")

synonym = public_factory(SynonymProperty, ".orm.synonym")

comparable_property = public_factory(ComparableProperty,
                                     ".orm.comparable_property")


@_sa_util.deprecated("0.7", message=":func:`.compile_mappers` "
                     "is renamed to :func:`.configure_mappers`")
def compile_mappers():
    """Initialize the inter-mapper relationships of all mappers that have
    been defined.

    """
    configure_mappers()


def clear_mappers():
    """Remove all mappers from all classes.

    This function removes all instrumentation from classes and disposes
    of their associated mappers.  Once called, the classes are unmapped
    and can be later re-mapped with new mappers.

    :func:`.clear_mappers` is *not* for normal use, as there is literally no
    valid usage for it outside of very specific testing scenarios. Normally,
    mappers are permanent structural components of user-defined classes, and
    are never discarded independently of their class.  If a mapped class
    itself is garbage collected, its mapper is automatically disposed of as
    well. As such, :func:`.clear_mappers` is only for usage in test suites
    that re-use the same classes with different mappings, which is itself an
    extremely rare use case - the only such use case is in fact SQLAlchemy's
    own test suite, and possibly the test suites of other ORM extension
    libraries which intend to test various combinations of mapper construction
    upon a fixed set of classes.

    """
    mapperlib._CONFIGURE_MUTEX.acquire()
    try:
        while _mapper_registry:
            try:
                # can't even reliably call list(weakdict) in jython
                mapper, b = _mapper_registry.popitem()
                mapper.dispose()
            except KeyError:
                pass
    finally:
        mapperlib._CONFIGURE_MUTEX.release()

from . import strategy_options

joinedload = strategy_options.joinedload._unbound_fn
joinedload_all = strategy_options.joinedload._unbound_all_fn
contains_eager = strategy_options.contains_eager._unbound_fn
defer = strategy_options.defer._unbound_fn
undefer = strategy_options.undefer._unbound_fn
undefer_group = strategy_options.undefer_group._unbound_fn
load_only = strategy_options.load_only._unbound_fn
lazyload = strategy_options.lazyload._unbound_fn
lazyload_all = strategy_options.lazyload_all._unbound_all_fn
subqueryload = strategy_options.subqueryload._unbound_fn
subqueryload_all = strategy_options.subqueryload_all._unbound_all_fn
immediateload = strategy_options.immediateload._unbound_fn
noload = strategy_options.noload._unbound_fn
defaultload = strategy_options.defaultload._unbound_fn

from .strategy_options import Load


def eagerload(*args, **kwargs):
    """A synonym for :func:`joinedload()`."""
    return joinedload(*args, **kwargs)


def eagerload_all(*args, **kwargs):
    """A synonym for :func:`joinedload_all()`"""
    return joinedload_all(*args, **kwargs)


contains_alias = public_factory(AliasOption, ".orm.contains_alias")


def __go(lcls):
    global __all__
    from .. import util as sa_util
    from . import dynamic
    from . import events
    import inspect as _inspect

    __all__ = sorted(name for name, obj in lcls.items()
                     if not (name.startswith('_') or _inspect.ismodule(obj)))

    _sa_util.dependencies.resolve_all("sqlalchemy.orm")

__go(locals())
