# orm/__init__.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""
Functional constructs for ORM configuration.

See the SQLAlchemy object relational tutorial and mapper configuration
documentation for an overview of how this module is used.

"""

from . import exc
from . import mapper as mapperlib
from . import strategy_options
from .attributes import AttributeEvent
from .attributes import InstrumentedAttribute
from .attributes import Mapped
from .attributes import QueryableAttribute
from .context import QueryContext
from .decl_api import as_declarative
from .decl_api import declarative_base
from .decl_api import declarative_mixin
from .decl_api import DeclarativeMeta
from .decl_api import declared_attr
from .decl_api import has_inherited_table
from .decl_api import registry
from .decl_api import synonym_for
from .descriptor_props import CompositeProperty
from .descriptor_props import SynonymProperty
from .identity import IdentityMap
from .instrumentation import ClassManager
from .interfaces import EXT_CONTINUE
from .interfaces import EXT_SKIP
from .interfaces import EXT_STOP
from .interfaces import InspectionAttr
from .interfaces import InspectionAttrInfo
from .interfaces import MANYTOMANY
from .interfaces import MANYTOONE
from .interfaces import MapperProperty
from .interfaces import NOT_EXTENSION
from .interfaces import ONETOMANY
from .interfaces import PropComparator
from .interfaces import UserDefinedOption
from .loading import merge_frozen_result
from .loading import merge_result
from .mapper import class_mapper
from .mapper import configure_mappers
from .mapper import Mapper
from .mapper import reconstructor
from .mapper import validates
from .properties import ColumnProperty
from .query import AliasOption
from .query import FromStatement
from .query import Query
from .relationships import foreign
from .relationships import RelationshipProperty
from .relationships import remote
from .scoping import scoped_session
from .session import close_all_sessions
from .session import make_transient
from .session import make_transient_to_detached
from .session import object_session
from .session import ORMExecuteState
from .session import Session
from .session import sessionmaker
from .session import SessionTransaction
from .state import AttributeState
from .state import InstanceState
from .strategy_options import contains_eager
from .strategy_options import defaultload
from .strategy_options import defer
from .strategy_options import immediateload
from .strategy_options import joinedload
from .strategy_options import lazyload
from .strategy_options import Load
from .strategy_options import load_only
from .strategy_options import noload
from .strategy_options import raiseload
from .strategy_options import selectin_polymorphic
from .strategy_options import selectinload
from .strategy_options import subqueryload
from .strategy_options import undefer
from .strategy_options import undefer_group
from .strategy_options import with_expression
from .unitofwork import UOWTransaction
from .util import aliased
from .util import Bundle
from .util import CascadeOptions
from .util import join
from .util import LoaderCriteriaOption
from .util import object_mapper
from .util import outerjoin
from .util import polymorphic_union
from .util import was_deleted
from .util import with_parent
from .util import with_polymorphic
from .. import sql as _sql
from .. import util as _sa_util
from ..exc import InvalidRequestError
from ..util.langhelpers import public_factory


def create_session(bind=None, **kwargs):
    r"""Create a new :class:`.Session`
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
    False.

    Usage::

      >>> from sqlalchemy.orm import create_session
      >>> session = create_session()

    It is recommended to use :func:`sessionmaker` instead of
    create_session().

    """

    kwargs.setdefault("autoflush", False)
    kwargs.setdefault("expire_on_commit", False)
    return Session(bind=bind, **kwargs)


with_loader_criteria = public_factory(LoaderCriteriaOption, ".orm")

relationship = public_factory(RelationshipProperty, ".orm.relationship")


def mapper(*arg, **kw):
    """Placeholder for the now-removed ``mapper()`` function.

    Classical mappings should be performed using the
    :meth:`_orm.registry.map_imperatively` method.

    This symbol remains in SQLAlchemy 2.0 to suit the deprecated use case
    of using the ``mapper()`` function as a target for ORM event listeners,
    which failed to be marked as deprecated in the 1.4 series.

    Global ORM mapper listeners should instead use the :class:`_orm.Mapper`
    class as the target.

    .. versionchanged:: 2.0  The ``mapper()`` function was removed; the
       symbol remains temporarily as a placeholder for the event listening
       use case.

    """
    raise InvalidRequestError(
        "The 'sqlalchemy.orm.mapper()' function is removed as of "
        "SQLAlchemy 2.0.  Use the "
        "'sqlalchemy.orm.registry.map_imperatively()` "
        "method of the ``sqlalchemy.orm.registry`` class to perform "
        "classical mapping."
    )


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
    kw["lazy"] = "dynamic"
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

    .. seealso::

        :ref:`relationships_backref`

    """

    return (name, kwargs)


def deferred(*columns, **kw):
    r"""Indicate a column-based mapped attribute that by default will
    not load unless accessed.

    :param \*columns: columns to be mapped.  This is typically a single
     :class:`_schema.Column` object,
     however a collection is supported in order
     to support multiple columns mapped under the same attribute.

    :param raiseload: boolean, if True, indicates an exception should be raised
     if the load operation is to take place.

     .. versionadded:: 1.4

     .. seealso::

        :ref:`deferred_raiseload`

    :param \**kw: additional keyword arguments passed to
     :class:`.ColumnProperty`.

    .. seealso::

        :ref:`deferred`

    """
    return ColumnProperty(deferred=True, *columns, **kw)


def query_expression(default_expr=_sql.null()):
    """Indicate an attribute that populates from a query-time SQL expression.

    :param default_expr: Optional SQL expression object that will be used in
        all cases if not assigned later with :func:`_orm.with_expression`.
        E.g.::

            from sqlalchemy.sql import literal

            class C(Base):
                #...
                my_expr = query_expression(literal(1))

        .. versionadded:: 1.3.18


    .. versionadded:: 1.2

    .. seealso::

        :ref:`mapper_querytime_expression`

    """
    prop = ColumnProperty(default_expr)
    prop.strategy_key = (("query_expression", True),)
    return prop


synonym = public_factory(SynonymProperty, ".orm.synonym")


def clear_mappers():
    """Remove all mappers from all classes.

    .. versionchanged:: 1.4  This function now locates all
       :class:`_orm.registry` objects and calls upon the
       :meth:`_orm.registry.dispose` method of each.

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

    mapperlib._dispose_registries(mapperlib._all_registries(), False)


contains_alias = public_factory(AliasOption, ".orm.contains_alias")

if True:
    from .events import AttributeEvents
    from .events import MapperEvents
    from .events import InstanceEvents
    from .events import InstrumentationEvents
    from .events import QueryEvents
    from .events import SessionEvents


def __go(lcls):
    global __all__
    global AppenderQuery
    from .. import util as sa_util
    from . import dynamic
    from . import events
    from . import loading
    import inspect as _inspect

    from .dynamic import AppenderQuery

    __all__ = sorted(
        name
        for name, obj in lcls.items()
        if not (name.startswith("_") or _inspect.ismodule(obj))
    )

    _sa_util.preloaded.import_prefix("sqlalchemy.orm")
    _sa_util.preloaded.import_prefix("sqlalchemy.ext")


__go(locals())
