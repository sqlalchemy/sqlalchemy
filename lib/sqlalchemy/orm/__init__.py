# orm/__init__.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
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
    make_transient
)
from .scoping import (
    scoped_session
)
from . import mapper as mapperlib
from .query import AliasOption, Query
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

        'items':relationship(SomeItem, backref=backref('parent', lazy='subquery'))

    """
    return (name, kwargs)


def deferred(*columns, **kwargs):
    """Return a :class:`.DeferredColumnProperty`, which indicates this
    object attributes should only be loaded from its corresponding
    table column when first accessed.

    Used with the "properties" dictionary sent to :func:`mapper`.

    See also:

    :ref:`deferred`

    """
    return ColumnProperty(deferred=True, *columns, **kwargs)


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
    are never discarded independently of their class.  If a mapped class itself
    is garbage collected, its mapper is automatically disposed of as well. As
    such, :func:`.clear_mappers` is only for usage in test suites that re-use
    the same classes with different mappings, which is itself an extremely rare
    use case - the only such use case is in fact SQLAlchemy's own test suite,
    and possibly the test suites of other ORM extension libraries which
    intend to test various combinations of mapper construction upon a fixed
    set of classes.

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


def joinedload(*keys, **kw):
    """Return a ``MapperOption`` that will convert the property of the given
    name or series of mapped attributes into an joined eager load.

    .. versionchanged:: 0.6beta3
        This function is known as :func:`eagerload` in all versions
        of SQLAlchemy prior to version 0.6beta3, including the 0.5 and 0.4
        series. :func:`eagerload` will remain available for the foreseeable
        future in order to enable cross-compatibility.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    examples::

        # joined-load the "orders" collection on "User"
        query(User).options(joinedload(User.orders))

        # joined-load the "keywords" collection on each "Item",
        # but not the "items" collection on "Order" - those
        # remain lazily loaded.
        query(Order).options(joinedload(Order.items, Item.keywords))

        # to joined-load across both, use joinedload_all()
        query(Order).options(joinedload_all(Order.items, Item.keywords))

        # set the default strategy to be 'joined'
        query(Order).options(joinedload('*'))

    :func:`joinedload` also accepts a keyword argument `innerjoin=True` which
    indicates using an inner join instead of an outer::

        query(Order).options(joinedload(Order.user, innerjoin=True))

    .. note::

       The join created by :func:`joinedload` is anonymously aliased such that
       it **does not affect the query results**.   An :meth:`.Query.order_by`
       or :meth:`.Query.filter` call **cannot** reference these aliased
       tables - so-called "user space" joins are constructed using
       :meth:`.Query.join`.   The rationale for this is that
       :func:`joinedload` is only applied in order to affect how related
       objects or collections are loaded as an optimizing detail - it can be
       added or removed with no impact on actual results.   See the section
       :ref:`zen_of_eager_loading` for a detailed description of how this is
       used, including how to use a single explicit JOIN for
       filtering/ordering and eager loading simultaneously.

    See also:  :func:`subqueryload`, :func:`lazyload`

    """
    innerjoin = kw.pop('innerjoin', None)
    if innerjoin is not None:
        return (
             _strategies.EagerLazyOption(keys, lazy='joined'),
             _strategies.EagerJoinOption(keys, innerjoin)
         )
    else:
        return _strategies.EagerLazyOption(keys, lazy='joined')


def joinedload_all(*keys, **kw):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path or series of mapped attributes
    into an joined eager load.

    .. versionchanged:: 0.6beta3
        This function is known as :func:`eagerload_all` in all versions
        of SQLAlchemy prior to version 0.6beta3, including the 0.5 and 0.4
        series. :func:`eagerload_all` will remain available for the
        foreseeable future in order to enable cross-compatibility.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    For example::

        query.options(joinedload_all('orders.items.keywords'))...

    will set all of ``orders``, ``orders.items``, and
    ``orders.items.keywords`` to load in one joined eager load.

    Individual descriptors are accepted as arguments as well::

        query.options(joinedload_all(User.orders, Order.items, Item.keywords))

    The keyword arguments accept a flag `innerjoin=True|False` which will
    override the value of the `innerjoin` flag specified on the
    relationship().

    See also:  :func:`subqueryload_all`, :func:`lazyload`

    """
    innerjoin = kw.pop('innerjoin', None)
    if innerjoin is not None:
        return (
            _strategies.EagerLazyOption(keys, lazy='joined', chained=True),
            _strategies.EagerJoinOption(keys, innerjoin, chained=True)
        )
    else:
        return _strategies.EagerLazyOption(keys, lazy='joined', chained=True)


def eagerload(*args, **kwargs):
    """A synonym for :func:`joinedload()`."""
    return joinedload(*args, **kwargs)


def eagerload_all(*args, **kwargs):
    """A synonym for :func:`joinedload_all()`"""
    return joinedload_all(*args, **kwargs)


def subqueryload(*keys):
    """Return a ``MapperOption`` that will convert the property
    of the given name or series of mapped attributes
    into an subquery eager load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    examples::

        # subquery-load the "orders" collection on "User"
        query(User).options(subqueryload(User.orders))

        # subquery-load the "keywords" collection on each "Item",
        # but not the "items" collection on "Order" - those
        # remain lazily loaded.
        query(Order).options(subqueryload(Order.items, Item.keywords))

        # to subquery-load across both, use subqueryload_all()
        query(Order).options(subqueryload_all(Order.items, Item.keywords))

        # set the default strategy to be 'subquery'
        query(Order).options(subqueryload('*'))

    See also:  :func:`joinedload`, :func:`lazyload`

    """
    return _strategies.EagerLazyOption(keys, lazy="subquery")


def subqueryload_all(*keys):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path or series of mapped attributes
    into a subquery eager load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    For example::

        query.options(subqueryload_all('orders.items.keywords'))...

    will set all of ``orders``, ``orders.items``, and
    ``orders.items.keywords`` to load in one subquery eager load.

    Individual descriptors are accepted as arguments as well::

        query.options(subqueryload_all(User.orders, Order.items,
        Item.keywords))

    See also:  :func:`joinedload_all`, :func:`lazyload`, :func:`immediateload`

    """
    return _strategies.EagerLazyOption(keys, lazy="subquery", chained=True)


def lazyload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given
    name or series of mapped attributes into a lazy load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`eagerload`, :func:`subqueryload`, :func:`immediateload`

    """
    return _strategies.EagerLazyOption(keys, lazy=True)


def lazyload_all(*keys):
    """Return a ``MapperOption`` that will convert all the properties
    along the given dot-separated path or series of mapped attributes
    into a lazy load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`eagerload`, :func:`subqueryload`, :func:`immediateload`

    """
    return _strategies.EagerLazyOption(keys, lazy=True, chained=True)


def noload(*keys):
    """Return a ``MapperOption`` that will convert the property of the
    given name or series of mapped attributes into a non-load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`lazyload`, :func:`eagerload`,
    :func:`subqueryload`, :func:`immediateload`

    """
    return _strategies.EagerLazyOption(keys, lazy=None)


def immediateload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given
    name or series of mapped attributes into an immediate load.

    The "immediate" load means the attribute will be fetched
    with a separate SELECT statement per parent in the
    same way as lazy loading - except the loader is guaranteed
    to be called at load time before the parent object
    is returned in the result.

    The normal behavior of lazy loading applies - if
    the relationship is a simple many-to-one, and the child
    object is already present in the :class:`.Session`,
    no SELECT statement will be emitted.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`lazyload`, :func:`eagerload`, :func:`subqueryload`

    .. versionadded:: 0.6.5

    """
    return _strategies.EagerLazyOption(keys, lazy='immediate')

contains_alias = public_factory(AliasOption, ".orm.contains_alias")


def contains_eager(*keys, **kwargs):
    """Return a ``MapperOption`` that will indicate to the query that
    the given attribute should be eagerly loaded from columns currently
    in the query.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    The option is used in conjunction with an explicit join that loads
    the desired rows, i.e.::

        sess.query(Order).\\
                join(Order.user).\\
                options(contains_eager(Order.user))

    The above query would join from the ``Order`` entity to its related
    ``User`` entity, and the returned ``Order`` objects would have the
    ``Order.user`` attribute pre-populated.

    :func:`contains_eager` also accepts an `alias` argument, which is the
    string name of an alias, an :func:`~sqlalchemy.sql.expression.alias`
    construct, or an :func:`~sqlalchemy.orm.aliased` construct. Use this when
    the eagerly-loaded rows are to come from an aliased table::

        user_alias = aliased(User)
        sess.query(Order).\\
                join((user_alias, Order.user)).\\
                options(contains_eager(Order.user, alias=user_alias))

    See also :func:`eagerload` for the "automatic" version of this
    functionality.

    For additional examples of :func:`contains_eager` see
    :ref:`contains_eager`.

    """
    alias = kwargs.pop('alias', None)
    if kwargs:
        raise exc.ArgumentError(
                'Invalid kwargs for contains_eager: %r' % list(kwargs.keys()))
    return _strategies.EagerLazyOption(keys, lazy='joined',
            propagate_to_loaders=False, chained=True), \
        _strategies.LoadEagerFromAliasOption(keys, alias=alias, chained=True)


def defer(*key):
    """Return a :class:`.MapperOption` that will convert the column property
    of the given name into a deferred load.

    Used with :meth:`.Query.options`.

    e.g.::

        from sqlalchemy.orm import defer

        query(MyClass).options(defer("attribute_one"),
                            defer("attribute_two"))

    A class bound descriptor is also accepted::

        query(MyClass).options(
                            defer(MyClass.attribute_one),
                            defer(MyClass.attribute_two))

    A "path" can be specified onto a related or collection object using a
    dotted name. The :func:`.orm.defer` option will be applied to that object
    when loaded::

        query(MyClass).options(
                            defer("related.attribute_one"),
                            defer("related.attribute_two"))

    To specify a path via class, send multiple arguments::

        query(MyClass).options(
                            defer(MyClass.related, MyOtherClass.attribute_one),
                            defer(MyClass.related, MyOtherClass.attribute_two))

    See also:

    :ref:`deferred`

    :param \*key: A key representing an individual path.   Multiple entries
     are accepted to allow a multiple-token path for a single target, not
     multiple targets.

    """
    return _strategies.DeferredOption(key, defer=True)


def undefer(*key):
    """Return a :class:`.MapperOption` that will convert the column property
    of the given name into a non-deferred (regular column) load.

    Used with :meth:`.Query.options`.

    e.g.::

        from sqlalchemy.orm import undefer

        query(MyClass).options(
                    undefer("attribute_one"),
                    undefer("attribute_two"))

    A class bound descriptor is also accepted::

        query(MyClass).options(
                    undefer(MyClass.attribute_one),
                    undefer(MyClass.attribute_two))

    A "path" can be specified onto a related or collection object using a
    dotted name. The :func:`.orm.undefer` option will be applied to that
    object when loaded::

        query(MyClass).options(
                    undefer("related.attribute_one"),
                    undefer("related.attribute_two"))

    To specify a path via class, send multiple arguments::

        query(MyClass).options(
                    undefer(MyClass.related, MyOtherClass.attribute_one),
                    undefer(MyClass.related, MyOtherClass.attribute_two))

    See also:

    :func:`.orm.undefer_group` as a means to "undefer" a group
    of attributes at once.

    :ref:`deferred`

    :param \*key: A key representing an individual path.   Multiple entries
     are accepted to allow a multiple-token path for a single target, not
     multiple targets.

    """
    return _strategies.DeferredOption(key, defer=False)


def undefer_group(name):
    """Return a :class:`.MapperOption` that will convert the given group of
    deferred column properties into a non-deferred (regular column) load.

    Used with :meth:`.Query.options`.

    e.g.::

        query(MyClass).options(undefer("group_one"))

    See also:

    :ref:`deferred`

    :param name: String name of the deferred group.   This name is
     established using the "group" name to the :func:`.orm.deferred`
     configurational function.

    """
    return _strategies.UndeferGroupOption(name)



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

