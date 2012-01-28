# orm/mapper.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Logic to map Python classes to and from selectables.

Defines the :class:`~sqlalchemy.orm.mapper.Mapper` class, the central
configurational unit which associates a class with a database table.

This is a semi-private module; the main configurational API of the ORM is
available in :class:`~sqlalchemy.orm.`.

"""

import types
import weakref
import operator
from itertools import chain, groupby
deque = __import__('collections').deque

from sqlalchemy import sql, util, log, exc as sa_exc, event, schema
from sqlalchemy.sql import expression, visitors, operators, util as sqlutil
from sqlalchemy.orm import instrumentation, attributes, sync, \
                        exc as orm_exc, unitofwork, events
from sqlalchemy.orm.interfaces import MapperProperty, EXT_CONTINUE, \
                                PropComparator

from sqlalchemy.orm.util import _INSTRUMENTOR, _class_to_mapper, \
     _state_mapper, class_mapper, instance_str, state_str

import sys
sessionlib = util.importlater("sqlalchemy.orm", "session")
properties = util.importlater("sqlalchemy.orm", "properties")

__all__ = (
    'Mapper',
    '_mapper_registry',
    'class_mapper',
    'object_mapper',
    )

_mapper_registry = weakref.WeakKeyDictionary()
_new_mappers = False
_already_compiling = False
_none_set = frozenset([None])

_memoized_configured_property = util.group_expirable_memoized_property()


# a constant returned by _get_attr_by_column to indicate
# this mapper is not handling an attribute for a particular
# column
NO_ATTRIBUTE = util.symbol('NO_ATTRIBUTE')

# lock used to synchronize the "mapper compile" step
_COMPILE_MUTEX = util.threading.RLock()

class Mapper(object):
    """Define the correlation of class attributes to database table
    columns.

    Instances of this class should be constructed via the
    :func:`~sqlalchemy.orm.mapper` function.

    """
    def __init__(self,
                 class_,
                 local_table,
                 properties = None,
                 primary_key = None,
                 non_primary = False,
                 inherits = None,
                 inherit_condition = None,
                 inherit_foreign_keys = None,
                 extension = None,
                 order_by = False,
                 always_refresh = False,
                 version_id_col = None,
                 version_id_generator = None,
                 polymorphic_on=None,
                 _polymorphic_map=None,
                 polymorphic_identity=None,
                 concrete=False,
                 with_polymorphic=None,
                 allow_null_pks=None,
                 allow_partial_pks=True,
                 batch=True,
                 column_prefix=None,
                 include_properties=None,
                 exclude_properties=None,
                 passive_updates=True,
                 eager_defaults=False,
                 _compiled_cache_size=100,
                 ):
        """Construct a new mapper.

        Mappers are normally constructed via the
        :func:`~sqlalchemy.orm.mapper` function.  See for details.

        """

        self.class_ = util.assert_arg_type(class_, type, 'class_')

        self.class_manager = None

        self._primary_key_argument = util.to_list(primary_key)
        self.non_primary = non_primary

        if order_by is not False:
            self.order_by = util.to_list(order_by)
        else:
            self.order_by = order_by

        self.always_refresh = always_refresh
        self.version_id_col = version_id_col
        self.version_id_generator = version_id_generator or \
                                        (lambda x:(x or 0) + 1)
        self.concrete = concrete
        self.single = False
        self.inherits = inherits
        self.local_table = local_table
        self.inherit_condition = inherit_condition
        self.inherit_foreign_keys = inherit_foreign_keys
        self._init_properties = properties or {}
        self.delete_orphans = []
        self.batch = batch
        self.eager_defaults = eager_defaults
        self.column_prefix = column_prefix
        self.polymorphic_on = expression._clause_element_as_expr(polymorphic_on)
        self._dependency_processors = []
        self.validators = util.immutabledict()
        self.passive_updates = passive_updates
        self._clause_adapter = None
        self._requires_row_aliasing = False
        self._inherits_equated_pairs = None
        self._memoized_values = {}
        self._compiled_cache_size = _compiled_cache_size
        self._reconstructor = None
        self._deprecated_extensions = util.to_list(extension or [])

        if allow_null_pks:
            util.warn_deprecated(
                    "the allow_null_pks option to Mapper() is "
                    "deprecated.  It is now allow_partial_pks=False|True, "
                    "defaults to True.")
            allow_partial_pks = allow_null_pks

        self.allow_partial_pks = allow_partial_pks

        self._set_with_polymorphic(with_polymorphic)

        if isinstance(self.local_table, expression._SelectBase):
            raise sa_exc.InvalidRequestError(
                "When mapping against a select() construct, map against "
                "an alias() of the construct instead."
                "This because several databases don't allow a "
                "SELECT from a subquery that does not have an alias."
                )

        if self.with_polymorphic and \
                    isinstance(self.with_polymorphic[1],
                                expression._SelectBase):
            self.with_polymorphic = (self.with_polymorphic[0],
                                self.with_polymorphic[1].alias())

        # our 'polymorphic identity', a string name that when located in a
        #  result set row indicates this Mapper should be used to construct
        # the object instance for that row.
        self.polymorphic_identity = polymorphic_identity

        # a dictionary of 'polymorphic identity' names, associating those
        # names with Mappers that will be used to construct object instances
        # upon a select operation.
        if _polymorphic_map is None:
            self.polymorphic_map = {}
        else:
            self.polymorphic_map = _polymorphic_map

        if include_properties is not None:
            self.include_properties = util.to_set(include_properties)
        else:
            self.include_properties = None
        if exclude_properties:
            self.exclude_properties = util.to_set(exclude_properties)
        else:
            self.exclude_properties = None

        self.configured = False

        # prevent this mapper from being constructed
        # while a configure_mappers() is occurring (and defer a configure_mappers()
        # until construction succeeds)
        _COMPILE_MUTEX.acquire()
        try:
            self._configure_inheritance()
            self._configure_legacy_instrument_class()
            self._configure_class_instrumentation()
            self._configure_listeners()
            self._configure_properties()
            self._configure_polymorphic_setter()
            self._configure_pks()
            global _new_mappers
            _new_mappers = True
            self._log("constructed")
            self._expire_memoizations()
        finally:
            _COMPILE_MUTEX.release()

    # major attributes initialized at the classlevel so that
    # they can be Sphinx-documented.

    local_table = None
    """The :class:`.Selectable` which this :class:`.Mapper` manages.

    Typically is an instance of :class:`.Table` or :class:`.Alias`. 
    May also be ``None``. 

    The "local" table is the
    selectable that the :class:`.Mapper` is directly responsible for 
    managing from an attribute access and flush perspective.   For
    non-inheriting mappers, the local table is the same as the
    "mapped" table.   For joined-table inheritance mappers, local_table
    will be the particular sub-table of the overall "join" which
    this :class:`.Mapper` represents.  If this mapper is a 
    single-table inheriting mapper, local_table will be ``None``.

    See also :attr:`~.Mapper.mapped_table`.

    """

    mapped_table = None
    """The :class:`.Selectable` to which this :class:`.Mapper` is mapped.

    Typically an instance of :class:`.Table`, :class:`.Join`, or 
    :class:`.Alias`.

    The "mapped" table is the selectable that 
    the mapper selects from during queries.   For non-inheriting 
    mappers, the mapped table is the same as the "local" table.
    For joined-table inheritance mappers, mapped_table references the
    full :class:`.Join` representing full rows for this particular
    subclass.  For single-table inheritance mappers, mapped_table
    references the base table.

    See also :attr:`~.Mapper.local_table`.

    """

    inherits = None
    """References the :class:`.Mapper` which this :class:`.Mapper` 
    inherits from, if any.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    configured = None
    """Represent ``True`` if this :class:`.Mapper` has been configured.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    See also :func:`.configure_mappers`.

    """

    concrete = None
    """Represent ``True`` if this :class:`.Mapper` is a concrete 
    inheritance mapper.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    tables = None
    """An iterable containing the collection of :class:`.Table` objects
    which this :class:`.Mapper` is aware of.

    If the mapper is mapped to a :class:`.Join`, or an :class:`.Alias`
    representing a :class:`.Select`, the individual :class:`.Table`
    objects that comprise the full construct will be represented here.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    primary_key = None
    """An iterable containing the collection of :class:`.Column` objects
    which comprise the 'primary key' of the mapped table, from the 
    perspective of this :class:`.Mapper`.

    This list is against the selectable in :attr:`~.Mapper.mapped_table`.  In the
    case of inheriting mappers, some columns may be managed by a superclass
    mapper.  For example, in the case of a :class:`.Join`, the primary
    key is determined by all of the primary key columns across all tables
    referenced by the :class:`.Join`.

    The list is also not necessarily the same as the primary key column
    collection associated with the underlying tables; the :class:`.Mapper` 
    features a ``primary_key`` argument that can override what the
    :class:`.Mapper` considers as primary key columns.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    class_ = None
    """The Python class which this :class:`.Mapper` maps.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    class_manager = None
    """The :class:`.ClassManager` which maintains event listeners
    and class-bound descriptors for this :class:`.Mapper`.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    single = None
    """Represent ``True`` if this :class:`.Mapper` is a single table 
    inheritance mapper.

    :attr:`~.Mapper.local_table` will be ``None`` if this flag is set.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    non_primary = None
    """Represent ``True`` if this :class:`.Mapper` is a "non-primary" 
    mapper, e.g. a mapper that is used only to selet rows but not for 
    persistence management.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    polymorphic_on = None
    """The :class:`.Column` specified as the ``polymorphic_on`` column
    for this :class:`.Mapper`, within an inheritance scenario.

    This attribute may also be of other types besides :class:`.Column`
    in a future SQLAlchemy release.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    polymorphic_map = None
    """A mapping of "polymorphic identity" identifiers mapped to :class:`.Mapper`
    instances, within an inheritance scenario.

    The identifiers can be of any type which is comparable to the 
    type of column represented by :attr:`~.Mapper.polymorphic_on`.

    An inheritance chain of mappers will all reference the same 
    polymorphic map object.  The object is used to correlate incoming
    result rows to target mappers.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    polymorphic_identity = None
    """Represent an identifier which is matched against the :attr:`~.Mapper.polymorphic_on`
    column during result row loading.

    Used only with inheritance, this object can be of any type which is
    comparable to the type of column represented by :attr:`~.Mapper.polymorphic_on`.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    base_mapper = None
    """The base-most :class:`.Mapper` in an inheritance chain.

    In a non-inheriting scenario, this attribute will always be this
    :class:`.Mapper`.   In an inheritance scenario, it references
    the :class:`.Mapper` which is parent to all other :class:`.Mapper`
    objects in the inheritance chain.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    columns = None
    """A collection of :class:`.Column` or other scalar expression 
    objects maintained by this :class:`.Mapper`.

    The collection behaves the same as that of the ``c`` attribute on 
    any :class:`.Table` object, except that only those columns included in
    this mapping are present, and are keyed based on the attribute name
    defined in the mapping, not necessarily the ``key`` attribute of the
    :class:`.Column` itself.   Additionally, scalar expressions mapped
    by :func:`.column_property` are also present here.

    This is a *read only* attribute determined during mapper construction.
    Behavior is undefined if directly modified.

    """

    validators = None
    """An immutable dictionary of attributes which have been decorated
    using the :func:`~.orm.validates` decorator.    
    
    The dictionary contains string attribute names as keys
    mapped to the actual validation method.
    
    """

    c = None
    """A synonym for :attr:`~.Mapper.columns`."""

    dispatch = event.dispatcher(events.MapperEvents)

    def _configure_inheritance(self):
        """Configure settings related to inherting and/or inherited mappers
        being present."""

        # a set of all mappers which inherit from this one.
        self._inheriting_mappers = set()

        if self.inherits:
            if isinstance(self.inherits, type):
                self.inherits = class_mapper(self.inherits, compile=False)
            if not issubclass(self.class_, self.inherits.class_):
                raise sa_exc.ArgumentError(
                        "Class '%s' does not inherit from '%s'" % 
                        (self.class_.__name__, self.inherits.class_.__name__))
            if self.non_primary != self.inherits.non_primary:
                np = not self.non_primary and "primary" or "non-primary"
                raise sa_exc.ArgumentError(
                        "Inheritance of %s mapper for class '%s' is "
                        "only allowed from a %s mapper" % 
                        (np, self.class_.__name__, np))
            # inherit_condition is optional.
            if self.local_table is None:
                self.local_table = self.inherits.local_table
                self.mapped_table = self.inherits.mapped_table
                self.single = True
            elif not self.local_table is self.inherits.local_table:
                if self.concrete:
                    self.mapped_table = self.local_table
                    for mapper in self.iterate_to_root():
                        if mapper.polymorphic_on is not None:
                            mapper._requires_row_aliasing = True
                else:
                    if self.inherit_condition is None:
                        # figure out inherit condition from our table to the
                        # immediate table of the inherited mapper, not its
                        # full table which could pull in other stuff we dont
                        # want (allows test/inheritance.InheritTest4 to pass)
                        self.inherit_condition = sqlutil.join_condition(
                                                    self.inherits.local_table,
                                                    self.local_table)
                    self.mapped_table = sql.join(
                                                self.inherits.mapped_table, 
                                                self.local_table,
                                                self.inherit_condition)

                    fks = util.to_set(self.inherit_foreign_keys)
                    self._inherits_equated_pairs = sqlutil.criterion_as_pairs(
                                                self.mapped_table.onclause,
                                                consider_as_foreign_keys=fks)
            else:
                self.mapped_table = self.local_table

            if self.polymorphic_identity is not None and not self.concrete:
                self._identity_class = self.inherits._identity_class
            else:
                self._identity_class = self.class_

            if self.version_id_col is None:
                self.version_id_col = self.inherits.version_id_col
                self.version_id_generator = self.inherits.version_id_generator
            elif self.inherits.version_id_col is not None and \
                self.version_id_col is not self.inherits.version_id_col:
                util.warn(
                    "Inheriting version_id_col '%s' does not match inherited "
                    "version_id_col '%s' and will not automatically populate "
                    "the inherited versioning column. "
                    "version_id_col should only be specified on "
                    "the base-most mapper that includes versioning." %
                    (self.version_id_col.description, 
                    self.inherits.version_id_col.description)
                )

            if self.order_by is False and \
                        not self.concrete and \
                        self.inherits.order_by is not False:
                self.order_by = self.inherits.order_by

            self.polymorphic_map = self.inherits.polymorphic_map
            self.batch = self.inherits.batch
            self.inherits._inheriting_mappers.add(self)
            self.base_mapper = self.inherits.base_mapper
            self.passive_updates = self.inherits.passive_updates
            self._all_tables = self.inherits._all_tables

            if self.polymorphic_identity is not None:
                self.polymorphic_map[self.polymorphic_identity] = self

        else:
            self._all_tables = set()
            self.base_mapper = self
            self.mapped_table = self.local_table
            if self.polymorphic_identity is not None:
                self.polymorphic_map[self.polymorphic_identity] = self
            self._identity_class = self.class_

        if self.mapped_table is None:
            raise sa_exc.ArgumentError(
                    "Mapper '%s' does not have a mapped_table specified." 
                    % self)

    def _set_with_polymorphic(self, with_polymorphic):
        if with_polymorphic == '*':
            self.with_polymorphic = ('*', None)
        elif isinstance(with_polymorphic, (tuple, list)):
            if isinstance(with_polymorphic[0], (basestring, tuple, list)):
                self.with_polymorphic = with_polymorphic
            else:
                self.with_polymorphic = (with_polymorphic, None)
        elif with_polymorphic is not None:
            raise sa_exc.ArgumentError("Invalid setting for with_polymorphic")
        else:
            self.with_polymorphic = None

        if isinstance(self.local_table, expression._SelectBase):
            raise sa_exc.InvalidRequestError(
                "When mapping against a select() construct, map against "
                "an alias() of the construct instead."
                "This because several databases don't allow a "
                "SELECT from a subquery that does not have an alias."
                )

        if self.with_polymorphic and \
                    isinstance(self.with_polymorphic[1],
                                expression._SelectBase):
            self.with_polymorphic = (self.with_polymorphic[0],
                                self.with_polymorphic[1].alias())
        if self.configured:
            self._expire_memoizations()

    def _set_concrete_base(self, mapper):
        """Set the given :class:`.Mapper` as the 'inherits' for this :class:`.Mapper`,
        assuming this :class:`.Mapper` is concrete and does not already have
        an inherits."""

        assert self.concrete
        assert not self.inherits
        assert isinstance(mapper, Mapper)
        self.inherits = mapper
        self.inherits.polymorphic_map.update(self.polymorphic_map)
        self.polymorphic_map = self.inherits.polymorphic_map
        for mapper in self.iterate_to_root():
            if mapper.polymorphic_on is not None:
                mapper._requires_row_aliasing = True
        self.batch = self.inherits.batch
        for mp in self.self_and_descendants:
            mp.base_mapper = self.inherits.base_mapper
        self.inherits._inheriting_mappers.add(self)
        self.passive_updates = self.inherits.passive_updates
        self._all_tables = self.inherits._all_tables

    def _set_polymorphic_on(self, polymorphic_on):
        self.polymorphic_on = polymorphic_on
        self._configure_polymorphic_setter(True)

    def _configure_legacy_instrument_class(self):

        if self.inherits:
            self.dispatch._update(self.inherits.dispatch)
            super_extensions = set(chain(*[m._deprecated_extensions 
                                    for m in self.inherits.iterate_to_root()]))
        else:
            super_extensions = set()

        for ext in self._deprecated_extensions:
            if ext not in super_extensions:
                ext._adapt_instrument_class(self, ext)

    def _configure_listeners(self):
        if self.inherits:
            super_extensions = set(chain(*[m._deprecated_extensions 
                                    for m in self.inherits.iterate_to_root()]))
        else:
            super_extensions = set()

        for ext in self._deprecated_extensions:
            if ext not in super_extensions:
                ext._adapt_listener(self, ext)

        if self.inherits:
            self.class_manager.dispatch._update(
                        self.inherits.class_manager.dispatch)

    def _configure_class_instrumentation(self):
        """If this mapper is to be a primary mapper (i.e. the
        non_primary flag is not set), associate this Mapper with the
        given class_ and entity name.

        Subsequent calls to ``class_mapper()`` for the class_/entity
        name combination will return this mapper.  Also decorate the
        `__init__` method on the mapped class to include optional
        auto-session attachment logic.

        """
        manager = attributes.manager_of_class(self.class_)

        if self.non_primary:
            if not manager or not manager.is_mapped:
                raise sa_exc.InvalidRequestError(
                    "Class %s has no primary mapper configured.  Configure "
                    "a primary mapper first before setting up a non primary "
                    "Mapper." % self.class_)
            self.class_manager = manager
            self._identity_class = manager.mapper._identity_class
            _mapper_registry[self] = True
            return

        if manager is not None:
            assert manager.class_ is self.class_
            if manager.is_mapped:
                raise sa_exc.ArgumentError(
                    "Class '%s' already has a primary mapper defined. "
                    "Use non_primary=True to "
                    "create a non primary Mapper.  clear_mappers() will "
                    "remove *all* current mappers from all classes." %
                    self.class_)
            #else:
                # a ClassManager may already exist as 
                # ClassManager.instrument_attribute() creates 
                # new managers for each subclass if they don't yet exist.

        _mapper_registry[self] = True

        self.dispatch.instrument_class(self, self.class_)

        if manager is None:
            manager = instrumentation.register_class(self.class_)

        self.class_manager = manager

        manager.mapper = self
        manager.deferred_scalar_loader = self._load_scalar_attributes
 
        # The remaining members can be added by any mapper, 
        # e_name None or not.
        if manager.info.get(_INSTRUMENTOR, False):
            return

        event.listen(manager, 'first_init', _event_on_first_init, raw=True)
        event.listen(manager, 'init', _event_on_init, raw=True)
        event.listen(manager, 'resurrect', _event_on_resurrect, raw=True)

        for key, method in util.iterate_attributes(self.class_):
            if isinstance(method, types.FunctionType):
                if hasattr(method, '__sa_reconstructor__'):
                    self._reconstructor = method
                    event.listen(manager, 'load', _event_on_load, raw=True)
                elif hasattr(method, '__sa_validators__'):
                    for name in method.__sa_validators__:
                        self.validators = self.validators.union(
                            {name : method}
                        )

        manager.info[_INSTRUMENTOR] = self

    @util.deprecated("0.7", message=":meth:`.Mapper.compile` "
                            "is replaced by :func:`.configure_mappers`")
    def compile(self):
        """Initialize the inter-mapper relationships of all mappers that
        have been constructed thus far.

        """
        configure_mappers()
        return self


    @property
    @util.deprecated("0.7", message=":attr:`.Mapper.compiled` "
                            "is replaced by :attr:`.Mapper.configured`")
    def compiled(self):
        return self.configured

    def dispose(self):
        # Disable any attribute-based compilation.
        self.configured = True

        if hasattr(self, '_configure_failed'):
            del self._configure_failed

        if not self.non_primary and \
            self.class_manager.is_mapped and \
            self.class_manager.mapper is self:
            instrumentation.unregister_class(self.class_)

    def _configure_pks(self):

        self.tables = sqlutil.find_tables(self.mapped_table)

        self._pks_by_table = {}
        self._cols_by_table = {}

        all_cols = util.column_set(chain(*[
                                col.proxy_set for col in
                                self._columntoproperty]))

        pk_cols = util.column_set(c for c in all_cols if c.primary_key)

        # identify primary key columns which are also mapped by this mapper.
        tables = set(self.tables + [self.mapped_table])
        self._all_tables.update(tables)
        for t in tables:
            if t.primary_key and pk_cols.issuperset(t.primary_key):
                # ordering is important since it determines the ordering of
                # mapper.primary_key (and therefore query.get())
                self._pks_by_table[t] =\
                                    util.ordered_column_set(t.primary_key).\
                                    intersection(pk_cols)
            self._cols_by_table[t] = \
                                    util.ordered_column_set(t.c).\
                                    intersection(all_cols)

        # determine cols that aren't expressed within our tables; mark these
        # as "read only" properties which are refreshed upon INSERT/UPDATE
        self._readonly_props = set(
            self._columntoproperty[col]
            for col in self._columntoproperty
            if not hasattr(col, 'table') or 
            col.table not in self._cols_by_table)

        # if explicit PK argument sent, add those columns to the 
        # primary key mappings
        if self._primary_key_argument:
            for k in self._primary_key_argument:
                if k.table not in self._pks_by_table:
                    self._pks_by_table[k.table] = util.OrderedSet()
                self._pks_by_table[k.table].add(k)

        # otherwise, see that we got a full PK for the mapped table
        elif self.mapped_table not in self._pks_by_table or \
                    len(self._pks_by_table[self.mapped_table]) == 0:
                raise sa_exc.ArgumentError(
                        "Mapper %s could not assemble any primary "
                        "key columns for mapped table '%s'" % 
                        (self, self.mapped_table.description))
        elif self.local_table not in self._pks_by_table and \
            isinstance(self.local_table, schema.Table):
            util.warn("Could not assemble any primary "
                        "keys for locally mapped table '%s' - "
                        "no rows will be persisted in this Table." 
                        % self.local_table.description)

        if self.inherits and \
                not self.concrete and \
                not self._primary_key_argument:
            # if inheriting, the "primary key" for this mapper is 
            # that of the inheriting (unless concrete or explicit)
            self.primary_key = self.inherits.primary_key
        else:
            # determine primary key from argument or mapped_table pks - 
            # reduce to the minimal set of columns
            if self._primary_key_argument:
                primary_key = sqlutil.reduce_columns(
                    [self.mapped_table.corresponding_column(c) for c in
                    self._primary_key_argument],
                    ignore_nonexistent_tables=True)
            else:
                primary_key = sqlutil.reduce_columns(
                                self._pks_by_table[self.mapped_table],
                                ignore_nonexistent_tables=True)

            if len(primary_key) == 0:
                raise sa_exc.ArgumentError(
                    "Mapper %s could not assemble any primary "
                    "key columns for mapped table '%s'" % 
                    (self, self.mapped_table.description))

            self.primary_key = tuple(primary_key)
            self._log("Identified primary key columns: %s", primary_key)

    def _configure_properties(self):

        # Column and other ClauseElement objects which are mapped
        self.columns = self.c = util.OrderedProperties()

        # object attribute names mapped to MapperProperty objects
        self._props = util.OrderedDict()

        # table columns mapped to lists of MapperProperty objects
        # using a list allows a single column to be defined as
        # populating multiple object attributes
        self._columntoproperty = _ColumnMapping(self)

        # load custom properties
        if self._init_properties:
            for key, prop in self._init_properties.iteritems():
                self._configure_property(key, prop, False)

        # pull properties from the inherited mapper if any.
        if self.inherits:
            for key, prop in self.inherits._props.iteritems():
                if key not in self._props and \
                    not self._should_exclude(key, key, local=False, column=None):
                    self._adapt_inherited_property(key, prop, False)

        # create properties for each column in the mapped table,
        # for those columns which don't already map to a property
        for column in self.mapped_table.columns:
            if column in self._columntoproperty:
                continue

            column_key = (self.column_prefix or '') + column.key

            if self._should_exclude(
                            column.key, column_key,
                             local=self.local_table.c.contains_column(column),
                             column=column
                            ):
                continue

            # adjust the "key" used for this column to that
            # of the inheriting mapper
            for mapper in self.iterate_to_root():
                if column in mapper._columntoproperty:
                    column_key = mapper._columntoproperty[column].key

            self._configure_property(column_key, 
                                    column, 
                                    init=False, 
                                    setparent=True)

    def _configure_polymorphic_setter(self, init=False):
        """Configure an attribute on the mapper representing the 
        'polymorphic_on' column, if applicable, and not 
        already generated by _configure_properties (which is typical).

        Also create a setter function which will assign this
        attribute to the value of the 'polymorphic_identity'
        upon instance construction, also if applicable.  This 
        routine will run when an instance is created.

        """
        setter = False

        if self.polymorphic_on is not None:
            setter = True

            if isinstance(self.polymorphic_on, basestring):
                # polymorphic_on specified as as string - link
                # it to mapped ColumnProperty
                try:
                    self.polymorphic_on = self._props[self.polymorphic_on]
                except KeyError:
                    raise sa_exc.ArgumentError(
                                "Can't determine polymorphic_on "
                                "value '%s' - no attribute is "
                                "mapped to this name." % self.polymorphic_on)

            if self.polymorphic_on in self._columntoproperty:
                # polymorphic_on is a column that is already mapped
                # to a ColumnProperty
                prop = self._columntoproperty[self.polymorphic_on]
                polymorphic_key = prop.key
                self.polymorphic_on = prop.columns[0]
                polymorphic_key = prop.key
            elif isinstance(self.polymorphic_on, MapperProperty):
                # polymorphic_on is directly a MapperProperty,
                # ensure it's a ColumnProperty
                if not isinstance(self.polymorphic_on, properties.ColumnProperty):
                    raise sa_exc.ArgumentError(
                            "Only direct column-mapped "
                            "property or SQL expression "
                            "can be passed for polymorphic_on")
                prop = self.polymorphic_on
                self.polymorphic_on = prop.columns[0]
                polymorphic_key = prop.key
            elif not expression.is_column(self.polymorphic_on):
                # polymorphic_on is not a Column and not a ColumnProperty;
                # not supported right now.
                raise sa_exc.ArgumentError(
                    "Only direct column-mapped "
                    "property or SQL expression "
                    "can be passed for polymorphic_on"
                )
            else:
                # polymorphic_on is a Column or SQL expression and doesn't
                # appear to be mapped.
                # this means it can be 1. only present in the with_polymorphic 
                # selectable or 2. a totally standalone SQL expression which we'd
                # hope is compatible with this mapper's mapped_table
                col = self.mapped_table.corresponding_column(self.polymorphic_on)
                if col is None:
                    # polymorphic_on doesn't derive from any column/expression 
                    # isn't present in the mapped table.
                    # we will make a "hidden" ColumnProperty for it. 
                    # Just check that if it's directly a schema.Column and we 
                    # have with_polymorphic, it's likely a user error if the
                    # schema.Column isn't represented somehow in either mapped_table or
                    # with_polymorphic.   Otherwise as of 0.7.4 we just go with it
                    # and assume the user wants it that way (i.e. a CASE statement)
                    setter = False
                    instrument = False
                    col = self.polymorphic_on
                    if isinstance(col, schema.Column) and (
                        self.with_polymorphic is  None or \
                       self.with_polymorphic[1].corresponding_column(col) is None
                        ):
                        raise sa_exc.InvalidRequestError(
                            "Could not map polymorphic_on column "
                            "'%s' to the mapped table - polymorphic "
                            "loads will not function properly"
                                 % col.description)
                else:
                    # column/expression that polymorphic_on derives from 
                    # is present in our mapped table
                    # and is probably mapped, but polymorphic_on itself
                    # is not.  This happens when 
                    # the polymorphic_on is only directly present in the 
                    # with_polymorphic selectable, as when use polymorphic_union.
                    # we'll make a separate ColumnProperty for it.
                    instrument = True

                key = getattr(col, 'key', None)
                if key:
                    if self._should_exclude(col.key, col.key, False, col):
                        raise sa_exc.InvalidRequestError(
                        "Cannot exclude or override the discriminator column %r" %
                        col.key)
                else:
                    self.polymorphic_on = col = col.label("_sa_polymorphic_on")
                    key = col.key

                self._configure_property(
                                key, 
                                properties.ColumnProperty(col, _instrument=instrument),
                                init=init, setparent=True)
                polymorphic_key = key
        else:
            # no polymorphic_on was set.
            # check inheriting mappers for one.
            for mapper in self.iterate_to_root():
                # determine if polymorphic_on of the parent
                # should be propagated here.   If the col
                # is present in our mapped table, or if our mapped
                # table is the same as the parent (i.e. single table
                # inheritance), we can use it
                if mapper.polymorphic_on is not None:
                    if self.mapped_table is mapper.mapped_table:
                        self.polymorphic_on = mapper.polymorphic_on
                    else:
                        self.polymorphic_on = \
                                self.mapped_table.corresponding_column(
                                                    mapper.polymorphic_on)
                    # we can use the parent mapper's _set_polymorphic_identity
                    # directly; it ensures the polymorphic_identity of the
                    # instance's mapper is used so is portable to subclasses.
                    if self.polymorphic_on is not None:
                        self._set_polymorphic_identity = mapper._set_polymorphic_identity
                    else:
                        self._set_polymorphic_identity = None
                    return

        if setter:
            def _set_polymorphic_identity(state):
                dict_ = state.dict
                state.get_impl(polymorphic_key).set(state, dict_,
                        state.manager.mapper.polymorphic_identity, None)

            self._set_polymorphic_identity = _set_polymorphic_identity
        else:
            self._set_polymorphic_identity = None



    def _adapt_inherited_property(self, key, prop, init):
        if not self.concrete:
            self._configure_property(key, prop, init=False, setparent=False)
        elif key not in self._props:
            self._configure_property(
                            key, 
                            properties.ConcreteInheritedProperty(), 
                            init=init, setparent=True)

    def _configure_property(self, key, prop, init=True, setparent=True):
        self._log("_configure_property(%s, %s)", key, prop.__class__.__name__)

        if not isinstance(prop, MapperProperty):
            # we were passed a Column or a list of Columns; 
            # generate a properties.ColumnProperty
            columns = util.to_list(prop)
            column = columns[0]
            if not expression.is_column(column):
                raise sa_exc.ArgumentError(
                        "%s=%r is not an instance of MapperProperty or Column"
                        % (key, prop))

            prop = self._props.get(key, None)

            if isinstance(prop, properties.ColumnProperty):
                if prop.parent is self:
                    raise sa_exc.InvalidRequestError(
                            "Implicitly combining column %s with column "
                            "%s under attribute '%s'.  Please configure one "
                            "or more attributes for these same-named columns "
                            "explicitly."
                             % (prop.columns[-1], column, key))

                # existing properties.ColumnProperty from an inheriting 
                # mapper. make a copy and append our column to it
                prop = prop.copy()
                prop.columns.insert(0, column)
                self._log("inserting column to existing list "
                            "in properties.ColumnProperty %s" % (key))

            elif prop is None or isinstance(prop, properties.ConcreteInheritedProperty):
                mapped_column = []
                for c in columns:
                    mc = self.mapped_table.corresponding_column(c)
                    if mc is None:
                        mc = self.local_table.corresponding_column(c)
                        if mc is not None:
                            # if the column is in the local table but not the
                            # mapped table, this corresponds to adding a
                            # column after the fact to the local table.
                            # [ticket:1523]
                            self.mapped_table._reset_exported()
                        mc = self.mapped_table.corresponding_column(c)
                        if mc is None:
                            raise sa_exc.ArgumentError(
                            "When configuring property '%s' on %s, "
                            "column '%s' is not represented in the mapper's "
                            "table. Use the `column_property()` function to "
                            "force this column to be mapped as a read-only "
                            "attribute." % (key, self, c))
                    mapped_column.append(mc)
                prop = properties.ColumnProperty(*mapped_column)
            else:
                raise sa_exc.ArgumentError(
                    "WARNING: when configuring property '%s' on %s, "
                    "column '%s' conflicts with property '%r'. "
                    "To resolve this, map the column to the class under a "
                    "different name in the 'properties' dictionary.  Or, "
                    "to remove all awareness of the column entirely "
                    "(including its availability as a foreign key), "
                    "use the 'include_properties' or 'exclude_properties' "
                    "mapper arguments to control specifically which table "
                    "columns get mapped." % 
                    (key, self, column.key, prop))

        if isinstance(prop, properties.ColumnProperty):
            col = self.mapped_table.corresponding_column(prop.columns[0])

            # if the column is not present in the mapped table, 
            # test if a column has been added after the fact to the 
            # parent table (or their parent, etc.) [ticket:1570]
            if col is None and self.inherits:
                path = [self]
                for m in self.inherits.iterate_to_root():
                    col = m.local_table.corresponding_column(prop.columns[0])
                    if col is not None:
                        for m2 in path:
                            m2.mapped_table._reset_exported()
                        col = self.mapped_table.corresponding_column(
                                                prop.columns[0])
                        break
                    path.append(m)

            # subquery expression, column not present in the mapped 
            # selectable.
            if col is None:
                col = prop.columns[0]

                # column is coming in after _readonly_props was 
                # initialized; check for 'readonly'
                if hasattr(self, '_readonly_props') and \
                    (not hasattr(col, 'table') or 
                    col.table not in self._cols_by_table):
                        self._readonly_props.add(prop)

            else:
                # if column is coming in after _cols_by_table was 
                # initialized, ensure the col is in the right set
                if hasattr(self, '_cols_by_table') and \
                                    col.table in self._cols_by_table and \
                                    col not in self._cols_by_table[col.table]:
                    self._cols_by_table[col.table].add(col)

            # if this properties.ColumnProperty represents the "polymorphic
            # discriminator" column, mark it.  We'll need this when rendering
            # columns in SELECT statements.
            if not hasattr(prop, '_is_polymorphic_discriminator'):
                prop._is_polymorphic_discriminator = \
                                    (col is self.polymorphic_on or
                                    prop.columns[0] is self.polymorphic_on)

            self.columns[key] = col
            for col in prop.columns + prop._orig_columns:
                for col in col.proxy_set:
                    self._columntoproperty[col] = prop

        prop.key = key

        if setparent:
            prop.set_parent(self, init)

        if key in self._props and \
                getattr(self._props[key], '_mapped_by_synonym', False):
            syn = self._props[key]._mapped_by_synonym
            raise sa_exc.ArgumentError(
                        "Can't call map_column=True for synonym %r=%r, "
                        "a ColumnProperty already exists keyed to the name "
                        "%r for column %r" % (syn, key, key, syn)
                    )

        self._props[key] = prop

        if not self.non_primary:
            prop.instrument_class(self)

        for mapper in self._inheriting_mappers:
            mapper._adapt_inherited_property(key, prop, init)

        if init:
            prop.init()
            prop.post_instrument_class(self)

        if self.configured:
            self._expire_memoizations()

    def _post_configure_properties(self):
        """Call the ``init()`` method on all ``MapperProperties``
        attached to this mapper.

        This is a deferred configuration step which is intended
        to execute once all mappers have been constructed.

        """

        self._log("_post_configure_properties() started")
        l = [(key, prop) for key, prop in self._props.iteritems()]
        for key, prop in l:
            self._log("initialize prop %s", key)

            if prop.parent is self and not prop._compile_started:
                prop.init()

            if prop._compile_finished:
                prop.post_instrument_class(self)

        self._log("_post_configure_properties() complete")
        self.configured = True

    def add_properties(self, dict_of_properties):
        """Add the given dictionary of properties to this mapper,
        using `add_property`.

        """
        for key, value in dict_of_properties.iteritems():
            self.add_property(key, value)

    def add_property(self, key, prop):
        """Add an individual MapperProperty to this mapper.

        If the mapper has not been configured yet, just adds the
        property to the initial properties dictionary sent to the
        constructor.  If this Mapper has already been configured, then
        the given MapperProperty is configured immediately.

        """
        self._init_properties[key] = prop
        self._configure_property(key, prop, init=self.configured)

    def _expire_memoizations(self):
        for mapper in self.iterate_to_root():
            _memoized_configured_property.expire_instance(mapper)

    @property
    def _log_desc(self):
        return "(" + self.class_.__name__ + \
            "|" + \
            (self.local_table is not None and 
                self.local_table.description or 
                str(self.local_table)) +\
            (self.non_primary and 
            "|non-primary" or "") + ")"

    def _log(self, msg, *args):

        self.logger.info(
            "%s " + msg, *((self._log_desc,) + args)
        )

    def _log_debug(self, msg, *args):
        self.logger.debug(
            "%s " + msg, *((self._log_desc,) + args)
        )

    def __repr__(self):
        return '<Mapper at 0x%x; %s>' % (
            id(self), self.class_.__name__)

    def __str__(self):
        return "Mapper|%s|%s%s" % (
            self.class_.__name__,
            self.local_table is not None and 
            self.local_table.description or None,
            self.non_primary and "|non-primary" or ""
        )

    def _is_orphan(self, state):
        o = False
        for mapper in self.iterate_to_root():
            for (key, cls) in mapper.delete_orphans:
                if attributes.manager_of_class(cls).has_parent(
                    state, key, optimistic=bool(state.key)):
                    return False
            o = o or bool(mapper.delete_orphans)
        return o

    def has_property(self, key):
        return key in self._props

    def get_property(self, key, _compile_mappers=True):
        """return a MapperProperty associated with the given key.
        """

        if _compile_mappers and _new_mappers:
            configure_mappers()

        try:
            return self._props[key]
        except KeyError:
            raise sa_exc.InvalidRequestError(
                    "Mapper '%s' has no property '%s'" % (self, key))

    @util.deprecated('0.6.4',
                     'Call to deprecated function mapper._get_col_to_pr'
                     'op(). Use mapper.get_property_by_column()')
    def _get_col_to_prop(self, col):
        return self._columntoproperty[col]

    def get_property_by_column(self, column):
        """Given a :class:`.Column` object, return the
        :class:`.MapperProperty` which maps this column."""

        return self._columntoproperty[column]

    @property
    def iterate_properties(self):
        """return an iterator of all MapperProperty objects."""
        if _new_mappers:
            configure_mappers()
        return self._props.itervalues()

    def _mappers_from_spec(self, spec, selectable):
        """given a with_polymorphic() argument, return the set of mappers it
        represents.

        Trims the list of mappers to just those represented within the given
        selectable, if present. This helps some more legacy-ish mappings.

        """
        if spec == '*':
            mappers = list(self.self_and_descendants)
        elif spec:
            mappers = [_class_to_mapper(m) for m in util.to_list(spec)]
            for m in mappers:
                if not m.isa(self):
                    raise sa_exc.InvalidRequestError(
                                "%r does not inherit from %r"  % 
                                (m, self))
        else:
            mappers = []

        if selectable is not None:
            tables = set(sqlutil.find_tables(selectable,
                            include_aliases=True))
            mappers = [m for m in mappers if m.local_table in tables]

        return mappers

    def _selectable_from_mappers(self, mappers):
        """given a list of mappers (assumed to be within this mapper's
        inheritance hierarchy), construct an outerjoin amongst those mapper's
        mapped tables.

        """

        from_obj = self.mapped_table
        for m in mappers:
            if m is self:
                continue
            if m.concrete:
                raise sa_exc.InvalidRequestError(
                        "'with_polymorphic()' requires 'selectable' argument "
                        "when concrete-inheriting mappers are used.")
            elif not m.single:
                from_obj = from_obj.outerjoin(m.local_table,
                                                m.inherit_condition)

        return from_obj

    @_memoized_configured_property
    def _single_table_criterion(self):
        if self.single and \
            self.inherits and \
            self.polymorphic_on is not None:
            return self.polymorphic_on.in_(
                m.polymorphic_identity
                for m in self.self_and_descendants)
        else:
            return None

    @_memoized_configured_property
    def _with_polymorphic_mappers(self):
        if not self.with_polymorphic:
            return [self]
        return self._mappers_from_spec(*self.with_polymorphic)

    @_memoized_configured_property
    def _with_polymorphic_selectable(self):
        if not self.with_polymorphic:
            return self.mapped_table

        spec, selectable = self.with_polymorphic
        if selectable is not None:
            return selectable
        else:
            return self._selectable_from_mappers(
                            self._mappers_from_spec(spec, selectable))

    def _with_polymorphic_args(self, spec=None, selectable=False):
        if self.with_polymorphic:
            if not spec:
                spec = self.with_polymorphic[0]
            if selectable is False:
                selectable = self.with_polymorphic[1]

        mappers = self._mappers_from_spec(spec, selectable)
        if selectable is not None:
            return mappers, selectable
        else:
            return mappers, self._selectable_from_mappers(mappers)

    @_memoized_configured_property
    def _polymorphic_properties(self):
        return tuple(self._iterate_polymorphic_properties(
            self._with_polymorphic_mappers))

    def _iterate_polymorphic_properties(self, mappers=None):
        """Return an iterator of MapperProperty objects which will render into
        a SELECT."""

        if mappers is None:
            mappers = self._with_polymorphic_mappers

        if not mappers:
            for c in self.iterate_properties:
                yield c
        else:
            # in the polymorphic case, filter out discriminator columns
            # from other mappers, as these are sometimes dependent on that
            # mapper's polymorphic selectable (which we don't want rendered)
            for c in util.unique_list(
                chain(*[list(mapper.iterate_properties) for mapper in [self] +
                        mappers])
            ):
                if getattr(c, '_is_polymorphic_discriminator', False) and \
                        (self.polymorphic_on is None or 
                        c.columns[0] is not self.polymorphic_on):
                        continue
                yield c

    @property
    def properties(self):
        raise NotImplementedError(
                    "Public collection of MapperProperty objects is "
                    "provided by the get_property() and iterate_properties "
                    "accessors.")

    @_memoized_configured_property
    def _get_clause(self):
        """create a "get clause" based on the primary key.  this is used
        by query.get() and many-to-one lazyloads to load this item
        by primary key.

        """
        params = [(primary_key, sql.bindparam(None, type_=primary_key.type))
                  for primary_key in self.primary_key]
        return sql.and_(*[k==v for (k, v) in params]), \
                util.column_dict(params)

    @_memoized_configured_property
    def _equivalent_columns(self):
        """Create a map of all *equivalent* columns, based on
        the determination of column pairs that are equated to
        one another based on inherit condition.  This is designed
        to work with the queries that util.polymorphic_union
        comes up with, which often don't include the columns from
        the base table directly (including the subclass table columns
        only).

        The resulting structure is a dictionary of columns mapped
        to lists of equivalent columns, i.e.

        {
            tablea.col1:
                set([tableb.col1, tablec.col1]),
            tablea.col2:
                set([tabled.col2])
        }

        """
        result = util.column_dict()
        def visit_binary(binary):
            if binary.operator == operators.eq:
                if binary.left in result:
                    result[binary.left].add(binary.right)
                else:
                    result[binary.left] = util.column_set((binary.right,))
                if binary.right in result:
                    result[binary.right].add(binary.left)
                else:
                    result[binary.right] = util.column_set((binary.left,))
        for mapper in self.base_mapper.self_and_descendants:
            if mapper.inherit_condition is not None:
                visitors.traverse(
                                    mapper.inherit_condition, {},
                                    {'binary':visit_binary})

        return result

    def _is_userland_descriptor(self, obj):
        return not isinstance(obj, 
                    (MapperProperty, attributes.QueryableAttribute)) and \
                    hasattr(obj, '__get__') and not \
                     isinstance(obj.__get__(None, obj),
                                    attributes.QueryableAttribute)


    def _should_exclude(self, name, assigned_name, local, column):
        """determine whether a particular property should be implicitly
        present on the class.

        This occurs when properties are propagated from an inherited class, or
        are applied from the columns present in the mapped table.

        """

        # check for descriptors, either local or from
        # an inherited class
        if local:
            if self.class_.__dict__.get(assigned_name, None) is not None \
                and self._is_userland_descriptor(
                            self.class_.__dict__[assigned_name]):
                return True
        else:
            if getattr(self.class_, assigned_name, None) is not None \
                and self._is_userland_descriptor(
                            getattr(self.class_, assigned_name)):
                return True

        if self.include_properties is not None and \
                name not in self.include_properties and \
                (column is None or column not in self.include_properties):
            self._log("not including property %s" % (name))
            return True

        if self.exclude_properties is not None and \
            (
                name in self.exclude_properties or \
                (column is not None and column in self.exclude_properties)
            ):
            self._log("excluding property %s" % (name))
            return True

        return False

    def common_parent(self, other):
        """Return true if the given mapper shares a 
        common inherited parent as this mapper."""

        return self.base_mapper is other.base_mapper

    def _canload(self, state, allow_subtypes):
        s = self.primary_mapper()
        if self.polymorphic_on is not None or allow_subtypes:
            return _state_mapper(state).isa(s)
        else:
            return _state_mapper(state) is s

    def isa(self, other):
        """Return True if the this mapper inherits from the given mapper."""

        m = self
        while m and m is not other:
            m = m.inherits
        return bool(m)

    def iterate_to_root(self):
        m = self
        while m:
            yield m
            m = m.inherits

    @_memoized_configured_property
    def self_and_descendants(self):
        """The collection including this mapper and all descendant mappers.

        This includes not just the immediately inheriting mappers but
        all their inheriting mappers as well.

        """
        descendants = []
        stack = deque([self])
        while stack:
            item = stack.popleft()
            descendants.append(item)
            stack.extend(item._inheriting_mappers)
        return tuple(descendants)

    def polymorphic_iterator(self):
        """Iterate through the collection including this mapper and
        all descendant mappers.

        This includes not just the immediately inheriting mappers but
        all their inheriting mappers as well.

        To iterate through an entire hierarchy, use
        ``mapper.base_mapper.polymorphic_iterator()``.

        """
        return iter(self.self_and_descendants)

    def primary_mapper(self):
        """Return the primary mapper corresponding to this mapper's class key
        (class)."""

        return self.class_manager.mapper

    @property
    def primary_base_mapper(self):
        return self.class_manager.mapper.base_mapper

    def identity_key_from_row(self, row, adapter=None):
        """Return an identity-map key for use in storing/retrieving an
        item from the identity map.

        row
          A ``sqlalchemy.engine.base.RowProxy`` instance or a
          dictionary corresponding result-set ``ColumnElement``
          instances to their values within a row.

        """
        pk_cols = self.primary_key
        if adapter:
            pk_cols = [adapter.columns[c] for c in pk_cols]

        return self._identity_class, \
                tuple(row[column] for column in pk_cols)

    def identity_key_from_primary_key(self, primary_key):
        """Return an identity-map key for use in storing/retrieving an
        item from an identity map.

        primary_key
          A list of values indicating the identifier.

        """
        return self._identity_class, tuple(primary_key)

    def identity_key_from_instance(self, instance):
        """Return the identity key for the given instance, based on
        its primary key attributes.

        This value is typically also found on the instance state under the
        attribute name `key`.

        """
        return self.identity_key_from_primary_key(
                        self.primary_key_from_instance(instance))

    def _identity_key_from_state(self, state):
        dict_ = state.dict
        manager = state.manager
        return self._identity_class, tuple([
            manager[self._columntoproperty[col].key].\
                impl.get(state, dict_, attributes.PASSIVE_OFF)
            for col in self.primary_key
        ])

    def primary_key_from_instance(self, instance):
        """Return the list of primary key values for the given
        instance.

        """
        state = attributes.instance_state(instance)
        return self._primary_key_from_state(state)

    def _primary_key_from_state(self, state):
        dict_ = state.dict
        manager = state.manager
        return [
            manager[self._columntoproperty[col].key].\
                impl.get(state, dict_, attributes.PASSIVE_OFF)
            for col in self.primary_key
        ]

    def _get_state_attr_by_column(self, state, dict_, column, 
                                    passive=attributes.PASSIVE_OFF):
        prop = self._columntoproperty[column]
        return state.manager[prop.key].impl.get(state, dict_, passive=passive)

    def _set_state_attr_by_column(self, state, dict_, column, value):
        prop = self._columntoproperty[column]
        state.manager[prop.key].impl.set(state, dict_, value, None)

    def _get_committed_attr_by_column(self, obj, column):
        state = attributes.instance_state(obj)
        dict_ = attributes.instance_dict(obj)
        return self._get_committed_state_attr_by_column(state, dict_, column)

    def _get_committed_state_attr_by_column(self, state, dict_, 
                        column, passive=attributes.PASSIVE_OFF):

        prop = self._columntoproperty[column]
        return state.manager[prop.key].impl.\
                    get_committed_value(state, dict_, passive=passive)

    def _load_scalar_attributes(self, state, attribute_names):
        """initiate a column-based attribute refresh operation."""

        #assert mapper is _state_mapper(state)
        session = sessionlib._state_session(state)
        if not session:
            raise orm_exc.DetachedInstanceError(
                        "Instance %s is not bound to a Session; "
                        "attribute refresh operation cannot proceed" %
                        (state_str(state)))

        has_key = bool(state.key)

        result = False

        if self.inherits and not self.concrete:
            statement = self._optimized_get_statement(state, attribute_names)
            if statement is not None:
                result = session.query(self).from_statement(statement).\
                                        _load_on_ident(None, 
                                            only_load_props=attribute_names, 
                                            refresh_state=state)

        if result is False:
            if has_key:
                identity_key = state.key
            else:
                # this codepath is rare - only valid when inside a flush, and the
                # object is becoming persistent but hasn't yet been assigned an identity_key.
                # check here to ensure we have the attrs we need.
                pk_attrs = [self._columntoproperty[col].key
                            for col in self.primary_key]
                if state.expired_attributes.intersection(pk_attrs):
                    raise sa_exc.InvalidRequestError("Instance %s cannot be refreshed - it's not "
                                                    " persistent and does not "
                                                    "contain a full primary key." % state_str(state))
                identity_key = self._identity_key_from_state(state)

            if (_none_set.issubset(identity_key) and \
                    not self.allow_partial_pks) or \
                    _none_set.issuperset(identity_key):
                util.warn("Instance %s to be refreshed doesn't "
                            "contain a full primary key - can't be refreshed "
                            "(and shouldn't be expired, either)." 
                            % state_str(state))
                return

            result = session.query(self)._load_on_ident(
                                                identity_key, 
                                                refresh_state=state, 
                                                only_load_props=attribute_names)

        # if instance is pending, a refresh operation 
        # may not complete (even if PK attributes are assigned)
        if has_key and result is None:
            raise orm_exc.ObjectDeletedError(state)

    def _optimized_get_statement(self, state, attribute_names):
        """assemble a WHERE clause which retrieves a given state by primary
        key, using a minimized set of tables.

        Applies to a joined-table inheritance mapper where the 
        requested attribute names are only present on joined tables,
        not the base table.  The WHERE clause attempts to include 
        only those tables to minimize joins.

        """
        props = self._props

        tables = set(chain(
                        *[sqlutil.find_tables(c, check_columns=True) 
                        for key in attribute_names
                        for c in props[key].columns]
                    ))

        if self.base_mapper.local_table in tables:
            return None

        class ColumnsNotAvailable(Exception):
            pass

        def visit_binary(binary):
            leftcol = binary.left
            rightcol = binary.right
            if leftcol is None or rightcol is None:
                return

            if leftcol.table not in tables:
                leftval = self._get_committed_state_attr_by_column(
                                    state, state.dict, 
                                    leftcol, 
                                    passive=attributes.PASSIVE_NO_INITIALIZE)
                if leftval is attributes.PASSIVE_NO_RESULT or leftval is None:
                    raise ColumnsNotAvailable()
                binary.left = sql.bindparam(None, leftval,
                                            type_=binary.right.type)
            elif rightcol.table not in tables:
                rightval = self._get_committed_state_attr_by_column(
                                    state, state.dict, 
                                    rightcol, 
                                    passive=attributes.PASSIVE_NO_INITIALIZE)
                if rightval is attributes.PASSIVE_NO_RESULT or rightval is None:
                    raise ColumnsNotAvailable()
                binary.right = sql.bindparam(None, rightval,
                                            type_=binary.right.type)

        allconds = []

        try:
            start = False
            for mapper in reversed(list(self.iterate_to_root())):
                if mapper.local_table in tables:
                    start = True
                if start and not mapper.single:
                    allconds.append(visitors.cloned_traverse(
                                                mapper.inherit_condition, 
                                                {}, 
                                                {'binary':visit_binary}
                                        )
                                    )
        except ColumnsNotAvailable:
            return None

        cond = sql.and_(*allconds)

        cols = []
        for key in attribute_names:
            cols.extend(props[key].columns)
        return sql.select(cols, cond, use_labels=True)

    def cascade_iterator(self, type_, state, halt_on=None):
        """Iterate each element and its mapper in an object graph,
        for all relationships that meet the given cascade rule.

        :param type_:
          The name of the cascade rule (i.e. save-update, delete,
          etc.)

        :param state:
          The lead InstanceState.  child items will be processed per
          the relationships defined for this object's mapper.

        the return value are object instances; this provides a strong
        reference so that they don't fall out of scope immediately.

        """
        visited_states = set()
        prp, mpp = object(), object()

        visitables = deque([(deque(self._props.values()), prp, 
                                state, state.dict)])

        while visitables:
            iterator, item_type, parent_state, parent_dict = visitables[-1]
            if not iterator:
                visitables.pop()
                continue

            if item_type is prp:
                prop = iterator.popleft()
                if type_ not in prop.cascade:
                    continue
                queue = deque(prop.cascade_iterator(type_, parent_state, 
                            parent_dict, visited_states, halt_on))
                if queue:
                    visitables.append((queue,mpp, None, None))
            elif item_type is mpp:
                instance, instance_mapper, corresponding_state, \
                                corresponding_dict = iterator.popleft()
                yield instance, instance_mapper, \
                        corresponding_state, corresponding_dict
                visitables.append((deque(instance_mapper._props.values()), 
                                        prp, corresponding_state, 
                                        corresponding_dict))

    @_memoized_configured_property
    def _compiled_cache(self):
        return util.LRUCache(self._compiled_cache_size)

    @_memoized_configured_property
    def _sorted_tables(self):
        table_to_mapper = {}
        for mapper in self.base_mapper.self_and_descendants:
            for t in mapper.tables:
                table_to_mapper[t] = mapper

        sorted_ = sqlutil.sort_tables(table_to_mapper.iterkeys())
        ret = util.OrderedDict()
        for t in sorted_:
            ret[t] = table_to_mapper[t]
        return ret

    def _per_mapper_flush_actions(self, uow):
        saves = unitofwork.SaveUpdateAll(uow, self.base_mapper)
        deletes = unitofwork.DeleteAll(uow, self.base_mapper)
        uow.dependencies.add((saves, deletes))

        for dep in self._dependency_processors:
            dep.per_property_preprocessors(uow)

        for prop in self._props.values():
            prop.per_property_preprocessors(uow)

    def _per_state_flush_actions(self, uow, states, isdelete):

        base_mapper = self.base_mapper
        save_all = unitofwork.SaveUpdateAll(uow, base_mapper)
        delete_all = unitofwork.DeleteAll(uow, base_mapper)
        for state in states:
            # keep saves before deletes -
            # this ensures 'row switch' operations work
            if isdelete:
                action = unitofwork.DeleteState(uow, state, base_mapper)
                uow.dependencies.add((save_all, action))
            else:
                action = unitofwork.SaveUpdateState(uow, state, base_mapper)
                uow.dependencies.add((action, delete_all))

            yield action

    def _memo(self, key, callable_):
        if key in self._memoized_values:
            return self._memoized_values[key]
        else:
            self._memoized_values[key] = value = callable_()
            return value

    def _post_update(self, states, uowtransaction, post_update_cols):
        """Issue UPDATE statements on behalf of a relationship() which
        specifies post_update.

        """
        cached_connections = util.PopulateDict(
            lambda conn:conn.execution_options(
            compiled_cache=self._compiled_cache
        ))

        # if session has a connection callable,
        # organize individual states with the connection 
        # to use for update
        if uowtransaction.session.connection_callable:
            connection_callable = \
                    uowtransaction.session.connection_callable
        else:
            connection = uowtransaction.transaction.connection(self)
            connection_callable = None

        tups = []
        for state in _sort_states(states):
            if connection_callable:
                conn = connection_callable(self, state.obj())
            else:
                conn = connection

            mapper = _state_mapper(state)

            tups.append((state, state.dict, mapper, conn))

        table_to_mapper = self._sorted_tables

        for table in table_to_mapper:
            update = []

            for state, state_dict, mapper, connection in tups:
                if table not in mapper._pks_by_table:
                    continue

                pks = mapper._pks_by_table[table]
                params = {}
                hasdata = False

                for col in mapper._cols_by_table[table]:
                    if col in pks:
                        params[col._label] = \
                                mapper._get_state_attr_by_column(
                                                state,
                                                state_dict, col)
                    elif col in post_update_cols:
                        prop = mapper._columntoproperty[col]
                        history = attributes.get_state_history(
                                    state, prop.key, 
                                    attributes.PASSIVE_NO_INITIALIZE)
                        if history.added:
                            value = history.added[0]
                            params[col.key] = value
                            hasdata = True
                if hasdata:
                    update.append((state, state_dict, params, mapper, 
                                    connection))

            if update:
                mapper = table_to_mapper[table]

                def update_stmt():
                    clause = sql.and_()

                    for col in mapper._pks_by_table[table]:
                        clause.clauses.append(col == sql.bindparam(col._label,
                                                        type_=col.type))

                    return table.update(clause)

                statement = self._memo(('post_update', table), update_stmt)

                # execute each UPDATE in the order according to the original
                # list of states to guarantee row access order, but
                # also group them into common (connection, cols) sets 
                # to support executemany().
                for key, grouper in groupby(
                    update, lambda rec: (rec[4], rec[2].keys())
                ):
                    multiparams = [params for state, state_dict, 
                                            params, mapper, conn in grouper]
                    cached_connections[connection].\
                                        execute(statement, multiparams)

    def _save_obj(self, states, uowtransaction, single=False):
        """Issue ``INSERT`` and/or ``UPDATE`` statements for a list 
        of objects.

        This is called within the context of a UOWTransaction during a
        flush operation, given a list of states to be flushed.  The
        base mapper in an inheritance hierarchy handles the inserts/
        updates for all descendant mappers.

        """

        # if batch=false, call _save_obj separately for each object
        if not single and not self.batch:
            for state in _sort_states(states):
                self._save_obj([state], 
                                uowtransaction, 
                                single=True)
            return

        # if session has a connection callable,
        # organize individual states with the connection 
        # to use for insert/update
        if uowtransaction.session.connection_callable:
            connection_callable = \
                    uowtransaction.session.connection_callable
        else:
            connection = uowtransaction.transaction.connection(self)
            connection_callable = None

        tups = []

        for state in _sort_states(states):
            if connection_callable:
                conn = connection_callable(self, state.obj())
            else:
                conn = connection

            has_identity = bool(state.key)
            mapper = _state_mapper(state)
            instance_key = state.key or mapper._identity_key_from_state(state)

            row_switch = None

            # call before_XXX extensions
            if not has_identity:
                mapper.dispatch.before_insert(mapper, conn, state)
            else:
                mapper.dispatch.before_update(mapper, conn, state)

            # detect if we have a "pending" instance (i.e. has 
            # no instance_key attached to it), and another instance 
            # with the same identity key already exists as persistent. 
            # convert to an UPDATE if so.
            if not has_identity and \
                instance_key in uowtransaction.session.identity_map:
                instance = \
                    uowtransaction.session.identity_map[instance_key]
                existing = attributes.instance_state(instance)
                if not uowtransaction.is_deleted(existing):
                    raise orm_exc.FlushError(
                        "New instance %s with identity key %s conflicts "
                        "with persistent instance %s" % 
                        (state_str(state), instance_key,
                         state_str(existing)))

                self._log_debug(
                    "detected row switch for identity %s.  "
                    "will update %s, remove %s from "
                    "transaction", instance_key, 
                    state_str(state), state_str(existing))

                # remove the "delete" flag from the existing element
                uowtransaction.remove_state_actions(existing)
                row_switch = existing

            tups.append(
                (state, state.dict, mapper, conn, 
                has_identity, instance_key, row_switch)
            )

        # dictionary of connection->connection_with_cache_options.
        cached_connections = util.PopulateDict(
            lambda conn:conn.execution_options(
            compiled_cache=self._compiled_cache
        ))

        table_to_mapper = self._sorted_tables

        for table in table_to_mapper:
            insert = []
            update = []

            for state, state_dict, mapper, connection, has_identity, \
                            instance_key, row_switch in tups:
                if table not in mapper._pks_by_table:
                    continue

                pks = mapper._pks_by_table[table]

                isinsert = not has_identity and not row_switch

                params = {}
                value_params = {}

                if isinsert:
                    has_all_pks = True
                    for col in mapper._cols_by_table[table]:
                        if col is mapper.version_id_col:
                            params[col.key] = \
                              mapper.version_id_generator(None)
                        else:
                            # pull straight from the dict for 
                            # pending objects
                            prop = mapper._columntoproperty[col]
                            value = state_dict.get(prop.key, None)

                            if value is None:
                                if col in pks:
                                    has_all_pks = False
                                elif col.default is None and \
                                     col.server_default is None:
                                    params[col.key] = value

                            elif isinstance(value, sql.ClauseElement):
                                value_params[col] = value
                            else:
                                params[col.key] = value

                    insert.append((state, state_dict, params, mapper, 
                                    connection, value_params, has_all_pks))
                else:
                    hasdata = False
                    for col in mapper._cols_by_table[table]:
                        if col is mapper.version_id_col:
                            params[col._label] = \
                                mapper._get_committed_state_attr_by_column(
                                            row_switch or state, 
                                            row_switch and row_switch.dict 
                                                        or state_dict,
                                            col)

                            prop = mapper._columntoproperty[col]
                            history = attributes.get_state_history(
                                state, prop.key, 
                                attributes.PASSIVE_NO_INITIALIZE
                            )
                            if history.added:
                                params[col.key] = history.added[0]
                                hasdata = True
                            else:
                                params[col.key] = \
                                            mapper.version_id_generator(
                                                params[col._label])

                                # HACK: check for history, in case the 
                                # history is only
                                # in a different table than the one 
                                # where the version_id_col is.
                                for prop in mapper._columntoproperty.\
                                                        itervalues():
                                    history = attributes.get_state_history(
                                            state, prop.key, 
                                            attributes.PASSIVE_NO_INITIALIZE)
                                    if history.added:
                                        hasdata = True
                        else:
                            prop = mapper._columntoproperty[col]
                            history = attributes.get_state_history(
                                            state, prop.key, 
                                            attributes.PASSIVE_NO_INITIALIZE)
                            if history.added:
                                if isinstance(history.added[0],
                                                sql.ClauseElement):
                                    value_params[col] = history.added[0]
                                else:
                                    value = history.added[0]
                                    params[col.key] = value

                                if col in pks:
                                    if history.deleted and \
                                        not row_switch:
                                        # if passive_updates and sync detected
                                        # this was a  pk->pk sync, use the new
                                        # value to locate the row, since the
                                        # DB would already have set this
                                        if ("pk_cascaded", state, col) in \
                                                        uowtransaction.\
                                                        attributes:
                                            value = history.added[0]
                                            params[col._label] = value
                                        else:
                                            # use the old value to 
                                            # locate the row
                                            value = history.deleted[0]
                                            params[col._label] = value
                                        hasdata = True
                                    else:
                                        # row switch logic can reach us here
                                        # remove the pk from the update params
                                        # so the update doesn't
                                        # attempt to include the pk in the
                                        # update statement
                                        del params[col.key]
                                        value = history.added[0]
                                        params[col._label] = value
                                    if value is None and hasdata:
                                        raise sa_exc.FlushError(
                                                "Can't update table "
                                                "using NULL for primary key "
                                                "value")
                                else:
                                    hasdata = True
                            elif col in pks:
                                value = state.manager[prop.key].\
                                            impl.get(state, state_dict)
                                if value is None:
                                    raise sa_exc.FlushError(
                                                "Can't update table "
                                                "using NULL for primary "
                                                "key value")
                                params[col._label] = value
                    if hasdata:
                        update.append((state, state_dict, params, mapper, 
                                        connection, value_params))

            if update:
                mapper = table_to_mapper[table]

                needs_version_id = mapper.version_id_col is not None and \
                            table.c.contains_column(mapper.version_id_col)

                def update_stmt():
                    clause = sql.and_()

                    for col in mapper._pks_by_table[table]:
                        clause.clauses.append(col == sql.bindparam(col._label,
                                                        type_=col.type))

                    if needs_version_id:
                        clause.clauses.append(mapper.version_id_col ==\
                                sql.bindparam(mapper.version_id_col._label,
                                                type_=col.type))

                    return table.update(clause)

                statement = self._memo(('update', table), update_stmt)

                rows = 0
                for state, state_dict, params, mapper, \
                            connection, value_params in update:

                    if value_params:
                        c = connection.execute(
                                            statement.values(value_params),
                                            params)
                    else:
                        c = cached_connections[connection].\
                                            execute(statement, params)

                    mapper._postfetch(
                            uowtransaction, 
                            table, 
                            state, 
                            state_dict, 
                            c.context.prefetch_cols, 
                            c.context.postfetch_cols,
                            c.context.compiled_parameters[0], 
                            value_params)
                    rows += c.rowcount

                if connection.dialect.supports_sane_rowcount:
                    if rows != len(update):
                        raise orm_exc.StaleDataError(
                                "UPDATE statement on table '%s' expected to update %d row(s); "
                                "%d were matched." %
                                (table.description, len(update), rows))

                elif needs_version_id:
                    util.warn("Dialect %s does not support updated rowcount "
                            "- versioning cannot be verified." % 
                            c.dialect.dialect_description,
                            stacklevel=12)

            if insert:
                statement = self._memo(('insert', table), table.insert)

                for (connection, pkeys, hasvalue, has_all_pks), \
                    records in groupby(insert, 
                                        lambda rec: (rec[4], 
                                                rec[2].keys(), 
                                                bool(rec[5]), 
                                                rec[6])
                ):
                    if has_all_pks and not hasvalue:
                        records = list(records)
                        multiparams = [rec[2] for rec in records]
                        c = cached_connections[connection].\
                                            execute(statement, multiparams)

                        for (state, state_dict, params, mapper, 
                                conn, value_params, has_all_pks), \
                                last_inserted_params in \
                                zip(records, c.context.compiled_parameters):
                            mapper._postfetch(
                                    uowtransaction, 
                                    table,
                                    state, 
                                    state_dict,
                                    c.context.prefetch_cols,
                                    c.context.postfetch_cols,
                                    last_inserted_params, 
                                    value_params)

                    else:
                        for state, state_dict, params, mapper, \
                                    connection, value_params, \
                                    has_all_pks in records:

                            if value_params:
                                result = connection.execute(
                                            statement.values(value_params),
                                            params)
                            else:
                                result = cached_connections[connection].\
                                                    execute(statement, params)

                            primary_key = result.context.inserted_primary_key

                            if primary_key is not None:
                                # set primary key attributes
                                for pk, col in zip(primary_key, 
                                                mapper._pks_by_table[table]):
                                    prop = mapper._columntoproperty[col]
                                    if state_dict.get(prop.key) is None:
                                        # TODO: would rather say:
                                        #state_dict[prop.key] = pk
                                        mapper._set_state_attr_by_column(
                                                    state, 
                                                    state_dict, 
                                                    col, pk)

                            mapper._postfetch(
                                    uowtransaction, 
                                    table, 
                                    state, 
                                    state_dict,
                                    result.context.prefetch_cols, 
                                    result.context.postfetch_cols,
                                    result.context.compiled_parameters[0], 
                                    value_params)


        for state, state_dict, mapper, connection, has_identity, \
                        instance_key, row_switch in tups:

            if mapper._readonly_props:
                readonly = state.unmodified_intersection(
                    [p.key for p in mapper._readonly_props 
                        if p.expire_on_flush or p.key not in state.dict]
                )
                if readonly:
                    state.expire_attributes(state.dict, readonly)

            # if eager_defaults option is enabled,
            # refresh whatever has been expired.
            if self.eager_defaults and state.unloaded:
                state.key = self._identity_key_from_state(state)
                uowtransaction.session.query(self)._load_on_ident(
                    state.key, refresh_state=state,
                    only_load_props=state.unloaded)

            # call after_XXX extensions
            if not has_identity:
                mapper.dispatch.after_insert(mapper, connection, state)
            else:
                mapper.dispatch.after_update(mapper, connection, state)

    def _postfetch(self, uowtransaction, table, 
                    state, dict_, prefetch_cols, postfetch_cols,
                                params, value_params):
        """During a flush, expire attributes in need of newly 
        persisted database state."""

        if self.version_id_col is not None:
            prefetch_cols = list(prefetch_cols) + [self.version_id_col]

        for c in prefetch_cols:
            if c.key in params and c in self._columntoproperty:
                self._set_state_attr_by_column(state, dict_, c, params[c.key])

        if postfetch_cols:
            state.expire_attributes(state.dict, 
                                [self._columntoproperty[c].key 
                                for c in postfetch_cols if c in 
                                self._columntoproperty]
                            )

        # synchronize newly inserted ids from one table to the next
        # TODO: this still goes a little too often.  would be nice to
        # have definitive list of "columns that changed" here
        for m, equated_pairs in self._table_to_equated[table]:
            sync.populate(state, m, state, m, 
                                            equated_pairs, 
                                            uowtransaction,
                                            self.passive_updates)

    @util.memoized_property
    def _table_to_equated(self):
        """memoized map of tables to collections of columns to be 
        synchronized upwards to the base mapper."""

        result = util.defaultdict(list)

        for table in self._sorted_tables:
            cols = set(table.c)
            for m in self.iterate_to_root():
                if m._inherits_equated_pairs and \
                    cols.intersection(
                        [l for l, r in m._inherits_equated_pairs]):
                    result[table].append((m, m._inherits_equated_pairs))

        return result

    def _delete_obj(self, states, uowtransaction):
        """Issue ``DELETE`` statements for a list of objects.

        This is called within the context of a UOWTransaction during a
        flush operation.

        """
        if uowtransaction.session.connection_callable:
            connection_callable = \
                    uowtransaction.session.connection_callable
        else:
            connection = uowtransaction.transaction.connection(self)
            connection_callable = None

        tups = []
        cached_connections = util.PopulateDict(
            lambda conn:conn.execution_options(
            compiled_cache=self._compiled_cache
        ))

        for state in _sort_states(states):
            mapper = _state_mapper(state)

            if connection_callable:
                conn = connection_callable(self, state.obj())
            else:
                conn = connection

            mapper.dispatch.before_delete(mapper, conn, state)

            tups.append((state, 
                    state.dict,
                    _state_mapper(state), 
                    bool(state.key),
                    conn))

        table_to_mapper = self._sorted_tables

        for table in reversed(table_to_mapper.keys()):
            delete = util.defaultdict(list)
            for state, state_dict, mapper, has_identity, connection in tups:
                if not has_identity or table not in mapper._pks_by_table:
                    continue

                params = {}
                delete[connection].append(params)
                for col in mapper._pks_by_table[table]:
                    params[col.key] = \
                            value = \
                            mapper._get_state_attr_by_column(
                                            state, state_dict, col)
                    if value is None:
                        raise sa_exc.FlushError(
                                    "Can't delete from table "
                                    "using NULL for primary "
                                    "key value")

                if mapper.version_id_col is not None and \
                            table.c.contains_column(mapper.version_id_col):
                    params[mapper.version_id_col.key] = \
                                mapper._get_committed_state_attr_by_column(
                                        state, state_dict,
                                        mapper.version_id_col)

            mapper = table_to_mapper[table]
            need_version_id = mapper.version_id_col is not None and \
                table.c.contains_column(mapper.version_id_col)

            def delete_stmt():
                clause = sql.and_()
                for col in mapper._pks_by_table[table]:
                    clause.clauses.append(
                            col == sql.bindparam(col.key, type_=col.type))

                if need_version_id:
                    clause.clauses.append(
                        mapper.version_id_col == 
                        sql.bindparam(
                                mapper.version_id_col.key, 
                                type_=mapper.version_id_col.type
                        )
                    )

                return table.delete(clause)

            for connection, del_objects in delete.iteritems():
                statement = self._memo(('delete', table), delete_stmt)
                rows = -1

                connection = cached_connections[connection]

                if need_version_id and \
                        not connection.dialect.supports_sane_multi_rowcount:
                    # TODO: need test coverage for this [ticket:1761]
                    if connection.dialect.supports_sane_rowcount:
                        rows = 0
                        # execute deletes individually so that versioned
                        # rows can be verified
                        for params in del_objects:
                            c = connection.execute(statement, params)
                            rows += c.rowcount
                    else:
                        util.warn(
                            "Dialect %s does not support deleted rowcount "
                            "- versioning cannot be verified." % 
                            connection.dialect.dialect_description,
                            stacklevel=12)
                        connection.execute(statement, del_objects)
                else:
                    c = connection.execute(statement, del_objects)
                    if connection.dialect.supports_sane_multi_rowcount:
                        rows = c.rowcount

                if rows != -1 and rows != len(del_objects):
                    raise orm_exc.StaleDataError(
                        "DELETE statement on table '%s' expected to delete %d row(s); "
                        "%d were matched." % 
                        (table.description, len(del_objects), c.rowcount)
                    )

        for state, state_dict, mapper, has_identity, connection in tups:
            mapper.dispatch.after_delete(mapper, connection, state)

    def _instance_processor(self, context, path, reduced_path, adapter, 
                                polymorphic_from=None, 
                                only_load_props=None, refresh_state=None,
                                polymorphic_discriminator=None):

        """Produce a mapper level row processor callable 
           which processes rows into mapped instances."""

        pk_cols = self.primary_key

        if polymorphic_from or refresh_state:
            polymorphic_on = None
        else:
            if polymorphic_discriminator is not None:
                polymorphic_on = polymorphic_discriminator
            else:
                polymorphic_on = self.polymorphic_on
            polymorphic_instances = util.PopulateDict(
                                        self._configure_subclass_mapper(
                                                context, path, reduced_path, adapter)
                                        )

        version_id_col = self.version_id_col

        if adapter:
            pk_cols = [adapter.columns[c] for c in pk_cols]
            if polymorphic_on is not None:
                polymorphic_on = adapter.columns[polymorphic_on]
            if version_id_col is not None:
                version_id_col = adapter.columns[version_id_col]

        identity_class = self._identity_class

        new_populators = []
        existing_populators = []
        eager_populators = []
        load_path = context.query._current_path + path

        def populate_state(state, dict_, row, isnew, only_load_props):
            if isnew:
                if context.propagate_options:
                    state.load_options = context.propagate_options
                if state.load_options:
                    state.load_path = load_path

            if not new_populators:
                self._populators(context, path, reduced_path, row, adapter,
                                new_populators,
                                existing_populators,
                                eager_populators
                )

            if isnew:
                populators = new_populators
            else:
                populators = existing_populators

            if only_load_props is None:
                for key, populator in populators:
                    populator(state, dict_, row)
            elif only_load_props:
                for key, populator in populators:
                    if key in only_load_props:
                        populator(state, dict_, row)

        session_identity_map = context.session.identity_map

        listeners = self.dispatch

        translate_row = listeners.translate_row or None
        create_instance = listeners.create_instance or None
        populate_instance = listeners.populate_instance or None
        append_result = listeners.append_result or None
        populate_existing = context.populate_existing or self.always_refresh
        invoke_all_eagers = context.invoke_all_eagers

        if self.allow_partial_pks:
            is_not_primary_key = _none_set.issuperset
        else:
            is_not_primary_key = _none_set.issubset

        def _instance(row, result):
            if not new_populators and invoke_all_eagers:
                self._populators(context, path, reduced_path, row, adapter,
                                new_populators,
                                existing_populators,
                                eager_populators
                )

            if translate_row:
                for fn in translate_row:
                    ret = fn(self, context, row)
                    if ret is not EXT_CONTINUE:
                        row = ret
                        break

            if polymorphic_on is not None:
                discriminator = row[polymorphic_on]
                if discriminator is not None:
                    _instance = polymorphic_instances[discriminator]
                    if _instance:
                        return _instance(row, result)

            # determine identity key
            if refresh_state:
                identitykey = refresh_state.key
                if identitykey is None:
                    # super-rare condition; a refresh is being called
                    # on a non-instance-key instance; this is meant to only
                    # occur within a flush()
                    identitykey = self._identity_key_from_state(refresh_state)
            else:
                identitykey = (
                                identity_class, 
                                tuple([row[column] for column in pk_cols])
                            )

            instance = session_identity_map.get(identitykey)
            if instance is not None:
                state = attributes.instance_state(instance)
                dict_ = attributes.instance_dict(instance)

                isnew = state.runid != context.runid
                currentload = not isnew
                loaded_instance = False

                if not currentload and \
                        version_id_col is not None and \
                        context.version_check and \
                        self._get_state_attr_by_column(
                                state, 
                                dict_, 
                                self.version_id_col) != \
                                        row[version_id_col]:

                    raise orm_exc.StaleDataError(
                            "Instance '%s' has version id '%s' which "
                            "does not match database-loaded version id '%s'." 
                            % (state_str(state), 
                                self._get_state_attr_by_column(
                                            state, dict_,
                                            self.version_id_col),
                                    row[version_id_col]))
            elif refresh_state:
                # out of band refresh_state detected (i.e. its not in the
                # session.identity_map) honor it anyway.  this can happen 
                # if a _get() occurs within save_obj(), such as
                # when eager_defaults is True.
                state = refresh_state
                instance = state.obj()
                dict_ = attributes.instance_dict(instance)
                isnew = state.runid != context.runid
                currentload = True
                loaded_instance = False
            else:
                # check for non-NULL values in the primary key columns,
                # else no entity is returned for the row
                if is_not_primary_key(identitykey[1]):
                    return None

                isnew = True
                currentload = True
                loaded_instance = True

                if create_instance:
                    for fn in create_instance:
                        instance = fn(self, context, 
                                            row, self.class_)
                        if instance is not EXT_CONTINUE:
                            manager = attributes.manager_of_class(
                                                    instance.__class__)
                            # TODO: if manager is None, raise a friendly error
                            # about returning instances of unmapped types
                            manager.setup_instance(instance)
                            break
                    else:
                        instance = self.class_manager.new_instance()
                else:
                    instance = self.class_manager.new_instance()

                dict_ = attributes.instance_dict(instance)
                state = attributes.instance_state(instance)
                state.key = identitykey

                # attach instance to session.
                state.session_id = context.session.hash_key
                session_identity_map.add(state)

            if currentload or populate_existing:
                # state is being fully loaded, so populate.
                # add to the "context.progress" collection.
                if isnew:
                    state.runid = context.runid
                    context.progress[state] = dict_

                if populate_instance:
                    for fn in populate_instance:
                        ret = fn(self, context, row, state, 
                            only_load_props=only_load_props, 
                            instancekey=identitykey, isnew=isnew)
                        if ret is not EXT_CONTINUE:
                            break
                    else:
                        populate_state(state, dict_, row, isnew, only_load_props)
                else:
                    populate_state(state, dict_, row, isnew, only_load_props)

                if loaded_instance:
                    state.manager.dispatch.load(state, context)
                elif isnew:
                    state.manager.dispatch.refresh(state, context, only_load_props)

            elif state in context.partials or state.unloaded or eager_populators:
                # state is having a partial set of its attributes
                # refreshed.  Populate those attributes,
                # and add to the "context.partials" collection.
                if state in context.partials:
                    isnew = False
                    (d_, attrs) = context.partials[state]
                else:
                    isnew = True
                    attrs = state.unloaded
                    context.partials[state] = (dict_, attrs)

                if populate_instance:
                    for fn in populate_instance:
                        ret = fn(self, context, row, state, 
                            only_load_props=attrs, 
                            instancekey=identitykey, isnew=isnew)
                        if ret is not EXT_CONTINUE:
                            break
                    else:
                        populate_state(state, dict_, row, isnew, attrs)
                else:
                    populate_state(state, dict_, row, isnew, attrs)

                for key, pop in eager_populators:
                    if key not in state.unloaded:
                        pop(state, dict_, row)

                if isnew:
                    state.manager.dispatch.refresh(state, context, attrs)


            if result is not None:
                if append_result:
                    for fn in append_result:
                        if fn(self, context, row, state, 
                                    result, instancekey=identitykey,
                                    isnew=isnew) is not EXT_CONTINUE:
                            break
                    else:
                        result.append(instance)
                else:
                    result.append(instance)

            return instance
        return _instance

    def _populators(self, context, path, reduced_path, row, adapter,
            new_populators, existing_populators, eager_populators):
        """Produce a collection of attribute level row processor callables."""

        delayed_populators = []
        pops = (new_populators, existing_populators, delayed_populators, eager_populators)
        for prop in self._props.itervalues():
            for i, pop in enumerate(prop.create_row_processor(
                                        context, path, 
                                        reduced_path,
                                        self, row, adapter)):
                if pop is not None:
                    pops[i].append((prop.key, pop))

        if delayed_populators:
            new_populators.extend(delayed_populators)

    def _configure_subclass_mapper(self, context, path, reduced_path, adapter):
        """Produce a mapper level row processor callable factory for mappers
        inheriting this one."""

        def configure_subclass_mapper(discriminator):
            try:
                mapper = self.polymorphic_map[discriminator]
            except KeyError:
                raise AssertionError(
                        "No such polymorphic_identity %r is defined" %
                        discriminator)
            if mapper is self:
                return None

            # replace the tip of the path info with the subclass mapper 
            # being used. that way accurate "load_path" info is available 
            # for options invoked during deferred loads.
            # we lose AliasedClass path elements this way, but currently,
            # those are not needed at this stage.

            # this asserts to true
            #assert mapper.isa(_class_to_mapper(path[-1]))

            return mapper._instance_processor(context, path[0:-1] + (mapper,), 
                                                    reduced_path[0:-1] + (mapper.base_mapper,),
                                                    adapter,
                                                    polymorphic_from=self)
        return configure_subclass_mapper

log.class_logger(Mapper)

def configure_mappers():
    """Initialize the inter-mapper relationships of all mappers that
    have been constructed thus far.

    This function can be called any number of times, but in 
    most cases is handled internally.

    """

    global _new_mappers
    if not _new_mappers:
        return 

    _call_configured = None
    _COMPILE_MUTEX.acquire()
    try:
        global _already_compiling
        if _already_compiling:
            return
        _already_compiling = True
        try:

            # double-check inside mutex
            if not _new_mappers:
                return

            # initialize properties on all mappers
            # note that _mapper_registry is unordered, which 
            # may randomly conceal/reveal issues related to 
            # the order of mapper compilation
            for mapper in list(_mapper_registry):
                if getattr(mapper, '_configure_failed', False):
                    e = sa_exc.InvalidRequestError(
                            "One or more mappers failed to initialize - "
                            "can't proceed with initialization of other "
                            "mappers.  Original exception was: %s"
                            % mapper._configure_failed)
                    e._configure_failed = mapper._configure_failed
                    raise e
                if not mapper.configured:
                    try:
                        mapper._post_configure_properties()
                        mapper._expire_memoizations()
                        mapper.dispatch.mapper_configured(mapper, mapper.class_)
                        _call_configured = mapper
                    except:
                        exc = sys.exc_info()[1]
                        if not hasattr(exc, '_configure_failed'):
                            mapper._configure_failed = exc
                        raise

            _new_mappers = False
        finally:
            _already_compiling = False
    finally:
        _COMPILE_MUTEX.release()
    if _call_configured is not None:
        _call_configured.dispatch.after_configured()

def reconstructor(fn):
    """Decorate a method as the 'reconstructor' hook.

    Designates a method as the "reconstructor", an ``__init__``-like
    method that will be called by the ORM after the instance has been
    loaded from the database or otherwise reconstituted.

    The reconstructor will be invoked with no arguments.  Scalar
    (non-collection) database-mapped attributes of the instance will
    be available for use within the function.  Eagerly-loaded
    collections are generally not yet available and will usually only
    contain the first element.  ORM state changes made to objects at
    this stage will not be recorded for the next flush() operation, so
    the activity within a reconstructor should be conservative.

    """
    fn.__sa_reconstructor__ = True
    return fn

def validates(*names):
    """Decorate a method as a 'validator' for one or more named properties.

    Designates a method as a validator, a method which receives the
    name of the attribute as well as a value to be assigned, or in the
    case of a collection, the value to be added to the collection.  The function
    can then raise validation exceptions to halt the process from continuing
    (where Python's built-in ``ValueError`` and ``AssertionError`` exceptions are
    reasonable choices), or can modify or replace the value before proceeding.
    The function should otherwise return the given value.

    Note that a validator for a collection **cannot** issue a load of that
    collection within the validation routine - this usage raises
    an assertion to avoid recursion overflows.  This is a reentrant
    condition which is not supported.

    """
    def wrap(fn):
        fn.__sa_validators__ = names
        return fn
    return wrap

def _event_on_load(state, ctx):
    instrumenting_mapper = state.manager.info[_INSTRUMENTOR]
    if instrumenting_mapper._reconstructor:
        instrumenting_mapper._reconstructor(state.obj())

def _event_on_first_init(manager, cls):
    """Initial mapper compilation trigger.
    
    instrumentation calls this one when InstanceState
    is first generated, and is needed for legacy mutable
    attributes to work.
    """

    instrumenting_mapper = manager.info.get(_INSTRUMENTOR)
    if instrumenting_mapper:
        if _new_mappers:
            configure_mappers()

def _event_on_init(state, args, kwargs):
    """Run init_instance hooks.
    
    This also includes mapper compilation, normally not needed
    here but helps with some piecemeal configuration
    scenarios (such as in the ORM tutorial).
    
    """

    instrumenting_mapper = state.manager.info.get(_INSTRUMENTOR)
    if instrumenting_mapper:
        if _new_mappers:
            configure_mappers()
        if instrumenting_mapper._set_polymorphic_identity:
            instrumenting_mapper._set_polymorphic_identity(state)

def _event_on_resurrect(state):
    # re-populate the primary key elements
    # of the dict based on the mapping.
    instrumenting_mapper = state.manager.info.get(_INSTRUMENTOR)
    if instrumenting_mapper:
        for col, val in zip(instrumenting_mapper.primary_key, state.key[1]):
            instrumenting_mapper._set_state_attr_by_column(
                                            state, state.dict, col, val)


def _sort_states(states):
    pending = set(states)
    persistent = set(s for s in pending if s.key is not None)
    pending.difference_update(persistent)
    return sorted(pending, key=operator.attrgetter("insert_order")) + \
        sorted(persistent, key=lambda q:q.key[1])

class _ColumnMapping(util.py25_dict):
    """Error reporting helper for mapper._columntoproperty."""

    def __init__(self, mapper):
        self.mapper = mapper

    def __missing__(self, column):
        prop = self.mapper._props.get(column)
        if prop:
            raise orm_exc.UnmappedColumnError(
                "Column '%s.%s' is not available, due to "
                "conflicting property '%s':%r" % (
                    column.table.name, column.name, column.key, prop))
        raise orm_exc.UnmappedColumnError(
            "No column %s is configured on mapper %s..." %
            (column, self.mapper))
