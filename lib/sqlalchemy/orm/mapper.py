# mapper.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Logic to map Python classes to and from selectables.

Defines the [sqlalchemy.orm.mapper#Mapper] class, the central configurational
unit which associates a class with a database table.

This is a semi-private module; the main configurational API of the ORM is
available in [sqlalchemy.orm#].

"""

import weakref
from itertools import chain

from sqlalchemy import sql, util, log
import sqlalchemy.exceptions as sa_exc
from sqlalchemy.sql import expression, visitors, operators
import sqlalchemy.sql.util as sqlutil
from sqlalchemy.orm import attributes
from sqlalchemy.orm import exc
from sqlalchemy.orm import sync
from sqlalchemy.orm.identity import IdentityManagedState
from sqlalchemy.orm.interfaces import MapperProperty, EXT_CONTINUE, \
     PropComparator
from sqlalchemy.orm.util import \
     ExtensionCarrier, _INSTRUMENTOR, _class_to_mapper, _is_mapped_class, \
     _state_has_identity, _state_mapper, class_mapper, has_identity, \
     has_mapper, instance_str, object_mapper, state_str


__all__ = (
    'Mapper',
    '_mapper_registry',
    'class_mapper',
    'object_mapper',
    )

_mapper_registry = weakref.WeakKeyDictionary()
_new_mappers = False
_already_compiling = False

# a list of MapperExtensions that will be installed in all mappers by default
global_extensions = []

# a constant returned by _get_attr_by_column to indicate
# this mapper is not handling an attribute for a particular
# column
NO_ATTRIBUTE = util.symbol('NO_ATTRIBUTE')

# lock used to synchronize the "mapper compile" step
_COMPILE_MUTEX = util.threading.RLock()

# initialize these lazily
ColumnProperty = None
SynonymProperty = None
ComparableProperty = None
_expire_state = None
_state_session = None


class Mapper(object):
    """Define the correlation of class attributes to database table
    columns.

    Instances of this class should be constructed via the
    [sqlalchemy.orm#mapper()] function.
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
                 entity_name = None,
                 always_refresh = False,
                 version_id_col = None,
                 polymorphic_on=None,
                 _polymorphic_map=None,
                 polymorphic_identity=None,
                 polymorphic_fetch=None,
                 concrete=False,
                 select_table=None,
                 with_polymorphic=None,
                 allow_null_pks=False,
                 batch=True,
                 column_prefix=None,
                 include_properties=None,
                 exclude_properties=None,
                 eager_defaults=False):
        """Construct a new mapper.

        Mappers are normally constructed via the [sqlalchemy.orm#mapper()]
        function.  See for details.

        """

        self.class_ = class_
        self.class_manager = None
        self.entity_name = entity_name
        self.primary_key_argument = primary_key
        self.non_primary = non_primary
        
        if order_by:
            self.order_by = util.to_list(order_by)
        else:
            self.order_by = order_by
            
        self.always_refresh = always_refresh
        self.version_id_col = version_id_col
        self.concrete = concrete
        self.single = False
        self.inherits = inherits
        self.local_table = local_table
        self.inherit_condition = inherit_condition
        self.inherit_foreign_keys = inherit_foreign_keys
        self.extension = extension
        self._init_properties = properties or {}
        self.allow_null_pks = allow_null_pks
        self.delete_orphans = []
        self.batch = batch
        self.eager_defaults = eager_defaults
        self.column_prefix = column_prefix
        self.polymorphic_on = polymorphic_on
        self._dependency_processors = []
        self._clause_adapter = None
        self._requires_row_aliasing = False
        self.__inherits_equated_pairs = None
        
        if not issubclass(class_, object):
            raise sa_exc.ArgumentError("Class '%s' is not a new-style class" % class_.__name__)

        self.select_table = select_table
        if select_table:
            if with_polymorphic:
                raise sa_exc.ArgumentError("select_table can't be used with with_polymorphic (they define conflicting settings)")
            self.with_polymorphic = ('*', select_table)
        else:
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

        if isinstance(self.local_table, expression._SelectBaseMixin):
            util.warn("mapper %s creating an alias for the given selectable - use Class attributes for queries." % self)
            self.local_table = self.local_table.alias()

        if self.with_polymorphic and isinstance(self.with_polymorphic[1], expression._SelectBaseMixin):
            self.with_polymorphic[1] = self.with_polymorphic[1].alias()

        # our 'polymorphic identity', a string name that when located in a result set row
        # indicates this Mapper should be used to construct the object instance for that row.
        self.polymorphic_identity = polymorphic_identity

        if polymorphic_fetch:
            util.warn_deprecated('polymorphic_fetch option is deprecated.  Unloaded columns load as deferred in all cases; loading can be controlled using the "with_polymorphic" option.')

        # a dictionary of 'polymorphic identity' names, associating those names with
        # Mappers that will be used to construct object instances upon a select operation.
        if _polymorphic_map is None:
            self.polymorphic_map = {}
        else:
            self.polymorphic_map = _polymorphic_map

        self.columns = self.c = util.OrderedProperties()

        self.include_properties = include_properties
        self.exclude_properties = exclude_properties

        # a set of all mappers which inherit from this one.
        self._inheriting_mappers = util.Set()

        self.compiled = False

        self.__should_log_info = log.is_info_enabled(self.logger)
        self.__should_log_debug = log.is_debug_enabled(self.logger)

        self.__compile_inheritance()
        self.__compile_extensions()
        self.__compile_class()
        self.__compile_properties()
        self.__compile_pks()
        global _new_mappers
        _new_mappers = True
        self.__log("constructed")

    def __log(self, msg):
        if self.__should_log_info:
            self.logger.info("(" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.description or str(self.local_table)) + (self.non_primary and "|non-primary" or "") + ") " + msg)

    def __log_debug(self, msg):
        if self.__should_log_debug:
            self.logger.debug("(" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.description or str(self.local_table)) + (self.non_primary and "|non-primary" or "") + ") " + msg)

    def _is_orphan(self, state):
        o = False
        for mapper in self.iterate_to_root():
            for (key, cls) in mapper.delete_orphans:
                if attributes.manager_of_class(cls).has_parent(
                    state, key, optimistic=_state_has_identity(state)):
                    return False
            o = o or bool(mapper.delete_orphans)
        return o

    def get_property(self, key, resolve_synonyms=False, raiseerr=True):
        """return a MapperProperty associated with the given key."""

        self.compile()
        return self._get_property(key, resolve_synonyms=resolve_synonyms, raiseerr=raiseerr)

    def _get_property(self, key, resolve_synonyms=False, raiseerr=True):
        prop = self.__props.get(key, None)
        if resolve_synonyms:
            while isinstance(prop, SynonymProperty):
                prop = self.__props.get(prop.name, None)
        if prop is None and raiseerr:
            raise sa_exc.InvalidRequestError("Mapper '%s' has no property '%s'" % (str(self), key))
        return prop

    def iterate_properties(self):
        """return an iterator of all MapperProperty objects."""
        self.compile()
        return self.__props.itervalues()
    iterate_properties = property(iterate_properties)

    def __mappers_from_spec(self, spec, selectable):
        """given a with_polymorphic() argument, return the set of mappers it represents.

        Trims the list of mappers to just those represented within the given selectable, if present.
        This helps some more legacy-ish mappings.

        """
        if spec == '*':
            mappers = list(self.polymorphic_iterator())
        elif spec:
            mappers = [_class_to_mapper(m) for m in util.to_list(spec)]
            for m in mappers:
                if not m.isa(self):
                    raise sa_exc.InvalidRequestError("%r does not inherit from %r"  % (m, self))
        else:
            mappers = []

        if selectable:
            tables = util.Set(sqlutil.find_tables(selectable, include_aliases=True))
            mappers = [m for m in mappers if m.local_table in tables]

        return mappers

    def __selectable_from_mappers(self, mappers):
        """given a list of mappers (assumed to be within this mapper's inheritance hierarchy),
        construct an outerjoin amongst those mapper's mapped tables.

        """
        from_obj = self.mapped_table
        for m in mappers:
            if m is self:
                continue
            if m.concrete:
                raise sa_exc.InvalidRequestError("'with_polymorphic()' requires 'selectable' argument when concrete-inheriting mappers are used.")
            elif not m.single:
                from_obj = from_obj.outerjoin(m.local_table, m.inherit_condition)

        return from_obj

    def _with_polymorphic_mappers(self):
        if not self.with_polymorphic:
            return [self]
        return self.__mappers_from_spec(*self.with_polymorphic)
    _with_polymorphic_mappers = property(util.cache_decorator(_with_polymorphic_mappers))

    def _with_polymorphic_selectable(self):
        if not self.with_polymorphic:
            return self.mapped_table

        spec, selectable = self.with_polymorphic
        if selectable:
            return selectable
        else:
            return self.__selectable_from_mappers(self.__mappers_from_spec(spec, selectable))
    _with_polymorphic_selectable = property(util.cache_decorator(_with_polymorphic_selectable))

    def _with_polymorphic_args(self, spec=None, selectable=False):
        if self.with_polymorphic:
            if not spec:
                spec = self.with_polymorphic[0]
            if selectable is False:
                selectable = self.with_polymorphic[1]

        mappers = self.__mappers_from_spec(spec, selectable)
        if selectable:
            return mappers, selectable
        else:
            return mappers, self.__selectable_from_mappers(mappers)

    def _iterate_polymorphic_properties(self, mappers=None):
        if mappers is None:
            mappers = self._with_polymorphic_mappers
        return iter(util.OrderedSet(
            chain(*[list(mapper.iterate_properties) for mapper in [self] + mappers])
        ))

    def properties(self):
        raise NotImplementedError("Public collection of MapperProperty objects is provided by the get_property() and iterate_properties accessors.")
    properties = property(properties)

    def dispose(self):
        # Disable any attribute-based compilation.
        self.compiled = True

        manager = self.class_manager
        mappers = manager.mappers

        if not self.non_primary and self.entity_name in mappers:
            del mappers[self.entity_name]
        if not mappers and manager.info.get(_INSTRUMENTOR, False):
            manager.events.remove_listener('on_init', _event_on_init)
            manager.events.remove_listener('on_init_failure',
                                           _event_on_init_failure)
            manager.uninstall_member('__init__')
            del manager.info[_INSTRUMENTOR]
            attributes.unregister_class(self.class_)

    def compile(self):
        """Compile this mapper and all other non-compiled mappers.

        This method checks the local compiled status as well as for
        any new mappers that have been defined, and is safe to call
        repeatedly.
        """
        global _new_mappers
        if self.compiled and not _new_mappers:
            return self
            
        _COMPILE_MUTEX.acquire()
        global _already_compiling
        if _already_compiling:
            # re-entrance to compile() occurs rarely, when a class-mapped construct is 
            # used within a ForeignKey, something that is possible 
            # when using the declarative layer
            return
        _already_compiling = True
        try:

            # double-check inside mutex
            if self.compiled and not _new_mappers:
                return self

            # initialize properties on all mappers
            for mapper in list(_mapper_registry):
                if not mapper.compiled:
                    mapper.__initialize_properties()
                    
            _new_mappers = False
            return self
        finally:
            _already_compiling = False
            _COMPILE_MUTEX.release()

    def __initialize_properties(self):
        """Call the ``init()`` method on all ``MapperProperties``
        attached to this mapper.

        This is a deferred configuration step which is intended
        to execute once all mappers have been constructed.
        """

        self.__log("__initialize_properties() started")
        l = [(key, prop) for key, prop in self.__props.iteritems()]
        for key, prop in l:
            self.__log("initialize prop " + key)
            if getattr(prop, 'key', None) is None:
                prop.init(key, self)
        self.__log("__initialize_properties() complete")
        self.compiled = True

    def __compile_extensions(self):
        """Go through the global_extensions list as well as the list
        of ``MapperExtensions`` specified for this ``Mapper`` and
        creates a linked list of those extensions.
        """

        extlist = util.OrderedSet()

        extension = self.extension
        if extension:
            for ext_obj in util.to_list(extension):
                # local MapperExtensions have already instrumented the class
                extlist.add(ext_obj)

        if self.inherits:
            for ext in self.inherits.extension:
                if ext not in extlist:
                    extlist.add(ext)
        else:
            for ext in global_extensions:
                if isinstance(ext, type):
                    ext = ext()
                if ext not in extlist:
                    extlist.add(ext)

        self.extension = ExtensionCarrier()
        for ext in extlist:
            self.extension.append(ext)

    def __compile_inheritance(self):
        """Configure settings related to inherting and/or inherited mappers being present."""

        if self.inherits:
            if isinstance(self.inherits, type):
                self.inherits = class_mapper(self.inherits, compile=False)
            if not issubclass(self.class_, self.inherits.class_):
                raise sa_exc.ArgumentError("Class '%s' does not inherit from '%s'" % (self.class_.__name__, self.inherits.class_.__name__))
            if self.non_primary != self.inherits.non_primary:
                np = not self.non_primary and "primary" or "non-primary"
                raise sa_exc.ArgumentError("Inheritance of %s mapper for class '%s' is only allowed from a %s mapper" % (np, self.class_.__name__, np))
            # inherit_condition is optional.
            if self.local_table is None:
                self.local_table = self.inherits.local_table
                self.mapped_table = self.inherits.mapped_table
                self.single = True
            elif not self.local_table is self.inherits.local_table:
                if self.concrete:
                    self.mapped_table = self.local_table
                    for mapper in self.iterate_to_root():
                        if mapper.polymorphic_on:
                            mapper._requires_row_aliasing = True
                else:
                    if not self.inherit_condition:
                        # figure out inherit condition from our table to the immediate table
                        # of the inherited mapper, not its full table which could pull in other
                        # stuff we dont want (allows test/inheritance.InheritTest4 to pass)
                        self.inherit_condition = sqlutil.join_condition(self.inherits.local_table, self.local_table)
                    self.mapped_table = sql.join(self.inherits.mapped_table, self.local_table, self.inherit_condition)

                    fks = util.to_set(self.inherit_foreign_keys)
                    self.__inherits_equated_pairs = sqlutil.criterion_as_pairs(self.mapped_table.onclause, consider_as_foreign_keys=fks)
            else:
                self.mapped_table = self.local_table

            if self.polymorphic_identity and not self.concrete:
                self._identity_class = self.inherits._identity_class
            else:
                self._identity_class = self.class_

            if self.version_id_col is None:
                self.version_id_col = self.inherits.version_id_col

            for mapper in self.iterate_to_root():
                util.reset_cached(mapper, '_equivalent_columns')

            if self.order_by is False and not self.concrete and self.inherits.order_by is not False:
                self.order_by = self.inherits.order_by
                
            self.polymorphic_map = self.inherits.polymorphic_map
            self.batch = self.inherits.batch
            self.inherits._inheriting_mappers.add(self)
            self.base_mapper = self.inherits.base_mapper
            self._all_tables = self.inherits._all_tables

            if self.polymorphic_identity is not None:
                self.polymorphic_map[self.polymorphic_identity] = self
                if not self.polymorphic_on:
                    for mapper in self.iterate_to_root():
                        # try to set up polymorphic on using correesponding_column(); else leave
                        # as None
                        if mapper.polymorphic_on:
                            self.polymorphic_on = self.mapped_table.corresponding_column(mapper.polymorphic_on)
                            break
                    else:
                        # TODO: this exception not covered
                        raise sa_exc.ArgumentError("Mapper '%s' specifies a polymorphic_identity of '%s', but no mapper in it's hierarchy specifies the 'polymorphic_on' column argument" % (str(self), self.polymorphic_identity))
        else:
            self._all_tables = util.Set()
            self.base_mapper = self
            self.mapped_table = self.local_table
            if self.polymorphic_identity:
                if self.polymorphic_on is None:
                    raise sa_exc.ArgumentError("Mapper '%s' specifies a polymorphic_identity of '%s', but no mapper in it's hierarchy specifies the 'polymorphic_on' column argument" % (str(self), self.polymorphic_identity))
                self.polymorphic_map[self.polymorphic_identity] = self
            self._identity_class = self.class_
            
        if self.mapped_table is None:
            raise sa_exc.ArgumentError("Mapper '%s' does not have a mapped_table specified.  (Are you using the return value of table.create()?  It no longer has a return value.)" % str(self))

    def __compile_pks(self):

        self.tables = sqlutil.find_tables(self.mapped_table)

        if not self.tables:
            raise sa_exc.InvalidRequestError("Could not find any Table objects in mapped table '%s'" % str(self.mapped_table))

        self._pks_by_table = {}
        self._cols_by_table = {}

        all_cols = util.Set(chain(*[col.proxy_set for col in self._columntoproperty]))
        pk_cols = util.Set([c for c in all_cols if c.primary_key])

        # identify primary key columns which are also mapped by this mapper.
        for t in util.Set(self.tables + [self.mapped_table]):
            self._all_tables.add(t)
            if t.primary_key and pk_cols.issuperset(t.primary_key):
                # ordering is important since it determines the ordering of mapper.primary_key (and therefore query.get())
                self._pks_by_table[t] = util.OrderedSet(t.primary_key).intersection(pk_cols)
            self._cols_by_table[t] = util.OrderedSet(t.c).intersection(all_cols)
        
        # determine cols that aren't expressed within our tables; 
        # mark these as "read only" properties which are refreshed upon 
        # INSERT/UPDATE
        self._readonly_props = util.Set([
            self._columntoproperty[col] for col in self._columntoproperty if 
                not hasattr(col, 'table') or col.table not in self._cols_by_table
        ])
        
        # if explicit PK argument sent, add those columns to the primary key mappings
        if self.primary_key_argument:
            for k in self.primary_key_argument:
                if k.table not in self._pks_by_table:
                    self._pks_by_table[k.table] = util.OrderedSet()
                self._pks_by_table[k.table].add(k)

        if self.mapped_table not in self._pks_by_table or len(self._pks_by_table[self.mapped_table]) == 0:
            raise sa_exc.ArgumentError("Mapper %s could not assemble any primary key columns for mapped table '%s'" % (self, self.mapped_table.description))

        if self.inherits and not self.concrete and not self.primary_key_argument:
            # if inheriting, the "primary key" for this mapper is that of the inheriting (unless concrete or explicit)
            self.primary_key = self.inherits.primary_key
        else:
            # determine primary key from argument or mapped_table pks - reduce to the minimal set of columns
            if self.primary_key_argument:
                primary_key = sqlutil.reduce_columns([self.mapped_table.corresponding_column(c) for c in self.primary_key_argument])
            else:
                primary_key = sqlutil.reduce_columns(self._pks_by_table[self.mapped_table])

            if len(primary_key) == 0:
                raise sa_exc.ArgumentError("Mapper %s could not assemble any primary key columns for mapped table '%s'" % (self, self.mapped_table.description))

            self.primary_key = primary_key
            self.__log("Identified primary key columns: " + str(primary_key))

    def _get_clause(self):
        """create a "get clause" based on the primary key.  this is used
        by query.get() and many-to-one lazyloads to load this item
        by primary key.

        """
        params = [(primary_key, sql.bindparam(None, type_=primary_key.type)) for primary_key in self.primary_key]
        return sql.and_(*[k==v for (k, v) in params]), dict(params)
    _get_clause = property(util.cache_decorator(_get_clause))

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

        result = {}
        def visit_binary(binary):
            if binary.operator == operators.eq:
                if binary.left in result:
                    result[binary.left].add(binary.right)
                else:
                    result[binary.left] = util.Set([binary.right])
                if binary.right in result:
                    result[binary.right].add(binary.left)
                else:
                    result[binary.right] = util.Set([binary.left])
        for mapper in self.base_mapper.polymorphic_iterator():
            if mapper.inherit_condition:
                visitors.traverse(mapper.inherit_condition, {}, {'binary':visit_binary})

        return result
    _equivalent_columns = property(util.cache_decorator(_equivalent_columns))

    class _CompileOnAttr(PropComparator):
        """A placeholder descriptor which triggers compilation on access."""

        def __init__(self, class_, key):
            self.class_ = class_
            self.key = key
            self.existing_prop = getattr(class_, key, None)

        def __getattribute__(self, key):
            cls = object.__getattribute__(self, 'class_')
            clskey = object.__getattribute__(self, 'key')

            if key.startswith('__') and key != '__clause_element__':
                return object.__getattribute__(self, key)

            class_mapper(cls)

            if cls.__dict__.get(clskey) is self:
                # FIXME: there should not be any scenarios where
                # a mapper compile leaves this CompileOnAttr in
                # place.
                util.warn(
                    ("Attribute '%s' on class '%s' was not replaced during "
                     "mapper compilation operation") % (clskey, cls.__name__))
                # clean us up explicitly
                delattr(cls, clskey)

            return getattr(getattr(cls, clskey), key)

    def __compile_properties(self):

        # object attribute names mapped to MapperProperty objects
        self.__props = util.OrderedDict()

        # table columns mapped to lists of MapperProperty objects
        # using a list allows a single column to be defined as
        # populating multiple object attributes
        self._columntoproperty = {}

        # load custom properties
        if self._init_properties:
            for key, prop in self._init_properties.iteritems():
                self._compile_property(key, prop, False)

        # pull properties from the inherited mapper if any.
        if self.inherits:
            for key, prop in self.inherits.__props.iteritems():
                if key not in self.__props:
                    self._adapt_inherited_property(key, prop)

        # create properties for each column in the mapped table,
        # for those columns which don't already map to a property
        for column in self.mapped_table.columns:
            if column in self._columntoproperty:
                continue

            if (self.include_properties is not None and
                column.key not in self.include_properties):
                self.__log("not including property %s" % (column.key))
                continue

            if (self.exclude_properties is not None and
                column.key in self.exclude_properties):
                self.__log("excluding property %s" % (column.key))
                continue

            column_key = (self.column_prefix or '') + column.key

            self._compile_property(column_key, column, init=False, setparent=True)

        # do a special check for the "discriminiator" column, as it may only be present
        # in the 'with_polymorphic' selectable but we need it for the base mapper
        if self.polymorphic_on and self.polymorphic_on not in self._columntoproperty:
            col = self.mapped_table.corresponding_column(self.polymorphic_on) or self.polymorphic_on
            self._compile_property(col.key, ColumnProperty(col), init=False, setparent=True)

    def _adapt_inherited_property(self, key, prop):
        if not self.concrete:
            self._compile_property(key, prop, init=False, setparent=False)
        # TODO: concrete properties dont adapt at all right now....will require copies of relations() etc.

    def _compile_property(self, key, prop, init=True, setparent=True):
        self.__log("_compile_property(%s, %s)" % (key, prop.__class__.__name__))

        if not isinstance(prop, MapperProperty):
            # we were passed a Column or a list of Columns; generate a ColumnProperty
            columns = util.to_list(prop)
            column = columns[0]
            if not expression.is_column(column):
                raise sa_exc.ArgumentError("%s=%r is not an instance of MapperProperty or Column" % (key, prop))

            prop = self.__props.get(key, None)

            if isinstance(prop, ColumnProperty):
                # TODO: the "property already exists" case is still not well defined here.
                # assuming single-column, etc.

                if prop.parent is not self:
                    # existing ColumnProperty from an inheriting mapper.
                    # make a copy and append our column to it
                    prop = prop.copy()
                prop.columns.append(column)
                self.__log("appending to existing ColumnProperty %s" % (key))
            elif prop is None:
                mapped_column = []
                for c in columns:
                    mc = self.mapped_table.corresponding_column(c)
                    if not mc:
                        raise sa_exc.ArgumentError("Column '%s' is not represented in mapper's table.  Use the `column_property()` function to force this column to be mapped as a read-only attribute." % str(c))
                    mapped_column.append(mc)
                prop = ColumnProperty(*mapped_column)
            else:
                raise sa_exc.ArgumentError("WARNING: column '%s' conflicts with property '%s'.  To resolve this, map the column to the class under a different name in the 'properties' dictionary.  Or, to remove all awareness of the column entirely (including its availability as a foreign key), use the 'include_properties' or 'exclude_properties' mapper arguments to control specifically which table columns get mapped." % (column.key, repr(prop)))

        if isinstance(prop, ColumnProperty):
            col = self.mapped_table.corresponding_column(prop.columns[0])
            # col might not be present! the selectable given to the mapper need not include "deferred"
            # columns (included in zblog tests)
            if col is None:
                col = prop.columns[0]
                
                # column is coming in after _readonly_props was initialized; check
                # for 'readonly'
                if hasattr(self, '_readonly_props') and \
                    (not hasattr(col, 'table') or col.table not in self._cols_by_table):
                        self._readonly_props.add(prop)
                    
            else:
                # if column is coming in after _cols_by_table was initialized, ensure the col is in the
                # right set
                if hasattr(self, '_cols_by_table') and col.table in self._cols_by_table and col not in self._cols_by_table[col.table]:
                    self._cols_by_table[col.table].add(col)

            self.columns[key] = col
            for col in prop.columns:
                for col in col.proxy_set:
                    self._columntoproperty[col] = prop

        elif isinstance(prop, (ComparableProperty, SynonymProperty)) and setparent:
            if prop.descriptor is None:
                prop.descriptor = getattr(self.class_, key, None)
                if isinstance(prop.descriptor, Mapper._CompileOnAttr):
                    prop.descriptor = object.__getattribute__(prop.descriptor, 'existing_prop')
            if getattr(prop, 'map_column', False):
                if key not in self.mapped_table.c:
                    raise sa_exc.ArgumentError("Can't compile synonym '%s': no column on table '%s' named '%s'"  % (prop.name, self.mapped_table.description, key))
                self._compile_property(prop.name, ColumnProperty(self.mapped_table.c[key]), init=init, setparent=setparent)

        self.__props[key] = prop

        if setparent:
            prop.set_parent(self)

            if not self.non_primary:
                self.class_manager.install_descriptor(
                    key, Mapper._CompileOnAttr(self.class_, key))
        if init:
            prop.init(key, self)

        for mapper in self._inheriting_mappers:
            mapper._adapt_inherited_property(key, prop)

    def __compile_class(self):
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
            if not manager or None not in manager.mappers:
                raise sa_exc.InvalidRequestError(
                    "Class %s has no primary mapper configured.  Configure "
                    "a primary mapper first before setting up a non primary "
                    "Mapper.")
            self.class_manager = manager
            _mapper_registry[self] = True
            return

        if manager is not None:
            if manager.class_ is not self.class_:
                # An inherited manager.  Install one for this subclass.
                manager = None
            elif self.entity_name in manager.mappers:
                raise sa_exc.ArgumentError(
                    "Class '%s' already has a primary mapper defined "
                    "with entity name '%s'.  Use non_primary=True to "
                    "create a non primary Mapper.  clear_mappers() will "
                    "remove *all* current mappers from all classes." %
                    (self.class_, self.entity_name))

        _mapper_registry[self] = True

        if manager is None:
            manager = attributes.create_manager_for_cls(self.class_)

        self.class_manager = manager

        has_been_initialized = bool(manager.info.get(_INSTRUMENTOR, False))
        manager.mappers[self.entity_name] = self

        # The remaining members can be added by any mapper, e_name None or not.
        if has_been_initialized:
            return

        self.extension.instrument_class(self, self.class_)

        manager.instantiable = True
        manager.instance_state_factory = IdentityManagedState
        manager.deferred_scalar_loader = _load_scalar_attributes

        event_registry = manager.events
        event_registry.add_listener('on_init', _event_on_init)
        event_registry.add_listener('on_init_failure', _event_on_init_failure)
        if 'on_reconstitute' in self.extension.methods:
            def reconstitute(instance):
                self.extension.on_reconstitute(self, instance)
            event_registry.add_listener('on_load', reconstitute)


        manager.info[_INSTRUMENTOR] = self

    def common_parent(self, other):
        """Return true if the given mapper shares a common inherited parent as this mapper."""

        return self.base_mapper is other.base_mapper

    def _canload(self, state, allow_subtypes):
        s = self.primary_mapper()
        if self.polymorphic_on or allow_subtypes:
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

    def polymorphic_iterator(self):
        """Iterate through the collection including this mapper and
        all descendant mappers.

        This includes not just the immediately inheriting mappers but
        all their inheriting mappers as well.

        To iterate through an entire hierarchy, use
        ``mapper.base_mapper.polymorphic_iterator()``."""

        yield self
        for mapper in self._inheriting_mappers:
            for m in mapper.polymorphic_iterator():
                yield m


    def add_properties(self, dict_of_properties):
        """Add the given dictionary of properties to this mapper,
        using `add_property`.
        """

        for key, value in dict_of_properties.iteritems():
            self.add_property(key, value)

    def add_property(self, key, prop):
        """Add an individual MapperProperty to this mapper.

        If the mapper has not been compiled yet, just adds the
        property to the initial properties dictionary sent to the
        constructor.  If this Mapper has already been compiled, then
        the given MapperProperty is compiled immediately.
        """

        self._init_properties[key] = prop
        self._compile_property(key, prop, init=self.compiled)

    def __repr__(self):
        return '<Mapper at 0x%x; %s>' % (
            id(self), self.class_.__name__)

    def __str__(self):
        return "Mapper|" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.description or str(self.local_table)) + (self.non_primary and "|non-primary" or "")

    def primary_mapper(self):
        """Return the primary mapper corresponding to this mapper's class key (class + entity_name)."""
        return self.class_manager.mappers[self.entity_name]

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

        return (self._identity_class, tuple([row[column] for column in pk_cols]), self.entity_name)

    def identity_key_from_primary_key(self, primary_key):
        """Return an identity-map key for use in storing/retrieving an
        item from an identity map.

        primary_key
          A list of values indicating the identifier.
        """
        return (self._identity_class, tuple(util.to_list(primary_key)), self.entity_name)

    def identity_key_from_instance(self, instance):
        """Return the identity key for the given instance, based on
        its primary key attributes.

        This value is typically also found on the instance state under the
        attribute name `key`.

        """
        return self.identity_key_from_primary_key(self.primary_key_from_instance(instance))

    def _identity_key_from_state(self, state):
        return self.identity_key_from_primary_key(self._primary_key_from_state(state))

    def primary_key_from_instance(self, instance):
        """Return the list of primary key values for the given
        instance.
        """
        state = attributes.instance_state(instance)
        return self._primary_key_from_state(state)

    def _primary_key_from_state(self, state):
        return [self._get_state_attr_by_column(state, column) for column in self.primary_key]

    def _get_col_to_prop(self, column):
        try:
            return self._columntoproperty[column]
        except KeyError:
            prop = self.__props.get(column.key, None)
            if prop:
                raise exc.UnmappedColumnError("Column '%s.%s' is not available, due to conflicting property '%s':%s" % (column.table.name, column.name, column.key, repr(prop)))
            else:
                raise exc.UnmappedColumnError("No column %s.%s is configured on mapper %s..." % (column.table.name, column.name, str(self)))

    # TODO: improve names
    def _get_state_attr_by_column(self, state, column):
        return self._get_col_to_prop(column).getattr(state, column)

    def _set_state_attr_by_column(self, state, column, value):
        return self._get_col_to_prop(column).setattr(state, value, column)

    def _get_committed_attr_by_column(self, obj, column):
        state = attributes.instance_state(obj)
        return self._get_committed_state_attr_by_column(state, column)

    def _get_committed_state_attr_by_column(self, state, column):
        return self._get_col_to_prop(column).getcommitted(state, column)

    def _save_obj(self, states, uowtransaction, postupdate=False, post_update_cols=None, single=False):
        """Issue ``INSERT`` and/or ``UPDATE`` statements for a list of objects.

        This is called within the context of a UOWTransaction during a
        flush operation.

        `_save_obj` issues SQL statements not just for instances mapped
        directly by this mapper, but for instances mapped by all
        inheriting mappers as well.  This is to maintain proper insert
        ordering among a polymorphic chain of instances. Therefore
        _save_obj is typically called only on a *base mapper*, or a
        mapper which does not inherit from any other mapper.
        """

        if self.__should_log_debug:
            self.__log_debug("_save_obj() start, " + (single and "non-batched" or "batched"))

        # if batch=false, call _save_obj separately for each object
        if not single and not self.batch:
            for state in states:
                self._save_obj([state], uowtransaction, postupdate=postupdate, post_update_cols=post_update_cols, single=True)
            return

        # if session has a connection callable,
        # organize individual states with the connection to use for insert/update
        if 'connection_callable' in uowtransaction.mapper_flush_opts:
            connection_callable = uowtransaction.mapper_flush_opts['connection_callable']
            tups = [(state, _state_mapper(state), connection_callable(self, state.obj()), _state_has_identity(state)) for state in states]
        else:
            connection = uowtransaction.transaction.connection(self)
            tups = [(state, _state_mapper(state), connection, _state_has_identity(state)) for state in states]

        if not postupdate:
            # call before_XXX extensions
            for state, mapper, connection, has_identity in tups:
                if not has_identity:
                    if 'before_insert' in mapper.extension.methods:
                        mapper.extension.before_insert(mapper, connection, state.obj())
                else:
                    if 'before_update' in mapper.extension.methods:
                        mapper.extension.before_update(mapper, connection, state.obj())

        for state, mapper, connection, has_identity in tups:
            # detect if we have a "pending" instance (i.e. has no instance_key attached to it),
            # and another instance with the same identity key already exists as persistent.  convert to an
            # UPDATE if so.
            instance_key = mapper._identity_key_from_state(state)
            if not postupdate and not has_identity and instance_key in uowtransaction.session.identity_map:
                instance = uowtransaction.session.identity_map[instance_key]
                existing = attributes.instance_state(instance)
                if not uowtransaction.is_deleted(existing):
                    raise exc.FlushError("New instance %s with identity key %s conflicts with persistent instance %s" % (state_str(state), str(instance_key), state_str(existing)))
                if self.__should_log_debug:
                    self.__log_debug("detected row switch for identity %s.  will update %s, remove %s from transaction" % (instance_key, state_str(state), state_str(existing)))
                uowtransaction.set_row_switch(existing)

        inserted_objects = util.Set()
        updated_objects = util.Set()

        table_to_mapper = {}
        for mapper in self.base_mapper.polymorphic_iterator():
            for t in mapper.tables:
                table_to_mapper[t] = mapper

        for table in sqlutil.sort_tables(table_to_mapper.keys()):
            insert = []
            update = []

            for state, mapper, connection, has_identity in tups:
                if table not in mapper._pks_by_table:
                    continue
                pks = mapper._pks_by_table[table]
                instance_key = mapper._identity_key_from_state(state)

                if self.__should_log_debug:
                    self.__log_debug("_save_obj() table '%s' instance %s identity %s" % (table.name, state_str(state), str(instance_key)))

                isinsert = not instance_key in uowtransaction.session.identity_map and not postupdate and not has_identity
                params = {}
                value_params = {}
                hasdata = False

                if isinsert:
                    for col in mapper._cols_by_table[table]:
                        if col is mapper.version_id_col:
                            params[col.key] = 1
                        elif col in pks:
                            value = mapper._get_state_attr_by_column(state, col)
                            if value is not None:
                                params[col.key] = value
                        elif mapper.polymorphic_on and mapper.polymorphic_on.shares_lineage(col):
                            if self.__should_log_debug:
                                self.__log_debug("Using polymorphic identity '%s' for insert column '%s'" % (mapper.polymorphic_identity, col.key))
                            value = mapper.polymorphic_identity
                            if ((col.default is None and
                                 col.server_default is None) or
                                value is not None):
                                params[col.key] = value
                        else:
                            value = mapper._get_state_attr_by_column(state, col)
                            if ((col.default is None and
                                 col.server_default is None) or
                                value is not None):
                                if isinstance(value, sql.ClauseElement):
                                    value_params[col] = value
                                else:
                                    params[col.key] = value
                    insert.append((state, params, mapper, connection, value_params))
                else:
                    for col in mapper._cols_by_table[table]:
                        if col is mapper.version_id_col:
                            params[col._label] = mapper._get_state_attr_by_column(state, col)
                            params[col.key] = params[col._label] + 1
                            for prop in mapper._columntoproperty.values():
                                (added, unchanged, deleted) = attributes.get_history(state, prop.key, passive=True)
                                if added:
                                    hasdata = True
                        elif mapper.polymorphic_on and mapper.polymorphic_on.shares_lineage(col):
                            pass
                        else:
                            if post_update_cols is not None and col not in post_update_cols:
                                if col in pks:
                                    params[col._label] = mapper._get_state_attr_by_column(state, col)
                                continue

                            prop = mapper._columntoproperty[col]
                            (added, unchanged, deleted) = attributes.get_history(state, prop.key, passive=True)
                            if added:
                                if isinstance(added[0], sql.ClauseElement):
                                    value_params[col] = added[0]
                                else:
                                    params[col.key] = prop.get_col_value(col, added[0])
                                if col in pks:
                                    if deleted:
                                        params[col._label] = deleted[0]
                                    else:
                                        # row switch logic can reach us here
                                        params[col._label] = added[0]
                                hasdata = True
                            elif col in pks:
                                params[col._label] = mapper._get_state_attr_by_column(state, col)
                    if hasdata:
                        update.append((state, params, mapper, connection, value_params))

            if update:
                mapper = table_to_mapper[table]
                clause = sql.and_()

                for col in mapper._pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col._label, type_=col.type))

                if mapper.version_id_col and table.c.contains_column(mapper.version_id_col):
                    clause.clauses.append(mapper.version_id_col == sql.bindparam(mapper.version_id_col._label, type_=col.type))

                statement = table.update(clause)
                pks = mapper._pks_by_table[table]
                def comparator(a, b):
                    for col in pks:
                        x = cmp(a[1][col._label], b[1][col._label])
                        if x != 0:
                            return x
                    return 0
                update.sort(comparator)

                rows = 0
                for rec in update:
                    (state, params, mapper, connection, value_params) = rec
                    c = connection.execute(statement.values(value_params), params)
                    mapper.__postfetch(uowtransaction, connection, table, state, c, c.last_updated_params(), value_params)

                    # testlib.pragma exempt:__hash__
                    updated_objects.add((state, connection))
                    rows += c.rowcount

                if c.supports_sane_rowcount() and rows != len(update):
                    raise exc.ConcurrentModificationError("Updated rowcount %d does not match number of objects updated %d" % (rows, len(update)))

            if insert:
                statement = table.insert()
                def comparator(a, b):
                    return cmp(a[0].insert_order, b[0].insert_order)
                insert.sort(comparator)
                for rec in insert:
                    (state, params, mapper, connection, value_params) = rec
                    c = connection.execute(statement.values(value_params), params)
                    primary_key = c.last_inserted_ids()

                    if primary_key is not None:
                        # set primary key attributes
                        for i, col in enumerate(mapper._pks_by_table[table]):
                            if mapper._get_state_attr_by_column(state, col) is None and len(primary_key) > i:
                                mapper._set_state_attr_by_column(state, col, primary_key[i])
                    mapper.__postfetch(uowtransaction, connection, table, state, c, c.last_inserted_params(), value_params)

                    # synchronize newly inserted ids from one table to the next
                    # TODO: this fires off more than needed, try to organize syncrules
                    # per table
                    for m in util.reversed(list(mapper.iterate_to_root())):
                        if m.__inherits_equated_pairs:
                            m.__synchronize_inherited(state)

                    # testlib.pragma exempt:__hash__
                    inserted_objects.add((state, connection))
        
        if not postupdate:
            for state, mapper, connection, has_identity in tups:
                
                # expire readonly attributes
                readonly = state.unmodified.intersection([
                    p.key for p in mapper._readonly_props
                ])
                
                if readonly:
                    _expire_state(state, readonly)

                # if specified, eagerly refresh whatever has
                # been expired.
                if self.eager_defaults and state.unloaded:
                    state.key = self._identity_key_from_state(state)
                    uowtransaction.session.query(self)._get(
                        state.key, refresh_state=state,
                        only_load_props=state.unloaded)
                
                # call after_XXX extensions
                if not has_identity:
                    if 'after_insert' in mapper.extension.methods:
                        mapper.extension.after_insert(mapper, connection, state.obj())
                else:
                    if 'after_update' in mapper.extension.methods:
                        mapper.extension.after_update(mapper, connection, state.obj())

    def __synchronize_inherited(self, state):
        sync.populate(state, self, state, self, self.__inherits_equated_pairs)

    def __postfetch(self, uowtransaction, connection, table, state, resultproxy, params, value_params):
        """For a given Table that has just been inserted/updated,
        mark as 'expired' those attributes which correspond to columns
        that are marked as 'postfetch', and populate attributes which 
        correspond to columns marked as 'prefetch' or were otherwise generated
        within _save_obj().
        
        """
        postfetch_cols = resultproxy.postfetch_cols()
        generated_cols = list(resultproxy.prefetch_cols())

        if self.polymorphic_on:
            po = table.corresponding_column(self.polymorphic_on)
            if po:
                generated_cols.append(po)

        if self.version_id_col:
            generated_cols.append(self.version_id_col)

        for c in generated_cols:
            if c.key in params:
                self._set_state_attr_by_column(state, c, params[c.key])

        deferred_props = [prop.key for prop in [self._columntoproperty[c] for c in postfetch_cols]]
        
        if deferred_props:
            _expire_state(state, deferred_props)

    def _delete_obj(self, states, uowtransaction):
        """Issue ``DELETE`` statements for a list of objects.

        This is called within the context of a UOWTransaction during a
        flush operation.
        """

        if self.__should_log_debug:
            self.__log_debug("_delete_obj() start")

        if 'connection_callable' in uowtransaction.mapper_flush_opts:
            connection_callable = uowtransaction.mapper_flush_opts['connection_callable']
            tups = [(state, _state_mapper(state), connection_callable(self, state.obj())) for state in states]
        else:
            connection = uowtransaction.transaction.connection(self)
            tups = [(state, _state_mapper(state), connection) for state in states]

        for state, mapper, connection in tups:
            if 'before_delete' in mapper.extension.methods:
                mapper.extension.before_delete(mapper, connection, state.obj())

        table_to_mapper = {}
        for mapper in self.base_mapper.polymorphic_iterator():
            for t in mapper.tables:
                table_to_mapper[t] = mapper

        for table in sqlutil.sort_tables(table_to_mapper.keys(), reverse=True):
            delete = {}
            for state, mapper, connection in tups:
                if table not in mapper._pks_by_table:
                    continue

                params = {}
                if not _state_has_identity(state):
                    continue
                else:
                    delete.setdefault(connection, []).append(params)
                for col in mapper._pks_by_table[table]:
                    params[col.key] = mapper._get_state_attr_by_column(state, col)
                if mapper.version_id_col and table.c.contains_column(mapper.version_id_col):
                    params[mapper.version_id_col.key] = mapper._get_state_attr_by_column(state, mapper.version_id_col)

            for connection, del_objects in delete.iteritems():
                mapper = table_to_mapper[table]
                def comparator(a, b):
                    for col in mapper._pks_by_table[table]:
                        x = cmp(a[col.key], b[col.key])
                        if x != 0:
                            return x
                    return 0
                del_objects.sort(comparator)
                clause = sql.and_()
                for col in mapper._pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col.key, type_=col.type))
                if mapper.version_id_col and table.c.contains_column(mapper.version_id_col):
                    clause.clauses.append(mapper.version_id_col == sql.bindparam(mapper.version_id_col.key, type_=mapper.version_id_col.type))
                statement = table.delete(clause)
                c = connection.execute(statement, del_objects)
                if c.supports_sane_multi_rowcount() and c.rowcount != len(del_objects):
                    raise exc.ConcurrentModificationError("Deleted rowcount %d does not match number of objects deleted %d" % (c.rowcount, len(del_objects)))

        for state, mapper, connection in tups:
            if 'after_delete' in mapper.extension.methods:
                mapper.extension.after_delete(mapper, connection, state.obj())

    def _register_dependencies(self, uowcommit):
        """Register ``DependencyProcessor`` instances with a
        ``unitofwork.UOWTransaction``.

        This call `register_dependencies` on all attached
        ``MapperProperty`` instances.
        """

        for prop in self.__props.values():
            prop.register_dependencies(uowcommit)
        for dep in self._dependency_processors:
            dep.register_dependencies(uowcommit)

    def cascade_iterator(self, type_, state, halt_on=None):
        """Iterate each element and its mapper in an object graph,
        for all relations that meet the given cascade rule.

        type\_
          The name of the cascade rule (i.e. save-update, delete,
          etc.)

        state
          The lead InstanceState.  child items will be processed per
          the relations defined for this object's mapper.

        the return value are object instances; this provides a strong
        reference so that they don't fall out of scope immediately.
        """

        visited_instances = util.IdentitySet()
        visitables = [(self.__props.itervalues(), 'property', state)]

        while visitables:
            iterator, item_type, parent_state = visitables[-1]
            try:
                if item_type == 'property':
                    prop = iterator.next()
                    visitables.append((prop.cascade_iterator(type_, parent_state, visited_instances, halt_on), 'mapper', None))
                elif item_type == 'mapper':
                    instance, instance_mapper, corresponding_state  = iterator.next()
                    yield (instance, instance_mapper)
                    visitables.append((instance_mapper.__props.itervalues(), 'property', corresponding_state))
            except StopIteration:
                visitables.pop()

    def _instance_processor(self, context, path, adapter, polymorphic_from=None, extension=None, only_load_props=None, refresh_state=None):
        pk_cols = self.primary_key

        if polymorphic_from or refresh_state:
            polymorphic_on = None
        else:
            polymorphic_on = self.polymorphic_on
            polymorphic_instances = util.PopulateDict(self.__configure_subclass_mapper(context, path, adapter))

        version_id_col = self.version_id_col

        if adapter:
            pk_cols = [adapter.columns[c] for c in pk_cols]
            if polymorphic_on:
                polymorphic_on = adapter.columns[polymorphic_on]
            if version_id_col:
                version_id_col = adapter.columns[version_id_col]

        identity_class, entity_name = self._identity_class, self.entity_name
        def identity_key(row):
            return (identity_class, tuple([row[column] for column in pk_cols]), entity_name)

        new_populators = []
        existing_populators = []

        def populate_state(state, row, isnew, only_load_props, **flags):
            if not new_populators:
                new_populators[:], existing_populators[:] = self.__populators(context, path, row, adapter)

            if isnew:
                populators = new_populators
            else:
                populators = existing_populators

            if only_load_props:
                populators = [p for p in populators if p[0] in only_load_props]

            for key, populator in populators:
                populator(state, row, isnew=isnew, **flags)

        session_identity_map = context.session.identity_map

        if not extension:
            extension = self.extension

        translate_row = 'translate_row' in extension.methods
        create_instance = 'create_instance' in extension.methods
        populate_instance = 'populate_instance' in extension.methods
        append_result = 'append_result' in extension.methods
        populate_existing = context.populate_existing or self.always_refresh

        def _instance(row, result):
            if translate_row:
                ret = extension.translate_row(self, context, row)
                if ret is not EXT_CONTINUE:
                    row = ret

            if polymorphic_on:
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
                identitykey = identity_key(row)

            if identitykey in session_identity_map:
                instance = session_identity_map[identitykey]
                state = attributes.instance_state(instance)

                if self.__should_log_debug:
                    self.__log_debug("_instance(): using existing instance %s identity %s" % (instance_str(instance), str(identitykey)))

                isnew = state.runid != context.runid
                currentload = not isnew
                loaded_instance = False

                if not currentload and version_id_col and context.version_check and self._get_state_attr_by_column(state, self.version_id_col) != row[version_id_col]:
                    raise exc.ConcurrentModificationError("Instance '%s' version of %s does not match %s" % (state_str(state), self._get_state_attr_by_column(state, self.version_id_col), row[version_id_col]))
            elif refresh_state:
                # out of band refresh_state detected (i.e. its not in the session.identity_map)
                # honor it anyway.  this can happen if a _get() occurs within save_obj(), such as
                # when eager_defaults is True.
                state = refresh_state
                instance = state.obj()
                isnew = state.runid != context.runid
                currentload = True
                loaded_instance = False
            else:
                if self.__should_log_debug:
                    self.__log_debug("_instance(): identity key %s not in session" % str(identitykey))

                if self.allow_null_pks:
                    for x in identitykey[1]:
                        if x is not None:
                            break
                    else:
                        return None
                else:
                    if None in identitykey[1]:
                        return None
                isnew = True
                currentload = True
                loaded_instance = True

                if create_instance:
                    instance = extension.create_instance(self, context, row, self.class_)
                    if instance is EXT_CONTINUE:
                        instance = self.class_manager.new_instance()
                    else:
                        # TODO: don't think theres coverage here
                        manager = attributes.manager_of_class(instance.__class__)
                        # TODO: if manager is None, raise a friendly error about
                        # returning instances of unmapped types
                        manager.setup_instance(instance)
                else:
                    instance = self.class_manager.new_instance()

                if self.__should_log_debug:
                    self.__log_debug("_instance(): created new instance %s identity %s" % (instance_str(instance), str(identitykey)))

                state = attributes.instance_state(instance)
                state.entity_name = self.entity_name
                state.key = identitykey
                # manually adding instance to session.  for a complete add,
                # session._finalize_loaded() must be called.
                state.session_id = context.session.hash_key
                session_identity_map.add(state)

            if currentload or populate_existing:
                if isnew:
                    state.runid = context.runid
                    context.progress.add(state)

                if not populate_instance or extension.populate_instance(self, context, row, instance, only_load_props=only_load_props, instancekey=identitykey, isnew=isnew) is EXT_CONTINUE:
                    populate_state(state, row, isnew, only_load_props)

            else:
                # populate attributes on non-loading instances which have been expired
                # TODO: apply eager loads to un-lazy loaded collections ?
                if state in context.partials or state.unloaded:
                        
                    if state in context.partials:
                        isnew = False
                        attrs = context.partials[state]
                    else:
                        isnew = True
                        attrs = state.unloaded
                        context.partials[state] = attrs  #<-- allow query.instances to commit the subset of attrs

                    if not populate_instance or extension.populate_instance(self, context, row, instance, only_load_props=attrs, instancekey=identitykey, isnew=isnew) is EXT_CONTINUE:
                        populate_state(state, row, isnew, attrs, instancekey=identitykey)

            if loaded_instance:
                state._run_on_load(instance)

            if result is not None and (not append_result or extension.append_result(self, context, row, instance, result, instancekey=identitykey, isnew=isnew) is EXT_CONTINUE):
                result.append(instance)

            return instance
        return _instance

    def __populators(self, context, path, row, adapter):
        new_populators, existing_populators = [], []
        for prop in self.__props.values():
            newpop, existingpop = prop.create_row_processor(context, path, self, row, adapter)
            if newpop:
                new_populators.append((prop.key, newpop))
            if existingpop:
                existing_populators.append((prop.key, existingpop))
        return new_populators, existing_populators

    def __configure_subclass_mapper(self, context, path, adapter):
        def configure_subclass_mapper(discriminator):
            try:
                mapper = self.polymorphic_map[discriminator]
            except KeyError:
                raise AssertionError("No such polymorphic_identity %r is defined" % discriminator)
            if mapper is self:
                return None
            return mapper._instance_processor(context, path, adapter, polymorphic_from=self)
        return configure_subclass_mapper

    def _optimized_get_statement(self, state, attribute_names):
        props = self.__props
        tables = util.Set([props[key].parent.local_table for key in attribute_names])
        if self.base_mapper.local_table in tables:
            return None

        def visit_binary(binary):
            leftcol = binary.left
            rightcol = binary.right
            if leftcol is None or rightcol is None:
                return

            if leftcol.table not in tables:
                binary.left = sql.bindparam(None, self._get_committed_state_attr_by_column(state, leftcol), type_=binary.right.type)
            elif rightcol.table not in tables:
                binary.right = sql.bindparam(None, self._get_committed_state_attr_by_column(state, rightcol), type_=binary.right.type)

        allconds = []

        start = False
        for mapper in util.reversed(list(self.iterate_to_root())):
            if mapper.local_table in tables:
                start = True
            if start and not mapper.single:
                allconds.append(visitors.cloned_traverse(mapper.inherit_condition, {}, {'binary':visit_binary}))

        cond = sql.and_(*allconds)
        return sql.select(tables, cond, use_labels=True)

Mapper.logger = log.class_logger(Mapper)


def _event_on_init(state, instance, args, kwargs):
    """Trigger mapper compilation and run init_instance hooks."""

    instrumenting_mapper = state.manager.info[_INSTRUMENTOR]
    # compile() always compiles all mappers
    instrumenting_mapper.compile()
    if 'init_instance' in instrumenting_mapper.extension.methods:
        instrumenting_mapper.extension.init_instance(
            instrumenting_mapper, instrumenting_mapper.class_,
            state.manager.events.original_init,
            instance, args, kwargs)

def _event_on_init_failure(state, instance, args, kwargs):
    """Run init_failed hooks."""

    instrumenting_mapper = state.manager.info[_INSTRUMENTOR]
    if 'init_failed' in instrumenting_mapper.extension.methods:
        util.warn_exception(
            instrumenting_mapper.extension.init_failed,
            instrumenting_mapper, instrumenting_mapper.class_,
            state.manager.events.original_init, instance, args, kwargs)


def _load_scalar_attributes(state, attribute_names):
    mapper = _state_mapper(state)
    session = _state_session(state)
    if not session:
        raise sa_exc.UnboundExecutionError("Instance %s is not bound to a Session; attribute refresh operation cannot proceed" % (state_str(state)))

    has_key = _state_has_identity(state)

    result = False
    if mapper.inherits and not mapper.concrete:
        statement = mapper._optimized_get_statement(state, attribute_names)
        if statement:
            result = session.query(mapper).from_statement(statement)._get(None, only_load_props=attribute_names, refresh_state=state)

    if result is False:
        if has_key:
            identity_key = state.key
        else:
            identity_key = mapper._identity_key_from_state(state)
        result = session.query(mapper)._get(identity_key, refresh_state=state, only_load_props=attribute_names)

    # if instance is pending, a refresh operation may not complete (even if PK attributes are assigned)
    if has_key and result is None:
        raise exc.ObjectDeletedError("Instance '%s' has been deleted." % state_str(state))
