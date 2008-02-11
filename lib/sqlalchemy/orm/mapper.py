# orm/mapper.py
# Copyright (C) 2005, 2006, 2007, 2008 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines the [sqlalchemy.orm.mapper#Mapper] class, the central configurational
unit which associates a class with a database table.

This is a semi-private module; the main configurational API of the ORM is
available in [sqlalchemy.orm#].
"""

import weakref
from itertools import chain
from sqlalchemy import sql, util, exceptions, logging
from sqlalchemy.sql import expression, visitors, operators, util as sqlutil
from sqlalchemy.sql.expression import _corresponding_column_or_error
from sqlalchemy.orm import sync, attributes
from sqlalchemy.orm.util import ExtensionCarrier, create_row_adapter, state_str, instance_str
from sqlalchemy.orm.interfaces import MapperProperty, EXT_CONTINUE, PropComparator

__all__ = ['Mapper', 'class_mapper', 'object_mapper', '_mapper_registry']

_mapper_registry = weakref.WeakKeyDictionary()

# a list of MapperExtensions that will be installed in all mappers by default
global_extensions = []

# a constant returned by _get_attr_by_column to indicate
# this mapper is not handling an attribute for a particular
# column
NO_ATTRIBUTE = object()

# lock used to synchronize the "mapper compile" step
_COMPILE_MUTEX = util.threading.Lock()

# initialize these two lazily
ColumnProperty = None
SynonymProperty = None

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
                 allow_column_override = False,
                 entity_name = None,
                 always_refresh = False,
                 version_id_col = None,
                 polymorphic_on=None,
                 _polymorphic_map=None,
                 polymorphic_identity=None,
                 polymorphic_fetch=None,
                 concrete=False,
                 select_table=None,
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

        if not issubclass(class_, object):
            raise exceptions.ArgumentError("Class '%s' is not a new-style class" % class_.__name__)

        for table in (local_table, select_table):
            if table is not None and isinstance(table, expression._SelectBaseMixin):
                # some db's, noteably postgres, dont want to select from a select
                # without an alias.  also if we make our own alias internally, then
                # the configured properties on the mapper are not matched against the alias
                # we make, theres workarounds but it starts to get really crazy (its crazy enough
                # the SQL that gets generated) so just require an alias
                raise exceptions.ArgumentError("Mapping against a Select object requires that it has a name.  Use an alias to give it a name, i.e. s = select(...).alias('myselect')")

        self.class_ = class_
        self.entity_name = entity_name
        self.primary_key_argument = primary_key
        self.non_primary = non_primary
        self.order_by = order_by
        self.always_refresh = always_refresh
        self.version_id_col = version_id_col
        self.concrete = concrete
        self.single = False
        self.inherits = inherits
        self.select_table = select_table
        self.local_table = local_table
        self.inherit_condition = inherit_condition
        self.inherit_foreign_keys = inherit_foreign_keys
        self.extension = extension
        self._init_properties = properties or {}
        self.allow_column_override = allow_column_override
        self.allow_null_pks = allow_null_pks
        self.delete_orphans = []
        self.batch = batch
        self.eager_defaults = eager_defaults
        self.column_prefix = column_prefix
        self.polymorphic_on = polymorphic_on
        self._eager_loaders = util.Set()
        self._row_translators = {}
        self._dependency_processors = []
        self._clause_adapter = None

        # our 'polymorphic identity', a string name that when located in a result set row
        # indicates this Mapper should be used to construct the object instance for that row.
        self.polymorphic_identity = polymorphic_identity

        if polymorphic_fetch not in (None, 'union', 'select', 'deferred'):
            raise exceptions.ArgumentError("Invalid option for 'polymorphic_fetch': '%s'" % polymorphic_fetch)
        if polymorphic_fetch is None:
            self.polymorphic_fetch = (self.select_table is None) and 'select' or 'union'
        else:
            self.polymorphic_fetch = polymorphic_fetch

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

        # a second mapper that is used for selecting, if the "select_table" argument
        # was sent to this mapper.
        self.__surrogate_mapper = None

        self.__props_init = False

        self.__should_log_info = logging.is_info_enabled(self.logger)
        self.__should_log_debug = logging.is_debug_enabled(self.logger)

        self._compile_class()
        self._compile_inheritance()
        self._compile_extensions()
        self._compile_tables()
        self._compile_properties()
        self._compile_pks()
        self._compile_selectable()

        self.__log("constructed")

    def __log(self, msg):
        if self.__should_log_info:
            self.logger.info("(" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.description or str(self.local_table)) + (not self.non_primary and "|non-primary" or "") + ") " + msg)

    def __log_debug(self, msg):
        if self.__should_log_debug:
            self.logger.debug("(" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.description or str(self.local_table)) + (not self.non_primary and "|non-primary" or "") + ") " + msg)

    def _is_orphan(self, obj):
        optimistic = has_identity(obj)
        for (key,klass) in self.delete_orphans:
            if attributes.has_parent(klass, obj, key, optimistic=optimistic):
               return False
        else:
            if self.delete_orphans:
                if not has_identity(obj):
                    raise exceptions.FlushError("instance %s is an unsaved, pending instance and is an orphan (is not attached to %s)" %
                    (
                        obj,
                        ", nor ".join(["any parent '%s' instance via that classes' '%s' attribute" % (klass.__name__, key) for (key,klass) in self.delete_orphans])
                    ))
                else:
                    return True
            else:
                return False

    def get_property(self, key, resolve_synonyms=False, raiseerr=True):
        """return a MapperProperty associated with the given key."""

        self.compile()
        return self._get_property(key, resolve_synonyms=resolve_synonyms, raiseerr=raiseerr)

    def _get_property(self, key, resolve_synonyms=False, raiseerr=True):
        """private in-compilation version of get_property()."""

        prop = self.__props.get(key, None)
        if resolve_synonyms:
            while isinstance(prop, SynonymProperty):
                prop = self.__props.get(prop.name, None)
        if prop is None and raiseerr:
            raise exceptions.InvalidRequestError("Mapper '%s' has no property '%s'" % (str(self), key))
        return prop

    def iterate_properties(self):
        self.compile()
        return self.__props.itervalues()
    iterate_properties = property(iterate_properties, doc="returns an iterator of all MapperProperty objects.")

    def properties(self):
        raise NotImplementedError("Public collection of MapperProperty objects is provided by the get_property() and iterate_properties accessors.")
    properties = property(properties)

    compiled = property(lambda self:self.__props_init, doc="return True if this mapper is compiled")

    def dispose(self):
        # disaable any attribute-based compilation
        self.__props_init = True
        try:
            del self.class_.c
        except AttributeError:
            pass
        if not self.non_primary and self.entity_name in self._class_state.mappers:
            del self._class_state.mappers[self.entity_name]
        if not self._class_state.mappers:
            attributes.unregister_class(self.class_)

    def compile(self):
        """Compile this mapper into its final internal format.
        """

        if self.__props_init:
            return self
        _COMPILE_MUTEX.acquire()
        try:
            # double-check inside mutex
            if self.__props_init:
                return self

            # initialize properties on all mappers
            for mapper in list(_mapper_registry):
                if not mapper.__props_init:
                    mapper.__initialize_properties()

            return self
        finally:
            _COMPILE_MUTEX.release()

    def __initialize_properties(self):
        """Call the ``init()`` method on all ``MapperProperties``
        attached to this mapper.

        This happens after all mappers have completed compiling
        everything else up until this point, so that all dependencies
        are fully available.
        """

        self.__log("_initialize_properties() started")
        l = [(key, prop) for key, prop in self.__props.iteritems()]
        for key, prop in l:
            self.__log("initialize prop " + key)
            if getattr(prop, 'key', None) is None:
                prop.init(key, self)
        self.__log("_initialize_properties() complete")
        self.__props_init = True


    def _compile_extensions(self):
        """Go through the global_extensions list as well as the list
        of ``MapperExtensions`` specified for this ``Mapper`` and
        creates a linked list of those extensions.
        """

        extlist = util.OrderedSet()

        extension = self.extension
        if extension is not None:
            for ext_obj in util.to_list(extension):
                # local MapperExtensions have already instrumented the class
                extlist.add(ext_obj)

        if self.inherits is not None:
            for ext in self.inherits.extension:
                if ext not in extlist:
                    extlist.add(ext)
                    ext.instrument_class(self, self.class_)
        else:
            for ext in global_extensions:
                if isinstance(ext, type):
                    ext = ext()
                if ext not in extlist:
                    extlist.add(ext)
                    ext.instrument_class(self, self.class_)

        self.extension = ExtensionCarrier()
        for ext in extlist:
            self.extension.append(ext)

    def _compile_inheritance(self):
        """Determine if this Mapper inherits from another mapper, and
        if so calculates the mapped_table for this Mapper taking the
        inherited mapper into account.

        For joined table inheritance, creates a ``SyncRule`` that will
        synchronize column values between the joined tables. also
        initializes polymorphic variables used in polymorphic loads.
        """

        if self.inherits is not None:
            if isinstance(self.inherits, type):
                self.inherits = class_mapper(self.inherits, compile=False)
            else:
                self.inherits = self.inherits
            if not issubclass(self.class_, self.inherits.class_):
                raise exceptions.ArgumentError("Class '%s' does not inherit from '%s'" % (self.class_.__name__, self.inherits.class_.__name__))
            if self.non_primary != self.inherits.non_primary:
                np = not self.non_primary and "primary" or "non-primary"
                raise exceptions.ArgumentError("Inheritance of %s mapper for class '%s' is only allowed from a %s mapper" % (np, self.class_.__name__, np))
            # inherit_condition is optional.
            if self.local_table is None:
                self.local_table = self.inherits.local_table
                self.single = True
            if not self.local_table is self.inherits.local_table:
                if self.concrete:
                    self._synchronizer= None
                    self.mapped_table = self.local_table
                else:
                    if self.inherit_condition is None:
                        # figure out inherit condition from our table to the immediate table
                        # of the inherited mapper, not its full table which could pull in other
                        # stuff we dont want (allows test/inheritance.InheritTest4 to pass)
                        self.inherit_condition = sql.join(self.inherits.local_table, self.local_table).onclause
                    self.mapped_table = sql.join(self.inherits.mapped_table, self.local_table, self.inherit_condition)
                    # generate sync rules.  similarly to creating the on clause, specify a
                    # stricter set of tables to create "sync rules" by,based on the immediate
                    # inherited table, rather than all inherited tables
                    self._synchronizer = sync.ClauseSynchronizer(self, self, sync.ONETOMANY)
                    if self.inherit_foreign_keys:
                        fks = util.Set(self.inherit_foreign_keys)
                    else:
                        fks = None
                    self._synchronizer.compile(self.mapped_table.onclause, foreign_keys=fks)
            else:
                self._synchronizer = None
                self.mapped_table = self.local_table
            if self.polymorphic_identity is not None:
                self.inherits.polymorphic_map[self.polymorphic_identity] = self
                if self.polymorphic_on is None:
                    for mapper in self.iterate_to_root():
                        # try to set up polymorphic on using correesponding_column(); else leave
                        # as None
                        if mapper.polymorphic_on:
                            self.polymorphic_on = self.mapped_table.corresponding_column(mapper.polymorphic_on)
                            break
                    else:
                        # TODO: this exception not covered
                        raise exceptions.ArgumentError("Mapper '%s' specifies a polymorphic_identity of '%s', but no mapper in it's hierarchy specifies the 'polymorphic_on' column argument" % (str(self), self.polymorphic_identity))

            if self.polymorphic_identity is not None and not self.concrete:
                self._identity_class = self.inherits._identity_class
            else:
                self._identity_class = self.class_

            if self.version_id_col is None:
                self.version_id_col = self.inherits.version_id_col

            for mapper in self.iterate_to_root():
                if hasattr(mapper, '_genned_equivalent_columns'):
                    del mapper._genned_equivalent_columns

            if self.order_by is False:
                self.order_by = self.inherits.order_by
            self.polymorphic_map = self.inherits.polymorphic_map
            self.batch = self.inherits.batch
            self.inherits._inheriting_mappers.add(self)
            self.base_mapper = self.inherits.base_mapper
            self._all_tables = self.inherits._all_tables
        else:
            self._all_tables = util.Set()
            self.base_mapper = self
            self._synchronizer = None
            self.mapped_table = self.local_table
            if self.polymorphic_identity is not None:
                if self.polymorphic_on is None:
                    raise exceptions.ArgumentError("Mapper '%s' specifies a polymorphic_identity of '%s', but no mapper in it's hierarchy specifies the 'polymorphic_on' column argument" % (str(self), self.polymorphic_identity))
                self.polymorphic_map[self.polymorphic_identity] = self
            self._identity_class = self.class_

        if self.mapped_table is None:
            raise exceptions.ArgumentError("Mapper '%s' does not have a mapped_table specified.  (Are you using the return value of table.create()?  It no longer has a return value.)" % str(self))

    def _compile_tables(self):
        # summary of the various Selectable units:
        # mapped_table - the Selectable that represents a join of the underlying Tables to be saved (or just the Table)
        # local_table - the Selectable that was passed to this Mapper's constructor, if any
        # select_table - the Selectable that will be used during queries.  if this is specified
        # as a constructor keyword argument, it takes precendence over mapped_table, otherwise its mapped_table
        # this is either select_table if it was given explicitly, or in the case of a mapper that inherits
        # its local_table
        # tables - a collection of underlying Table objects pulled from mapped_table

        if self.select_table is None:
            self.select_table = self.mapped_table

        # locate all tables contained within the "table" passed in, which
        # may be a join or other construct
        self.tables = sqlutil.find_tables(self.mapped_table)

        if not self.tables:
            raise exceptions.InvalidRequestError("Could not find any Table objects in mapped table '%s'" % str(self.mapped_table))

    def _compile_pks(self):

        self._pks_by_table = {}
        self._cols_by_table = {}

        all_cols = util.Set(chain(*[c2 for c2 in [col.proxy_set for col in [c for c in self._columntoproperty]]]))
        pk_cols = util.Set([c for c in all_cols if c.primary_key])

        # identify primary key columns which are also mapped by this mapper.
        for t in util.Set(self.tables + [self.mapped_table]):
            self._all_tables.add(t)
            if t.primary_key and pk_cols.issuperset(t.primary_key):
                # ordering is important since it determines the ordering of mapper.primary_key (and therefore query.get())
                self._pks_by_table[t] = util.OrderedSet(t.primary_key).intersection(pk_cols)
            self._cols_by_table[t] = util.OrderedSet(t.c).intersection(all_cols)

        # if explicit PK argument sent, add those columns to the primary key mappings
        if self.primary_key_argument:
            for k in self.primary_key_argument:
                if k.table not in self._pks_by_table:
                    self._pks_by_table[k.table] = util.OrderedSet()
                self._pks_by_table[k.table].add(k)

        if self.mapped_table not in self._pks_by_table or len(self._pks_by_table[self.mapped_table]) == 0:
            raise exceptions.ArgumentError("Mapper %s could not assemble any primary key columns for mapped table '%s'" % (self, self.mapped_table.description))

        if self.inherits is not None and not self.concrete and not self.primary_key_argument:
            # if inheriting, the "primary key" for this mapper is that of the inheriting (unless concrete or explicit)
            self.primary_key = self.inherits.primary_key
            self._get_clause = self.inherits._get_clause
        else:
            # determine primary key from argument or mapped_table pks - reduce to the minimal set of columns
            if self.primary_key_argument:
                primary_key = sqlutil.reduce_columns([self.mapped_table.corresponding_column(c) for c in self.primary_key_argument])
            else:
                primary_key = sqlutil.reduce_columns(self._pks_by_table[self.mapped_table])

            if len(primary_key) == 0:
                raise exceptions.ArgumentError("Mapper %s could not assemble any primary key columns for mapped table '%s'" % (self, self.mapped_table.description))

            self.primary_key = primary_key
            self.__log("Identified primary key columns: " + str(primary_key))

            # create a "get clause" based on the primary key.  this is used
            # by query.get() and many-to-one lazyloads to load this item
            # by primary key.
            _get_clause = sql.and_()
            _get_params = {}
            for primary_key in self.primary_key:
                bind = sql.bindparam(None, type_=primary_key.type)
                _get_params[primary_key] = bind
                _get_clause.clauses.append(primary_key == bind)
            self._get_clause = (_get_clause, _get_params)

    def __get_equivalent_columns(self):
        """Create a map of all *equivalent* columns, based on
        the determination of column pairs that are equated to
        one another either by an established foreign key relationship
        or by a joined-table inheritance join.

        This is used to determine the minimal set of primary key
        columns for the mapper, as well as when relating
        columns to those of a polymorphic selectable (i.e. a UNION of
        several mapped tables), as that selectable usually only contains
        one column in its columns clause out of a group of several which
        are equated to each other.

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
            if mapper.inherit_condition is not None:
                visitors.traverse(mapper.inherit_condition, visit_binary=visit_binary)

        # TODO: matching of cols to foreign keys might better be generalized
        # into general column translation (i.e. corresponding_column)

        # recursively descend into the foreign key collection of the given column
        # and assemble each FK-related col as an "equivalent" for the given column
        def equivs(col, recursive, equiv):
            if col in recursive:
                return
            recursive.add(col)
            for fk in col.foreign_keys:
                if fk.column not in result:
                    result[fk.column] = util.Set()
                result[fk.column].add(equiv)
                equivs(fk.column, recursive, col)

        for column in (self.primary_key_argument or self._pks_by_table[self.mapped_table]):
            for col in column.proxy_set:
                if not col.foreign_keys:
                    if col not in result:
                        result[col] = util.Set()
                    result[col].add(col)
                else:
                    equivs(col, util.Set(), col)

        return result
    def _equivalent_columns(self):
        if hasattr(self, '_genned_equivalent_columns'):
            return self._genned_equivalent_columns
        else:
            self._genned_equivalent_columns  = self.__get_equivalent_columns()
            return self._genned_equivalent_columns
    _equivalent_columns = property(_equivalent_columns)

    class _CompileOnAttr(PropComparator):
        """placeholder class attribute which fires mapper compilation on access"""

        def __init__(self, class_, key):
            self.class_ = class_
            self.key = key
            self.existing_prop = getattr(class_, key, None)

        def __getattribute__(self, key):
            cls = object.__getattribute__(self, 'class_')
            clskey = object.__getattribute__(self, 'key')

            if key.startswith('__'):
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

    def _compile_properties(self):

        # object attribute names mapped to MapperProperty objects
        self.__props = util.OrderedDict()

        # table columns mapped to lists of MapperProperty objects
        # using a list allows a single column to be defined as
        # populating multiple object attributes
        self._columntoproperty = {}

        # load custom properties
        if self._init_properties is not None:
            for key, prop in self._init_properties.iteritems():
                self._compile_property(key, prop, False)

        # pull properties from the inherited mapper if any.
        if self.inherits is not None:
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
                raise exceptions.ArgumentError("%s=%r is not an instance of MapperProperty or Column" % (key, prop))

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
                        raise exceptions.ArgumentError("Column '%s' is not represented in mapper's table.  Use the `column_property()` function to force this column to be mapped as a read-only attribute." % str(c))
                    mapped_column.append(mc)
                prop = ColumnProperty(*mapped_column)
            else:
                if not self.allow_column_override:
                    raise exceptions.ArgumentError("WARNING: column '%s' not being added due to property '%s'.  Specify 'allow_column_override=True' to mapper() to ignore this condition." % (column.key, repr(prop)))
                else:
                    return

        if isinstance(prop, ColumnProperty):
            col = self.mapped_table.corresponding_column(prop.columns[0])
            # col might not be present! the selectable given to the mapper need not include "deferred"
            # columns (included in zblog tests)
            if col is None:
                col = prop.columns[0]

            self.columns[key] = col
            for col in prop.columns:
                for col in col.proxy_set:
                    self._columntoproperty[col] = prop
        elif isinstance(prop, SynonymProperty):
            prop.instrument = getattr(self.class_, key, None)
            if isinstance(prop.instrument, Mapper._CompileOnAttr):
                prop.instrument = object.__getattribute__(prop.instrument, 'existing_prop')
            if prop.map_column:
                if not key in self.mapped_table.c:
                    raise exceptions.ArgumentError("Can't compile synonym '%s': no column on table '%s' named '%s'"  % (prop.name, self.mapped_table.description, key))
                self._compile_property(prop.name, ColumnProperty(self.mapped_table.c[key]), init=init, setparent=setparent)
        self.__props[key] = prop

        if setparent:
            prop.set_parent(self)

            if not self.non_primary:
                setattr(self.class_, key, Mapper._CompileOnAttr(self.class_, key))

        if init:
            prop.init(key, self)

        for mapper in self._inheriting_mappers:
            mapper._adapt_inherited_property(key, prop)

    def _compile_selectable(self):
        """If the 'select_table' keyword argument was specified, set
        up a second *surrogate mapper* that will be used for select
        operations.

        The columns of `select_table` should encompass all the columns
        of the `mapped_table` either directly or through proxying
        relationships. Currently, non-column properties are **not**
        copied.  This implies that a polymorphic mapper can't do any
        eager loading right now.
        """

        if self.select_table is not self.mapped_table:
            # turn a straight join into an aliased selectable
            if isinstance(self.select_table, sql.Join):
                self.select_table = self.select_table.select(use_labels=True).alias()

            self.__surrogate_mapper = Mapper(self.class_, self.select_table, non_primary=True, _polymorphic_map=self.polymorphic_map, polymorphic_on=_corresponding_column_or_error(self.select_table, self.polymorphic_on), primary_key=self.primary_key_argument)
            adapter = sqlutil.ClauseAdapter(self.select_table, equivalents=self.__surrogate_mapper._equivalent_columns)

            if self.order_by:
                order_by = [expression._literal_as_text(o) for o in util.to_list(self.order_by) or []]
                order_by = adapter.copy_and_process(order_by)
                self.__surrogate_mapper.order_by=order_by

            if self._init_properties is not None:
                for key, prop in self._init_properties.iteritems():
                    if expression.is_column(prop):
                        self.__surrogate_mapper.add_property(key, _corresponding_column_or_error(self.select_table, prop))
                    elif (isinstance(prop, list) and expression.is_column(prop[0])):
                        self.__surrogate_mapper.add_property(key, [_corresponding_column_or_error(self.select_table, c) for c in prop])

            self.__surrogate_mapper._clause_adapter = adapter

    def _compile_class(self):
        """If this mapper is to be a primary mapper (i.e. the
        non_primary flag is not set), associate this Mapper with the
        given class_ and entity name.

        Subsequent calls to ``class_mapper()`` for the class_/entity
        name combination will return this mapper.  Also decorate the
        `__init__` method on the mapped class to include optional
        auto-session attachment logic.
        """

        if self.non_primary:
            self._class_state = self.class_._class_state
            _mapper_registry[self] = True
            return

        if not self.non_primary and '_class_state' in self.class_.__dict__ and (self.entity_name in self.class_._class_state.mappers):
             raise exceptions.ArgumentError("Class '%s' already has a primary mapper defined with entity name '%s'.  Use non_primary=True to create a non primary Mapper.  clear_mappers() will remove *all* current mappers from all classes." % (self.class_, self.entity_name))

        def extra_init(class_, oldinit, instance, args, kwargs):
            self.compile()
            if 'init_instance' in self.extension.methods:
                self.extension.init_instance(self, class_, oldinit, instance, args, kwargs)

        def on_exception(class_, oldinit, instance, args, kwargs):
            util.warn_exception(self.extension.init_failed, self, class_, oldinit, instance, args, kwargs)

        attributes.register_class(self.class_, extra_init=extra_init, on_exception=on_exception, deferred_scalar_loader=_load_scalar_attributes)

        self._class_state = self.class_._class_state
        _mapper_registry[self] = True

        self.class_._class_state.mappers[self.entity_name] = self

        for ext in util.to_list(self.extension, []):
            ext.instrument_class(self, self.class_)

        if self.entity_name is None:
            self.class_.c = self.c

    def common_parent(self, other):
        """Return true if the given mapper shares a common inherited parent as this mapper."""

        return self.base_mapper is other.base_mapper

    def isa(self, other):
        """Return True if the given mapper inherits from this mapper."""

        m = other
        while m is not self and m.inherits is not None:
            m = m.inherits
        return m is self

    def iterate_to_root(self):
        m = self
        while m is not None:
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
        self._compile_property(key, prop, init=self.__props_init)

    def __str__(self):
        return "Mapper|" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.description or str(self.local_table)) + (self.non_primary and "|non-primary" or "")

    def primary_mapper(self):
        """Return the primary mapper corresponding to this mapper's class key (class + entity_name)."""
        return self._class_state.mappers[self.entity_name]

    def get_session(self):
        """Return the contextual session provided by the mapper
        extension chain, if any.

        Raise ``InvalidRequestError`` if a session cannot be retrieved
        from the extension chain.
        """

        if 'get_session' in self.extension.methods:
            s = self.extension.get_session()
            if s is not EXT_CONTINUE:
                return s

        raise exceptions.InvalidRequestError("No contextual Session is established.")

    def instances(self, cursor, session, *mappers, **kwargs):
        """Return a list of mapped instances corresponding to the rows
        in a given ResultProxy.

        DEPRECATED.
        """

        import sqlalchemy.orm.query
        return sqlalchemy.orm.Query(self, session).instances(cursor, *mappers, **kwargs)
    instances = util.deprecated(instances, add_deprecation_to_docstring=False)

    def identity_key_from_row(self, row):
        """Return an identity-map key for use in storing/retrieving an
        item from the identity map.

        row
          A ``sqlalchemy.engine.base.RowProxy`` instance or a
          dictionary corresponding result-set ``ColumnElement``
          instances to their values within a row.
        """
        return (self._identity_class, tuple([row[column] for column in self.primary_key]), self.entity_name)

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

        This value is typically also found on the instance itself
        under the attribute name `_instance_key`.
        """
        return self.identity_key_from_primary_key(self.primary_key_from_instance(instance))

    def _identity_key_from_state(self, state):
        return self.identity_key_from_primary_key(self._primary_key_from_state(state))

    def primary_key_from_instance(self, instance):
        """Return the list of primary key values for the given
        instance.
        """

        return [self._get_state_attr_by_column(instance._state, column) for column in self.primary_key]

    def _primary_key_from_state(self, state):
        return [self._get_state_attr_by_column(state, column) for column in self.primary_key]

    def _canload(self, state):
        if self.polymorphic_on is not None:
            return issubclass(state.class_, self.class_)
        else:
            return state.class_ is self.class_

    def _get_col_to_prop(self, column):
        try:
            return self._columntoproperty[column]
        except KeyError:
            prop = self.__props.get(column.key, None)
            if prop:
                raise exceptions.UnmappedColumnError("Column '%s.%s' is not available, due to conflicting property '%s':%s" % (column.table.name, column.name, column.key, repr(prop)))
            else:
                raise exceptions.UnmappedColumnError("No column %s.%s is configured on mapper %s..." % (column.table.name, column.name, str(self)))

    def _get_state_attr_by_column(self, state, column):
        return self._get_col_to_prop(column).getattr(state, column)

    def _set_state_attr_by_column(self, state, column, value):
        return self._get_col_to_prop(column).setattr(state, value, column)

    def _get_attr_by_column(self, obj, column):
        return self._get_col_to_prop(column).getattr(obj._state, column)

    def _get_committed_attr_by_column(self, obj, column):
        return self._get_col_to_prop(column).getcommitted(obj._state, column)

    def _set_attr_by_column(self, obj, column, value):
        self._get_col_to_prop(column).setattr(obj._state, column, value)

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
            tups = [(state, connection_callable(self, state.obj()), _state_has_identity(state)) for state in states]
        else:
            connection = uowtransaction.transaction.connection(self)
            tups = [(state, connection, _state_has_identity(state)) for state in states]

        if not postupdate:
            # call before_XXX extensions
            for state, connection, has_identity in tups:
                mapper = _state_mapper(state)
                if not has_identity:
                    if 'before_insert' in mapper.extension.methods:
                        mapper.extension.before_insert(mapper, connection, state.obj())
                else:
                    if 'before_update' in mapper.extension.methods:
                        mapper.extension.before_update(mapper, connection, state.obj())

        for state, connection, has_identity in tups:
            # detect if we have a "pending" instance (i.e. has no instance_key attached to it),
            # and another instance with the same identity key already exists as persistent.  convert to an
            # UPDATE if so.
            mapper = _state_mapper(state)
            instance_key = mapper._identity_key_from_state(state)
            if not postupdate and not has_identity and instance_key in uowtransaction.uow.identity_map:
                existing = uowtransaction.uow.identity_map[instance_key]._state
                if not uowtransaction.is_deleted(existing):
                    raise exceptions.FlushError("New instance %s with identity key %s conflicts with persistent instance %s" % (state_str(state), str(instance_key), state_str(existing)))
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

            for state, connection, has_identity in tups:
                mapper = _state_mapper(state)
                if table not in mapper._pks_by_table:
                    continue
                pks = mapper._pks_by_table[table]
                instance_key = mapper._identity_key_from_state(state)

                if self.__should_log_debug:
                    self.__log_debug("_save_obj() table '%s' instance %s identity %s" % (table.name, state_str(state), str(instance_key)))

                isinsert = not instance_key in uowtransaction.uow.identity_map and not postupdate and not has_identity
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
                        elif mapper.polymorphic_on is not None and mapper.polymorphic_on.shares_lineage(col):
                            if self.__should_log_debug:
                                self.__log_debug("Using polymorphic identity '%s' for insert column '%s'" % (mapper.polymorphic_identity, col.key))
                            value = mapper.polymorphic_identity
                            if col.default is None or value is not None:
                                params[col.key] = value
                        else:
                            value = mapper._get_state_attr_by_column(state, col)
                            if col.default is None or value is not None:
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
                        elif mapper.polymorphic_on is not None and mapper.polymorphic_on.shares_lineage(col):
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

                if mapper.version_id_col is not None and table.c.contains_column(mapper.version_id_col):
                    clause.clauses.append(mapper.version_id_col == sql.bindparam(mapper.version_id_col._label, type_=col.type))

                statement = table.update(clause)
                pks = mapper._pks_by_table[table]
                def comparator(a, b):
                    for col in pks:
                        x = cmp(a[1][col._label],b[1][col._label])
                        if x != 0:
                            return x
                    return 0
                update.sort(comparator)

                rows = 0
                for rec in update:
                    (state, params, mapper, connection, value_params) = rec
                    c = connection.execute(statement.values(value_params), params)
                    mapper._postfetch(uowtransaction, connection, table, state, c, c.last_updated_params(), value_params)

                    # testlib.pragma exempt:__hash__
                    updated_objects.add((state, connection))
                    rows += c.rowcount

                if c.supports_sane_rowcount() and rows != len(update):
                    raise exceptions.ConcurrentModificationError("Updated rowcount %d does not match number of objects updated %d" % (rows, len(update)))

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
                    mapper._postfetch(uowtransaction, connection, table, state, c, c.last_inserted_params(), value_params)

                    # synchronize newly inserted ids from one table to the next
                    # TODO: this fires off more than needed, try to organize syncrules
                    # per table
                    for m in util.reversed(list(mapper.iterate_to_root())):
                        if m._synchronizer is not None:
                            m._synchronizer.execute(state, state)

                    # testlib.pragma exempt:__hash__
                    inserted_objects.add((state, connection))

        if not postupdate:
            # call after_XXX extensions
            for state, connection, has_identity in tups:
                mapper = _state_mapper(state)
                if not has_identity:
                    if 'after_insert' in mapper.extension.methods:
                        mapper.extension.after_insert(mapper, connection, state.obj())
                else:
                    if 'after_update' in mapper.extension.methods:
                        mapper.extension.after_update(mapper, connection, state.obj())

    def _postfetch(self, uowtransaction, connection, table, state, resultproxy, params, value_params):
        """After an ``INSERT`` or ``UPDATE``, assemble newly generated
        values on an instance.  For columns which are marked as being generated
        on the database side, set up a group-based "deferred" loader
        which will populate those attributes in one query when next accessed.
        """

        postfetch_cols = util.Set(resultproxy.postfetch_cols()).union(util.Set(value_params.keys()))
        deferred_props = []

        for c in self._cols_by_table[table]:
            if c in postfetch_cols and (not c.key in params or c in value_params):
                prop = self._columntoproperty[c]
                deferred_props.append(prop.key)
            elif not c.primary_key and c.key in params and self._get_state_attr_by_column(state, c) != params[c.key]:
                self._set_state_attr_by_column(state, c, params[c.key])

        if deferred_props:
            if self.eager_defaults:
                _instance_key = self._identity_key_from_state(state)
                state.dict['_instance_key'] = _instance_key
                uowtransaction.session.query(self)._get(_instance_key, refresh_instance=state, only_load_props=deferred_props)
            else:
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
            tups = [(state, connection_callable(self, state.obj())) for state in states]
        else:
            connection = uowtransaction.transaction.connection(self)
            tups = [(state, connection) for state in states]

        for (state, connection) in tups:
            mapper = _state_mapper(state)
            if 'before_delete' in mapper.extension.methods:
                mapper.extension.before_delete(mapper, connection, state.obj())

        deleted_objects = util.Set()
        table_to_mapper = {}
        for mapper in self.base_mapper.polymorphic_iterator():
            for t in mapper.tables:
                table_to_mapper[t] = mapper

        for table in sqlutil.sort_tables(table_to_mapper.keys(), reverse=True):
            delete = {}
            for (state, connection) in tups:
                mapper = _state_mapper(state)
                if table not in mapper._pks_by_table:
                    continue

                params = {}
                if not _state_has_identity(state):
                    continue
                else:
                    delete.setdefault(connection, []).append(params)
                for col in mapper._pks_by_table[table]:
                    params[col.key] = mapper._get_state_attr_by_column(state, col)
                if mapper.version_id_col is not None and table.c.contains_column(mapper.version_id_col):
                    params[mapper.version_id_col.key] = mapper._get_state_attr_by_column(state, mapper.version_id_col)
                # testlib.pragma exempt:__hash__
                deleted_objects.add((state, connection))
            for connection, del_objects in delete.iteritems():
                mapper = table_to_mapper[table]
                def comparator(a, b):
                    for col in mapper._pks_by_table[table]:
                        x = cmp(a[col.key],b[col.key])
                        if x != 0:
                            return x
                    return 0
                del_objects.sort(comparator)
                clause = sql.and_()
                for col in mapper._pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col.key, type_=col.type))
                if mapper.version_id_col is not None and table.c.contains_column(mapper.version_id_col):
                    clause.clauses.append(mapper.version_id_col == sql.bindparam(mapper.version_id_col.key, type_=mapper.version_id_col.type))
                statement = table.delete(clause)
                c = connection.execute(statement, del_objects)
                if c.supports_sane_multi_rowcount() and c.rowcount != len(del_objects):
                    raise exceptions.ConcurrentModificationError("Deleted rowcount %d does not match number of objects deleted %d" % (c.rowcount, len(del_objects)))

        for state, connection in deleted_objects:
            mapper = _state_mapper(state)
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

    def cascade_iterator(self, type, state, recursive=None, halt_on=None):
        """Iterate each element and its mapper in an object graph,
        for all relations that meet the given cascade rule.

        type
          The name of the cascade rule (i.e. save-update, delete,
          etc.)

        state
          The lead InstanceState.  child items will be processed per
          the relations defined for this object's mapper.

        recursive
          Used by the function for internal context during recursive
          calls, leave as None.

        the return value are object instances; this provides a strong
        reference so that they don't fall out of scope immediately.
        """

        if recursive is None:
            recursive=util.IdentitySet()
        for prop in self.__props.values():
            for (c, m) in prop.cascade_iterator(type, state, recursive, halt_on=halt_on):
                yield (c, m)

    def get_select_mapper(self):
        """Return the mapper used for issuing selects.

        This mapper is the same mapper as `self` unless the
        select_table argument was specified for this mapper.
        """

        return self.__surrogate_mapper or self

    def _instance(self, context, row, result=None, skip_polymorphic=False, extension=None, only_load_props=None, refresh_instance=None):
        if not extension:
            extension = self.extension

        if 'translate_row' in extension.methods:
            ret = extension.translate_row(self, context, row)
            if ret is not EXT_CONTINUE:
                row = ret

        if not refresh_instance and not skip_polymorphic and self.polymorphic_on:
            discriminator = row[self.polymorphic_on]
            if discriminator:
                mapper = self.polymorphic_map[discriminator]
                if mapper is not self:
                    if ('polymorphic_fetch', mapper) not in context.attributes:
                        context.attributes[('polymorphic_fetch', mapper)] = (self, [t for t in mapper.tables if t not in self.tables])
                    row = self.translate_row(mapper, row)
                    return mapper._instance(context, row, result=result, skip_polymorphic=True)

        # determine identity key
        if refresh_instance:
            try:
                identitykey = refresh_instance.dict['_instance_key']
            except KeyError:
                # super-rare condition; a refresh is being called
                # on a non-instance-key instance; this is meant to only
                # occur wihtin a flush()
                identitykey = self._identity_key_from_state(refresh_instance)
        else:
            identitykey = self.identity_key_from_row(row)

        session_identity_map = context.session.identity_map

        if identitykey in session_identity_map:
            instance = session_identity_map[identitykey]
            state = instance._state

            if self.__should_log_debug:
                self.__log_debug("_instance(): using existing instance %s identity %s" % (instance_str(instance), str(identitykey)))

            isnew = state.runid != context.runid
            currentload = not isnew

            if not currentload and context.version_check and self.version_id_col and self._get_attr_by_column(instance, self.version_id_col) != row[self.version_id_col]:
                raise exceptions.ConcurrentModificationError("Instance '%s' version of %s does not match %s" % (instance, self._get_attr_by_column(instance, self.version_id_col), row[self.version_id_col]))
        elif refresh_instance:
            # out of band refresh_instance detected (i.e. its not in the session.identity_map)
            # honor it anyway.  this can happen if a _get() occurs within save_obj(), such as
            # when eager_defaults is True.
            state = refresh_instance
            instance = state.obj()
            isnew = state.runid != context.runid
            currentload = True
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

            if 'create_instance' in extension.methods:
                instance = extension.create_instance(self, context, row, self.class_)
                if instance is EXT_CONTINUE:
                    instance = attributes.new_instance(self.class_)
                else:
                    attributes.manage(instance)
            else:
                instance = attributes.new_instance(self.class_)

            if self.__should_log_debug:
                self.__log_debug("_instance(): created new instance %s identity %s" % (instance_str(instance), str(identitykey)))

            state = instance._state
            instance._entity_name = self.entity_name
            instance._instance_key = identitykey
            instance._sa_session_id = context.session.hash_key
            session_identity_map[identitykey] = instance

        if currentload or context.populate_existing or self.always_refresh:
            if isnew:
                state.runid = context.runid
                context.progress.add(state)

            if 'populate_instance' not in extension.methods or extension.populate_instance(self, context, row, instance, only_load_props=only_load_props, instancekey=identitykey, isnew=isnew) is EXT_CONTINUE:
                self.populate_instance(context, instance, row, only_load_props=only_load_props, instancekey=identitykey, isnew=isnew)
        
        else:
            attrs = getattr(state, 'expired_attributes', None)
            # populate attributes on non-loading instances which have been expired
            # TODO: also support deferred attributes here [ticket:870]
            if attrs: 
                if state in context.partials:
                    isnew = False
                    attrs = context.partials[state]
                else:
                    isnew = True
                    attrs = state.expired_attributes.intersection(state.unmodified)
                    context.partials[state] = attrs  #<-- allow query.instances to commit the subset of attrs

                if 'populate_instance' not in extension.methods or extension.populate_instance(self, context, row, instance, only_load_props=attrs, instancekey=identitykey, isnew=isnew) is EXT_CONTINUE:
                    self.populate_instance(context, instance, row, only_load_props=attrs, instancekey=identitykey, isnew=isnew)

        if result is not None and ('append_result' not in extension.methods or extension.append_result(self, context, row, instance, result, instancekey=identitykey, isnew=isnew) is EXT_CONTINUE):
            result.append(instance)

        return instance

    def translate_row(self, tomapper, row):
        """Translate the column keys of a row into a new or proxied
        row that can be understood by another mapper.

        This can be used in conjunction with populate_instance to
        populate an instance using an alternate mapper.
        """

        if tomapper in self._row_translators:
            # row translators are cached based on target mapper
            return self._row_translators[tomapper](row)
        else:
            translator = create_row_adapter(self.mapped_table, tomapper.mapped_table, equivalent_columns=self._equivalent_columns)
            self._row_translators[tomapper] = translator
            return translator(row)

    def populate_instance(self, selectcontext, instance, row, ispostselect=None, isnew=False, only_load_props=None, **flags):
        """populate an instance from a result row."""

        snapshot = selectcontext.path + (self,)
        # retrieve a set of "row population" functions derived from the MapperProperties attached
        # to this Mapper.  These are keyed in the select context based primarily off the
        # "snapshot" of the stack, which represents a path from the lead mapper in the query to this one,
        # including relation() names.  the key also includes "self", and allows us to distinguish between
        # other mappers within our inheritance hierarchy
        (new_populators, existing_populators) = selectcontext.attributes.get(('populators', self, snapshot, ispostselect), (None, None))
        if new_populators is None:
            # no populators; therefore this is the first time we are receiving a row for
            # this result set.  issue create_row_processor() on all MapperProperty objects
            # and cache in the select context.
            new_populators = []
            existing_populators = []
            post_processors = []
            for prop in self.__props.values():
                (newpop, existingpop, post_proc) = selectcontext.exec_with_path(self, prop.key, prop.create_row_processor, selectcontext, self, row)
                if newpop is not None:
                    new_populators.append((prop.key, newpop))
                if existingpop is not None:
                    existing_populators.append((prop.key, existingpop))
                if post_proc is not None:
                    post_processors.append(post_proc)

            # install a post processor for immediate post-load of joined-table inheriting mappers
            poly_select_loader = self._get_poly_select_loader(selectcontext, row)
            if poly_select_loader is not None:
                post_processors.append(poly_select_loader)

            selectcontext.attributes[('populators', self, snapshot, ispostselect)] = (new_populators, existing_populators)
            selectcontext.attributes[('post_processors', self, ispostselect)] = post_processors

        if isnew or ispostselect:
            populators = new_populators
        else:
            populators = existing_populators

        if only_load_props:
            populators = [p for p in populators if p[0] in only_load_props]

        for (key, populator) in populators:
            selectcontext.exec_with_path(self, key, populator, instance, row, ispostselect=ispostselect, isnew=isnew, **flags)

        if self.non_primary:
            selectcontext.attributes[('populating_mapper', instance._state)] = self

    def _post_instance(self, selectcontext, state, **kwargs):
        post_processors = selectcontext.attributes[('post_processors', self, None)]
        for p in post_processors:
            p(state.obj(), **kwargs)

    def _get_poly_select_loader(self, selectcontext, row):
        """set up attribute loaders for 'select' and 'deferred' polymorphic loading.

        this loading uses a second SELECT statement to load additional tables,
        either immediately after loading the main table or via a deferred attribute trigger.
        """

        (hosted_mapper, needs_tables) = selectcontext.attributes.get(('polymorphic_fetch', self), (None, None))

        if hosted_mapper is None or not needs_tables:
            return

        cond, param_names = self._deferred_inheritance_condition(hosted_mapper, needs_tables)
        statement = sql.select(needs_tables, cond, use_labels=True)

        if hosted_mapper.polymorphic_fetch == 'select':
            def post_execute(instance, **flags):
                if self.__should_log_debug:
                    self.__log_debug("Post query loading instance " + instance_str(instance))

                identitykey = self.identity_key_from_instance(instance)

                only_load_props = flags.get('only_load_props', None)

                params = {}
                for c, bind in param_names:
                    params[bind] = self._get_attr_by_column(instance, c)
                row = selectcontext.session.connection(self).execute(statement, params).fetchone()
                self.populate_instance(selectcontext, instance, row, isnew=False, instancekey=identitykey, ispostselect=True, only_load_props=only_load_props)
            return post_execute
        elif hosted_mapper.polymorphic_fetch == 'deferred':
            from sqlalchemy.orm.strategies import DeferredColumnLoader

            def post_execute(instance, **flags):
                def create_statement(instance):
                    params = {}
                    for (c, bind) in param_names:
                        # use the "committed" (database) version to get query column values
                        params[bind] = self._get_committed_attr_by_column(instance, c)
                    return (statement, params)

                props = [prop for prop in [self._get_col_to_prop(col) for col in statement.inner_columns] if prop.key not in instance.__dict__]
                keys = [p.key for p in props]
                
                only_load_props = flags.get('only_load_props', None)
                if only_load_props:
                    keys = util.Set(keys).difference(only_load_props)
                    props = [p for p in props if p.key in only_load_props]
                    
                for prop in props:
                    strategy = prop._get_strategy(DeferredColumnLoader)
                    instance._state.set_callable(prop.key, strategy.setup_loader(instance, props=keys, create_statement=create_statement))
            return post_execute
        else:
            return None

    def _deferred_inheritance_condition(self, base_mapper, needs_tables):
        base_mapper = base_mapper.primary_mapper()

        def visit_binary(binary):
            leftcol = binary.left
            rightcol = binary.right
            if leftcol is None or rightcol is None:
                return
            if leftcol.table not in needs_tables:
                binary.left = sql.bindparam(None, None, type_=binary.right.type)
                param_names.append((leftcol, binary.left))
            elif rightcol not in needs_tables:
                binary.right = sql.bindparam(None, None, type_=binary.right.type)
                param_names.append((rightcol, binary.right))

        allconds = []
        param_names = []

        for mapper in self.iterate_to_root():
            if mapper is base_mapper:
                break
            allconds.append(visitors.traverse(mapper.inherit_condition, clone=True, visit_binary=visit_binary))

        return sql.and_(*allconds), param_names

Mapper.logger = logging.class_logger(Mapper)


def has_identity(object):
    return hasattr(object, '_instance_key')

def _state_has_identity(state):
    return '_instance_key' in state.dict

def has_mapper(object):
    """Return True if the given object has had a mapper association
    set up, either through loading, or via insertion in a session.
    """

    return hasattr(object, '_entity_name')

object_session = None

def _load_scalar_attributes(instance, attribute_names):
    mapper = object_mapper(instance)

    global object_session
    if not object_session:
        from sqlalchemy.orm.session import object_session
    session = object_session(instance)
    if not session:
        try:
            session = mapper.get_session()
        except exceptions.InvalidRequestError:
            raise exceptions.UnboundExecutionError("Instance %s is not bound to a Session, and no contextual session is established; attribute refresh operation cannot proceed" % (instance.__class__))

    state = instance._state
    if '_instance_key' in state.dict:
        identity_key = state.dict['_instance_key']
    else:
        identity_key = mapper._identity_key_from_state(state)
    if session.query(mapper)._get(identity_key, refresh_instance=state, only_load_props=attribute_names) is None:
        raise exceptions.InvalidRequestError("Could not refresh instance '%s'" % instance_str(instance))

def _state_mapper(state, entity_name=None):
    return state.class_._class_state.mappers[state.dict.get('_entity_name', entity_name)]

def object_mapper(object, entity_name=None, raiseerror=True):
    """Given an object, return the primary Mapper associated with the object instance.

        object
            The object instance.

        entity_name
            Entity name of the mapper to retrieve, if the given instance is
            transient.  Otherwise uses the entity name already associated
            with the instance.

        raiseerror
            Defaults to True: raise an ``InvalidRequestError`` if no mapper can
            be located.  If False, return None.

    """

    try:
        mapper = object.__class__._class_state.mappers[getattr(object, '_entity_name', entity_name)]
    except (KeyError, AttributeError):
        if raiseerror:
            raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (object.__class__.__name__, getattr(object, '_entity_name', entity_name)))
        else:
            return None
    return mapper

def class_mapper(class_, entity_name=None, compile=True, raiseerror=True):
    """Given a class and optional entity_name, return the primary Mapper associated with the key.

    If no mapper can be located, raises ``InvalidRequestError``.
    """

    try:
        mapper = class_._class_state.mappers[entity_name]
    except (KeyError, AttributeError):
        if raiseerror:
            raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (class_.__name__, entity_name))
        else:
            return None
    if compile:
        return mapper.compile()
    else:
        return mapper
