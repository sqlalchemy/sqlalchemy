# orm/mapper.py
# Copyright (C) 2005, 2006, 2007 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, schema, util, exceptions, logging
from sqlalchemy import sql_util as sqlutil
from sqlalchemy.orm import util as mapperutil
from sqlalchemy.orm import sync
from sqlalchemy.orm.interfaces import MapperProperty, MapperOption, OperationContext, SynonymProperty
import weakref, warnings

__all__ = ['Mapper', 'MapperExtension', 'class_mapper', 'object_mapper', 'EXT_PASS', 'mapper_registry', 'ExtensionOption']

# a dictionary mapping classes to their primary mappers
mapper_registry = weakref.WeakKeyDictionary()

# a list of MapperExtensions that will be installed in all mappers by default
global_extensions = []

# a constant returned by get_attr_by_column to indicate
# this mapper is not handling an attribute for a particular
# column
NO_ATTRIBUTE = object()

# returned by a MapperExtension method to indicate a "do nothing" response
EXT_PASS = object()

# lock used to synchronize the "mapper compile" step
_COMPILE_MUTEX = util.threading.Lock()

class Mapper(object):
    """Define the correlation of class attributes to database table
    columns.

    Instances of this class should be constructed via the
    ``sqlalchemy.orm.mapper()`` function.
    """

    def __init__(self,
                class_,
                local_table,
                properties = None,
                primary_key = None,
                non_primary = False,
                inherits = None,
                inherit_condition = None,
                extension = None,
                order_by = False,
                allow_column_override = False,
                entity_name = None,
                always_refresh = False,
                version_id_col = None,
                polymorphic_on=None,
                _polymorphic_map=None,
                polymorphic_identity=None,
                concrete=False,
                select_table=None,
                allow_null_pks=False,
                batch=True,
                column_prefix=None):
        """Construct a new mapper.

        All arguments may be sent to the ``sqlalchemy.orm.mapper()``
        function where they are passed through to here.

        class\_
          The class to be mapped.

        local_table
          The table to which the class is mapped, or None if this
          mapper inherits from another mapper using concrete table
          inheritance.

        properties
          A dictionary mapping the string names of object attributes
          to ``MapperProperty`` instances, which define the
          persistence behavior of that attribute.  Note that the
          columns in the mapped table are automatically converted into
          ``ColumnProperty`` instances based on the `key` property of
          each ``Column`` (although they can be overridden using this
          dictionary).

        primary_key
          A list of ``Column`` objects which define the *primary key*
          to be used against this mapper's selectable unit.  This is
          normally simply the primary key of the `local_table`, but
          can be overridden here.

        non_primary
          Construct a ``Mapper`` that will define only the selection
          of instances, not their persistence.

        inherits
          Another ``Mapper`` for which this ``Mapper`` will have an
          inheritance relationship with.

        inherit_condition
          For joined table inheritance, a SQL expression (constructed
          ``ClauseElement``) which will define how the two tables are
          joined; defaults to a natural join between the two tables.

        extension
          A ``MapperExtension`` instance or list of
          ``MapperExtension`` instances which will be applied to all
          operations by this ``Mapper``.

        order_by
          A single ``Column`` or list of ``Columns`` for which
          selection operations should use as the default ordering for
          entities.  Defaults to the OID/ROWID of the table if any, or
          the first primary key column of the table.

        allow_column_override
          If True, allows the usage of a ``relation()`` which has the
          same name as a column in the mapped table.  The table column
          will no longer be mapped.

        entity_name
          A name to be associated with the `class`, to allow alternate
          mappings for a single class.

        always_refresh
          If True, all query operations for this mapped class will
          overwrite all data within object instances that already
          exist within the session, erasing any in-memory changes with
          whatever information was loaded from the database.

        version_id_col
          A ``Column`` which must have an integer type that will be
          used to keep a running *version id* of mapped entities in
          the database.  this is used during save operations to ensure
          that no other thread or process has updated the instance
          during the lifetime of the entity, else a
          ``ConcurrentModificationError`` exception is thrown.

        polymorphic_on
          Used with mappers in an inheritance relationship, a ``Column``
          which will identify the class/mapper combination to be used
          with a particular row.  requires the polymorphic_identity
          value to be set for all mappers in the inheritance
          hierarchy.

        _polymorphic_map
          Used internally to propigate the full map of polymorphic
          identifiers to surrogate mappers.

        polymorphic_identity
          A value which will be stored in the Column denoted by
          polymorphic_on, corresponding to the *class identity* of
          this mapper.

        concrete
          If True, indicates this mapper should use concrete table
          inheritance with its parent mapper.

        select_table
          A ``Table`` or (more commonly) ``Selectable`` which will be
          used to select instances of this mapper's class.  usually
          used to provide polymorphic loading among several classes in
          an inheritance hierarchy.

        allow_null_pks
          Indicates that composite primary keys where one or more (but
          not all) columns contain NULL is a valid primary key.
          Primary keys which contain NULL values usually indicate that
          a result row does not contain an entity and should be
          skipped.

        batch
          Indicates that save operations of multiple entities can be
          batched together for efficiency.  setting to False indicates
          that an instance will be fully saved before saving the next
          instance, which includes inserting/updating all table rows
          corresponding to the entity as well as calling all
          ``MapperExtension`` methods corresponding to the save
          operation.

        column_prefix
          A string which will be prepended to the `key` name of all
          Columns when creating column-based properties from the given
          Table.  Does not affect explicitly specified column-based
          properties
        """

        if not issubclass(class_, object):
            raise exceptions.ArgumentError("Class '%s' is not a new-style class" % class_.__name__)

        for table in (local_table, select_table):
            if table is not None and isinstance(table, sql._SelectBaseMixin):
                # some db's, noteably postgres, dont want to select from a select
                # without an alias.  also if we make our own alias internally, then
                # the configured properties on the mapper are not matched against the alias
                # we make, theres workarounds but it starts to get really crazy (its crazy enough
                # the SQL that gets generated) so just require an alias
                raise exceptions.ArgumentError("Mapping against a Select object requires that it has a name.  Use an alias to give it a name, i.e. s = select(...).alias('myselect')")

        self.class_ = class_
        self.entity_name = entity_name
        self.class_key = ClassKey(class_, entity_name)
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
        self.extension = extension
        self.properties = properties or {}
        self.allow_column_override = allow_column_override
        self.allow_null_pks = allow_null_pks
        self.delete_orphans = []
        self.batch = batch
        self.column_prefix = column_prefix
        # a Column which is used during a select operation to retrieve the
        # "polymorphic identity" of the row, which indicates which Mapper should be used
        # to construct a new object instance from that row.
        self.polymorphic_on = polymorphic_on
        self._eager_loaders = util.Set()

        # our 'polymorphic identity', a string name that when located in a result set row
        # indicates this Mapper should be used to construct the object instance for that row.
        self.polymorphic_identity = polymorphic_identity

        # a dictionary of 'polymorphic identity' names, associating those names with
        # Mappers that will be used to construct object instances upon a select operation.
        if _polymorphic_map is None:
            self.polymorphic_map = {}
        else:
            self.polymorphic_map = _polymorphic_map

        class LOrderedProp(util.OrderedProperties):
            """this extends OrderedProperties to trigger a compile() before the
            members of the object are accessed."""
            def _get_data(s):
                self.compile()
                return s.__dict__['_data']
            _data = property(_get_data)

        self.columns = LOrderedProp()
        self.c = self.columns

        # each time the options() method is called, the resulting Mapper is
        # stored in this dictionary based on the given options for fast re-access
        self._options = {}

        # a set of all mappers which inherit from this one.
        self._inheriting_mappers = util.Set()

        # a second mapper that is used for selecting, if the "select_table" argument
        # was sent to this mapper.
        self.__surrogate_mapper = None

        # whether or not our compile() method has been called already.
        self.__is_compiled = False

        # if this mapper is to be a primary mapper (i.e. the non_primary flag is not set),
        # associate this Mapper with the given class_ and entity name.  subsequent
        # calls to class_mapper() for the class_/entity name combination will return this
        # mapper.
        self._compile_class()

        self.__should_log_debug = logging.is_debug_enabled(self.logger)
        self.__log("constructed")

        # uncomment to compile at construction time (the old way)
        # this will break mapper setups that arent declared in the order
        # of dependency
        #self.compile()

    def __log(self, msg):
        self.logger.info("(" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.name or str(self.local_table)) + (not self._is_primary_mapper() and "|non-primary" or "") + ") " + msg)

    def __log_debug(self, msg):
        self.logger.debug("(" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.name or str(self.local_table)) + (not self._is_primary_mapper() and "|non-primary" or "") + ") " + msg)

    def _is_orphan(self, obj):
        optimistic = has_identity(obj)
        for (key,klass) in self.delete_orphans:
            if getattr(klass, key).hasparent(obj, optimistic=optimistic):
               return False
        else:
            if len(self.delete_orphans):
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

    def _get_props(self):
        self.compile()
        return self.__props

    props = property(_get_props, doc="compiles this mapper if needed, and returns the "
                     "dictionary of MapperProperty objects associated with this mapper."
                     "(Deprecated; use get_property() and iterate_properties)")

    def get_property(self, key, resolve_synonyms=False, raiseerr=True):
        """return MapperProperty with the given key.

        forwards compatible with 0.4.
        """

        self.compile()
        prop = self.__props.get(key, None)
        if resolve_synonyms:
            while isinstance(prop, SynonymProperty):
                prop = self.__props.get(prop.name, None)
        if prop is None and raiseerr:
            raise exceptions.InvalidRequestError("Mapper '%s' has no property '%s'" % (str(self), key))
        return prop

    iterate_properties = property(lambda self: self._get_props().itervalues(), doc="returns an iterator of all MapperProperty objects."
                                    "  Forwards compatible with 0.4")

    def compile(self):
        """Compile this mapper into its final internal format.

        This is the *external* version of the method which is not
        reentrant.
        """

        if self.__is_compiled:
            return self
        _COMPILE_MUTEX.acquire()
        try:
            # double-check inside mutex
            if self.__is_compiled:
                return self
            self._compile_all()

            # if we're not primary, compile us
            if self.non_primary:
                self._do_compile()
                self._initialize_properties()

            return self
        finally:
            _COMPILE_MUTEX.release()

    def _compile_all(self):
        # compile all primary mappers
        for mapper in mapper_registry.values():
            if not mapper.__is_compiled:
                mapper._do_compile()

        # initialize properties on all mappers
        for mapper in mapper_registry.values():
            if not mapper.__props_init:
                mapper._initialize_properties()

    def _check_compile(self):
        if self.non_primary:
            self._do_compile()
            self._initialize_properties()
        return self

    def _do_compile(self):
        """Compile this mapper into its final internal format.

        This is the *internal* version of the method which is assumed
        to be called within compile() and is reentrant.
        """

        if self.__is_compiled:
            return self
        self.__log("_do_compile() started")
        self.__is_compiled = True
        self.__props_init = False
        self._compile_extensions()
        self._compile_inheritance()
        self._compile_tables()
        self._compile_properties()
        self._compile_selectable()
        self.__log("_do_compile() complete")
        return self

    def _compile_extensions(self):
        """Go through the global_extensions list as well as the list
        of ``MapperExtensions`` specified for this ``Mapper`` and
        creates a linked list of those extensions.
        """

        extlist = util.Set()
        for ext_class in global_extensions:
            if isinstance(ext_class, MapperExtension):
                extlist.add(ext_class)
            else:
                extlist.add(ext_class())

        extension = self.extension
        if extension is not None:
            for ext_obj in util.to_list(extension):
                extlist.add(ext_obj)

        self.extension = _ExtensionCarrier()
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
                self.inherits = class_mapper(self.inherits, compile=False)._do_compile()
            else:
                self.inherits = self.inherits._do_compile()
            if not issubclass(self.class_, self.inherits.class_):
                raise exceptions.ArgumentError("Class '%s' does not inherit from '%s'" % (self.class_.__name__, self.inherits.class_.__name__))
            if self._is_primary_mapper() != self.inherits._is_primary_mapper():
                np = self._is_primary_mapper() and "primary" or "non-primary"
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
                    self._synchronizer.compile(self.mapped_table.onclause)
            else:
                self._synchronizer = None
                self.mapped_table = self.local_table
            if self.polymorphic_identity is not None:
                self.inherits._add_polymorphic_mapping(self.polymorphic_identity, self)
            if self.polymorphic_on is None and self.inherits.polymorphic_on is not None:
                self.polymorphic_on = self.mapped_table.corresponding_column(self.inherits.polymorphic_on, keys_ok=True, raiseerr=False)
            if self.order_by is False:
                self.order_by = self.inherits.order_by
            self.polymorphic_map = self.inherits.polymorphic_map
            self.batch = self.inherits.batch
            self.inherits._inheriting_mappers.add(self)
        else:
            self._synchronizer = None
            self.mapped_table = self.local_table
            if self.polymorphic_identity is not None:
                self._add_polymorphic_mapping(self.polymorphic_identity, self)

        if self.mapped_table is None:
            raise exceptions.ArgumentError("Mapper '%s' does not have a mapped_table specified.  (Are you using the return value of table.create()?  It no longer has a return value.)" % str(self))

        # convert polymorphic class associations to mappers
        for key in self.polymorphic_map.keys():
            if isinstance(self.polymorphic_map[key], type):
                self.polymorphic_map[key] = class_mapper(self.polymorphic_map[key])

    def _add_polymorphic_mapping(self, key, class_or_mapper, entity_name=None):
        """Add a Mapper to our *polymorphic map*."""

        if isinstance(class_or_mapper, type):
            class_or_mapper = class_mapper(class_or_mapper, entity_name=entity_name)
        self.polymorphic_map[key] = class_or_mapper

    def _compile_tables(self):
        """After the inheritance relationships have been reconciled,
        set up some more table-based instance variables and determine
        the *primary key* columns for all tables represented by this
        ``Mapper``.
        """

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
        self.tables = sqlutil.TableFinder(self.mapped_table)

        # determine primary key columns, either passed in, or get them from our set of tables
        self.pks_by_table = {}
        if self.primary_key_argument is not None:
            # determine primary keys using user-given list of primary key columns as a guide
            #
            # TODO: this might not work very well for joined-table and/or polymorphic
            # inheritance mappers since local_table isnt taken into account nor is select_table
            # need to test custom primary key columns used with inheriting mappers
            for k in self.primary_key_argument:
                self.pks_by_table.setdefault(k.table, util.OrderedSet()).add(k)
                if k.table != self.mapped_table:
                    # associate pk cols from subtables to the "main" table
                    corr = self.mapped_table.corresponding_column(k, raiseerr=False)
                    if corr is not None:
                        self.pks_by_table.setdefault(self.mapped_table, util.OrderedSet()).add(corr)
        else:
            # no user-defined primary key columns - go through all of our represented tables
            # and assemble primary key columns
            for t in self.tables + [self.mapped_table]:
                try:
                    l = self.pks_by_table[t]
                except KeyError:
                    l = self.pks_by_table.setdefault(t, util.OrderedSet())
                for k in t.primary_key:
                    #if k.key not in t.c and k._label not in t.c:
                        # this is a condition that was occurring when table reflection was doubling up primary keys
                        # that were overridden in the Table constructor
                    #    raise exceptions.AssertionError("Column " + str(k) + " not located in the column set of table " + str(t))
                    l.add(k)

        if len(self.pks_by_table[self.mapped_table]) == 0:
            raise exceptions.ArgumentError("Could not assemble any primary key columns for mapped table '%s'" % (self.mapped_table.name))
        self.primary_key = self.pks_by_table[self.mapped_table]
        
    def _compile_properties(self):
        """Inspect the properties dictionary sent to the Mapper's
        constructor as well as the mapped_table, and create
        ``MapperProperty`` objects corresponding to each mapped column
        and relation.

        Also grab ``MapperProperties`` from the inherited mapper, if
        any, and create copies of them to attach to this Mapper.
        """

        # object attribute names mapped to MapperProperty objects
        self.__props = {}

        # table columns mapped to lists of MapperProperty objects
        # using a list allows a single column to be defined as
        # populating multiple object attributes
        self.columntoproperty = mapperutil.TranslatingDict(self.mapped_table)

        # load custom properties
        if self.properties is not None:
            for key, prop in self.properties.iteritems():
                self._compile_property(key, prop, False)

        if self.inherits is not None:
            for key, prop in self.inherits.__props.iteritems():
                if not self.__props.has_key(key):
                    self._adapt_inherited_property(key, prop)

        # load properties from the main table object,
        # not overriding those set up in the 'properties' argument
        for column in self.mapped_table.columns:
            if self.columntoproperty.has_key(column):
                continue
            if not self.columns.has_key(column.key):
                self.columns[column.key] = self.select_table.corresponding_column(column, keys_ok=True, raiseerr=True)

            column_key = (self.column_prefix or '') + column.key
            prop = self.__props.get(column_key, None)
            if prop is None:
                prop = ColumnProperty(column)
                self.__props[column_key] = prop
                prop.set_parent(self)
                self.__log("adding ColumnProperty %s" % (column_key))
            elif isinstance(prop, ColumnProperty):
                if prop.parent is not self:
                    prop = ColumnProperty(deferred=prop.deferred, group=prop.group, *prop.columns)
                    prop.set_parent(self)
                    self.__props[column_key] = prop
                if column in self.primary_key and prop.columns[-1] in self.primary_key:
                    warnings.warn(RuntimeWarning("On mapper %s, primary key column '%s' is being combined with distinct primary key column '%s' in attribute '%s'.  Use explicit properties to give each column its own mapped attribute name." % (str(self), str(column), str(prop.columns[-1]), column_key)))
                prop.columns.append(column)
                self.__log("appending to existing ColumnProperty %s" % (column_key))
            else:
                if not self.allow_column_override:
                    raise exceptions.ArgumentError("WARNING: column '%s' not being added due to property '%s'.  Specify 'allow_column_override=True' to mapper() to ignore this condition." % (column.key, repr(prop)))
                else:
                    continue

            # its a ColumnProperty - match the ultimate table columns
            # back to the property
            proplist = self.columntoproperty.setdefault(column, [])
            proplist.append(prop)


    def _initialize_properties(self):
        """Call the ``init()`` method on all ``MapperProperties``
        attached to this mapper.

        This happens after all mappers have completed compiling
        everything else up until this point, so that all dependencies
        are fully available.
        """

        self.__log("_initialize_properties() started")
        l = [(key, prop) for key, prop in self.__props.iteritems()]
        for key, prop in l:
            if getattr(prop, 'key', None) is None:
                prop.init(key, self)
        self.__log("_initialize_properties() complete")
        self.__props_init = True

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
            props = {}
            if self.properties is not None:
                for key, prop in self.properties.iteritems():
                    if sql.is_column(prop):
                        props[key] = self.select_table.corresponding_column(prop)
                    elif (isinstance(prop, list) and sql.is_column(prop[0])):
                        props[key] = [self.select_table.corresponding_column(c) for c in prop]
            self.__surrogate_mapper = Mapper(self.class_, self.select_table, non_primary=True, properties=props, _polymorphic_map=self.polymorphic_map, polymorphic_on=self.select_table.corresponding_column(self.polymorphic_on), primary_key=self.primary_key_argument)

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
            return

        if not self.non_primary and (mapper_registry.has_key(self.class_key)):
             raise exceptions.ArgumentError("Class '%s' already has a primary mapper defined with entity name '%s'.  Use non_primary=True to create a non primary Mapper, or to create a new primary mapper, remove this mapper first via sqlalchemy.orm.clear_mapper(mapper), or preferably sqlalchemy.orm.clear_mappers() to clear all mappers." % (self.class_, self.entity_name))

        attribute_manager.reset_class_managed(self.class_)

        oldinit = self.class_.__init__
        def init(self, *args, **kwargs):
            entity_name = kwargs.pop('_sa_entity_name', None)
            mapper = mapper_registry.get(ClassKey(self.__class__, entity_name))
            if mapper is not None:
                mapper = mapper.compile()

                # this gets the AttributeManager to do some pre-initialization,
                # in order to save on KeyErrors later on
                attribute_manager.init_attr(self)

            if kwargs.has_key('_sa_session'):
                session = kwargs.pop('_sa_session')
            else:
                # works for whatever mapper the class is associated with
                if mapper is not None:
                    session = mapper.extension.get_session()
                    if session is EXT_PASS:
                        session = None
                else:
                    session = None
            # if a session was found, either via _sa_session or via mapper extension,
            # and we have found a mapper, save() this instance to the session, and give it an associated entity_name.
            # otherwise, this instance will not have a session or mapper association until it is
            # save()d to some session.
            if session is not None and mapper is not None:
                self._entity_name = entity_name
                session._register_pending(self)

            if oldinit is not None:
                try:
                    oldinit(self, *args, **kwargs)
                except:
                    def go():
                        if session is not None:
                            session.expunge(self)
                    # convert expunge() exceptions to warnings
                    util.warn_exception(go)
                    raise
                    
        # override oldinit, insuring that its not already a Mapper-decorated init method
        if oldinit is None or not hasattr(oldinit, '_sa_mapper_init'):
            init._sa_mapper_init = True
            try:
                init.__name__ = oldinit.__name__
                init.__doc__ = oldinit.__doc__
            except:
                # cant set __name__ in py 2.3 !
                pass
            self.class_.__init__ = init
        _COMPILE_MUTEX.acquire()
        try:
            mapper_registry[self.class_key] = self
        finally:
            _COMPILE_MUTEX.release()
        if self.entity_name is None:
            self.class_.c = self.c

    def base_mapper(self):
        """Return the ultimate base mapper in an inheritance chain."""

        if self.inherits is not None:
            return self.inherits.base_mapper()
        else:
            return self

    def common_parent(self, other):
        """Return true if the given mapper shares a common inherited parent as this mapper."""

        return self.base_mapper() is other.base_mapper()

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
        ``mapper.base_mapper().polymorphic_iterator()``."""

        yield self
        for mapper in self._inheriting_mappers:
            for m in mapper.polymorphic_iterator():
                yield m

    def _get_inherited_column_equivalents(self):
        """Return a map of all *equivalent* columns, based on
        traversing the full set of inherit_conditions across all
        inheriting mappers and determining column pairs that are
        equated to one another.

        This is used when relating columns to those of a polymorphic
        selectable, as the selectable usually only contains one of two (or more)
        columns that are equated to one another.
        
        The resulting structure is a dictionary of columns mapped
        to lists of equivalent columns, i.e.
        
        {
            tablea.col1: 
                [tableb.col1, tablec.col1],
            tablea.col2:
                [tabled.col2]
        }
        """

        result = {}
        def visit_binary(binary):
            if binary.operator == '=':
                if binary.left in result:
                    result[binary.left].append(binary.right)
                else:
                    result[binary.left] = [binary.right]
                if binary.right in result:
                    result[binary.right].append(binary.left)
                else:
                    result[binary.right] = [binary.left]
        vis = mapperutil.BinaryVisitor(visit_binary)
        for mapper in self.base_mapper().polymorphic_iterator():
            if mapper.inherit_condition is not None:
                vis.traverse(mapper.inherit_condition)
        return result

    def add_properties(self, dict_of_properties):
        """Add the given dictionary of properties to this mapper,
        using `add_property`.
        """

        for key, value in dict_of_properties.iteritems():
            self.add_property(key, value)

    def add_property(self, key, prop):
        """Add an indiviual MapperProperty to this mapper.

        If the mapper has not been compiled yet, just adds the
        property to the initial properties dictionary sent to the
        constructor.  If this Mapper has already been compiled, then
        the given MapperProperty is compiled immediately.
        """

        self.properties[key] = prop
        if self.__is_compiled:
            # if we're compiled, make sure all the other mappers are compiled too
            self._compile_all()
            self._compile_property(key, prop, init=True)

    def _create_prop_from_column(self, column):
        column = util.to_list(column)
        if not sql.is_column(column[0]):
            return None
        mapped_column = []
        for c in column:
            mc = self.mapped_table.corresponding_column(c, raiseerr=False)
            if not mc:
                raise exceptions.ArgumentError("Column '%s' is not represented in mapper's table.  Use the `column_property()` function to force this column to be mapped as a read-only attribute." % str(c))
            mapped_column.append(mc)
        return ColumnProperty(*mapped_column)

    def _adapt_inherited_property(self, key, prop):
        if not self.concrete:
            self._compile_property(key, prop, init=False, setparent=False)
        # TODO: concrete properties dont adapt at all right now....will require copies of relations() etc.

    def _compile_property(self, key, prop, init=True, setparent=True):
        """Add a ``MapperProperty`` to this or another ``Mapper``,
        including configuration of the property.

        The properties' parent attribute will be set, and the property
        will also be copied amongst the mappers which inherit from
        this one.

        If the given `prop` is a ``Column`` or list of Columns, a
        ``ColumnProperty`` will be created.
        """

        self.__log("_compile_property(%s, %s)" % (key, prop.__class__.__name__))

        if not isinstance(prop, MapperProperty):
            col = self._create_prop_from_column(prop)
            if col is None:
                raise exceptions.ArgumentError("%s=%r is not an instance of MapperProperty or Column" % (key, prop))
            prop = col

        self.__props[key] = prop
        if setparent:
            prop.set_parent(self)

        if isinstance(prop, ColumnProperty):
            # relate the mapper's "select table" to the given ColumnProperty
            col = self.select_table.corresponding_column(prop.columns[0], keys_ok=True, raiseerr=False)
            # col might not be present! the selectable given to the mapper need not include "deferred"
            # columns (included in zblog tests)
            if col is None:
                col = prop.columns[0]
            self.columns[key] = col
            for col in prop.columns:
                proplist = self.columntoproperty.setdefault(col, [])
                proplist.append(prop)

        if init:
            prop.init(key, self)

        for mapper in self._inheriting_mappers:
            mapper._adapt_inherited_property(key, prop)

    def __str__(self):
        return "Mapper|" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + (self.local_table and self.local_table.encodedname or str(self.local_table)) + (not self._is_primary_mapper() and "|non-primary" or "")

    def _is_primary_mapper(self):
        """Return True if this mapper is the primary mapper for its class key (class + entity_name)."""
        return mapper_registry.get(self.class_key, None) is self

    def primary_mapper(self):
        """Return the primary mapper corresponding to this mapper's class key (class + entity_name)."""
        return mapper_registry[self.class_key]

    def is_assigned(self, instance):
        """Return True if this mapper handles the given instance.

        This is dependent not only on class assignment but the
        optional `entity_name` parameter as well.
        """

        return instance.__class__ is self.class_ and getattr(instance, '_entity_name', None) == self.entity_name

    def _assign_entity_name(self, instance):
        """Assign this Mapper's entity name to the given instance.

        Subsequent Mapper lookups for this instance will return the
        primary mapper corresponding to this Mapper's class and entity
        name.
        """

        instance._entity_name = self.entity_name

    def get_session(self):
        """Return the contextual session provided by the mapper
        extension chain, if any.

        Raise ``InvalidRequestError`` if a session cannot be retrieved
        from the extension chain.
        """

        self.compile()
        s = self.extension.get_session()
        if s is EXT_PASS:
            raise exceptions.InvalidRequestError("No contextual Session is established.  Use a MapperExtension that implements get_session or use 'import sqlalchemy.mods.threadlocal' to establish a default thread-local contextual session.")
        return s

    def has_eager(self):
        """Return True if one of the properties attached to this
        Mapper is eager loading.
        """

        return len(self._eager_loaders) > 0

    def instances(self, cursor, session, *mappers, **kwargs):
        """Return a list of mapped instances corresponding to the rows
        in a given ResultProxy.
        """

        import sqlalchemy.orm.query
        return sqlalchemy.orm.Query(self, session).instances(cursor, *mappers, **kwargs)

    def identity_key_from_row(self, row):
        """Return an identity-map key for use in storing/retrieving an
        item from the identity map.

        row
          A ``sqlalchemy.engine.base.RowProxy`` instance or a
          dictionary corresponding result-set ``ColumnElement``
          instances to their values within a row.
        """
        return (self.class_, tuple([row[column] for column in self.pks_by_table[self.mapped_table]]), self.entity_name)

    def identity_key_from_primary_key(self, primary_key):
        """Return an identity-map key for use in storing/retrieving an
        item from an identity map.

        primary_key
          A list of values indicating the identifier.
        """
        return (self.class_, tuple(util.to_list(primary_key)), self.entity_name)

    def identity_key_from_instance(self, instance):
        """Return the identity key for the given instance, based on
        its primary key attributes.

        This value is typically also found on the instance itself
        under the attribute name `_instance_key`.
        """
        return self.identity_key_from_primary_key(self.primary_key_from_instance(instance))

    def primary_key_from_instance(self, instance):
        """Return the list of primary key values for the given
        instance.
        """

        return [self.get_attr_by_column(instance, column) for column in self.pks_by_table[self.mapped_table]]

    def canload(self, instance):
        """return true if this mapper is capable of loading the given instance"""
        if self.polymorphic_on is not None:
            return isinstance(instance, self.class_)
        else:
            return instance.__class__ is self.class_
        
    def instance_key(self, instance):
        """Deprecated. A synonym for `identity_key_from_instance`."""

        return self.identity_key_from_instance(instance)

    def identity_key(self, primary_key):
        """Deprecated. A synonym for `identity_key_from_primary_key`."""

        return self.identity_key_from_primary_key(primary_key)

    def identity(self, instance):
        """Deprecated. A synoynm for `primary_key_from_instance`."""

        return self.primary_key_from_instance(instance)

    def _getpropbycolumn(self, column, raiseerror=True):
        try:
            prop = self.columntoproperty[column]
        except KeyError:
            try:
                prop = self.__props[column.key]
                if not raiseerror:
                    return None
                raise exceptions.InvalidRequestError("Column '%s.%s' is not available, due to conflicting property '%s':%s" % (column.table.name, column.name, column.key, repr(prop)))
            except KeyError:
                if not raiseerror:
                    return None
                raise exceptions.InvalidRequestError("No column %s.%s is configured on mapper %s..." % (column.table.name, column.name, str(self)))
        return prop[0]

    def get_attr_by_column(self, obj, column, raiseerror=True):
        """Return an instance attribute using a Column as the key."""

        prop = self._getpropbycolumn(column, raiseerror)
        if prop is None:
            return NO_ATTRIBUTE
        #print "get column attribute '%s' from instance %s" % (column.key, mapperutil.instance_str(obj))
        return prop.getattr(obj)

    def set_attr_by_column(self, obj, column, value):
        """Set the value of an instance attribute using a Column as the key."""

        self.columntoproperty[column][0].setattr(obj, value)

    def save_obj(self, objects, uowtransaction, postupdate=False, post_update_cols=None, single=False):
        """Issue ``INSERT`` and/or ``UPDATE`` statements for a list of objects.

        This is called within the context of a UOWTransaction during a
        flush operation.

        `save_obj` issues SQL statements not just for instances mapped
        directly by this mapper, but for instances mapped by all
        inheriting mappers as well.  This is to maintain proper insert
        ordering among a polymorphic chain of instances. Therefore
        save_obj is typically called only on a *base mapper*, or a
        mapper which does not inherit from any other mapper.
        """

        if self.__should_log_debug:
            self.__log_debug("save_obj() start, " + (single and "non-batched" or "batched"))

        # if batch=false, call save_obj separately for each object
        if not single and not self.batch:
            for obj in objects:
                self.save_obj([obj], uowtransaction, postupdate=postupdate, post_update_cols=post_update_cols, single=True)
            return

        connection = uowtransaction.transaction.connection(self)

        if not postupdate:
            for obj in objects:
                if not has_identity(obj):
                    for mapper in object_mapper(obj).iterate_to_root():
                        mapper.extension.before_insert(mapper, connection, obj)
                else:
                    for mapper in object_mapper(obj).iterate_to_root():
                        mapper.extension.before_update(mapper, connection, obj)

        for obj in objects:
            # detect if we have a "pending" instance (i.e. has no instance_key attached to it),
            # and another instance with the same identity key already exists as persistent.  convert to an
            # UPDATE if so.
            mapper = object_mapper(obj)
            instance_key = mapper.instance_key(obj)
            is_row_switch = not postupdate and not has_identity(obj) and instance_key in uowtransaction.uow.identity_map
            if is_row_switch:
                existing = uowtransaction.uow.identity_map[instance_key]
                if not uowtransaction.is_deleted(existing):
                    raise exceptions.FlushError("New instance %s with identity key %s conflicts with persistent instance %s" % (mapperutil.instance_str(obj), str(instance_key), mapperutil.instance_str(existing)))
                if self.__should_log_debug:
                    self.__log_debug("detected row switch for identity %s.  will update %s, remove %s from transaction" % (instance_key, mapperutil.instance_str(obj), mapperutil.instance_str(existing)))
                uowtransaction.unregister_object(existing)
            if has_identity(obj):
                if obj._instance_key != instance_key:
                    raise exceptions.FlushError("Can't change the identity of instance %s in session (existing identity: %s; new identity: %s)" % (mapperutil.instance_str(obj), obj._instance_key, instance_key))

        inserted_objects = util.Set()
        updated_objects = util.Set()

        table_to_mapper = {}
        for mapper in self.base_mapper().polymorphic_iterator():
            for t in mapper.tables:
                table_to_mapper.setdefault(t, mapper)

        for table in sqlutil.TableCollection(list(table_to_mapper.keys())).sort(reverse=False):
            # two lists to store parameters for each table/object pair located
            insert = []
            update = []

            for obj in objects:
                mapper = object_mapper(obj)
                if table not in mapper.tables or not mapper._has_pks(table):
                    continue
                instance_key = mapper.instance_key(obj)
                if self.__should_log_debug:
                    self.__log_debug("save_obj() table '%s' instance %s identity %s" % (table.name, mapperutil.instance_str(obj), str(instance_key)))

                isinsert = not instance_key in uowtransaction.uow.identity_map and not postupdate and not has_identity(obj)
                params = {}
                hasdata = False
                for col in table.columns:
                    if col is mapper.version_id_col:
                        if not isinsert:
                            params[col._label] = mapper.get_attr_by_column(obj, col)
                            params[col.key] = params[col._label] + 1
                        else:
                            params[col.key] = 1
                    elif col in mapper.pks_by_table[table]:
                        # column is a primary key ?
                        if not isinsert:
                            # doing an UPDATE?  put primary key values as "WHERE" parameters
                            # matching the bindparam we are creating below, i.e. "<tablename>_<colname>"
                            params[col._label] = mapper.get_attr_by_column(obj, col)
                        else:
                            # doing an INSERT, primary key col ?
                            # if the primary key values are not populated,
                            # leave them out of the INSERT altogether, since PostGres doesn't want
                            # them to be present for SERIAL to take effect.  A SQLEngine that uses
                            # explicit sequences will put them back in if they are needed
                            value = mapper.get_attr_by_column(obj, col)
                            if value is not None:
                                params[col.key] = value
                    elif mapper.polymorphic_on is not None and mapper.polymorphic_on.shares_lineage(col):
                        if isinsert:
                            if self.__should_log_debug:
                                self.__log_debug("Using polymorphic identity '%s' for insert column '%s'" % (mapper.polymorphic_identity, col.key))
                            value = mapper.polymorphic_identity
                            if col.default is None or value is not None:
                                params[col.key] = value
                    else:
                        # column is not a primary key ?
                        if not isinsert:
                            # doing an UPDATE ? get the history for the attribute, with "passive"
                            # so as not to trigger any deferred loads.  if there is a new
                            # value, add it to the bind parameters
                            if post_update_cols is not None and col not in post_update_cols:
                                continue
                            elif is_row_switch:
                                params[col.key] = self.get_attr_by_column(obj, col)
                                hasdata = True
                                continue
                            prop = mapper._getpropbycolumn(col, False)
                            if prop is None:
                                continue
                            history = prop.get_history(obj, passive=True)
                            if history:
                                a = history.added_items()
                                if len(a):
                                    params[col.key] = a[0]
                                    hasdata = True
                        else:
                            # doing an INSERT, non primary key col ?
                            # add the attribute's value to the
                            # bind parameters, unless its None and the column has a
                            # default.  if its None and theres no default, we still might
                            # not want to put it in the col list but SQLIte doesnt seem to like that
                            # if theres no columns at all
                            value = mapper.get_attr_by_column(obj, col, False)
                            if value is NO_ATTRIBUTE:
                                continue
                            if col.default is None or value is not None:
                                params[col.key] = value

                if not isinsert:
                    if hasdata:
                        # if none of the attributes changed, dont even
                        # add the row to be updated.
                        update.append((obj, params, mapper))
                else:
                    insert.append((obj, params, mapper))

            if len(update):
                mapper = table_to_mapper[table]
                clause = sql.and_()
                for col in mapper.pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col._label, type=col.type, unique=True))
                if mapper.version_id_col is not None:
                    clause.clauses.append(mapper.version_id_col == sql.bindparam(mapper.version_id_col._label, type=col.type, unique=True))
                statement = table.update(clause)
                rows = 0
                supports_sane_rowcount = True
                def comparator(a, b):
                    for col in mapper.pks_by_table[table]:
                        x = cmp(a[1][col._label],b[1][col._label])
                        if x != 0:
                            return x
                    return 0
                update.sort(comparator)
                for rec in update:
                    (obj, params, mapper) = rec
                    c = connection.execute(statement, params)
                    mapper._postfetch(connection, table, obj, c, c.last_updated_params())

                    updated_objects.add(obj)
                    rows += c.rowcount

                if c.supports_sane_rowcount() and rows != len(update):
                    raise exceptions.ConcurrentModificationError("Updated rowcount %d does not match number of objects updated %d" % (rows, len(update)))

            if len(insert):
                statement = table.insert()
                def comparator(a, b):
                    return cmp(a[0]._sa_insert_order, b[0]._sa_insert_order)
                insert.sort(comparator)
                for rec in insert:
                    (obj, params, mapper) = rec
                    c = connection.execute(statement, params)
                    primary_key = c.last_inserted_ids()
                    if primary_key is not None:
                        i = 0
                        for col in mapper.pks_by_table[table]:
                            if mapper.get_attr_by_column(obj, col) is None and len(primary_key) > i:
                                mapper.set_attr_by_column(obj, col, primary_key[i])
                            i+=1
                    mapper._postfetch(connection, table, obj, c, c.last_inserted_params())

                    # synchronize newly inserted ids from one table to the next
                    # TODO: this fires off more than needed, try to organize syncrules
                    # per table
                    def sync(mapper):
                        inherit = mapper.inherits
                        if inherit is not None:
                            sync(inherit)
                        if mapper._synchronizer is not None:
                            mapper._synchronizer.execute(obj, obj)
                    sync(mapper)

                    inserted_objects.add(obj)
        if not postupdate:
            for obj in inserted_objects:
                for mapper in object_mapper(obj).iterate_to_root():
                    mapper.extension.after_insert(mapper, connection, obj)
            for obj in updated_objects:
                for mapper in object_mapper(obj).iterate_to_root():
                    mapper.extension.after_update(mapper, connection, obj)

    def _postfetch(self, connection, table, obj, resultproxy, params):
        """After an ``INSERT`` or ``UPDATE``, ask the returned result
        if ``PassiveDefaults`` fired off on the database side which
        need to be post-fetched, **or** if pre-exec defaults like
        ``ColumnDefaults`` were fired off and should be populated into
        the instance. this is only for non-primary key columns.
        """

        if resultproxy.lastrow_has_defaults():
            clause = sql.and_()
            for p in self.pks_by_table[table]:
                clause.clauses.append(p == self.get_attr_by_column(obj, p))
            row = connection.execute(table.select(clause), None).fetchone()
            for c in table.c:
                if self.get_attr_by_column(obj, c, False) is None:
                    self.set_attr_by_column(obj, c, row[c])
        else:
            for c in table.c:
                if c.primary_key or not c.key in params:
                    continue
                v = self.get_attr_by_column(obj, c, False)
                if v is NO_ATTRIBUTE:
                    continue
                elif v != params.get_original(c.key):
                    self.set_attr_by_column(obj, c, params.get_original(c.key))

    def delete_obj(self, objects, uowtransaction):
        """Issue ``DELETE`` statements for a list of objects.

        This is called within the context of a UOWTransaction during a
        flush operation.
        """

        if self.__should_log_debug:
            self.__log_debug("delete_obj() start")

        connection = uowtransaction.transaction.connection(self)

        for obj in objects:
            for mapper in object_mapper(obj).iterate_to_root():
                mapper.extension.before_delete(mapper, connection, obj)
        
        deleted_objects = util.Set()
        table_to_mapper = {}
        for mapper in self.base_mapper().polymorphic_iterator():
            for t in mapper.tables:
                table_to_mapper.setdefault(t, mapper)

        for table in sqlutil.TableCollection(list(table_to_mapper.keys())).sort(reverse=True):
            delete = []
            for obj in objects:
                mapper = object_mapper(obj)
                if table not in mapper.tables or not mapper._has_pks(table):
                    continue

                params = {}
                if not hasattr(obj, '_instance_key'):
                    continue
                else:
                    delete.append(params)
                for col in mapper.pks_by_table[table]:
                    params[col.key] = mapper.get_attr_by_column(obj, col)
                if mapper.version_id_col is not None:
                    params[mapper.version_id_col.key] = mapper.get_attr_by_column(obj, mapper.version_id_col)
                deleted_objects.add(obj)
            if len(delete):
                mapper = table_to_mapper[table]
                def comparator(a, b):
                    for col in mapper.pks_by_table[table]:
                        x = cmp(a[col.key],b[col.key])
                        if x != 0:
                            return x
                    return 0
                delete.sort(comparator)
                clause = sql.and_()
                for col in mapper.pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col.key, type=col.type, unique=True))
                if mapper.version_id_col is not None:
                    clause.clauses.append(mapper.version_id_col == sql.bindparam(mapper.version_id_col.key, type=mapper.version_id_col.type, unique=True))
                statement = table.delete(clause)
                c = connection.execute(statement, delete)
                if c.supports_sane_rowcount() and c.rowcount != len(delete):
                    raise exceptions.ConcurrentModificationError("Updated rowcount %d does not match number of objects updated %d" % (c.rowcount, len(delete)))

        for obj in deleted_objects:
            for mapper in object_mapper(obj).iterate_to_root():
                mapper.extension.after_delete(mapper, connection, obj)

    def _has_pks(self, table):
        try:
            pk = self.pks_by_table[table]
            if not pk:
                return False
            for k in pk:
                if not self.columntoproperty.has_key(k):
                    return False
            else:
                return True
        except KeyError:
            return False

    def register_dependencies(self, uowcommit, *args, **kwargs):
        """Register ``DependencyProcessor`` instances with a
        ``unitofwork.UOWTransaction``.

        This call `register_dependencies` on all attached
        ``MapperProperty`` instances.
        """

        for prop in self.__props.values():
            prop.register_dependencies(uowcommit, *args, **kwargs)

    def cascade_iterator(self, type, object, recursive=None, halt_on=None):
        """Iterate each element in an object graph, for all relations
        taht meet the given cascade rule.

        type
          The name of the cascade rule (i.e. save-update, delete,
          etc.)

        object
          The lead object instance.  child items will be processed per
          the relations defined for this object's mapper.

        recursive
          Used by the function for internal context during recursive
          calls, leave as None.
        """

        if recursive is None:
            recursive=util.Set()
        for prop in self.__props.values():
            for c in prop.cascade_iterator(type, object, recursive, halt_on=halt_on):
                yield c

    def cascade_callable(self, type, object, callable_, recursive=None, halt_on=None):
        """Execute a callable for each element in an object graph, for
        all relations that meet the given cascade rule.

        type
          The name of the cascade rule (i.e. save-update, delete, etc.)

        object
          The lead object instance.  child items will be processed per
          the relations defined for this object's mapper.

        callable\_
          The callable function.

        recursive
          Used by the function for internal context during recursive
          calls, leave as None.
          
        """

        if recursive is None:
            recursive=util.Set()
        for prop in self.__props.values():
            prop.cascade_callable(type, object, callable_, recursive, halt_on=halt_on)

    def get_select_mapper(self):
        """Return the mapper used for issuing selects.

        This mapper is the same mapper as `self` unless the
        select_table argument was specified for this mapper.
        """

        return self.__surrogate_mapper or self

    def _instance(self, context, row, result = None, skip_polymorphic=False):
        """Pull an object instance from the given row and append it to
        the given result list.

        If the instance already exists in the given identity map, its
        not added.  In either case, execute all the property loaders
        on the instance to also process extra information in the row.
        """

        # apply ExtensionOptions applied to the Query to this mapper,
        # but only if our mapper matches.
        # TODO: what if our mapper inherits from the mapper (i.e. as in a polymorphic load?)
        if context.mapper is self:
            extension = context.extension
        else:
            extension = self.extension

        ret = extension.translate_row(self, context, row)
        if ret is not EXT_PASS:
            row = ret

        if not skip_polymorphic and self.polymorphic_on is not None:
            discriminator = row[self.polymorphic_on]
            if discriminator is not None:
                mapper = self.polymorphic_map[discriminator]
                if mapper is not self:
                    row = self.translate_row(mapper, row)
                    return mapper._instance(context, row, result=result, skip_polymorphic=True)

        # look in main identity map.  if its there, we dont do anything to it,
        # including modifying any of its related items lists, as its already
        # been exposed to being modified by the application.

        populate_existing = context.populate_existing or self.always_refresh
        identitykey = self.identity_key_from_row(row)
        if context.session.has_key(identitykey):
            instance = context.session._get(identitykey)
            if self.__should_log_debug:
                self.__log_debug("_instance(): using existing instance %s identity %s" % (mapperutil.instance_str(instance), str(identitykey)))
            isnew = False
            if context.version_check and self.version_id_col is not None and self.get_attr_by_column(instance, self.version_id_col) != row[self.version_id_col]:
                raise exceptions.ConcurrentModificationError("Instance '%s' version of %s does not match %s" % (instance, self.get_attr_by_column(instance, self.version_id_col), row[self.version_id_col]))

            if populate_existing or context.session.is_expired(instance, unexpire=True):
                if not context.identity_map.has_key(identitykey):
                    context.identity_map[identitykey] = instance
                    isnew = True
                if extension.populate_instance(self, context, row, instance, identitykey, isnew) is EXT_PASS:
                    self.populate_instance(context, instance, row, identitykey, isnew)
            if extension.append_result(self, context, row, instance, identitykey, result, isnew) is EXT_PASS:
                if result is not None:
                    result.append(instance)
            return instance
        else:
            if self.__should_log_debug:
                self.__log_debug("_instance(): identity key %s not in session" % str(identitykey) + repr([mapperutil.instance_str(x) for x in context.session]))
        # look in result-local identitymap for it.
        exists = context.identity_map.has_key(identitykey)
        if not exists:
            if self.allow_null_pks:
                # check if *all* primary key cols in the result are None - this indicates
                # an instance of the object is not present in the row.
                for col in self.pks_by_table[self.mapped_table]:
                    if row[col] is not None:
                        break
                else:
                    return None
            else:
                # otherwise, check if *any* primary key cols in the result are None - this indicates
                # an instance of the object is not present in the row.
                for col in self.pks_by_table[self.mapped_table]:
                    if row[col] is None:
                        return None

            # plugin point
            instance = extension.create_instance(self, context, row, self.class_)
            if instance is EXT_PASS:
                instance = self._create_instance(context.session)
            else:
                instance._entity_name = self.entity_name
            if self.__should_log_debug:
                self.__log_debug("_instance(): created new instance %s identity %s" % (mapperutil.instance_str(instance), str(identitykey)))
            context.identity_map[identitykey] = instance
            isnew = True
        else:
            instance = context.identity_map[identitykey]
            isnew = False

        # call further mapper properties on the row, to pull further
        # instances from the row and possibly populate this item.
        if extension.populate_instance(self, context, row, instance, identitykey, isnew) is EXT_PASS:
            self.populate_instance(context, instance, row, identitykey, isnew)
        if extension.append_result(self, context, row, instance, identitykey, result, isnew) is EXT_PASS:
            if result is not None:
                result.append(instance)
        return instance

    def _create_instance(self, session):
        obj = self.class_.__new__(self.class_)
        obj._entity_name = self.entity_name

        # this gets the AttributeManager to do some pre-initialization,
        # in order to save on KeyErrors later on
        attribute_manager.init_attr(obj)

        return obj

    def translate_row(self, tomapper, row):
        """Translate the column keys of a row into a new or proxied
        row that can be understood by another mapper.

        This can be used in conjunction with populate_instance to
        populate an instance using an alternate mapper.
        """

        newrow = util.DictDecorator(row)
        for c in tomapper.mapped_table.c:
            c2 = self.mapped_table.corresponding_column(c, keys_ok=True, raiseerr=True)
            if row.has_key(c2):
                newrow[c] = row[c2]
        return newrow

    def populate_instance(self, selectcontext, instance, row, identitykey, isnew):
        """populate an instance from a result row.

        This method iterates through the list of MapperProperty objects attached to this Mapper
        and calls each properties execute() method."""
        for prop in self.__props.values():
            prop.execute(selectcontext, instance, row, identitykey, isnew)

Mapper.logger = logging.class_logger(Mapper)


class MapperExtension(object):
    """Base implementation for an object that provides overriding
    behavior to various Mapper functions.  For each method in
    MapperExtension, a result of EXT_PASS indicates the functionality
    is not overridden.
    """

    def get_session(self):
        """Retrieve a contextual Session instance with which to
        register a new object.

        Note: this is not called if a session is provided with the
        `__init__` params (i.e. `_sa_session`).
        """

        return EXT_PASS

    def load(self, query, *args, **kwargs):
        """Override the `load` method of the Query object.

        The return value of this method is used as the result of
        ``query.load()`` if the value is anything other than EXT_PASS.
        """

        return EXT_PASS

    def get(self, query, *args, **kwargs):
        """Override the `get` method of the Query object.

        The return value of this method is used as the result of
        ``query.get()`` if the value is anything other than EXT_PASS.
        """

        return EXT_PASS

    def get_by(self, query, *args, **kwargs):
        """Override the `get_by` method of the Query object.

        The return value of this method is used as the result of
        ``query.get_by()`` if the value is anything other than
        EXT_PASS.
        """

        return EXT_PASS

    def select_by(self, query, *args, **kwargs):
        """Override the `select_by` method of the Query object.

        The return value of this method is used as the result of
        ``query.select_by()`` if the value is anything other than
        EXT_PASS.
        """

        return EXT_PASS

    def select(self, query, *args, **kwargs):
        """Override the `select` method of the Query object.

        The return value of this method is used as the result of
        ``query.select()`` if the value is anything other than
        EXT_PASS.
        """

        return EXT_PASS


    def translate_row(self, mapper, context, row):
        """Perform pre-processing on the given result row and return a
        new row instance.

        This is called as the very first step in the ``_instance()``
        method.
        """

        return EXT_PASS

    def create_instance(self, mapper, selectcontext, row, class_):
        """Receive a row when a new object instance is about to be
        created from that row.

        The method can choose to create the instance itself, or it can
        return None to indicate normal object creation should take
        place.

        mapper
          The mapper doing the operation

        selectcontext
          SelectionContext corresponding to the instances() call

        row
          The result row from the database

        class\_
          The class we are mapping.
        """

        return EXT_PASS

    def append_result(self, mapper, selectcontext, row, instance, identitykey, result, isnew):
        """Receive an object instance before that instance is appended
        to a result list.

        If this method returns EXT_PASS, result appending will proceed
        normally.  if this method returns any other value or None,
        result appending will not proceed for this instance, giving
        this extension an opportunity to do the appending itself, if
        desired.

        mapper
          The mapper doing the operation.

        selectcontext
          SelectionContext corresponding to the instances() call.

        row
          The result row from the database.

        instance
          The object instance to be appended to the result.

        identitykey
          The identity key of the instance.

        result
          List to which results are being appended.

        isnew
          Indicates if this is the first time we have seen this object
          instance in the current result set.  if you are selecting
          from a join, such as an eager load, you might see the same
          object instance many times in the same result set.
        """

        return EXT_PASS

    def populate_instance(self, mapper, selectcontext, row, instance, identitykey, isnew):
        """Receive a newly-created instance before that instance has
        its attributes populated.

        The normal population of attributes is according to each
        attribute's corresponding MapperProperty (which includes
        column-based attributes as well as relationships to other
        classes).  If this method returns EXT_PASS, instance
        population will proceed normally.  If any other value or None
        is returned, instance population will not proceed, giving this
        extension an opportunity to populate the instance itself, if
        desired.
        """

        return EXT_PASS

    def before_insert(self, mapper, connection, instance):
        """Receive an object instance before that instance is INSERTed
        into its table.

        This is a good place to set up primary key values and such
        that aren't handled otherwise.
        """

        return EXT_PASS

    def before_update(self, mapper, connection, instance):
        """Receive an object instance before that instance is UPDATEed."""

        return EXT_PASS

    def after_update(self, mapper, connection, instance):
        """Receive an object instance after that instance is UPDATEed."""

        return EXT_PASS

    def after_insert(self, mapper, connection, instance):
        """Receive an object instance after that instance is INSERTed."""

        return EXT_PASS

    def before_delete(self, mapper, connection, instance):
        """Receive an object instance before that instance is DELETEed."""

        return EXT_PASS

    def after_delete(self, mapper, connection, instance):
        """Receive an object instance after that instance is DELETEed."""

        return EXT_PASS

class _ExtensionCarrier(MapperExtension):
    def __init__(self):
        self.__elements = []

    def __iter__(self):
        return iter(self.__elements)
        
    def insert(self, extension):
        """Insert a MapperExtension at the beginning of this ExtensionCarrier's list."""

        self.__elements.insert(0, extension)

    def append(self, extension):
        """Append a MapperExtension at the end of this ExtensionCarrier's list."""

        self.__elements.append(extension)

    def get_session(self, *args, **kwargs):
        return self._do('get_session', *args, **kwargs)

    def load(self, *args, **kwargs):
        return self._do('load', *args, **kwargs)

    def get(self, *args, **kwargs):
        return self._do('get', *args, **kwargs)

    def get_by(self, *args, **kwargs):
        return self._do('get_by', *args, **kwargs)

    def select_by(self, *args, **kwargs):
        return self._do('select_by', *args, **kwargs)

    def select(self, *args, **kwargs):
        return self._do('select', *args, **kwargs)

    def translate_row(self, *args, **kwargs):
        return self._do('translate_row', *args, **kwargs)

    def create_instance(self, *args, **kwargs):
        return self._do('create_instance', *args, **kwargs)

    def append_result(self, *args, **kwargs):
        return self._do('append_result', *args, **kwargs)

    def populate_instance(self, *args, **kwargs):
        return self._do('populate_instance', *args, **kwargs)

    def before_insert(self, *args, **kwargs):
        return self._do('before_insert', *args, **kwargs)

    def before_update(self, *args, **kwargs):
        return self._do('before_update', *args, **kwargs)

    def after_update(self, *args, **kwargs):
        return self._do('after_update', *args, **kwargs)

    def after_insert(self, *args, **kwargs):
        return self._do('after_insert', *args, **kwargs)

    def before_delete(self, *args, **kwargs):
        return self._do('before_delete', *args, **kwargs)

    def after_delete(self, *args, **kwargs):
        return self._do('after_delete', *args, **kwargs)

    def _do(self, funcname, *args, **kwargs):
        for elem in self.__elements:
            ret = getattr(elem, funcname)(*args, **kwargs)
            if ret is not EXT_PASS:
                return ret
        else:
            return EXT_PASS

class ExtensionOption(MapperOption):
    def __init__(self, ext):
        self.ext = ext

    def process_query(self, query):
        query.extension.append(self.ext)

class ClassKey(object):
    """Key a class and an entity name to a mapper, via the mapper_registry."""

    __metaclass__ = util.ArgSingleton

    def __init__(self, class_, entity_name):
        self.class_ = class_
        self.entity_name = entity_name

    def __hash__(self):
        return hash((self.class_, self.entity_name))

    def __eq__(self, other):
        return self is other

    def __repr__(self):
        return "ClassKey(%s, %s)" % (repr(self.class_), repr(self.entity_name))

    def dispose(self):
        type(self).dispose_static(self.class_, self.entity_name)

def has_identity(object):
    return hasattr(object, '_instance_key')

def has_mapper(object):
    """Return True if the given object has had a mapper association
    set up, either through loading, or via insertion in a session.
    """

    return hasattr(object, '_entity_name')

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
        mapper = mapper_registry[ClassKey(object.__class__, getattr(object, '_entity_name', entity_name))]
    except (KeyError, AttributeError):
        if raiseerror:
            raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (object.__class__.__name__, getattr(object, '_entity_name', entity_name)))
        else:
            return None
    return mapper.compile()

def class_mapper(class_, entity_name=None, compile=True):
    """Given a class and optional entity_name, return the primary Mapper associated with the key.
    
    If no mapper can be located, raises ``InvalidRequestError``.
    """

    try:
        mapper = mapper_registry[ClassKey(class_, entity_name)]
    except (KeyError, AttributeError):
        raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (class_.__name__, entity_name))
    if compile:
        return mapper.compile()
    else:
        return mapper
