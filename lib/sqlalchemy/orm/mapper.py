# orm/mapper.py
# Copyright (C) 2005,2006 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import sql, schema, util, exceptions
from sqlalchemy import sql_util as sqlutil
import util as mapperutil
import sync
import query as querylib
import session as sessionlib
import weakref

__all__ = ['Mapper', 'MapperExtension', 'class_mapper', 'object_mapper', 'SynonymProperty', 'EXT_PASS']

# a dictionary mapping classes to their primary mappers
mapper_registry = weakref.WeakKeyDictionary()

# a list of MapperExtensions that will be installed in all mappers by default
global_extensions = []

# a constant returned by _getattrbycolumn to indicate
# this mapper is not handling an attribute for a particular
# column
NO_ATTRIBUTE = object()

# returned by a MapperExtension method to indicate a "do nothing" response
EXT_PASS = object()

                
class Mapper(object):
    """Persists object instances to and from schema.Table objects via the sql package.
    Instances of this class should be constructed through this package's mapper() or
    relation() function."""
    def __init__(self, 
                class_, 
                local_table, 
                properties = None, 
                primary_key = None, 
                is_primary = False, 
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
                polymorphic_map=None,
                polymorphic_identity=None,
                concrete=False,
                select_table=None):

        if not issubclass(class_, object):
            raise exceptions.ArgumentError("Class '%s' is not a new-style class" % class_.__name__)

        for table in (local_table, select_table):
            if table is not None and isinstance(table, sql.SelectBaseMixin):
                # some db's, noteably postgres, dont want to select from a select
                # without an alias.  also if we make our own alias internally, then
                # the configured properties on the mapper are not matched against the alias 
                # we make, theres workarounds but it starts to get really crazy (its crazy enough
                # the SQL that gets generated) so just require an alias
                raise exceptions.ArgumentError("Mapping against a Select object requires that it has a name.  Use an alias to give it a name, i.e. s = select(...).alias('myselect')")

        self.class_ = class_
        self.entity_name = entity_name
        self.class_key = ClassKey(class_, entity_name)
        self.is_primary = is_primary
        self.primary_key = primary_key
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
        
        # a Column which is used during a select operation to retrieve the 
        # "polymorphic identity" of the row, which indicates which Mapper should be used
        # to construct a new object instance from that row.
        self.polymorphic_on = polymorphic_on
        
        # our 'polymorphic identity', a string name that when located in a result set row
        # indicates this Mapper should be used to construct the object instance for that row.
        self.polymorphic_identity = polymorphic_identity
        
        # a dictionary of 'polymorphic identity' names, associating those names with
        # Mappers that will be used to construct object instances upon a select operation.
        if polymorphic_map is None:
            self.polymorphic_map = {}
        else:
            self.polymorphic_map = polymorphic_map

        class LOrderedProp(util.OrderedProperties):
            """this extends OrderedProperties to trigger a compile() before the
            members of the object are accessed."""
            def _get_data(s):
                self.compile()
                return s.__dict__['_OrderedProperties__data']
            _OrderedProperties__data = property(_get_data)
                
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
        
        # uncomment to compile at construction time (the old way)
        # this will break mapper setups that arent declared in the order
        # of dependency
        #self.compile()
    
    def _get_props(self):
        self.compile()
        return self.__props
    props = property(_get_props, doc="compiles this mapper if needed, and returns the \
    dictionary of MapperProperty objects associated with this mapper.")
        
    def compile(self):
        """compile this mapper into its final internal format.
        
        this is the 'external' version of the method which is not reentrant."""
        if self.__is_compiled:
            return self
            
        # compile all primary mappers
        for mapper in mapper_registry.values():
            if not mapper.__is_compiled:
                mapper._do_compile()
                
        # initialize properties on all mappers
        for mapper in mapper_registry.values():
            if not mapper.__props_init:
                mapper._initialize_properties()
        
        # if we're not primary, compile us
        if self.non_primary:
            self._do_compile()
            self._initialize_properties()
                
        return self
    
    def _check_compile(self):
        if self.non_primary:
            self._do_compile()
            self._initialize_properties()
        return self
        
    def _do_compile(self):
        """compile this mapper into its final internal format.  
        
        this is the 'internal' version of the method which is assumed to be called within compile()
        and is reentrant.
        """
        if self.__is_compiled:
            return self
        #print "COMPILING!", self.class_key, "non primary: ", self.non_primary
        self.__is_compiled = True
        self.__props_init = False
        self._compile_extensions()
        self._compile_inheritance()
        self._compile_tables()
        self._compile_properties()
        self._compile_selectable()
#        self._initialize_properties()

        return self
        
    def _compile_extensions(self):
        """goes through the global_extensions list as well as the list of MapperExtensions
        specified for this Mapper and creates a linked list of those extensions."""
        # uber-pendantic style of making mapper chain, as various testbase/
        # threadlocal/assignmapper combinations keep putting dupes etc. in the list
        # TODO: do something that isnt 21 lines....

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

        self.extension = ExtensionCarrier()
        for ext in extlist:
            self.extension.elements.append(ext)
        
    def _compile_inheritance(self):
        """determines if this Mapper inherits from another mapper, and if so calculates the mapped_table
        for this Mapper taking the inherited mapper into account.  for joined table inheritance, creates
        a SyncRule that will synchronize column values between the joined tables. also initializes polymorphic variables
        used in polymorphic loads."""
        if self.inherits is not None:
            if isinstance(self.inherits, type):
                self.inherits = class_mapper(self.inherits, compile=False)._do_compile()
            else:
                self.inherits = self.inherits._do_compile()
            if self.class_.__mro__[1] != self.inherits.class_:
                raise exceptions.ArgumentError("Class '%s' does not inherit from '%s'" % (self.class_.__name__, self.inherits.class_.__name__))
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
                    self._synchronizer.compile(self.mapped_table.onclause, util.Set([self.inherits.local_table]), sqlutil.TableFinder(self.local_table))
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
        else:
            self._synchronizer = None
            self.mapped_table = self.local_table
            if self.polymorphic_identity is not None:
                self._add_polymorphic_mapping(self.polymorphic_identity, self)
                
        # convert polymorphic class associations to mappers
        for key in self.polymorphic_map.keys():
            if isinstance(self.polymorphic_map[key], type):
                self.polymorphic_map[key] = class_mapper(self.polymorphic_map[key])

    def _add_polymorphic_mapping(self, key, class_or_mapper, entity_name=None):
        """adds a Mapper to our 'polymorphic map' """
        if isinstance(class_or_mapper, type):
            class_or_mapper = class_mapper(class_or_mapper, entity_name=entity_name)
        self.polymorphic_map[key] = class_or_mapper

    def _compile_tables(self):
        """after the inheritance relationships have been reconciled, sets up some more table-based instance
        variables and determines the "primary key" columns for all tables represented by this Mapper."""

        # summary of the various Selectable units:
        # mapped_table - the Selectable that represents a join of the underlying Tables to be saved (or just the Table)
        # local_table - the Selectable that was passed to this Mapper's constructor, if any
        # select_table - the Selectable that will be used during queries.  if this is specified
        # as a constructor keyword argument, it takes precendence over mapped_table, otherwise its mapped_table
        # unjoined_table - our Selectable, minus any joins constructed against the inherits table.
        # this is either select_table if it was given explicitly, or in the case of a mapper that inherits
        # its local_table
        # tables - a collection of underlying Table objects pulled from mapped_table

        if self.select_table is None:
            self.select_table = self.mapped_table
        self.unjoined_table = self.local_table

        # locate all tables contained within the "table" passed in, which
        # may be a join or other construct
        self.tables = sqlutil.TableFinder(self.mapped_table)

        # determine primary key columns, either passed in, or get them from our set of tables
        self.pks_by_table = {}
        if self.primary_key is not None:
            # determine primary keys using user-given list of primary key columns as a guide
            #
            # TODO: this might not work very well for joined-table and/or polymorphic 
            # inheritance mappers since local_table isnt taken into account nor is select_table
            # need to test custom primary key columns used with inheriting mappers
            for k in self.primary_key:
                self.pks_by_table.setdefault(k.table, util.OrderedSet()).add(k)
                if k.table != self.mapped_table:
                    # associate pk cols from subtables to the "main" table
                    self.pks_by_table.setdefault(self.mapped_table, util.OrderedSet()).add(k)
        else:
            # no user-defined primary key columns - go through all of our represented tables
            # and assemble primary key columns
            for t in self.tables + [self.mapped_table]:
                try:
                    l = self.pks_by_table[t]
                except KeyError:
                    l = self.pks_by_table.setdefault(t, util.OrderedSet())
                for k in t.primary_key:
                    l.add(k)

        if len(self.pks_by_table[self.mapped_table]) == 0:
            raise exceptions.ArgumentError("Could not assemble any primary key columns for mapped table '%s'" % (self.mapped_table.name))


    def _compile_properties(self):
        """inspects the properties dictionary sent to the Mapper's constructor as well as the mapped_table, and creates
        MapperProperty objects corresponding to each mapped column and relation.  also grabs MapperProperties from the 
        inherited mapper, if any, and creates copies of them to attach to this Mapper."""
        # object attribute names mapped to MapperProperty objects
        self.__props = {}

        # table columns mapped to lists of MapperProperty objects
        # using a list allows a single column to be defined as 
        # populating multiple object attributes
        self.columntoproperty = TranslatingDict(self.mapped_table)

        # load custom properties 
        if self.properties is not None:
            for key, prop in self.properties.iteritems():
                self._compile_property(key, prop, False)

        if self.inherits is not None:
            # transfer properties from the inherited mapper to here.
            # this includes column properties as well as relations.
            # the column properties will attempt to be translated from the selectable unit
            # of the parent mapper to this mapper's selectable unit.
            self.inherits._inheriting_mappers.add(self)
            for key, prop in self.inherits.__props.iteritems():
                if not self.__props.has_key(key):
                    prop.adapt_to_inherited(key, self)

        # load properties from the main table object,
        # not overriding those set up in the 'properties' argument
        for column in self.mapped_table.columns:
            if self.columntoproperty.has_key(column):
                continue
            if not self.columns.has_key(column.key):
                self.columns[column.key] = self.select_table.corresponding_column(column, keys_ok=True, raiseerr=True)

            prop = self.__props.get(column.key, None)
            if prop is None:
                prop = ColumnProperty(column)
                self.__props[column.key] = prop
                prop.set_parent(self)
            elif isinstance(prop, ColumnProperty):
                prop.columns.append(column)
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
        """calls the init() method on all MapperProperties attached to this mapper.  this will incur the
        compilation of related mappers."""
        l = [(key, prop) for key, prop in self.__props.iteritems()]
        for key, prop in l:
            if getattr(prop, 'key', None) is None:
                prop.init(key, self)
        self.__props_init = True
        
    def _compile_selectable(self):
        """if the 'select_table' keyword argument was specified, 
        set up a second "surrogate mapper" that will be used for select operations.
        the columns of select_table should encompass all the columns of the mapped_table either directly
        or through proxying relationships."""
        if self.select_table is not self.mapped_table:
            props = {}
            if self.properties is not None:
                for key, prop in self.properties.iteritems():
                    if sql.is_column(prop):
                        props[key] = self.select_table.corresponding_column(prop)
                    elif (isinstance(prop, list) and sql.is_column(prop[0])):
                        props[key] = [self.select_table.corresponding_column(c) for c in prop]
            self.__surrogate_mapper = Mapper(self.class_, self.select_table, non_primary=True, properties=props, polymorphic_map=self.polymorphic_map, polymorphic_on=self.select_table.corresponding_column(self.polymorphic_on))

    def _compile_class(self):
        """if this mapper is to be a primary mapper (i.e. the non_primary flag is not set),
        associate this Mapper with the given class_ and entity name.  subsequent
        calls to class_mapper() for the class_/entity name combination will return this 
        mapper.  also decorates the __init__ method on the mapped class to include auto-session attachment logic."""
        if self.non_primary:
            return
        
        if not self.non_primary and (mapper_registry.has_key(self.class_key) and not self.is_primary):
             raise exceptions.ArgumentError("Class '%s' already has a primary mapper defined.  Use is_primary=True to assign a new primary mapper to the class, or use non_primary=True to create a non primary Mapper" % self.class_)

        sessionlib.attribute_manager.reset_class_managed(self.class_)
    
        oldinit = self.class_.__init__
        def init(self, *args, **kwargs):
            entity_name = kwargs.pop('_sa_entity_name', None)
            mapper = mapper_registry.get(ClassKey(self.__class__, entity_name))
            if mapper is not None:
                mapper = mapper.compile()

                # this gets the AttributeManager to do some pre-initialization,
                # in order to save on KeyErrors later on
                sessionlib.attribute_manager.init_attr(self)

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
                session._register_new(self)
                
            if oldinit is not None:
                try:
                    oldinit(self, *args, **kwargs)
                except:
                    if session is not None:
                        session.expunge(self)
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
        mapper_registry[self.class_key] = self
        if self.entity_name is None:
            self.class_.c = self.c
            
    def base_mapper(self):
        """return the ultimate base mapper in an inheritance chain"""
        if self.inherits is not None:
            return self.inherits.base_mapper()
        else:
            return self
    
    def isa(self, other):
        """return True if the given mapper inherits from this mapper"""
        m = other
        while m is not self and m.inherits is not None:
            m = m.inherits
        return m is self
            
    def add_properties(self, dict_of_properties):
        """adds the given dictionary of properties to this mapper, using add_property."""
        for key, value in dict_of_properties.iteritems():
            self.add_property(key, value)

    def add_property(self, key, prop):
        """adds an indiviual MapperProperty to this mapper.  If the mapper has not been compiled yet,
        just adds the property to the initial properties dictionary sent to the constructor.  if this Mapper
        has already been compiled, then the given MapperProperty is compiled immediately."""
        self.properties[key] = prop
        if self.__is_compiled:
            self._compile_property(key, prop, init=True)
            
    def _create_prop_from_column(self, column, skipmissing=False):
        if sql.is_column(column):
            try:
                column = self.mapped_table.corresponding_column(column)
            except KeyError:
                if skipmissing:
                    return
                raise exceptions.ArgumentError("Column '%s' is not represented in mapper's table" % prop._label)
            return ColumnProperty(column)
        elif isinstance(column, list) and sql.is_column(column[0]):
            try:
                column = [self.mapped_table.corresponding_column(c) for c in column]
            except KeyError, e:
                # TODO: want to take the columns we have from this
                if skipmissing:
                    return
                raise exceptions.ArgumentError("Column '%s' is not represented in mapper's table" % e.args[0])
            return ColumnProperty(*column)
        else:
            return None

    def _compile_property(self, key, prop, init=True, skipmissing=False):
        """adds an additional property to this mapper.  this is the same as if it were 
        specified within the 'properties' argument to the constructor.  if the named
        property already exists, this will replace it.  Useful for
        circular relationships, or overriding the parameters of auto-generated properties
        such as backreferences."""

        if not isinstance(prop, MapperProperty):
            prop = self._create_prop_from_column(prop, skipmissing=skipmissing)
            if prop is None:
                raise exceptions.ArgumentError("'%s' is not an instance of MapperProperty or Column" % repr(prop))

        self.__props[key] = prop
        prop.set_parent(self)
        
        if isinstance(prop, ColumnProperty):
            col = self.select_table.corresponding_column(prop.columns[0], keys_ok=True, raiseerr=False)
            if col is None:
                col = prop.columns[0]
            self.columns[key] = col
            for col in prop.columns:
                proplist = self.columntoproperty.setdefault(col, [])
                proplist.append(prop)

        if init:
            prop.init(key, self)

        for mapper in self._inheriting_mappers:
            prop.adapt_to_inherited(key, mapper)
        
    def __str__(self):
        return "Mapper|" + self.class_.__name__ + "|" + (self.entity_name is not None and "/%s" % self.entity_name or "") + str(self.local_table)
    
    def _is_primary_mapper(self):
        """returns True if this mapper is the primary mapper for its class key (class + entity_name)"""
        return mapper_registry.get(self.class_key, None) is self

    def primary_mapper(self):
        """returns the primary mapper corresponding to this mapper's class key (class + entity_name)"""
        return mapper_registry[self.class_key]

    def is_assigned(self, instance):
        """returns True if this mapper handles the given instance.  this is dependent
        not only on class assignment but the optional "entity_name" parameter as well."""
        return instance.__class__ is self.class_ and getattr(instance, '_entity_name', None) == self.entity_name

    def _assign_entity_name(self, instance):
        """assigns this Mapper's entity name to the given instance.  subsequent Mapper lookups for this
        instance will return the primary mapper corresponding to this Mapper's class and entity name."""
        instance._entity_name = self.entity_name
        
    def get_session(self):
        """returns the contextual session provided by the mapper extension chain
        
        raises InvalidRequestError if a session cannot be retrieved from the
        extension chain
        """
        self.compile()
        s = self.extension.get_session()
        if s is EXT_PASS:
            raise exceptions.InvalidRequestError("No contextual Session is established.  Use a MapperExtension that implements get_session or use 'import sqlalchemy.mods.threadlocal' to establish a default thread-local contextual session.")
        return s
    
    def has_eager(self):
        """returns True if one of the properties attached to this Mapper is eager loading"""
        return getattr(self, '_has_eager', False)
        
    
    def instances(self, cursor, session, *mappers, **kwargs):
        """given a cursor (ResultProxy) from an SQLEngine, returns a list of object instances
        corresponding to the rows in the cursor."""
        limit = kwargs.get('limit', None)
        offset = kwargs.get('offset', None)
        populate_existing = kwargs.get('populate_existing', False)
        
        result = util.UniqueAppender([])
        if mappers:
            otherresults = []
            for m in mappers:
                otherresults.append(util.UniqueAppender([]))
                
        imap = {}
        scratch = {}
        imap['_scratch'] = scratch
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            self._instance(session, row, imap, result, populate_existing=populate_existing)
            i = 0
            for m in mappers:
                m._instance(session, row, imap, otherresults[i])
                i+=1
                
        # store new stuff in the identity map
        for value in imap.values():
            if value is scratch:
                continue
            session._register_clean(value)
            
        if mappers:
            return [result.data] + [o.data for o in otherresults]
        else:
            return result.data
        
    def identity_key(self, primary_key):
        """returns the instance key for the given identity value.  this is a global tracking object used by the Session, and is usually available off a mapped object as instance._instance_key."""
        return sessionlib.get_id_key(util.to_list(primary_key), self.class_, self.entity_name)

    def instance_key(self, instance):
        """returns the instance key for the given instance.  this is a global tracking object used by the Session, and is usually available off a mapped object as instance._instance_key."""
        return self.identity_key(self.identity(instance))

    def identity(self, instance):
        """returns the identity (list of primary key values) for the given instance.  The list of values can be fed directly into the get() method as mapper.get(*key)."""
        return [self._getattrbycolumn(instance, column) for column in self.pks_by_table[self.mapped_table]]
        

    def copy(self, **kwargs):
        mapper = Mapper.__new__(Mapper)
        mapper.__dict__.update(self.__dict__)
        mapper.__dict__.update(kwargs)
        mapper.__props = self.__props.copy()
        mapper._inheriting_mappers = []
        for m in self._inheriting_mappers:
            mapper._inheriting_mappers.append(m.copy())
        return mapper

    def options(self, *options, **kwargs):
        """uses this mapper as a prototype for a new mapper with different behavior.
        *options is a list of options directives, which include eagerload(), lazyload(), and noload()"""
        self.compile()
        optkey = repr([hash_key(o) for o in options])
        try:
            return self._options[optkey]
        except KeyError:
            mapper = self.copy(**kwargs)
            for option in options:
                option.process(mapper)
            self._options[optkey] = mapper
            return mapper

            
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
        
    def _getattrbycolumn(self, obj, column, raiseerror=True):
        prop = self._getpropbycolumn(column, raiseerror)
        if prop is None:
            return NO_ATTRIBUTE
        return prop.getattr(obj)

    def _setattrbycolumn(self, obj, column, value):
        self.columntoproperty[column][0].setattr(obj, value)
            
    def save_obj(self, objects, uow, postupdate=False):
        """called by a UnitOfWork object to save objects, which involves either an INSERT or
        an UPDATE statement for each table used by this mapper, for each element of the
        list."""
        #print "SAVE_OBJ MAPPER", self.class_.__name__, objects
        connection = uow.transaction.connection(self)

        if not postupdate:
            for obj in objects:
                if not hasattr(obj, "_instance_key"):
                    self.extension.before_insert(self, connection, obj)
                else:
                    self.extension.before_update(self, connection, obj)

        inserted_objects = util.Set()
        updated_objects = util.Set()
        for table in self.tables.sort(reverse=False):
            #print "SAVE_OBJ table ", self.class_.__name__, table.name
            # looping through our set of tables, which are all "real" tables, as opposed
            # to our main table which might be a select statement or something non-writeable
            
            # the loop structure is tables on the outer loop, objects on the inner loop.
            # this allows us to bundle inserts/updates on the same table together...although currently
            # they are separate execs via execute(), not executemany()
            
            if not self._has_pks(table):
                #print "NO PKS ?", str(table)
                # if we dont have a full set of primary keys for this table, we cant really
                # do any CRUD with it, so skip.  this occurs if we are mapping against a query
                # that joins on other tables so its not really an error condition.
                continue

            # two lists to store parameters for each table/object pair located
            insert = []
            update = []
            
            # we have our own idea of the primary key columns 
            # for this table, in the case that the user
            # specified custom primary key cols.
            for obj in objects:
                #print "SAVE_OBJ we are Mapper(" + str(id(self)) + ") obj: " +  obj.__class__.__name__ + repr(id(obj))
                params = {}

                # 'postupdate' means a PropertyLoader is telling us, "yes I know you 
                # already inserted/updated this row but I need you to UPDATE one more 
                # time"
                isinsert = not postupdate and not hasattr(obj, "_instance_key")
                hasdata = False
                for col in table.columns:
                    if col is self.version_id_col:
                        if not isinsert:
                            params[col._label] = self._getattrbycolumn(obj, col)
                            params[col.key] = params[col._label] + 1
                        else:
                            params[col.key] = 1
                    elif col in self.pks_by_table[table]:
                        # column is a primary key ?
                        if not isinsert:
                            # doing an UPDATE?  put primary key values as "WHERE" parameters
                            # matching the bindparam we are creating below, i.e. "<tablename>_<colname>"
                            params[col._label] = self._getattrbycolumn(obj, col)
                        else:
                            # doing an INSERT, primary key col ? 
                            # if the primary key values are not populated,
                            # leave them out of the INSERT altogether, since PostGres doesn't want
                            # them to be present for SERIAL to take effect.  A SQLEngine that uses
                            # explicit sequences will put them back in if they are needed
                            value = self._getattrbycolumn(obj, col)
                            if value is not None:
                                params[col.key] = value
                    elif self.polymorphic_on is not None and self.polymorphic_on.shares_lineage(col):
                        if isinsert:
                            value = self.polymorphic_identity
                            if col.default is None or value is not None:
                                params[col.key] = value
                    else:
                        # column is not a primary key ?
                        if not isinsert:
                            # doing an UPDATE ? get the history for the attribute, with "passive"
                            # so as not to trigger any deferred loads.  if there is a new
                            # value, add it to the bind parameters
                            prop = self._getpropbycolumn(col, False)
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
                            value = self._getattrbycolumn(obj, col, False)
                            if value is NO_ATTRIBUTE:
                                continue
                            if col.default is None or value is not None:
                                params[col.key] = value

                if not isinsert:
                    if hasdata:
                        # if none of the attributes changed, dont even
                        # add the row to be updated.
                        update.append((obj, params))
                else:
                    insert.append((obj, params))
            if len(update):
                clause = sql.and_()
                for col in self.pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col._label, type=col.type))
                if self.version_id_col is not None:
                    clause.clauses.append(self.version_id_col == sql.bindparam(self.version_id_col._label, type=col.type))
                statement = table.update(clause)
                rows = 0
                supports_sane_rowcount = True
                def comparator(a, b):
                    for col in self.pks_by_table[table]:
                        x = cmp(a[1][col._label],b[1][col._label])
                        if x != 0:
                            return x
                    return 0
                update.sort(comparator)
                for rec in update:
                    (obj, params) = rec
                    c = connection.execute(statement, params)
                    self._postfetch(connection, table, obj, c, c.last_updated_params())

                    updated_objects.add(obj)
                    rows += c.cursor.rowcount

                if c.supports_sane_rowcount() and rows != len(update):
                    raise exceptions.FlushError("ConcurrencyError - updated rowcount %d does not match number of objects updated %d" % (rows, len(update)))

            if len(insert):
                statement = table.insert()
                for rec in insert:
                    (obj, params) = rec
                    c = connection.execute(statement, params)
                    primary_key = c.last_inserted_ids()
                    if primary_key is not None:
                        i = 0
                        for col in self.pks_by_table[table]:
                            if self._getattrbycolumn(obj, col) is None and len(primary_key) > i:
                                self._setattrbycolumn(obj, col, primary_key[i])
                            i+=1
                    self._postfetch(connection, table, obj, c, c.last_inserted_params())
                    
                    # synchronize newly inserted ids from one table to the next
                    def sync(mapper):
                        inherit = mapper.inherits
                        if inherit is not None:
                            sync(inherit)
                        if mapper._synchronizer is not None:
                            mapper._synchronizer.execute(obj, obj)
                    sync(self)
                    
                    inserted_objects.add(obj)
        if not postupdate:
            [self.extension.after_insert(self, connection, obj) for obj in inserted_objects]
            [self.extension.after_update(self, connection, obj) for obj in updated_objects]

    def _postfetch(self, connection, table, obj, resultproxy, params):
        """after an INSERT or UPDATE, asks the returned result if PassiveDefaults fired off on the database side
        which need to be post-fetched, *or* if pre-exec defaults like ColumnDefaults were fired off
        and should be populated into the instance. this is only for non-primary key columns."""
        if resultproxy.lastrow_has_defaults():
            clause = sql.and_()
            for p in self.pks_by_table[table]:
                clause.clauses.append(p == self._getattrbycolumn(obj, p))
            row = connection.execute(table.select(clause), None).fetchone()
            for c in table.c:
                if self._getattrbycolumn(obj, c, False) is None:
                    self._setattrbycolumn(obj, c, row[c])
        else:
            for c in table.c:
                if c.primary_key or not params.has_key(c.name):
                    continue
                v = self._getattrbycolumn(obj, c, False)
                if v is NO_ATTRIBUTE:
                    continue
                elif v != params.get_original(c.name):
                    self._setattrbycolumn(obj, c, params.get_original(c.name))

    def delete_obj(self, objects, uow):
        """called by a UnitOfWork object to delete objects, which involves a
        DELETE statement for each table used by this mapper, for each object in the list."""
        connection = uow.transaction.connection(self)
        #print "DELETE_OBJ MAPPER", self.class_.__name__, objects

        [self.extension.before_delete(self, connection, obj) for obj in objects]
        deleted_objects = util.Set()
        for table in self.tables.sort(reverse=True):
            if not self._has_pks(table):
                continue
            delete = []
            for obj in objects:
                params = {}
                if not hasattr(obj, "_instance_key"):
                    continue
                else:
                    delete.append(params)
                for col in self.pks_by_table[table]:
                    params[col.key] = self._getattrbycolumn(obj, col)
                if self.version_id_col is not None:
                    params[self.version_id_col.key] = self._getattrbycolumn(obj, self.version_id_col)
                deleted_objects.add(obj)
            if len(delete):
                def comparator(a, b):
                    for col in self.pks_by_table[table]:
                        x = cmp(a[col.key],b[col.key])
                        if x != 0:
                            return x
                    return 0
                delete.sort(comparator)
                clause = sql.and_()
                for col in self.pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col.key, type=col.type))
                if self.version_id_col is not None:
                    clause.clauses.append(self.version_id_col == sql.bindparam(self.version_id_col.key, type=self.version_id_col.type))
                statement = table.delete(clause)
                c = connection.execute(statement, delete)
                if c.supports_sane_rowcount() and c.rowcount != len(delete):
                    raise exceptions.FlushError("ConcurrencyError - updated rowcount %d does not match number of objects updated %d" % (c.cursor.rowcount, len(delete)))
                    
        [self.extension.after_delete(self, connection, obj) for obj in deleted_objects]

    def _has_pks(self, table):
        try:
            for k in self.pks_by_table[table]:
                if not self.columntoproperty.has_key(k):
                    return False
            else:
                return True
        except KeyError:
            return False
            
    def register_dependencies(self, uowcommit, *args, **kwargs):
        """called by an instance of unitofwork.UOWTransaction to register 
        which mappers are dependent on which, as well as DependencyProcessor 
        objects which will process lists of objects in between saves and deletes."""
        for prop in self.__props.values():
            prop.register_dependencies(uowcommit, *args, **kwargs)
    
    def cascade_iterator(self, type, object, recursive=None):
        if recursive is None:
            recursive=util.Set()
        for prop in self.__props.values():
            for c in prop.cascade_iterator(type, object, recursive):
                yield c

    def cascade_callable(self, type, object, callable_, recursive=None):
        if recursive is None:
            recursive=util.Set()
        for prop in self.__props.values():
            prop.cascade_callable(type, object, callable_, recursive)
            
    def _row_identity_key(self, row):
        return sessionlib.get_row_key(row, self.class_, self.pks_by_table[self.mapped_table], self.entity_name)

    def get_select_mapper(self):
        return self.__surrogate_mapper or self
        
    def _instance(self, session, row, imap, result = None, populate_existing = False):
        """pulls an object instance from the given row and appends it to the given result
        list. if the instance already exists in the given identity map, its not added.  in
        either case, executes all the property loaders on the instance to also process extra
        information in the row."""

        if self.polymorphic_on is not None:
            discriminator = row[self.polymorphic_on]
            mapper = self.polymorphic_map[discriminator]
            if mapper is not self:
                row = self.translate_row(mapper, row)
                return mapper._instance(session, row, imap, result=result, populate_existing=populate_existing)
        
        # look in main identity map.  if its there, we dont do anything to it,
        # including modifying any of its related items lists, as its already
        # been exposed to being modified by the application.
        
        populate_existing = populate_existing or self.always_refresh
        identitykey = self._row_identity_key(row)
        if session.has_key(identitykey):
            instance = session._get(identitykey)
            isnew = False
            if populate_existing or session.is_expired(instance, unexpire=True):
                if not imap.has_key(identitykey):
                    imap[identitykey] = instance
                for prop in self.__props.values():
                    prop.execute(session, instance, row, identitykey, imap, True)
            if self.extension.append_result(self, session, row, imap, result, instance, isnew, populate_existing=populate_existing) is EXT_PASS:
                if result is not None:
                    result.append(instance)
            return instance
                    
        # look in result-local identitymap for it.
        exists = imap.has_key(identitykey)      
        if not exists:
            # check if primary key cols in the result are None - this indicates 
            # an instance of the object is not present in the row
            for col in self.pks_by_table[self.mapped_table]:
                if row[col] is None:
                    return None
            # plugin point
            instance = self.extension.create_instance(self, session, row, imap, self.class_)
            if instance is EXT_PASS:
                instance = self._create_instance(session)
            imap[identitykey] = instance
            isnew = True
        else:
            instance = imap[identitykey]
            isnew = False

        # plugin point
        
        # call further mapper properties on the row, to pull further 
        # instances from the row and possibly populate this item.
        if self.extension.populate_instance(self, session, instance, row, identitykey, imap, isnew) is EXT_PASS:
            self.populate_instance(session, instance, row, identitykey, imap, isnew)
        if self.extension.append_result(self, session, row, imap, result, instance, isnew, populate_existing=populate_existing) is EXT_PASS:
            if result is not None:
                result.append(instance)
        return instance

    def _create_instance(self, session):
        obj = self.class_.__new__(self.class_)
        obj._entity_name = self.entity_name
        
        # this gets the AttributeManager to do some pre-initialization,
        # in order to save on KeyErrors later on
        sessionlib.attribute_manager.init_attr(obj)

        return obj

    def translate_row(self, tomapper, row):
        """attempts to take a row and translate its values to a row that can
        be understood by another mapper."""
        newrow = util.DictDecorator(row)
        for c in tomapper.mapped_table.c:
            c2 = self.mapped_table.corresponding_column(c, keys_ok=True, raiseerr=True)
            if row.has_key(c2):
                newrow[c] = row[c2]
        return newrow
        
    def populate_instance(self, session, instance, row, identitykey, imap, isnew, frommapper=None):
        if frommapper is not None:
            row = frommapper.translate_row(self, row)
        for prop in self.__props.values():
            prop.execute(session, instance, row, identitykey, imap, isnew)

    # deprecated query methods.  Query is constructed from Session, and the rest 
    # of these methods are called off of Query now.
    def query(self, session=None):
        """deprecated. use Query instead."""
        if session is not None:
            return querylib.Query(self, session=session)

        try:
            if self._query.mapper is not self:
                self._query = querylib.Query(self)
            return self._query
        except AttributeError:
            self._query = querylib.Query(self)
            return self._query
    def using(self, session):
        """deprecated. use Query instead."""
        return querylib.Query(self, session=session)
    def get(self, ident, **kwargs):
        """deprecated. use Query instead."""
        return self.query().get(ident, **kwargs)
    def _get(self, key, ident=None, reload=False):
        """deprecated. use Query instead."""
        return self.query()._get(key, ident=ident, reload=reload)
    def get_by(self, *args, **params):
        """deprecated. use Query instead."""
        return self.query().get_by(*args, **params)
    def select_by(self, *args, **params):
        """deprecated. use Query instead."""
        return self.query().select_by(*args, **params)
    def selectfirst_by(self, *args, **params):
        """deprecated. use Query instead."""
        return self.query().selectfirst_by(*args, **params)
    def selectone_by(self, *args, **params):
        """deprecated. use Query instead."""
        return self.query().selectone_by(*args, **params)
    def count_by(self, *args, **params):
        """deprecated. use Query instead."""
        return self.query().count_by(*args, **params)
    def selectfirst(self, *args, **params):
        """deprecated. use Query instead."""
        return self.query().selectfirst(*args, **params)
    def selectone(self, *args, **params):
        """deprecated. use Query instead."""
        return self.query().selectone(*args, **params)
    def select(self, arg=None, **kwargs):
        """deprecated. use Query instead."""
        return self.query().select(arg=arg, **kwargs)
    def select_whereclause(self, whereclause=None, params=None, **kwargs):
        """deprecated. use Query instead."""
        return self.query().select_whereclause(whereclause=whereclause, params=params, **kwargs)
    def count(self, whereclause=None, params=None, **kwargs):
        """deprecated. use Query instead."""
        return self.query().count(whereclause=whereclause, params=params, **kwargs)
    def select_statement(self, statement, **params):
        """deprecated. use Query instead."""
        return self.query().select_statement(statement, **params)
    def select_text(self, text, **params):
        """deprecated. use Query instead."""
        return self.query().select_text(text, **params)
        
class MapperProperty(object):
    """an element attached to a Mapper that describes and assists in the loading and saving 
    of an attribute on an object instance."""
    def execute(self, session, instance, row, identitykey, imap, isnew):
        """called when the mapper receives a row.  instance is the parent instance
        corresponding to the row. """
        raise NotImplementedError()
    def cascade_iterator(self, type, object, recursive=None):
        return []
    def cascade_callable(self, type, object, callable_, recursive=None):
        return []
    def copy(self):
        raise NotImplementedError()
    def get_criterion(self, query, key, value):
        """Returns a WHERE clause suitable for this MapperProperty corresponding to the 
        given key/value pair, where the key is a column or object property name, and value
        is a value to be matched.  This is only picked up by PropertyLoaders.
            
        this is called by a mappers select_by method to formulate a set of key/value pairs into 
        a WHERE criterion that spans multiple tables if needed."""
        return None
    def setup(self, key, statement, **options):
        """called when a statement is being constructed.  """
        return self
    def set_parent(self, parent):
        self.parent = parent
    def init(self, key, parent):
        """called after all mappers are compiled to assemble relationships between 
        mappers, establish instrumented class attributes"""
        self.key = key
        self.localparent = parent
        if not hasattr(self, 'inherits'):
            self.inherits = None
        self.do_init()
    def adapt_to_inherited(self, key, newparent):
        """adapt this MapperProperty to a new parent, assuming the new parent is an inheriting
        descendant of the old parent.  """
        p = self.copy()
        newparent._compile_property(key, p, init=False)
        p.localparent = newparent
        p.parent = self.parent
        p.inherits = getattr(self, 'inherits', self)
    def do_init(self):
        """template method for subclasses"""
        pass
    def register_deleted(self, object, uow):
        """called when the instance is being deleted"""
        pass
    def register_dependencies(self, *args, **kwargs):
        """called by the Mapper in response to the UnitOfWork calling the Mapper's
        register_dependencies operation.  Should register with the UnitOfWork all 
        inter-mapper dependencies as well as dependency processors (see UOW docs for more details)"""
        pass
    def is_primary(self):
        """a return value of True indicates we are the primary MapperProperty for this loader's
        attribute on our mapper's class.  It means we can set the object's attribute behavior
        at the class level.  otherwise we have to set attribute behavior on a per-instance level."""
        return self.inherits is None and self.parent._is_primary_mapper()

class SynonymProperty(MapperProperty):
    """a marker object used by query.select_by to allow a property name to refer to another.
    
    this object may be expanded in the future."""
    def __init__(self, name):
        self.name = name
    def execute(self, session, instance, row, identitykey, imap, isnew):
        pass
    def copy(self):
        return SynonymProperty(self.name)
        
class MapperOption(object):
    """describes a modification to a Mapper in the context of making a copy
    of it.  This is used to assist in the prototype pattern used by mapper.options()."""
    def process(self, mapper):
        raise NotImplementedError()
    def hash_key(self):
        return repr(self)

class ExtensionOption(MapperOption):
    """adds a new MapperExtension to a mapper's chain of extensions"""
    def __init__(self, ext):
        self.ext = ext
    def process(self, mapper):
        self.ext.next = mapper.extension
        mapper.extension = self.ext

class MapperExtension(object):
    """base implementation for an object that provides overriding behavior to various
    Mapper functions.  For each method in MapperExtension, a result of EXT_PASS indicates
    the functionality is not overridden."""
    def get_session(self):
        """called to retrieve a contextual Session instance with which to
        register a new object. Note: this is not called if a session is 
        provided with the __init__ params (i.e. _sa_session)"""
        return EXT_PASS
    def select_by(self, query, *args, **kwargs):
        """overrides the select_by method of the Query object"""
        return EXT_PASS
    def select(self, query, *args, **kwargs):
        """overrides the select method of the Query object"""
        return EXT_PASS
    def create_instance(self, mapper, session, row, imap, class_):
        """called when a new object instance is about to be created from a row.  
        the method can choose to create the instance itself, or it can return 
        None to indicate normal object creation should take place.
        
        mapper - the mapper doing the operation
        
        row - the result row from the database
        
        imap - a dictionary that is storing the running set of objects collected from the
        current result set
        
        class_ - the class we are mapping.
        """
        return EXT_PASS
    def append_result(self, mapper, session, row, imap, result, instance, isnew, populate_existing=False):
        """called when an object instance is being appended to a result list.
        
        If this method returns True, it is assumed that the mapper should do the appending, else
        if this method returns False, it is assumed that the append was handled by this method.

        mapper - the mapper doing the operation
        
        row - the result row from the database
        
        imap - a dictionary that is storing the running set of objects collected from the
        current result set
        
        result - an instance of util.HistoryArraySet(), which may be an attribute on an
        object if this is a related object load (lazy or eager).  
        
        instance - the object instance to be appended to the result
        
        isnew - indicates if this is the first time we have seen this object instance in the current result
        set.  if you are selecting from a join, such as an eager load, you might see the same object instance
        many times in the same result set.
        
        populate_existing - usually False, indicates if object instances that were already in the main 
        identity map, i.e. were loaded by a previous select(), get their attributes overwritten
        """
        return EXT_PASS
    def populate_instance(self, mapper, session, instance, row, identitykey, imap, isnew):
        """called right before the mapper, after creating an instance from a row, passes the row
        to its MapperProperty objects which are responsible for populating the object's attributes.
        If this method returns True, it is assumed that the mapper should do the appending, else
        if this method returns False, it is assumed that the append was handled by this method.
        
        Essentially, this method is used to have a different mapper populate the object:
        
            def populate_instance(self, mapper, session, instance, row, identitykey, imap, isnew):
                othermapper.populate_instance(session, instance, row, identitykey, imap, isnew, frommapper=mapper)
                return True
        """
        return EXT_PASS
    def before_insert(self, mapper, connection, instance):
        """called before an object instance is INSERTed into its table.
        
        this is a good place to set up primary key values and such that arent handled otherwise."""
        return EXT_PASS
    def before_update(self, mapper, connection, instance):
        """called before an object instnace is UPDATED"""
        return EXT_PASS
    def after_update(self, mapper, connection, instance):
        """called after an object instnace is UPDATED"""
        return EXT_PASS
    def after_insert(self, mapper, connection, instance):
        """called after an object instance has been INSERTed"""
        return EXT_PASS
    def before_delete(self, mapper, connection, instance):
        """called before an object instance is DELETEed"""
        return EXT_PASS
    def after_delete(self, mapper, connection, instance):
        """called after an object instance is DELETEed"""
        return EXT_PASS

class ExtensionCarrier(MapperExtension):
    def __init__(self):
        self.elements = []
    # TODO: shrink down this approach using __getattribute__ or similar
    def get_session(self):
        return self._do('get_session')
    def select_by(self, *args, **kwargs):
        return self._do('select_by', *args, **kwargs)
    def select(self, *args, **kwargs):
        return self._do('select', *args, **kwargs)
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
        for elem in self.elements:
            if elem is self:
                raise "WTF"
            ret = getattr(elem, funcname)(*args, **kwargs)
            if ret is not EXT_PASS:
                return ret
        else:
            return EXT_PASS
            
class TranslatingDict(dict):
    """a dictionary that stores ColumnElement objects as keys.  incoming ColumnElement
    keys are translated against those of an underling FromClause for all operations.
    This way the columns from any Selectable that is derived from or underlying this
    TranslatingDict's selectable can be used as keys."""
    def __init__(self, selectable):
        super(TranslatingDict, self).__init__()
        self.selectable = selectable
    def __translate_col(self, col):
        ourcol = self.selectable.corresponding_column(col, keys_ok=False, raiseerr=False)
        #if col is not ourcol:
        #    print "TD TRANSLATING ", col, "TO", ourcol
        if ourcol is None:
            return col
        else:
            return ourcol
    def __getitem__(self, col):
        return super(TranslatingDict, self).__getitem__(self.__translate_col(col))
    def has_key(self, col):
        return super(TranslatingDict, self).has_key(self.__translate_col(col))
    def __setitem__(self, col, value):
        return super(TranslatingDict, self).__setitem__(self.__translate_col(col), value)
    def __contains__(self, col):
        return self.has_key(col)
    def setdefault(self, col, value):
        return super(TranslatingDict, self).setdefault(self.__translate_col(col), value)
            
class ClassKey(object):
    """keys a class and an entity name to a mapper, via the mapper_registry."""
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

def hash_key(obj):
    if obj is None:
        return 'None'
    elif isinstance(obj, list):
        return repr([hash_key(o) for o in obj])
    elif hasattr(obj, 'hash_key'):
        return obj.hash_key()
    else:
        return repr(obj)

def has_mapper(object):
    """returns True if the given object has a mapper association"""
    return hasattr(object, '_entity_name')
        
def object_mapper(object, raiseerror=True):
    """given an object, returns the primary Mapper associated with the object instance"""
    try:
        mapper = mapper_registry[ClassKey(object.__class__, getattr(object, '_entity_name', None))]
    except (KeyError, AttributeError):        
        if raiseerror:
            raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (object.__class__.__name__, getattr(object, '_entity_name', None)))
        else:
            return None
    return mapper.compile()
    
def class_mapper(class_, entity_name=None, compile=True):
    """given a ClassKey, returns the primary Mapper associated with the key."""
    try:
        mapper = mapper_registry[ClassKey(class_, entity_name)]
    except (KeyError, AttributeError):
        raise exceptions.InvalidRequestError("Class '%s' entity name '%s' has no mapper associated with it" % (class_.__name__, entity_name))
    if compile:
        return mapper.compile()
    else:
        return mapper
    
    
