# mapper.py
# Copyright (C) 2005 Michael Bayer mike_mp@zzzcomputing.com
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

"""
the mapper package provides object-relational functionality, building upon the schema and sql
packages and tying operations to class properties and constructors.
"""
import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.util as util
import sqlalchemy.objectstore as objectstore
import random, copy, types

__ALL__ = ['relation', 'eagerload', 'lazyload', 'noload', 'assignmapper', 
        'mapper', 'clear_mappers', 'objectstore', 'sql', 'extension', 'class_mapper', 'object_mapper', 'MapperExtension']

def relation(*args, **params):
    """provides a relationship of a primary Mapper to a secondary Mapper, which corresponds
    to a parent-child or associative table relationship."""
    if isinstance(args[0], type) and len(args) == 1:
        return _relation_loader(*args, **params)
    elif isinstance(args[0], Mapper):
        return _relation_loader(*args, **params)
    else:
        return _relation_mapper(*args, **params)

def _relation_loader(mapper, secondary=None, primaryjoin=None, secondaryjoin=None, lazy=True, **kwargs):
    if lazy:
        return LazyLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)
    elif lazy is None:
        return PropertyLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)
    else:
        return EagerLoader(mapper, secondary, primaryjoin, secondaryjoin, **kwargs)

def _relation_mapper(class_, table=None, secondary=None, 
                    primaryjoin=None, secondaryjoin=None, 
                    foreignkey=None, uselist=None, private=False, live=False, association=None, lazy=True, **kwargs):

    return _relation_loader(mapper(class_, table, **kwargs), secondary, primaryjoin, secondaryjoin, 
                    foreignkey=foreignkey, uselist=uselist, private=private, live=live, association=association, lazy=lazy)

#def _relation_mapper(class_, table=None, secondary=None, 
#                    primaryjoin=None, secondaryjoin=None, foreignkey=None, 
#                    uselist=None, private=False, live=False, association=None, **kwargs):
#    return _relation_loader(mapper(class_, table, **kwargs), secondary, primaryjoin=primaryjoin, secondaryjoin=secondaryjoin, foreignkey=foreignkey, uselist=uselist, private=private, live=live, association=association)

class assignmapper(object):
    """provides a property object that will instantiate a Mapper for a given class the first
    time it is called off of the object.  This is useful for attaching a Mapper to a class
    that has dependencies on other classes and tables which may not have been defined yet."""
    def __init__(self, table, class_ = None, **kwargs):
        self.table = table
        self.kwargs = kwargs
        if class_:
            self.__get__(None, class_)
            
    def __get__(self, instance, owner):
        if not hasattr(self, 'mapper'):
            self.mapper = mapper(owner, self.table, **self.kwargs)
            self.mapper._init_class()
            if self.mapper.class_ is not owner:
                raise "no match " + repr(self.mapper.class_) + " " + repr(owner)
            if not hasattr(owner, 'c'):
                raise "no c"
        return self.mapper
    
_mappers = {}
def mapper(class_, table = None, engine = None, autoload = False, *args, **params):
    """returns a new or already cached Mapper object."""
    if table is None:
        return class_mapper(class_)

    if isinstance(table, str):
        table = schema.Table(table, engine, autoload = autoload, mustexist = not autoload)
            
    hashkey = mapper_hash_key(class_, table, *args, **params)
    #print "HASHKEY: " + hashkey
    try:
        return _mappers[hashkey]
    except KeyError:
        m = Mapper(hashkey, class_, table, *args, **params)
        _mappers.setdefault(hashkey, m)
        m._init_properties()
        return _mappers[hashkey]

def clear_mappers():
    """removes all mappers that have been created thus far.  when new mappers are 
    created, they will be assigned to their classes as their primary mapper."""
    _mappers.clear()

def clear_mapper(m):
    """removes the given mapper from the storage of mappers.  when a new mapper is 
    created for the previous mapper's class, it will be used as that classes' 
    new primary mapper."""
    del _mappers[m.hash_key]

def extension(ext):
    """returns a MapperOption that will add the given MapperExtension to the 
    mapper returned by mapper.options()."""
    return ExtensionOption(ext)
def eagerload(name):
    """returns a MapperOption that will convert the property of the given name
    into an eager load.  Used with mapper.options()"""
    return EagerLazyOption(name, toeager=True)

def lazyload(name):
    """returns a MapperOption that will convert the property of the given name
    into a lazy load.  Used with mapper.options()"""
    return EagerLazyOption(name, toeager=False)

def noload(name):
    """returns a MapperOption that will convert the property of the given name
    into a non-load.  Used with mapper.options()"""
    return EagerLazyOption(name, toeager=None)
    
def object_mapper(object):
    """given an object, returns the primary Mapper associated with the object
    or the object's class."""
    return class_mapper(object.__class__)

def class_mapper(class_):
    """given a class, returns the primary Mapper associated with the class."""
    try:
        return _mappers[class_._mapper]
    except KeyError:
        pass
    except AttributeError:
        pass
        raise "Class '%s' has no mapper associated with it" % class_.__name__

        
class Mapper(object):
    """Persists object instances to and from schema.Table objects via the sql package.
    Instances of this class should be constructed through this package's mapper() or
    relation() function."""
    def __init__(self, 
                hashkey, 
                class_, 
                table, 
                primarytable = None, 
                scope = "thread", 
                properties = None, 
                primary_keys = None, 
                is_primary = False, 
                inherits = None, 
                inherit_condition = None, 
                extension = None,
                **kwargs):

        self.copyargs = {
            'class_':class_,
            'table':table,
            'primarytable':primarytable,
            'scope':scope,
            'properties':properties,
            'primary_keys':primary_keys,
            'is_primary':False,
            'inherits':inherits,
            'inherit_condition':inherit_condition,
            'extension':extension
        }
        
        if extension is None:
            self.extension = MapperExtension()
        else:
            self.extension = extension                
        self.hashkey = hashkey
        self.class_ = class_
        self.scope = scope
        self.is_primary = is_primary
        
        if not issubclass(class_, object):
            raise "Class '%s' is not a new-style class" % class_.__name__

        if inherits is not None:
            # TODO: determine inherit_condition (make JOIN do natural joins)
            primarytable = inherits.primarytable
            table = sql.join(table, inherits.table, inherit_condition)
            
        self.table = table
            
        # locate all tables contained within the "table" passed in, which
        # may be a join or other construct
        tf = TableFinder()
        self.table.accept_visitor(tf)
        self.tables = tf.tables

        # determine "primary" table        
        if primarytable is None:
            if len(self.tables) > 1:
                raise "table contains multiple tables - specify primary table argument to Mapper"
            self.primarytable = self.tables[0]
        else:
            self.primarytable = primarytable

        # determine primary keys, either passed in, or get them from our set of tables
        self.primary_keys = {}
        if primary_keys is not None:
            for k in primary_keys:
                self.primary_keys.setdefault(k.table, []).append(k)
                if k.table != self.table:
                    self.primary_keys.setdefault(self.table, []).append(k)
        else:
            for t in self.tables + [self.table]:
                try:
                    list = self.primary_keys[t]
                except KeyError:
                    list = self.primary_keys.setdefault(t, util.HashSet())
                if not len(t.primary_keys):
                    raise "Table " + t.name + " has no primary keys. Specify primary_keys argument to mapper."
                for k in t.primary_keys:
                    list.append(k)

        # make table columns addressable via the mapper
        self.columns = util.OrderedProperties()
        self.c = self.columns
        
        # object attribute names mapped to MapperProperty objects
        self.props = {}
        
        # table columns mapped to lists of MapperProperty objects
        # using a list allows a single column to be defined as 
        # populating multiple object attributes
        self.columntoproperty = {}
        
        # load custom properties 
        if properties is not None:
            for key, prop in properties.iteritems():
                if isinstance(prop, schema.Column):
                    self.columns[key] = prop
                    prop = ColumnProperty(prop)
                self.props[key] = prop
                if isinstance(prop, ColumnProperty):
                    for col in prop.columns:
                        proplist = self.columntoproperty.setdefault(col.original, [])
                        proplist.append(prop)

        # load properties from the main table object,
        # not overriding those set up in the 'properties' argument
        for column in self.table.columns:
            self.columns[column.key] = column

            if self.columntoproperty.has_key(column.original):
                continue
                
            prop = self.props.get(column.key, None)
            if prop is None:
                prop = ColumnProperty(column)
                self.props[column.key] = prop
            elif isinstance(prop, ColumnProperty):
                prop.columns.append(column)
            else:
                #print "WARNING: column %s not being added due to property %s" % (column.key, repr(prop))
                continue
        
            # its a ColumnProperty - match the ultimate table columns
            # back to the property
            proplist = self.columntoproperty.setdefault(column.original, [])
            proplist.append(prop)

        if inherits is not None:
            for key, prop in inherits.props.iteritems():
                if not self.props.has_key(key):
                    self.props[key] = prop._copy()
                
        if not hasattr(self.class_, '_mapper') or self.is_primary or not _mappers.has_key(self.class_._mapper) or (inherits is not None and inherits._is_primary_mapper()):
            self._init_class()
        
    engines = property(lambda s: [t.engine for t in s.tables])

    def add_property(self, key, prop):
        if isinstance(prop, schema.Column):
            self.columns[key] = prop
            prop = ColumnProperty(prop)
        self.props[key] = prop
        if isinstance(prop, ColumnProperty):
            for col in prop.columns:
                proplist = self.columntoproperty.setdefault(col.original, [])
                proplist.append(prop)
        prop.init(key, self)
        
    def _init_properties(self):
        for key, prop in self.props.iteritems():
            if getattr(prop, 'key', None) is None:
                prop.init(key, self)
        
    def __str__(self):
        return "Mapper|" + self.class_.__name__ + "|" + self.primarytable.name
        
    def hash_key(self):
        return self.hashkey

    def _is_primary_mapper(self):
        return getattr(self.class_, '_mapper') == self.hashkey
        
    def _init_class(self):
        """sets up our classes' overridden __init__ method, this mappers hash key as its
        '_mapper' property, and our columns as its 'c' property.  if the class already had a
        mapper, the old __init__ method is kept the same."""
        if not hasattr(self.class_, '_mapper'):
            oldinit = self.class_.__init__
            def init(self, *args, **kwargs):
                nohist = kwargs.pop('_mapper_nohistory', False)
                if oldinit is not None:
                    oldinit(self, *args, **kwargs)
                if not nohist:
                    objectstore.uow().register_new(self)
            self.class_.__init__ = init
        self.class_._mapper = self.hashkey
        self.class_.c = self.c
        
    def set_property(self, key, prop):
        self.props[key] = prop
        prop.init(key, self)

    
    def instances(self, cursor, *mappers):
        result = util.HistoryArraySet()
        if len(mappers):
            otherresults = []
            for m in mappers:
                otherresults.append(util.HistoryArraySet())
                
        imap = {}
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            self._instance(row, imap, result)
            i = 0
            for m in mappers:
                m._instance(row, imap, otherresults[i])
                i+=1
                
        # store new stuff in the identity map
        for value in imap.values():
            objectstore.uow().register_clean(value)

        if len(mappers):
            return [result] + otherresults
        else:
            return result

    def get(self, *ident):
        """returns an instance of the object based on the given identifier, or None
        if not found.  The *ident argument is a 
        list of primary keys in the order of the table def's primary keys."""
        key = objectstore.get_id_key(ident, self.class_, self.primarytable)
        #print "key: " + repr(key) + " ident: " + repr(ident)
        try:
            return objectstore.uow()._get(key)
        except KeyError:
            clause = sql.and_()
            i = 0
            for primary_key in self.primary_keys[self.primarytable]:
                # appending to the and_'s clause list directly to skip
                # typechecks etc.
                clause.clauses.append(primary_key == ident[i])
                i += 1
            try:
                return self.select(clause)[0]
            except IndexError:
                return None

        
    def identity_key(self, *primary_keys):
        return objectstore.get_id_key(tuple(primary_keys), self.class_, self.primarytable)
    
    def instance_key(self, instance):
        return self.identity_key(*[self._getattrbycolumn(instance, column) for column in self.primary_keys[self.table]])

    def compile(self, whereclause = None, **options):
        """works like select, except returns the SQL statement object without 
        compiling or executing it"""
        return self._compile(whereclause, **options)

    def options(self, *options):
        """uses this mapper as a prototype for a new mapper with different behavior.
        *options is a list of options directives, which include eagerload(), lazyload(), and noload()"""

        hashkey = hash_key(self) + "->" + repr([hash_key(o) for o in options])
        try:
            return _mappers[hashkey]
        except KeyError:
            mapper = Mapper(hashkey, **self.copyargs)
            mapper._init_properties()

            for option in options:
                option.process(mapper)
            return _mappers.setdefault(hashkey, mapper)

    def selectone(self, *args, **params):
        """works like select(), but only returns the first result by itself, or None if no 
        objects returned."""
        ret = self.select(*args, **params)
        if len(ret):
            return ret[0]
        else:
            return None
            
    def select(self, arg = None, **params):
        """selects instances of the object from the database.  
        
        arg can be any ClauseElement, which will form the criterion with which to
        load the objects.
        
        For more advanced usage, arg can also be a Select statement object, which
        will be executed and its resulting rowset used to build new object instances.  
        in this case, the developer must insure that an adequate set of columns exists in the 
        rowset with which to build new object instances."""
        if arg is not None and isinstance(arg, sql.Select):
            return self.select_statement(arg, **params)
        else:
            return self.select_whereclause(arg, **params)

    def select_whereclause(self, whereclause = None, order_by = None, **params):
        statement = self._compile(whereclause, order_by = order_by)
        return self.select_statement(statement, **params)

    def select_statement(self, statement, **params):
        statement.use_labels = True
        return self.instances(statement.execute(**params))

    def select_text(self, text, **params):
        t = sql.text(text, engine=self.primarytable.engine)
        return self.instances(t.execute(**params))

    def _getattrbycolumn(self, obj, column):
        try:
            prop = self.columntoproperty[column]
        except KeyError:
            try:
                prop = self.props[column.key]
                raise "Column '%s.%s' is not available, due to conflicting property '%s':%s" % (column.table.name, column.name, column.key, repr(prop))
            except KeyError:
                raise "No column %s.%s is configured on mapper %s..." % (column.table.name, column.name, str(self))
                
        return prop[0].getattr(obj)

    def _setattrbycolumn(self, obj, column, value):
        self.columntoproperty[column][0].setattr(obj, value)

        
    def save_obj(self, objects, uow):
        """called by a UnitOfWork object to save objects, which involves either an INSERT or
        an UPDATE statement for each table used by this mapper, for each element of the
        list."""
          
        for table in self.tables:
            # loop thru tables in the outer loop, objects on the inner loop.
            # this is important for an object represented across two tables
            # so that it gets its primary keys populated for the benefit of the
            # second table.
            insert = []
            update = []
            
            # we have our own idea of the primary keys 
            # for this table, in the case that the user
            # specified custom primary keys.
            pk = {}
            for k in self.primary_keys[table]:
                pk[k] = k
            for obj in objects:
                
#                print "SAVE_OBJ we are " + hash_key(self) + " obj: " +  obj.__class__.__name__ + repr(id(obj))
                params = {}

                for col in table.columns:
                    #if col.primary_key:
                    if pk.has_key(col):
                        if hasattr(obj, "_instance_key"):
                            params[col.table.name + "_" + col.key] = self._getattrbycolumn(obj, col)
                        else:
                            # its an INSERT - if its NULL, leave it out as pgsql doesnt 
                            # like it for an autoincrement
                            value = self._getattrbycolumn(obj, col)
                            if value is not None:
                                params[col.key] = value
                    else:
                        params[col.key] = self._getattrbycolumn(obj, col)

                if hasattr(obj, "_instance_key"):
                    update.append(params)
                else:
                    insert.append((obj, params))
                uow.register_saved_object(obj)
            if len(update):
                #print "REGULAR UPDATES"
                clause = sql.and_()
                for col in self.primary_keys[table]:
                    clause.clauses.append(col == sql.bindparam(col.table.name + "_" + col.key))
                statement = table.update(clause)
                c = statement.execute(*update)
                if c.rowcount != len(update):
                    raise "ConcurrencyError - updated rowcount %d does not match number of objects updated %d" % (c.cursor.rowcount, len(update))
            if len(insert):
                import sys
                statement = table.insert()
                for rec in insert:
                    (obj, params) = rec
                    statement.execute(**params)
                    primary_keys = table.engine.last_inserted_ids()
                    if primary_keys is not None:
                        i = 0
                        for col in self.primary_keys[table]:
                    #        print "col: " + table.name + "." + col.key + " val: " + repr(self._getattrbycolumn(obj, col))
                            if self._getattrbycolumn(obj, col) is None:
                                self._setattrbycolumn(obj, col, primary_keys[i])
                            i+=1
                    self.extension.after_insert(self, obj)
                    
    def delete_obj(self, objects, uow):
        """called by a UnitOfWork object to delete objects, which involves a
        DELETE statement for each table used by this mapper, for each object in the list."""
        for table in self.tables:
            delete = []
            for obj in objects:
                params = {}
                if not hasattr(obj, "_instance_key"):
                    continue
                else:
                    delete.append(params)
                for col in self.primary_keys[table]:
                    params[col.key] = self._getattrbycolumn(obj, col)
                uow.register_deleted_object(obj)
                self.extension.before_delete(self, obj)
            if len(delete):
                clause = sql.and_()
                for col in self.primary_keys[table]:
                    clause.clauses.append(col == sql.bindparam(col.key))
                statement = table.delete(clause)
                c = statement.execute(*delete)
                if c.rowcount != len(delete):
                    raise "ConcurrencyError - updated rowcount %d does not match number of objects updated %d" % (c.cursor.rowcount, len(delete))

    def register_dependencies(self, *args, **kwargs):
        """called by an instance of objectstore.UOWTransaction to register 
        which mappers are dependent on which, as well as DependencyProcessor 
        objects which will process lists of objects in between saves and deletes."""
        for prop in self.props.values():
            prop.register_dependencies(*args, **kwargs)

    def register_deleted(self, obj, uow):
        for prop in self.props.values():
            prop.register_deleted(obj, uow)
            
    def _compile(self, whereclause = None, order_by = None, **options):
        statement = sql.select([self.table], whereclause, order_by = order_by)
        statement.order_by(self.table.rowid_column)
        # plugin point
        for key, value in self.props.iteritems():
            value.setup(key, statement, **options) 
        statement.use_labels = True
        return statement


    def _identity_key(self, row):
        return objectstore.get_row_key(row, self.class_, self.primarytable, self.primary_keys[self.table])

    def _instance(self, row, imap, result = None, populate_existing = False):
        """pulls an object instance from the given row and appends it to the given result
        list. if the instance already exists in the given identity map, its not added.  in
        either case, executes all the property loaders on the instance to also process extra
        information in the row."""

        # look in main identity map.  if its there, we dont do anything to it,
        # including modifying any of its related items lists, as its already
        # been exposed to being modified by the application.
        identitykey = self._identity_key(row)
        if objectstore.uow().has_key(identitykey):
            instance = objectstore.uow()._get(identitykey)

            isnew = False
            if populate_existing:
                isnew = not imap.has_key(identitykey)
                if isnew:
                    imap[identitykey] = instance
                for prop in self.props.values():
                    prop.execute(instance, row, identitykey, imap, isnew)

            if self.extension.append_result(self, row, imap, result, instance, isnew, populate_existing=populate_existing):
                if result is not None:
                    result.append_nohistory(instance)

            return instance
                    
        # look in result-local identitymap for it.
        exists = imap.has_key(identitykey)      
        if not exists:
            # check if primary keys in the result are None - this indicates 
            # an instance of the object is not present in the row
            for col in self.primary_keys[self.table]:
                if row[col] is None:
                    return None
            # plugin point
            instance = self.extension.create_instance(self, row, imap, self.class_)
            if instance is None:
                instance = self.class_(_mapper_nohistory=True)
            # attach mapper hashkey to the instance ?
            #instance._mapper = self.hashkey
            instance._instance_key = identitykey

            imap[identitykey] = instance
            isnew = True
        else:
            instance = imap[identitykey]
            isnew = False


        # plugin point
        
        # call further mapper properties on the row, to pull further 
        # instances from the row and possibly populate this item.
        for prop in self.props.values():
            prop.execute(instance, row, identitykey, imap, isnew)

        if self.extension.append_result(self, row, imap, result, instance, isnew, populate_existing=populate_existing):
            if result is not None:
                result.append_nohistory(instance)
            
        return instance

        
class MapperProperty(object):
    """an element attached to a Mapper that describes and assists in the loading and saving 
    of an attribute on an object instance."""
    def execute(self, instance, row, identitykey, imap, isnew):
        """called when the mapper receives a row.  instance is the parent instance
        corresponding to the row. """
        raise NotImplementedError()
    def _copy(self):
        raise NotImplementedError()
    def hash_key(self):
        """describes this property and its instantiated arguments in such a way
        as to uniquely identify the concept this MapperProperty represents,within 
        a process."""
        raise NotImplementedError()
    def setup(self, key, statement, **options):
        """called when a statement is being constructed.  """
        return self
    def init(self, key, parent):
        """called when the MapperProperty is first attached to a new parent Mapper."""
        pass
    def register_deleted(self, object, uow):
        """called when the instance is being deleted"""
        pass
    def register_dependencies(self, *args, **kwargs):
        pass

class ColumnProperty(MapperProperty):
    """describes an object attribute that corresponds to a table column."""
    def __init__(self, *columns):
        """the list of columns describes a single object property populating 
        multiple columns, typcially across multiple tables"""
        self.columns = list(columns)

    def getattr(self, object):
        return getattr(object, self.key, None)
    def setattr(self, object, value):
        setattr(object, self.key, value)
    def hash_key(self):
        return "ColumnProperty(%s)" % repr([hash_key(c) for c in self.columns])

    def _copy(self):
        return ColumnProperty(*self.columns)
        
    def init(self, key, parent):
        self.key = key
        # establish a SmartProperty property manager on the object for this key
        if parent._is_primary_mapper():
            #print "regiser col on class %s key %s" % (parent.class_.__name__, key)
            objectstore.uow().register_attribute(parent.class_, key, uselist = False)

    def execute(self, instance, row, identitykey, imap, isnew):
        if isnew:
            instance.__dict__[self.key] = row[self.columns[0]]
        

class PropertyLoader(MapperProperty):
    LEFT = 0
    RIGHT = 1
    CENTER = 2

    """describes an object property that holds a single item or list of items that correspond
    to a related database table."""
    def __init__(self, argument, secondary, primaryjoin, secondaryjoin, foreignkey=None, uselist=None, private=False, live=False, isoption=False, association=None, **kwargs):
        self.uselist = uselist
        self.argument = argument
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.foreignkey = foreignkey
        self.private = private
        self.live = live
        self.isoption = isoption
        self.association = association
        self._hash_key = "%s(%s, %s, %s, %s, %s, %s, %s)" % (self.__class__.__name__, hash_key(self.argument), hash_key(secondary), hash_key(primaryjoin), hash_key(secondaryjoin), hash_key(foreignkey), repr(uselist), repr(private))

    def _copy(self):
        return self.__class__(self.mapper, self.secondary, self.primaryjoin, self.secondaryjoin, self.foreignkey, self.uselist, self.private)
        
    def hash_key(self):
        return self._hash_key

    def init(self, key, parent):
        if isinstance(self.argument, type):
            self.mapper = class_mapper(self.argument)
        else:
            self.mapper = self.argument

        if self.association is not None:
            if isinstance(self.association, type):
                self.association = class_mapper(self.association)
                
        self.target = self.mapper.table
        self.key = key
        self.parent = parent

        # if join conditions were not specified, figure them out based on foreign keys
        if self.secondary is not None:
            if self.secondaryjoin is None:
                self.secondaryjoin = self._match_primaries(self.target, self.secondary)
            if self.primaryjoin is None:
                self.primaryjoin = self._match_primaries(parent.table, self.secondary)
        else:
            if self.primaryjoin is None:
                self.primaryjoin = self._match_primaries(parent.table, self.target)
        
        # if the foreign key wasnt specified and theres no assocaition table, try to figure
        # out who is dependent on who. we dont need all the foreign keys represented in the join,
        # just one of them.  
        if self.foreignkey is None and self.secondaryjoin is None:
            # else we usually will have a one-to-many where the secondary depends on the primary
            # but its possible that its reversed
            self.foreignkey = self._find_dependent()

        self.direction = self._get_direction()
        
        if self.uselist is None and self.direction == PropertyLoader.RIGHT:
            self.uselist = False

        if self.uselist is None:
            self.uselist = True
                    
        self._compile_synchronizers()
                
        #if not hasattr(parent.class_, key):
            #print "regiser list col on class %s key %s" % (parent.class_.__name__, key)
        if self._is_primary():
            self._set_class_attribute(parent.class_, key)
    
    def _is_primary(self):
        """a return value of True indicates we are the primary PropertyLoader for this loader's
        attribute on our mapper's class.  It means we can set the object's attribute behavior
        at the class level.  otherwise we have to set attribute behavior on a per-instance level."""
        return self.parent._is_primary_mapper and not self.isoption
        
    def _set_class_attribute(self, class_, key):
        """sets attribute behavior on our target class."""
        objectstore.uow().register_attribute(class_, key, uselist = self.uselist, deleteremoved = self.private)
        
    def _get_direction(self):
        if self.parent.primarytable is self.target:
            if self.foreignkey.primary_key:
                return PropertyLoader.RIGHT
            else:
                return PropertyLoader.LEFT
        elif self.secondaryjoin is not None:
            return PropertyLoader.CENTER
        elif self.foreignkey.table == self.target:
            return PropertyLoader.LEFT
        elif self.foreignkey.table == self.parent.primarytable:
            return PropertyLoader.RIGHT

    def _find_dependent(self):
        dependent = [None]
        def foo(binary):
            if binary.operator != '=':
                return
            if isinstance(binary.left, schema.Column) and binary.left.primary_key:
                if dependent[0] is binary.left:
                    raise "bidirectional dependency not supported...specify foreignkey"
                dependent[0] = binary.right
            elif isinstance(binary.right, schema.Column) and binary.right.primary_key:
                if dependent[0] is binary.right:
                    raise "bidirectional dependency not supported...specify foreignkey"
                dependent[0] = binary.left
        visitor = BinaryVisitor(foo)
        self.primaryjoin.accept_visitor(visitor)
        if dependent[0] is None:
            raise "cant determine primary foreign key in the join relationship....specify foreignkey=<column>"
        else:
            return dependent[0]

    def _match_primaries(self, primary, secondary):
        crit = []
        for fk in secondary.foreign_keys:
            if fk.column.table is primary:
                crit.append(fk.column == fk.parent)
                self.foreignkey = fk.parent
        for fk in primary.foreign_keys:
            if fk.column.table is secondary:
                crit.append(fk.column == fk.parent)
                self.foreignkey = fk.parent

        if len(crit) == 0:
            raise "Cant find any foreign key relationships between '%s' (%s) and '%s' (%s)" % (primary.table.name, repr(primary.table), secondary.table.name, repr(secondary.table))
        elif len(crit) == 1:
            return (crit[0])
        else:
            return sql.and_(*crit)

    def _compile_synchronizers(self):
        def compile(binary):
            if binary.operator != '=' or not isinstance(binary.left, schema.Column) or not isinstance(binary.right, schema.Column):
                return

            if binary.left.table == binary.right.table:
                if binary.left.primary_key:
                    source = binary.left
                    dest = binary.right
                elif binary.right.primary_key:
                    source = binary.right
                    dest = binary.left
                else:
                    raise "Cant determine direction for relationship %s = %s" % (binary.left.fullname, binary.right.fullname)
                if self.direction == PropertyLoader.LEFT:
                    self.syncrules.append((self.parent, source, self.mapper, dest))
                elif self.direction == PropertyLoader.RIGHT:
                    self.syncrules.append((self.mapper, source, self.parent, dest))
                else:
                    raise "assert failed"
            else:
                colmap = {binary.left.table : binary.left, binary.right.table : binary.right}
                if colmap.has_key(self.parent.primarytable) and colmap.has_key(self.target):
                    if self.direction == PropertyLoader.LEFT:
                        self.syncrules.append((self.parent, colmap[self.parent.primarytable], self.mapper, colmap[self.target]))
                    elif self.direction == PropertyLoader.RIGHT:
                        self.syncrules.append((self.mapper, colmap[self.target], self.parent, colmap[self.parent.primarytable]))
                    else:
                        raise "assert failed"
                elif colmap.has_key(self.parent.primarytable) and colmap.has_key(self.secondary):
                    self.syncrules.append((self.parent, colmap[self.parent.primarytable], PropertyLoader.LEFT, colmap[self.secondary]))
                elif colmap.has_key(self.target) and colmap.has_key(self.secondary):
                    self.syncrules.append((self.mapper, colmap[self.target], PropertyLoader.RIGHT, colmap[self.secondary]))

        self.syncrules = []
        processor = BinaryVisitor(compile)
        self.primaryjoin.accept_visitor(processor)
        if self.secondaryjoin is not None:
            self.secondaryjoin.accept_visitor(processor)


    def register_deleted(self, obj, uow):
        if not self.private:
            return

        if self.uselist:
            childlist = uow.attributes.get_history(obj, self.key, passive = False)
        else: 
            childlist = uow.attributes.get_history(obj, self.key)
        for child in childlist.deleted_items() + childlist.unchanged_items():
            if child is not None:
                uow.register_deleted(child)

            
    def register_dependencies(self, uowcommit):
        if self.association is not None:
            uowcommit.register_dependency(self.parent, self.mapper)
            uowcommit.register_dependency(self.association, self.parent)
            uowcommit.register_processor(self.parent, self, self.parent, False)
            uowcommit.register_processor(self.parent, self, self.parent, True)
        elif self.direction == PropertyLoader.CENTER:
            # with many-to-many, set the parent as dependent on us, then the 
            # list of associations as dependent on the parent
            # if only a list changes, the parent mapper is the only mapper that
            # gets added to the "todo" list
            uowcommit.register_dependency(self.mapper, self.parent)
            uowcommit.register_processor(self.parent, self, self.parent, False)
            uowcommit.register_processor(self.parent, self, self.parent, True)
        elif self.direction == PropertyLoader.LEFT:
            uowcommit.register_dependency(self.parent, self.mapper)
            uowcommit.register_processor(self.parent, self, self.parent, False)
            uowcommit.register_processor(self.parent, self, self.parent, True)
        elif self.direction == PropertyLoader.RIGHT:
            uowcommit.register_dependency(self.mapper, self.parent)
            uowcommit.register_processor(self.mapper, self, self.parent, False)
        else:
            raise " no foreign key ?"

    def get_object_dependencies(self, obj, uowcommit, passive = True):
        return uowcommit.uow.attributes.get_history(obj, self.key, passive = passive)

    def whose_dependent_on_who(self, obj1, obj2):
        if obj1 is obj2:
            return None
        elif self.direction == PropertyLoader.LEFT:
            return (obj1, obj2)
        else:
            return (obj2, obj1)
            
    def process_dependencies(self, task, deplist, uowcommit, delete = False):
        #print self.mapper.table.name + " " + self.key + " " + repr(len(deplist)) + " process_dep isdelete " + repr(delete) + " direction " + repr(self.direction)

        def getlist(obj, passive=True):
            return self.get_object_dependencies(obj, uowcommit, passive)

        # plugin point
        
        if self.direction == PropertyLoader.CENTER:
            secondary_delete = []
            secondary_insert = []
            if delete:
                for obj in deplist:
                    childlist = getlist(obj, False)
                    for child in childlist.deleted_items() + childlist.unchanged_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_delete.append(associationrow)
                    uowcommit.register_deleted_list(childlist)
            else:
                for obj in deplist:
                    childlist = getlist(obj)
                    if childlist is None: continue
                    for child in childlist.added_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_insert.append(associationrow)
                    for child in childlist.deleted_items():
                        associationrow = {}
                        self._synchronize(obj, child, associationrow, False)
                        secondary_delete.append(associationrow)
                    uowcommit.register_saved_list(childlist)
            if len(secondary_delete):
                # TODO: precompile the delete/insert queries and store them as instance variables
                # on the PropertyLoader
                statement = self.secondary.delete(sql.and_(*[c == sql.bindparam(c.key) for c in self.secondary.c]))
                statement.execute(*secondary_delete)
            if len(secondary_insert):
                statement = self.secondary.insert()
                statement.execute(*secondary_insert)
        elif self.direction == PropertyLoader.RIGHT and delete:
            # head object is being deleted, and we manage a foreign key object.
            # dont have to do anything to it.
            pass
        elif self.direction == PropertyLoader.LEFT and delete:
            # head object is being deleted, and we manage its list of child objects
            # the child objects have to have their foreign key to the parent set to NULL
            if self.private:
                # if we are privately managed, then all our objects should
                # have been marked as "todelete" already and no attribute adjustment is needed
                return
            for obj in deplist:
                childlist = getlist(obj, False)
                for child in childlist.deleted_items() + childlist.unchanged_items():
                    self._synchronize(obj, child, None, True)
                    uowcommit.register_object(child)
                uowcommit.register_deleted_list(childlist)
        elif self.association is not None:
            # TODO: this is new code, for managing "association objects".
            # its probably glitchy.
            for obj in deplist:
                childlist = getlist(obj, passive=True)
                if childlist is None: continue
                uowcommit.register_saved_list(childlist)
                
                d = {}
                for child in childlist:
                    self._synchronize(obj, child, None, False)
                    key = self.mapper.instance_key(child)
                    d[key] = child
                    uowcommit.unregister_object(child)

                for child in childlist.added_items():
                    uowcommit.register_object(child)
                    key = self.mapper.instance_key(child)
                    d[key] = child
                    
                for child in childlist.unchanged_items():
                    key = self.mapper.instance_key(child)
                    o = d[key]
                    o._instance_key= key
                    
                for child in childlist.deleted_items():
                    key = self.mapper.instance_key(child)
                    if d.has_key(key):
                        o = d[key]
                        o._instance_key = key
                        uowcommit.unregister_object(child)
                    else:
                        uowcommit.register_object(child, isdelete=True)
        else:
            for obj in deplist:
                if self.direction == PropertyLoader.RIGHT:
                    uowcommit.register_object(obj)
                childlist = getlist(obj, passive=True)
                if childlist is None: continue
                uowcommit.register_saved_list(childlist)
                for child in childlist.added_items():
                    self._synchronize(obj, child, None, False)
                    if self.direction == PropertyLoader.LEFT:
                        uowcommit.register_object(child)
                if self.direction != PropertyLoader.RIGHT or len(childlist.added_items()) == 0:
                    for child in childlist.deleted_items():
                        if not self.private:
                            # TODO: we arent sync'ing if this child object
                            # is to be deleted.  this is because if its an "association"
                            # object, it needs its data in order to be located.  
                            # need more explicit support for "association" objects.
                            self._synchronize(obj, child, None, True)
                        if self.direction == PropertyLoader.LEFT:
                            uowcommit.register_object(child, isdelete=self.private)

                
    def _synchronize(self, obj, child, associationrow, clearkeys):
        if self.direction == PropertyLoader.LEFT:
            source = obj
            dest = child
        elif self.direction == PropertyLoader.RIGHT:
            source = child
            dest = obj
        elif self.direction == PropertyLoader.CENTER:
            source = None
            dest = associationrow

        for rule in self.syncrules:
            localsource = source
            (smapper, scol, dmapper, dcol) = rule
            if localsource is None:
                if dmapper == PropertyLoader.LEFT:
                    localsource = obj
                elif dmapper == PropertyLoader.RIGHT:
                    localsource = child

            if clearkeys:
                value = None
            else:
                value = smapper._getattrbycolumn(localsource, scol)

            if dest is associationrow:
                associationrow[dcol.key] = value
            else:
                dmapper._setattrbycolumn(dest, dcol, value)

    def execute(self, instance, row, identitykey, imap, isnew):
        if self._is_primary():
            return
        #print "PLAIN PROPLOADER EXEC NON-PRIAMRY", repr(id(self)), repr(self.mapper.class_), self.key
        objectstore.global_attributes.create_history(instance, self.key, self.uselist)

class LazyLoader(PropertyLoader):
    def init(self, key, parent):
        PropertyLoader.init(self, key, parent)
        (self.lazywhere, self.lazybinds) = create_lazy_clause(self.parent.table, self.primaryjoin, self.secondaryjoin, self.foreignkey)

    def _set_class_attribute(self, class_, key):
        # establish a class-level lazy loader on our class
        #print "SETCLASSATTR LAZY", repr(class_), key
        objectstore.global_attributes.register_attribute(class_, key, uselist = self.uselist, deleteremoved = self.private, live=self.live, callable_=lambda i: self.setup_loader(i))

    def setup_loader(self, instance):
        def lazyload():
            params = {}
            allparams = True
            #print "setting up loader, lazywhere", str(self.lazywhere)
            for col, bind in self.lazybinds.iteritems():
                params[bind.key] = self.parent._getattrbycolumn(instance, col)
                if params[bind.key] is None:
                    allparams = False
                    break
            if allparams:
                if self.secondary is not None:
                    order_by = [self.secondary.rowid_column]
                else:
                    order_by = []
                result = self.mapper.select(self.lazywhere, order_by=order_by,**params)
            else:
                result = []
            if self.uselist:
                return result
            else:
                if len(result):
                    return result[0]
                else:
                    return None
        return lazyload
        
    def execute(self, instance, row, identitykey, imap, isnew):
        if isnew:
            # new object instance being loaded from a result row
            if not self._is_primary():
                #print "EXEC NON-PRIAMRY", repr(self.mapper.class_), self.key
                # we are not the primary manager for this attribute on this class - set up a per-instance lazyloader,
                # which will override the class-level behavior
                objectstore.global_attributes.create_history(instance, self.key, self.uselist, callable_=self.setup_loader(instance))
            else:
                #print "EXEC PRIMARY", repr(self.mapper.class_), self.key
                # we are the primary manager for this attribute on this class - reset its per-instance attribute state, 
                # so that the class-level lazy loader is executed when next referenced on this instance.
                # this usually is not needed unless the constructor of the object referenced the attribute before we got 
                # to load data into it.
                objectstore.global_attributes.reset_history(instance, self.key)
 
def create_lazy_clause(table, primaryjoin, secondaryjoin, foreignkey):
    binds = {}
    def visit_binary(binary):
        circular = isinstance(binary.left, schema.Column) and isinstance(binary.right, schema.Column) and binary.left.table is binary.right.table
        if isinstance(binary.left, schema.Column) and ((not circular and binary.left.table is table) or (circular and foreignkey is binary.right)):
            binary.left = binds.setdefault(binary.left,
                    sql.BindParamClause(table.name + "_" + binary.left.name, None, shortname = binary.left.name))
            binary.swap()

        if isinstance(binary.right, schema.Column) and ((not circular and binary.right.table is table) or (circular and foreignkey is binary.left)):
            binary.right = binds.setdefault(binary.right,
                    sql.BindParamClause(table.name + "_" + binary.right.name, None, shortname = binary.right.name))
                    
    if secondaryjoin is not None:
        lazywhere = sql.and_(primaryjoin, secondaryjoin)
    else:
        lazywhere = primaryjoin
    lazywhere = lazywhere.copy_container()
    li = BinaryVisitor(visit_binary)
    lazywhere.accept_visitor(li)
    return (lazywhere, binds)
        

class EagerLoader(PropertyLoader):
    """loads related objects inline with a parent query."""
    def init(self, key, parent):
        PropertyLoader.init(self, key, parent)
        
        # figure out tables in the various join clauses we have, because user-defined
        # whereclauses that reference the same tables will be converted to use
        # aliases of those tables
        self.to_alias = util.HashSet()
        [self.to_alias.append(f) for f in self.primaryjoin._get_from_objects()]
        if self.secondaryjoin is not None:
            [self.to_alias.append(f) for f in self.secondaryjoin._get_from_objects()]
        del self.to_alias[parent.primarytable]

    def setup(self, key, statement, **options):
        """add a left outer join to the statement thats being constructed"""

        if statement.whereclause is not None:
            # "aliasize" the tables referenced in the user-defined whereclause to not 
            # collide with the tables used by the eager load
            # note that we arent affecting the mapper's table, nor our own primary or secondary joins
            aliasizer = Aliasizer(*self.to_alias)
            statement.whereclause.accept_visitor(aliasizer)
            for alias in aliasizer.aliases.values():
                statement.append_from(alias)

        if hasattr(statement, '_outerjoin'):
            towrap = statement._outerjoin
        else:
            towrap = self.parent.table

        if self.secondaryjoin is not None:
            statement._outerjoin = sql.outerjoin(towrap, self.secondary, self.primaryjoin).outerjoin(self.target, self.secondaryjoin)
            statement.order_by(self.secondary.rowid_column)
        else:
            statement._outerjoin = towrap.outerjoin(self.target, self.primaryjoin)
            statement.order_by(self.target.rowid_column)

        statement.append_from(statement._outerjoin)
        statement.append_column(self.target)
        for key, value in self.mapper.props.iteritems():
            if value is self:
                raise "Cant use eager loading on a self-referential mapper relationship " + str(self.mapper) + " " + key + repr(self.mapper.props)
            value.setup(key, statement)

    def execute(self, instance, row, identitykey, imap, isnew):
        """receive a row.  tell our mapper to look for a new object instance in the row, and attach
        it to a list on the parent instance."""
        
        if isnew:
            # new row loaded from the database.  initialize a blank container on the instance.
            # this will override any per-class lazyloading type of stuff.
            h = objectstore.global_attributes.create_history(instance, self.key, self.uselist)
            
        if not self.uselist:
            if isnew:
                h.setattr(self.mapper._instance(row, imap))
            return
        elif isnew:
            result_list = h
        else:
            result_list = getattr(instance, self.key)
            
        self.mapper._instance(row, imap, result_list)

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
        ext.next = mapper.extension
        mapper.extension = ext
        
class EagerLazyOption(MapperOption):
    """an option that switches a PropertyLoader to be an EagerLoader or LazyLoader"""
    def __init__(self, key, toeager = True):
        self.key = key
        self.toeager = toeager

    def hash_key(self):
        return "EagerLazyOption(%s, %s)" % (repr(self.key), repr(self.toeager))

    def process(self, mapper):

        tup = self.key.split('.', 1)
        key = tup[0]
        oldprop = mapper.props[key]

        if len(tup) > 1:
            submapper = mapper.props[key].mapper
            submapper = submapper.options(EagerLazyOption(tup[1], self.toeager))
        else:
            submapper = oldprop.mapper
        
        if self.toeager:
            class_ = EagerLoader
        elif self.toeager is None:
            class_ = PropertyLoader
        else:
            class_ = LazyLoader
        mapper.set_property(key, class_(submapper, oldprop.secondary, primaryjoin = oldprop.primaryjoin, secondaryjoin = oldprop.secondaryjoin, foreignkey=oldprop.foreignkey, uselist=oldprop.uselist, private=oldprop.private, live=oldprop.live, isoption=True ))

class Aliasizer(sql.ClauseVisitor):
    """converts a table instance within an expression to be an alias of that table."""
    def __init__(self, *tables):
        self.tables = {}
        for t in tables:
            self.tables[t] = t
        self.binary = None
        self.match = False
        self.aliases = {}
        
    def get_alias(self, table):
        try:
            return self.aliases[table]
        except:
            aliasname = table.name + "_" + hex(random.randint(0, 65535))[2:]
            return self.aliases.setdefault(table, sql.alias(table, aliasname))
            
    def visit_binary(self, binary):
        if isinstance(binary.left, schema.Column) and self.tables.has_key(binary.left.table):
            binary.left = self.get_alias(binary.left.table).c[binary.left.name]
            self.match = True
        if isinstance(binary.right, schema.Column) and self.tables.has_key(binary.right.table):
            binary.right = self.get_alias(binary.right.table).c[binary.right.name]
            self.match = True

class TableFinder(sql.ClauseVisitor):
    """given a Clause, locates all the Tables within it into a list."""
    def __init__(self):
        self.tables = []
    def visit_table(self, table):
        self.tables.append(table)

class BinaryVisitor(sql.ClauseVisitor):
    def __init__(self, func):
        self.func = func
    def visit_binary(self, binary):
        self.func(binary)
        

class MapperExtension(object):
    def __init__(self):
        self.next = None
    def create_instance(self, mapper, row, imap, class_):
        if self.next is None:
            return None
        else:
            return self.next.create_instance(mapper, row, imap, class_)
    def append_result(self, mapper, row, imap, result, instance, isnew, populate_existing=False):
        if self.next is None:
            return True
        else:
            return self.next.append_result(mapper, row, imap, result, instance, isnew, populate_existing)
    def after_insert(self, mapper, instance):
        if self.next is not None:
            self.next.after_insert(mapper, instance)
    def before_delete(self, mapper, instance):
        if self.next is not None:
            self.next.before_delete(mapper, instance)
        
def hash_key(obj):
    if obj is None:
        return 'None'
    elif hasattr(obj, 'hash_key'):
        return obj.hash_key()
    else:
        return repr(obj)
        
def mapper_hash_key(class_, table, primarytable = None, properties = None, scope = "thread", **kwargs):
    if properties is None:
        properties = {}
    return (
        "Mapper(%s, %s, primarytable=%s, properties=%s, scope=%s)" % (
            repr(class_),
            hash_key(table),
            hash_key(primarytable),
            repr(dict([(k, hash_key(p)) for k,p in properties.iteritems()])),
            scope        )
    )

