# mapper/mapper.py
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


import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.util as util
import objectstore


mapper_registry = {}

class Mapper(object):
    """Persists object instances to and from schema.Table objects via the sql package.
    Instances of this class should be constructed through this package's mapper() or
    relation() function."""
    def __init__(self, 
                hashkey, 
                class_, 
                table, 
                primarytable = None, 
                properties = None, 
                primary_key = None, 
                is_primary = False, 
                inherits = None, 
                inherit_condition = None, 
                extension = None,
                **kwargs):

        self.copyargs = {
            'class_':class_,
            'table':table,
            'primarytable':primarytable,
            'properties':properties or {},
            'primary_key':primary_key,
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

        # determine primary key columns, either passed in, or get them from our set of tables
        self.pks_by_table = {}
        if primary_key is not None:
            for k in primary_key:
                self.pks_by_table.setdefault(k.table, []).append(k)
                if k.table != self.table:
                    self.pks_by_table.setdefault(self.table, []).append(k)
        else:
            for t in self.tables + [self.table]:
                try:
                    list = self.pks_by_table[t]
                except KeyError:
                    list = self.pks_by_table.setdefault(t, util.HashSet())
                if not len(t.primary_key):
                    raise "Table " + t.name + " has no primary key columns. Specify primary_key argument to mapper."
                for k in t.primary_key:
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
                
        if not hasattr(self.class_, '_mapper') or self.is_primary or not mapper_registry.has_key(self.class_._mapper) or (inherits is not None and inherits._is_primary_mapper()):
            self._init_class()
       
        
    engines = property(lambda s: [t.engine for t in s.tables])

    def add_property(self, key, prop):
        self.copyargs['properties'][key] = prop
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

    
    def instances(self, cursor, *mappers, **kwargs):
        limit = kwargs.get('limit', None)
        offset = kwargs.get('offset', None)
        
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
        list of primary key columns in the order of the table def's primary key columns."""
        key = objectstore.get_id_key(ident, self.class_, self.primarytable)
        #print "key: " + repr(key) + " ident: " + repr(ident)
        try:
            return objectstore.uow()._get(key)
        except KeyError:
            clause = sql.and_()
            i = 0
            for primary_key in self.pks_by_table[self.primarytable]:
                # appending to the and_'s clause list directly to skip
                # typechecks etc.
                clause.clauses.append(primary_key == ident[i])
                i += 1
            try:
                return self.select(clause)[0]
            except IndexError:
                return None

        
    def identity_key(self, *primary_key):
        return objectstore.get_id_key(tuple(primary_key), self.class_, self.primarytable)
    
    def instance_key(self, instance):
        return self.identity_key(*[self._getattrbycolumn(instance, column) for column in self.pks_by_table[self.table]])

    def compile(self, whereclause = None, **options):
        """works like select, except returns the SQL statement object without 
        compiling or executing it"""
        return self._compile(whereclause, **options)

    def options(self, *options):
        """uses this mapper as a prototype for a new mapper with different behavior.
        *options is a list of options directives, which include eagerload(), lazyload(), and noload()"""

        hashkey = hash_key(self) + "->" + repr([hash_key(o) for o in options])
        try:
            return mapper_registry[hashkey]
        except KeyError:
            mapper = Mapper(hashkey, **self.copyargs)
            mapper._init_properties()

            for option in options:
                option.process(mapper)
            return mapper_registry.setdefault(hashkey, mapper)

    def selectone(self, *args, **params):
        """works like select(), but only returns the first result by itself, or None if no 
        objects returned."""
        ret = self.select(*args, **params)
        if len(ret):
            return ret[0]
        else:
            return None
            
    def select(self, arg = None, **kwargs):
        """selects instances of the object from the database.  
        
        arg can be any ClauseElement, which will form the criterion with which to
        load the objects.
        
        For more advanced usage, arg can also be a Select statement object, which
        will be executed and its resulting rowset used to build new object instances.  
        in this case, the developer must insure that an adequate set of columns exists in the 
        rowset with which to build new object instances."""
        if arg is not None and isinstance(arg, sql.Select):
            return self.select_statement(arg, **kwargs)
        else:
            return self.select_whereclause(arg, **kwargs)

    def select_whereclause(self, whereclause = None, params=None, **kwargs):
        statement = self._compile(whereclause, **kwargs)
        if params is not None:
            return self.select_statement(statement, **params)
        else:
            return self.select_statement(statement)

    def select_statement(self, statement, **params):
        statement.use_labels = True
        return self.instances(statement.execute(**params))

    def select_text(self, text, **params):
        t = sql.text(text, engine=self.primarytable.engine)
        return self.instances(t.execute(**params))

    def _getattrbycolumn(self, obj, column):
        try:
            prop = self.columntoproperty[column.original]
        except KeyError:
            try:
                prop = self.props[column.key]
                raise "Column '%s.%s' is not available, due to conflicting property '%s':%s" % (column.table.name, column.name, column.key, repr(prop))
            except KeyError:
                raise "No column %s.%s is configured on mapper %s..." % (column.table.name, column.name, str(self))
                
        return prop[0].getattr(obj)

    def _setattrbycolumn(self, obj, column, value):
        self.columntoproperty[column.original][0].setattr(obj, value)

        
    def save_obj(self, objects, uow):
        """called by a UnitOfWork object to save objects, which involves either an INSERT or
        an UPDATE statement for each table used by this mapper, for each element of the
        list."""
          
        for table in self.tables:
            # loop thru tables in the outer loop, objects on the inner loop.
            # this is important for an object represented across two tables
            # so that it gets its primary key columns populated for the benefit of the
            # second table.
            insert = []
            update = []
            
            # we have our own idea of the primary key columns 
            # for this table, in the case that the user
            # specified custom primary key cols.
            pk = {}
            for k in self.pks_by_table[table]:
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
                for col in self.pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col.table.name + "_" + col.key))
                statement = table.update(clause)
                c = statement.execute(*update)
                if table.engine.supports_sane_rowcount() and c.rowcount != len(update):
                    raise "ConcurrencyError - updated rowcount %d does not match number of objects updated %d" % (c.cursor.rowcount, len(update))
            if len(insert):
                import sys
                statement = table.insert()
                for rec in insert:
                    (obj, params) = rec
                    statement.execute(**params)
                    primary_key = table.engine.last_inserted_ids()
                    if primary_key is not None:
                        i = 0
                        for col in self.pks_by_table[table]:
                    #        print "col: " + table.name + "." + col.key + " val: " + repr(self._getattrbycolumn(obj, col))
                            if self._getattrbycolumn(obj, col) is None:
                                self._setattrbycolumn(obj, col, primary_key[i])
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
                for col in self.pks_by_table[table]:
                    params[col.key] = self._getattrbycolumn(obj, col)
                uow.register_deleted_object(obj)
                self.extension.before_delete(self, obj)
            if len(delete):
                clause = sql.and_()
                for col in self.pks_by_table[table]:
                    clause.clauses.append(col == sql.bindparam(col.key))
                statement = table.delete(clause)
                c = statement.execute(*delete)
                if table.engine.supports_sane_rowcount() and c.rowcount != len(delete):
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
            
    def _compile(self, whereclause = None, **kwargs):
        if getattr(self, '_has_eager', False) and (kwargs.has_key('limit') or kwargs.has_key('offset')):
            s2 = sql.select(self.table.primary_key, whereclause, **kwargs)
            s2.order_by(self.table.rowid_column)
            s3 = s2.alias('rowcount')
            crit = []
            for i in range(0, len(self.table.primary_key)):
                crit.append(s3.primary_key[i] == self.table.primary_key[i])
            statement = sql.select([self.table], sql.and_(*crit))
            if kwargs.has_key('order_by'):
                statement.order_by(kwargs['order_by'])
        else:
            statement = sql.select([self.table], whereclause, **kwargs)
            statement.order_by(self.table.rowid_column)
        # plugin point
        for key, value in self.props.iteritems():
            value.setup(key, statement, **kwargs) 
        statement.use_labels = True
        return statement

    def _identity_key(self, row):
        return objectstore.get_row_key(row, self.class_, self.primarytable, self.pks_by_table[self.table])

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
            # check if primary key cols in the result are None - this indicates 
            # an instance of the object is not present in the row
            for col in self.pks_by_table[self.table]:
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

class TableFinder(sql.ClauseVisitor):
    """given a Clause, locates all the Tables within it into a list."""
    def __init__(self):
        self.tables = []
    def visit_table(self, table):
        self.tables.append(table)

def hash_key(obj):
    if obj is None:
        return 'None'
    elif isinstance(obj, list):
        return repr([hash_key(o) for o in obj])
    elif hasattr(obj, 'hash_key'):
        return obj.hash_key()
    else:
        return repr(obj)

def mapper_hash_key(class_, table, primarytable = None, properties = None, **kwargs):
    if properties is None:
        properties = {}
    return (
        "Mapper(%s, %s, primarytable=%s, properties=%s)" % (
            repr(class_),
            hash_key(table),
            hash_key(primarytable),
            repr(dict([(k, hash_key(p)) for k,p in properties.iteritems()]))
        )
    )



