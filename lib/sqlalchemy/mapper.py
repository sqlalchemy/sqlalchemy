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

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.util as util
import sqlalchemy.objectstore as objectstore
import random, copy, types

__ALL__ = ['eagermapper', 'eagerloader', 'lazymapper', 'lazyloader', 'eagerload', 'lazyload', 'mapper', 'lazyloader', 'lazymapper', 'clear_mappers']

def relation(*args, **params):
    if isinstance(args[0], Mapper):
        return relation_loader(*args, **params)
    else:
        return relation_mapper(*args, **params)

def relation_loader(mapper, secondary = None, primaryjoin = None, secondaryjoin = None, lazy = True, **options):
    if lazy:
        return LazyLoader(mapper, secondary, primaryjoin, secondaryjoin, **options)
    else:
        return EagerLoader(mapper, secondary, primaryjoin, secondaryjoin, **options)
    
def relation_mapper(class_, selectable, secondary = None, primaryjoin = None, secondaryjoin = None, table = None, properties = None, lazy = True, uselist = True, foreignkey = None, primary_keys = None, **options):
    return relation_loader(mapper(class_, selectable, table=table, properties=properties, primary_keys=primary_keys, **options), secondary, primaryjoin, secondaryjoin, lazy = lazy, uselist = uselist, foreignkey = foreignkey, **options)

# TODO: where do we want to register these mappers, register them against their classes/objects etc
_mappers = {}
def mapper(*args, **params):
    hashkey = mapper_hash_key(*args, **params)
    #print "HASHKEY: " + hashkey
    try:
        return _mappers[hashkey]
    except KeyError:
        m = Mapper(hashkey, *args, **params)
        return _mappers.setdefault(hashkey, m)

def clear_mappers():
    _mappers.clear()
        
def eagerload(name):
    return EagerLazySwitcher(name, toeager = True)

def lazyload(name):
    return EagerLazySwitcher(name, toeager = False)

def object_mapper(object):
    try:
        return _mappers[object._mapper]
    except AttributeError:
        try:
            return _mappers[object.__class__._mapper]
        except AttributeError:
            raise "Object " + object.__class__.__name__ + "/" + repr(id(object)) + " has no mapper specified"
        
class Mapper(object):
    def __init__(self, hashkey, class_, selectable, table = None, scope = "thread", properties = None, primary_keys = None, **kwargs):
        self.hashkey = hashkey
        self.class_ = class_
        self.scope = scope
        self.selectable = selectable
        tf = TableFinder()
        self.selectable.accept_visitor(tf)
        self.tables = tf.tables
        self.primary_keys = {}

        if table is None:
            if len(self.tables) > 1:
                raise "Selectable contains multiple tables - specify primary table argument to Mapper"
            self.table = self.tables[0]
        else:
            self.table = table

        if primary_keys is not None:
            for k in primary_keys:
                self.primary_keys.setdefault(k.table, []).append(k)
                self.primary_keys.setdefault(self.selectable, []).append(k)
        else:
            for t in self.tables + [self.selectable]:
                try:
                    list = self.primary_keys[t]
                except KeyError:
                    list = self.primary_keys.setdefault(t, util.HashSet())
                if not len(t.primary_keys):
                    raise "Table " + t.name + " has no primary keys. Specify primary_keys argument to mapper."
                for k in t.primary_keys:
                    list.append(k)

        # object attribute names mapped to MapperProperty objects
        self.props = {}
        
        # table columns mapped to lists of MapperProperty objects
        # using a list allows a single column to be defined as 
        # populating multiple object attributes
        self.columntoproperty = {}
        
        # the original properties argument to match against similar 
        # arguments, for caching purposes
        self.properties = properties

        # load custom properties 
        if self.properties is not None:
            for key, prop in self.properties.iteritems():
                self.props[key] = prop
                if isinstance(prop, ColumnProperty):
                    for col in prop.columns:
                        proplist = self.columntoproperty.setdefault(col.original, [])
                        proplist.append(prop)

        # load properties from the main Selectable object,
        # not overriding those set up in the 'properties' argument
        for column in self.selectable.columns:
            if self.columntoproperty.has_key(column.original):
                continue
                
            prop = self.props.get(column.key, None)
            if prop is None:
                prop = ColumnProperty(column)
                self.props[column.key] = prop
            elif isinstance(prop, ColumnProperty):
                prop.columns.append(column)
            else:
                continue
        
            # its a ColumnProperty - match the ultimate table columns
            # back to the property
            proplist = self.columntoproperty.setdefault(column.original, [])
            proplist.append(prop)

        self.init()

    engines = property(lambda s: [t.engine for t in s.tables])

    def hash_key(self):
        return self.hashkey
        
    def set_property(self, key, prop):
        self.props[key] = prop
        prop.init(key, self)

    def init(self):
        [prop.init(key, self) for key, prop in self.props.iteritems()]
        # TODO: get some notion of "primary mapper" going so multiple mappers dont collide
        self.class_._mapper = self.hashkey

    def instances(self, cursor, db = None):
        result = util.HistoryArraySet()
        cursor = engine.ResultProxy(cursor, echo = db and db.echo)
        imap = {}
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            #self._instance(row, result)
            self._instance(row, imap, result)
        
        # store new stuff in the identity map
        for value in imap.values():
            objectstore.uow().register_clean(value)
            
        return result

    def get(self, *ident):
        """returns an instance of the object based on the given identifier, or None
        if not found.  The *ident argument is a 
        list of primary keys in the order of the table def's primary keys."""
        key = objectstore.get_id_key(ident, self.class_, self.table)
        try:
            return objectstore.get(key)
        except KeyError:
            clause = sql.and_()
            i = 0
            for primary_key in self.primary_keys[self.table]:
                # appending to the and_'s clause list directly to skip
                # typechecks etc.
                clause.clauses.append(primary_key == ident[i])
                i += 2
            try:
                return self.select(clause)[0]
            except IndexError:
                return None

    def put(self, instance):
        key = self.identity_key(instance)
        objectstore.put(key, instance, self.scope)
        return key

    def identity_key(self, instance):
        return objectstore.get_id_key(tuple([self._getattrbycolumn(instance, column) for column in self.primary_keys[self.selectable]]), self.class_, self.table)

    def compile(self, whereclause = None, **options):
        """works like select, except returns the SQL statement object without 
        compiling or executing it"""
        return self._compile(whereclause, **options)

    def options(self, *options):
        """uses this mapper as a prototype for a new mapper with different behavior.
        *options is a list of options directives, which include eagerload() and lazyload()"""

        hashkey = hash_key(self) + "->" + repr([hash_key(o) for o in options])
        #print "HASHKEY: " + hashkey
        try:
            return _mappers[hashkey]
        except KeyError:
            mapper = copy.copy(self)
            for option in options:
                option.process(mapper)
            return _mappers.setdefault(hashkey, mapper)

    
    def select(self, arg = None, **params):
        """selects instances of the object from the database.  
        
        arg can be any ClauseElement, which will form the criterion with which to
        load the objects.
        
        For more advanced usage, arg can also be a Select statement object, which
        will be executed and its resulting rowset used to build new object instances.  
        in this case, the developer must insure that an adequate set of columns exists in the 
        rowset with which to build new object instances."""
        if arg is not None and isinstance(arg, sql.Select):
            return self._select_statement(arg, **params)
        else:
            return self._select_whereclause(arg, **params)

    def _getattrbycolumn(self, obj, column):
        return self.columntoproperty[column][0].getattr(obj)

    def _setattrbycolumn(self, obj, column, value):
        self.columntoproperty[column][0].setattr(obj, value)

    def save_obj(self, objects, uow):
        # try to get inserts to be en-masse with the "guess-the-id" thing maybe
                
        for table in self.tables:
            # loop thru tables in the outer loop, objects on the inner loop.
            # this is important for an object represented across two tables
            # so that it gets its primary keys populated for the benefit of the
            # second table.
            insert = []
            update = []
            for obj in objects:
                
#                print "SAVE_OBJ we are " + hash_key(self) + " obj: " +  obj.__class__.__name__ + repr(id(obj))
                params = {}
                for col in table.columns:
                    params[col.key] = self._getattrbycolumn(obj, col)

                if hasattr(obj, "_instance_key"):
                    update.append(params)
                else:
                    insert.append((obj, params))
                uow.register_saved_object(obj)
            if len(update):
                clause = sql.and_()
                for col in self.primary_keys[table]:
                    clause.clauses.append(col == sql.bindparam(col.key))
                statement = table.update(clause)
                c = statement.execute(*update)
                if c.rowcount != len(update):
                    raise "ConcurrencyError - updated rowcount does not match number of objects updated"
            
            if len(insert):
                statement = table.insert()
                for rec in insert:
                    (obj, params) = rec
                    statement.execute(**params)
                    primary_key = table.engine.last_inserted_ids()[0]
                    found = False
                    for col in self.primary_keys[table]:
                        if self._getattrbycolumn(obj, col) is None:
                            if found:
                                raise "Only one primary key per inserted row can be set via autoincrement/sequence"
                            else:
                                self._setattrbycolumn(obj, col, primary_key)
                                found = True

    def register_dependencies(self, obj, uow):
        for prop in self.props.values():
            prop.register_dependencies(obj, uow)
            
    def transaction(self, f):
        return self.table.engine.multi_transaction(self.tables, f)

    def remove(self, obj, traverse = True):
        """removes the object.  traverse indicates attached objects should be removed as well."""
        pass

    def _compile(self, whereclause = None, order_by = None, **options):
        statement = sql.select([self.selectable], whereclause, order_by = order_by)
        for key, value in self.props.iteritems():
            value.setup(key, statement, **options) 
        statement.use_labels = True
        return statement

    def _select_whereclause(self, whereclause = None, order_by = None, **params):
        statement = self._compile(whereclause, order_by = order_by)
        return self._select_statement(statement, **params)

    def _select_statement(self, statement, **params):
        statement.use_labels = True
        return self.instances(statement.execute(**params), statement.engine)

    def _identity_key(self, row):
        return objectstore.get_row_key(row, self.class_, self.table, self.primary_keys[self.selectable])

    def _instance(self, row, imap, result = None, populate_existing = False):
        """pulls an object instance from the given row and appends it to the given result list.
        if the instance already exists in the given identity map, its not added.  in either
        case, executes all the property loaders on the instance to also process extra information
        in the row."""

        # look in main identity map.  if its there, we dont do anything to it,
        # including modifying any of its related items lists, as its already
        # been exposed to being modified by the application.
        identitykey = self._identity_key(row)
        if objectstore.has_key(identitykey):
            instance = objectstore.get(identitykey)
            if result is not None:
                result.append_nohistory(instance)

            if populate_existing:
                isnew = not imap.has_key(identitykey)
                if isnew:
                    imap[identitykey] = instance
                for prop in self.props.values():
                    prop.execute(instance, row, identitykey, imap, isnew)

            return instance
                    
        # look in result-local identitymap for it.
        exists = imap.has_key(identitykey)      
        if not exists:
            # check if primary keys in the result are None - this indicates 
            # an instance of the object is not present in the row
            for col in self.primary_keys[self.selectable]:
                if row[col.label] is None:
                    return None
            instance = self.class_()
            instance._mapper = self.hashkey
            instance._instance_key = identitykey

            imap[identitykey] = instance
            isnew = True
        else:
            instance = imap[identitykey]
            isnew = False

        if result is not None:
            result.append_nohistory(instance)
            
        # call further mapper properties on the row, to pull further 
        # instances from the row and possibly populate this item.
        for prop in self.props.values():
            prop.execute(instance, row, identitykey, imap, isnew)
            
        return instance

    def rollback(self, obj):
        objectstore.uow().rollback_object(obj)
        
class MapperOption:
    """describes a modification to a Mapper in the context of making a copy
    of it.  This is used to assist in the prototype pattern used by mapper.options()."""
    def process(self, mapper):
        raise NotImplementedError()
    def hash_key(self):
        return repr(self)

class MapperProperty:
    """an element attached to a Mapper that describes and assists in the loading and saving 
    of an attribute on an object instance."""
    def execute(self, instance, row, identitykey, imap, isnew):
        """called when the mapper receives a row.  instance is the parent instance corresponding
        to the row. """
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
    def delete(self, object):
        """called when the instance is being deleted"""
        pass
    def register_dependencies(self, obj, uow):
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

    def init(self, key, parent):
        self.key = key
        # establish a SmartProperty property manager on the object for this key
        if not hasattr(parent.class_, key):
            objectstore.uow().register_attribute(parent.class_, key, uselist = False)

    def execute(self, instance, row, identitykey, imap, isnew):
        if isnew:
            instance.__dict__[self.key] = row[self.columns[0].label]
            #setattr(instance, self.key, row[self.columns[0].label])
        


class PropertyLoader(MapperProperty):
    """describes an object property that holds a single item or list of items that correspond to a related
    database table."""
    def __init__(self, mapper, secondary, primaryjoin, secondaryjoin, uselist = True, foreignkey = None, private = False):
        self.uselist = uselist
        self.mapper = mapper
        self.target = self.mapper.selectable
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.foreignkey = foreignkey
        self.private = private
        self._hash_key = "%s(%s, %s, %s, %s, %s, uselist=%s)" % (self.__class__.__name__, hash_key(mapper), hash_key(secondary), hash_key(primaryjoin), hash_key(secondaryjoin), hash_key(foreignkey), repr(self.uselist))
            
    def hash_key(self):
        return self._hash_key

    def init(self, key, parent):
        self.key = key
        self.parent = parent
        
        # if join conditions were not specified, figure them out based on foreign keys
        if self.secondary is not None:
            if self.secondaryjoin is None:
                self.secondaryjoin = self.match_primaries(self.target, self.secondary)
            if self.primaryjoin is None:
                self.primaryjoin = self.match_primaries(parent.selectable, self.secondary)
        else:
            if self.primaryjoin is None:
                self.primaryjoin = self.match_primaries(parent.selectable, self.target)
        
        # if the foreign key wasnt specified and theres no assocaition table, try to figure
        # out who is dependent on who. we dont need all the foreign keys represented in the join,
        # just one of them.  
        if self.foreignkey is None and self.secondaryjoin is None:
            # else we usually will have a one-to-many where the secondary depends on the primary
            # but its possible that its reversed
            w = PropertyLoader.FindDependent()
            self.primaryjoin.accept_visitor(w)
            if w.dependent is None:
                raise "cant determine primary foreign key in the join relationship....specify foreignkey=<column>"
            else:
                self.foreignkey = w.dependent
                
        if not hasattr(parent.class_, key):
            objectstore.uow().register_attribute(parent.class_, key, uselist = self.uselist)

    class FindDependent(sql.ClauseVisitor):
        def __init__(self):
            self.dependent = None
        def visit_binary(self, binary):
            if binary.operator == '=':
                if isinstance(binary.left, schema.Column) and binary.left.primary_key:
                    if self.dependent is binary.left:
                        raise "bidirectional dependency not supported...specify foreignkey"
                    self.dependent = binary.right
                elif isinstance(binary.right, schema.Column) and binary.right.primary_key:
                    if self.dependent is binary.right:
                        raise "bidirectional dependency not supported...specify foreignkey"
                    self.dependent = binary.left
                

    def match_primaries(self, primary, secondary):
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
            raise "Cant find any foreign key relationships between " + primary.table.name + " and " + secondary.table.name
        elif len(crit) == 1:
            return (crit[0])
        else:
            return sql.and_(crit)
            
    def register_dependencies(self, objlist, uow):
        if self.secondaryjoin is not None:
            # with many-to-many, set the parent as dependent on us, then the 
            # list of associations as dependent on the parent
            # if only a list changes, the parent mapper is the only mapper that
            # gets added to the "todo" list
            uow.register_dependency(self.mapper, self.parent, None, None)
            uow.register_dependency(self.parent, None, self, objlist)
        elif self.foreignkey.table == self.target:
            uow.register_dependency(self.parent, self.mapper, self, objlist)
        elif self.foreignkey.table == self.parent.table:
            uow.register_dependency(self.mapper, self.parent, self, objlist)
        else:
            raise " no foreign key ?"

    def process_dependencies(self, deplist, uowcommit):

        def getlist(obj):
            if self.uselist:
                return uowcommit.uow.attributes.get_list_history(obj, self.key)
            else: 
                return uowcommit.uow.attributes.get_history(obj, self.key)

        clearkeys = False
        
        def sync_foreign_keys(binary):
            self._sync_foreign_keys(binary, obj, child, associationrow, clearkeys)
        setter = BinaryVisitor(sync_foreign_keys)

        if self.secondaryjoin is not None:
            secondary_delete = []
            secondary_insert = []
            for obj in deplist:
                childlist = getlist(obj)
                for child in childlist.added_items():
                    associationrow = {}
                    self.primaryjoin.accept_visitor(setter)
                    self.secondaryjoin.accept_visitor(setter)
                    secondary_insert.append(associationrow)
                for child in childlist.deleted_items():
                    associationrow = {}
                    clearkeys = True
                    self.primaryjoin.accept_visitor(setter)
                    self.secondaryjoin.accept_visitor(setter)
                    secondary_delete.append(associationrow)
                    if self.private:
                        uowcommit.add_item_to_delete(obj)
                uowcommit.register_saved_list(childlist)
            if len(secondary_delete):
                statement = self.secondary.delete(sql.and_(*[c == sql.bindparam(c.key) for c in self.secondary.c]))
                statement.execute(*secondary_delete)
            if len(secondary_insert):
                statement = self.secondary.insert()
                statement.execute(*secondary_insert)
        elif self.foreignkey.table == self.target:
            for obj in deplist:
                childlist = getlist(obj)
                clearkeys = False
                for child in childlist.added_items():
                    associationrow = {}
                    self.primaryjoin.accept_visitor(setter)
                    uowcommit.register_saved_list(childlist)
                clearkeys = True
                for child in childlist.deleted_items():
                     associationrow = {}
                     self.primaryjoin.accept_visitor(setter)
                     uowcommit.register_saved_list(childlist)
                     if self.private:
                         uowcommit.add_item_to_delete(child)
        elif self.foreignkey.table == self.parent.table:
            for child in deplist:
                childlist = getlist(child)
                clearkeys = False
                for obj in childlist.added_items():
                    associationrow = {}
                    self.primaryjoin.accept_visitor(setter)
                    uowcommit.register_saved_list(childlist)
                clearkeys = True
                for obj in childlist.deleted_items():
                    if self.private:
                        uowcommit.add_item_to_delete(obj)
                    associationrow = {}
                    self.primaryjoin.accept_visitor(setter)
                    uowcommit.register_saved_list(childlist)
        else:
            raise " no foreign key ?"

    def _sync_foreign_keys(self, binary, obj, child, associationrow, clearkeys):
        """given a binary clause with an = operator joining two table columns, synchronizes the values 
        of the corresponding attributes within a parent object and a child object, or the attributes within an 
        an "association row" that represents an association link between the 'parent' and 'child' object."""
        if binary.operator == '=':
            colmap = {binary.left.table : binary.left, binary.right.table : binary.right}
            if colmap.has_key(self.parent.table) and colmap.has_key(self.target):
                #print "set " + repr(child) + ":" + colmap[self.target].key + " to " + repr(obj) + ":" + colmap[self.parent.table].key
                if clearkeys:
                    self.mapper._setattrbycolumn(child, colmap[self.target], None)
                else:
                    self.mapper._setattrbycolumn(child, colmap[self.target], self.parent._getattrbycolumn(obj, colmap[self.parent.table]))
            elif colmap.has_key(self.parent.table) and colmap.has_key(self.secondary):
                associationrow[colmap[self.secondary].key] = self.parent._getattrbycolumn(obj, colmap[self.parent.table])
            elif colmap.has_key(self.target) and colmap.has_key(self.secondary):
                associationrow[colmap[self.secondary].key] = self.mapper._getattrbycolumn(child, colmap[self.target])
            
    def delete(self):
        self.mapper.delete()


class LazyLoader(PropertyLoader):

    def init(self, key, parent):
        PropertyLoader.init(self, key, parent)
        if self.secondaryjoin is not None:
            self.lazywhere = sql.and_(self.primaryjoin, self.secondaryjoin)
        else:
            self.lazywhere = self.primaryjoin

        # we dont want to screw with the primaryjoin and secondary join of the PropertyLoader,
        # so create a copy
        self.lazywhere = self.lazywhere.copy_container()
        self.binds = {}
        li = BinaryVisitor(lambda b: self._create_lazy_clause(b, self.binds))
        self.lazywhere.accept_visitor(li)

    def execute(self, instance, row, identitykey, imap, isnew):
        if isnew:
            # TODO: get lazy callables to be stored within the unit of work?
            # allows serializable ?  still need lazyload state to exist in the application
            # when u deserialize tho
            objectstore.uow().attribute_set_callable(instance, self.key, LazyLoadInstance(self, row))

    def _create_lazy_clause(self, binary, binds):
        if isinstance(binary.left, schema.Column) and binary.left.table == self.parent.selectable:
            binary.left = binds.setdefault(self.parent.selectable.name + "_" + binary.left.name,
                    sql.BindParamClause(self.parent.selectable.name + "_" + binary.left.name, None, shortname = binary.left.name))

        if isinstance(binary.right, schema.Column) and binary.right.table == self.parent.selectable:
            binary.right = binds.setdefault(self.parent.selectable.name + "_" + binary.right.name,
                    sql.BindParamClause(self.parent.selectable.name + "_" + binary.right.name, None, shortname = binary.right.name))

class LazyLoadInstance(object):
    """attached to a specific object instance to load related rows."""
    def __init__(self, lazyloader, row):
        self.params = {}
        for key, value in lazyloader.binds.iteritems():
            self.params[key] = row[key]
        self.mapper = lazyloader.mapper
        self.lazywhere = lazyloader.lazywhere
        self.uselist = lazyloader.uselist
    def __call__(self):
        result = self.mapper.select(self.lazywhere, **self.params)
        if self.uselist:
            return result
        else:
            if len(result):
                return result[0]
            else:
                return None

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
        del self.to_alias[parent.selectable]

    def setup(self, key, statement, **options):
        """add a left outer join to the statement thats being constructed"""

        if statement.whereclause is not None:
            # "aliasize" the tables referenced in the user-defined whereclause to not 
            # collide with the tables used by the eager load
            # note that we arent affecting the mapper's selectable, nor our own primary or secondary joins
            aliasizer = Aliasizer(*self.to_alias)
            statement.whereclause.accept_visitor(aliasizer)
            for alias in aliasizer.aliases.values():
                statement.append_from(alias)

        if hasattr(statement, '_outerjoin'):
            towrap = statement._outerjoin
        else:
            towrap = self.parent.selectable

        if self.secondaryjoin is not None:
            statement._outerjoin = sql.outerjoin(sql.outerjoin(towrap, self.secondary, self.secondaryjoin), self.target, self.primaryjoin)
        else:
            statement._outerjoin = sql.outerjoin(towrap, self.target, self.primaryjoin)

        statement.append_from(statement._outerjoin)
        statement.append_column(self.target)
        for key, value in self.mapper.props.iteritems():
            value.setup(key, statement)

    def execute(self, instance, row, identitykey, imap, isnew):
        """receive a row.  tell our mapper to look for a new object instance in the row, and attach
        it to a list on the parent instance."""
        if not self.uselist:
            # TODO: check for multiple values on single-element child element ?
            instance.__dict__[self.key] = self.mapper._instance(row, imap)
            #setattr(instance, self.key, self.mapper._instance(row, imap))
            return
        elif isnew:
            result_list = []
            setattr(instance, self.key, result_list)
            result_list = getattr(instance, self.key)
            result_list.commit()
        else:
            result_list = getattr(instance, self.key)
            
        self.mapper._instance(row, imap, result_list)
            
class EagerLazySwitcher(MapperOption):
    """an option that switches a PropertyLoader to be an EagerLoader"""
    def __init__(self, key, toeager = True):
        self.key = key
        self.toeager = toeager

    def hash_key(self):
        return "EagerLazySwitcher(%s, %s)" % (repr(self.key), repr(self.toeager))

    def process(self, mapper):
        oldprop = mapper.props[self.key]
        if self.toeager:
            class_ = EagerLoader
        else:
            class_ = LazyLoader
        mapper.set_property(self.key, class_(oldprop.mapper, oldprop.secondary, primaryjoin = oldprop.primaryjoin, secondaryjoin = oldprop.secondaryjoin))

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
        
  
def hash_key(obj):
    if obj is None:
        return 'None'
    else:
        return obj.hash_key()

def mapper_hash_key(class_, selectable, table = None, properties = None, scope = "thread", **kwargs):
    if properties is None:
        properties = {}
    return (
        "Mapper(%s, %s, table=%s, properties=%s, scope=%s)" % (
            repr(class_),
            hash_key(selectable),
            hash_key(table),
            repr(dict([(k, hash_key(p)) for k,p in properties.iteritems()])),
            scope        )
    )



