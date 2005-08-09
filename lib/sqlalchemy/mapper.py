"""
usermapper = mapper(User, users)

userlist = usermapper.select(usermapper.table.user_id == 10)

userlist = usermapper.select(
        and_(usermapper.table.user_name == 'fred', usermapper.table.user_id == 12)
    )

userlist = usermapper.select("user_id =12 and foo=bar", from_obj=["foo"])

addressmapper = mapper(Address, addresses)

usermapper = mapper(
    User, 
    users, 
    properties = {
        'addresses' : relation(addressmapper, users.c.user_id == addresses.c.user_id, lazy = False),
        'permissions' : relation(Permissions, permissions, users.c.user_id == permissions.c.user_id, lazy = True)
    },
    )

usermapper.select("user_id LIKE "%foo%")

"""

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import sqlalchemy.util as util
import random, copy, types

__ALL__ = ['eagermapper', 'eagerloader', 'lazymapper', 'lazyloader', 'eagerload', 'lazyload', 'mapper', 'lazyloader', 'lazymapper', 'identitymap', 'globalidentity']


def relation(*args, **params):
    #multimethod poverty
    if type(args[0]) == Mapper:
        return relation_loader(*args, **params)
    else:
        return relation_mapper(*args, **params)

def relation_loader(mapper, secondary = None, primaryjoin = None, secondaryjoin = None, lazy = True, **options):
    if lazy:
        return LazyLoader(mapper, secondary, primaryjoin, secondaryjoin, **options)
    else:
        return EagerLoader(mapper, secondary, primaryjoin, secondaryjoin, **options)
    
def relation_mapper(class_, selectable, secondary = None, primaryjoin = None, secondaryjoin = None, table = None, properties = None, lazy = True, **options):
    return relation_loader(mapper(class_, selectable, table = table, properties = properties, isroot = False, **options), secondary, primaryjoin, secondaryjoin, lazy = lazy, **options)

_mappers = {}
def mapper(*args, **params):
    hashkey = mapper_hash_key(*args, **params)
    print "HASHKEY: " + hashkey
    try:
        return _mappers[hashkey]
    except KeyError:
        return _mappers.setdefault(hashkey, Mapper(*args, **params))

def identitymap():
    return IdentityMap()

def globalidentity():
    return _global_identitymap
    
def eagerload(name):
    return EagerLazySwitcher(name, toeager = True)
    
def lazyload(name):
    return EagerLazySwitcher(name, toeager = False)

class Mapper(object):
    def __init__(self, class_, selectable, table = None, properties = None, identitymap = None, use_smart_properties = True, isroot = True, echo = None):
        self.class_ = class_
        self.selectable = selectable
        self.use_smart_properties = use_smart_properties
        if table is None:
            self.table = self._find_table(selectable)
        else:
            self.table = table

        self.echo = echo

        if identitymap is not None:
            self.identitymap = identitymap
        else:
            self.identitymap = _global_identitymap

        self.props = {}
        for column in self.selectable.columns:
            self.props[column.key] = ColumnProperty(column)
        self.properties = properties
        if properties is not None:
            for key, value in properties.iteritems():
                self.props[key] = value

        if isroot:
            self.init(self)
    
    def hash_key(self):
        return mapper_hash_key(
            self.class_,
            self.selectable,
            self.table,
            self.properties,
            self.identitymap,
            self.use_smart_properties,
            self.echo
        )

    def set_property(self, key, prop):
        self.props[key] = prop
        prop.init(key, self, self.root)

    def init(self, root):
        self.root = root
        self.identitymap = root.identitymap
        self.echo = self.root.echo
        [prop.init(key, self, root) for key, prop in self.props.iteritems()]

    def instances(self, cursor):
        result = []
        cursor = engine.ResultProxy(cursor)

        localmap = {}
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            self._instance(row, localmap, result)
        return result

    def get(self, id):
        """returns an instance of the object based on the given ID."""
        pass

    def compile(self, whereclause = None, **options):
        """works like select, except returns the SQL statement object without 
        compiling or executing it"""
        return self._compile(whereclause, **options)

    def options(self, *options):
        """uses this mapper as a prototype for a new mapper with different behavior.
        *options is a list of options directives, which include eagerload() and lazyload()"""

        hashkey = hash_key(self) + "->" + repr([hash_key(o) for o in options])
        print "HASHKEY: " + hashkey
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
        
    def save(self, object, traverse = True, refetch = False):
        """saves the object.  based on the existence of its primary key, either inserts or updates.
        primary key is determined by the underlying database engine's sequence methodology.
        traverse indicates attached objects should be saved as well.
        
        if smart attributes are being used for the object, the "dirty" flag, or the absense 
        of the attribute, determines if the item is saved.  if smart attributes are not being 
        used, the item is saved unconditionally.
        """
        if getattr(object, 'dirty', True):
            pass
            # do the save
        for prop in self.props.values():
            prop.save(object, traverse, refetch)
    
    def remove(self, object, traverse = True):
        """removes the object.  traverse indicates attached objects should be removed as well."""
        pass
    
    def insert(self, object):
        """inserts the object into its table, regardless of primary key being set.  this is a 
        lower-level operation than save."""
        params = {}
        for col in self.table.columns:
            params[col.key] = getattr(object, col.key)
        ins = self.table.insert()
        ins.execute(**params)
        primary_keys = self.table.engine.last_inserted_ids()
        # TODO: put the primary keys into the object props

    def update(self, object):
        """inserts the object into its table, regardless of primary key being set.  this is a 
        lower-level operation than save."""
        params = {}
        for col in self.table.columns:
            params[col.key] = getattr(object, col.key)
        upd = self.table.update()
        upd.execute(**params)
        
    def delete(self, object):
        """deletes the object's row from its table unconditionally. this is a lower-level
        operation than remove."""
        pass

    class TableFinder(sql.ClauseVisitor):
        def visit_table(self, table):
            if hasattr(self, 'table'):
                raise "Mapper can only create object instances against a single-table identity - specify the 'table' argument to the Mapper constructor"
            self.table = table
            
    def _find_table(self, selectable):
        tf = Mapper.TableFinder()
        selectable.accept_visitor(tf)
        return tf.table

    def _compile(self, whereclause = None, **options):
        statement = sql.select([self.selectable], whereclause)
        for key, value in self.props.iteritems():
            value.setup(key, self.selectable, statement, **options) 
        statement.use_labels = True
        return statement
        
    def _select_whereclause(self, whereclause = None, **params):
        statement = self._compile(whereclause)
        return self._select_statement(statement, **params)
    
    def _select_statement(self, statement, **params):
        statement.use_labels = True
        statement.echo = self.echo
        return self.instances(statement.execute(**params))

    def _identity_key(self, row):
        return self.identitymap.get_key(row, self.class_, self.table, self.selectable)

    def _instance(self, row, localmap, result):
        """pulls an object instance from the given row and appends it to the given result list.
        if the instance already exists in the given identity map, its not added.  in either
        case, executes all the property loaders on the instance to also process extra information
        in the row."""
            
        # create the instance if its not in the identity map,
        # else retrieve it
        identitykey = self._identity_key(row)
        exists = self.identitymap.has_key(identitykey)
        if not exists:
            instance = self.class_()
            for column in self.selectable.primary_keys:
                if row[column.label] is None:
                    return None
            self.identitymap[identitykey] = instance
        else:
            instance = self.identitymap[identitykey]
        instance.dirty = False

        # now add to the result list, but we only want to add 
        # to the result list uniquely, so get another identity map
        # that is associated with that list
        try:
            imap = localmap[id(result)]
        except KeyError:
            imap = localmap.setdefault(id(result), IdentityMap())
        
        isduplicate = imap.has_key(identitykey)
        if not isduplicate:
            imap[identitykey] = instance
            result.append(instance)

        # call further mapper properties on the row, to pull further 
        # instances from the row and possibly populate this item.
        for key, prop in self.props.iteritems():
            prop.execute(instance, row, identitykey, localmap, isduplicate)


class MapperOption:
    def process(self, mapper):
        raise NotImplementedError()
    
    def hash_key(self):
        return repr(self)

class MapperProperty:
    """an element attached to a Mapper that describes the loading and population
    of an attribute on an object instance."""
    def execute(self, instance, row, identitykey, localmap, isduplicate):
        """called when the mapper receives a row.  instance is the parent instance corresponding
        to the row. """
        raise NotImplementedError()

    def hash_key(self):
        """describes this property and its instantiated arguments in such a way
        as to uniquely identify the concept this MapperProperty represents"""
        raise NotImplementedError()

    def setup(self, key, primarytable, statement, **options):
        """called when a statement is being constructed.  """
        return self

    def init(self, key, parent, root):
        """called when the MapperProperty is first attached to a new parent Mapper."""
        pass
    
    def save(self, object, traverse, refetch):
        pass
    
    def delete(self, object):
        pass

class ColumnProperty(MapperProperty):
    """describes an object attribute that corresponds to the value in a result set column."""
    def __init__(self, column):
        self.column = column

    def hash_key(self):
        return "ColumnProperty(%s)" % hash_key(self.column)
        
    def init(self, key, parent, root):
        self.key = key
        if root.use_smart_properties:
            self.use_smart = True
            if not hasattr(parent.class_, key):
                setattr(parent.class_, key, SmartProperty(key).property())
        else:
            self.use_smart = False

    def execute(self, instance, row, identitykey, localmap, isduplicate):
        if not isduplicate:
            if self.use_smart:
                instance.__dict__[self.key] = row[self.column.label]
            else:
                setattr(instance, self.key, row[self.column.label])


def hash_key(obj):
    if obj is None:
        return 'None'
    else:
        return obj.hash_key()

def mapper_hash_key(class_, selectable, table = None, properties = None, identitymap = None, use_smart_properties = True, isroot = True, echo = None):
    if properties is None:
        properties = {}
    return (
        "Mapper(%s, %s, table=%s, properties=%s, identitymap=%s, use_smart_properties=%s, echo=%s)" % (
            repr(class_),
            hash_key(selectable),
            hash_key(table),
            repr(dict([(k, hash_key(p)) for k,p in properties.iteritems()])),
            hash_key(identitymap),
            repr(use_smart_properties),
            repr(echo)

        )
    )


        
class PropertyLoader(MapperProperty):
    def __init__(self, mapper, secondary, primaryjoin, secondaryjoin):
        self.mapper = mapper
        self.target = self.mapper.selectable
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self._hash_key = "%s(%s, %s, %s, %s)" % (self.__class__.__name__, hash_key(mapper), hash_key(secondary), hash_key(primaryjoin), hash_key(secondaryjoin))
        
    def hash_key(self):
        return self._hash_key
        
    def init(self, key, parent, root):
        self.key = key
        self.mapper.init(root)
        if self.secondary is not None:
            if self.secondaryjoin is None:
                self.secondaryjoin = match_primaries(self.target, self.secondary)
            if self.primaryjoin is None:
                self.primaryjoin = match_primaries(parent.selectable, self.secondary)
        else:
            if self.primaryjoin is None:
                self.primaryjoin = match_primaries(parent.selectable, self.target)

    def save(self, object, traverse, refetch):
        # if a mapping table does not exist, save a row for all objects
        # in our list normally, setting their primary keys
        # else, determine the foreign key column in our table, set it to the parent
        # of all child objects before saving
        # if a mapping table exists, determine the two foreign key columns 
        # in the mapping table, set the two values, and insert that row, for
        # each row in the list
        pass

    def delete(self):
        self.mapper.delete()

        
class LazyLoader(PropertyLoader):

    def init(self, key, parent, root):
        PropertyLoader.init(self, key, parent, root)
        if not hasattr(parent.class_, key):
            if not issubclass(parent.class_, object):
                raise "LazyLoader can only be used with new-style classes, i.e. subclass object"
            setattr(parent.class_, key, SmartProperty(key).property())
        
    def setup(self, key, primarytable, statement, **options):
        if self.secondaryjoin is not None:
            self.lazywhere = sql.and_(self.primaryjoin, self.secondaryjoin)
        else:
            self.lazywhere = self.primaryjoin
        self.lazywhere = self.lazywhere.copy_container()
        li = LazyIzer(primarytable)
        self.lazywhere.accept_visitor(li)
        self.binds = li.binds

    def execute(self, instance, row, identitykey, localmap, isduplicate):
        if not isduplicate:
            def load():
                m = {}
                for key, value in self.binds.iteritems():
                    m[key] = row[key]
                return self.mapper.select(self.lazywhere, **m)

            setattr(instance, self.key, load)
        
class EagerLoader(PropertyLoader):
    def init(self, key, parent, root):
        PropertyLoader.init(self, key, parent, root)
        self.to_alias = util.Set()
        [self.to_alias.append(f) for f in self.primaryjoin._get_from_objects()]
        if self.secondaryjoin is not None:
            [self.to_alias.append(f) for f in self.secondaryjoin._get_from_objects()]
        del self.to_alias[parent.selectable]
    
            
    def setup(self, key, primarytable, statement, **options):
        """add a left outer join to the statement thats being constructed"""

        if statement.whereclause is not None:
            # if the whereclause of the statement references tables that are also
            # in the outer join we are constructing, then convert those objects to 
            # reference "aliases" of those tables so that their where condition does not interfere
            # with ours
            for target in self.to_alias:
                aliasizer = Aliasizer(target, "aliased_" + target.name + "_" + hex(random.randint(0, 65535))[2:])
                statement.whereclause.accept_visitor(aliasizer)
                statement.append_from(aliasizer.alias)
        
        if hasattr(statement, '_outerjoin'):
            towrap = statement._outerjoin
        else:
            towrap = primarytable
        
        if self.secondaryjoin is not None:
            statement._outerjoin = sql.outerjoin(sql.outerjoin(towrap, self.secondary, self.secondaryjoin), self.target, self.primaryjoin)
        else:
            statement._outerjoin = sql.outerjoin(towrap, self.target, self.primaryjoin)
            
        statement.append_from(statement._outerjoin)
        statement.append_column(self.target)
        for key, value in self.mapper.props.iteritems():
            value.setup(key, self.mapper.selectable, statement)
        
    def execute(self, instance, row, identitykey, localmap, isduplicate):
        """receive a row.  tell our mapper to look for a new object instance in the row, and attach
        it to a list on the parent instance."""
        if not isduplicate:
            list = []
            setattr(instance, self.key, list)
        else:
            list = getattr(instance, self.key)

        self.mapper._instance(row, localmap, list)

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
    def __init__(self, table, aliasname):
        self.table = table
        self.alias = sql.alias(table, aliasname)
        self.binary = None

    def visit_binary(self, binary):
        if isinstance(binary.left, schema.Column) and binary.left.table == self.table:
            binary.left = self.alias.c[binary.left.name]
        if isinstance(binary.right, schema.Column) and binary.right.table == self.table:
            binary.right = self.alias.c[binary.right.name]

class LazyIzer(sql.ClauseVisitor):
    def __init__(self, table):
        self.table = table
        self.binds = {}
        
    def visit_binary(self, binary):
        if isinstance(binary.left, schema.Column) and binary.left.table == self.table:
            binary.left = self.binds.setdefault(self.table.name + "_" + binary.left.name,
                    sql.BindParamClause(self.table.name + "_" + binary.left.name, None, shortname = binary.left.name))

        if isinstance(binary.right, schema.Column) and binary.right.table == self.table:
            binary.right = self.binds.setdefault(self.table.name + "_" + binary.right.name,
                    sql.BindParamClause(self.table.name + "_" + binary.right.name, None, shortname = binary.left.name))
    
class LazyRow(MapperProperty):
    def __init__(self, table, whereclause, **options):
        self.table = table
        self.whereclause = whereclause

    def init(self, key, parent, root):
        self.keys.append(key)

    def execute(self, instance, row, identitykey, localmap, isduplicate):
        pass


class SmartProperty(object):
    def __init__(self, key):
        self.key = key

    def property(self):
        def set_prop(s, value):
            s.__dict__[self.key] = value
            s.dirty = True
        def del_prop(s):
            del s.__dict__[self.key]
            s.dirty = True
        def get_prop(s):
            try:
                v = s.__dict__[self.key]
            except KeyError:
                raise AttributeError(self.key)
            if isinstance(v, types.FunctionType):
                s.__dict__[self.key] = v()
            return s.__dict__[self.key]
        return property(get_prop, set_prop, del_prop)

def match_primaries(primary, secondary):
    pk = primary.primary_keys
    if len(pk) == 1:
        return (pk[0] == secondary.c[pk[0].name])
    else:
        return sql.and_([pk == secondary.c[pk.name] for pk in primary.primary_keys])

class IdentityMap(dict):
    def get_key(self, row, class_, table, selectable):
        return (class_, table, tuple([row[column.label] for column in selectable.primary_keys]))
    def hash_key(self):
        return "IdentityMap(%s)" % id(self)
        
_global_identitymap = IdentityMap()

