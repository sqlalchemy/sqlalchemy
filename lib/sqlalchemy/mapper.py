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
    #print "HASHKEY: " + hashkey
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
        self.use_smart_properties = use_smart_properties

        self.selectable = selectable
        tf = Mapper.TableFinder()
        self.selectable.accept_visitor(tf)
        self.tables = tf.tables

        if table is None:
            if len(self.tables) > 1:
                raise "Selectable contains multiple tables - specify primary table argument to Mapper"
            self.table = self.tables[0]
        else:
            self.table = table

        self.echo = echo

        if identitymap is not None:
            self.identitymap = identitymap
        else:
            self.identitymap = _global_identitymap

        self.props = {}
        for column in self.selectable.columns:
            prop = self.props.get(column.key, None)
            if prop is None:
                prop = ColumnProperty(column)
                self.props[column.key] = prop
            else:
                prop.columns.append(column)
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
        cursor = engine.ResultProxy(cursor, echo = self.echo)

        localmap = {}
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            self._instance(row, localmap, result)
        return result

    def get(self, *ident):
        """returns an instance of the object based on the given identifier, or None
        if not found.  The *ident argument is a 
        list of primary keys in the order of the table def's primary keys."""
        key = self.identitymap.get_id_key(ident, self.class_, self.table, self.selectable)
        try:
            return self.identitymap[key]
        except KeyError:
            clause = sql.and_()
            i = 0
            for primary_key in self.table.primary_keys:
                # appending to the and_'s clause list directly to skip
                # typechecks etc.
                clause.clauses.append(primary_key == ident[i])
                i += 2
            try:
                return self.select(clause)[0]
            except IndexError:
                return None

    def put(self, instance):
        key = self.identitymap.get_instance_key(instance, self.class_, self.table, self.selectable)
        self.identitymap[key] = instance
        return key

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

    def save(self, obj, traverse = True, refetch = False):
        """saves the object across all its primary tables.  
        based on the existence of the primary key for each table, either inserts or updates.
        primary key is determined by the underlying database engine's sequence methodology.
        the traverse flag indicates attached objects should be saved as well.
        
        if smart attributes are being used for the object, the "dirty" flag, or the absense 
        of the attribute, determines if the item is saved.  if smart attributes are not being 
        used, the item is saved unconditionally.
        """

        if getattr(obj, 'dirty', True):
            def foo():
                props = {}
                for prop in self.props.values():
                    if not isinstance(prop, ColumnProperty):
                        continue
                    for col in prop.columns:
                        props[col] = prop
                for table in self.tables:
                    params = {}
                    for primary_key in table.primary_keys:
                        if props[primary_key].getattr(obj) is None:
                            statement = table.insert()
                            for col in table.columns:
                                params[col.key] = props[col].getattr(obj)
                            break
                    else:
                        clause = sql.and_()
                        for col in table.columns:
                            if col.primary_key:
                                clause.clauses.append(col == props[col].getattr(obj))
                            else:
                                params[col.key] = props[col].getattr(obj)
                        statement = table.update(clause)
                    statement.echo = self.echo
                    statement.execute(**params)
                    if isinstance(statement, sql.Insert):
                        primary_keys = table.engine.last_inserted_ids()
                        index = 0
                        for col in table.primary_keys:
                            newid = primary_keys[index]
                            index += 1
                            props[col].setattr(obj, newid)
                        self.put(obj)
                # unset dirty flag
                obj.dirty = False
                for prop in self.props.values():
                    if not isinstance(prop, ColumnProperty):
                        prop.save(obj, traverse, refetch)
            self.transaction(foo)
        else:
            for prop in self.props.values():
                prop.save(obj, traverse, refetch)

    def transaction(self, f):
        return self.table.engine.multi_transaction(self.tables, f)

    def remove(self, obj, traverse = True):
        """removes the object.  traverse indicates attached objects should be removed as well."""
        pass


    def delete(self, obj):
        """deletes the object's row from its table unconditionally. this is a lower-level
        operation than remove."""
        # delete dependencies ?
        # delete row
        # remove primary keys
        # unset dirty flag
        pass

    class TableFinder(sql.ClauseVisitor):
        def __init__(self):
            self.tables = []
        def visit_table(self, table):
            self.tables.append(table)


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
            for column in self.table.primary_keys:
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
    """describes a modification to a Mapper in the context of making a copy
    of it.  This is used to assist in the prototype pattern used by mapper.options()."""
    def process(self, mapper):
        raise NotImplementedError()
    def hash_key(self):
        return repr(self)

class MapperProperty:
    """an element attached to a Mapper that describes and assists in the loading and saving 
    of an attribute on an object instance."""
    def execute(self, instance, row, identitykey, localmap, isduplicate):
        """called when the mapper receives a row.  instance is the parent instance corresponding
        to the row. """
        raise NotImplementedError()

    def hash_key(self):
        """describes this property and its instantiated arguments in such a way
        as to uniquely identify the concept this MapperProperty represents,within 
        a process."""
        raise NotImplementedError()

    def setup(self, key, primarytable, statement, **options):
        """called when a statement is being constructed.  """
        return self

    def init(self, key, parent, root):
        """called when the MapperProperty is first attached to a new parent Mapper."""
        pass

    def save(self, object, traverse, refetch):
        """called when the instance is being saved"""
        pass

    def delete(self, object):
        """called when the instance is being deleted"""
        pass

class ColumnProperty(MapperProperty):
    """describes an object attribute that corresponds to a table column."""
    def __init__(self, *columns):
        self.columns = list(columns)

    def getattr(self, object):
        return getattr(object, self.key, None)
    def setattr(self, object, value):
        setattr(object, self.key, value)
    def hash_key(self):
        return "ColumnProperty(%s)" % repr([hash_key(c) for c in self.columns])

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



class PropertyLoader(MapperProperty):
    """describes an object property that holds a list of items that correspond to a related
    database table."""
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
        self.parenttable = parent.selectable
        if self.secondary is not None:
            if self.secondaryjoin is None:
                self.secondaryjoin = match_primaries(self.target, self.secondary)
            if self.primaryjoin is None:
                self.primaryjoin = match_primaries(parent.selectable, self.secondary)
        else:
            if self.primaryjoin is None:
                self.primaryjoin = match_primaries(parent.selectable, self.target)

    def save(self, obj, traverse, refetch):
        # if a mapping table does not exist, save a row for all objects
        # in our list normally, setting their primary keys
        # else, determine the foreign key column in our table, set it to the parent
        # of all child objects before saving
        # if a mapping table exists, determine the two foreign key columns 
        # in the mapping table, set the two values, and insert that row, for
        # each row in the list
        setter = ForeignKeySetter(self.parenttable, self.target, obj)
        for child in getattr(obj, self.key):
            setter.child = child
            self.primaryjoin.accept_visitor(setter)
            self.mapper.save(child)
        #pass

    def delete(self):
        self.mapper.delete()

class ForeignKeySetter(sql.ClauseVisitor):
    def __init__(self, primarytable, secondarytable, obj):
        self.child = None
        self.obj = obj
        self.primarytable = primarytable
        self.secondarytable = secondarytable

    def visit_binary(self, binary):
        if binary.operator == '=':
            if binary.left.table == self.primarytable and binary.right.table == self.secondarytable:
                setattr(self.child, binary.left.key, getattr(self.obj, binary.right.key))
            elif binary.right.table == self.primarytable and binary.left.table == self.secondarytable:
                setattr(self.child, binary.right.key, getattr(self.obj, binary.left.key))

class LazyLoader(PropertyLoader):

    def init(self, key, parent, root):
        PropertyLoader.init(self, key, parent, root)
        if not hasattr(parent.class_, key):
            if not issubclass(parent.class_, object):
                raise "LazyLoader can only be used with new-style classes"
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
            setattr(instance, self.key, LazyLoadInstance(self, row))

class LazyIzer(sql.ClauseVisitor):
    """converts an expression which refers to a table column into an
    expression refers to a Bind Param, i.e. a specific value.  
    e.g. the clause 'WHERE tablea.foo=tableb.foo' becomes 'WHERE tablea.foo=:foo'.  
    this is used to turn a join expression into one useable by a lazy load
    for a specific parent row."""

    def __init__(self, table):
        self.table = table
        self.binds = {}

    def visit_binary(self, binary):
        if isinstance(binary.left, schema.Column) and binary.left.table == self.table:
            binary.left = self.binds.setdefault(self.table.name + "_" + binary.left.name,
                    sql.BindParamClause(self.table.name + "_" + binary.left.name, None, shortname = binary.left.name))

        if isinstance(binary.right, schema.Column) and binary.right.table == self.table:
            binary.right = self.binds.setdefault(self.table.name + "_" + binary.right.name,
                    sql.BindParamClause(self.table.name + "_" + binary.right.name, None, shortname = binary.right.name))

class LazyLoadInstance(object):
    """attached to a specific object instance to load related rows."""
    def __init__(self, lazyloader, row):
        self.params = {}
        for key, value in lazyloader.binds.iteritems():
            self.params[key] = row[key]
        # TODO: this still sucks. the mapper points to tables, which point
        # to dbengines, which cant be serialized, or are too huge to be serialized
        # quickly, so an object with a lazyloader still cant really be serialized
        self.mapper = lazyloader.mapper
        self.lazywhere = lazyloader.lazywhere
    def __call__(self):
        return self.mapper.select(self.lazywhere, **self.params)

class EagerLoader(PropertyLoader):
    """loads related objects inline with a parent query."""
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
    """converts a table instance within an expression to be an alias of that table."""
    def __init__(self, table, aliasname):
        self.table = table
        self.alias = sql.alias(table, aliasname)
        self.binary = None

    def visit_binary(self, binary):
        if isinstance(binary.left, schema.Column) and binary.left.table == self.table:
            binary.left = self.alias.c[binary.left.name]
        if isinstance(binary.right, schema.Column) and binary.right.table == self.table:
            binary.right = self.alias.c[binary.right.name]

class LazyRow(MapperProperty):
    """TODO: this will lazy-load additional properties of an object from a secondary table."""
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
            if callable(v):
                s.__dict__[self.key] = v()
            return s.__dict__[self.key]
        return property(get_prop, set_prop, del_prop)

class IdentityMap(dict):
    def get_id_key(self, ident, class_, table, selectable):
        return (class_, table, tuple(ident))
    def get_instance_key(self, object, class_, table, selectable):
        return (class_, table, tuple([getattr(object, column.key, None) for column in table.primary_keys]))
    def get_key(self, row, class_, table, selectable):
        return (class_, table, tuple([row[column.label] for column in table.primary_keys]))
    def hash_key(self):
        return "IdentityMap(%s)" % id(self)


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

def match_primaries(primary, secondary):
    pk = primary.primary_keys
    if len(pk) == 1:
        return (pk[0] == secondary.c[pk[0].name])
    else:
        return sql.and_([pk == secondary.c[pk.name] for pk in primary.primary_keys])


_global_identitymap = IdentityMap()

