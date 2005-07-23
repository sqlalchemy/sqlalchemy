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
import weakref, random, copy

__ALL__ = ['eagermapper', 'eagerloader', 'lazymapper', 'lazyloader', 'eagerload', 'lazyload', 'mapper', 'lazyloader', 'lazymapper', 'identitymap', 'globalidentity']


def relation(*args, **params):
    #multimethod poverty
    if type(args[0]) == Mapper:
        return relation_loader(*args, **params)
    else:
        return relation_mapper(*args, **params)

def relation_loader(mapper, whereclause, lazy = True, **options):
    if lazy:
        return LazyLoader(mapper, whereclause, **options)
    else:
        return EagerLoader(mapper, whereclause, **options)
    
def relation_mapper(class_, selectable, whereclause, table = None, properties = None, lazy = True, **options):
    return relation_loader(mapper(class_, selectable, table = table, properties = properties, isroot = False), whereclause, lazy = lazy, **options)

def mapper(class_, selectable, table = None, properties = None, identitymap = None, use_smart_properties = True, isroot = True):
    return Mapper(class_, selectable, table = table, properties = properties, identitymap = identitymap, use_smart_properties = use_smart_properties, isroot = isroot)

def identitymap():
    return IdentityMap()

def globalidentity():
    return _global_identitymap
    
def eagerload(name):
    return EagerOption(name)
    
def lazyload(name):
    return LazyOption(name)

class Mapper(object):
    def __init__(self, class_, selectable, table = None, properties = None, identitymap = None, use_smart_properties = True, isroot = True):
        self.class_ = class_
        self.selectable = selectable
        self.use_smart_properties = use_smart_properties
        if table is None:
            self.table = self._find_table(selectable)
        else:
            self.table = table
        self.props = {}
        
        for column in self.selectable.columns:
            self.props[column.key] = ColumnProperty(column)

        if identitymap is not None:
            self.identitymap = identitymap
        else:
            self.identitymap = _global_identitymap

        if properties is not None:
            for key, value in properties.iteritems():
                self.props[key] = value

        if isroot:
            self.init(self)
    
    def set_property(self, key, prop):
        self.props[key] = prop
        prop.init(key, self, self.root)
        
    def init(self, root):
        self.root = root
        self.identitymap = root.identitymap
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
        mapper = copy.copy(self)
        for option in options:
            option.process(mapper)
        return mapper
        
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
        pass

    def update(self, object):
        """inserts the object into its table, regardless of primary key being set.  this is a 
        lower-level operation than save."""
        pass
        
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

        # call further mapper properties on the row, to pull further 
        # instances from the row and possibly populate this item.
        for key, prop in self.props.iteritems():
            prop.execute(instance, row, identitykey, localmap, exists)

        # now add to the result list, but we only want to add 
        # to the result list uniquely, so get another identity map
        # that is associated with that list
        try:
            imap = localmap[id(result)]
        except KeyError:
            imap = localmap.setdefault(id(result), IdentityMap())
        if not imap.has_key(identitykey):
            imap[identitykey] = instance
            result.append(instance)


class MapperOption:
    def process(self, mapper):
        raise NotImplementedError()
        
class MapperProperty:
    def execute(self, instance, row, identitykey, localmap, isduplicate):
        """called when the mapper receives a row.  instance is the parent instance corresponding
        to the row. """
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
    def __init__(self, column):
        self.column = column

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
    def __init__(self, mapper, whereclause, **options):
        self.mapper = mapper
        self.whereclause = whereclause

    def init(self, key, parent, root):
        self.key = key
        self.mapper.init(root)

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
    def setup(self, key, primarytable, statement, **options):
        self.lazywhere = self.whereclause.copy_structure()
        li = LazyIzer(primarytable)
        self.lazywhere.accept_visitor(li)
        self.binds = li.binds

    def init(self, key, parent, root):
        PropertyLoader.init(self, key, parent, root)
        setattr(parent.class_, key, SmartProperty(key).property())

    def execute(self, instance, row, identitykey, localmap, isduplicate):
        if not isduplicate:
            def load():
                m = {}
                for key, value in self.binds.iteritems():
                    m[key] = row[key]
                return self.mapper.select(**m)

            setattr(instance, self.key, load)
        
class EagerLoader(PropertyLoader):
    def setup(self, key, primarytable, statement, **options):
        """add a left outer join to the statement thats being constructed"""
        targettable = self.mapper.selectable

        if statement.whereclause is not None:
            # if the whereclause of the statement references tables that are also
            # in the outer join we are constructing, then convert those objects to 
            # reference "aliases" of those tables so that their where condition does not interfere
            # with ours
            targets = util.Set([targettable] + self.whereclause._get_from_objects())
            del targets[primarytable]
            for target in targets:
                aliasizer = Aliasizer(target, "aliased_" + target.name + "_" + hex(random.randint(0, 65535))[2:])
                statement.whereclause.accept_visitor(aliasizer)
                statement.append_from(aliasizer.alias)
        
        if hasattr(statement, '_outerjoin'):
            statement._outerjoin = sql.outerjoin(statement._outerjoin, targettable, self.whereclause)
        else:
            statement._outerjoin = sql.outerjoin(primarytable, targettable, self.whereclause)
        statement.append_from(statement._outerjoin)
        statement.append_column(targettable)
        for key, value in self.mapper.props.iteritems():
            value.setup(key, self.mapper.selectable, statement) 
        
    def execute(self, instance, row, identitykey, localmap, isduplicate):
        """receive a row.  tell our mapper to look for a new object instance in the row, and attach
        it to a list on the parent instance."""
        try:
            list = getattr(instance, self.key)
        except AttributeError:
            list = []
            setattr(instance, self.key, list)
        self.mapper._instance(row, localmap, list)

class EagerOption(MapperOption):
    """an option that switches a PropertyLoader to be an EagerLoader"""
    def __init__(self, key):
        self.key = key

    def process(self, mapper):
        oldprop = mapper.props[self.key]
        mapper.set_property(self.key, EagerLoader(oldprop.mapper, oldprop.whereclause))
        
class LazyOption(MapperOption):
    """an option that switches a PropertyLoader to be a LazyLoader"""
    def __init__(self, key):
        self.key = key

    def process(self, mapper):
        oldprop = mapper.props[self.key]
        mapper.set_property(self.key, LazyLoader(oldprop.mapper, oldprop.whereclause))
        
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
            binary.left = self.binds.setdefault(binary.left.name,
                    sql.BindParamClause(self.table.name + "_" + binary.left.name, None, shortname = binary.left.name))

        if isinstance(binary.right, schema.Column) and binary.right.table == self.table:
            binary.right = self.binds.setdefault(binary.right.name,
                    sql.BindParamClause(self.table.name + "_" + binary.right.name, None, shortname = binary.left.name))
    


class SmartProperty(object):
    def __init__(self, key):
        self.key = key

    def property(self):
        def set_prop(s, value):
            print "hi setting is " + repr(value)
            raise "hi"
            s.__dict__[self.key] = value
            s.dirty = True
        def del_prop(s):
            del s.__dict__[self.key]
            s.dirty = True
        def get_prop(s):
            v = s.__dict__[self.key]
            # TODO: this sucks a little
            print "hi thing is " + repr(v)
            if isinstance(v, types.FunctionType):
                s.__dict__[self.key] = v()
            return s.__dict__[self.key]
        return property(get_prop, set_prop, del_prop)


class IdentityMap(dict):
    def get_key(self, row, class_, table, selectable):
        return (class_, table, tuple([row[column.label] for column in selectable.primary_keys]))
        
_global_identitymap = IdentityMap()

