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
        'addresses' : eagerloader(addressmapper, users.c.user_id == addresses.c.user_id),
        'permissions' : lazymapper(Permissions, permissions, users.c.user_id == permissions.c.user_id)
    },
    )

"""

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine
import weakref

__ALL__ = ['eagermapper', 'eagerloader', 'mapper', 'lazyloader', 'lazymapper', 'identitymap', 'globalidentity']

def eagermapper(class_, selectable, whereclause, table = None, properties = None):
    return eagerloader(mapper(class_, selectable, table = table, properties = properties, isroot = False), whereclause)

def eagerloader(mapper, whereclause):
    return EagerLoader(mapper, whereclause)

def mapper(class_, selectable, table = None, properties = None, identitymap = None, use_smart_properties = True, isroot = True):
    return Mapper(class_, selectable, table = table, properties = properties, identitymap = identitymap, use_smart_properties = use_smart_properties, isroot = isroot)

def identitymap():
    return IdentityMap()

def globalidentity():
    return _global_identitymap

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
    
    def init(self, root):
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
        if not getattr(object, 'dirty', True):
            return
    
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
        
    def _select_whereclause(self, whereclause = None, **params):
        statement = sql.select([self.selectable], whereclause)
        for key, value in self.props.iteritems():
            value.setup(key, self.selectable, statement) 
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
            prop.execute(instance, key, row, identitykey, localmap, exists)

        # now add to the result list, but we only want to add 
        # to the result list uniquely, so get another identity map
        # that is associated with that list
        try:
            imap = localmap[id(result)]
        except:
            imap = localmap.setdefault(id(result), IdentityMap())
        if not imap.has_key(identitykey):
            imap[identitykey] = instance
            result.append(instance)



class MapperProperty:
    def execute(self, instance, key, row, isduplicate):
        """called when the mapper receives a row.  instance is the parent instance corresponding
        to the row. """
        raise NotImplementedError()
    def setup(self, key, primarytable, statement):
        """called when a statement is being constructed."""
        pass
    def init(self, key, parent, root):
        """called when the MapperProperty is first attached to a new parent Mapper."""
        pass

class ColumnProperty(MapperProperty):
    def __init__(self, column):
        self.column = column

    def init(self, key, parent, root):
        if root.use_smart_properties:
            self.use_smart = True
            if not hasattr(parent.class_, key):
                setattr(parent.class_, key, SmartProperty(key).property())
        else:
            self.use_smart = False
            
    def execute(self, instance, key, row, identitykey, localmap, isduplicate):
        if not isduplicate:
            if self.use_smart:
                instance.__dict__[key] = row[self.column.label]
            else:
                setattr(instance, key, row[self.column.label])

    
class EagerLoader(MapperProperty):
    def __init__(self, mapper, whereclause):
        self.mapper = mapper
        self.whereclause = whereclause
    
    def init(self, key, parent, root):
        self.mapper.init(root)
        
    def setup(self, key, primarytable, statement):
        """add a left outer join to the statement thats being constructed"""
        targettable = self.mapper.selectable
        if hasattr(statement, '_outerjoin'):
            statement._outerjoin = sql.outerjoin(statement._outerjoin, targettable, self.whereclause)
        else:
            statement._outerjoin = sql.outerjoin(primarytable, targettable, self.whereclause)
        statement.append_from(statement._outerjoin)
        statement.append_column(targettable)
        for key, value in self.mapper.props.iteritems():
            value.setup(key, self.mapper.selectable, statement) 
        
    def execute(self, instance, key, row, identitykey, localmap, isduplicate):
        """receive a row.  tell our mapper to look for a new object instance in the row, and attach
        it to a list on the parent instance."""
        try:
            list = getattr(instance, key)
        except AttributeError:
            list = []
            setattr(instance, key, list)
        self.mapper._instance(row, localmap, list)

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
            return s.__dict__[self.key]
        return property(get_prop, set_prop, del_prop)
        
class IdentityMap(dict):
    def get_key(self, row, class_, table, selectable):
        return (class_, table, tuple([row[column.label] for column in selectable.primary_keys]))
        
_global_identitymap = IdentityMap()

