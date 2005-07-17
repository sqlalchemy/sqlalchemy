"""
# create a mapper from a class and table object
usermapper = Mapper(User, users)


# get primary key
usermapper.get(10)

userlist = usermapper.select(usermapper.table.user_id == 10)

userlist = usermapper.select(
        and_(usermapper.table.user_name == 'fred', usermapper.table.user_id == 12)
    )

userlist = usermapper.select("user_id =12 and foo=bar", from_obj=["foo"])

usermapper = Mapper(
    User, 
    users, 
    properties = {
        'addresses' : Relation(addressmapper, lazy = False),
        'permissions' : Relation(permissions, 
        
                # one or the other
                associationtable = userpermissions, 
                criterion = and_(users.user_id == userpermissions.user_id, userpermissions.permission_id=permissions.permission_id), 
                lazy = True),
        '*' : [users, userinfo]
    },
    )

addressmapper = Mapper(Address, addresses, properties = {
    'street': addresses.address_1,
})
"""

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema
import sqlalchemy.engine as engine

class Mapper(object):
    def __init__(self, class_, selectable, table = None, properties = None, identitymap = None):
        self.class_ = class_
        self.selectable = selectable
        if table is None:
            self.table = self._find_table(selectable)
        else:
            self.table = table
        self.props = {}
        
        for column in self.selectable.columns:
            self.props[column.key] = ColumnProperty(column)

        if properties is not None:
            for key, value in properties.iteritems():
                self.props[key] = value
                
        if identitymap is not None:
            self.identitymap = identitymap
        else:
            self.identitymap = _global_identitymap
            
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
        
    def save(self, object):
        pass
        
    def delete(self, whereclause = None, **params):
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
        raise NotImplementedError()
    def setup(self, key, primarytable, statement):
        pass

class ColumnProperty(MapperProperty):
    def __init__(self, column):
        self.column = column
        
    def execute(self, instance, key, row, identitykey, localmap, isduplicate):
        if not isduplicate:
            setattr(instance, key, row[self.column.label])

class EagerLoader(MapperProperty):
    def __init__(self, mapper, whereclause):
        self.mapper = mapper
        self.whereclause = whereclause
        
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
        """a row.  tell our mapper to look for a new object instance in the row, and attach
        it to a list on the parent instance."""
        try:
            list = getattr(instance, key)
        except AttributeError:
            list = []
            setattr(instance, key, list)
        self.mapper._instance(row, localmap, list)


        
class IdentityMap(dict):
    def get_key(self, row, class_, table, selectable):
        return (class_, table, tuple([row[column.label] for column in selectable.primary_keys]))
        
_global_identitymap = IdentityMap()

def clear_identity():
    _global_identitymap.clear()
