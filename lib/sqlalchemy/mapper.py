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

class Mapper(object):
    def __init__(self, class_, table, properties = None, identitymap = None):
        self.class_ = class_
        self.table = table
        
        self.props = {}
        
        for column in table.columns:
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
        cursor = ResultProxy(cursor)
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            result.append(self.identitymap.get(row, self.class_, self.table, creator = self._create))
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


    def _select_whereclause(self, whereclause = None, **params):
        statement = sql.select([self.table], whereclause)
        for value in self.props.values():
            value.setup(self.table, statement) 
        return self._select_statement(statement, **params)
    
    def _select_statement(self, statement, **params):
        statement.use_labels = True
        return self.instances(statement.execute(**params))

    def _create(self, row):
        instance = self.class_()
        for key, prop in self.props.iteritems():
            prop.execute(instance, key, row)

        return instance


class MapperProperty:
    def execute(self, instance, key, row):
        raise NotImplementedError()
    def setup(self, primarytable, statement):
        pass
        
class ColumnProperty(MapperProperty):
    def __init__(self, column):
        self.column = column
        
    def execute(self, instance, key, row):
        setattr(instance, key, row[self.column.label])

class EagerLoader(MapperProperty):
    def __init__(self, mapper, whereclause):
        self.mapper = mapper
        self.whereclause = whereclause
    def setup(self, primarytable, statement):
        statement.append_from(sql.outerjoin(primarytable, self.mapper.table, self.whereclause))
        statement.append_column(self.mapper.table)
    def execute(self, instance, key, row):
        print "eagerloader exec for key " + key
        list = getattr(instance, key, [])
        list.append(self.mapper._create(row))
        
        
    
class ResultProxy:
    def __init__(self, cursor):
        self.cursor = cursor
        metadata = cursor.description
        self.props = {}
        i = 0
        for item in metadata:
            self.props[item[0]] = i
            self.props[i] = i
            i+=1

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            return RowProxy(self, row)
        else:
            return None
        
class RowProxy:
    def __init__(self, parent, row):
        self.parent = parent
        self.row = row
    def __getitem__(self, key):
        return self.row[self.parent.props[key]]
        
class IdentityMap(object):
    def __init__(self):
        self.map = {}
        self.keystereotypes = {}
        
    def get(self, row, class_, table, creator = None):
        """given a database row, a class to be instantiated, and a table corresponding 
        to the row, returns a corrseponding object instance, if any, from the identity
        map.  the primary keys specified in the table will be used to indicate which
        columns from the row form the effective key of the instance."""
        key = (class_, table, tuple([row[column.label] for column in table.primary_keys]))
        
        try:
            return self.map[key]
        except KeyError:
            return self.map.setdefault(key, creator(row))
            
    
    
_global_identitymap = IdentityMap()
