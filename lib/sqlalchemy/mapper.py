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

class Mapper:
    def __init__(self, class_, table, properties = None, identitymap = None):
        self.class_ = class_
        self.table = table
        
        self.props = {}
        
        if properties is not None:
            for key, column in properties.iteritems():
                desc = (key, column)
                self.props["key_" + key] = desc
                self.props["column_" + column.label] = desc
                self.props["column_" + column.name] = desc
        else:
            for column in table.columns:
                desc = (column.name, column)
                self.props["key_" + column.name] = desc
                self.props["column_" + column.label] = desc
                self.props["column_" + column.name] = desc
                
        if identitymap is not None:
            self.identitymap = identitymap
        else:
            self.identitymap = _global_identitymap

    def _create(self, row):
        instance = self.class_()
        for desc in self.props.values():
            setattr(instance, desc[0], row[desc[1].name])
        return instance
            
    def instance(self, row):
        return self.identitymap.get(row, self.class_, self.table, creator = self._create)

    def get(self, id):
        """returns an instance of the object based on the given ID."""
        pass
        
    def _select_whereclause(self, whereclause = None, **params):
        statement = sql.select([self.table], whereclause)    
        return self._select_statement(statement, **params)
    
    def _select_statement(self, statement, **params):
        result = []
        cursor = ResultProxy(statement.execute(**params))
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            result.append(self.instance(row))
        return result

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
        
class IdentityMap:
    def __init__(self):
        self.map = {}
        self.keystereotypes = {}
        
    def get(self, row, class_, table, creator = None):
        """given a database row, a class to be instantiated, and a table corresponding 
        to the row, returns a corrseponding object instance, if any, from the identity
        map.  the primary keys specified in the table will be used to indicate which
        columns from the row form the effective key of the instance."""
        key = (class_, table, tuple([row[column.name] for column in table.primary_keys]))
        
        try:
            return self.map[key]
        except KeyError:
            return self.map.setdefault(key, creator(row))
            
    
    
_global_identitymap = IdentityMap()
