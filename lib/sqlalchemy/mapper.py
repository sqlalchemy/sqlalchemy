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
    '*' : addresses
})
"""

import sqlalchemy.sql as sql
import sqlalchemy.schema as schema

class Mapper:
    def __init__(self, class_, table, properties):
        self.class_ = class_
        self.table = table
        self.properties = properties

    def instance(self, row):
        pass
                
    def select_whereclause(self, whereclause, **params):
        pass
    
    def select_statement(self, statement, **params):
        pass

    def select(self, arg, **params):
        if isinstance(arg, sql.Select):
            return self.select_statement(arg, **params)
        else:
            return self.select_whereclause(arg, **params)
        
    def save(self, object):
        pass
        
    def delete(self, whereclause = None, **params):
        pass