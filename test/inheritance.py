from sqlalchemy import *
import testbase
import string
import sqlalchemy.attributes as attr


principals = Table(
    'principals',
    testbase.db,
    Column('principal_id', Integer, Sequence('principal_id_seq', optional=False), primary_key=True),
    Column('name', String(50), nullable=False),    
    )

users = Table(
    'prin_users',
    testbase.db,
    Column('principal_id', Integer, ForeignKey('principals.principal_id'), primary_key=True),
    Column('password', String(50), nullable=False),
    Column('email', String(50), nullable=False),
    Column('login_id', String(50), nullable=False),
    
    )

groups = Table(
    'prin_groups',
    testbase.db,
    Column( 'principal_id', Integer, ForeignKey('principals.principal_id'), primary_key=True),
    
    )

user_group_map = Table(
    'prin_user_group_map',
    testbase.db,
    Column('user_id', Integer, ForeignKey( "prin_users.principal_id"), primary_key=True ),
    Column('group_id', Integer, ForeignKey( "prin_groups.principal_id"), primary_key=True ),
    #Column('user_id', Integer, ForeignKey( "prin_users.principal_id"),  ),
    #Column('group_id', Integer, ForeignKey( "prin_groups.principal_id"),  ),    
    
    )

class Principal( object ):
    pass

class User( Principal ):
    pass

class Group( Principal ):
    pass

class InheritTest(testbase.AssertMixin):
        def setUpAll(self):
            principals.create()
            users.create()
            groups.create()
            user_group_map.create()
        def tearDownAll(self):
            user_group_map.drop()
            groups.drop()
            users.drop()
            principals.drop()
        def setUp(self):
            objectstore.clear()
            clear_mappers()
            
        def testbasic(self):
            assign_mapper( Principal, principals )
            assign_mapper( 
                User, 
                users,
                inherits=Principal.mapper
                )

            assign_mapper( 
                Group,
                groups,
                inherits=Principal.mapper,
                properties=dict( users = relation(User.mapper, user_group_map, lazy=True, backref="groups") )
                )

            g = Group(name="group1")
            g.users.append(User(name="user1", password="pw", email="foo@bar.com", login_id="lg1"))
            
            objectstore.commit()

if __name__ == "__main__":    
    testbase.main()
