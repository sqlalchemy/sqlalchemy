from sqlalchemy import *
import testbase
import string
import sqlalchemy.attributes as attr



class Principal( object ):
    pass

class User( Principal ):
    pass

class Group( Principal ):
    pass

class InheritTest(testbase.AssertMixin):
        def setUpAll(self):
            global principals
            global users
            global groups
            global user_group_map
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

class InheritTest2(testbase.AssertMixin):
	def setUpAll(self):
		engine = testbase.db
		global foo, bar, foo_bar
		foo = Table('foo', engine,
		    Column('id', Integer, primary_key=True),
			Column('data', String(20)),
			).create()

		bar = Table('bar', engine,
		    Column('bid', Integer, ForeignKey('foo.id'), primary_key=True),
		    #Column('fid', Integer, ForeignKey('foo.id'), )
			).create()

		foo_bar = Table('foo_bar', engine,
		    Column('foo_id', Integer, ForeignKey('foo.id')),
		    Column('bar_id', Integer, ForeignKey('bar.bid'))).create()

	def tearDownAll(self):
		foo_bar.drop()
		bar.drop()
		foo.drop()

	def testbasic(self):
		class Foo(object): 
			def __init__(self, data=None):
				self.data = data
			def __str__(self):
				return "Foo(%s)" % self.data
			def __repr__(self):
				return str(self)

		Foo.mapper = mapper(Foo, foo)
		class Bar(Foo):
			def __str__(self):
				return "Bar(%s)" % self.data

		Bar.mapper = mapper(Bar, bar, inherits=Foo.mapper, properties = {
				# TODO: use syncrules for this
				'id':[bar.c.bid, foo.c.id]
			})

		Bar.mapper.add_property('foos', relation(Foo.mapper, foo_bar, primaryjoin=bar.c.bid==foo_bar.c.bar_id, secondaryjoin=foo_bar.c.foo_id==foo.c.id, lazy=False))
		#Bar.mapper.add_property('foos', relation(Foo.mapper, foo_bar, lazy=False))


		b = Bar('barfoo')
		objectstore.commit()


		b.foos.append(Foo('subfoo1'))
		b.foos.append(Foo('subfoo2'))

		objectstore.commit()
		objectstore.clear()

		l =b.mapper.select()
		print l[0]
		print l[0].foos


if __name__ == "__main__":    
    testbase.main()
