from sqlalchemy import *
import testbase
import string
import sqlalchemy.attributes as attr
import sys

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
            testbase.db.tables.clear()
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
        testbase.db.tables.clear()

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
                # the old way, you needed to explicitly set up a compound
                # column like this.  but now the mapper uses SyncRules to match up
                # the parent/child inherited columns
                #'id':[bar.c.bid, foo.c.id]
            })

        #Bar.mapper.add_property('foos', relation(Foo.mapper, foo_bar, primaryjoin=bar.c.bid==foo_bar.c.bar_id, secondaryjoin=foo_bar.c.foo_id==foo.c.id, lazy=False))
        Bar.mapper.add_property('foos', relation(Foo.mapper, foo_bar, lazy=False))

        b = Bar('barfoo')
        objectstore.commit()

        f1 = Foo('subfoo1')
        f2 = Foo('subfoo2')
        b.foos.append(f1)
        b.foos.append(f2)

        objectstore.commit()
        objectstore.clear()

        l =b.mapper.select()
        print l[0]
        print l[0].foos
        self.assert_result(l, Bar,
#            {'id':1, 'data':'barfoo', 'bid':1, 'foos':(Foo, [{'id':2,'data':'subfoo1'}, {'id':3,'data':'subfoo2'}])},
            {'id':b.id, 'data':'barfoo', 'foos':(Foo, [{'id':f1.id,'data':'subfoo1'}, {'id':f2.id,'data':'subfoo2'}])},
            )

class InheritTest3(testbase.AssertMixin):
    def setUpAll(self):
        engine = testbase.db
        global foo, bar, blub, bar_foo, blub_bar, blub_foo,tables
        engine.engine.echo = 'debug'
        # the 'data' columns are to appease SQLite which cant handle a blank INSERT
        foo = Table('foo', engine,
            Column('id', Integer, Sequence('foo_seq'), primary_key=True),
            Column('data', String(20)))

        bar = Table('bar', engine,
            Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
            Column('data', String(20)))

        blub = Table('blub', engine,
            Column('id', Integer, ForeignKey('bar.id'), primary_key=True),
            Column('data', String(20)))

        bar_foo = Table('bar_foo', engine, 
            Column('bar_id', Integer, ForeignKey('bar.id')),
            Column('foo_id', Integer, ForeignKey('foo.id')))
            
        blub_bar = Table('bar_blub', engine,
            Column('blub_id', Integer, ForeignKey('blub.id')),
            Column('bar_id', Integer, ForeignKey('bar.id')))

        blub_foo = Table('blub_foo', engine,
            Column('blub_id', Integer, ForeignKey('blub.id')),
            Column('foo_id', Integer, ForeignKey('foo.id')))

        tables = [foo, bar, blub, bar_foo, blub_bar, blub_foo]
        for table in tables:
            table.create()
    def tearDownAll(self):
        for table in reversed(tables):
            table.drop()
        testbase.db.tables.clear()

    def tearDown(self):
        for table in reversed(tables):
            table.delete().execute()
            
    def testbasic(self):
        class Foo(object):
            def __init__(self, data=None):
                self.data = data
            def __repr__(self):
                return "Foo id %d, data %s" % (self.id, self.data)
        Foo.mapper = mapper(Foo, foo)

        class Bar(Foo):
            def __repr__(self):
                return "Bar id %d, data %s" % (self.id, self.data)
                
        Bar.mapper = mapper(Bar, bar, inherits=Foo.mapper, properties={
        #'foos' :relation(Foo.mapper, bar_foo, primaryjoin=bar.c.id==bar_foo.c.bar_id, lazy=False)
        'foos' :relation(Foo.mapper, bar_foo, lazy=True)
        })

        b = Bar('bar #1')
        b.foos.append(Foo("foo #1"))
        b.foos.append(Foo("foo #2"))
        objectstore.commit()
        objectstore.clear()
        l = Bar.mapper.select()
        print l[0], l[0].foos
    
    def testadvanced(self):    
        class Foo(object):
            def __init__(self, data=None):
                self.data = data
            def __repr__(self):
                return "Foo id %d, data %s" % (self.id, self.data)
        Foo.mapper = mapper(Foo, foo)

        class Bar(Foo):
            def __repr__(self):
                return "Bar id %d, data %s" % (self.id, self.data)
        Bar.mapper = mapper(Bar, bar, inherits=Foo.mapper)

        class Blub(Bar):
            def __repr__(self):
                return "Blub id %d, data %s, bars %s, foos %s" % (self.id, self.data, repr([b for b in self.bars]), repr([f for f in self.foos]))
            
        Blub.mapper = mapper(Blub, blub, inherits=Bar.mapper, properties={
#            'bars':relation(Bar.mapper, blub_bar, primaryjoin=blub.c.id==blub_bar.c.blub_id, lazy=False),
#            'foos':relation(Foo.mapper, blub_foo, primaryjoin=blub.c.id==blub_foo.c.blub_id, lazy=False),
            'bars':relation(Bar.mapper, blub_bar, lazy=False),
            'foos':relation(Foo.mapper, blub_foo, lazy=False),
        })

        useobjects = True
        if (useobjects):
            f1 = Foo("foo #1")
            b1 = Bar("bar #1")
            b2 = Bar("bar #2")
            bl1 = Blub("blub #1")
            bl1.foos.append(f1)
            bl1.bars.append(b2)
            objectstore.commit()
            compare = repr(bl1)
            blubid = bl1.id
            objectstore.clear()
        else:
            foo.insert().execute(data='foo #1')
            foo.insert().execute(data='foo #2')
            bar.insert().execute(id=1, data="bar #1")
            bar.insert().execute(id=2, data="bar #2")
            blub.insert().execute(id=1, data="blub #1")
            blub_bar.insert().execute(blub_id=1, bar_id=2)
            blub_foo.insert().execute(blub_id=1, foo_id=2)

        l = Blub.mapper.select()
        self.echo(l)
        self.assert_(repr(l[0]) == compare)
        objectstore.clear()
        x = Blub.mapper.get_by(id=blubid) #traceback 2
        self.echo(x)
        self.assert_(repr(x) == compare)
        


if __name__ == "__main__":    
    testbase.main()
