import testbase
from sqlalchemy import *
import string
import sys

class Principal( object ):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

class User( Principal ):
    pass

class Group( Principal ):
    pass

class InheritTest(testbase.AssertMixin):
    """deals with inheritance and many-to-many relationships"""
    def setUpAll(self):
        global principals
        global users
        global groups
        global user_group_map
        global metadata
        metadata = BoundMetaData(testbase.db)
        principals = Table(
            'principals',
            metadata,
            Column('principal_id', Integer, Sequence('principal_id_seq', optional=False), primary_key=True),
            Column('name', String(50), nullable=False),    
            )

        users = Table(
            'prin_users',
            metadata, 
            Column('principal_id', Integer, ForeignKey('principals.principal_id'), primary_key=True),
            Column('password', String(50), nullable=False),
            Column('email', String(50), nullable=False),
            Column('login_id', String(50), nullable=False),

            )

        groups = Table(
            'prin_groups',
            metadata,
            Column( 'principal_id', Integer, ForeignKey('principals.principal_id'), primary_key=True),

            )

        user_group_map = Table(
            'prin_user_group_map',
            metadata,
            Column('user_id', Integer, ForeignKey( "prin_users.principal_id"), primary_key=True ),
            Column('group_id', Integer, ForeignKey( "prin_groups.principal_id"), primary_key=True ),
            #Column('user_id', Integer, ForeignKey( "prin_users.principal_id"),  ),
            #Column('group_id', Integer, ForeignKey( "prin_groups.principal_id"),  ),    

            )

        metadata.create_all()
        
    def tearDownAll(self):
        metadata.drop_all()

    def setUp(self):
        clear_mappers()
        
    def testbasic(self):
        mapper( Principal, principals )
        mapper( 
            User, 
            users,
            inherits=Principal
            )

        mapper( 
            Group,
            groups,
            inherits=Principal,
            properties=dict( users = relation(User, secondary=user_group_map, lazy=True, backref="groups") )
            )

        g = Group(name="group1")
        g.users.append(User(name="user1", password="pw", email="foo@bar.com", login_id="lg1"))
        sess = create_session()
        sess.save(g)
        sess.flush()
        # TODO: put an assertion
        
class InheritTest2(testbase.AssertMixin):
    """deals with inheritance and many-to-many relationships"""
    def setUpAll(self):
        global foo, bar, foo_bar, metadata
        metadata = BoundMetaData(testbase.db)
        foo = Table('foo', metadata,
            Column('id', Integer, Sequence('foo_id_seq'), primary_key=True),
            Column('data', String(20)),
            )

        bar = Table('bar', metadata,
            Column('bid', Integer, ForeignKey('foo.id'), primary_key=True),
            #Column('fid', Integer, ForeignKey('foo.id'), )
            )

        foo_bar = Table('foo_bar', metadata,
            Column('foo_id', Integer, ForeignKey('foo.id')),
            Column('bar_id', Integer, ForeignKey('bar.bid')))
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()

    def testbasic(self):
        class Foo(object): 
            def __init__(self, data=None):
                self.data = data
            def __str__(self):
                return "Foo(%s)" % self.data
            def __repr__(self):
                return str(self)

        mapper(Foo, foo)
        class Bar(Foo):
            def __str__(self):
                return "Bar(%s)" % self.data

        mapper(Bar, bar, inherits=Foo, properties={
            'foos': relation(Foo, secondary=foo_bar, lazy=False)
        })
        
        sess = create_session()
        b = Bar('barfoo', _sa_session=sess)
        sess.flush()

        f1 = Foo('subfoo1')
        f2 = Foo('subfoo2')
        b.foos.append(f1)
        b.foos.append(f2)

        sess.flush()
        sess.clear()

        l = sess.query(Bar).select()
        print l[0]
        print l[0].foos
        self.assert_result(l, Bar,
#            {'id':1, 'data':'barfoo', 'bid':1, 'foos':(Foo, [{'id':2,'data':'subfoo1'}, {'id':3,'data':'subfoo2'}])},
            {'id':b.id, 'data':'barfoo', 'foos':(Foo, [{'id':f1.id,'data':'subfoo1'}, {'id':f2.id,'data':'subfoo2'}])},
            )

class InheritTest3(testbase.AssertMixin):
    """deals with inheritance and many-to-many relationships"""
    def setUpAll(self):
        global foo, bar, blub, bar_foo, blub_bar, blub_foo,metadata
        metadata = BoundMetaData(testbase.db)
        # the 'data' columns are to appease SQLite which cant handle a blank INSERT
        foo = Table('foo', metadata,
            Column('id', Integer, Sequence('foo_seq'), primary_key=True),
            Column('data', String(20)))

        bar = Table('bar', metadata,
            Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
            Column('data', String(20)))

        blub = Table('blub', metadata,
            Column('id', Integer, ForeignKey('bar.id'), primary_key=True),
            Column('data', String(20)))

        bar_foo = Table('bar_foo', metadata, 
            Column('bar_id', Integer, ForeignKey('bar.id')),
            Column('foo_id', Integer, ForeignKey('foo.id')))
            
        blub_bar = Table('bar_blub', metadata,
            Column('blub_id', Integer, ForeignKey('blub.id')),
            Column('bar_id', Integer, ForeignKey('bar.id')))

        blub_foo = Table('blub_foo', metadata,
            Column('blub_id', Integer, ForeignKey('blub.id')),
            Column('foo_id', Integer, ForeignKey('foo.id')))
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()

    def tearDown(self):
        for table in metadata.table_iterator():
            table.delete().execute()
            
    def testbasic(self):
        class Foo(object):
            def __init__(self, data=None):
                self.data = data
            def __repr__(self):
                return "Foo id %d, data %s" % (self.id, self.data)
        mapper(Foo, foo)

        class Bar(Foo):
            def __repr__(self):
                return "Bar id %d, data %s" % (self.id, self.data)
                
        mapper(Bar, bar, inherits=Foo, properties={
            'foos' :relation(Foo, secondary=bar_foo, lazy=True)
        })

        sess = create_session()
        b = Bar('bar #1', _sa_session=sess)
        b.foos.append(Foo("foo #1"))
        b.foos.append(Foo("foo #2"))
        sess.flush()
        compare = repr(b) + repr(b.foos)
        sess.clear()
        l = sess.query(Bar).select()
        self.echo(repr(l[0]) + repr(l[0].foos))
        self.assert_(repr(l[0]) + repr(l[0].foos) == compare)
    
    def testadvanced(self):    
        class Foo(object):
            def __init__(self, data=None):
                self.data = data
            def __repr__(self):
                return "Foo id %d, data %s" % (self.id, self.data)
        mapper(Foo, foo)

        class Bar(Foo):
            def __repr__(self):
                return "Bar id %d, data %s" % (self.id, self.data)
        mapper(Bar, bar, inherits=Foo)

        class Blub(Bar):
            def __repr__(self):
                return "Blub id %d, data %s, bars %s, foos %s" % (self.id, self.data, repr([b for b in self.bars]), repr([f for f in self.foos]))
            
        mapper(Blub, blub, inherits=Bar, properties={
            'bars':relation(Bar, secondary=blub_bar, lazy=False),
            'foos':relation(Foo, secondary=blub_foo, lazy=False),
        })

        sess = create_session()
        f1 = Foo("foo #1", _sa_session=sess)
        b1 = Bar("bar #1", _sa_session=sess)
        b2 = Bar("bar #2", _sa_session=sess)
        bl1 = Blub("blub #1", _sa_session=sess)
        bl1.foos.append(f1)
        bl1.bars.append(b2)
        sess.flush()
        compare = repr(bl1)
        blubid = bl1.id
        sess.clear()

        l = sess.query(Blub).select()
        self.echo(l)
        self.assert_(repr(l[0]) == compare)
        sess.clear()
        x = sess.query(Blub).get_by(id=blubid)
        self.echo(x)
        self.assert_(repr(x) == compare)
        
class InheritTest4(testbase.AssertMixin):
    """deals with inheritance and one-to-many relationships"""
    def setUpAll(self):
        global foo, bar, blub, metadata
        metadata = BoundMetaData(testbase.db)
        # the 'data' columns are to appease SQLite which cant handle a blank INSERT
        foo = Table('foo', metadata,
            Column('id', Integer, Sequence('foo_seq'), primary_key=True),
            Column('data', String(20)))

        bar = Table('bar', metadata,
            Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
            Column('data', String(20)))

        blub = Table('blub', metadata,
            Column('id', Integer, ForeignKey('bar.id'), primary_key=True),
            Column('foo_id', Integer, ForeignKey('foo.id'), nullable=False),
            Column('data', String(20)))
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()

    def tearDown(self):
        for table in metadata.table_iterator():
            table.delete().execute()

    def testbasic(self):
        class Foo(object):
            def __init__(self, data=None):
                self.data = data
            def __repr__(self):
                return "Foo id %d, data %s" % (self.id, self.data)
        mapper(Foo, foo)

        class Bar(Foo):
            def __repr__(self):
                return "Bar id %d, data %s" % (self.id, self.data)

        mapper(Bar, bar, inherits=Foo)
        
        class Blub(Bar):
            def __repr__(self):
                return "Blub id %d, data %s" % (self.id, self.data)

        mapper(Blub, blub, inherits=Bar, properties={
            'parent_foo':relation(Foo)
        })

        sess = create_session()
        b1 = Blub("blub #1", _sa_session=sess)
        b2 = Blub("blub #2", _sa_session=sess)
        f = Foo("foo #1", _sa_session=sess)
        b1.parent_foo = f
        b2.parent_foo = f
        sess.flush()
        compare = repr(b1) + repr(b2) + repr(b1.parent_foo) + repr(b2.parent_foo)
        sess.clear()
        l = sess.query(Blub).select()
        result = repr(l[0]) + repr(l[1]) + repr(l[0].parent_foo) + repr(l[1].parent_foo)
        self.echo(result)
        self.assert_(compare == result)
        self.assert_(l[0].parent_foo.data == 'foo #1' and l[1].parent_foo.data == 'foo #1')

class InheritTest5(testbase.AssertMixin):
    """testing that construction of inheriting mappers works regardless of when extra properties
    are added to the superclass mapper"""
    def setUpAll(self):
        global content_type, content, product, metadata
        metadata = BoundMetaData(testbase.db)
        content_type = Table('content_type', metadata, 
            Column('id', Integer, primary_key=True)
            )
        content = Table('content', metadata,
            Column('id', Integer, primary_key=True),
            Column('content_type_id', Integer, ForeignKey('content_type.id'))
            )
        product = Table('product', metadata, 
            Column('id', Integer, ForeignKey('content.id'), primary_key=True)
        )
    def tearDownAll(self):
        pass
    def tearDown(self):
        pass

    def testbasic(self):
        class ContentType(object): pass
        class Content(object): pass
        class Product(Content): pass

        content_types = mapper(ContentType, content_type)
        contents = mapper(Content, content, properties={
            'content_type':relation(content_types)
        })
        #contents.add_property('content_type', relation(content_types)) #adding this makes the inheritance stop working
        # shouldnt throw exception
        products = mapper(Product, product, inherits=contents)
        # TODO: assertion ??

    def testbackref(self):
        """tests adding a property to the superclass mapper"""
        class ContentType(object): pass
        class Content(object): pass
        class Product(Content): pass

        contents = mapper(Content, content)
        products = mapper(Product, product, inherits=contents)
        content_types = mapper(ContentType, content_type, properties={
            'content':relation(contents, backref='contenttype')
        })
        p = Product()
        p.contenttype = ContentType()
        # TODO: assertion ??
        
class InheritTest6(testbase.AssertMixin):
    """tests eager load/lazy load of child items off inheritance mappers, tests that
    LazyLoader constructs the right query condition."""
    def setUpAll(self):
        global foo, bar, bar_foo, metadata
        metadata=BoundMetaData(testbase.db)
        foo = Table('foo', metadata, Column('id', Integer, Sequence('foo_seq'), primary_key=True),
        Column('data', String(30)))
        bar = Table('bar', metadata, Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
     Column('data', String(30)))

        bar_foo = Table('bar_foo', metadata,
        Column('bar_id', Integer, ForeignKey('bar.id')),
        Column('foo_id', Integer, ForeignKey('foo.id'))
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
        
    def testbasic(self):
        class Foo(object): pass
        class Bar(Foo): pass

        foos = mapper(Foo, foo)
        bars = mapper(Bar, bar, inherits=foos)
        bars.add_property('lazy', relation(foos, bar_foo, lazy=True))
        print bars.props['lazy'].primaryjoin, bars.props['lazy'].secondaryjoin
        bars.add_property('eager', relation(foos, bar_foo, lazy=False))

        foo.insert().execute(data='foo1')
        bar.insert().execute(id=1, data='bar1')

        foo.insert().execute(data='foo2')
        bar.insert().execute(id=2, data='bar2')

        foo.insert().execute(data='foo3') #3
        foo.insert().execute(data='foo4') #4

        bar_foo.insert().execute(bar_id=1, foo_id=3)
        bar_foo.insert().execute(bar_id=2, foo_id=4)
        
        sess = create_session()
        q = sess.query(Bar)
        self.assert_(len(q.selectfirst().lazy) == 1)
        self.assert_(len(q.selectfirst().eager) == 1)


class InheritTest7(testbase.AssertMixin):
    """test dependency sorting among inheriting mappers"""
    def setUpAll(self):
        global users, roles, user_roles, admins, metadata
        metadata=BoundMetaData(testbase.db)
        users = Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('email', String(128)),
            Column('password', String(16)),
        )

        roles = Table('role', metadata,
            Column('id', Integer, primary_key=True),
            Column('description', String(32)) 
        )

        user_roles = Table('user_role', metadata,
            Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
            Column('role_id', Integer, ForeignKey('role.id'), primary_key=True)
        )

        admins = Table('admin', metadata,
            Column('id', Integer, primary_key=True),
            Column('user_id', Integer, ForeignKey('users.id'))
        )
        metadata.create_all()
    def tearDownAll(self):
        metadata.drop_all()
    def tearDown(self):
        for t in metadata.table_iterator(reverse=True):
            t.delete().execute()
            
    def testone(self):
        class User(object):pass
        class Role(object):pass
        class Admin(User):pass
        role_mapper = mapper(Role, roles)
        user_mapper = mapper(User, users, properties = {
                'roles' : relation(Role, secondary=user_roles, lazy=False, private=False) 
            }
        )
        admin_mapper = mapper(Admin, admins, inherits=user_mapper)
        sess = create_session()
        adminrole = Role('admin')
        sess.save(adminrole)
        sess.flush()

        # create an Admin, and append a Role.  the dependency processors
        # corresponding to the "roles" attribute for the Admin mapper and the User mapper
        # have to insure that two dependency processors dont fire off and insert the
        # many to many row twice.
        a = Admin()
        a.roles.append(adminrole)
        a.password = 'admin'
        sess.save(a)
        sess.flush()
        
        assert user_roles.count().scalar() == 1

    def testtwo(self):
        class User(object):
            def __init__(self, email=None, password=None):
                self.email = email
                self.password = password

        class Role(object):
            def __init__(self, description=None):
                self.description = description

        class Admin(User):pass

        role_mapper = mapper(Role, roles)
        user_mapper = mapper(User, users, properties = {
                'roles' : relation(Role, secondary=user_roles, lazy=False, private=False)
            }
        )

        admin_mapper = mapper(Admin, admins, inherits=user_mapper) 

        # create roles
        adminrole = Role('admin')

        sess = create_session()
        sess.save(adminrole)
        sess.flush()

        # create admin user
        a = Admin(email='tim', password='admin')
        a.roles.append(adminrole)
        sess.save(a)
        sess.flush()

        a.password = 'sadmin'
        sess.flush()
        assert user_roles.count().scalar() == 1
        
if __name__ == "__main__":    
    testbase.main()
