import testbase
from sqlalchemy import *


class O2MTest(testbase.ORMTest):
    """deals with inheritance and one-to-many relationships"""
    def define_tables(self, metadata):
        global foo, bar, blub
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

class AddPropTest(testbase.ORMTest):
    """testing that construction of inheriting mappers works regardless of when extra properties
    are added to the superclass mapper"""
    def define_tables(self, metadata):
        global content_type, content, product
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
        
class EagerLazyTest(testbase.ORMTest):
    """tests eager load/lazy load of child items off inheritance mappers, tests that
    LazyLoader constructs the right query condition."""
    def define_tables(self, metadata):
        global foo, bar, bar_foo
        foo = Table('foo', metadata, Column('id', Integer, Sequence('foo_seq'), primary_key=True),
        Column('data', String(30)))
        bar = Table('bar', metadata, Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
     Column('data', String(30)))

        bar_foo = Table('bar_foo', metadata,
        Column('bar_id', Integer, ForeignKey('bar.id')),
        Column('foo_id', Integer, ForeignKey('foo.id'))
        )

    def testbasic(self):
        class Foo(object): pass
        class Bar(Foo): pass

        foos = mapper(Foo, foo)
        bars = mapper(Bar, bar, inherits=foos)
        bars.add_property('lazy', relation(foos, bar_foo, lazy=True))
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


class FlushTest(testbase.ORMTest):
    """test dependency sorting among inheriting mappers"""
    def define_tables(self, metadata):
        global users, roles, user_roles, admins
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
        # have to ensure that two dependency processors dont fire off and insert the
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
