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

class InheritTest(testbase.ORMTest):
    """deals with inheritance and many-to-many relationships"""
    def define_tables(self, metadata):
        global principals
        global users
        global groups
        global user_group_map

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
        
class InheritTest2(testbase.ORMTest):
    """deals with inheritance and many-to-many relationships"""
    def define_tables(self, metadata):
        global foo, bar, foo_bar
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

    def testget(self):
        class Foo(object):
            def __init__(self, data=None):
                self.data = data
        class Bar(Foo):pass
        
        mapper(Foo, foo)
        mapper(Bar, bar, inherits=Foo)
        
        b = Bar('somedata')
        sess = create_session()
        sess.save(b)
        sess.flush()
        sess.clear()
        
        # test that "bar.bid" does not need to be referenced in a get
        # (ticket 185)
        assert sess.query(Bar).get(b.id).id == b.id
        
    def testbasic(self):
        class Foo(object): 
            def __init__(self, data=None):
                self.data = data

        mapper(Foo, foo)
        class Bar(Foo):
            pass

        mapper(Bar, bar, inherits=Foo, properties={
            'foos': relation(Foo, secondary=foo_bar, lazy=False)
        })
        
        sess = create_session()
        b = Bar('barfoo')
        sess.save(b)
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

class InheritTest3(testbase.ORMTest):
    """deals with inheritance and many-to-many relationships"""
    def define_tables(self, metadata):
        global foo, bar, blub, bar_foo, blub_bar, blub_foo

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
        
class InheritTest4(testbase.ORMTest):
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

class InheritTest5(testbase.ORMTest):
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
        
class InheritTest6(testbase.ORMTest):
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


class InheritTest7(testbase.ORMTest):
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
        class Role(object):
            def __init__(self, description):
                self.description = description
        class Admin(User):pass
        role_mapper = mapper(Role, roles)
        user_mapper = mapper(User, users, properties = {
                'roles' : relation(Role, secondary=user_roles, lazy=False, private=False) 
            }
        )
        admin_mapper = mapper(Admin, admins, inherits=user_mapper, properties={'aid':admins.c.id})
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

        admin_mapper = mapper(Admin, admins, inherits=user_mapper, properties={'aid':admins.c.id}) 

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

class InheritTest8(testbase.ORMTest):
    """test the construction of mapper.primary_key when an inheriting relationship
    joins on a column other than primary key column."""
    keep_data = True
    
    def define_tables(self, metadata):
        global person_table, employee_table, Person, Employee
        
        person_table = Table("persons", metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(80)),
                )

        employee_table = Table("employees", metadata,
                Column("id", Integer, primary_key=True),
                Column("salary", Integer),
                Column("person_id", Integer, ForeignKey("persons.id")),
                )

        class Person(object):
            def __init__(self, name):
                self.name = name

        class Employee(Person): pass

        import warnings
        warnings.filterwarnings("error", r".*On mapper.*distinct primary key")
    
    def insert_data(self):
        person_insert = person_table.insert()
        person_insert.execute(id=1, name='alice')
        person_insert.execute(id=2, name='bob')

        employee_insert = employee_table.insert()
        employee_insert.execute(id=2, salary=250, person_id=1) # alice
        employee_insert.execute(id=3, salary=200, person_id=2) # bob
        
    def test_implicit(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper)
        try:
            print class_mapper(Employee).primary_key
            assert list(class_mapper(Employee).primary_key) == [person_table.c.id, employee_table.c.id]
            assert False
        except RuntimeWarning, e:
            assert str(e) == "On mapper Mapper|Employee|employees, primary key column 'employees.id' is being combined with distinct primary key column 'persons.id' in attribute 'id'.  Use explicit properties to give each column its own mapped attribute name."

    def test_explicit_props(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper, properties={'pid':person_table.c.id, 'eid':employee_table.c.id})
        self._do_test(True)
    
    def test_explicit_composite_pk(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper, primary_key=[person_table.c.id, employee_table.c.id])
        try:
            self._do_test(True)
            assert False
        except RuntimeWarning, e:
            assert str(e) == "On mapper Mapper|Employee|employees, primary key column 'employees.id' is being combined with distinct primary key column 'persons.id' in attribute 'id'.  Use explicit properties to give each column its own mapped attribute name."

    def test_explicit_pk(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper, primary_key=[person_table.c.id])
        self._do_test(False)
        
    def _do_test(self, composite):
        session = create_session()
        query = session.query(Employee)

        if composite:
            try:
                query.get(1)
                assert False
            except exceptions.InvalidRequestError, e:
                assert str(e) == "Could not find enough values to formulate primary key for query.get(); primary key columns are 'persons.id', 'employees.id'"
            alice1 = query.get([1,2])
            bob = query.get([2,3])
            alice2 = query.get([1,2])
        else:
            alice1 = query.get(1)
            bob = query.get(2)
            alice2 = query.get(1)
            
            assert alice1.name == alice2.name == 'alice'
            assert bob.name == 'bob'
        

        
if __name__ == "__main__":    
    testbase.main()
