import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exceptions, util
from sqlalchemy.orm import *
from testlib import *
from testlib import fixtures

class O2MTest(ORMTest):
    """deals with inheritance and one-to-many relationships"""
    def define_tables(self, metadata):
        global foo, bar, blub
        foo = Table('foo', metadata,
            Column('id', Integer, Sequence('foo_seq', optional=True),
                   primary_key=True),
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
        b1 = Blub("blub #1")
        b2 = Blub("blub #2")
        f = Foo("foo #1")
        sess.save(b1)
        sess.save(b2)
        sess.save(f)
        b1.parent_foo = f
        b2.parent_foo = f
        sess.flush()
        compare = ','.join([repr(b1), repr(b2), repr(b1.parent_foo), repr(b2.parent_foo)])
        sess.clear()
        l = sess.query(Blub).all()
        result = ','.join([repr(l[0]), repr(l[1]), repr(l[0].parent_foo), repr(l[1].parent_foo)])
        print compare
        print result
        self.assert_(compare == result)
        self.assert_(l[0].parent_foo.data == 'foo #1' and l[1].parent_foo.data == 'foo #1')

class CascadeTest(ORMTest):
    """that cascades on polymorphic relations continue
    cascading along the path of the instance's mapper, not
    the base mapper."""

    def define_tables(self, metadata):
        global t1, t2, t3, t4
        t1= Table('t1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30))
            )

        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True),
            Column('t1id', Integer, ForeignKey('t1.id')),
            Column('type', String(30)),
            Column('data', String(30))
        )
        t3 = Table('t3', metadata,
            Column('id', Integer, ForeignKey('t2.id'), primary_key=True),
            Column('moredata', String(30)))

        t4 = Table('t4', metadata,
            Column('id', Integer, primary_key=True),
            Column('t3id', Integer, ForeignKey('t3.id')),
            Column('data', String(30)))

    def test_cascade(self):
        class T1(fixtures.Base):
            pass
        class T2(fixtures.Base):
            pass
        class T3(T2):
            pass
        class T4(fixtures.Base):
            pass

        mapper(T1, t1, properties={
            't2s':relation(T2, cascade="all")
        })
        mapper(T2, t2, polymorphic_on=t2.c.type, polymorphic_identity='t2')
        mapper(T3, t3, inherits=T2, polymorphic_identity='t3', properties={
            't4s':relation(T4, cascade="all")
        })
        mapper(T4, t4)

        sess = create_session()
        t1_1 = T1(data='t1')

        t3_1 = T3(data ='t3', moredata='t3')
        t2_1 = T2(data='t2')

        t1_1.t2s.append(t2_1)
        t1_1.t2s.append(t3_1)

        t4_1 = T4(data='t4')
        t3_1.t4s.append(t4_1)

        sess.save(t1_1)


        assert t4_1 in sess.new
        sess.flush()

        sess.delete(t1_1)
        assert t4_1 in sess.deleted
        sess.flush()



class GetTest(ORMTest):
    def define_tables(self, metadata):
        global foo, bar, blub
        foo = Table('foo', metadata,
            Column('id', Integer, Sequence('foo_seq', optional=True),
                   primary_key=True),
            Column('type', String(30)),
            Column('data', String(20)))

        bar = Table('bar', metadata,
            Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
            Column('data', String(20)))

        blub = Table('blub', metadata,
            Column('id', Integer, primary_key=True),
            Column('foo_id', Integer, ForeignKey('foo.id')),
            Column('bar_id', Integer, ForeignKey('bar.id')),
            Column('data', String(20)))

    def create_test(polymorphic, name):
        def test_get(self):
            class Foo(object):
                pass

            class Bar(Foo):
                pass

            class Blub(Bar):
                pass

            if polymorphic:
                mapper(Foo, foo, polymorphic_on=foo.c.type, polymorphic_identity='foo')
                mapper(Bar, bar, inherits=Foo, polymorphic_identity='bar')
                mapper(Blub, blub, inherits=Bar, polymorphic_identity='blub')
            else:
                mapper(Foo, foo)
                mapper(Bar, bar, inherits=Foo)
                mapper(Blub, blub, inherits=Bar)

            sess = create_session()
            f = Foo()
            b = Bar()
            bl = Blub()
            sess.save(f)
            sess.save(b)
            sess.save(bl)
            sess.flush()

            if polymorphic:
                def go():
                    assert sess.query(Foo).get(f.id) == f
                    assert sess.query(Foo).get(b.id) == b
                    assert sess.query(Foo).get(bl.id) == bl
                    assert sess.query(Bar).get(b.id) == b
                    assert sess.query(Bar).get(bl.id) == bl
                    assert sess.query(Blub).get(bl.id) == bl

                self.assert_sql_count(testing.db, go, 0)
            else:
                # this is testing the 'wrong' behavior of using get()
                # polymorphically with mappers that are not configured to be
                # polymorphic.  the important part being that get() always
                # returns an instance of the query's type.
                def go():
                    assert sess.query(Foo).get(f.id) == f

                    bb = sess.query(Foo).get(b.id)
                    assert isinstance(b, Foo) and bb.id==b.id

                    bll = sess.query(Foo).get(bl.id)
                    assert isinstance(bll, Foo) and bll.id==bl.id

                    assert sess.query(Bar).get(b.id) == b

                    bll = sess.query(Bar).get(bl.id)
                    assert isinstance(bll, Bar) and bll.id == bl.id

                    assert sess.query(Blub).get(bl.id) == bl

                self.assert_sql_count(testing.db, go, 3)

        test_get = _function_named(test_get, name)
        return test_get

    test_get_polymorphic = create_test(True, 'test_get_polymorphic')
    test_get_nonpolymorphic = create_test(False, 'test_get_nonpolymorphic')


class ConstructionTest(ORMTest):
    def define_tables(self, metadata):
        global content_type, content, product
        content_type = Table('content_type', metadata,
            Column('id', Integer, primary_key=True)
            )
        content = Table('content', metadata,
            Column('id', Integer, primary_key=True),
            Column('content_type_id', Integer, ForeignKey('content_type.id')),
            Column('type', String(30))
            )
        product = Table('product', metadata,
            Column('id', Integer, ForeignKey('content.id'), primary_key=True)
        )

    def testbasic(self):
        class ContentType(object): pass
        class Content(object): pass
        class Product(Content): pass

        content_types = mapper(ContentType, content_type)
        try:
            contents = mapper(Content, content, properties={
                'content_type':relation(content_types)
            }, polymorphic_identity='contents')
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "Mapper 'Mapper|Content|content' specifies a polymorphic_identity of 'contents', but no mapper in it's hierarchy specifies the 'polymorphic_on' column argument"

    def testbackref(self):
        """tests adding a property to the superclass mapper"""
        class ContentType(object): pass
        class Content(object): pass
        class Product(Content): pass

        contents = mapper(Content, content, polymorphic_on=content.c.type, polymorphic_identity='content')
        products = mapper(Product, product, inherits=contents, polymorphic_identity='product')
        content_types = mapper(ContentType, content_type, properties={
            'content':relation(contents, backref='contenttype')
        })
        p = Product()
        p.contenttype = ContentType()
        # TODO: assertion ??

class EagerLazyTest(ORMTest):
    """tests eager load/lazy load of child items off inheritance mappers, tests that
    LazyLoader constructs the right query condition."""
    def define_tables(self, metadata):
        global foo, bar, bar_foo
        foo = Table('foo', metadata,
                    Column('id', Integer, Sequence('foo_seq', optional=True),
                           primary_key=True),
                    Column('data', String(30)))
        bar = Table('bar', metadata,
                    Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
                    Column('data', String(30)))

        bar_foo = Table('bar_foo', metadata,
                        Column('bar_id', Integer, ForeignKey('bar.id')),
                        Column('foo_id', Integer, ForeignKey('foo.id'))
        )

    @testing.fails_on('maxdb')
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
        self.assert_(len(q.first().lazy) == 1)
        self.assert_(len(q.first().eager) == 1)


class FlushTest(ORMTest):
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
            Column('admin_id', Integer, primary_key=True),
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
        adminrole = Role()
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

class VersioningTest(ORMTest):
    def define_tables(self, metadata):
        global base, subtable, stuff
        base = Table('base', metadata,
            Column('id', Integer, Sequence('version_test_seq', optional=True), primary_key=True ),
            Column('version_id', Integer, nullable=False),
            Column('value', String(40)),
            Column('discriminator', Integer, nullable=False)
        )
        subtable = Table('subtable', metadata,
            Column('id', None, ForeignKey('base.id'), primary_key=True),
            Column('subdata', String(50))
            )
        stuff = Table('stuff', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent', Integer, ForeignKey('base.id'))
            )

    @engines.close_open_connections
    def test_save_update(self):
        class Base(fixtures.Base):
            pass
        class Sub(Base):
            pass
        class Stuff(Base):
            pass
        mapper(Stuff, stuff)
        mapper(Base, base, polymorphic_on=base.c.discriminator, version_id_col=base.c.version_id, polymorphic_identity=1, properties={
            'stuff':relation(Stuff)
        })
        mapper(Sub, subtable, inherits=Base, polymorphic_identity=2)

        sess = create_session()

        b1 = Base(value='b1')
        s1 = Sub(value='sub1', subdata='some subdata')
        sess.save(b1)
        sess.save(s1)

        sess.flush()

        sess2 = create_session()
        s2 = sess2.query(Base).get(s1.id)
        s2.subdata = 'sess2 subdata'

        s1.subdata = 'sess1 subdata'

        sess.flush()

        try:
            sess2.query(Base).with_lockmode('read').get(s1.id)
            assert False
        except exceptions.ConcurrentModificationError, e:
            assert True

        try:
            sess2.flush()
            assert False
        except exceptions.ConcurrentModificationError, e:
            assert True

        sess2.refresh(s2)
        assert s2.subdata == 'sess1 subdata'
        s2.subdata = 'sess2 subdata'
        sess2.flush()

    def test_delete(self):
        class Base(fixtures.Base):
            pass
        class Sub(Base):
            pass

        mapper(Base, base, polymorphic_on=base.c.discriminator, version_id_col=base.c.version_id, polymorphic_identity=1)
        mapper(Sub, subtable, inherits=Base, polymorphic_identity=2)

        sess = create_session()

        b1 = Base(value='b1')
        s1 = Sub(value='sub1', subdata='some subdata')
        s2 = Sub(value='sub2', subdata='some other subdata')
        sess.save(b1)
        sess.save(s1)
        sess.save(s2)

        sess.flush()

        sess2 = create_session()
        s3 = sess2.query(Base).get(s1.id)
        sess2.delete(s3)
        sess2.flush()

        s2.subdata = 'some new subdata'
        sess.flush()

        try:
            s1.subdata = 'some new subdata'
            sess.flush()
            assert False
        except exceptions.ConcurrentModificationError, e:
            assert True



class DistinctPKTest(ORMTest):
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
        assert list(class_mapper(Employee).primary_key) == [person_table.c.id]

    def test_explicit_props(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper, properties={'pid':person_table.c.id, 'eid':employee_table.c.id})
        self._do_test(True)

    def test_explicit_composite_pk(self):
        person_mapper = mapper(Person, person_table)
        try:
            mapper(Employee, employee_table, inherits=person_mapper, primary_key=[person_table.c.id, employee_table.c.id])
            self._do_test(True)
            assert False
        except exceptions.SAWarning, e:
            assert str(e) == "On mapper Mapper|Employee|employees, primary key column 'employees.id' is being combined with distinct primary key column 'persons.id' in attribute 'id'.  Use explicit properties to give each column its own mapped attribute name.", str(e)

    def test_explicit_pk(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, inherits=person_mapper, primary_key=[person_table.c.id])
        self._do_test(False)

    def _do_test(self, composite):
        session = create_session()
        query = session.query(Employee)

        if composite:
            alice1 = query.get([1,2])
            bob = query.get([2,3])
            alice2 = query.get([1,2])
        else:
            alice1 = query.get(1)
            bob = query.get(2)
            alice2 = query.get(1)

            assert alice1.name == alice2.name == 'alice'
            assert bob.name == 'bob'

class SyncCompileTest(ORMTest):
    """test that syncrules compile properly on custom inherit conds"""
    def define_tables(self, metadata):
        global _a_table, _b_table, _c_table

        _a_table = Table('a', metadata,
           Column('id', Integer, primary_key=True),
           Column('data1', String(128))
        )

        _b_table = Table('b', metadata,
           Column('a_id', Integer, ForeignKey('a.id'), primary_key=True),
           Column('data2', String(128))
        )

        _c_table = Table('c', metadata,
        #   Column('a_id', Integer, ForeignKey('b.a_id'), primary_key=True), #works
           Column('b_a_id', Integer, ForeignKey('b.a_id'), primary_key=True),
           Column('data3', String(128))
        )

    def test_joins(self):
        for j1 in (None, _b_table.c.a_id==_a_table.c.id, _a_table.c.id==_b_table.c.a_id):
            for j2 in (None, _b_table.c.a_id==_c_table.c.b_a_id, _c_table.c.b_a_id==_b_table.c.a_id):
                self._do_test(j1, j2)
                for t in _a_table.metadata.table_iterator(reverse=True):
                    t.delete().execute().close()

    def _do_test(self, j1, j2):
        class A(object):
           def __init__(self, **kwargs):
               for key, value in kwargs.items():
                    setattr(self, key, value)

        class B(A):
            pass

        class C(B):
            pass

        mapper(A, _a_table)
        mapper(B, _b_table, inherits=A,
               inherit_condition=j1
               )
        mapper(C, _c_table, inherits=B,
               inherit_condition=j2
               )

        session = create_session()

        a = A(data1='a1')
        session.save(a)

        b = B(data1='b1', data2='b2')
        session.save(b)

        c = C(data1='c1', data2='c2', data3='c3')
        session.save(c)

        session.flush()
        session.clear()

        assert len(session.query(A).all()) == 3
        assert len(session.query(B).all()) == 2
        assert len(session.query(C).all()) == 1



if __name__ == "__main__":
    testenv.main()
