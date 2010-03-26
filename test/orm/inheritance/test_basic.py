import warnings
from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
from sqlalchemy import *
from sqlalchemy import exc as sa_exc, util
from sqlalchemy.orm import *
from sqlalchemy.orm import exc as orm_exc

from sqlalchemy.test import testing, engines
from sqlalchemy.util import function_named
from test.orm import _base, _fixtures
from sqlalchemy.test.schema import Table, Column

class O2MTest(_base.MappedTest):
    """deals with inheritance and one-to-many relationships"""
    @classmethod
    def define_tables(cls, metadata):
        global foo, bar, blub
        foo = Table('foo', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(20)))

        bar = Table('bar', metadata,
            Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
            Column('data', String(20)))

        blub = Table('blub', metadata,
            Column('id', Integer, ForeignKey('bar.id'), primary_key=True),
            Column('foo_id', Integer, ForeignKey('foo.id'), nullable=False),
            Column('data', String(20)))

    def test_basic(self):
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
            'parent_foo':relationship(Foo)
        })

        sess = create_session()
        b1 = Blub("blub #1")
        b2 = Blub("blub #2")
        f = Foo("foo #1")
        sess.add(b1)
        sess.add(b2)
        sess.add(f)
        b1.parent_foo = f
        b2.parent_foo = f
        sess.flush()
        compare = ','.join([repr(b1), repr(b2), repr(b1.parent_foo), repr(b2.parent_foo)])
        sess.expunge_all()
        l = sess.query(Blub).all()
        result = ','.join([repr(l[0]), repr(l[1]), repr(l[0].parent_foo), repr(l[1].parent_foo)])
        print compare
        print result
        self.assert_(compare == result)
        self.assert_(l[0].parent_foo.data == 'foo #1' and l[1].parent_foo.data == 'foo #1')

class FalseDiscriminatorTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global t1
        t1 = Table('t1', metadata, 
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True), 
            Column('type', Boolean, nullable=False))
        
    def test_false_on_sub(self):
        class Foo(object):pass
        class Bar(Foo):pass
        mapper(Foo, t1, polymorphic_on=t1.c.type, polymorphic_identity=True)
        mapper(Bar, inherits=Foo, polymorphic_identity=False)
        sess = create_session()
        b1 = Bar()
        sess.add(b1)
        sess.flush()
        assert b1.type is False
        sess.expunge_all()
        assert isinstance(sess.query(Foo).one(), Bar)

    def test_false_on_base(self):
        class Ding(object):pass
        class Bat(Ding):pass
        mapper(Ding, t1, polymorphic_on=t1.c.type, polymorphic_identity=False)
        mapper(Bat, inherits=Ding, polymorphic_identity=True)
        sess = create_session()
        d1 = Ding()
        sess.add(d1)
        sess.flush()
        assert d1.type is False
        sess.expunge_all()
        assert sess.query(Ding).one() is not None
        
class PolymorphicSynonymTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global t1, t2
        t1 = Table('t1', metadata,
                   Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                   Column('type', String(10), nullable=False),
                   Column('info', String(255)))
        t2 = Table('t2', metadata,
                   Column('id', Integer, ForeignKey('t1.id'), primary_key=True),
                   Column('data', String(10), nullable=False))
    
    def test_polymorphic_synonym(self):
        class T1(_fixtures.Base):
            def info(self):
                return "THE INFO IS:" + self._info
            def _set_info(self, x):
                self._info = x
            info = property(info, _set_info)
            
        class T2(T1):pass
        
        mapper(T1, t1, polymorphic_on=t1.c.type, polymorphic_identity='t1', properties={
            'info':synonym('_info', map_column=True)
        })
        mapper(T2, t2, inherits=T1, polymorphic_identity='t2')
        sess = create_session()
        at1 = T1(info='at1')
        at2 = T2(info='at2', data='t2 data')
        sess.add(at1)
        sess.add(at2)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(T2).filter(T2.info=='at2').one(), at2)
        eq_(at2.info, "THE INFO IS:at2")
        
    
class CascadeTest(_base.MappedTest):
    """that cascades on polymorphic relationships continue
    cascading along the path of the instance's mapper, not
    the base mapper."""

    @classmethod
    def define_tables(cls, metadata):
        global t1, t2, t3, t4
        t1= Table('t1', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(30))
            )

        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('t1id', Integer, ForeignKey('t1.id')),
            Column('type', String(30)),
            Column('data', String(30))
        )
        t3 = Table('t3', metadata,
            Column('id', Integer, ForeignKey('t2.id'), primary_key=True),
            Column('moredata', String(30)))

        t4 = Table('t4', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('t3id', Integer, ForeignKey('t3.id')),
            Column('data', String(30)))

    def test_cascade(self):
        class T1(_fixtures.Base):
            pass
        class T2(_fixtures.Base):
            pass
        class T3(T2):
            pass
        class T4(_fixtures.Base):
            pass

        mapper(T1, t1, properties={
            't2s':relationship(T2, cascade="all")
        })
        mapper(T2, t2, polymorphic_on=t2.c.type, polymorphic_identity='t2')
        mapper(T3, t3, inherits=T2, polymorphic_identity='t3', properties={
            't4s':relationship(T4, cascade="all")
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

        sess.add(t1_1)


        assert t4_1 in sess.new
        sess.flush()

        sess.delete(t1_1)
        assert t4_1 in sess.deleted
        sess.flush()

class M2OUseGetTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('type', String(30))
        )
        Table('sub', metadata,
            Column('id', Integer, ForeignKey('base.id'), primary_key=True),
        )
        Table('related', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('sub_id', Integer, ForeignKey('sub.id')),
        )

    @testing.resolve_artifact_names
    def test_use_get(self):
        # test [ticket:1186]
        class Base(_base.BasicEntity):
            pass
        class Sub(Base):
            pass
        class Related(Base):
            pass
        mapper(Base, base, polymorphic_on=base.c.type, polymorphic_identity='b')
        mapper(Sub, sub, inherits=Base, polymorphic_identity='s')
        mapper(Related, related, properties={
            # previously, this was needed for the comparison to occur:
            # the 'primaryjoin' looks just like "Sub"'s "get" clause (based on the Base id),
            # and foreign_keys since that join condition doesn't actually have any fks in it
            #'sub':relationship(Sub, primaryjoin=base.c.id==related.c.sub_id, foreign_keys=related.c.sub_id)
            
            # now we can use this:
            'sub':relationship(Sub)
        })
        
        assert class_mapper(Related).get_property('sub').strategy.use_get
        
        sess = create_session()
        s1 = Sub()
        r1 = Related(sub=s1)
        sess.add(r1)
        sess.flush()
        sess.expunge_all()

        r1 = sess.query(Related).first()
        s1 = sess.query(Sub).first()
        def go():
            assert r1.sub
        self.assert_sql_count(testing.db, go, 0)
        

class GetTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global foo, bar, blub
        foo = Table('foo', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('type', String(30)),
            Column('data', String(20)))

        bar = Table('bar', metadata,
            Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
            Column('data', String(20)))

        blub = Table('blub', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('foo_id', Integer, ForeignKey('foo.id')),
            Column('bar_id', Integer, ForeignKey('bar.id')),
            Column('data', String(20)))
    
    @classmethod
    def setup_classes(cls):
        class Foo(_base.BasicEntity):
            pass

        class Bar(Foo):
            pass

        class Blub(Bar):
            pass

    def test_get_polymorphic(self):
        self._do_get_test(True)
    
    def test_get_nonpolymorphic(self):
        self._do_get_test(False)

    @testing.resolve_artifact_names
    def _do_get_test(self, polymorphic):
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
        sess.add(f)
        sess.add(b)
        sess.add(bl)
        sess.flush()

        if polymorphic:
            def go():
                assert sess.query(Foo).get(f.id) is f
                assert sess.query(Foo).get(b.id) is b
                assert sess.query(Foo).get(bl.id) is bl
                assert sess.query(Bar).get(b.id) is b
                assert sess.query(Bar).get(bl.id) is bl
                assert sess.query(Blub).get(bl.id) is bl

                # test class mismatches - item is present
                # in the identity map but we requested a subclass
                assert sess.query(Blub).get(f.id) is None
                assert sess.query(Blub).get(b.id) is None
                assert sess.query(Bar).get(f.id) is None
                
            self.assert_sql_count(testing.db, go, 0)
        else:
            # this is testing the 'wrong' behavior of using get()
            # polymorphically with mappers that are not configured to be
            # polymorphic.  the important part being that get() always
            # returns an instance of the query's type.
            def go():
                assert sess.query(Foo).get(f.id) is f

                bb = sess.query(Foo).get(b.id)
                assert isinstance(b, Foo) and bb.id==b.id

                bll = sess.query(Foo).get(bl.id)
                assert isinstance(bll, Foo) and bll.id==bl.id

                assert sess.query(Bar).get(b.id) is b

                bll = sess.query(Bar).get(bl.id)
                assert isinstance(bll, Bar) and bll.id == bl.id

                assert sess.query(Blub).get(bl.id) is bl

            self.assert_sql_count(testing.db, go, 3)


class EagerLazyTest(_base.MappedTest):
    """tests eager load/lazy load of child items off inheritance mappers, tests that
    LazyLoader constructs the right query condition."""
    
    @classmethod
    def define_tables(cls, metadata):
        global foo, bar, bar_foo
        foo = Table('foo', metadata,
                    Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                    Column('data', String(30)))
        bar = Table('bar', metadata,
                    Column('id', Integer, ForeignKey('foo.id'), primary_key=True),
                    Column('data', String(30)))

        bar_foo = Table('bar_foo', metadata,
                        Column('bar_id', Integer, ForeignKey('bar.id')),
                        Column('foo_id', Integer, ForeignKey('foo.id'))
        )

    @testing.fails_on('maxdb', 'FIXME: unknown')
    def test_basic(self):
        class Foo(object): pass
        class Bar(Foo): pass

        foos = mapper(Foo, foo)
        bars = mapper(Bar, bar, inherits=foos)
        bars.add_property('lazy', relationship(foos, bar_foo, lazy='select'))
        bars.add_property('eager', relationship(foos, bar_foo, lazy='joined'))

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

class EagerTargetingTest(_base.MappedTest):
    """test a scenario where joined table inheritance might be 
    confused as an eagerly loaded joined table."""
    
    @classmethod
    def define_tables(cls, metadata):
        Table('a_table', metadata,
           Column('id', Integer, primary_key=True),
           Column('name', String(50)),
           Column('type', String(30), nullable=False),
           Column('parent_id', Integer, ForeignKey('a_table.id'))
        )

        Table('b_table', metadata,
           Column('id', Integer, ForeignKey('a_table.id'), primary_key=True),
           Column('b_data', String(50)),
        )
    
    @testing.resolve_artifact_names
    def test_adapt_stringency(self):
        class A(_base.ComparableEntity):
            pass
        class B(A):
            pass
        
        mapper(A, a_table, polymorphic_on=a_table.c.type, polymorphic_identity='A', 
                properties={
                    'children': relationship(A, order_by=a_table.c.name)
            })

        mapper(B, b_table, inherits=A, polymorphic_identity='B', properties={
                'b_derived':column_property(b_table.c.b_data + "DATA")
                })
        
        sess=create_session()

        b1=B(id=1, name='b1',b_data='i')
        sess.add(b1)
        sess.flush()

        b2=B(id=2, name='b2', b_data='l', parent_id=1)
        sess.add(b2)
        sess.flush()

        bid=b1.id

        sess.expunge_all()
        node = sess.query(B).filter(B.id==bid).all()[0]
        eq_(node, B(id=1, name='b1',b_data='i'))
        eq_(node.children[0], B(id=2, name='b2',b_data='l'))
        
        sess.expunge_all()
        node = sess.query(B).options(joinedload(B.children)).filter(B.id==bid).all()[0]
        eq_(node, B(id=1, name='b1',b_data='i'))
        eq_(node.children[0], B(id=2, name='b2',b_data='l'))
        
class FlushTest(_base.MappedTest):
    """test dependency sorting among inheriting mappers"""
    
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('email', String(128)),
            Column('password', String(16)),
        )

        Table('roles', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('description', String(32))
        )

        Table('user_roles', metadata,
            Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
            Column('role_id', Integer, ForeignKey('roles.id'), primary_key=True)
        )

        Table('admins', metadata,
            Column('admin_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('user_id', Integer, ForeignKey('users.id'))
        )

    @testing.resolve_artifact_names
    def test_one(self):
        class User(object):pass
        class Role(object):pass
        class Admin(User):pass
        role_mapper = mapper(Role, roles)
        user_mapper = mapper(User, users, properties = {
                'roles' : relationship(Role, secondary=user_roles, lazy='joined')
            }
        )
        admin_mapper = mapper(Admin, admins, inherits=user_mapper)
        sess = create_session()
        adminrole = Role()
        sess.add(adminrole)
        sess.flush()

        # create an Admin, and append a Role.  the dependency processors
        # corresponding to the "roles" attribute for the Admin mapper and the User mapper
        # have to ensure that two dependency processors dont fire off and insert the
        # many to many row twice.
        a = Admin()
        a.roles.append(adminrole)
        a.password = 'admin'
        sess.add(a)
        sess.flush()

        assert user_roles.count().scalar() == 1

    @testing.resolve_artifact_names
    def test_two(self):
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
                'roles' : relationship(Role, secondary=user_roles, lazy='joined')
            }
        )

        admin_mapper = mapper(Admin, admins, inherits=user_mapper)

        # create roles
        adminrole = Role('admin')

        sess = create_session()
        sess.add(adminrole)
        sess.flush()

        # create admin user
        a = Admin(email='tim', password='admin')
        a.roles.append(adminrole)
        sess.add(a)
        sess.flush()

        a.password = 'sadmin'
        sess.flush()
        assert user_roles.count().scalar() == 1

class VersioningTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('base', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('version_id', Integer, nullable=False),
            Column('value', String(40)),
            Column('discriminator', Integer, nullable=False)
        )
        Table('subtable', metadata,
            Column('id', None, ForeignKey('base.id'), primary_key=True),
            Column('subdata', String(50))
            )
        Table('stuff', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('parent', Integer, ForeignKey('base.id'))
            )

    @testing.emits_warning(r".*updated rowcount")
    @engines.close_open_connections
    @testing.resolve_artifact_names
    def test_save_update(self):
        class Base(_fixtures.Base):
            pass
        class Sub(Base):
            pass
        class Stuff(Base):
            pass
        mapper(Stuff, stuff)
        mapper(Base, base, 
                    polymorphic_on=base.c.discriminator, 
                    version_id_col=base.c.version_id, 
                    polymorphic_identity=1, properties={
            'stuff':relationship(Stuff)
        })
        mapper(Sub, subtable, inherits=Base, polymorphic_identity=2)

        sess = create_session()

        b1 = Base(value='b1')
        s1 = Sub(value='sub1', subdata='some subdata')
        sess.add(b1)
        sess.add(s1)

        sess.flush()

        sess2 = create_session()
        s2 = sess2.query(Base).get(s1.id)
        s2.subdata = 'sess2 subdata'

        s1.subdata = 'sess1 subdata'

        sess.flush()

        assert_raises(orm_exc.ConcurrentModificationError,
                        sess2.query(Base).with_lockmode('read').get, 
                        s1.id)

        if not testing.db.dialect.supports_sane_rowcount:
            sess2.flush()
        else:
            assert_raises(orm_exc.ConcurrentModificationError, sess2.flush)

        sess2.refresh(s2)
        if testing.db.dialect.supports_sane_rowcount:
            assert s2.subdata == 'sess1 subdata'
        s2.subdata = 'sess2 subdata'
        sess2.flush()

    @testing.emits_warning(r".*updated rowcount")
    @testing.resolve_artifact_names
    def test_delete(self):
        class Base(_fixtures.Base):
            pass
        class Sub(Base):
            pass

        mapper(Base, base, 
                    polymorphic_on=base.c.discriminator, 
                    version_id_col=base.c.version_id, polymorphic_identity=1)
        mapper(Sub, subtable, inherits=Base, polymorphic_identity=2)

        sess = create_session()

        b1 = Base(value='b1')
        s1 = Sub(value='sub1', subdata='some subdata')
        s2 = Sub(value='sub2', subdata='some other subdata')
        sess.add(b1)
        sess.add(s1)
        sess.add(s2)

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
            assert not testing.db.dialect.supports_sane_rowcount
        except orm_exc.ConcurrentModificationError, e:
            assert True

class DistinctPKTest(_base.MappedTest):
    """test the construction of mapper.primary_key when an inheriting relationship
    joins on a column other than primary key column."""
    
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        global person_table, employee_table, Person, Employee

        person_table = Table("persons", metadata,
                Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
                Column("name", String(80)),
                )

        employee_table = Table("employees", metadata,
                Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
                Column("salary", Integer),
                Column("person_id", Integer, ForeignKey("persons.id")),
                )

        class Person(object):
            def __init__(self, name):
                self.name = name

        class Employee(Person): pass

    @classmethod
    def insert_data(cls):
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
        mapper(Employee, employee_table, inherits=person_mapper,
                        properties={'pid':person_table.c.id, 
                                    'eid':employee_table.c.id})
        self._do_test(True)

    def test_explicit_composite_pk(self):
        person_mapper = mapper(Person, person_table)
        mapper(Employee, employee_table, 
                    inherits=person_mapper, 
                    primary_key=[person_table.c.id, employee_table.c.id])
        assert_raises_message(sa_exc.SAWarning, 
                                    r"On mapper Mapper\|Employee\|employees, "
                                    "primary key column 'employees.id' is being "
                                    "combined with distinct primary key column 'persons.id' "
                                    "in attribute 'id'.  Use explicit properties to give "
                                    "each column its own mapped attribute name.",
            self._do_test, True
        )

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

class SyncCompileTest(_base.MappedTest):
    """test that syncrules compile properly on custom inherit conds"""
    
    @classmethod
    def define_tables(cls, metadata):
        global _a_table, _b_table, _c_table

        _a_table = Table('a', metadata,
           Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
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
            for j2 in (None, _b_table.c.a_id==_c_table.c.b_a_id,
                                    _c_table.c.b_a_id==_b_table.c.a_id):
                self._do_test(j1, j2)
                for t in reversed(_a_table.metadata.sorted_tables):
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
        session.add(a)

        b = B(data1='b1', data2='b2')
        session.add(b)

        c = C(data1='c1', data2='c2', data3='c3')
        session.add(c)

        session.flush()
        session.expunge_all()

        assert len(session.query(A).all()) == 3
        assert len(session.query(B).all()) == 2
        assert len(session.query(C).all()) == 1

class OverrideColKeyTest(_base.MappedTest):
    """test overriding of column attributes."""
    
    @classmethod
    def define_tables(cls, metadata):
        global base, subtable
        
        base = Table('base', metadata, 
            Column('base_id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(255)),
            Column('sqlite_fixer', String(10))
            )
            
        subtable = Table('subtable', metadata,
            Column('base_id', Integer, ForeignKey('base.base_id'), primary_key=True),
            Column('subdata', String(255))
        )

    def test_plain(self):
        # control case
        class Base(object):
            pass
        class Sub(Base):
            pass

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)
        
        # Sub gets a "base_id" property using the "base_id"
        # column of both tables.
        eq_(
            class_mapper(Sub).get_property('base_id').columns,
            [base.c.base_id, subtable.c.base_id]
        )

    def test_override_explicit(self):
        # this pattern is what you see when using declarative
        # in particular, here we do a "manual" version of
        # what we'd like the mapper to do.
        
        class Base(object):
            pass
        class Sub(Base):
            pass
        
        mapper(Base, base, properties={
            'id':base.c.base_id
        })
        mapper(Sub, subtable, inherits=Base, properties={
            # this is the manual way to do it, is not really
            # possible in declarative
            'id':[base.c.base_id, subtable.c.base_id]
        })

        eq_(
            class_mapper(Sub).get_property('id').columns,
            [base.c.base_id, subtable.c.base_id]
        )
 
        s1 = Sub()
        s1.id = 10
        sess = create_session()
        sess.add(s1)
        sess.flush()
        assert sess.query(Sub).get(10) is s1
    
    def test_override_onlyinparent(self):
        class Base(object):
            pass
        class Sub(Base):
            pass

        mapper(Base, base, properties={
            'id':base.c.base_id
        })
        mapper(Sub, subtable, inherits=Base)
        
        eq_(
            class_mapper(Sub).get_property('id').columns,
            [base.c.base_id]
        )

        eq_(
            class_mapper(Sub).get_property('base_id').columns,
            [subtable.c.base_id]
        )
        
        s1 = Sub()
        s1.id = 10
        
        s2 = Sub()
        s2.base_id = 15
        
        sess = create_session()
        sess.add_all([s1, s2])
        sess.flush()
        
        # s1 gets '10'
        assert sess.query(Sub).get(10) is s1
        
        # s2 gets a new id, base_id is overwritten by the ultimate
        # PK col
        assert s2.id == s2.base_id != 15
        
    def test_override_implicit(self):
        # this is how the pattern looks intuitively when 
        # using declarative.
        # fixed as part of [ticket:1111]
        
        class Base(object):
            pass
        class Sub(Base):
            pass

        mapper(Base, base, properties={
            'id':base.c.base_id
        })
        mapper(Sub, subtable, inherits=Base, properties={
            'id':subtable.c.base_id
        })
        
        # Sub mapper compilation needs to detect that "base.c.base_id"
        # is renamed in the inherited mapper as "id", even though
        # it has its own "id" property.  Sub's "id" property 
        # gets joined normally with the extra column.
        
        eq_(
            set(class_mapper(Sub).get_property('id').columns),
            set([base.c.base_id, subtable.c.base_id])
        )
        
        s1 = Sub()
        s1.id = 10
        sess = create_session()
        sess.add(s1)
        sess.flush()
        assert sess.query(Sub).get(10) is s1

    def test_plain_descriptor(self):
        """test that descriptors prevent inheritance from propigating properties to subclasses."""
        
        class Base(object):
            pass
        class Sub(Base):
            @property
            def data(self):
                return "im the data"

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)
        
        s1 = Sub()
        sess = create_session()
        sess.add(s1)
        sess.flush()
        assert sess.query(Sub).one().data == "im the data"

    def test_custom_descriptor(self):
        """test that descriptors prevent inheritance from propigating properties to subclasses."""

        class MyDesc(object):
            def __get__(self, instance, owner):
                if instance is None:
                    return self
                return "im the data"
            
        class Base(object):
            pass
        class Sub(Base):
            data = MyDesc()

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)

        s1 = Sub()
        sess = create_session()
        sess.add(s1)
        sess.flush()
        assert sess.query(Sub).one().data == "im the data"
    
    def test_sub_columns_over_base_descriptors(self):
        class Base(object):
            @property
            def subdata(self):
                return "this is base"

        class Sub(Base):
            pass

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)
        
        sess = create_session()
        b1 = Base()
        assert b1.subdata == "this is base"
        s1 = Sub()
        s1.subdata = "this is sub"
        assert s1.subdata == "this is sub"

        sess.add_all([s1, b1])
        sess.flush()
        sess.expunge_all()
        
        assert sess.query(Base).get(b1.base_id).subdata == "this is base"
        assert sess.query(Sub).get(s1.base_id).subdata == "this is sub"

    def test_base_descriptors_over_base_cols(self):
        class Base(object):
            @property
            def data(self):
                return "this is base"

        class Sub(Base):
            pass

        mapper(Base, base)
        mapper(Sub, subtable, inherits=Base)

        sess = create_session()
        b1 = Base()
        assert b1.data == "this is base"
        s1 = Sub()
        assert s1.data == "this is base"

        sess.add_all([s1, b1])
        sess.flush()
        sess.expunge_all()

        assert sess.query(Base).get(b1.base_id).data == "this is base"
        assert sess.query(Sub).get(s1.base_id).data == "this is base"

class OptimizedLoadTest(_base.MappedTest):
    """tests for the "optimized load" routine."""
    
    @classmethod
    def define_tables(cls, metadata):
        global base, sub, with_comp
        base = Table('base', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(50)),
            Column('type', String(50))
        )
        sub = Table('sub', metadata, 
            Column('id', Integer, ForeignKey('base.id'), primary_key=True),
            Column('sub', String(50))
        )
        with_comp = Table('with_comp', metadata,
            Column('id', Integer, ForeignKey('base.id'), primary_key=True),
            Column('a', String(10)),
            Column('b', String(10))
        )
    
    def test_optimized_passes(self):
        """"test that the 'optimized load' routine doesn't crash when 
        a column in the join condition is not available."""
        
        class Base(_base.BasicEntity):
            pass
        class Sub(Base):
            pass
            
        mapper(Base, base, polymorphic_on=base.c.type, polymorphic_identity='base')
        
        # redefine Sub's "id" to favor the "id" col in the subtable.
        # "id" is also part of the primary join condition
        mapper(Sub, sub, inherits=Base, polymorphic_identity='sub', properties={'id':sub.c.id})
        sess = sessionmaker()()
        s1 = Sub(data='s1data', sub='s1sub')
        sess.add(s1)
        sess.commit()
        sess.expunge_all()
        
        # load s1 via Base.  s1.id won't populate since it's relative to 
        # the "sub" table.  The optimized load kicks in and tries to 
        # generate on the primary join, but cannot since "id" is itself unloaded.
        # the optimized load needs to return "None" so regular full-row loading proceeds
        s1 = sess.query(Base).first()
        assert s1.sub == 's1sub'

    def test_column_expression(self):
        class Base(_base.BasicEntity):
            pass
        class Sub(Base):
            pass
        mapper(Base, base, polymorphic_on=base.c.type, polymorphic_identity='base')
        mapper(Sub, sub, inherits=Base, polymorphic_identity='sub', properties={
            'concat':column_property(sub.c.sub + "|" + sub.c.sub)
        })
        sess = sessionmaker()()
        s1 = Sub(data='s1data', sub='s1sub')
        sess.add(s1)
        sess.commit()
        sess.expunge_all()
        s1 = sess.query(Base).first()
        assert s1.concat == 's1sub|s1sub'

    def test_column_expression_joined(self):
        class Base(_base.ComparableEntity):
            pass
        class Sub(Base):
            pass
        mapper(Base, base, polymorphic_on=base.c.type, polymorphic_identity='base')
        mapper(Sub, sub, inherits=Base, polymorphic_identity='sub', properties={
            'concat':column_property(base.c.data + "|" + sub.c.sub)
        })
        sess = sessionmaker()()
        s1 = Sub(data='s1data', sub='s1sub')
        s2 = Sub(data='s2data', sub='s2sub')
        s3 = Sub(data='s3data', sub='s3sub')
        sess.add_all([s1, s2, s3])
        sess.commit()
        sess.expunge_all()
        # query a bunch of rows to ensure there's no cartesian
        # product against "base" occurring, it is in fact
        # detecting that "base" needs to be in the join 
        # criterion
        eq_(
            sess.query(Base).order_by(Base.id).all(),
            [
                Sub(data='s1data', sub='s1sub', concat='s1data|s1sub'),
                Sub(data='s2data', sub='s2sub', concat='s2data|s2sub'),
                Sub(data='s3data', sub='s3sub', concat='s3data|s3sub')
            ]
        )

    def test_composite_column_joined(self):
        class Base(_base.ComparableEntity):
            pass
        class WithComp(Base):
            pass
        class Comp(object):
            def __init__(self, a, b):
                self.a = a
                self.b = b
            def __composite_values__(self):
                return self.a, self.b
            def __eq__(self, other):
                return (self.a == other.a) and (self.b == other.b)
        mapper(Base, base, polymorphic_on=base.c.type, polymorphic_identity='base')
        mapper(WithComp, with_comp, inherits=Base, polymorphic_identity='wc', properties={
            'comp': composite(Comp, with_comp.c.a, with_comp.c.b)
        })
        sess = sessionmaker()()
        s1 = WithComp(data='s1data', comp=Comp('ham', 'cheese'))
        s2 = WithComp(data='s2data', comp=Comp('bacon', 'eggs'))
        sess.add_all([s1, s2])
        sess.commit()
        sess.expunge_all()
        s1test, s2test = sess.query(Base).order_by(Base.id).all()
        assert s1test.comp
        assert s2test.comp
        eq_(s1test.comp, Comp('ham', 'cheese'))
        eq_(s2test.comp, Comp('bacon', 'eggs'))
        
        
class PKDiscriminatorTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        parents = Table('parents', metadata,
                           Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                           Column('name', String(60)))
                           
        children = Table('children', metadata,
                        Column('id', Integer, ForeignKey('parents.id'), primary_key=True),
                        Column('type', Integer,primary_key=True),
                        Column('name', String(60)))

    @testing.resolve_artifact_names
    def test_pk_as_discriminator(self):
        class Parent(object):
                def __init__(self, name=None):
                    self.name = name

        class Child(object):
            def __init__(self, name=None):
                self.name = name

        class A(Child):
            pass
            
        mapper(Parent, parents, properties={
            'children': relationship(Child, backref='parent'),
        })
        mapper(Child, children, polymorphic_on=children.c.type,
            polymorphic_identity=1)
            
        mapper(A, inherits=Child, polymorphic_identity=2)

        s = create_session()
        p = Parent('p1')
        a = A('a1')
        p.children.append(a)
        s.add(p)
        s.flush()

        assert a.id
        assert a.type == 2
        
        p.name='p1new'
        a.name='a1new'
        s.flush()
        
        s.expire_all()
        assert a.name=='a1new'
        assert p.name=='p1new'
        
        
class DeleteOrphanTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        global single, parent
        single = Table('single', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('type', String(50), nullable=False),
            Column('data', String(50)),
            Column('parent_id', Integer, ForeignKey('parent.id'), nullable=False),
            )
            
        parent = Table('parent', metadata,
                Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                Column('data', String(50))
            )
    
    def test_orphan_message(self):
        class Base(_fixtures.Base):
            pass
        
        class SubClass(Base):
            pass
        
        class Parent(_fixtures.Base):
            pass
        
        mapper(Base, single, polymorphic_on=single.c.type, polymorphic_identity='base')
        mapper(SubClass, inherits=Base, polymorphic_identity='sub')
        mapper(Parent, parent, properties={
            'related':relationship(Base, cascade="all, delete-orphan")
        })
        
        sess = create_session()
        s1 = SubClass(data='s1')
        sess.add(s1)
        assert_raises_message(orm_exc.FlushError, 
            r"is not attached to any parent 'Parent' instance via "
            "that classes' 'related' attribute", sess.flush)
        
    
