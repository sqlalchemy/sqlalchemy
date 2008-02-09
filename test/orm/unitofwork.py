# coding: utf-8

"""Tests unitofwork operations."""

import testenv; testenv.configure_for_tests()
import pickleable
from sqlalchemy import *
from sqlalchemy import exceptions, sql
from sqlalchemy.orm import *
from testlib import *
from testlib.tables import *
from testlib import engines, tables, fixtures


# TODO: convert suite to not use Session.mapper, use fixtures.Base
# with explicit session.save()
Session = scoped_session(sessionmaker(autoflush=True, transactional=True))
orm_mapper = mapper
mapper = Session.mapper

class UnitOfWorkTest(object):
    pass

class HistoryTest(ORMTest):
    metadata = tables.metadata
    def define_tables(self, metadata):
        pass

    def test_backref(self):
        s = Session()
        class User(object):pass
        class Address(object):pass
        am = mapper(Address, addresses)
        m = mapper(User, users, properties = dict(
            addresses = relation(am, backref='user', lazy=False))
        )

        u = User(_sa_session=s)
        a = Address(_sa_session=s)
        a.user = u

        self.assert_(u.addresses == [a])
        s.commit()

        s.close()
        u = s.query(m).all()[0]
        print u.addresses[0].user

class VersioningTest(ORMTest):
    def define_tables(self, metadata):
        global version_table
        version_table = Table('version_test', metadata,
        Column('id', Integer, Sequence('version_test_seq', optional=True),
               primary_key=True ),
        Column('version_id', Integer, nullable=False),
        Column('value', String(40), nullable=False)
        )

    @engines.close_open_connections
    def test_basic(self):
        s = Session(scope=None)
        class Foo(object):pass
        mapper(Foo, version_table, version_id_col=version_table.c.version_id)
        f1 = Foo(value='f1', _sa_session=s)
        f2 = Foo(value='f2', _sa_session=s)
        s.commit()

        f1.value='f1rev2'
        s.commit()
        s2 = Session()
        f1_s = s2.query(Foo).get(f1.id)
        f1_s.value='f1rev3'
        s2.commit()

        f1.value='f1rev3mine'
        success = False
        try:
            # a concurrent session has modified this, should throw
            # an exception
            s.commit()
        except exceptions.ConcurrentModificationError, e:
            #print e
            success = True

        # Only dialects with a sane rowcount can detect the ConcurrentModificationError
        if testing.db.dialect.supports_sane_rowcount:
            assert success

        s.close()
        f1 = s.query(Foo).get(f1.id)
        f2 = s.query(Foo).get(f2.id)

        f1_s.value='f1rev4'
        s2.commit()

        s.delete(f1)
        s.delete(f2)
        success = False
        try:
            s.commit()
        except exceptions.ConcurrentModificationError, e:
            #print e
            success = True
        if testing.db.dialect.supports_sane_multi_rowcount:
            assert success

    @engines.close_open_connections
    def test_versioncheck(self):
        """test that query.with_lockmode performs a 'version check' on an already loaded instance"""
        s1 = Session(scope=None)
        class Foo(object):pass
        mapper(Foo, version_table, version_id_col=version_table.c.version_id)
        f1s1 =Foo(value='f1', _sa_session=s1)
        s1.commit()
        s2 = Session()
        f1s2 = s2.query(Foo).get(f1s1.id)
        f1s2.value='f1 new value'
        s2.commit()
        try:
            # load, version is wrong
            s1.query(Foo).with_lockmode('read').get(f1s1.id)
            assert False
        except exceptions.ConcurrentModificationError, e:
            assert True
        # reload it
        s1.query(Foo).load(f1s1.id)
        # now assert version OK
        s1.query(Foo).with_lockmode('read').get(f1s1.id)

        # assert brand new load is OK too
        s1.close()
        s1.query(Foo).with_lockmode('read').get(f1s1.id)

    @engines.close_open_connections
    def test_noversioncheck(self):
        """test that query.with_lockmode works OK when the mapper has no version id col"""
        s1 = Session()
        class Foo(object):pass
        mapper(Foo, version_table)
        f1s1 =Foo(value='f1', _sa_session=s1)
        f1s1.version_id=0
        s1.commit()
        s2 = Session()
        f1s2 = s2.query(Foo).with_lockmode('read').get(f1s1.id)
        assert f1s2.id == f1s1.id
        assert f1s2.value == f1s1.value

class UnicodeTest(ORMTest):
    def define_tables(self, metadata):
        global uni_table, uni_table2
        uni_table = Table('uni_test', metadata,
            Column('id',  Integer, Sequence("uni_test_id_seq", optional=True), primary_key=True),
            Column('txt', Unicode(50), unique=True))
        uni_table2 = Table('uni2', metadata,
            Column('id',  Integer, Sequence("uni2_test_id_seq", optional=True), primary_key=True),
            Column('txt', Unicode(50), ForeignKey(uni_table.c.txt)))

    def test_basic(self):
        class Test(object):
            def __init__(self, id, txt):
                self.id = id
                self.txt = txt
        mapper(Test, uni_table)

        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(id=1, txt = txt)
        self.assert_(t1.txt == txt)
        Session.commit()
        self.assert_(t1.txt == txt)

    def test_relation(self):
        class Test(object):
            def __init__(self, txt):
                self.txt = txt
        class Test2(object):pass

        mapper(Test, uni_table, properties={
            't2s':relation(Test2)
        })
        mapper(Test2, uni_table2)

        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(txt=txt)
        t1.t2s.append(Test2())
        t1.t2s.append(Test2())
        Session.commit()
        Session.close()
        t1 = Session.query(Test).filter_by(id=t1.id).one()
        assert len(t1.t2s) == 2

class UnicodeSchemaTest(ORMTest):
    __unsupported_on__ = ('oracle', 'mssql', 'firebird', 'sybase',
                          'access', 'maxdb')
    __excluded_on__ = (('mysql', '<', (4, 1, 1)),)

    metadata = MetaData(engines.utf8_engine())

    def define_tables(self, metadata):
        global t1, t2

        t1 = Table('unitable1', metadata,
            Column(u'méil', Integer, primary_key=True, key='a'),
            Column(u'\u6e2c\u8a66', Integer, key='b'),
            Column('type',  String(20)),
            test_needs_fk=True,
            )
        t2 = Table(u'Unitéble2', metadata,
            Column(u'méil', Integer, primary_key=True, key="cc"),
            Column(u'\u6e2c\u8a66', Integer, ForeignKey(u'unitable1.a'), key="d"),
           Column(u'\u6e2c\u8a66_2', Integer, key="e"),
                  test_needs_fk=True,
            )

    def test_mapping(self):
        class A(fixtures.Base):pass
        class B(fixtures.Base):pass

        mapper(A, t1, properties={
            't2s':relation(B),
        })
        mapper(B, t2)
        a1 = A()
        b1 = B()
        a1.t2s.append(b1)
        Session.flush()
        Session.clear()
        new_a1 = Session.query(A).filter(t1.c.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        Session.clear()

        new_a1 = Session.query(A).options(eagerload('t2s')).filter(t1.c.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        Session.clear()

        new_a1 = Session.query(A).filter(A.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        Session.clear()

    def test_inheritance_mapping(self):
        class A(fixtures.Base):pass
        class B(A):pass
        mapper(A, t1, polymorphic_on=t1.c.type, polymorphic_identity='a')
        mapper(B, t2, inherits=A, polymorphic_identity='b')
        a1 = A(b=5)
        b1 = B(e=7)

        Session.flush()
        Session.clear()
        # TODO: somehow, not assigning to "l" first
        # breaks the comparison ?????
        l = Session.query(A).all()
        assert [A(b=5), B(e=7)] == l

class MutableTypesTest(ORMTest):
    def define_tables(self, metadata):
        global table
        table = Table('mutabletest', metadata,
            Column('id', Integer, Sequence('mutableidseq', optional=True), primary_key=True),
            Column('data', PickleType),
            Column('val', Unicode(30)))

    def test_basic(self):
        """test that types marked as MutableType get changes detected on them"""
        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.data = pickleable.Bar(4,5)
        Session.commit()
        Session.close()
        f2 = Session.query(Foo).filter_by(id=f1.id).one()
        assert f2.data == f1.data
        f2.data.y = 19
        assert f2 in Session.dirty
        Session.commit()
        Session.close()
        f3 = Session.query(Foo).filter_by(id=f1.id).one()
        print f2.data, f3.data
        assert f3.data != f1.data
        assert f3.data == pickleable.Bar(4, 19)

    def test_mutablechanges(self):
        """test that mutable changes are detected or not detected correctly"""
        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.data = pickleable.Bar(4,5)
        f1.val = unicode('hi')
        Session.commit()
        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 0)
        f1.val = unicode('someothervalue')
        self.assert_sql(testing.db, lambda: Session.commit(), [
            (
                "UPDATE mutabletest SET val=:val WHERE mutabletest.id = :mutabletest_id",
                {'mutabletest_id': f1.id, 'val': u'someothervalue'}
            ),
        ])
        f1.val = unicode('hi')
        f1.data.x = 9
        self.assert_sql(testing.db, lambda: Session.commit(), [
            (
                "UPDATE mutabletest SET data=:data, val=:val WHERE mutabletest.id = :mutabletest_id",
                {'mutabletest_id': f1.id, 'val': u'hi', 'data':f1.data}
            ),
        ])

    def test_nocomparison(self):
        """test that types marked as MutableType get changes detected on them when the type has no __eq__ method"""
        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.data = pickleable.BarWithoutCompare(4,5)
        Session.commit()

        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 0)

        Session.close()

        f2 = Session.query(Foo).filter_by(id=f1.id).one()

        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 0)

        f2.data.y = 19
        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 1)

        Session.close()
        f3 = Session.query(Foo).filter_by(id=f1.id).one()
        print f2.data, f3.data
        assert (f3.data.x, f3.data.y) == (4,19)

        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 0)

    def test_unicode(self):
        """test that two equivalent unicode values dont get flagged as changed.

        apparently two equal unicode objects dont compare via "is" in all cases, so this
        tests the compare_values() call on types.String and its usage via types.Unicode."""
        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.val = u'hi'
        Session.commit()
        Session.close()
        f1 = Session.get(Foo, f1.id)
        f1.val = u'hi'
        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 0)

class MutableTypesTest2(ORMTest):
    def define_tables(self, metadata):
        global table
        import operator
        table = Table('mutabletest', metadata,
            Column('id', Integer, Sequence('mutableidseq', optional=True), primary_key=True),
            Column('data', PickleType(comparator=operator.eq)),
            )

    def test_dicts(self):
        """dictionaries dont pickle the same way twice, sigh."""

        class Foo(object):pass
        mapper(Foo, table)
        f1 = Foo()
        f1.data = [{'personne': {'nom': u'Smith', 'pers_id': 1, 'prenom': u'john', 'civilite': u'Mr', \
                    'int_3': False, 'int_2': False, 'int_1': u'23', 'VenSoir': True, 'str_1': u'Test', \
                    'SamMidi': False, 'str_2': u'chien', 'DimMidi': False, 'SamSoir': True, 'SamAcc': False}}]

        Session.commit()
        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 0)

        f1.data = [{'personne': {'nom': u'Smith', 'pers_id': 1, 'prenom': u'john', 'civilite': u'Mr', \
                    'int_3': False, 'int_2': False, 'int_1': u'23', 'VenSoir': True, 'str_1': u'Test', \
                    'SamMidi': False, 'str_2': u'chien', 'DimMidi': False, 'SamSoir': True, 'SamAcc': False}}]

        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 0)

        f1.data[0]['personne']['VenSoir']= False
        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 1)

        Session.clear()
        f = Session.query(Foo).get(f1.id)
        assert f.data == [{'personne': {'nom': u'Smith', 'pers_id': 1, 'prenom': u'john', 'civilite': u'Mr', \
                    'int_3': False, 'int_2': False, 'int_1': u'23', 'VenSoir': False, 'str_1': u'Test', \
                    'SamMidi': False, 'str_2': u'chien', 'DimMidi': False, 'SamSoir': True, 'SamAcc': False}}]

class PKTest(ORMTest):
    def define_tables(self, metadata):
        global table, table2, table3

        table = Table(
            'multipk', metadata,
            Column('multi_id', Integer, Sequence("multi_id_seq", optional=True), primary_key=True),
            Column('multi_rev', Integer, primary_key=True),
            Column('name', String(50), nullable=False),
            Column('value', String(100))
        )

        table2 = Table('multipk2', metadata,
            Column('pk_col_1', String(30), primary_key=True),
            Column('pk_col_2', String(30), primary_key=True),
            Column('data', String(30), )
            )
        table3 = Table('multipk3', metadata,
            Column('pri_code', String(30), key='primary', primary_key=True),
            Column('sec_code', String(30), key='secondary', primary_key=True),
            Column('date_assigned', Date, key='assigned', primary_key=True),
            Column('data', String(30), )
            )

    # not supported on sqlite since sqlite's auto-pk generation only works with
    # single column primary keys
    @testing.fails_on('sqlite')
    def test_primarykey(self):
        class Entry(object):
            pass
        Entry.mapper = mapper(Entry, table)
        e = Entry()
        e.name = 'entry1'
        e.value = 'this is entry 1'
        e.multi_rev = 2
        Session.commit()
        Session.close()
        e2 = Query(Entry).get((e.multi_id, 2))
        self.assert_(e is not e2 and e._instance_key == e2._instance_key)

    # this one works with sqlite since we are manually setting up pk values
    def test_manualpk(self):
        class Entry(object):
            pass
        Entry.mapper = mapper(Entry, table2)
        e = Entry()
        e.pk_col_1 = 'pk1'
        e.pk_col_2 = 'pk1_related'
        e.data = 'im the data'
        Session.commit()

    def test_keypks(self):
        import datetime
        class Entity(object):
            pass
        Entity.mapper = mapper(Entity, table3)
        e = Entity()
        e.primary = 'pk1'
        e.secondary = 'pk2'
        e.assigned = datetime.date.today()
        e.data = 'some more data'
        Session.commit()

class ForeignPKTest(ORMTest):
    """tests mapper detection of the relationship direction when parent/child tables are joined on their
    primary keys"""

    def define_tables(self, metadata):
        global people, peoplesites

        people = Table("people", metadata,
           Column('person', String(10), primary_key=True),
           Column('firstname', String(10)),
           Column('lastname', String(10)),
        )

        peoplesites = Table("peoplesites", metadata,
            Column('person', String(10), ForeignKey("people.person"),
        primary_key=True),
            Column('site', String(10)),
        )

    def test_basic(self):
        class PersonSite(object):pass
        class Person(object):pass
        m1 = mapper(PersonSite, peoplesites)

        m2 = mapper(Person, people,
              properties = {
                      'sites' : relation(PersonSite),
              },
            )
        compile_mappers()
        assert list(m2.get_property('sites').foreign_keys) == [peoplesites.c.person]
        p = Person()
        p.person = 'im the key'
        p.firstname = 'asdf'
        ps = PersonSite()
        ps.site = 'asdf'
        p.sites.append(ps)
        Session.commit()
        assert people.count(people.c.person=='im the key').scalar() == peoplesites.count(peoplesites.c.person=='im the key').scalar() == 1

class ClauseAttributesTest(ORMTest):
    def define_tables(self, metadata):
        global users_table
        users_table = Table('users', metadata,
            Column('id', Integer, Sequence('users_id_seq', optional=True), primary_key=True),
            Column('name', String(30)),
            Column('counter', Integer, default=1))

    def test_update(self):
        class User(object):
            pass
        mapper(User, users_table)
        u = User(name='test')
        sess = Session()
        sess.save(u)
        sess.flush()
        assert u.counter == 1
        u.counter = User.counter + 1
        sess.flush()
        def go():
            assert u.counter == 2
        self.assert_sql_count(testing.db, go, 1)

    def test_multi_update(self):
        class User(object):
            pass
        mapper(User, users_table)
        u = User(name='test')
        sess = Session()
        sess.save(u)
        sess.flush()
        assert u.counter == 1
        u.name = 'test2'
        u.counter = User.counter + 1
        sess.flush()
        def go():
            assert u.name == 'test2'
            assert u.counter == 2
        self.assert_sql_count(testing.db, go, 1)

        sess.clear()
        u = sess.query(User).get(u.id)
        assert u.name == 'test2'
        assert u.counter == 2

    @testing.unsupported('mssql')
    def test_insert(self):
        class User(object):
            pass
        mapper(User, users_table)
        u = User(name='test', counter=select([5]))
        sess = Session()
        sess.save(u)
        sess.flush()
        assert u.counter == 5


class PassiveDeletesTest(ORMTest):
    def define_tables(self, metadata):
        global mytable,myothertable

        mytable = Table('mytable', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            test_needs_fk=True,
            )

        myothertable = Table('myothertable', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer),
            Column('data', String(30)),
            ForeignKeyConstraint(['parent_id'],['mytable.id'], ondelete="CASCADE"),
            test_needs_fk=True,
            )

    @testing.unsupported('sqlite')
    def test_basic(self):
        class MyClass(object):
            pass
        class MyOtherClass(object):
            pass

        mapper(MyOtherClass, myothertable)

        mapper(MyClass, mytable, properties={
            'children':relation(MyOtherClass, passive_deletes=True, cascade="all")
        })

        sess = Session
        mc = MyClass()
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        sess.save(mc)
        sess.commit()
        sess.close()
        assert myothertable.count().scalar() == 4
        mc = sess.query(MyClass).get(mc.id)
        sess.delete(mc)
        sess.commit()
        assert mytable.count().scalar() == 0
        assert myothertable.count().scalar() == 0

class ExtraPassiveDeletesTest(ORMTest):
    def define_tables(self, metadata):
        global mytable,myothertable

        mytable = Table('mytable', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30)),
            test_needs_fk=True,
            )

        myothertable = Table('myothertable', metadata,
            Column('id', Integer, primary_key=True),
            Column('parent_id', Integer),
            Column('data', String(30)),
            ForeignKeyConstraint(['parent_id'],['mytable.id']),  # no CASCADE, the same as ON DELETE RESTRICT
            test_needs_fk=True,
            )

    def test_assertions(self):
        class MyClass(object):
            pass
        class MyOtherClass(object):
            pass

        mapper(MyOtherClass, myothertable)

        try:
            mapper(MyClass, mytable, properties={
                'children':relation(MyOtherClass, passive_deletes='all', cascade="all")
            })
            assert False
        except exceptions.ArgumentError, e:
            assert str(e) == "Can't set passive_deletes='all' in conjunction with 'delete' or 'delete-orphan' cascade"

    @testing.unsupported('sqlite')
    def test_extra_passive(self):
        class MyClass(object):
            pass
        class MyOtherClass(object):
            pass

        mapper(MyOtherClass, myothertable)

        mapper(MyClass, mytable, properties={
            'children':relation(MyOtherClass, passive_deletes='all', cascade="save-update")
        })

        sess = Session
        mc = MyClass()
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        sess.save(mc)
        sess.commit()

        assert myothertable.count().scalar() == 4
        mc = sess.query(MyClass).get(mc.id)
        sess.delete(mc)
        try:
            sess.commit()
            assert False
        except exceptions.DBAPIError:
            assert True


class DefaultTest(ORMTest):
    """tests that when saving objects whose table contains DefaultGenerators, either python-side, preexec or database-side,
    the newly saved instances receive all the default values either through a post-fetch or getting the pre-exec'ed
    defaults back from the engine."""

    def define_tables(self, metadata):
        db = testing.db
        use_string_defaults = testing.against('postgres', 'oracle', 'sqlite') 
        global hohoval, althohoval

        if use_string_defaults:
            hohotype = String(30)
            hohoval = "im hoho"
            althohoval = "im different hoho"
        else:
            hohotype = Integer
            hohoval = 9
            althohoval = 15

        global default_table, secondary_table
        default_table = Table('default_test', metadata,
            Column('id', Integer, Sequence("dt_seq", optional=True), primary_key=True),
            Column('hoho', hohotype, PassiveDefault(str(hohoval))),
            Column('counter', Integer, default=func.length("1234567")),
            Column('foober', String(30), default="im foober", onupdate="im the update"),
        )
        
        secondary_table = Table('secondary_table', metadata, 
            Column('id', Integer, primary_key=True),
            Column('data', String(50))
            )
        
        if testing.against('postgres', 'oracle'):
            default_table.append_column(Column('secondary_id', Integer, Sequence('sec_id_seq'), unique=True))
            secondary_table.append_column(Column('fk_val', Integer, ForeignKey('default_test.secondary_id')))
        else:
            secondary_table.append_column(Column('hoho', hohotype, ForeignKey('default_test.hoho')))

    def test_insert(self):
        class Hoho(object):pass
        mapper(Hoho, default_table)

        h1 = Hoho(hoho=althohoval)
        h2 = Hoho(counter=12)
        h3 = Hoho(hoho=althohoval, counter=12)
        h4 = Hoho()
        h5 = Hoho(foober='im the new foober')
        Session.commit()

        self.assert_(h1.hoho==althohoval)
        self.assert_(h3.hoho==althohoval)

        def go():
            # test deferred load of attribues, one select per instance
            self.assert_(h2.hoho==h4.hoho==h5.hoho==hohoval)
        self.assert_sql_count(testing.db, go, 3)

        def go():
            self.assert_(h1.counter ==  h4.counter==h5.counter==7)
        self.assert_sql_count(testing.db, go, 1)

        def go():
            self.assert_(h3.counter == h2.counter == 12)
            self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
            self.assert_(h5.foober=='im the new foober')
        self.assert_sql_count(testing.db, go, 0)

        Session.close()

        l = Hoho.query.all()

        (h1, h2, h3, h4, h5) = l

        self.assert_(h1.hoho==althohoval)
        self.assert_(h3.hoho==althohoval)
        self.assert_(h2.hoho==h4.hoho==h5.hoho==hohoval)
        self.assert_(h3.counter == h2.counter == 12)
        self.assert_(h1.counter ==  h4.counter==h5.counter==7)
        self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
        self.assert_(h5.foober=='im the new foober')

    def test_eager_defaults(self):
        class Hoho(object):pass
        mapper(Hoho, default_table, eager_defaults=True)
        h1 = Hoho()
        Session.commit()

        def go():
            self.assert_(h1.hoho==hohoval)
        self.assert_sql_count(testing.db, go, 0)

    def test_insert_nopostfetch(self):
        # populates the PassiveDefaults explicitly so there is no "post-update"
        class Hoho(object):pass
        mapper(Hoho, default_table)

        h1 = Hoho(hoho="15", counter="15")

        Session.commit()
        def go():
            self.assert_(h1.hoho=="15")
            self.assert_(h1.counter=="15")
            self.assert_(h1.foober=="im foober")
        self.assert_sql_count(testing.db, go, 0)

    def test_update(self):
        class Hoho(object):pass
        mapper(Hoho, default_table)
        h1 = Hoho()
        Session.commit()
        self.assertEquals(h1.foober, 'im foober')
        h1.counter = 19
        Session.commit()
        self.assertEquals(h1.foober, 'im the update')
    
    def test_used_in_relation(self):
        """test that a server-side generated default can be used as the target of a foreign key"""
        
        class Hoho(fixtures.Base):
            pass
        class Secondary(fixtures.Base):
            pass
        mapper(Hoho, default_table, properties={
            'secondaries':relation(Secondary)
        }, save_on_init=False)
        
        mapper(Secondary, secondary_table, save_on_init=False)
        h1 = Hoho()
        s1 = Secondary(data='s1')
        h1.secondaries.append(s1)
        Session.save(h1)
        Session.commit()
        Session.clear()
        
        self.assertEquals(Session.query(Hoho).get(h1.id), Hoho(hoho=hohoval, secondaries=[Secondary(data='s1')]))
        
        h1 = Session.query(Hoho).get(h1.id)
        h1.secondaries.append(Secondary(data='s2'))
        Session.commit()
        Session.clear()

        self.assertEquals(Session.query(Hoho).get(h1.id), 
            Hoho(hoho=hohoval, secondaries=[Secondary(data='s1'), Secondary(data='s2')])
        )
        
            
class OneToManyTest(ORMTest):
    metadata = tables.metadata

    def define_tables(self, metadata):
        pass

    def test_onetomany_1(self):
        """test basic save of one to many."""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u = User()
        u.user_name = 'one2manytester'
        u.addresses = []
        a = Address()
        a.email_address = 'one2many@test.org'
        u.addresses.append(a)
        a2 = Address()
        a2.email_address = 'lala@test.org'
        u.addresses.append(a2)
        print repr(u.addresses)
        Session.commit()

        usertable = users.select(users.c.user_id.in_([u.user_id])).execute().fetchall()
        self.assertEqual(usertable[0].values(), [u.user_id, 'one2manytester'])
        addresstable = addresses.select(addresses.c.address_id.in_([a.address_id, a2.address_id]), order_by=[addresses.c.email_address]).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [a2.address_id, u.user_id, 'lala@test.org'])
        self.assertEqual(addresstable[1].values(), [a.address_id, u.user_id, 'one2many@test.org'])

        userid = u.user_id
        addressid = a2.address_id

        a2.email_address = 'somethingnew@foo.com'

        Session.commit()

        addresstable = addresses.select(addresses.c.address_id == addressid).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [addressid, userid, 'somethingnew@foo.com'])
        self.assert_(u.user_id == userid and a2.address_id == addressid)

    def test_onetomany_2(self):
        """digs deeper into modifying the child items of an object to insure the correct
        updates take place"""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u1 = User()
        u1.user_name = 'user1'
        u1.addresses = []
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1.addresses.append(a1)
        u2 = User()
        u2.user_name = 'user2'
        u2.addresses = []
        a2 = Address()
        a2.email_address = 'emailaddress2'
        u2.addresses.append(a2)

        a3 = Address()
        a3.email_address = 'emailaddress3'

        Session.commit()

        # modify user2 directly, append an address to user1.
        # upon commit, user2 should be updated, user1 should not
        # both address1 and address3 should be updated
        u2.user_name = 'user2modified'
        u1.addresses.append(a3)
        del u1.addresses[0]
        self.assert_sql(testing.db, lambda: Session.commit(),
                [
                    (
                        "UPDATE users SET user_name=:user_name WHERE users.user_id = :users_user_id",
                        {'users_user_id': u2.user_id, 'user_name': 'user2modified'}
                    ),
                    ("UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id",
                        {'user_id': None, 'email_addresses_address_id': a1.address_id}
                    ),
                    (
                        "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id",
                        {'user_id': u1.user_id, 'email_addresses_address_id': a3.address_id}
                    ),
                ])

    def test_childmove(self):
        """tests moving a child from one parent to the other, then deleting the first parent, properly
        updates the child with the new parent.  this tests the 'trackparent' option in the attributes module."""
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u1 = User()
        u1.user_name = 'user1'
        u2 = User()
        u2.user_name = 'user2'
        a = Address()
        a.email_address = 'address1'
        u1.addresses.append(a)
        Session.commit()
        del u1.addresses[0]
        u2.addresses.append(a)
        Session.delete(u1)
        Session.commit()
        Session.close()
        u2 = Session.get(User, u2.user_id)
        assert len(u2.addresses) == 1

    def test_childmove_2(self):
        m = mapper(User, users, properties = dict(
            addresses = relation(mapper(Address, addresses), lazy = True)
        ))
        u1 = User()
        u1.user_name = 'user1'
        u2 = User()
        u2.user_name = 'user2'
        a = Address()
        a.email_address = 'address1'
        u1.addresses.append(a)
        Session.commit()
        del u1.addresses[0]
        u2.addresses.append(a)
        Session.commit()
        Session.close()
        u2 = Session.get(User, u2.user_id)
        assert len(u2.addresses) == 1

    def test_o2m_delete_parent(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = True, uselist = False, private = False)
        ))
        u = User()
        a = Address()
        u.user_name = 'one2onetester'
        u.address = a
        u.address.email_address = 'myonlyaddress@foo.com'
        Session.commit()
        Session.delete(u)
        Session.commit()
        self.assert_(a.address_id is not None and a.user_id is None and u._instance_key not in Session.identity_map and a._instance_key in Session.identity_map)

    def test_onetoone(self):
        m = mapper(User, users, properties = dict(
            address = relation(mapper(Address, addresses), lazy = True, uselist = False)
        ))
        u = User()
        u.user_name = 'one2onetester'
        u.address = Address()
        u.address.email_address = 'myonlyaddress@foo.com'
        Session.commit()
        u.user_name = 'imnew'
        Session.commit()
        u.address.email_address = 'imnew@foo.com'
        Session.commit()

    def test_bidirectional(self):
        m1 = mapper(User, users)

        m2 = mapper(Address, addresses, properties = dict(
            user = relation(m1, lazy = False, backref='addresses')
        ))


        u = User()
        print repr(u.addresses)
        u.user_name = 'test'
        a = Address()
        a.email_address = 'testaddress'
        a.user = u
        Session.commit()
        print repr(u.addresses)
        x = False
        try:
            u.addresses.append('hi')
            x = True
        except:
            pass

        if x:
            self.assert_(False, "User addresses element should be scalar based")

        Session.delete(u)
        Session.commit()

    def test_doublerelation(self):
        m2 = mapper(Address, addresses)
        m = mapper(User, users, properties={
            'boston_addresses' : relation(m2, primaryjoin=
                        and_(users.c.user_id==addresses.c.user_id,
                        addresses.c.email_address.like('%boston%'))),
            'newyork_addresses' : relation(m2, primaryjoin=
                        and_(users.c.user_id==addresses.c.user_id,
                        addresses.c.email_address.like('%newyork%'))),
        })
        u = User()
        a = Address()
        a.email_address = 'foo@boston.com'
        b = Address()
        b.email_address = 'bar@newyork.com'

        u.boston_addresses.append(a)
        u.newyork_addresses.append(b)
        Session.commit()

class SaveTest(ORMTest):
    metadata = tables.metadata
    def define_tables(self, metadata):
        pass

    def setUp(self):
        super(SaveTest, self).setUp()
        keywords.insert().execute(
            dict(name='blue'),
            dict(name='red'),
            dict(name='green'),
            dict(name='big'),
            dict(name='small'),
            dict(name='round'),
            dict(name='square')
        )

    def test_basic(self):
        # save two users
        u = User()
        u.user_name = 'savetester'
        m = mapper(User, users)
        u2 = User()
        u2.user_name = 'savetester2'

        Session.save(u)

        Session.flush([u])
        Session.commit()

        # assert the first one retreives the same from the identity map
        nu = Session.get(m, u.user_id)
        print "U: " + repr(u) + "NU: " + repr(nu)
        self.assert_(u is nu)

        # clear out the identity map, so next get forces a SELECT
        Session.close()

        # check it again, identity should be different but ids the same
        nu = Session.get(m, u.user_id)
        self.assert_(u is not nu and u.user_id == nu.user_id and nu.user_name == 'savetester')
        Session.close()

        # change first users name and save
        Session.update(u)
        u.user_name = 'modifiedname'
        assert u in Session.dirty
        Session.commit()

        # select both
        #Session.close()
        userlist = User.query.filter(users.c.user_id.in_([u.user_id, u2.user_id])).order_by([users.c.user_name]).all()
        print repr(u.user_id), repr(userlist[0].user_id), repr(userlist[0].user_name)
        self.assert_(u.user_id == userlist[0].user_id and userlist[0].user_name == 'modifiedname')
        self.assert_(u2.user_id == userlist[1].user_id and userlist[1].user_name == 'savetester2')

    def test_synonym(self):
        class User(object):
            def _get_name(self):
                return "User:" + self.user_name
            def _set_name(self, name):
                self.user_name = name + ":User"
            name = property(_get_name, _set_name)

        mapper(User, users, properties={
            'name':synonym('user_name')
        })

        u = User()
        u.name = "some name"
        assert u.name == 'User:some name:User'
        Session.save(u)
        Session.flush()
        Session.clear()
        u = Session.query(User).first()
        assert u.name == 'User:some name:User'

    def test_lazyattr_commit(self):
        """tests that when a lazy-loaded list is unloaded, and a commit occurs, that the
        'passive' call on that list does not blow away its value"""

        m1 = mapper(User, users, properties = {
            'addresses': relation(mapper(Address, addresses))
        })

        u = User()
        u.addresses.append(Address())
        u.addresses.append(Address())
        u.addresses.append(Address())
        u.addresses.append(Address())
        Session.commit()
        Session.close()
        ulist = Session.query(m1).all()
        u1 = ulist[0]
        u1.user_name = 'newname'
        Session.commit()
        self.assert_(len(u1.addresses) == 4)

    def test_inherits(self):
        m1 = mapper(User, users)

        class AddressUser(User):
            """a user object that also has the users mailing address."""
            pass

        # define a mapper for AddressUser that inherits the User.mapper, and joins on the user_id column
        AddressUser.mapper = mapper(
                AddressUser,
                addresses, inherits=m1
                )

        au = AddressUser()
        Session.commit()
        Session.close()
        l = Session.query(AddressUser).one()
        self.assert_(l.user_id == au.user_id and l.address_id == au.address_id)

    def test_deferred(self):
        """test deferred column operations"""

        mapper(User, users, properties={
            'user_name':deferred(users.c.user_name)
        })

        # dont set deferred attribute, commit session
        u = User()
        u.user_id=42
        Session.commit()

        #  assert that changes get picked up
        u.user_name = 'some name'
        Session.commit()
        assert list(Session.execute(users.select(), mapper=User)) == [(42, 'some name')]
        Session.clear()

        # assert that a set operation doesn't trigger a load operation
        u = Session.query(User).filter(User.user_name=='some name').one()
        def go():
            u.user_name = 'some other name'
        self.assert_sql_count(testing.db, go, 0)
        Session.flush()
        assert list(Session.execute(users.select(), mapper=User)) == [(42, 'some other name')]

        Session.clear()

        # test assigning None to an unloaded deferred also works
        u = Session.query(User).filter(User.user_name=='some other name').one()
        u.user_name = None
        Session.flush()
        assert list(Session.execute(users.select(), mapper=User)) == [(42, None)]


    # why no support on oracle ?  because oracle doesn't save
    # "blank" strings; it saves a single space character.
    @testing.unsupported('oracle')
    def test_dont_update_blanks(self):
        mapper(User, users)
        u = User()
        u.user_name = ""
        Session.commit()
        Session.close()
        u = Session.query(User).get(u.user_id)
        u.user_name = ""
        def go():
            Session.commit()
        self.assert_sql_count(testing.db, go, 0)

    def test_multitable(self):
        """tests a save of an object where each instance spans two tables. also tests
        redefinition of the keynames for the column properties."""
        usersaddresses = sql.join(users, addresses, users.c.user_id == addresses.c.user_id)
        m = mapper(User, usersaddresses,
            properties = dict(
                email = addresses.c.email_address,
                foo_id = [users.c.user_id, addresses.c.user_id],
                )
            )

        u = User()
        u.user_name = 'multitester'
        u.email = 'multi@test.org'

        Session.commit()
        id = m.primary_key_from_instance(u)

        Session.close()

        u = Session.get(User, id)
        assert u.user_name == 'multitester'

        usertable = users.select(users.c.user_id.in_([u.foo_id])).execute().fetchall()
        self.assertEqual(usertable[0].values(), [u.foo_id, 'multitester'])
        addresstable = addresses.select(addresses.c.address_id.in_([u.address_id])).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [u.address_id, u.foo_id, 'multi@test.org'])

        u.email = 'lala@hey.com'
        u.user_name = 'imnew'
        Session.commit()

        usertable = users.select(users.c.user_id.in_([u.foo_id])).execute().fetchall()
        self.assertEqual(usertable[0].values(), [u.foo_id, 'imnew'])
        addresstable = addresses.select(addresses.c.address_id.in_([u.address_id])).execute().fetchall()
        self.assertEqual(addresstable[0].values(), [u.address_id, u.foo_id, 'lala@hey.com'])

        Session.close()
        u = Session.get(User, id)
        assert u.user_name == 'imnew'

    def test_history_get(self):
        """tests that the history properly lazy-fetches data when it wasnt otherwise loaded"""
        mapper(User, users, properties={
            'addresses':relation(Address, cascade="all, delete-orphan")
        })
        mapper(Address, addresses)

        u = User()
        u.addresses.append(Address())
        u.addresses.append(Address())
        Session.commit()
        Session.close()
        u = Session.query(User).get(u.user_id)
        Session.delete(u)
        Session.commit()
        assert users.count().scalar() == 0
        assert addresses.count().scalar() == 0



    def test_batchmode(self):
        """test the 'batch=False' flag on mapper()"""

        class TestExtension(MapperExtension):
            def before_insert(self, mapper, connection, instance):
                self.current_instance = instance
            def after_insert(self, mapper, connection, instance):
                assert instance is self.current_instance
        m = mapper(User, users, extension=TestExtension(), batch=False)
        u1 = User()
        u1.username = 'user1'
        u2 = User()
        u2.username = 'user2'
        Session.commit()

        clear_mappers()

        m = mapper(User, users, extension=TestExtension())
        u1 = User()
        u1.username = 'user1'
        u2 = User()
        u2.username = 'user2'
        try:
            Session.commit()
            assert False
        except AssertionError:
            assert True


class ManyToOneTest(ORMTest):
    metadata = tables.metadata

    def define_tables(self, metadata):
        pass

    def test_m2o_onetoone(self):
        # TODO: put assertion in here !!!
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True, uselist = False)
        ))
        data = [
            {'user_name' : 'thesub' , 'email_address' : 'bar@foo.com'},
            {'user_name' : 'assdkfj' , 'email_address' : 'thesdf@asdf.com'},
            {'user_name' : 'n4knd' , 'email_address' : 'asf3@bar.org'},
            {'user_name' : 'v88f4' , 'email_address' : 'adsd5@llala.net'},
            {'user_name' : 'asdf8d' , 'email_address' : 'theater@foo.com'}
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem['email_address']
            a.user = User()
            a.user.user_name = elem['user_name']
            objects.append(a)

        Session.commit()
        objects[2].email_address = 'imnew@foo.bar'
        objects[3].user = User()
        objects[3].user.user_name = 'imnewlyadded'
        self.assert_sql(testing.db, lambda: Session.commit(), [
                (
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'imnewlyadded'}
                ),
                {
                    "UPDATE email_addresses SET email_address=:email_address WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda ctx: {'email_address': 'imnew@foo.bar', 'email_addresses_address_id': objects[2].address_id}
                ,

                    "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda ctx: {'user_id': objects[3].user.user_id, 'email_addresses_address_id': objects[3].address_id}
                },

        ],
        with_sequences=[
                (
                    "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                    lambda ctx:{'user_name': 'imnewlyadded', 'user_id':ctx.last_inserted_ids()[0]}
                ),
                {
                    "UPDATE email_addresses SET email_address=:email_address WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda ctx: {'email_address': 'imnew@foo.bar', 'email_addresses_address_id': objects[2].address_id}
                ,

                    "UPDATE email_addresses SET user_id=:user_id WHERE email_addresses.address_id = :email_addresses_address_id":
                    lambda ctx: {'user_id': objects[3].user.user_id, 'email_addresses_address_id': objects[3].address_id}
                },

        ])
        l = sql.select([users, addresses], sql.and_(users.c.user_id==addresses.c.user_id, addresses.c.address_id==a.address_id)).execute()
        assert l.fetchone().values() == [a.user.user_id, 'asdf8d', a.address_id, a.user_id, 'theater@foo.com']


    def test_manytoone_1(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True)
        ))
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1 = User()
        u1.user_name='user1'

        a1.user = u1
        Session.commit()
        Session.close()
        a1 = Session.query(Address).get(a1.address_id)
        u1 = Session.query(User).get(u1.user_id)
        assert a1.user is u1

        a1.user = None
        Session.commit()
        Session.close()
        a1 = Session.query(Address).get(a1.address_id)
        u1 = Session.query(User).get(u1.user_id)
        assert a1.user is None

    def test_manytoone_2(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True)
        ))
        a1 = Address()
        a1.email_address = 'emailaddress1'
        a2 = Address()
        a2.email_address = 'emailaddress2'
        u1 = User()
        u1.user_name='user1'

        a1.user = u1
        Session.commit()
        Session.close()
        a1 = Session.query(Address).get(a1.address_id)
        a2 = Session.query(Address).get(a2.address_id)
        u1 = Session.query(User).get(u1.user_id)
        assert a1.user is u1
        a1.user = None
        a2.user = u1
        Session.commit()
        Session.close()
        a1 = Session.query(Address).get(a1.address_id)
        a2 = Session.query(Address).get(a2.address_id)
        u1 = Session.query(User).get(u1.user_id)
        assert a1.user is None
        assert a2.user is u1

    def test_manytoone_3(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True)
        ))
        a1 = Address()
        a1.email_address = 'emailaddress1'
        u1 = User()
        u1.user_name='user1'
        u2 = User()
        u2.user_name='user2'

        a1.user = u1
        Session.commit()
        Session.close()
        a1 = Session.query(Address).get(a1.address_id)
        u1 = Session.query(User).get(u1.user_id)
        u2 = Session.query(User).get(u2.user_id)
        assert a1.user is u1

        a1.user = u2
        Session.commit()
        Session.close()
        a1 = Session.query(Address).get(a1.address_id)
        u1 = Session.query(User).get(u1.user_id)
        u2 = Session.query(User).get(u2.user_id)
        assert a1.user is u2

    def test_bidirectional_noload(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref='user', lazy=None)
        })
        mapper(Address, addresses)

        sess = Session()

        # try it on unsaved objects
        u1 = User()
        a1 = Address()
        a1.user = u1
        sess.save(u1)
        sess.flush()
        sess.clear()

        a1 = sess.query(Address).get(a1.address_id)

        a1.user = None
        sess.flush()
        sess.clear()
        assert sess.query(Address).get(a1.address_id).user is None
        assert sess.query(User).get(u1.user_id).addresses == []


class ManyToManyTest(ORMTest):
    metadata = tables.metadata

    def define_tables(self, metadata):
        pass

    def test_manytomany(self):
        items = orderitems

        keywordmapper = mapper(Keyword, keywords)

        m = mapper(Item, items, properties = dict(
                keywords = relation(keywordmapper, itemkeywords, lazy = False, order_by=keywords.c.name),
            ))

        data = [Item,
            {'item_name': 'mm_item1', 'keywords' : (Keyword,[{'name': 'big'},{'name': 'green'}, {'name': 'purple'},{'name': 'round'}])},
            {'item_name': 'mm_item2', 'keywords' : (Keyword,[{'name':'blue'}, {'name':'imnew'},{'name':'round'}, {'name':'small'}])},
            {'item_name': 'mm_item3', 'keywords' : (Keyword,[])},
            {'item_name': 'mm_item4', 'keywords' : (Keyword,[{'name':'big'}, {'name':'blue'},])},
            {'item_name': 'mm_item5', 'keywords' : (Keyword,[{'name':'big'},{'name':'exacting'},{'name':'green'}])},
            {'item_name': 'mm_item6', 'keywords' : (Keyword,[{'name':'red'},{'name':'round'},{'name':'small'}])},
        ]
        objects = []
        for elem in data[1:]:
            item = Item()
            objects.append(item)
            item.item_name = elem['item_name']
            item.keywords = []
            if elem['keywords'][1]:
                klist = Session.query(keywordmapper).filter(keywords.c.name.in_([e['name'] for e in elem['keywords'][1]]))
            else:
                klist = []
            khash = {}
            for k in klist:
                khash[k.name] = k
            for kname in [e['name'] for e in elem['keywords'][1]]:
                try:
                    k = khash[kname]
                except KeyError:
                    k = Keyword()
                    k.name = kname
                item.keywords.append(k)

        Session.commit()

        l = Session.query(m).filter(items.c.item_name.in_([e['item_name'] for e in data[1:]])).order_by(items.c.item_name).all()
        self.assert_result(l, *data)

        objects[4].item_name = 'item4updated'
        k = Keyword()
        k.name = 'yellow'
        objects[5].keywords.append(k)
        self.assert_sql(testing.db, lambda:Session.commit(), [
            {
                "UPDATE items SET item_name=:item_name WHERE items.item_id = :items_item_id":
                {'item_name': 'item4updated', 'items_item_id': objects[4].item_id}
            ,
                "INSERT INTO keywords (name) VALUES (:name)":
                {'name': 'yellow'}
            },
            ("INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
            lambda ctx: [{'item_id': objects[5].item_id, 'keyword_id': k.keyword_id}]
            )
        ],

        with_sequences = [
            {
                "UPDATE items SET item_name=:item_name WHERE items.item_id = :items_item_id":
                {'item_name': 'item4updated', 'items_item_id': objects[4].item_id}
            ,
                "INSERT INTO keywords (keyword_id, name) VALUES (:keyword_id, :name)":
                lambda ctx: {'name': 'yellow', 'keyword_id':ctx.last_inserted_ids()[0]}
            },
            ("INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
            lambda ctx: [{'item_id': objects[5].item_id, 'keyword_id': k.keyword_id}]
            )
        ]
        )
        objects[2].keywords.append(k)
        dkid = objects[5].keywords[1].keyword_id
        del objects[5].keywords[1]
        self.assert_sql(testing.db, lambda:Session.commit(), [
                (
                    "DELETE FROM itemkeywords WHERE itemkeywords.item_id = :item_id AND itemkeywords.keyword_id = :keyword_id",
                    [{'item_id': objects[5].item_id, 'keyword_id': dkid}]
                ),
                (
                    "INSERT INTO itemkeywords (item_id, keyword_id) VALUES (:item_id, :keyword_id)",
                    lambda ctx: [{'item_id': objects[2].item_id, 'keyword_id': k.keyword_id}]
                )
        ])

        Session.delete(objects[3])
        Session.commit()

    def test_manytomany_remove(self):
        """tests that setting a list-based attribute to '[]' properly affects the history and allows
        the many-to-many rows to be deleted"""
        keywordmapper = mapper(Keyword, keywords)

        m = mapper(Item, orderitems, properties = dict(
                keywords = relation(keywordmapper, itemkeywords, lazy = False),
            ))

        i = Item()
        k1 = Keyword()
        k2 = Keyword()
        i.keywords.append(k1)
        i.keywords.append(k2)
        Session.commit()

        assert itemkeywords.count().scalar() == 2
        i.keywords = []
        Session.commit()
        assert itemkeywords.count().scalar() == 0

    def test_scalar(self):
        """test that dependency.py doesnt try to delete an m2m relation referencing None."""

        mapper(Keyword, keywords)

        mapper(Item, orderitems, properties = dict(
                keyword = relation(Keyword, secondary=itemkeywords, uselist=False),
            ))

        i = Item()
        Session.commit()
        Session.delete(i)
        Session.commit()



    def test_manytomany_update(self):
        """tests some history operations on a many to many"""
        class Keyword(object):
            def __init__(self, name):
                self.name = name
            def __eq__(self, other):
                return other.__class__ == Keyword and other.name == self.name
            def __repr__(self):
                return "Keyword(%s, %s)" % (getattr(self, 'keyword_id', 'None'), self.name)

        mapper(Keyword, keywords)
        mapper(Item, orderitems, properties = dict(
                keywords = relation(Keyword, secondary=itemkeywords, lazy=False, order_by=keywords.c.name),
            ))

        (k1, k2, k3) = (Keyword('keyword 1'), Keyword('keyword 2'), Keyword('keyword 3'))
        item = Item()
        item.item_name = 'item 1'
        item.keywords.append(k1)
        item.keywords.append(k2)
        item.keywords.append(k3)
        Session.commit()

        item.keywords = []
        item.keywords.append(k1)
        item.keywords.append(k2)
        Session.commit()

        Session.close()
        item = Session.query(Item).get(item.item_id)
        print [k1, k2]
        print item.keywords
        assert item.keywords == [k1, k2]

    def test_association(self):
        """basic test of an association object"""
        class IKAssociation(object):
            def __repr__(self):
                return "\nIKAssociation " + repr(self.item_id) + " " + repr(self.keyword)

        items = orderitems

        keywordmapper = mapper(Keyword, keywords)

        # note that we are breaking a rule here, making a second mapper(Keyword, keywords)
        # the reorganization of mapper construction affected this, but was fixed again
        m = mapper(Item, items, properties = dict(
                keywords = relation(mapper(IKAssociation, itemkeywords, properties = dict(
                    keyword = relation(mapper(Keyword, keywords, non_primary=True), lazy = False, uselist = False, order_by=keywords.c.name)
                ), primary_key = [itemkeywords.c.item_id, itemkeywords.c.keyword_id]),
                lazy = False)
            ))

        data = [Item,
            {'item_name': 'a_item1', 'keywords' : (IKAssociation,
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'big'})},
                                                        {'keyword' : (Keyword, {'name': 'green'})},
                                                        {'keyword' : (Keyword, {'name': 'purple'})},
                                                        {'keyword' : (Keyword, {'name': 'round'})}
                                                    ]
                                                 )
            },
            {'item_name': 'a_item2', 'keywords' : (IKAssociation,
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'huge'})},
                                                        {'keyword' : (Keyword, {'name': 'violet'})},
                                                        {'keyword' : (Keyword, {'name': 'yellow'})}
                                                    ]
                                                 )
            },
            {'item_name': 'a_item3', 'keywords' : (IKAssociation,
                                                    [
                                                        {'keyword' : (Keyword, {'name': 'big'})},
                                                        {'keyword' : (Keyword, {'name': 'blue'})},
                                                    ]
                                                 )
            }
        ]
        for elem in data[1:]:
            item = Item()
            item.item_name = elem['item_name']
            item.keywords = []
            for kname in [e['keyword'][1]['name'] for e in elem['keywords'][1]]:
                try:
                    k = Keyword.query.filter(keywords.c.name == kname)[0]
                except IndexError:
                    k = Keyword()
                    k.name= kname
                ik = IKAssociation()
                ik.keyword = k
                item.keywords.append(ik)

        Session.commit()
        Session.close()
        l = Item.query.filter(items.c.item_name.in_([e['item_name'] for e in data[1:]])).order_by(items.c.item_name).all()
        self.assert_result(l, *data)

class SaveTest2(ORMTest):

    def define_tables(self, metadata):
        global users, addresses
        users = Table('users', metadata,
            Column('user_id', Integer, Sequence('user_id_seq', optional=True), primary_key = True),
            Column('user_name', String(20)),
        )

        addresses = Table('email_addresses', metadata,
            Column('address_id', Integer, Sequence('address_id_seq', optional=True), primary_key = True),
            Column('rel_user_id', Integer, ForeignKey(users.c.user_id)),
            Column('email_address', String(20)),
        )

    def test_m2o_nonmatch(self):
        m = mapper(Address, addresses, properties = dict(
            user = relation(mapper(User, users), lazy = True, uselist = False)
        ))
        data = [
            {'user_name' : 'thesub' , 'email_address' : 'bar@foo.com'},
            {'user_name' : 'assdkfj' , 'email_address' : 'thesdf@asdf.com'},
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem['email_address']
            a.user = User()
            a.user.user_name = elem['user_name']
            objects.append(a)
        self.assert_sql(testing.db, lambda: Session.commit(), [
                (
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'thesub'}
                ),
                (
                    "INSERT INTO users (user_name) VALUES (:user_name)",
                    {'user_name': 'assdkfj'}
                ),
                (
                "INSERT INTO email_addresses (rel_user_id, email_address) VALUES (:rel_user_id, :email_address)",
                {'rel_user_id': 1, 'email_address': 'bar@foo.com'}
                ),
                (
                "INSERT INTO email_addresses (rel_user_id, email_address) VALUES (:rel_user_id, :email_address)",
                {'rel_user_id': 2, 'email_address': 'thesdf@asdf.com'}
                )
                ],

                with_sequences = [
                        (
                            "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                            lambda ctx: {'user_name': 'thesub', 'user_id':ctx.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO users (user_id, user_name) VALUES (:user_id, :user_name)",
                            lambda ctx: {'user_name': 'assdkfj', 'user_id':ctx.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO email_addresses (address_id, rel_user_id, email_address) VALUES (:address_id, :rel_user_id, :email_address)",
                        lambda ctx:{'rel_user_id': 1, 'email_address': 'bar@foo.com', 'address_id':ctx.last_inserted_ids()[0]}
                        ),
                        (
                        "INSERT INTO email_addresses (address_id, rel_user_id, email_address) VALUES (:address_id, :rel_user_id, :email_address)",
                        lambda ctx:{'rel_user_id': 2, 'email_address': 'thesdf@asdf.com', 'address_id':ctx.last_inserted_ids()[0]}
                        )
                        ]
        )


class SaveTest3(ORMTest):
    def define_tables(self, metadata):
        global t1, t2, t3

        t1 = Table('items', metadata,
            Column('item_id', INT, Sequence('items_id_seq', optional=True), primary_key = True),
            Column('item_name', VARCHAR(50)),
        )

        t3 = Table('keywords', metadata,
            Column('keyword_id', Integer, Sequence('keyword_id_seq', optional=True), primary_key = True),
            Column('name', VARCHAR(50)),

        )
        t2 = Table('assoc', metadata,
            Column('item_id', INT, ForeignKey("items")),
            Column('keyword_id', INT, ForeignKey("keywords")),
            Column('foo', Boolean, default=True)
        )

    def test_manytomany_xtracol_delete(self):
        """test that a many-to-many on a table that has an extra column can properly delete rows from the table
        without referencing the extra column"""
        mapper(Keyword, t3)

        mapper(Item, t1, properties = dict(
                keywords = relation(Keyword, secondary=t2, lazy = False),
            ))

        i = Item()
        k1 = Keyword()
        k2 = Keyword()
        i.keywords.append(k1)
        i.keywords.append(k2)
        Session.commit()

        assert t2.count().scalar() == 2
        i.keywords = []
        print i.keywords
        Session.commit()
        assert t2.count().scalar() == 0

class BooleanColTest(ORMTest):
    def define_tables(self, metadata):
        global t
        t =Table('t1', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)),
            Column('value', Boolean))

    def test_boolean(self):
        # use the regular mapper
        from sqlalchemy.orm import mapper

        class T(fixtures.Base):
            pass
        mapper(T, t)

        sess = create_session()
        t1 = T(value=True, name="t1")
        t2 = T(value=False, name="t2")
        t3 = T(value=True, name="t3")
        sess.save(t1)
        sess.save(t2)
        sess.save(t3)

        sess.flush()

        for clear in (False, True):
            if clear:
                sess.clear()
            self.assertEquals(sess.query(T).all(), [T(value=True, name="t1"), T(value=False, name="t2"), T(value=True, name="t3")])
            if clear:
                sess.clear()
            self.assertEquals(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"),T(value=True, name="t3")])
            if clear:
                sess.clear()
            self.assertEquals(sess.query(T).filter(T.value==False).all(), [T(value=False, name="t2")])

        t2 = sess.query(T).get(t2.id)
        t2.value = True
        sess.flush()
        self.assertEquals(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"), T(value=True, name="t2"), T(value=True, name="t3")])
        t2.value = False
        sess.flush()
        self.assertEquals(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"),T(value=True, name="t3")])


class RowSwitchTest(ORMTest):
    def define_tables(self, metadata):
        global t1, t2, t3, t1t3

        global T1, T2, T3

        Session.remove()

        # parent
        t1 = Table('t1', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False))

        # onetomany
        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False),
            Column('t1id', Integer, ForeignKey('t1.id'),nullable=False),
            )

        # associated
        t3 = Table('t3', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False),
            )

        #manytomany
        t1t3 = Table('t1t3', metadata,
            Column('t1id', Integer, ForeignKey('t1.id'),nullable=False),
            Column('t3id', Integer, ForeignKey('t3.id'),nullable=False),
        )

        class T1(fixtures.Base):
            pass

        class T2(fixtures.Base):
            pass

        class T3(fixtures.Base):
            pass

    def tearDown(self):
        Session.remove()
        super(RowSwitchTest, self).tearDown()

    def test_onetomany(self):
        mapper(T1, t1, properties={
            't2s':relation(T2, cascade="all, delete-orphan")
        })
        mapper(T2, t2)

        sess = Session(autoflush=False)

        o1 = T1(data='some t1', id=1)
        o1.t2s.append(T2(data='some t2', id=1))
        o1.t2s.append(T2(data='some other t2', id=2))

        sess.save(o1)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some t1')]
        assert list(sess.execute(t2.select(), mapper=T1)) == [(1, 'some t2', 1), (2, 'some other t2', 1)]

        o2 = T1(data='some other t1', id=o1.id, t2s=[
            T2(data='third t2', id=3),
            T2(data='fourth t2', id=4),
            ])
        sess.delete(o1)
        sess.save(o2)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some other t1')]
        assert list(sess.execute(t2.select(), mapper=T1)) == [(3, 'third t2', 1), (4, 'fourth t2', 1)]

    def test_manytomany(self):
        mapper(T1, t1, properties={
            't3s':relation(T3, secondary=t1t3, cascade="all, delete-orphan")
        })
        mapper(T3, t3)

        sess = Session(autoflush=False)

        o1 = T1(data='some t1', id=1)
        o1.t3s.append(T3(data='some t3', id=1))
        o1.t3s.append(T3(data='some other t3', id=2))

        sess.save(o1)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some t1')]
        assert rowset(sess.execute(t1t3.select(), mapper=T1)) == set([(1,1), (1, 2)])
        assert list(sess.execute(t3.select(), mapper=T1)) == [(1, 'some t3'), (2, 'some other t3')]

        o2 = T1(data='some other t1', id=1, t3s=[
            T3(data='third t3', id=3),
            T3(data='fourth t3', id=4),
            ])
        sess.delete(o1)
        sess.save(o2)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some other t1')]
        assert list(sess.execute(t3.select(), mapper=T1)) == [(3, 'third t3'), (4, 'fourth t3')]

    def test_manytoone(self):

        mapper(T2, t2, properties={
            't1':relation(T1)
        })
        mapper(T1, t1)

        sess = Session(autoflush=False)

        o1 = T2(data='some t2', id=1)
        o1.t1 = T1(data='some t1', id=1)

        sess.save(o1)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(1, 'some t1')]
        assert list(sess.execute(t2.select(), mapper=T1)) == [(1, 'some t2', 1)]

        o2 = T2(data='some other t2', id=1, t1=T1(data='some other t1', id=2))
        sess.delete(o1)
        sess.delete(o1.t1)
        sess.save(o2)
        sess.flush()

        assert list(sess.execute(t1.select(), mapper=T1)) == [(2, 'some other t1')]
        assert list(sess.execute(t2.select(), mapper=T1)) == [(1, 'some other t2', 2)]

class TransactionTest(ORMTest):
    __unsupported_on__ = ('mysql', 'mssql')

    # sqlite doesn't have deferrable constraints, but it allows them to
    # be specified.  it'll raise immediately post-INSERT, instead of at
    # COMMIT. either way, this test should pass.

    def define_tables(self, metadata):
        global t1, T1, t2, T2

        Session.remove()

        t1 = Table('t1', metadata,
            Column('id', Integer, primary_key=True))

        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True),
            Column('t1_id', Integer,
                   ForeignKey('t1.id', deferrable=True, initially='deferred')
                   ))

        # deferred_constraint = \
        #   DDL("ALTER TABLE t2 ADD CONSTRAINT t2_t1_id_fk FOREIGN KEY (t1_id) "
        #       "REFERENCES t1 (id) DEFERRABLE INITIALLY DEFERRED")
        # deferred_constraint.execute_at('after-create', t2)
        # t1.create()
        # t2.create()
        # t2.append_constraint(ForeignKeyConstraint(['t1_id'], ['t1.id']))

        class T1(fixtures.Base):
            pass

        class T2(fixtures.Base):
            pass

        orm_mapper(T1, t1)
        orm_mapper(T2, t2)

    def test_close_transaction_on_commit_fail(self):
        Session = sessionmaker(autoflush=False, transactional=False)
        sess = Session()

        # with a deferred constraint, this fails at COMMIT time instead
        # of at INSERT time.
        sess.save(T2(t1_id=123))

        try:
            sess.flush()
            assert False
        except:
            # Flush needs to rollback also when commit fails
            assert sess.transaction is None

        # todo: on 8.3 at least, the failed commit seems to close the cursor?
        # needs investigation.  leaving in the DDL above now to help verify
        # that the new deferrable support on FK isn't involved in this issue.
        if testing.against('postgres'):
            t1.bind.engine.dispose()
if __name__ == "__main__":
    testenv.main()
