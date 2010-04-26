# coding: utf-8
"""Tests unitofwork operations."""

from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
import datetime
import operator
from sqlalchemy.orm import mapper as orm_mapper

import sqlalchemy as sa
from sqlalchemy.test import engines, testing, pickleable
from sqlalchemy import Integer, String, ForeignKey, literal_column
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import mapper, relationship, create_session, column_property, attributes
from sqlalchemy.test.testing import eq_, ne_
from test.orm import _base, _fixtures
from test.engine import _base as engine_base
from sqlalchemy.test.assertsql import AllOf, CompiledSQL
import gc

class UnitOfWorkTest(object):
    pass

class HistoryTest(_fixtures.FixtureTest):
    run_inserts = None

    @classmethod
    def setup_classes(cls):
        class User(_base.ComparableEntity):
            pass
        class Address(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_backref(self):
        am = mapper(Address, addresses)
        m = mapper(User, users, properties=dict(
            addresses = relationship(am, backref='user', lazy='joined')))

        session = create_session(autocommit=False)

        u = User(name='u1')
        a = Address(email_address='u1@e')
        a.user = u
        session.add(u)

        self.assert_(u.addresses == [a])
        session.commit()
        session.expunge_all()

        u = session.query(m).one()
        assert u.addresses[0].user == u
        session.close()

class UnicodeTest(_base.MappedTest):
    __requires__ = ('unicode_connections',)

    @classmethod
    def define_tables(cls, metadata):
        if testing.against('mysql+oursql'):
            from sqlalchemy.dialects.mysql import VARCHAR
            uni_type = VARCHAR(50, collation='utf8_unicode_ci')
        else:
            uni_type = sa.Unicode(50)
        
        Table('uni_t1', metadata,
            Column('id',  Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('txt', uni_type, unique=True))
        Table('uni_t2', metadata,
            Column('id',  Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('txt', uni_type, ForeignKey('uni_t1')))

    @classmethod
    def setup_classes(cls):
        class Test(_base.BasicEntity):
            pass
        class Test2(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_basic(self):
        mapper(Test, uni_t1)

        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(id=1, txt=txt)
        self.assert_(t1.txt == txt)

        session = create_session(autocommit=False)
        session.add(t1)
        session.commit()

        self.assert_(t1.txt == txt)
    
    @testing.resolve_artifact_names
    def test_relationship(self):
        mapper(Test, uni_t1, properties={
            't2s': relationship(Test2)})
        mapper(Test2, uni_t2)

        txt = u"\u0160\u0110\u0106\u010c\u017d"
        t1 = Test(txt=txt)
        t1.t2s.append(Test2())
        t1.t2s.append(Test2())
        session = create_session(autocommit=False)
        session.add(t1)
        session.commit()
        session.close()

        session = create_session()
        t1 = session.query(Test).filter_by(id=t1.id).one()
        assert len(t1.t2s) == 2

class UnicodeSchemaTest(engine_base.AltEngineTest, _base.MappedTest):
    __requires__ = ('unicode_connections', 'unicode_ddl',)

    @classmethod
    def create_engine(cls):
        return engines.utf8_engine()

    @classmethod
    def define_tables(cls, metadata):
        t1 = Table('unitable1', metadata,
              Column(u'méil', Integer, primary_key=True, key='a', test_needs_autoincrement=True),
              Column(u'\u6e2c\u8a66', Integer, key='b'),
              Column('type',  String(20)),
              test_needs_fk=True,
              test_needs_autoincrement=True)
        t2 = Table(u'Unitéble2', metadata,
              Column(u'méil', Integer, primary_key=True, key="cc", test_needs_autoincrement=True),
              Column(u'\u6e2c\u8a66', Integer,
                     ForeignKey(u'unitable1.a'), key="d"),
              Column(u'\u6e2c\u8a66_2', Integer, key="e"),
              test_needs_fk=True,
              test_needs_autoincrement=True)

        cls.tables['t1'] = t1
        cls.tables['t2'] = t2

    @classmethod
    def setup_class(cls):
        super(UnicodeSchemaTest, cls).setup_class()

    @classmethod
    def teardown_class(cls):
        super(UnicodeSchemaTest, cls).teardown_class()

    @testing.fails_on('mssql+pyodbc',
                      'pyodbc returns a non unicode encoding of the results description.')
    @testing.resolve_artifact_names
    def test_mapping(self):
        class A(_base.ComparableEntity):
            pass
        class B(_base.ComparableEntity):
            pass

        mapper(A, t1, properties={
            't2s':relationship(B)})
        mapper(B, t2)

        a1 = A()
        b1 = B()
        a1.t2s.append(b1)

        session = create_session()
        session.add(a1)
        session.flush()
        session.expunge_all()

        new_a1 = session.query(A).filter(t1.c.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.expunge_all()

        new_a1 = (session.query(A).options(sa.orm.joinedload('t2s')).
                  filter(t1.c.a == a1.a)).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.expunge_all()

        new_a1 = session.query(A).filter(A.a == a1.a).one()
        assert new_a1.a == a1.a
        assert new_a1.t2s[0].d == b1.d
        session.expunge_all()

    @testing.fails_on('mssql+pyodbc',
                      'pyodbc returns a non unicode encoding of the results description.')
    @testing.resolve_artifact_names
    def test_inheritance_mapping(self):
        class A(_base.ComparableEntity):
            pass
        class B(A):
            pass

        mapper(A, t1,
               polymorphic_on=t1.c.type,
               polymorphic_identity='a')
        mapper(B, t2,
               inherits=A,
               polymorphic_identity='b')
        a1 = A(b=5)
        b1 = B(e=7)

        session = create_session()
        session.add_all((a1, b1))
        session.flush()
        session.expunge_all()

        eq_([A(b=5), B(e=7)], session.query(A).all())

class BinaryHistTest(_base.MappedTest, testing.AssertsExecutionResults):
    @classmethod
    def define_tables(cls, metadata):
        Table('t1', metadata,
            Column('id', sa.Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', sa.LargeBinary),
        )

    @classmethod
    def setup_classes(cls):
        class Foo(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_binary_equality(self):
        
        # Py3K
        #data = b"this is some data"
        # Py2K
        data = "this is some data"
        # end Py2K
        
        mapper(Foo, t1)
        
        s = create_session()
        
        f1 = Foo(data=data)
        s.add(f1)
        s.flush()
        s.expire_all()
        f1 = s.query(Foo).first()
        assert f1.data == data
        f1.data = data
        eq_(
            sa.orm.attributes.get_history(f1, "data"),
            ((), [data], ())
        )
        def go():
            s.flush()
        self.assert_sql_count(testing.db, go, 0)
        
class MutableTypesTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('mutable_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('data', sa.PickleType),
            Column('val', sa.Unicode(30)))

    @classmethod
    def setup_classes(cls):
        class Foo(_base.BasicEntity):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Foo, mutable_t)

    @testing.resolve_artifact_names
    def test_basic(self):
        """Changes are detected for types marked as MutableType."""

        f1 = Foo()
        f1.data = pickleable.Bar(4,5)

        session = create_session()
        session.add(f1)
        session.flush()
        session.expunge_all()

        f2 = session.query(Foo).filter_by(id=f1.id).one()
        assert 'data' in sa.orm.attributes.instance_state(f2).unmodified
        eq_(f2.data, f1.data)

        f2.data.y = 19
        assert f2 in session.dirty
        assert 'data' not in sa.orm.attributes.instance_state(f2).unmodified
        session.flush()
        session.expunge_all()

        f3 = session.query(Foo).filter_by(id=f1.id).one()
        ne_(f3.data,f1.data)
        eq_(f3.data, pickleable.Bar(4, 19))

    @testing.resolve_artifact_names
    def test_mutable_changes(self):
        """Mutable changes are detected or not detected correctly"""

        f1 = Foo()
        f1.data = pickleable.Bar(4,5)
        f1.val = u'hi'

        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()

        bind = self.metadata.bind

        self.sql_count_(0, session.commit)
        f1.val = u'someothervalue'
        self.assert_sql(bind, session.commit, [
            ("UPDATE mutable_t SET val=:val "
             "WHERE mutable_t.id = :mutable_t_id",
             {'mutable_t_id': f1.id, 'val': u'someothervalue'})])

        f1.val = u'hi'
        f1.data.x = 9
        self.assert_sql(bind, session.commit, [
            ("UPDATE mutable_t SET data=:data, val=:val "
             "WHERE mutable_t.id = :mutable_t_id",
             {'mutable_t_id': f1.id, 'val': u'hi', 'data':f1.data})])

    @testing.resolve_artifact_names
    def test_resurrect(self):
        f1 = Foo()
        f1.data = pickleable.Bar(4,5)
        f1.val = u'hi'

        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()

        f1.data.y = 19
        del f1

        gc.collect()
        assert len(session.identity_map) == 1

        session.commit()

        assert session.query(Foo).one().data == pickleable.Bar(4, 19)

    @testing.resolve_artifact_names
    def test_resurrect_two(self):
        f1 = Foo()
        f1.data = pickleable.Bar(4,5)
        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()
        
        session = create_session(autocommit=False)
        f1 = session.query(Foo).first()
        del f1 # modified flag flips by accident
        gc.collect()
        f1 = session.query(Foo).first()
        assert not attributes.instance_state(f1).modified
        
    @testing.resolve_artifact_names
    def test_unicode(self):
        """Equivalent Unicode values are not flagged as changed."""

        f1 = Foo(val=u'hi')

        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()
        session.expunge_all()

        f1 = session.query(Foo).get(f1.id)
        f1.val = u'hi'
        self.sql_count_(0, session.commit)


class PickledDictsTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('mutable_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('data', sa.PickleType(comparator=operator.eq)))

    @classmethod
    def setup_classes(cls):
        class Foo(_base.BasicEntity):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(Foo, mutable_t)

    @testing.resolve_artifact_names
    def test_dicts(self):
        """Dictionaries may not pickle the same way twice."""

        f1 = Foo()
        f1.data = [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': True,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ]

        session = create_session(autocommit=False)
        session.add(f1)
        session.commit()

        self.sql_count_(0, session.commit)

        f1.data = [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': True,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ]

        self.sql_count_(0, session.commit)

        f1.data[0]['personne']['VenSoir']= False
        self.sql_count_(1, session.commit)

        session.expunge_all()
        f = session.query(Foo).get(f1.id)
        eq_(f.data,
            [ {
            'personne': {'nom': u'Smith',
                         'pers_id': 1,
                         'prenom': u'john',
                         'civilite': u'Mr',
                         'int_3': False,
                         'int_2': False,
                         'int_1': u'23',
                         'VenSoir': False,
                         'str_1': u'Test',
                         'SamMidi': False,
                         'str_2': u'chien',
                         'DimMidi': False,
                         'SamSoir': True,
                         'SamAcc': False} } ])


class PKTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('multipk1', metadata,
              Column('multi_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('multi_rev', Integer, primary_key=True),
              Column('name', String(50), nullable=False),
              Column('value', String(100)))

        Table('multipk2', metadata,
              Column('pk_col_1', String(30), primary_key=True),
              Column('pk_col_2', String(30), primary_key=True),
              Column('data', String(30)))
        Table('multipk3', metadata,
              Column('pri_code', String(30), key='primary', primary_key=True),
              Column('sec_code', String(30), key='secondary', primary_key=True),
              Column('date_assigned', sa.Date, key='assigned', primary_key=True),
              Column('data', String(30)))

    @classmethod
    def setup_classes(cls):
        class Entry(_base.BasicEntity):
            pass

    # not supported on sqlite since sqlite's auto-pk generation only works with
    # single column primary keys
    @testing.fails_on('sqlite', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_primary_key(self):
        mapper(Entry, multipk1)

        e = Entry(name='entry1', value='this is entry 1', multi_rev=2)

        session = create_session()
        session.add(e)
        session.flush()
        session.expunge_all()

        e2 = session.query(Entry).get((e.multi_id, 2))
        self.assert_(e is not e2)
        state = sa.orm.attributes.instance_state(e)
        state2 = sa.orm.attributes.instance_state(e2)
        eq_(state.key, state2.key)

    # this one works with sqlite since we are manually setting up pk values
    @testing.resolve_artifact_names
    def test_manual_pk(self):
        mapper(Entry, multipk2)

        e = Entry(pk_col_1='pk1', pk_col_2='pk1_related', data='im the data')

        session = create_session()
        session.add(e)
        session.flush()

    @testing.resolve_artifact_names
    def test_key_pks(self):
        mapper(Entry, multipk3)

        e = Entry(primary= 'pk1', secondary='pk2',
                   assigned=datetime.date.today(), data='some more data')

        session = create_session()
        session.add(e)
        session.flush()


class ForeignPKTest(_base.MappedTest):
    """Detection of the relationship direction on PK joins."""

    @classmethod
    def define_tables(cls, metadata):
        Table("people", metadata,
              Column('person', String(10), primary_key=True),
              Column('firstname', String(10)),
              Column('lastname', String(10)))

        Table("peoplesites", metadata,
              Column('person', String(10), ForeignKey("people.person"),
                     primary_key=True),
              Column('site', String(10)))

    @classmethod
    def setup_classes(cls):
        class Person(_base.BasicEntity):
            pass
        class PersonSite(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_basic(self):
        m1 = mapper(PersonSite, peoplesites)
        m2 = mapper(Person, people, properties={
            'sites' : relationship(PersonSite)})

        sa.orm.compile_mappers()
        eq_(list(m2.get_property('sites').synchronize_pairs),
            [(people.c.person, peoplesites.c.person)])

        p = Person(person='im the key', firstname='asdf')
        ps = PersonSite(site='asdf')
        p.sites.append(ps)

        session = create_session()
        session.add(p)
        session.flush()

        p_count = people.count(people.c.person=='im the key').scalar()
        eq_(p_count, 1)
        eq_(peoplesites.count(peoplesites.c.person=='im the key').scalar(), 1)


class ClauseAttributesTest(_base.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('users_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('name', String(30)),
            Column('counter', Integer, default=1))

    @classmethod
    def setup_classes(cls):
        class User(_base.ComparableEntity):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        mapper(User, users_t)

    @testing.resolve_artifact_names
    def test_update(self):
        u = User(name='test')

        session = create_session()
        session.add(u)
        session.flush()

        eq_(u.counter, 1)
        u.counter = User.counter + 1
        session.flush()

        def go():
            assert (u.counter == 2) is True  # ensure its not a ClauseElement
        self.sql_count_(1, go)

    @testing.resolve_artifact_names
    def test_multi_update(self):
        u = User(name='test')

        session = create_session()
        session.add(u)
        session.flush()

        eq_(u.counter, 1)
        u.name = 'test2'
        u.counter = User.counter + 1
        session.flush()

        def go():
            eq_(u.name, 'test2')
            assert (u.counter == 2) is True
        self.sql_count_(1, go)

        session.expunge_all()
        u = session.query(User).get(u.id)
        eq_(u.name, 'test2')
        eq_(u.counter,  2)

    @testing.resolve_artifact_names
    def test_insert(self):
        u = User(name='test', counter=sa.select([5]))

        session = create_session()
        session.add(u)
        session.flush()

        assert (u.counter == 5) is True


class PassiveDeletesTest(_base.MappedTest):
    __requires__ = ('foreign_keys',)

    @classmethod
    def define_tables(cls, metadata):
        Table('mytable', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', String(30)),
              test_needs_fk=True)

        Table('myothertable', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('parent_id', Integer),
              Column('data', String(30)),
              sa.ForeignKeyConstraint(['parent_id'],
                                      ['mytable.id'],
                                      ondelete="CASCADE"),
              test_needs_fk=True)

    @classmethod
    def setup_classes(cls):
        class MyClass(_base.BasicEntity):
            pass
        class MyOtherClass(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_basic(self):
        mapper(MyOtherClass, myothertable)
        mapper(MyClass, mytable, properties={
            'children':relationship(MyOtherClass,
                                passive_deletes=True,
                                cascade="all")})
        session = create_session()
        mc = MyClass()
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())

        session.add(mc)
        session.flush()
        session.expunge_all()

        assert myothertable.count().scalar() == 4
        mc = session.query(MyClass).get(mc.id)
        session.delete(mc)
        session.flush()

        assert mytable.count().scalar() == 0
        assert myothertable.count().scalar() == 0
    
    @testing.resolve_artifact_names
    def test_backwards_pd(self):
        # the unusual scenario where a trigger or something might be deleting
        # a many-to-one on deletion of the parent row
        mapper(MyOtherClass, myothertable, properties={
            'myclass':relationship(MyClass, cascade="all, delete", passive_deletes=True)
        })
        mapper(MyClass, mytable)
        
        session = create_session()
        mc = MyClass()
        mco = MyOtherClass()
        mco.myclass = mc
        session.add(mco)
        session.flush()

        assert mytable.count().scalar() == 1
        assert myothertable.count().scalar() == 1
        
        session.expire(mco, ['myclass'])
        session.delete(mco)
        session.flush()
        
        assert mytable.count().scalar() == 1
        assert myothertable.count().scalar() == 0
        
class ExtraPassiveDeletesTest(_base.MappedTest):
    __requires__ = ('foreign_keys',)

    @classmethod
    def define_tables(cls, metadata):
        Table('mytable', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('data', String(30)),
              test_needs_fk=True)

        Table('myothertable', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('parent_id', Integer),
              Column('data', String(30)),
              # no CASCADE, the same as ON DELETE RESTRICT
              sa.ForeignKeyConstraint(['parent_id'],
                                      ['mytable.id']),
              test_needs_fk=True)

    @classmethod
    def setup_classes(cls):
        class MyClass(_base.BasicEntity):
            pass
        class MyOtherClass(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_assertions(self):
        mapper(MyOtherClass, myothertable)
        try:
            mapper(MyClass, mytable, properties={
                'children':relationship(MyOtherClass,
                                    passive_deletes='all',
                                    cascade="all")})
            assert False
        except sa.exc.ArgumentError, e:
            eq_(str(e),
                "Can't set passive_deletes='all' in conjunction with 'delete' "
                "or 'delete-orphan' cascade")

    @testing.resolve_artifact_names
    def test_extra_passive(self):
        mapper(MyOtherClass, myothertable)
        mapper(MyClass, mytable, properties={
            'children': relationship(MyOtherClass,
                                 passive_deletes='all',
                                 cascade="save-update")})

        session = create_session()
        mc = MyClass()
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        mc.children.append(MyOtherClass())
        session.add(mc)
        session.flush()
        session.expunge_all()

        assert myothertable.count().scalar() == 4
        mc = session.query(MyClass).get(mc.id)
        session.delete(mc)
        assert_raises(sa.exc.DBAPIError, session.flush)

    @testing.resolve_artifact_names
    def test_extra_passive_2(self):
        mapper(MyOtherClass, myothertable)
        mapper(MyClass, mytable, properties={
            'children': relationship(MyOtherClass,
                                 passive_deletes='all',
                                 cascade="save-update")})

        session = create_session()
        mc = MyClass()
        mc.children.append(MyOtherClass())
        session.add(mc)
        session.flush()
        session.expunge_all()

        assert myothertable.count().scalar() == 1

        mc = session.query(MyClass).get(mc.id)
        session.delete(mc)
        mc.children[0].data = 'some new data'
        assert_raises(sa.exc.DBAPIError, session.flush)


class ColumnCollisionTest(_base.MappedTest):
    """Ensure the mapper doesn't break bind param naming rules on flush."""

    @classmethod
    def define_tables(cls, metadata):
        Table('book', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('book_id', String(50)),
            Column('title', String(50))
        )
    
    @testing.resolve_artifact_names
    def test_naming(self):
        class Book(_base.ComparableEntity):
            pass
    
        mapper(Book, book)
        sess = create_session()
        
        b1 = Book(book_id='abc', title='def')
        sess.add(b1)
        sess.flush()
        
        b1.title = 'ghi'
        sess.flush()
        sess.close()
        eq_(
            sess.query(Book).first(),
            Book(book_id='abc', title='ghi')
        )
        
        
        
class DefaultTest(_base.MappedTest):
    """Exercise mappings on columns with DefaultGenerators.

    Tests that when saving objects whose table contains DefaultGenerators,
    either python-side, preexec or database-side, the newly saved instances
    receive all the default values either through a post-fetch or getting the
    pre-exec'ed defaults back from the engine.

    """

    @classmethod
    def define_tables(cls, metadata):
        use_string_defaults = testing.against('postgresql', 'oracle', 'sqlite', 'mssql')

        if use_string_defaults:
            hohotype = String(30)
            hohoval = "im hoho"
            althohoval = "im different hoho"
        else:
            hohotype = Integer
            hohoval = 9
            althohoval = 15

        cls.other_artifacts['hohoval'] = hohoval
        cls.other_artifacts['althohoval'] = althohoval

        dt = Table('default_t', metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('hoho', hohotype, server_default=str(hohoval)),
            Column('counter', Integer, default=sa.func.char_length("1234567", type_=Integer)),
            Column('foober', String(30), default="im foober", onupdate="im the update"))

        st = Table('secondary_table', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('data', String(50)))

        if testing.against('postgresql', 'oracle'):
            dt.append_column(
                Column('secondary_id', Integer, sa.Sequence('sec_id_seq'),
                       unique=True))
            st.append_column(
                Column('fk_val', Integer,
                       ForeignKey('default_t.secondary_id')))
        elif testing.against('mssql'):
            st.append_column(
                Column('fk_val', Integer,
                       ForeignKey('default_t.id')))
        else:
            st.append_column(
                Column('hoho', hohotype, ForeignKey('default_t.hoho')))

    @classmethod
    def setup_classes(cls):
        class Hoho(_base.ComparableEntity):
            pass
        class Secondary(_base.ComparableEntity):
            pass

    @testing.fails_on('firebird', 'Data type unknown on the parameter')
    @testing.resolve_artifact_names
    def test_insert(self):
        mapper(Hoho, default_t)

        h1 = Hoho(hoho=althohoval)
        h2 = Hoho(counter=12)
        h3 = Hoho(hoho=althohoval, counter=12)
        h4 = Hoho()
        h5 = Hoho(foober='im the new foober')

        session = create_session(autocommit=False)
        session.add_all((h1, h2, h3, h4, h5))
        session.commit()

        eq_(h1.hoho, althohoval)
        eq_(h3.hoho, althohoval)

        def go():
            # test deferred load of attribues, one select per instance
            self.assert_(h2.hoho == h4.hoho == h5.hoho == hohoval)
        self.sql_count_(3, go)

        def go():
            self.assert_(h1.counter == h4.counter == h5.counter == 7)
        self.sql_count_(1, go)

        def go():
            self.assert_(h3.counter == h2.counter == 12)
            self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
            self.assert_(h5.foober == 'im the new foober')
        self.sql_count_(0, go)

        session.expunge_all()

        (h1, h2, h3, h4, h5) = session.query(Hoho).order_by(Hoho.id).all()

        eq_(h1.hoho, althohoval)
        eq_(h3.hoho, althohoval)
        self.assert_(h2.hoho == h4.hoho == h5.hoho == hohoval)
        self.assert_(h3.counter == h2.counter == 12)
        self.assert_(h1.counter ==  h4.counter == h5.counter == 7)
        self.assert_(h2.foober == h3.foober == h4.foober == 'im foober')
        eq_(h5.foober, 'im the new foober')

    @testing.fails_on('firebird', 'Data type unknown on the parameter')
    @testing.resolve_artifact_names
    def test_eager_defaults(self):
        mapper(Hoho, default_t, eager_defaults=True)

        h1 = Hoho()

        session = create_session()
        session.add(h1)
        session.flush()

        self.sql_count_(0, lambda: eq_(h1.hoho, hohoval))

    @testing.resolve_artifact_names
    def test_insert_nopostfetch(self):
        # populates from the FetchValues explicitly so there is no
        # "post-update"
        mapper(Hoho, default_t)

        h1 = Hoho(hoho="15", counter=15)
        session = create_session()
        session.add(h1)
        session.flush()

        def go():
            eq_(h1.hoho, "15")
            eq_(h1.counter, 15)
            eq_(h1.foober, "im foober")
        self.sql_count_(0, go)

    @testing.fails_on('firebird', 'Data type unknown on the parameter')
    @testing.resolve_artifact_names
    def test_update(self):
        mapper(Hoho, default_t)

        h1 = Hoho()
        session = create_session()
        session.add(h1)
        session.flush()

        eq_(h1.foober, 'im foober')
        h1.counter = 19
        session.flush()
        eq_(h1.foober, 'im the update')

    @testing.fails_on('firebird', 'Data type unknown on the parameter')
    @testing.resolve_artifact_names
    def test_used_in_relationship(self):
        """A server-side default can be used as the target of a foreign key"""

        mapper(Hoho, default_t, properties={
            'secondaries':relationship(Secondary, order_by=secondary_table.c.id)})
        mapper(Secondary, secondary_table)

        h1 = Hoho()
        s1 = Secondary(data='s1')
        h1.secondaries.append(s1)

        session = create_session()
        session.add(h1)
        session.flush()
        session.expunge_all()

        eq_(session.query(Hoho).get(h1.id),
            Hoho(hoho=hohoval,
                 secondaries=[
                   Secondary(data='s1')]))

        h1 = session.query(Hoho).get(h1.id)
        h1.secondaries.append(Secondary(data='s2'))
        session.flush()
        session.expunge_all()

        eq_(session.query(Hoho).get(h1.id),
            Hoho(hoho=hohoval,
                 secondaries=[
                    Secondary(data='s1'),
                    Secondary(data='s2')]))

class ColumnPropertyTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('data', metadata, 
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('a', String(50)),
            Column('b', String(50))
            )

        Table('subdata', metadata, 
            Column('id', Integer, ForeignKey('data.id'), primary_key=True),
            Column('c', String(50)),
            )
            
    @classmethod
    def setup_mappers(cls):
        class Data(_base.BasicEntity):
            pass
        
    @testing.resolve_artifact_names
    def test_refreshes(self):
        mapper(Data, data, properties={
            'aplusb':column_property(data.c.a + literal_column("' '") + data.c.b)
        })
        self._test()

    @testing.resolve_artifact_names
    def test_refreshes_post_init(self):
        m = mapper(Data, data)
        m.add_property('aplusb', column_property(data.c.a + literal_column("' '") + data.c.b))
        self._test()
    
    @testing.resolve_artifact_names
    def test_with_inheritance(self):
        class SubData(Data):
            pass
        mapper(Data, data, properties={
            'aplusb':column_property(data.c.a + literal_column("' '") + data.c.b)
        })
        mapper(SubData, subdata, inherits=Data)
        
        sess = create_session()
        sd1 = SubData(a="hello", b="there", c="hi")
        sess.add(sd1)
        sess.flush()
        eq_(sd1.aplusb, "hello there")
        
    @testing.resolve_artifact_names
    def _test(self):
        sess = create_session()
        
        d1 = Data(a="hello", b="there")
        sess.add(d1)
        sess.flush()
        
        eq_(d1.aplusb, "hello there")
        
        d1.b = "bye"
        sess.flush()
        eq_(d1.aplusb, "hello bye")
        
        d1.b = 'foobar'
        d1.aplusb = 'im setting this explicitly'
        sess.flush()
        eq_(d1.aplusb, "im setting this explicitly")
    
class OneToManyTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_one_to_many_1(self):
        """Basic save of one to many."""

        m = mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='select')
        ))
        u = User(name= 'one2manytester')
        a = Address(email_address='one2many@test.org')
        u.addresses.append(a)

        a2 = Address(email_address='lala@test.org')
        u.addresses.append(a2)

        session = create_session()
        session.add(u)
        session.flush()

        user_rows = users.select(users.c.id.in_([u.id])).execute().fetchall()
        eq_(user_rows[0].values(), [u.id, 'one2manytester'])

        address_rows = addresses.select(
            addresses.c.id.in_([a.id, a2.id]),
            order_by=[addresses.c.email_address]).execute().fetchall()
        eq_(address_rows[0].values(), [a2.id, u.id, 'lala@test.org'])
        eq_(address_rows[1].values(), [a.id, u.id, 'one2many@test.org'])

        userid = u.id
        addressid = a2.id

        a2.email_address = 'somethingnew@foo.com'

        session.flush()

        address_rows = addresses.select(
            addresses.c.id == addressid).execute().fetchall()
        eq_(address_rows[0].values(),
            [addressid, userid, 'somethingnew@foo.com'])
        self.assert_(u.id == userid and a2.id == addressid)

    @testing.resolve_artifact_names
    def test_one_to_many_2(self):
        """Modifying the child items of an object."""

        m = mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='select')))

        u1 = User(name='user1')
        u1.addresses = []
        a1 = Address(email_address='emailaddress1')
        u1.addresses.append(a1)

        u2 = User(name='user2')
        u2.addresses = []
        a2 = Address(email_address='emailaddress2')
        u2.addresses.append(a2)

        a3 = Address(email_address='emailaddress3')

        session = create_session()
        session.add_all((u1, u2, a3))
        session.flush()

        # modify user2 directly, append an address to user1.
        # upon commit, user2 should be updated, user1 should not
        # both address1 and address3 should be updated
        u2.name = 'user2modified'
        u1.addresses.append(a3)
        del u1.addresses[0]

        self.assert_sql(testing.db, session.flush, [
            ("UPDATE users SET name=:name "
             "WHERE users.id = :users_id",
             {'users_id': u2.id, 'name': 'user2modified'}),

            ("UPDATE addresses SET user_id=:user_id "
             "WHERE addresses.id = :addresses_id",
             {'user_id': None, 'addresses_id': a1.id}),

            ("UPDATE addresses SET user_id=:user_id "
             "WHERE addresses.id = :addresses_id",
             {'user_id': u1.id, 'addresses_id': a3.id})])

    @testing.resolve_artifact_names
    def test_child_move(self):
        """Moving a child from one parent to another, with a delete.

        Tests that deleting the first parent properly updates the child with
        the new parent.  This tests the 'trackparent' option in the attributes
        module.

        """
        m = mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='select')))

        u1 = User(name='user1')
        u2 = User(name='user2')
        a = Address(email_address='address1')
        u1.addresses.append(a)

        session = create_session()
        session.add_all((u1, u2))
        session.flush()

        del u1.addresses[0]
        u2.addresses.append(a)
        session.delete(u1)

        session.flush()
        session.expunge_all()

        u2 = session.query(User).get(u2.id)
        eq_(len(u2.addresses), 1)

    @testing.resolve_artifact_names
    def test_child_move_2(self):
        m = mapper(User, users, properties=dict(
            addresses = relationship(mapper(Address, addresses), lazy='select')))

        u1 = User(name='user1')
        u2 = User(name='user2')
        a = Address(email_address='address1')
        u1.addresses.append(a)

        session = create_session()
        session.add_all((u1, u2))
        session.flush()

        del u1.addresses[0]
        u2.addresses.append(a)

        session.flush()
        session.expunge_all()

        u2 = session.query(User).get(u2.id)
        eq_(len(u2.addresses), 1)

    @testing.resolve_artifact_names
    def test_o2m_delete_parent(self):
        m = mapper(User, users, properties=dict(
            address = relationship(mapper(Address, addresses),
                               lazy='select',
                               uselist=False)))

        u = User(name='one2onetester')
        a = Address(email_address='myonlyaddress@foo.com')
        u.address = a

        session = create_session()
        session.add(u)
        session.flush()

        session.delete(u)
        session.flush()

        assert a.id is not None
        assert a.user_id is None
        assert sa.orm.attributes.instance_state(a).key in session.identity_map
        assert sa.orm.attributes.instance_state(u).key not in session.identity_map

    @testing.resolve_artifact_names
    def test_one_to_one(self):
        m = mapper(User, users, properties=dict(
            address = relationship(mapper(Address, addresses),
                               lazy='select',
                               uselist=False)))

        u = User(name='one2onetester')
        u.address = Address(email_address='myonlyaddress@foo.com')

        session = create_session()
        session.add(u)
        session.flush()

        u.name = 'imnew'
        session.flush()

        u.address.email_address = 'imnew@foo.com'
        session.flush()

    @testing.resolve_artifact_names
    def test_bidirectional(self):
        m1 = mapper(User, users)
        m2 = mapper(Address, addresses, properties=dict(
            user = relationship(m1, lazy='joined', backref='addresses')))


        u = User(name='test')
        a = Address(email_address='testaddress', user=u)

        session = create_session()
        session.add(u)
        session.flush()
        session.delete(u)
        session.flush()

    @testing.resolve_artifact_names
    def test_double_relationship(self):
        m2 = mapper(Address, addresses)
        m = mapper(User, users, properties={
            'boston_addresses' : relationship(m2, primaryjoin=
                        sa.and_(users.c.id==addresses.c.user_id,
                                addresses.c.email_address.like('%boston%'))),
            'newyork_addresses' : relationship(m2, primaryjoin=
                        sa.and_(users.c.id==addresses.c.user_id,
                                addresses.c.email_address.like('%newyork%')))})

        u = User(name='u1')
        a = Address(email_address='foo@boston.com')
        b = Address(email_address='bar@newyork.com')
        u.boston_addresses.append(a)
        u.newyork_addresses.append(b)

        session = create_session()
        session.add(u)
        session.flush()

class SaveTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_basic(self):
        m = mapper(User, users)

        # save two users
        u = User(name='savetester')
        u2 = User(name='savetester2')

        session = create_session()
        session.add_all((u, u2))
        session.flush()

        # assert the first one retreives the same from the identity map
        nu = session.query(m).get(u.id)
        assert u is nu

        # clear out the identity map, so next get forces a SELECT
        session.expunge_all()

        # check it again, identity should be different but ids the same
        nu = session.query(m).get(u.id)
        assert u is not nu and u.id == nu.id and nu.name == 'savetester'

        # change first users name and save
        session = create_session()
        session.add(u)
        u.name = 'modifiedname'
        assert u in session.dirty
        session.flush()

        # select both
        userlist = session.query(User).filter(
            users.c.id.in_([u.id, u2.id])).order_by(users.c.name).all()

        eq_(u.id, userlist[0].id)
        eq_(userlist[0].name, 'modifiedname')
        eq_(u2.id, userlist[1].id)
        eq_(userlist[1].name, 'savetester2')

    @testing.resolve_artifact_names
    def test_synonym(self):
        class SUser(_base.BasicEntity):
            def _get_name(self):
                return "User:" + self.name
            def _set_name(self, name):
                self.name = name + ":User"
            syn_name = property(_get_name, _set_name)

        mapper(SUser, users, properties={
            'syn_name': sa.orm.synonym('name')
        })

        u = SUser(syn_name="some name")
        eq_(u.syn_name, 'User:some name:User')

        session = create_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        u = session.query(SUser).first()
        eq_(u.syn_name, 'User:some name:User')

    @testing.resolve_artifact_names
    def test_lazyattr_commit(self):
        """Lazily loaded relationships.

        When a lazy-loaded list is unloaded, and a commit occurs, that the
        'passive' call on that list does not blow away its value

        """
        mapper(User, users, properties = {
            'addresses': relationship(mapper(Address, addresses))})

        u = User(name='u1')
        u.addresses.append(Address(email_address='u1@e1'))
        u.addresses.append(Address(email_address='u1@e2'))
        u.addresses.append(Address(email_address='u1@e3'))
        u.addresses.append(Address(email_address='u1@e4'))

        session = create_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        u = session.query(User).one()
        u.name = 'newname'
        session.flush()
        eq_(len(u.addresses), 4)

    @testing.resolve_artifact_names
    def test_inherits(self):
        m1 = mapper(User, users)

        class AddressUser(User):
            """a user object that also has the users mailing address."""
            pass

        # define a mapper for AddressUser that inherits the User.mapper, and
        # joins on the id column
        mapper(AddressUser, addresses, inherits=m1)

        au = AddressUser(name='u', email_address='u@e')

        session = create_session()
        session.add(au)
        session.flush()
        session.expunge_all()

        rt = session.query(AddressUser).one()
        eq_(au.user_id, rt.user_id)
        eq_(rt.id, rt.id)

    @testing.resolve_artifact_names
    def test_deferred(self):
        """Deferred column operations"""

        mapper(Order, orders, properties={
            'description': sa.orm.deferred(orders.c.description)})

        # dont set deferred attribute, commit session
        o = Order(id=42)
        session = create_session(autocommit=False)
        session.add(o)
        session.commit()

        # assert that changes get picked up
        o.description = 'foo'
        session.commit()

        eq_(list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, 'foo', None)])
        session.expunge_all()

        # assert that a set operation doesn't trigger a load operation
        o = session.query(Order).filter(Order.description == 'foo').one()
        def go():
            o.description = 'hoho'
        self.sql_count_(0, go)
        session.flush()

        eq_(list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, 'hoho', None)])

        session.expunge_all()

        # test assigning None to an unloaded deferred also works
        o = session.query(Order).filter(Order.description == 'hoho').one()
        o.description = None
        session.flush()
        eq_(list(session.execute(orders.select(), mapper=Order)),
            [(42, None, None, None, None)])
        session.close()

    # why no support on oracle ?  because oracle doesn't save
    # "blank" strings; it saves a single space character.
    @testing.fails_on('oracle', 'FIXME: unknown')
    @testing.resolve_artifact_names
    def test_dont_update_blanks(self):
        mapper(User, users)

        u = User(name='')
        session = create_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        u = session.query(User).get(u.id)
        u.name = ''
        self.sql_count_(0, session.flush)

    @testing.resolve_artifact_names
    def test_multi_table_selectable(self):
        """Mapped selectables that span tables.

        Also tests redefinition of the keynames for the column properties.

        """
        usersaddresses = sa.join(users, addresses,
                                 users.c.id == addresses.c.user_id)

        m = mapper(User, usersaddresses,
            properties=dict(
                email = addresses.c.email_address,
                foo_id = [users.c.id, addresses.c.user_id]))

        u = User(name='multitester', email='multi@test.org')
        session = create_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        id = m.primary_key_from_instance(u)

        u = session.query(User).get(id)
        assert u.name == 'multitester'

        user_rows = users.select(users.c.id.in_([u.foo_id])).execute().fetchall()
        eq_(user_rows[0].values(), [u.foo_id, 'multitester'])
        address_rows = addresses.select(addresses.c.id.in_([u.id])).execute().fetchall()
        eq_(address_rows[0].values(), [u.id, u.foo_id, 'multi@test.org'])

        u.email = 'lala@hey.com'
        u.name = 'imnew'
        session.flush()

        user_rows = users.select(users.c.id.in_([u.foo_id])).execute().fetchall()
        eq_(user_rows[0].values(), [u.foo_id, 'imnew'])
        address_rows = addresses.select(addresses.c.id.in_([u.id])).execute().fetchall()
        eq_(address_rows[0].values(), [u.id, u.foo_id, 'lala@hey.com'])

        session.expunge_all()
        u = session.query(User).get(id)
        assert u.name == 'imnew'

    @testing.resolve_artifact_names
    def test_history_get(self):
        """The history lazy-fetches data when it wasn't otherwise loaded."""
        mapper(User, users, properties={
            'addresses':relationship(Address, cascade="all, delete-orphan")})
        mapper(Address, addresses)

        u = User(name='u1')
        u.addresses.append(Address(email_address='u1@e1'))
        u.addresses.append(Address(email_address='u1@e2'))
        session = create_session()
        session.add(u)
        session.flush()
        session.expunge_all()

        u = session.query(User).get(u.id)
        session.delete(u)
        session.flush()
        assert users.count().scalar() == 0
        assert addresses.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_batch_mode(self):
        """The 'batch=False' flag on mapper()"""

        names = []
        class TestExtension(sa.orm.MapperExtension):
            def before_insert(self, mapper, connection, instance):
                self.current_instance = instance
                names.append(instance.name)
            def after_insert(self, mapper, connection, instance):
                assert instance is self.current_instance

        mapper(User, users, extension=TestExtension(), batch=False)
        u1 = User(name='user1')
        u2 = User(name='user2')

        session = create_session()
        session.add_all((u1, u2))
        session.flush()
        
        u3 = User(name='user3')
        u4 = User(name='user4')
        u5 = User(name='user5')
        
        session.add_all([u4, u5, u3])
        session.flush()
        
        # test insert ordering is maintained
        assert names == ['user1', 'user2', 'user4', 'user5', 'user3']
        session.expunge_all()
        
        sa.orm.clear_mappers()

        m = mapper(User, users, extension=TestExtension())
        u1 = User(name='user1')
        u2 = User(name='user2')
        session.add_all((u1, u2))
        assert_raises(AssertionError, session.flush)


class ManyToOneTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_m2o_one_to_one(self):
        # TODO: put assertion in here !!!
        m = mapper(Address, addresses, properties=dict(
            user = relationship(mapper(User, users), lazy='select', uselist=False)))

        session = create_session()

        data = [
            {'name': 'thesub' ,  'email_address': 'bar@foo.com'},
            {'name': 'assdkfj' , 'email_address': 'thesdf@asdf.com'},
            {'name': 'n4knd' ,   'email_address': 'asf3@bar.org'},
            {'name': 'v88f4' ,   'email_address': 'adsd5@llala.net'},
            {'name': 'asdf8d' ,  'email_address': 'theater@foo.com'}
        ]
        objects = []
        for elem in data:
            a = Address()
            a.email_address = elem['email_address']
            a.user = User()
            a.user.name = elem['name']
            objects.append(a)
            session.add(a)

        session.flush()

        objects[2].email_address = 'imnew@foo.bar'
        objects[3].user = User()
        objects[3].user.name = 'imnewlyadded'
        self.assert_sql_execution(testing.db,
                        session.flush,
                        CompiledSQL("INSERT INTO users (name) VALUES (:name)",
                         {'name': 'imnewlyadded'} ),

                         AllOf(
                            CompiledSQL("UPDATE addresses SET email_address=:email_address "
                                        "WHERE addresses.id = :addresses_id",
                                        lambda ctx: {'email_address': 'imnew@foo.bar',
                                          'addresses_id': objects[2].id}),
                            CompiledSQL("UPDATE addresses SET user_id=:user_id "
                                          "WHERE addresses.id = :addresses_id",
                                          lambda ctx: {'user_id': objects[3].user.id,
                                                       'addresses_id': objects[3].id})
                        )
                    )

        l = sa.select([users, addresses],
                      sa.and_(users.c.id==addresses.c.user_id,
                              addresses.c.id==a.id)).execute()
        eq_(l.first().values(),
            [a.user.id, 'asdf8d', a.id, a.user_id, 'theater@foo.com'])

    @testing.resolve_artifact_names
    def test_many_to_one_1(self):
        m = mapper(Address, addresses, properties=dict(
            user = relationship(mapper(User, users), lazy='select')))

        a1 = Address(email_address='emailaddress1')
        u1 = User(name='user1')
        a1.user = u1

        session = create_session()
        session.add(a1)
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is u1

        a1.user = None
        session.flush()
        session.expunge_all()
        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is None

    @testing.resolve_artifact_names
    def test_many_to_one_2(self):
        m = mapper(Address, addresses, properties=dict(
            user = relationship(mapper(User, users), lazy='select')))

        a1 = Address(email_address='emailaddress1')
        a2 = Address(email_address='emailaddress2')
        u1 = User(name='user1')
        a1.user = u1

        session = create_session()
        session.add_all((a1, a2))
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)
        a2 = session.query(Address).get(a2.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is u1

        a1.user = None
        a2.user = u1
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)
        a2 = session.query(Address).get(a2.id)
        u1 = session.query(User).get(u1.id)
        assert a1.user is None
        assert a2.user is u1

    @testing.resolve_artifact_names
    def test_many_to_one_3(self):
        m = mapper(Address, addresses, properties=dict(
            user = relationship(mapper(User, users), lazy='select')))

        a1 = Address(email_address='emailaddress1')
        u1 = User(name='user1')
        u2 = User(name='user2')
        a1.user = u1

        session = create_session()
        session.add_all((a1, u1, u2))
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        u2 = session.query(User).get(u2.id)
        assert a1.user is u1

        a1.user = u2
        session.flush()
        session.expunge_all()
        a1 = session.query(Address).get(a1.id)
        u1 = session.query(User).get(u1.id)
        u2 = session.query(User).get(u2.id)
        assert a1.user is u2

    @testing.resolve_artifact_names
    def test_bidirectional_no_load(self):
        mapper(User, users, properties={
            'addresses':relationship(Address, backref='user', lazy='noload')})
        mapper(Address, addresses)

        # try it on unsaved objects
        u1 = User(name='u1')
        a1 = Address(email_address='e1')
        a1.user = u1

        session = create_session()
        session.add(u1)
        session.flush()
        session.expunge_all()

        a1 = session.query(Address).get(a1.id)

        a1.user = None
        session.flush()
        session.expunge_all()
        assert session.query(Address).get(a1.id).user is None
        assert session.query(User).get(u1.id).addresses == []


class ManyToManyTest(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_many_to_many(self):
        mapper(Keyword, keywords)

        m = mapper(Item, items, properties=dict(
                keywords=relationship(Keyword,
                                  item_keywords,
                                  lazy='joined',
                                  order_by=keywords.c.name)))

        data = [Item,
            {'description': 'mm_item1',
             'keywords' : (Keyword, [{'name': 'big'},
                                     {'name': 'green'},
                                     {'name': 'purple'},
                                     {'name': 'round'}])},
            {'description': 'mm_item2',
             'keywords' : (Keyword, [{'name':'blue'},
                                     {'name':'imnew'},
                                     {'name':'round'},
                                     {'name':'small'}])},
            {'description': 'mm_item3',
             'keywords' : (Keyword, [])},
            {'description': 'mm_item4',
             'keywords' : (Keyword, [{'name':'big'},
                                    {'name':'blue'},])},
            {'description': 'mm_item5',
             'keywords' : (Keyword, [{'name':'big'},
                                     {'name':'exacting'},
                                     {'name':'green'}])},
            {'description': 'mm_item6',
             'keywords' : (Keyword, [{'name':'red'},
                                     {'name':'round'},
                                     {'name':'small'}])}]

        session = create_session()

        objects = []
        _keywords = dict([(k.name, k) for k in session.query(Keyword)])

        for elem in data[1:]:
            item = Item(description=elem['description'])
            objects.append(item)

            for spec in elem['keywords'][1]:
                keyword_name = spec['name']
                try:
                    kw = _keywords[keyword_name]
                except KeyError:
                    _keywords[keyword_name] = kw = Keyword(name=keyword_name)
                item.keywords.append(kw)

        session.add_all(objects)
        session.flush()

        l = (session.query(Item).
             filter(Item.description.in_([e['description']
                                          for e in data[1:]])).
             order_by(Item.description).all())
        self.assert_result(l, *data)

        objects[4].description = 'item4updated'
        k = Keyword()
        k.name = 'yellow'
        objects[5].keywords.append(k)
        self.assert_sql_execution(
            testing.db, 
            session.flush, 
            AllOf(
                CompiledSQL("UPDATE items SET description=:description "
                 "WHERE items.id = :items_id",
                     {'description': 'item4updated',
                      'items_id': objects[4].id},
                ),
                CompiledSQL("INSERT INTO keywords (name) "
                    "VALUES (:name)",
                    {'name': 'yellow'},
                )
            ),
            CompiledSQL("INSERT INTO item_keywords (item_id, keyword_id) "
                    "VALUES (:item_id, :keyword_id)",
                     lambda ctx: [{'item_id': objects[5].id,
                                   'keyword_id': k.id}])
            )

        objects[2].keywords.append(k)
        dkid = objects[5].keywords[1].id
        del objects[5].keywords[1]
        self.assert_sql_execution(
            testing.db, 
            session.flush, 
            CompiledSQL("DELETE FROM item_keywords "
                     "WHERE item_keywords.item_id = :item_id AND "
                     "item_keywords.keyword_id = :keyword_id",
                     [{'item_id': objects[5].id, 'keyword_id': dkid}]),
            CompiledSQL("INSERT INTO item_keywords (item_id, keyword_id) "
                    "VALUES (:item_id, :keyword_id)",
                    lambda ctx: [{'item_id': objects[2].id, 'keyword_id': k.id}]
             ))

        session.delete(objects[3])
        session.flush()

    @testing.resolve_artifact_names
    def test_many_to_many_remove(self):
        """Setting a collection to empty deletes many-to-many rows.

        Tests that setting a list-based attribute to '[]' properly affects the
        history and allows the many-to-many rows to be deleted

        """
        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords = relationship(Keyword, item_keywords, lazy='joined'),
            ))

        i = Item(description='i1')
        k1 = Keyword(name='k1')
        k2 = Keyword(name='k2')
        i.keywords.append(k1)
        i.keywords.append(k2)

        session = create_session()
        session.add(i)
        session.flush()

        assert item_keywords.count().scalar() == 2
        i.keywords = []
        session.flush()
        assert item_keywords.count().scalar() == 0

    @testing.resolve_artifact_names
    def test_scalar(self):
        """sa.dependency won't delete an m2m relationship referencing None."""

        mapper(Keyword, keywords)

        mapper(Item, items, properties=dict(
            keyword=relationship(Keyword, secondary=item_keywords, uselist=False)))

        i = Item(description='x')
        session = create_session()
        session.add(i)
        session.flush()
        session.delete(i)
        session.flush()

    @testing.resolve_artifact_names
    def test_many_to_many_update(self):
        """Assorted history operations on a many to many"""
        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
            keywords=relationship(Keyword,
                              secondary=item_keywords,
                              lazy='joined',
                              order_by=keywords.c.name)))

        k1 = Keyword(name='keyword 1')
        k2 = Keyword(name='keyword 2')
        k3 = Keyword(name='keyword 3')

        item = Item(description='item 1')
        item.keywords.extend([k1, k2, k3])

        session = create_session()
        session.add(item)
        session.flush()

        item.keywords = []
        item.keywords.append(k1)
        item.keywords.append(k2)
        session.flush()

        session.expunge_all()
        item = session.query(Item).get(item.id)
        assert item.keywords == [k1, k2]

    @testing.resolve_artifact_names
    def test_association(self):
        """Basic test of an association object"""

        class IKAssociation(_base.ComparableEntity):
            pass

        mapper(Keyword, keywords)

        # note that we are breaking a rule here, making a second
        # mapper(Keyword, keywords) the reorganization of mapper construction
        # affected this, but was fixed again

        mapper(IKAssociation, item_keywords,
               primary_key=[item_keywords.c.item_id, item_keywords.c.keyword_id],
               properties=dict(
                 keyword=relationship(mapper(Keyword, keywords, non_primary=True),
                                  lazy='joined',
                                  uselist=False,
                                  order_by=keywords.c.name      # note here is a valid place where order_by can be used
                                  )))                           # on a scalar relationship(); to determine eager ordering of
                                                                # the parent object within its collection.

        mapper(Item, items, properties=dict(
            keywords=relationship(IKAssociation, lazy='joined')))

        session = create_session()

        def fixture():
            _kw = dict([(k.name, k) for k in session.query(Keyword)])
            for n in ('big', 'green', 'purple', 'round', 'huge',
                      'violet', 'yellow', 'blue'):
                if n not in _kw:
                    _kw[n] = Keyword(name=n)

            def assocs(*names):
                return [IKAssociation(keyword=kw)
                        for kw in [_kw[n] for n in names]]

            return [
                Item(description='a_item1',
                     keywords=assocs('big', 'green', 'purple', 'round')),
                Item(description='a_item2',
                     keywords=assocs('huge', 'violet', 'yellow')),
                Item(description='a_item3',
                     keywords=assocs('big', 'blue'))]

        session.add_all(fixture())
        session.flush()
        eq_(fixture(), session.query(Item).order_by(Item.description).all())


class SaveTest2(_fixtures.FixtureTest):
    run_inserts = None

    @testing.resolve_artifact_names
    def test_m2o_nonmatch(self):
        mapper(User, users)
        mapper(Address, addresses, properties=dict(
            user = relationship(User, lazy='select', uselist=False)))

        session = create_session()

        def fixture():
            return [
                Address(email_address='a1', user=User(name='u1')),
                Address(email_address='a2', user=User(name='u2'))]

        session.add_all(fixture())

        self.assert_sql_execution(
            testing.db, 
            session.flush, 
            CompiledSQL("INSERT INTO users (name) VALUES (:name)",
             {'name': 'u1'}),
            CompiledSQL("INSERT INTO users (name) VALUES (:name)",
             {'name': 'u2'}),
            CompiledSQL("INSERT INTO addresses (user_id, email_address) "
             "VALUES (:user_id, :email_address)",
             {'user_id': 1, 'email_address': 'a1'}),
            CompiledSQL("INSERT INTO addresses (user_id, email_address) "
             "VALUES (:user_id, :email_address)",
             {'user_id': 2, 'email_address': 'a2'}),
        )

class SaveTest3(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('items', metadata,
              Column('item_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('item_name', String(50)))

        Table('keywords', metadata,
              Column('keyword_id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('name', String(50)))

        Table('assoc', metadata,
              Column('item_id', Integer, ForeignKey("items")),
              Column('keyword_id', Integer, ForeignKey("keywords")),
              Column('foo', sa.Boolean, default=True))

    @classmethod
    def setup_classes(cls):
        class Keyword(_base.BasicEntity):
            pass
        class Item(_base.BasicEntity):
            pass

    @testing.resolve_artifact_names
    def test_manytomany_xtracol_delete(self):
        """A many-to-many on a table that has an extra column can properly delete rows from the table without referencing the extra column"""

        mapper(Keyword, keywords)
        mapper(Item, items, properties=dict(
                keywords = relationship(Keyword, secondary=assoc, lazy='joined'),))

        i = Item()
        k1 = Keyword()
        k2 = Keyword()
        i.keywords.append(k1)
        i.keywords.append(k2)

        session = create_session()
        session.add(i)
        session.flush()

        assert assoc.count().scalar() == 2
        i.keywords = []
        print i.keywords
        session.flush()
        assert assoc.count().scalar() == 0

class BooleanColTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('t1_t', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(30)),
            Column('value', sa.Boolean))

    @testing.resolve_artifact_names
    def test_boolean(self):
        # use the regular mapper
        class T(_base.ComparableEntity):
            pass
        orm_mapper(T, t1_t, order_by=t1_t.c.id)

        sess = create_session()
        t1 = T(value=True, name="t1")
        t2 = T(value=False, name="t2")
        t3 = T(value=True, name="t3")
        sess.add_all((t1, t2, t3))

        sess.flush()

        for clear in (False, True):
            if clear:
                sess.expunge_all()
            eq_(sess.query(T).all(), [T(value=True, name="t1"), T(value=False, name="t2"), T(value=True, name="t3")])
            if clear:
                sess.expunge_all()
            eq_(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"),T(value=True, name="t3")])
            if clear:
                sess.expunge_all()
            eq_(sess.query(T).filter(T.value==False).all(), [T(value=False, name="t2")])

        t2 = sess.query(T).get(t2.id)
        t2.value = True
        sess.flush()
        eq_(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"), T(value=True, name="t2"), T(value=True, name="t3")])
        t2.value = False
        sess.flush()
        eq_(sess.query(T).filter(T.value==True).all(), [T(value=True, name="t1"),T(value=True, name="t3")])


class RowSwitchTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        # parent
        Table('t5', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False))

        # onetomany
        Table('t6', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False),
            Column('t5id', Integer, ForeignKey('t5.id'),nullable=False))

        # associated
        Table('t7', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', String(30), nullable=False))

        #manytomany
        Table('t5t7', metadata,
            Column('t5id', Integer, ForeignKey('t5.id'),nullable=False),
            Column('t7id', Integer, ForeignKey('t7.id'),nullable=False))

    @classmethod
    def setup_classes(cls):
        class T5(_base.ComparableEntity):
            pass

        class T6(_base.ComparableEntity):
            pass

        class T7(_base.ComparableEntity):
            pass

    @testing.resolve_artifact_names
    def test_onetomany(self):
        mapper(T5, t5, properties={
            't6s':relationship(T6, cascade="all, delete-orphan")
        })
        mapper(T6, t6)

        sess = create_session()

        o5 = T5(data='some t5', id=1)
        o5.t6s.append(T6(data='some t6', id=1))
        o5.t6s.append(T6(data='some other t6', id=2))

        sess.add(o5)
        sess.flush()

        eq_(
            list(sess.execute(t5.select(), mapper=T5)),
            [(1, 'some t5')]
        )
        eq_(
            list(sess.execute(t6.select().order_by(t6.c.id), mapper=T5)),
            [(1, 'some t6', 1), (2, 'some other t6', 1)]
        )

        o6 = T5(data='some other t5', id=o5.id, t6s=[
            T6(data='third t6', id=3),
            T6(data='fourth t6', id=4),
            ])
        sess.delete(o5)
        sess.add(o6)
        sess.flush()

        eq_(
            list(sess.execute(t5.select(), mapper=T5)),
            [(1, 'some other t5')]
        )
        eq_(
            list(sess.execute(t6.select().order_by(t6.c.id), mapper=T5)),
            [(3, 'third t6', 1), (4, 'fourth t6', 1)]
        )

    @testing.resolve_artifact_names
    def test_manytomany(self):
        mapper(T5, t5, properties={
            't7s':relationship(T7, secondary=t5t7, cascade="all")
        })
        mapper(T7, t7)

        sess = create_session()

        o5 = T5(data='some t5', id=1)
        o5.t7s.append(T7(data='some t7', id=1))
        o5.t7s.append(T7(data='some other t7', id=2))

        sess.add(o5)
        sess.flush()

        assert list(sess.execute(t5.select(), mapper=T5)) == [(1, 'some t5')]
        assert testing.rowset(sess.execute(t5t7.select(), mapper=T5)) == set([(1,1), (1, 2)])
        assert list(sess.execute(t7.select(), mapper=T5)) == [(1, 'some t7'), (2, 'some other t7')]

        o6 = T5(data='some other t5', id=1, t7s=[
            T7(data='third t7', id=3),
            T7(data='fourth t7', id=4),
            ])

        sess.delete(o5)
        assert o5 in sess.deleted
        assert o5.t7s[0] in sess.deleted
        assert o5.t7s[1] in sess.deleted
        
        sess.add(o6)
        sess.flush()

        assert list(sess.execute(t5.select(), mapper=T5)) == [(1, 'some other t5')]
        assert list(sess.execute(t7.select(), mapper=T5)) == [(3, 'third t7'), (4, 'fourth t7')]

    @testing.resolve_artifact_names
    def test_manytoone(self):

        mapper(T6, t6, properties={
            't5':relationship(T5)
        })
        mapper(T5, t5)

        sess = create_session()

        o5 = T6(data='some t6', id=1)
        o5.t5 = T5(data='some t5', id=1)

        sess.add(o5)
        sess.flush()

        assert list(sess.execute(t5.select(), mapper=T5)) == [(1, 'some t5')]
        assert list(sess.execute(t6.select(), mapper=T5)) == [(1, 'some t6', 1)]

        o6 = T6(data='some other t6', id=1, t5=T5(data='some other t5', id=2))
        sess.delete(o5)
        sess.delete(o5.t5)
        sess.add(o6)
        sess.flush()

        assert list(sess.execute(t5.select(), mapper=T5)) == [(2, 'some other t5')]
        assert list(sess.execute(t6.select(), mapper=T5)) == [(1, 'some other t6', 2)]

class InheritingRowSwitchTest(_base.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('parent', metadata,
            Column('id', Integer, primary_key=True),
            Column('pdata', String(30))
        )
        Table('child', metadata,
            Column('id', Integer, primary_key=True),
            Column('pid', Integer, ForeignKey('parent.id')),
            Column('cdata', String(30))
        )

    @classmethod
    def setup_classes(cls):
        class P(_base.ComparableEntity):
            pass

        class C(P):
            pass
    
    @testing.resolve_artifact_names
    def test_row_switch_no_child_table(self):
        mapper(P, parent)
        mapper(C, child, inherits=P)
        
        sess = create_session()
        c1 = C(id=1, pdata='c1', cdata='c1')
        sess.add(c1)
        sess.flush()
        
        # establish a row switch between c1 and c2.
        # c2 has no value for the "child" table
        c2 = C(id=1, pdata='c2')
        sess.add(c2)
        sess.delete(c1)

        self.assert_sql_execution(testing.db, sess.flush,
            CompiledSQL("UPDATE parent SET pdata=:pdata WHERE parent.id = :parent_id",
                {'pdata':'c2', 'parent_id':1}
            ),
            
            # this fires as of [ticket:1362], since we synchronzize
            # PK/FKs on UPDATES.  c2 is new so the history shows up as
            # pure added, update occurs.  If a future change limits the
            # sync operation during _save_obj().update, this is safe to remove again.
            CompiledSQL("UPDATE child SET pid=:pid WHERE child.id = :child_id",
                {'pid':1, 'child_id':1}
            )
        )
        
        

class TransactionTest(_base.MappedTest):
    __requires__ = ('deferrable_constraints',)

    __whitelist__ = ('sqlite',)
    # sqlite doesn't have deferrable constraints, but it allows them to
    # be specified.  it'll raise immediately post-INSERT, instead of at
    # COMMIT. either way, this test should pass.

    @classmethod
    def define_tables(cls, metadata):
        t1 = Table('t1', metadata,
            Column('id', Integer, primary_key=True))

        t2 = Table('t2', metadata,
            Column('id', Integer, primary_key=True),
            Column('t1_id', Integer,
                   ForeignKey('t1.id', deferrable=True, initially='deferred')
                   ))
    @classmethod
    def setup_classes(cls):
        class T1(_base.ComparableEntity):
            pass

        class T2(_base.ComparableEntity):
            pass

    @classmethod
    @testing.resolve_artifact_names
    def setup_mappers(cls):
        orm_mapper(T1, t1)
        orm_mapper(T2, t2)

    @testing.resolve_artifact_names
    def test_close_transaction_on_commit_fail(self):
        session = create_session(autocommit=True)

        # with a deferred constraint, this fails at COMMIT time instead
        # of at INSERT time.
        session.add(T2(t1_id=123))

        try:
            session.flush()
            assert False
        except:
            # Flush needs to rollback also when commit fails
            assert session.transaction is None

        # todo: on 8.3 at least, the failed commit seems to close the cursor?
        # needs investigation.  leaving in the DDL above now to help verify
        # that the new deferrable support on FK isn't involved in this issue.
        if testing.against('postgresql'):
            t1.bind.engine.dispose()

