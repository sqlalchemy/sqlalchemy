import testenv; testenv.configure_for_tests()

from sqlalchemy.ext import declarative as decl
from sqlalchemy import exc
from testlib import sa, testing
from testlib.sa import MetaData, Table, Column, Integer, String, ForeignKey, ForeignKeyConstraint, asc
from testlib.sa.orm import relation, create_session, class_mapper, eagerload, compile_mappers, backref
from testlib.testing import eq_
from orm._base import ComparableEntity


class DeclarativeTest(testing.TestBase, testing.AssertsExecutionResults):
    def setUp(self):
        global Base
        Base = decl.declarative_base(testing.db)

    def tearDown(self):
        Base.metadata.drop_all()

    def test_basic(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'

            id = Column(Integer, primary_key=True)
            email = Column(String(50), key='_email')
            user_id = Column('user_id', Integer, ForeignKey('users.id'),
                             key='_user_id')

        Base.metadata.create_all()

        eq_(Address.__table__.c['id'].name, 'id')
        eq_(Address.__table__.c['_email'].name, 'email')
        eq_(Address.__table__.c['_user_id'].name, 'user_id')
        
        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(name='u1'))

    def test_recompile_on_othermapper(self):
        """declarative version of the same test in mappers.py"""

        from sqlalchemy.orm import mapperlib

        class User(Base):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))

        class Address(Base):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))
            user = relation("User", primaryjoin=user_id == User.id,
                            backref="addresses")

        assert mapperlib._new_mappers is True
        u = User()
        assert User.addresses
        assert mapperlib._new_mappers is False
    
    def test_string_dependency_resolution(self):
        from sqlalchemy.sql import desc
        
        class User(Base, ComparableEntity):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            addresses = relation("Address", order_by="desc(Address.email)", 
                primaryjoin="User.id==Address.user_id", foreign_keys="[Address.user_id]")
        
        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer)  # note no foreign key
        
        Base.metadata.create_all()
        
        sess = create_session()
        u1 = User(name='ed', addresses=[Address(email='abc'), Address(email='def'), Address(email='xyz')])
        sess.add(u1)
        sess.flush()
        sess.clear()
        self.assertEquals(sess.query(User).filter(User.name == 'ed').one(),
            User(name='ed', addresses=[Address(email='xyz'), Address(email='def'), Address(email='abc')])
        )
        
        class Foo(Base, ComparableEntity):
            __tablename__ = 'foo'
            id = Column(Integer, primary_key=True)
            rel = relation("User", primaryjoin="User.addresses==Foo.id")
        self.assertRaisesMessage(exc.InvalidRequestError, "'addresses' is not an instance of ColumnProperty", compile_mappers)

    def test_string_dependency_resolution_in_backref(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            addresses = relation("Address", 
                primaryjoin="User.id==Address.user_id", 
                backref="user"
                )

        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))  

        compile_mappers()
        eq_(str(User.addresses.property.primaryjoin), str(Address.user.property.primaryjoin))
        
        
    def test_uncompiled_attributes_in_relation(self):
        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            addresses = relation("Address", order_by=Address.email, 
                foreign_keys=Address.user_id, 
                remote_side=Address.user_id,
                )
        
        # get the mapper for User.   User mapper will compile,
        # "addresses" relation will call upon Address.user_id for
        # its clause element.  Address.user_id is a _CompileOnAttr,
        # which then calls class_mapper(Address).  But !  We're already
        # "in compilation", but class_mapper(Address) needs to initialize
        # regardless, or COA's assertion fails
        # and things generally go downhill from there.
        class_mapper(User)
        
        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='ed', addresses=[Address(email='abc'), Address(email='xyz'), Address(email='def')])
        sess.add(u1)
        sess.flush()
        sess.clear()
        self.assertEquals(sess.query(User).filter(User.name == 'ed').one(),
            User(name='ed', addresses=[Address(email='abc'), Address(email='def'), Address(email='xyz')])
        )
            
    def test_nice_dependency_error(self):
        class User(Base):
            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True)
            addresses = relation("Address")

        def go():
            class Address(Base):
                __tablename__ = 'addresses'

                id = Column(Integer, primary_key=True)
                foo = sa.orm.column_property(User.id == 5)
        self.assertRaises(sa.exc.InvalidRequestError, go)

    def test_custom_base(self):
        class MyBase(object):
            def foobar(self):
                return "foobar"
        Base = decl.declarative_base(cls=MyBase)
        assert hasattr(Base, 'metadata')
        assert Base().foobar() == "foobar"
        
    def test_add_prop(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
        User.name = Column('name', String(50))
        User.addresses = relation("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'

            id = Column(Integer, primary_key=True)
        Address.email = Column(String(50), key='_email')
        Address.user_id = Column('user_id', Integer, ForeignKey('users.id'),
                             key='_user_id')

        Base.metadata.create_all()

        eq_(Address.__table__.c['id'].name, 'id')
        eq_(Address.__table__.c['_email'].name, 'email')
        eq_(Address.__table__.c['_user_id'].name, 'user_id')

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(name='u1'))
    
    def test_eager_order_by(self):
        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", order_by=Address.email)

        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[
            Address(email='two'),
            Address(email='one'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()
        eq_(sess.query(User).options(eagerload(User.addresses)).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

            
    def test_as_declarative(self):
        class User(ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", backref="user")

        class Address(ComparableEntity):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))
        
        reg = {}
        decl.instrument_declarative(User, reg, Base.metadata)
        decl.instrument_declarative(Address, reg, Base.metadata)
        Base.metadata.create_all()
        
        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])
        
    def test_custom_mapper(self):
        class MyExt(sa.orm.MapperExtension):
            def create_instance(self):
                return "CHECK"

        def mymapper(cls, tbl, **kwargs):
            kwargs['extension'] = MyExt()
            return sa.orm.mapper(cls, tbl, **kwargs)

        from sqlalchemy.orm.mapper import Mapper
        class MyMapper(Mapper):
            def __init__(self, *args, **kwargs):
                kwargs['extension'] = MyExt()
                Mapper.__init__(self, *args, **kwargs)

        from sqlalchemy.orm import scoping
        ss = scoping.ScopedSession(create_session)
        ss.extension = MyExt()
        ss_mapper = ss.mapper

        for mapperfunc in (mymapper, MyMapper, ss_mapper):
            base = decl.declarative_base()
            class Foo(base):
                __tablename__ = 'foo'
                __mapper_cls__ = mapperfunc
                id = Column(Integer, primary_key=True)
            eq_(Foo.__mapper__.compile().extension.create_instance(), 'CHECK')

            base = decl.declarative_base(mapper=mapperfunc)
            class Foo(base):
                __tablename__ = 'foo'
                id = Column(Integer, primary_key=True)
            eq_(Foo.__mapper__.compile().extension.create_instance(), 'CHECK')


    @testing.emits_warning('Ignoring declarative-like tuple value of '
                           'attribute id')
    def test_oops(self):
        def define():
            class User(Base, ComparableEntity):
                __tablename__ = 'users'

                id = Column('id', Integer, primary_key=True),
                name = Column('name', String(50))
            assert False
        self.assertRaisesMessage(
            sa.exc.ArgumentError,
            "Mapper Mapper|User|users could not assemble any primary key",
            define)
        
    def test_table_args(self):
        class Foo(Base):
            __tablename__ = 'foo'
            __table_args__ = {'mysql_engine':'InnoDB'}
            id = Column('id', Integer, primary_key=True)
            
        assert Foo.__table__.kwargs['mysql_engine'] == 'InnoDB'

        class Bar(Base):
            __tablename__ = 'bar'
            __table_args__ = (ForeignKeyConstraint(['id'], ['foo.id']), {'mysql_engine':'InnoDB'})
            id = Column('id', Integer, primary_key=True)
        
        assert Bar.__table__.c.id.references(Foo.__table__.c.id)
        assert Bar.__table__.kwargs['mysql_engine'] == 'InnoDB'
            
    def test_expression(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        User.address_count = sa.orm.column_property(
            sa.select([sa.func.count(Address.id)]).
            where(Address.user_id == User.id).as_scalar())

        Base.metadata.create_all()

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(),
            [User(name='u1', address_count=2, addresses=[
              Address(email='one'),
              Address(email='two')])])

    def test_column(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))

        User.a = Column('a', String(10))
        User.b = Column(String(10))

        Base.metadata.create_all()

        u1 = User(name='u1', a='a', b='b')
        eq_(u1.a, 'a')
        eq_(User.a.get_history(u1), (['a'], (), ()))
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(),
            [User(name='u1', a='a', b='b')])

    def test_column_properties(self):
        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            adr_count = sa.orm.column_property(
                sa.select([sa.func.count(Address.id)], Address.user_id == id).
                as_scalar())
            addresses = relation(Address)

        Base.metadata.create_all()

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(),
            [User(name='u1', adr_count=2, addresses=[
              Address(email='one'),
              Address(email='two')])])

    def test_column_properties_2(self):
        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            # this is not "valid" but we want to test that Address.id doesnt
            # get stuck into user's table
            adr_count = Address.id

        eq_(set(User.__table__.c.keys()), set(['id', 'name']))
        eq_(set(Address.__table__.c.keys()), set(['id', 'email', 'user_id']))

    def test_deferred(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column(Integer, primary_key=True)
            name = sa.orm.deferred(Column(String(50)))

        Base.metadata.create_all()
        sess = create_session()
        sess.save(User(name='u1'))
        sess.flush()
        sess.clear()

        u1 = sess.query(User).filter(User.name == 'u1').one()
        assert 'name' not in u1.__dict__
        def go():
            eq_(u1.name, 'u1')
        self.assert_sql_count(testing.db, go, 1)

    def test_synonym_inline(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            _name = Column('name', String(50))
            def _set_name(self, name):
                self._name = "SOMENAME " + name
            def _get_name(self):
                return self._name
            name = sa.orm.synonym('_name',
                                  descriptor=property(_get_name, _set_name))

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        eq_(u1.name, "SOMENAME someuser")
        sess.save(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == "SOMENAME someuser").one(), u1)

    def test_synonym_added(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            _name = Column('name', String(50))
            def _set_name(self, name):
                self._name = "SOMENAME " + name
            def _get_name(self):
                return self._name
            name = property(_get_name, _set_name)
        User.name = sa.orm.synonym('_name', descriptor=User.name)

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        eq_(u1.name, "SOMENAME someuser")
        sess.save(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == "SOMENAME someuser").one(), u1)

    def test_joined_inheritance(self):
        class Company(Base, ComparableEntity):
            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            employees = relation("Person")

        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            company_id = Column('company_id', Integer,
                                ForeignKey('companies.id'))
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
            primary_language = Column('primary_language', String(50))

        class Manager(Person):
            __tablename__ = 'managers'
            __mapper_args__ = {'polymorphic_identity':'manager'}
            id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
            golf_swing = Column('golf_swing', String(50))

        Base.metadata.create_all()

        sess = create_session()
        c1 = Company(name="MegaCorp, Inc.", employees=[
            Engineer(name="dilbert", primary_language="java"),
            Engineer(name="wally", primary_language="c++"),
            Manager(name="dogbert", golf_swing="fore!")
        ])

        c2 = Company(name="Elbonia, Inc.", employees=[
            Engineer(name="vlad", primary_language="cobol")
        ])

        sess.save(c1)
        sess.save(c2)
        sess.flush()
        sess.clear()

        eq_((sess.query(Company).
             filter(Company.employees.of_type(Engineer).
                    any(Engineer.primary_language == 'cobol')).first()),
            c2)

        # ensure that the Manager mapper was compiled
        # with the Person id column as higher priority.
        # this ensures that "id" will get loaded from the Person row
        # and not the possibly non-present Manager row
        assert Manager.id.property.columns == [Person.__table__.c.id, Manager.__table__.c.id]
        
        # assert that the "id" column is available without a second load.
        # this would be the symptom of the previous step not being correct.
        sess.clear()
        def go():
            assert sess.query(Manager).filter(Manager.name=='dogbert').one().id
        self.assert_sql_count(testing.db, go, 1)
        sess.clear()
        def go():
            assert sess.query(Person).filter(Manager.name=='dogbert').one().id
        self.assert_sql_count(testing.db, go, 1)
        
    def test_inheritance_with_undefined_relation(self):
        class Parent(Base):
           __tablename__ = 'parent'
           id = Column('id', Integer, primary_key=True)
           tp = Column('type', String(50))
           __mapper_args__ = dict(polymorphic_on = tp)


        class Child1(Parent):
           __tablename__ = 'child1'
           id = Column('id', Integer, ForeignKey('parent.id'), primary_key=True)
           related_child2 = Column('c2', Integer, ForeignKey('child2.id'))
           __mapper_args__ = dict(polymorphic_identity = 'child1')

        # no exception is raised by the ForeignKey to "child2" even though
        # child2 doesn't exist yet

        class Child2(Parent):
           __tablename__ = 'child2'
           id = Column('id', Integer, ForeignKey('parent.id'), primary_key=True)
           related_child1 = Column('c1', Integer)
           __mapper_args__ = dict(polymorphic_identity = 'child2')

        sa.orm.compile_mappers()  # no exceptions here

    def test_reentrant_compile_via_foreignkey(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey(User.id))

        # this forces a re-entrant compile() due to the User.id within the
        # ForeignKey
        sa.orm.compile_mappers()

        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

    def test_relation_reference(self):
        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", backref="user",
                                 primaryjoin=id == Address.user_id)

        User.address_count = sa.orm.column_property(
            sa.select([sa.func.count(Address.id)]).
            where(Address.user_id == User.id).as_scalar())

        Base.metadata.create_all()

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(),
            [User(name='u1', address_count=2, addresses=[
              Address(email='one'),
              Address(email='two')])])

    def test_pk_with_fk_init(self):
        class Bar(Base):
            __tablename__ = 'bar'

            id = sa.Column(sa.Integer, sa.ForeignKey("foo.id"), primary_key=True)
            ex = sa.Column(sa.Integer, primary_key=True)

        class Foo(Base):
            __tablename__ = 'foo'

            id = sa.Column(sa.Integer, primary_key=True)
            bars = sa.orm.relation(Bar)
        
        assert Bar.__mapper__.primary_key[0] is Bar.__table__.c.id
        assert Bar.__mapper__.primary_key[1] is Bar.__table__.c.ex
        
    def test_single_inheritance(self):
        class Company(Base, ComparableEntity):
            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            employees = relation("Person")

        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            company_id = Column('company_id', Integer,
                                ForeignKey('companies.id'))
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            primary_language = Column('primary_language', String(50))
            golf_swing = Column('golf_swing', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __mapper_args__ = {'polymorphic_identity':'engineer'}

        class Manager(Person):
            __mapper_args__ = {'polymorphic_identity':'manager'}

        Base.metadata.create_all()

        sess = create_session()
        c1 = Company(name="MegaCorp, Inc.", employees=[
            Engineer(name="dilbert", primary_language="java"),
            Engineer(name="wally", primary_language="c++"),
            Manager(name="dogbert", golf_swing="fore!")
        ])

        c2 = Company(name="Elbonia, Inc.", employees=[
            Engineer(name="vlad", primary_language="cobol")
        ])

        sess.save(c1)
        sess.save(c2)
        sess.flush()
        sess.clear()

        eq_((sess.query(Person).
             filter(Engineer.primary_language == 'cobol').first()),
            Engineer(name='vlad'))
        eq_((sess.query(Company).
             filter(Company.employees.of_type(Engineer).
                    any(Engineer.primary_language == 'cobol')).first()),
            c2)

    def test_with_explicit_autoloaded(self):
        meta = MetaData(testing.db)
        t1 = Table('t1', meta,
                   Column('id', String(50), primary_key=True),
                   Column('data', String(50)))
        meta.create_all()
        try:
            class MyObj(Base):
                __table__ = Table('t1', Base.metadata, autoload=True)

            sess = create_session()
            m = MyObj(id="someid", data="somedata")
            sess.save(m)
            sess.flush()

            eq_(t1.select().execute().fetchall(), [('someid', 'somedata')])
        finally:
            meta.drop_all()


class DeclarativeReflectionTest(testing.TestBase):
    def setUpAll(self):
        global reflection_metadata
        reflection_metadata = MetaData(testing.db)

        Table('users', reflection_metadata,
              Column('id', Integer, primary_key=True),
              Column('name', String(50)),
              test_needs_fk=True)
        Table('addresses', reflection_metadata,
              Column('id', Integer, primary_key=True),
              Column('email', String(50)),
              Column('user_id', Integer, ForeignKey('users.id')),
              test_needs_fk=True)
        Table('imhandles', reflection_metadata,
              Column('id', Integer, primary_key=True),
              Column('user_id', Integer),
              Column('network', String(50)),
              Column('handle', String(50)),
              test_needs_fk=True)

        reflection_metadata.create_all()

    def setUp(self):
        global Base
        Base = decl.declarative_base(testing.db)

    def tearDown(self):
        for t in reversed(reflection_metadata.sorted_tables):
            t.delete().execute()

    def tearDownAll(self):
        reflection_metadata.drop_all()

    def test_basic(self):
        meta = MetaData(testing.db)

        class User(Base, ComparableEntity):
            __tablename__ = 'users'
            __autoload__ = True
            addresses = relation("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'
            __autoload__ = True

        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])])

        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(name='u1'))

    def test_rekey(self):
        meta = MetaData(testing.db)

        class User(Base, ComparableEntity):
            __tablename__ = 'users'
            __autoload__ = True
            nom = Column('name', String(50), key='nom')
            addresses = relation("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'
            __autoload__ = True

        u1 = User(nom='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(), [User(nom='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])])

        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(nom='u1'))

        self.assertRaises(TypeError, User, name='u3')

    def test_supplied_fk(self):
        meta = MetaData(testing.db)

        class IMHandle(Base, ComparableEntity):
            __tablename__ = 'imhandles'
            __autoload__ = True

            user_id = Column('user_id', Integer,
                             ForeignKey('users.id'))
        class User(Base, ComparableEntity):
            __tablename__ = 'users'
            __autoload__ = True
            handles = relation("IMHandle", backref="user")

        u1 = User(name='u1', handles=[
            IMHandle(network='blabber', handle='foo'),
            IMHandle(network='lol', handle='zomg')
            ])
        sess = create_session()
        sess.save(u1)
        sess.flush()
        sess.clear()

        eq_(sess.query(User).all(), [User(name='u1', handles=[
            IMHandle(network='blabber', handle='foo'),
            IMHandle(network='lol', handle='zomg')
            ])])

        a1 = sess.query(IMHandle).filter(IMHandle.handle == 'zomg').one()
        eq_(a1, IMHandle(network='lol', handle='zomg'))
        eq_(a1.user, User(name='u1'))

    def test_synonym_for(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))

            @decl.synonym_for('name')
            @property
            def namesyn(self):
                return self.name

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        eq_(u1.name, "someuser")
        eq_(u1.namesyn, 'someuser')
        sess.save(u1)
        sess.flush()

        rt = sess.query(User).filter(User.namesyn == 'someuser').one()
        eq_(rt, u1)

    def test_comparable_using(self):
        class NameComparator(sa.orm.PropComparator):
            @property
            def upperself(self):
                cls = self.prop.parent.class_
                col = getattr(cls, 'name')
                return sa.func.upper(col)

            def operate(self, op, other, **kw):
                return op(self.upperself, other, **kw)

        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))

            @decl.comparable_using(NameComparator)
            @property
            def uc_name(self):
                return self.name is not None and self.name.upper() or None

        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser')
        eq_(u1.name, "someuser", u1.name)
        eq_(u1.uc_name, 'SOMEUSER', u1.uc_name)
        sess.save(u1)
        sess.flush()
        sess.clear()

        rt = sess.query(User).filter(User.uc_name == 'SOMEUSER').one()
        eq_(rt, u1)
        sess.clear()

        rt = sess.query(User).filter(User.uc_name.startswith('SOMEUSE')).one()
        eq_(rt, u1)


if __name__ == '__main__':
    testing.main()
