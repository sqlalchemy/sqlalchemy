
from sqlalchemy.test.testing import eq_, assert_raises, assert_raises_message
from sqlalchemy.ext import declarative as decl
from sqlalchemy import exc
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy import MetaData, Integer, String, ForeignKey, ForeignKeyConstraint, asc, Index
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
from sqlalchemy.orm import relation, create_session, class_mapper, eagerload, compile_mappers, backref, clear_mappers, polymorphic_union, deferred
from sqlalchemy.test.testing import eq_


from test.orm._base import ComparableEntity, MappedTest

class DeclarativeTestBase(testing.TestBase, testing.AssertsExecutionResults):
    def setup(self):
        global Base
        Base = decl.declarative_base(testing.db)

    def teardown(self):
        clear_mappers()
        Base.metadata.drop_all()
    
class DeclarativeTest(DeclarativeTestBase):
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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(name='u1'))

    def test_no_table(self):
        def go():
            class User(Base):
                id = Column('id', Integer, primary_key=True)
        assert_raises_message(sa.exc.InvalidRequestError, "does not have a __table__", go)

    def test_cant_add_columns(self):
        t = Table('t', Base.metadata, Column('id', Integer, primary_key=True), Column('data', String))
        def go():
            class User(Base):
                __table__ = t
                foo = Column(Integer, primary_key=True)
        # can't specify new columns not already in the table
        assert_raises_message(sa.exc.ArgumentError, "Can't add additional column 'foo' when specifying __table__", go)

        # regular re-mapping works tho
        class Bar(Base):
            __table__ = t
            some_data = t.c.data
            
        assert class_mapper(Bar).get_property('some_data').columns[0] is t.c.data
    
    def test_undefer_column_name(self):
        # TODO: not sure if there was an explicit
        # test for this elsewhere
        foo = Column(Integer)
        eq_(str(foo), '(no name)')
        eq_(foo.key, None)
        eq_(foo.name, None)
        decl._undefer_column_name('foo', foo)
        eq_(str(foo), 'foo')
        eq_(foo.key, 'foo')
        eq_(foo.name, 'foo')
        
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
                primaryjoin="User.id==Address.user_id", foreign_keys="[Address.user_id]",
                backref=backref('user', primaryjoin="User.id==Address.user_id", foreign_keys="[Address.user_id]")
                )
        
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
        sess.expunge_all()
        eq_(sess.query(User).filter(User.name == 'ed').one(),
            User(name='ed', addresses=[Address(email='xyz'), Address(email='def'), Address(email='abc')])
        )
        
        class Foo(Base, ComparableEntity):
            __tablename__ = 'foo'
            id = Column(Integer, primary_key=True)
            rel = relation("User", primaryjoin="User.addresses==Foo.id")
        assert_raises_message(exc.InvalidRequestError, "'addresses' is not an instance of ColumnProperty", compile_mappers)

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
        
    def test_string_dependency_resolution_tables(self):
        class User(Base, ComparableEntity):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            
            props = relation("Prop", 
                        secondary="user_to_prop", 
                        primaryjoin="User.id==user_to_prop.c.user_id", 
                        secondaryjoin="user_to_prop.c.prop_id==Prop.id", 
                    backref="users")

        class Prop(Base, ComparableEntity):
            __tablename__ = 'props'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
        
        user_to_prop = Table('user_to_prop', Base.metadata, 
            Column('user_id', Integer, ForeignKey('users.id')),
            Column('prop_id', Integer, ForeignKey('props.id')),
        )

        compile_mappers()
        assert class_mapper(User).get_property("props").secondary is user_to_prop

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
        sess.expunge_all()
        eq_(sess.query(User).filter(User.name == 'ed').one(),
            User(name='ed', addresses=[Address(email='abc'), Address(email='def'), Address(email='xyz')])
        )
            
    def test_nice_dependency_error(self):
        class User(Base):
            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True)
            addresses = relation("Address")

        class Address(Base):
            __tablename__ = 'addresses'

            id = Column(Integer, primary_key=True)
            foo = sa.orm.column_property(User.id == 5)

        # this used to raise an error when accessing User.id but that's no longer the case
        # since we got rid of _CompileOnAttr.
        assert_raises(sa.exc.ArgumentError, compile_mappers)
        
    def test_nice_dependency_error_works_with_hasattr(self):
        class User(Base):
            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True)
            addresses = relation("Addresss")

        # hasattr() on a compile-loaded attribute
        hasattr(User.addresses, 'property')
        # the exeption is preserved
        assert_raises_message(sa.exc.InvalidRequestError, r"suppressed within a hasattr\(\)", compile_mappers)

    def test_custom_base(self):
        class MyBase(object):
            def foobar(self):
                return "foobar"
        Base = decl.declarative_base(cls=MyBase)
        assert hasattr(Base, 'metadata')
        assert Base().foobar() == "foobar"
        
    def test_index_doesnt_compile(self):
        class User(Base):
            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            error = relation("Address")
            
        i = Index('my_index', User.name)
        
        # compile fails due to the nonexistent Addresses relation
        assert_raises(sa.exc.InvalidRequestError, compile_mappers)
        
        # index configured
        assert i in User.__table__.indexes
        assert User.__table__.c.id not in set(i.columns)
        assert User.__table__.c.name in set(i.columns)
        
        # tables create fine
        Base.metadata.create_all()
        
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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).options(eagerload(User.addresses)).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])

    def test_order_by_multi(self):
        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'

            id = Column('id', Integer, primary_key=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            addresses = relation("Address", order_by=(Address.email, Address.id))

        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[
            Address(email='two'),
            Address(email='one'),
        ])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        u = sess.query(User).filter(User.name == 'u1').one()
        a = u.addresses
            
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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(User).all(), [User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])])
        
    @testing.uses_deprecated()
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
        assert_raises_message(
            sa.exc.ArgumentError,
            "Mapper Mapper|User|users could not assemble any primary key",
            define)
        
    def test_table_args(self):
        
        def err():
            class Foo(Base):
                __tablename__ = 'foo'
                __table_args__ = (ForeignKeyConstraint(['id'], ['foo.id']),)
                id = Column('id', Integer, primary_key=True)
                
        assert_raises_message(sa.exc.ArgumentError, "Tuple form of __table_args__ is ", err)
        
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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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
        sess.add(User(name='u1'))
        sess.flush()
        sess.expunge_all()

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
        sess.add(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == "SOMENAME someuser").one(), u1)
    
    def test_synonym_no_descriptor(self):
        from sqlalchemy.orm.properties import ColumnProperty
        
        class CustomCompare(ColumnProperty.Comparator):
            __hash__ = None
            def __eq__(self, other):
                return self.__clause_element__() == other + ' FOO'
                
        class User(Base, ComparableEntity):
            __tablename__ = 'users'

            id = Column('id', Integer, primary_key=True)
            _name = Column('name', String(50))
            name = sa.orm.synonym('_name', comparator_factory=CustomCompare)
        
        Base.metadata.create_all()

        sess = create_session()
        u1 = User(name='someuser FOO')
        sess.add(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == "someuser").one(), u1)
        
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
        sess.add(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == "SOMENAME someuser").one(), u1)

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

        # previous versions would force a re-entrant mapper compile
        # via the User.id inside the ForeignKey but this is no
        # longer the case
        sa.orm.compile_mappers()

        eq_(str(Address.user_id.property.columns[0].foreign_keys[0]), "ForeignKey('users.id')")
        
        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
        ])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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
            sess.add(m)
            sess.flush()

            eq_(t1.select().execute().fetchall(), [('someid', 'somedata')])
        finally:
            meta.drop_all()

class DeclarativeInheritanceTest(DeclarativeTestBase):
    def test_custom_join_condition(self):
        class Foo(Base):
            __tablename__ = 'foo'
            id = Column('id', Integer, primary_key=True)

        class Bar(Foo):
            __tablename__ = 'bar'
            id = Column('id', Integer, primary_key=True)
            foo_id = Column('foo_id', Integer)
            __mapper_args__ = {'inherit_condition':foo_id==Foo.id}
        
        # compile succeeds because inherit_condition is honored
        compile_mappers()
    
    def test_joined(self):
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

        sess.add(c1)
        sess.add(c2)
        sess.flush()
        sess.expunge_all()

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
        sess.expunge_all()
        def go():
            assert sess.query(Manager).filter(Manager.name=='dogbert').one().id
        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()
        def go():
            assert sess.query(Person).filter(Manager.name=='dogbert').one().id
        self.assert_sql_count(testing.db, go, 1)
    
    def test_add_subcol_after_the_fact(self):
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True, test_needs_autoincrement=True)
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
        
        Engineer.primary_language = Column('primary_language', String(50))
        
        Base.metadata.create_all()

        sess = create_session()
        e1 = Engineer(primary_language='java', name='dilbert')
        sess.add(e1)
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(Person).first(),
            Engineer(primary_language='java', name='dilbert')
        )
    
    def test_add_parentcol_after_the_fact(self):
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True, test_needs_autoincrement=True)
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            primary_language = Column(String(50))
            id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
        
        Person.name = Column('name', String(50))
        
        Base.metadata.create_all()

        sess = create_session()
        e1 = Engineer(primary_language='java', name='dilbert')
        sess.add(e1)
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(Person).first(),
            Engineer(primary_language='java', name='dilbert')
        )

    def test_add_sub_parentcol_after_the_fact(self):
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True, test_needs_autoincrement=True)
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            primary_language = Column(String(50))
            id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
        
        class Admin(Engineer):
            __tablename__ = 'admins'
            __mapper_args__ = {'polymorphic_identity':'admin'}
            workstation = Column(String(50))
            id = Column('id', Integer, ForeignKey('engineers.id'), primary_key=True)
            
        Person.name = Column('name', String(50))

        Base.metadata.create_all()

        sess = create_session()
        e1 = Admin(primary_language='java', name='dilbert', workstation='foo')
        sess.add(e1)
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(Person).first(),
            Admin(primary_language='java', name='dilbert', workstation='foo')
        )
        
    def test_subclass_mixin(self):
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}
        
        class MyMixin(object):
            pass
            
        class Engineer(MyMixin, Person):
            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            id = Column('id', Integer, ForeignKey('people.id'), primary_key=True)
            primary_language = Column('primary_language', String(50))
            
        assert class_mapper(Engineer).inherits is class_mapper(Person)
        
    def test_with_undefined_foreignkey(self):
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

    def test_single_colsonbase(self):
        """test single inheritance where all the columns are on the base class."""
        
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

        sess.add(c1)
        sess.add(c2)
        sess.flush()
        sess.expunge_all()

        eq_((sess.query(Person).
             filter(Engineer.primary_language == 'cobol').first()),
            Engineer(name='vlad'))
        eq_((sess.query(Company).
             filter(Company.employees.of_type(Engineer).
                    any(Engineer.primary_language == 'cobol')).first()),
            c2)

    def test_single_colsonsub(self):
        """test single inheritance where the columns are local to their class.
        
        this is a newer usage.
        
        """

        class Company(Base, ComparableEntity):
            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            employees = relation("Person")

        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            company_id = Column(Integer,
                                ForeignKey('companies.id'))
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            primary_language = Column(String(50))

        class Manager(Person):
            __mapper_args__ = {'polymorphic_identity':'manager'}
            golf_swing = Column(String(50))

        # we have here a situation that is somewhat unique.
        # the Person class is mapped to the "people" table, but it
        # was mapped when the table did not include the "primary_language"
        # or "golf_swing" columns.  declarative will also manipulate
        # the exclude_properties collection so that sibling classes
        # don't cross-pollinate.

        assert Person.__table__.c.company_id
        assert Person.__table__.c.golf_swing
        assert Person.__table__.c.primary_language
        assert Engineer.primary_language
        assert Manager.golf_swing
        assert not hasattr(Person, 'primary_language')
        assert not hasattr(Person, 'golf_swing')
        assert not hasattr(Engineer, 'golf_swing')
        assert not hasattr(Manager, 'primary_language')
        
        Base.metadata.create_all()

        sess = create_session()
        
        e1 = Engineer(name="dilbert", primary_language="java")
        e2 = Engineer(name="wally", primary_language="c++")
        m1 = Manager(name="dogbert", golf_swing="fore!")
        c1 = Company(name="MegaCorp, Inc.", employees=[e1, e2, m1])

        e3 =Engineer(name="vlad", primary_language="cobol") 
        c2 = Company(name="Elbonia, Inc.", employees=[e3])
        sess.add(c1)
        sess.add(c2)
        sess.flush()
        sess.expunge_all()

        eq_((sess.query(Person).
             filter(Engineer.primary_language == 'cobol').first()),
            Engineer(name='vlad'))
        eq_((sess.query(Company).
             filter(Company.employees.of_type(Engineer).
                    any(Engineer.primary_language == 'cobol')).first()),
            c2)
            
        eq_(
            sess.query(Engineer).filter_by(primary_language='cobol').one(),
            Engineer(name="vlad", primary_language="cobol") 
        )

    def test_joined_from_single(self):
        class Company(Base, ComparableEntity):
            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            employees = relation("Person")
        
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            company_id = Column(Integer, ForeignKey('companies.id'))
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Manager(Person):
            __mapper_args__ = {'polymorphic_identity':'manager'}
            golf_swing = Column(String(50))

        class Engineer(Person):
            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            id = Column(Integer, ForeignKey('people.id'), primary_key=True)
            primary_language = Column(String(50))

        assert Person.__table__.c.golf_swing
        assert not Person.__table__.c.has_key('primary_language')
        assert Engineer.__table__.c.primary_language
        assert Engineer.primary_language
        assert Manager.golf_swing
        assert not hasattr(Person, 'primary_language')
        assert not hasattr(Person, 'golf_swing')
        assert not hasattr(Engineer, 'golf_swing')
        assert not hasattr(Manager, 'primary_language')

        Base.metadata.create_all()

        sess = create_session()

        e1 = Engineer(name="dilbert", primary_language="java")
        e2 = Engineer(name="wally", primary_language="c++")
        m1 = Manager(name="dogbert", golf_swing="fore!")
        c1 = Company(name="MegaCorp, Inc.", employees=[e1, e2, m1])
        e3 =Engineer(name="vlad", primary_language="cobol") 
        c2 = Company(name="Elbonia, Inc.", employees=[e3])
        sess.add(c1)
        sess.add(c2)
        sess.flush()
        sess.expunge_all()

        eq_((sess.query(Person).with_polymorphic(Engineer).
             filter(Engineer.primary_language == 'cobol').first()),
            Engineer(name='vlad'))
        eq_((sess.query(Company).
             filter(Company.employees.of_type(Engineer).
                    any(Engineer.primary_language == 'cobol')).first()),
            c2)

        eq_(
            sess.query(Engineer).filter_by(primary_language='cobol').one(),
            Engineer(name="vlad", primary_language="cobol") 
        )

    def test_add_deferred(self):
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)

        Person.name = deferred(Column(String(10)))

        Base.metadata.create_all()
        sess = create_session()
        p = Person(name='ratbert')

        sess.add(p)
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(Person).all(),
            [
                Person(name='ratbert')
            ]
        )
        person = sess.query(Person).filter(Person.name == 'ratbert').one()
        assert 'name' not in person.__dict__

    def test_single_fksonsub(self):
        """test single inheritance with a foreign key-holding column on a subclass.

        """

        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            primary_language_id = Column(Integer, ForeignKey('languages.id'))
            primary_language = relation("Language")
            
        class Language(Base, ComparableEntity):
            __tablename__ = 'languages'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        assert not hasattr(Person, 'primary_language_id')

        Base.metadata.create_all()

        sess = create_session()

        java, cpp, cobol = Language(name='java'),Language(name='cpp'), Language(name='cobol')
        e1 = Engineer(name="dilbert", primary_language=java)
        e2 = Engineer(name="wally", primary_language=cpp)
        e3 =Engineer(name="vlad", primary_language=cobol) 
        sess.add_all([e1, e2, e3])
        sess.flush()
        sess.expunge_all()

        eq_((sess.query(Person).
             filter(Engineer.primary_language.has(Language.name=='cobol')).first()),
            Engineer(name='vlad', primary_language=Language(name='cobol')))

        eq_(
            sess.query(Engineer).filter(Engineer.primary_language.has(Language.name=='cobol')).one(),
            Engineer(name="vlad", primary_language=Language(name='cobol')) 
        )
        
        eq_(
            sess.query(Person).join(Engineer.primary_language).order_by(Language.name).all(),
            [
                Engineer(name='vlad', primary_language=Language(name='cobol')),
                Engineer(name='wally', primary_language=Language(name='cpp')),
                Engineer(name='dilbert', primary_language=Language(name='java')),
            ]
        )
        
    def test_single_three_levels(self):
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        class Engineer(Person):
            __mapper_args__ = {'polymorphic_identity':'engineer'}
            primary_language = Column(String(50))

        class JuniorEngineer(Engineer):
            __mapper_args__ = {'polymorphic_identity':'junior_engineer'}
            nerf_gun = Column(String(50))

        class Manager(Person):
            __mapper_args__ = {'polymorphic_identity':'manager'}
            golf_swing = Column(String(50))

        assert JuniorEngineer.nerf_gun
        assert JuniorEngineer.primary_language
        assert JuniorEngineer.name
        assert Manager.golf_swing
        assert Engineer.primary_language
        assert not hasattr(Engineer, 'golf_swing')
        assert not hasattr(Engineer, 'nerf_gun')
        assert not hasattr(Manager, 'nerf_gun')
        assert not hasattr(Manager, 'primary_language')
            
    def test_single_no_special_cols(self):
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        def go():
            class Engineer(Person):
                __mapper_args__ = {'polymorphic_identity':'engineer'}
                primary_language = Column('primary_language', String(50))
                foo_bar = Column(Integer, primary_key=True)
        assert_raises_message(sa.exc.ArgumentError, "place primary key", go)
        
    def test_single_no_table_args(self):
        class Person(Base, ComparableEntity):
            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on':discriminator}

        def go():
            class Engineer(Person):
                __mapper_args__ = {'polymorphic_identity':'engineer'}
                primary_language = Column('primary_language', String(50))
                __table_args__ = ()
        assert_raises_message(sa.exc.ArgumentError, "place __table_args__", go)
        
    def test_concrete(self):
        engineers = Table('engineers', Base.metadata,
                        Column('id', Integer, primary_key=True),
                        Column('name', String(50)),
                        Column('primary_language', String(50))
                    )
        managers = Table('managers', Base.metadata,
                        Column('id', Integer, primary_key=True),
                        Column('name', String(50)),
                        Column('golf_swing', String(50))
                    )

        punion = polymorphic_union({
            'engineer':engineers,
            'manager':managers
        }, 'type', 'punion')

        class Person(Base, ComparableEntity):
            __table__ = punion
            __mapper_args__ = {'polymorphic_on':punion.c.type}

        class Engineer(Person):
            __table__ = engineers
            __mapper_args__ = {'polymorphic_identity':'engineer', 'concrete':True}

        class Manager(Person):
            __table__ = managers
            __mapper_args__ = {'polymorphic_identity':'manager', 'concrete':True}
        
        Base.metadata.create_all()
        sess = create_session()
        
        e1 = Engineer(name="dilbert", primary_language="java")
        e2 = Engineer(name="wally", primary_language="c++")
        m1 = Manager(name="dogbert", golf_swing="fore!")
        e3 = Engineer(name="vlad", primary_language="cobol") 
        
        sess.add_all([e1, e2, m1, e3])
        sess.flush()
        sess.expunge_all()
        eq_(
            sess.query(Person).order_by(Person.name).all(),
            [
                Engineer(name='dilbert'), Manager(name='dogbert'), 
                Engineer(name='vlad'), Engineer(name='wally')
            ]
        )
        
        
def _produce_test(inline, stringbased):
    class ExplicitJoinTest(MappedTest):
    
        @classmethod
        def define_tables(cls, metadata):
            global User, Address
            Base = decl.declarative_base(metadata=metadata)

            class User(Base, ComparableEntity):
                __tablename__ = 'users'
                id = Column(Integer, primary_key=True)
                name = Column(String(50))
            
            class Address(Base, ComparableEntity):
                __tablename__ = 'addresses'
                id = Column(Integer, primary_key=True)
                email = Column(String(50))
                user_id = Column(Integer, ForeignKey('users.id'))
                if inline:
                    if stringbased:
                        user = relation("User", primaryjoin="User.id==Address.user_id", backref="addresses")
                    else:
                        user = relation(User, primaryjoin=User.id==user_id, backref="addresses")
            
            if not inline:
                compile_mappers()
                if stringbased:
                    Address.user = relation("User", primaryjoin="User.id==Address.user_id", backref="addresses")
                else:
                    Address.user = relation(User, primaryjoin=User.id==Address.user_id, backref="addresses")

        @classmethod
        def insert_data(cls):
            params = [dict(zip(('id', 'name'), column_values)) for column_values in 
                [(7, 'jack'),
                (8, 'ed'),
                (9, 'fred'),
                (10, 'chuck')]
            ]
            User.__table__.insert().execute(params)
        
            Address.__table__.insert().execute(
                [dict(zip(('id', 'user_id', 'email'), column_values)) for column_values in 
                    [(1, 7, "jack@bean.com"),
                    (2, 8, "ed@wood.com"),
                    (3, 8, "ed@bettyboop.com"),
                    (4, 8, "ed@lala.com"),
                    (5, 9, "fred@fred.com")]
                ]
            )
    
        def test_aliased_join(self):
            # this query will screw up if the aliasing 
            # enabled in query.join() gets applied to the right half of the join condition inside the any().
            # the join condition inside of any() comes from the "primaryjoin" of the relation,
            # and should not be annotated with _orm_adapt.  PropertyLoader.Comparator will annotate
            # the left side with _orm_adapt, though.
            sess = create_session()
            eq_(
                sess.query(User).join(User.addresses, aliased=True).
                    filter(Address.email=='ed@wood.com').filter(User.addresses.any(Address.email=='jack@bean.com')).all(),
                []
            )
    
    ExplicitJoinTest.__name__ = "ExplicitJoinTest%s%s" % (inline and 'Inline' or 'Separate', stringbased and 'String' or 'Literal')
    return ExplicitJoinTest

for inline in (True, False):
    for stringbased in (True, False):
        testclass = _produce_test(inline, stringbased)
        exec("%s = testclass" % testclass.__name__)
        del testclass
        
class DeclarativeReflectionTest(testing.TestBase):
    @classmethod
    def setup_class(cls):
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

    def setup(self):
        global Base
        Base = decl.declarative_base(testing.db)

    def teardown(self):
        for t in reversed(reflection_metadata.sorted_tables):
            t.delete().execute()

    @classmethod
    def teardown_class(cls):
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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        eq_(sess.query(User).all(), [User(nom='u1', addresses=[
            Address(email='one'),
            Address(email='two'),
            ])])

        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(nom='u1'))

        assert_raises(TypeError, User, name='u3')

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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

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
        sess.add(u1)
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
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        rt = sess.query(User).filter(User.uc_name == 'SOMEUSER').one()
        eq_(rt, u1)
        sess.expunge_all()

        rt = sess.query(User).filter(User.uc_name.startswith('SOMEUSE')).one()
        eq_(rt, u1)


if __name__ == '__main__':
    testing.main()
