
from sqlalchemy.test.testing import eq_, assert_raises, \
    assert_raises_message
from sqlalchemy.ext import declarative as decl
from sqlalchemy import exc
import sqlalchemy as sa
from sqlalchemy.test import testing
from sqlalchemy import MetaData, Integer, String, ForeignKey, \
    ForeignKeyConstraint, asc, Index
from sqlalchemy.test.schema import Table, Column
from sqlalchemy.orm import relationship, create_session, class_mapper, \
    joinedload, compile_mappers, backref, clear_mappers, \
    polymorphic_union, deferred, column_property
from sqlalchemy.test.testing import eq_
from sqlalchemy.util import classproperty
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

            id = Column('id', Integer, primary_key=True,
                                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            addresses = relationship("Address", backref="user")

        class Address(Base, ComparableEntity):
            __tablename__ = 'addresses'

            id = Column(Integer, primary_key=True,
                                        test_needs_autoincrement=True)
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

        assert_raises_message(sa.exc.InvalidRequestError,
                              'does not have a __table__', go)

    def test_cant_add_columns(self):
        t = Table('t', Base.metadata, Column('id', Integer,
                  primary_key=True), Column('data', String))

        def go():
            class User(Base):
                __table__ = t
                foo = Column(Integer, primary_key=True)

        # can't specify new columns not already in the table

        assert_raises_message(sa.exc.ArgumentError,
                              "Can't add additional column 'foo' when "
                              "specifying __table__", go)

        # regular re-mapping works tho

        class Bar(Base):
            __table__ = t
            some_data = t.c.data

        assert class_mapper(Bar).get_property('some_data').columns[0] \
            is t.c.data
    
    def test_difficult_class(self):
        """test no getattr() errors with a customized class"""

        # metaclass to mock the way zope.interface breaks getattr()
        class BrokenMeta(type):
            def __getattribute__(self, attr):
                if attr == 'xyzzy':
                    raise AttributeError, 'xyzzy'
                else:
                    return object.__getattribute__(self,attr)

        # even though this class has an xyzzy attribute, getattr(cls,"xyzzy")
        # fails
        class BrokenParent(object):
            __metaclass__ = BrokenMeta
            xyzzy = "magic"

        # _as_declarative() inspects obj.__class__.__bases__
        class User(BrokenParent,ComparableEntity):
            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                test_needs_autoincrement=True)
            name = Column('name', String(50))

        decl.instrument_declarative(User,{},Base.metadata)

        
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
            user = relationship("User", primaryjoin=user_id == User.id,
                            backref="addresses")

        assert mapperlib._new_mappers is True
        u = User()
        assert User.addresses
        assert mapperlib._new_mappers is False
    
    def test_string_dependency_resolution(self):
        from sqlalchemy.sql import desc

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column(String(50))
            addresses = relationship('Address',
                    order_by='desc(Address.email)',
                    primaryjoin='User.id==Address.user_id',
                    foreign_keys='[Address.user_id]',
                    backref=backref('user',
                    primaryjoin='User.id==Address.user_id',
                    foreign_keys='[Address.user_id]'))
        
        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column(String(50))
            user_id = Column(Integer)  # note no foreign key

        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name='ed', addresses=[Address(email='abc'),
                  Address(email='def'), Address(email='xyz')])
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).filter(User.name == 'ed').one(),
            User(name='ed', addresses=[Address(email='xyz'),
            Address(email='def'), Address(email='abc')]))

        class Foo(Base, ComparableEntity):

            __tablename__ = 'foo'
            id = Column(Integer, primary_key=True)
            rel = relationship('User',
                               primaryjoin='User.addresses==Foo.id')

        assert_raises_message(exc.InvalidRequestError,
                              "'addresses' is not an instance of "
                              "ColumnProperty", compile_mappers)

    def test_string_dependency_resolution_two(self):

        class User(Base, ComparableEntity):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column(String(50))

        class Bar(Base, ComparableEntity):
            __tablename__ = 'bar'
            id = Column(Integer, primary_key=True)
            rel = relationship('User',
                               primaryjoin='User.id==Bar.__table__.id')

        assert_raises_message(exc.InvalidRequestError,
                              "does not have a mapped column named "
                              "'__table__'", compile_mappers)

    def test_string_dependency_resolution_no_magic(self):
        """test that full tinkery expressions work as written"""

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            addresses = relationship('Address',
                    primaryjoin='User.id==Address.user_id.prop.columns['
                    '0]')

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            user_id = Column(Integer, ForeignKey('users.id'))

        compile_mappers()
        eq_(str(User.addresses.prop.primaryjoin),
            'users.id = addresses.user_id')
        
    def test_string_dependency_resolution_in_backref(self):

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            addresses = relationship('Address',
                    primaryjoin='User.id==Address.user_id',
                    backref='user')

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))

        compile_mappers()
        eq_(str(User.addresses.property.primaryjoin),
            str(Address.user.property.primaryjoin))
        
    def test_string_dependency_resolution_tables(self):

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            props = relationship('Prop', secondary='user_to_prop',
                                 primaryjoin='User.id==user_to_prop.c.u'
                                 'ser_id',
                                 secondaryjoin='user_to_prop.c.prop_id='
                                 '=Prop.id', backref='users')

        class Prop(Base, ComparableEntity):

            __tablename__ = 'props'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))

        user_to_prop = Table('user_to_prop', Base.metadata,
                             Column('user_id', Integer,
                             ForeignKey('users.id')), Column('prop_id',
                             Integer, ForeignKey('props.id')))
        compile_mappers()
        assert class_mapper(User).get_property('props').secondary \
            is user_to_prop

    def test_uncompiled_attributes_in_relationship(self):

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column(String(50))
            addresses = relationship('Address', order_by=Address.email,
                    foreign_keys=Address.user_id,
                    remote_side=Address.user_id)

        # get the mapper for User.   User mapper will compile,
        # "addresses" relationship will call upon Address.user_id for
        # its clause element.  Address.user_id is a _CompileOnAttr,
        # which then calls class_mapper(Address).  But !  We're already
        # "in compilation", but class_mapper(Address) needs to
        # initialize regardless, or COA's assertion fails and things
        # generally go downhill from there.

        class_mapper(User)
        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name='ed', addresses=[Address(email='abc'),
                  Address(email='xyz'), Address(email='def')])
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).filter(User.name == 'ed').one(),
            User(name='ed', addresses=[Address(email='abc'),
            Address(email='def'), Address(email='xyz')]))
            
    def test_nice_dependency_error(self):

        class User(Base):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True)
            addresses = relationship('Address')

        class Address(Base):

            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True)
            foo = sa.orm.column_property(User.id == 5)

        # this used to raise an error when accessing User.id but that's
        # no longer the case since we got rid of _CompileOnAttr.

        assert_raises(sa.exc.ArgumentError, compile_mappers)

    def test_nice_dependency_error_works_with_hasattr(self):

        class User(Base):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True)
            addresses = relationship('Addresss')

        # hasattr() on a compile-loaded attribute

        hasattr(User.addresses, 'property')

        # the exeption is preserved

        assert_raises_message(sa.exc.InvalidRequestError,
                              r"suppressed within a hasattr\(\)",
                              compile_mappers)

    def test_custom_base(self):
        class MyBase(object):
            def foobar(self):
                return "foobar"
        Base = decl.declarative_base(cls=MyBase)
        assert hasattr(Base, 'metadata')
        assert Base().foobar() == "foobar"
    
    def test_uses_get_on_class_col_fk(self):

        # test [ticket:1492]

        class Master(Base):

            __tablename__ = 'master'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        class Detail(Base):

            __tablename__ = 'detail'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            master_id = Column(None, ForeignKey(Master.id))
            master = relationship(Master)

        Base.metadata.create_all()
        compile_mappers()
        assert class_mapper(Detail).get_property('master'
                ).strategy.use_get
        m1 = Master()
        d1 = Detail(master=m1)
        sess = create_session()
        sess.add(d1)
        sess.flush()
        sess.expunge_all()
        d1 = sess.query(Detail).first()
        m1 = sess.query(Master).first()

        def go():
            assert d1.master

        self.assert_sql_count(testing.db, go, 0)
        
    def test_index_doesnt_compile(self):
        class User(Base):
            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            error = relationship("Address")
            
        i = Index('my_index', User.name)
        
        # compile fails due to the nonexistent Addresses relationship
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
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)

        User.name = Column('name', String(50))
        User.addresses = relationship('Address', backref='user')

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        Address.email = Column(String(50), key='_email')
        Address.user_id = Column('user_id', Integer,
                                 ForeignKey('users.id'), key='_user_id')
        Base.metadata.create_all()
        eq_(Address.__table__.c['id'].name, 'id')
        eq_(Address.__table__.c['_email'].name, 'email')
        eq_(Address.__table__.c['_user_id'].name, 'user_id')
        u1 = User(name='u1', addresses=[Address(email='one'),
                  Address(email='two')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name='u1',
            addresses=[Address(email='one'), Address(email='two')])])
        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(name='u1'))
    
    def test_eager_order_by(self):

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            addresses = relationship('Address', order_by=Address.email)

        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[Address(email='two'),
                  Address(email='one')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).options(joinedload(User.addresses)).all(),
            [User(name='u1', addresses=[Address(email='one'),
            Address(email='two')])])

    def test_order_by_multi(self):

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            addresses = relationship('Address',
                    order_by=(Address.email, Address.id))

        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[Address(email='two'),
                  Address(email='one')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        u = sess.query(User).filter(User.name == 'u1').one()
        a = u.addresses

    def test_as_declarative(self):

        class User(ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            addresses = relationship('Address', backref='user')

        class Address(ComparableEntity):

            __tablename__ = 'addresses'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        reg = {}
        decl.instrument_declarative(User, reg, Base.metadata)
        decl.instrument_declarative(Address, reg, Base.metadata)
        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[Address(email='one'),
                  Address(email='two')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name='u1',
            addresses=[Address(email='one'), Address(email='two')])])

    @testing.uses_deprecated()
    def test_custom_mapper(self):

        class MyExt(sa.orm.MapperExtension):

            def create_instance(self):
                return 'CHECK'

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
        for mapperfunc in mymapper, MyMapper, ss_mapper:
            base = decl.declarative_base()

            class Foo(base):

                __tablename__ = 'foo'
                __mapper_cls__ = mapperfunc
                id = Column(Integer, primary_key=True)

            eq_(Foo.__mapper__.compile().extension.create_instance(),
                'CHECK')
            base = decl.declarative_base(mapper=mapperfunc)

            class Foo(base):

                __tablename__ = 'foo'
                id = Column(Integer, primary_key=True)

            eq_(Foo.__mapper__.compile().extension.create_instance(),
                'CHECK')

    @testing.emits_warning('Ignoring declarative-like tuple value of '
                           'attribute id')
    def test_oops(self):

        def define():

            class User(Base, ComparableEntity):

                __tablename__ = 'users'
                id = Column('id', Integer, primary_key=True),
                name = Column('name', String(50))

            assert False

        assert_raises_message(sa.exc.ArgumentError,
                              'Mapper Mapper|User|users could not '
                              'assemble any primary key', define)
        
    def test_table_args(self):

        def err():
            class Foo(Base):

                __tablename__ = 'foo'
                __table_args__ = ForeignKeyConstraint(['id'], ['foo.id'
                        ]),
                id = Column('id', Integer, primary_key=True)

        assert_raises_message(sa.exc.ArgumentError,
                              'Tuple form of __table_args__ is ', err)

        class Foo(Base):

            __tablename__ = 'foo'
            __table_args__ = {'mysql_engine': 'InnoDB'}
            id = Column('id', Integer, primary_key=True)

        assert Foo.__table__.kwargs['mysql_engine'] == 'InnoDB'

        class Bar(Base):

            __tablename__ = 'bar'
            __table_args__ = ForeignKeyConstraint(['id'], ['foo.id']), \
                {'mysql_engine': 'InnoDB'}
            id = Column('id', Integer, primary_key=True)

        assert Bar.__table__.c.id.references(Foo.__table__.c.id)
        assert Bar.__table__.kwargs['mysql_engine'] == 'InnoDB'

    def test_expression(self):

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            addresses = relationship('Address', backref='user')

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        User.address_count = \
            sa.orm.column_property(sa.select([sa.func.count(Address.id)]).
                    where(Address.user_id
                                   == User.id).as_scalar())
        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[Address(email='one'),
                  Address(email='two')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name='u1', address_count=2,
            addresses=[Address(email='one'), Address(email='two')])])

    def test_column(self):

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
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
        eq_(sess.query(User).all(), [User(name='u1', a='a', b='b')])

    def test_column_properties(self):

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column(String(50))
            user_id = Column(Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            adr_count = \
                sa.orm.column_property(sa.select([sa.func.count(Address.id)],
                    Address.user_id == id).as_scalar())
            addresses = relationship(Address)

        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[Address(email='one'),
                  Address(email='two')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name='u1', adr_count=2,
            addresses=[Address(email='one'), Address(email='two')])])

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

            # this is not "valid" but we want to test that Address.id
            # doesnt get stuck into user's table

            adr_count = Address.id

        eq_(set(User.__table__.c.keys()), set(['id', 'name']))
        eq_(set(Address.__table__.c.keys()), set(['id', 'email',
            'user_id']))

    def test_deferred(self):

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
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
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            _name = Column('name', String(50))

            def _set_name(self, name):
                self._name = 'SOMENAME ' + name

            def _get_name(self):
                return self._name

            name = sa.orm.synonym('_name',
                                  descriptor=property(_get_name,
                                  _set_name))

        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name='someuser')
        eq_(u1.name, 'SOMENAME someuser')
        sess.add(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == 'SOMENAME someuser'
            ).one(), u1)

    def test_synonym_no_descriptor(self):
        from sqlalchemy.orm.properties import ColumnProperty

        class CustomCompare(ColumnProperty.Comparator):

            __hash__ = None

            def __eq__(self, other):
                return self.__clause_element__() == other + ' FOO'

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            _name = Column('name', String(50))
            name = sa.orm.synonym('_name',
                                  comparator_factory=CustomCompare)

        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name='someuser FOO')
        sess.add(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == 'someuser').one(), u1)

    def test_synonym_added(self):

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            _name = Column('name', String(50))

            def _set_name(self, name):
                self._name = 'SOMENAME ' + name

            def _get_name(self):
                return self._name

            name = property(_get_name, _set_name)

        User.name = sa.orm.synonym('_name', descriptor=User.name)
        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name='someuser')
        eq_(u1.name, 'SOMENAME someuser')
        sess.add(u1)
        sess.flush()
        eq_(sess.query(User).filter(User.name == 'SOMENAME someuser'
            ).one(), u1)

    def test_reentrant_compile_via_foreignkey(self):

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            addresses = relationship('Address', backref='user')

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey(User.id))

        # previous versions would force a re-entrant mapper compile via
        # the User.id inside the ForeignKey but this is no longer the
        # case

        sa.orm.compile_mappers()
        eq_(str(Address.user_id.property.columns[0].foreign_keys[0]),
            "ForeignKey('users.id')")
        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[Address(email='one'),
                  Address(email='two')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name='u1',
            addresses=[Address(email='one'), Address(email='two')])])

    def test_relationship_reference(self):

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            email = Column('email', String(50))
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            addresses = relationship('Address', backref='user',
                    primaryjoin=id == Address.user_id)

        User.address_count = \
            sa.orm.column_property(sa.select([sa.func.count(Address.id)]).
                    where(Address.user_id
                                   == User.id).as_scalar())
        Base.metadata.create_all()
        u1 = User(name='u1', addresses=[Address(email='one'),
                  Address(email='two')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name='u1', address_count=2,
            addresses=[Address(email='one'), Address(email='two')])])

    def test_pk_with_fk_init(self):

        class Bar(Base):

            __tablename__ = 'bar'
            id = sa.Column(sa.Integer, sa.ForeignKey('foo.id'),
                           primary_key=True)
            ex = sa.Column(sa.Integer, primary_key=True)

        class Foo(Base):

            __tablename__ = 'foo'
            id = sa.Column(sa.Integer, primary_key=True)
            bars = sa.orm.relationship(Bar)

        assert Bar.__mapper__.primary_key[0] is Bar.__table__.c.id
        assert Bar.__mapper__.primary_key[1] is Bar.__table__.c.ex

    def test_with_explicit_autoloaded(self):
        meta = MetaData(testing.db)
        t1 = Table('t1', meta, Column('id', String(50),
                   primary_key=True, test_needs_autoincrement=True),
                   Column('data', String(50)))
        meta.create_all()
        try:

            class MyObj(Base):

                __table__ = Table('t1', Base.metadata, autoload=True)

            sess = create_session()
            m = MyObj(id='someid', data='somedata')
            sess.add(m)
            sess.flush()
            eq_(t1.select().execute().fetchall(), [('someid', 'somedata'
                )])
        finally:
            meta.drop_all()

    def test_synonym_for(self):

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))

            @decl.synonym_for('name')
            @property
            def namesyn(self):
                return self.name

        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name='someuser')
        eq_(u1.name, 'someuser')
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

            def operate(
                self,
                op,
                other,
                **kw
                ):
                return op(self.upperself, other, **kw)

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))

            @decl.comparable_using(NameComparator)
            @property
            def uc_name(self):
                return self.name is not None and self.name.upper() \
                    or None

        Base.metadata.create_all()
        sess = create_session()
        u1 = User(name='someuser')
        eq_(u1.name, 'someuser', u1.name)
        eq_(u1.uc_name, 'SOMEUSER', u1.uc_name)
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        rt = sess.query(User).filter(User.uc_name == 'SOMEUSER').one()
        eq_(rt, u1)
        sess.expunge_all()
        rt = sess.query(User).filter(User.uc_name.startswith('SOMEUSE'
                )).one()
        eq_(rt, u1)

class DeclarativeInheritanceTest(DeclarativeTestBase):

    def test_we_must_copy_mapper_args(self):

        class Person(Base):

            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator,
                               'polymorphic_identity': 'person'}

        class Engineer(Person):

            primary_language = Column(String(50))

        assert 'inherits' not in Person.__mapper_args__
        assert class_mapper(Engineer).polymorphic_on is None

    def test_custom_join_condition(self):

        class Foo(Base):

            __tablename__ = 'foo'
            id = Column('id', Integer, primary_key=True)

        class Bar(Foo):

            __tablename__ = 'bar'
            id = Column('id', Integer, primary_key=True)
            foo_id = Column('foo_id', Integer)
            __mapper_args__ = {'inherit_condition': foo_id == Foo.id}

        # compile succeeds because inherit_condition is honored

        compile_mappers()

    def test_joined(self):

        class Company(Base, ComparableEntity):

            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            employees = relationship('Person')

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            company_id = Column('company_id', Integer,
                                ForeignKey('companies.id'))
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            id = Column('id', Integer, ForeignKey('people.id'),
                        primary_key=True)
            primary_language = Column('primary_language', String(50))

        class Manager(Person):

            __tablename__ = 'managers'
            __mapper_args__ = {'polymorphic_identity': 'manager'}
            id = Column('id', Integer, ForeignKey('people.id'),
                        primary_key=True)
            golf_swing = Column('golf_swing', String(50))

        Base.metadata.create_all()
        sess = create_session()
        c1 = Company(name='MegaCorp, Inc.',
                     employees=[Engineer(name='dilbert',
                     primary_language='java'), Engineer(name='wally',
                     primary_language='c++'), Manager(name='dogbert',
                     golf_swing='fore!')])
        c2 = Company(name='Elbonia, Inc.',
                     employees=[Engineer(name='vlad',
                     primary_language='cobol')])
        sess.add(c1)
        sess.add(c2)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Company).filter(Company.employees.of_type(Engineer).
            any(Engineer.primary_language
            == 'cobol')).first(), c2)

        # ensure that the Manager mapper was compiled with the Person id
        # column as higher priority. this ensures that "id" will get
        # loaded from the Person row and not the possibly non-present
        # Manager row

        assert Manager.id.property.columns == [Person.__table__.c.id,
                Manager.__table__.c.id]

        # assert that the "id" column is available without a second
        # load. this would be the symptom of the previous step not being
        # correct.

        sess.expunge_all()

        def go():
            assert sess.query(Manager).filter(Manager.name == 'dogbert'
                    ).one().id

        self.assert_sql_count(testing.db, go, 1)
        sess.expunge_all()

        def go():
            assert sess.query(Person).filter(Manager.name == 'dogbert'
                    ).one().id

        self.assert_sql_count(testing.db, go, 1)

    def test_add_subcol_after_the_fact(self):

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            id = Column('id', Integer, ForeignKey('people.id'),
                        primary_key=True)

        Engineer.primary_language = Column('primary_language',
                String(50))
        Base.metadata.create_all()
        sess = create_session()
        e1 = Engineer(primary_language='java', name='dilbert')
        sess.add(e1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).first(), Engineer(primary_language='java'
            , name='dilbert'))

    def test_add_parentcol_after_the_fact(self):

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            primary_language = Column(String(50))
            id = Column('id', Integer, ForeignKey('people.id'),
                        primary_key=True)

        Person.name = Column('name', String(50))
        Base.metadata.create_all()
        sess = create_session()
        e1 = Engineer(primary_language='java', name='dilbert')
        sess.add(e1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).first(), 
            Engineer(primary_language='java', name='dilbert'))

    def test_add_sub_parentcol_after_the_fact(self):

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            primary_language = Column(String(50))
            id = Column('id', Integer, ForeignKey('people.id'),
                        primary_key=True)

        class Admin(Engineer):

            __tablename__ = 'admins'
            __mapper_args__ = {'polymorphic_identity': 'admin'}
            workstation = Column(String(50))
            id = Column('id', Integer, ForeignKey('engineers.id'),
                        primary_key=True)

        Person.name = Column('name', String(50))
        Base.metadata.create_all()
        sess = create_session()
        e1 = Admin(primary_language='java', name='dilbert',
                   workstation='foo')
        sess.add(e1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).first(), Admin(primary_language='java',
            name='dilbert', workstation='foo'))

    def test_subclass_mixin(self):

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class MyMixin(object):

            pass

        class Engineer(MyMixin, Person):

            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            id = Column('id', Integer, ForeignKey('people.id'),
                        primary_key=True)
            primary_language = Column('primary_language', String(50))

        assert class_mapper(Engineer).inherits is class_mapper(Person)

    def test_with_undefined_foreignkey(self):

        class Parent(Base):

            __tablename__ = 'parent'
            id = Column('id', Integer, primary_key=True)
            tp = Column('type', String(50))
            __mapper_args__ = dict(polymorphic_on=tp)

        class Child1(Parent):

            __tablename__ = 'child1'
            id = Column('id', Integer, ForeignKey('parent.id'),
                        primary_key=True)
            related_child2 = Column('c2', Integer,
                                    ForeignKey('child2.id'))
            __mapper_args__ = dict(polymorphic_identity='child1')

        # no exception is raised by the ForeignKey to "child2" even
        # though child2 doesn't exist yet

        class Child2(Parent):

            __tablename__ = 'child2'
            id = Column('id', Integer, ForeignKey('parent.id'),
                        primary_key=True)
            related_child1 = Column('c1', Integer)
            __mapper_args__ = dict(polymorphic_identity='child2')

        sa.orm.compile_mappers()  # no exceptions here

    def test_single_colsonbase(self):
        """test single inheritance where all the columns are on the base
        class."""

        class Company(Base, ComparableEntity):

            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            employees = relationship('Person')

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            company_id = Column('company_id', Integer,
                                ForeignKey('companies.id'))
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            primary_language = Column('primary_language', String(50))
            golf_swing = Column('golf_swing', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __mapper_args__ = {'polymorphic_identity': 'engineer'}

        class Manager(Person):

            __mapper_args__ = {'polymorphic_identity': 'manager'}

        Base.metadata.create_all()
        sess = create_session()
        c1 = Company(name='MegaCorp, Inc.',
                     employees=[Engineer(name='dilbert',
                     primary_language='java'), Engineer(name='wally',
                     primary_language='c++'), Manager(name='dogbert',
                     golf_swing='fore!')])
        c2 = Company(name='Elbonia, Inc.',
                     employees=[Engineer(name='vlad',
                     primary_language='cobol')])
        sess.add(c1)
        sess.add(c2)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).filter(Engineer.primary_language
            == 'cobol').first(), Engineer(name='vlad'))
        eq_(sess.query(Company).filter(Company.employees.of_type(Engineer).
            any(Engineer.primary_language
            == 'cobol')).first(), c2)

    def test_single_colsonsub(self):
        """test single inheritance where the columns are local to their
        class.
        
        this is a newer usage.
        
        """

        class Company(Base, ComparableEntity):

            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            employees = relationship('Person')

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            company_id = Column(Integer, ForeignKey('companies.id'))
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            primary_language = Column(String(50))

        class Manager(Person):

            __mapper_args__ = {'polymorphic_identity': 'manager'}
            golf_swing = Column(String(50))

        # we have here a situation that is somewhat unique. the Person
        # class is mapped to the "people" table, but it was mapped when
        # the table did not include the "primary_language" or
        # "golf_swing" columns.  declarative will also manipulate the
        # exclude_properties collection so that sibling classes don't
        # cross-pollinate.

        assert Person.__table__.c.company_id is not None
        assert Person.__table__.c.golf_swing is not None
        assert Person.__table__.c.primary_language is not None
        assert Engineer.primary_language is not None
        assert Manager.golf_swing is not None
        assert not hasattr(Person, 'primary_language')
        assert not hasattr(Person, 'golf_swing')
        assert not hasattr(Engineer, 'golf_swing')
        assert not hasattr(Manager, 'primary_language')
        Base.metadata.create_all()
        sess = create_session()
        e1 = Engineer(name='dilbert', primary_language='java')
        e2 = Engineer(name='wally', primary_language='c++')
        m1 = Manager(name='dogbert', golf_swing='fore!')
        c1 = Company(name='MegaCorp, Inc.', employees=[e1, e2, m1])
        e3 = Engineer(name='vlad', primary_language='cobol')
        c2 = Company(name='Elbonia, Inc.', employees=[e3])
        sess.add(c1)
        sess.add(c2)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).filter(Engineer.primary_language
            == 'cobol').first(), Engineer(name='vlad'))
        eq_(sess.query(Company).filter(Company.employees.of_type(Engineer).
            any(Engineer.primary_language
            == 'cobol')).first(), c2)
        eq_(sess.query(Engineer).filter_by(primary_language='cobol'
            ).one(), Engineer(name='vlad', primary_language='cobol'))

    def test_joined_from_single(self):

        class Company(Base, ComparableEntity):

            __tablename__ = 'companies'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column('name', String(50))
            employees = relationship('Person')

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            company_id = Column(Integer, ForeignKey('companies.id'))
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Manager(Person):

            __mapper_args__ = {'polymorphic_identity': 'manager'}
            golf_swing = Column(String(50))

        class Engineer(Person):

            __tablename__ = 'engineers'
            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            id = Column(Integer, ForeignKey('people.id'),
                        primary_key=True)
            primary_language = Column(String(50))

        assert Person.__table__.c.golf_swing is not None
        assert not Person.__table__.c.has_key('primary_language')
        assert Engineer.__table__.c.primary_language is not None
        assert Engineer.primary_language is not None
        assert Manager.golf_swing is not None
        assert not hasattr(Person, 'primary_language')
        assert not hasattr(Person, 'golf_swing')
        assert not hasattr(Engineer, 'golf_swing')
        assert not hasattr(Manager, 'primary_language')
        Base.metadata.create_all()
        sess = create_session()
        e1 = Engineer(name='dilbert', primary_language='java')
        e2 = Engineer(name='wally', primary_language='c++')
        m1 = Manager(name='dogbert', golf_swing='fore!')
        c1 = Company(name='MegaCorp, Inc.', employees=[e1, e2, m1])
        e3 = Engineer(name='vlad', primary_language='cobol')
        c2 = Company(name='Elbonia, Inc.', employees=[e3])
        sess.add(c1)
        sess.add(c2)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).with_polymorphic(Engineer).
            filter(Engineer.primary_language
            == 'cobol').first(), Engineer(name='vlad'))
        eq_(sess.query(Company).filter(Company.employees.of_type(Engineer).
            any(Engineer.primary_language
            == 'cobol')).first(), c2)
        eq_(sess.query(Engineer).filter_by(primary_language='cobol'
            ).one(), Engineer(name='vlad', primary_language='cobol'))

    def test_add_deferred(self):

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True,
                        test_needs_autoincrement=True)

        Person.name = deferred(Column(String(10)))
        Base.metadata.create_all()
        sess = create_session()
        p = Person(name='ratbert')
        sess.add(p)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).all(), [Person(name='ratbert')])
        sess.expunge_all()
        person = sess.query(Person).filter(Person.name == 'ratbert'
                ).one()
        assert 'name' not in person.__dict__

    def test_single_fksonsub(self):
        """test single inheritance with a foreign key-holding column on
        a subclass.
        
        """

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            primary_language_id = Column(Integer,
                    ForeignKey('languages.id'))
            primary_language = relationship('Language')

        class Language(Base, ComparableEntity):

            __tablename__ = 'languages'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column(String(50))

        assert not hasattr(Person, 'primary_language_id')
        Base.metadata.create_all()
        sess = create_session()
        java, cpp, cobol = Language(name='java'), Language(name='cpp'), \
            Language(name='cobol')
        e1 = Engineer(name='dilbert', primary_language=java)
        e2 = Engineer(name='wally', primary_language=cpp)
        e3 = Engineer(name='vlad', primary_language=cobol)
        sess.add_all([e1, e2, e3])
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).filter(Engineer.primary_language.has(
            Language.name
            == 'cobol')).first(), Engineer(name='vlad',
            primary_language=Language(name='cobol')))
        eq_(sess.query(Engineer).filter(Engineer.primary_language.has(
            Language.name
            == 'cobol')).one(), Engineer(name='vlad',
            primary_language=Language(name='cobol')))
        eq_(sess.query(Person).join(Engineer.primary_language).order_by(
            Language.name).all(),
            [Engineer(name='vlad',
            primary_language=Language(name='cobol')),
            Engineer(name='wally', primary_language=Language(name='cpp'
            )), Engineer(name='dilbert',
            primary_language=Language(name='java'))])

    def test_single_three_levels(self):

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            primary_language = Column(String(50))

        class JuniorEngineer(Engineer):

            __mapper_args__ = \
                {'polymorphic_identity': 'junior_engineer'}
            nerf_gun = Column(String(50))

        class Manager(Person):

            __mapper_args__ = {'polymorphic_identity': 'manager'}
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

    def test_single_detects_conflict(self):

        class Person(Base):

            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        class Engineer(Person):

            __mapper_args__ = {'polymorphic_identity': 'engineer'}
            primary_language = Column(String(50))

        # test sibling col conflict

        def go():

            class Manager(Person):

                __mapper_args__ = {'polymorphic_identity': 'manager'}
                golf_swing = Column(String(50))
                primary_language = Column(String(50))

        assert_raises(sa.exc.ArgumentError, go)

        # test parent col conflict

        def go():

            class Salesman(Person):

                __mapper_args__ = {'polymorphic_identity': 'manager'}
                name = Column(String(50))

        assert_raises(sa.exc.ArgumentError, go)

    def test_single_no_special_cols(self):

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        def go():

            class Engineer(Person):

                __mapper_args__ = {'polymorphic_identity': 'engineer'}
                primary_language = Column('primary_language',
                        String(50))
                foo_bar = Column(Integer, primary_key=True)

        assert_raises_message(sa.exc.ArgumentError, 'place primary key'
                              , go)

    def test_single_no_table_args(self):

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column('id', Integer, primary_key=True)
            name = Column('name', String(50))
            discriminator = Column('type', String(50))
            __mapper_args__ = {'polymorphic_on': discriminator}

        def go():

            class Engineer(Person):

                __mapper_args__ = {'polymorphic_identity': 'engineer'}
                primary_language = Column('primary_language',
                        String(50))

                # this should be on the Person class, as this is single
                # table inheritance, which is why we test that this
                # throws an exception!

                __table_args__ = {'mysql_engine': 'InnoDB'}

        assert_raises_message(sa.exc.ArgumentError,
                              'place __table_args__', go)

    def test_concrete(self):
        engineers = Table('engineers', Base.metadata, Column('id',
                          Integer, primary_key=True,
                          test_needs_autoincrement=True), Column('name'
                          , String(50)), Column('primary_language',
                          String(50)))
        managers = Table('managers', Base.metadata, Column('id',
                         Integer, primary_key=True,
                         test_needs_autoincrement=True), Column('name',
                         String(50)), Column('golf_swing', String(50)))
        punion = polymorphic_union({'engineer': engineers, 'manager'
                                   : managers}, 'type', 'punion')

        class Person(Base, ComparableEntity):

            __table__ = punion
            __mapper_args__ = {'polymorphic_on': punion.c.type}

        class Engineer(Person):

            __table__ = engineers
            __mapper_args__ = {'polymorphic_identity': 'engineer',
                               'concrete': True}

        class Manager(Person):

            __table__ = managers
            __mapper_args__ = {'polymorphic_identity': 'manager',
                               'concrete': True}

        Base.metadata.create_all()
        sess = create_session()
        e1 = Engineer(name='dilbert', primary_language='java')
        e2 = Engineer(name='wally', primary_language='c++')
        m1 = Manager(name='dogbert', golf_swing='fore!')
        e3 = Engineer(name='vlad', primary_language='cobol')
        sess.add_all([e1, e2, m1, e3])
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Person).order_by(Person.name).all(),
            [Engineer(name='dilbert'), Manager(name='dogbert'),
            Engineer(name='vlad'), Engineer(name='wally')])

    def test_concrete_inline_non_polymorphic(self):
        """test the example from the declarative docs."""

        class Person(Base, ComparableEntity):

            __tablename__ = 'people'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            name = Column(String(50))

        class Engineer(Person):

            __tablename__ = 'engineers'
            __mapper_args__ = {'concrete': True}
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            primary_language = Column(String(50))
            name = Column(String(50))

        class Manager(Person):

            __tablename__ = 'manager'
            __mapper_args__ = {'concrete': True}
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)
            golf_swing = Column(String(50))
            name = Column(String(50))

        Base.metadata.create_all()
        sess = create_session()
        e1 = Engineer(name='dilbert', primary_language='java')
        e2 = Engineer(name='wally', primary_language='c++')
        m1 = Manager(name='dogbert', golf_swing='fore!')
        e3 = Engineer(name='vlad', primary_language='cobol')
        sess.add_all([e1, e2, m1, e3])
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(Engineer).order_by(Engineer.name).all(),
            [Engineer(name='dilbert'), Engineer(name='vlad'),
            Engineer(name='wally')])
        eq_(sess.query(Manager).all(), [Manager(name='dogbert')])

def _produce_test(inline, stringbased):

    class ExplicitJoinTest(MappedTest):

        @classmethod
        def define_tables(cls, metadata):
            global User, Address
            Base = decl.declarative_base(metadata=metadata)

            class User(Base, ComparableEntity):

                __tablename__ = 'users'
                id = Column(Integer, primary_key=True,
                            test_needs_autoincrement=True)
                name = Column(String(50))

            class Address(Base, ComparableEntity):

                __tablename__ = 'addresses'
                id = Column(Integer, primary_key=True,
                            test_needs_autoincrement=True)
                email = Column(String(50))
                user_id = Column(Integer, ForeignKey('users.id'))
                if inline:
                    if stringbased:
                        user = relationship('User',
                                primaryjoin='User.id==Address.user_id',
                                backref='addresses')
                    else:
                        user = relationship(User, primaryjoin=User.id
                                == user_id, backref='addresses')

            if not inline:
                compile_mappers()
                if stringbased:
                    Address.user = relationship('User',
                            primaryjoin='User.id==Address.user_id',
                            backref='addresses')
                else:
                    Address.user = relationship(User,
                            primaryjoin=User.id == Address.user_id,
                            backref='addresses')

        @classmethod
        def insert_data(cls):
            params = [dict(zip(('id', 'name'), column_values))
                      for column_values in [(7, 'jack'), (8, 'ed'), (9,
                      'fred'), (10, 'chuck')]]
            User.__table__.insert().execute(params)
            Address.__table__.insert().execute([dict(zip(('id',
                    'user_id', 'email'), column_values))
                    for column_values in [(1, 7, 'jack@bean.com'), (2,
                    8, 'ed@wood.com'), (3, 8, 'ed@bettyboop.com'), (4,
                    8, 'ed@lala.com'), (5, 9, 'fred@fred.com')]])

        def test_aliased_join(self):

            # this query will screw up if the aliasing enabled in
            # query.join() gets applied to the right half of the join
            # condition inside the any(). the join condition inside of
            # any() comes from the "primaryjoin" of the relationship,
            # and should not be annotated with _orm_adapt.
            # PropertyLoader.Comparator will annotate the left side with
            # _orm_adapt, though.

            sess = create_session()
            eq_(sess.query(User).join(User.addresses,
                aliased=True).filter(Address.email == 'ed@wood.com'
                ).filter(User.addresses.any(Address.email
                == 'jack@bean.com')).all(), [])

    ExplicitJoinTest.__name__ = 'ExplicitJoinTest%s%s' % (inline
            and 'Inline' or 'Separate', stringbased and 'String'
            or 'Literal')
    return ExplicitJoinTest

for inline in True, False:
    for stringbased in True, False:
        testclass = _produce_test(inline, stringbased)
        exec '%s = testclass' % testclass.__name__
        del testclass

class DeclarativeReflectionTest(testing.TestBase):

    @classmethod
    def setup_class(cls):
        global reflection_metadata
        reflection_metadata = MetaData(testing.db)
        Table('users', reflection_metadata, Column('id', Integer,
              primary_key=True, test_needs_autoincrement=True),
              Column('name', String(50)), test_needs_fk=True)
        Table(
            'addresses',
            reflection_metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('email', String(50)),
            Column('user_id', Integer, ForeignKey('users.id')),
            test_needs_fk=True,
            )
        Table(
            'imhandles',
            reflection_metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('user_id', Integer),
            Column('network', String(50)),
            Column('handle', String(50)),
            test_needs_fk=True,
            )
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
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)
            addresses = relationship('Address', backref='user')

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            __autoload__ = True
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)

        u1 = User(name='u1', addresses=[Address(email='one'),
                  Address(email='two')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name='u1',
            addresses=[Address(email='one'), Address(email='two')])])
        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(name='u1'))

    def test_rekey(self):
        meta = MetaData(testing.db)

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            __autoload__ = True
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)
            nom = Column('name', String(50), key='nom')
            addresses = relationship('Address', backref='user')

        class Address(Base, ComparableEntity):

            __tablename__ = 'addresses'
            __autoload__ = True
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)

        u1 = User(nom='u1', addresses=[Address(email='one'),
                  Address(email='two')])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(nom='u1',
            addresses=[Address(email='one'), Address(email='two')])])
        a1 = sess.query(Address).filter(Address.email == 'two').one()
        eq_(a1, Address(email='two'))
        eq_(a1.user, User(nom='u1'))
        assert_raises(TypeError, User, name='u3')

    def test_supplied_fk(self):
        meta = MetaData(testing.db)

        class IMHandle(Base, ComparableEntity):

            __tablename__ = 'imhandles'
            __autoload__ = True
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, ComparableEntity):

            __tablename__ = 'users'
            __autoload__ = True
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)
            handles = relationship('IMHandle', backref='user')

        u1 = User(name='u1', handles=[IMHandle(network='blabber',
                  handle='foo'), IMHandle(network='lol', handle='zomg'
                  )])
        sess = create_session()
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        eq_(sess.query(User).all(), [User(name='u1',
            handles=[IMHandle(network='blabber', handle='foo'),
            IMHandle(network='lol', handle='zomg')])])
        a1 = sess.query(IMHandle).filter(IMHandle.handle == 'zomg'
                ).one()
        eq_(a1, IMHandle(network='lol', handle='zomg'))
        eq_(a1.user, User(name='u1'))

class DeclarativeMixinTest(DeclarativeTestBase):

    def test_simple(self):

        class MyMixin(object):

            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

            def foo(self):
                return 'bar' + str(self.id)

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            name = Column(String(100), nullable=False, index=True)

        Base.metadata.create_all()
        session = create_session()
        session.add(MyModel(name='testing'))
        session.flush()
        session.expunge_all()
        obj = session.query(MyModel).one()
        eq_(obj.id, 1)
        eq_(obj.name, 'testing')
        eq_(obj.foo(), 'bar1')

    def test_unique_column(self):

        class MyMixin(object):

            id = Column(Integer, primary_key=True)
            value = Column(String, unique=True)

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'

        assert MyModel.__table__.c.value.unique

    def test_hierarchical_bases(self):

        class MyMixinParent:

            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

            def foo(self):
                return 'bar' + str(self.id)

        class MyMixin(MyMixinParent):

            baz = Column(String(100), nullable=False, index=True)

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            name = Column(String(100), nullable=False, index=True)

        Base.metadata.create_all()
        session = create_session()
        session.add(MyModel(name='testing', baz='fu'))
        session.flush()
        session.expunge_all()
        obj = session.query(MyModel).one()
        eq_(obj.id, 1)
        eq_(obj.name, 'testing')
        eq_(obj.foo(), 'bar1')
        eq_(obj.baz, 'fu')

    def test_not_allowed(self):

        class MyMixin:
            foo = Column(Integer, ForeignKey('bar.id'))

        def go():
            class MyModel(Base, MyMixin):
                __tablename__ = 'foo'

        assert_raises(sa.exc.InvalidRequestError, go)

        class MyRelMixin:
            foo = relationship('Bar')

        def go():
            class MyModel(Base, MyRelMixin):

                __tablename__ = 'foo'

        assert_raises(sa.exc.InvalidRequestError, go)

        class MyDefMixin:
            foo = deferred(Column('foo', String))

        def go():
            class MyModel(Base, MyDefMixin):
                __tablename__ = 'foo'

        assert_raises(sa.exc.InvalidRequestError, go)

        class MyCPropMixin:
            foo = column_property(Column('foo', String))

        def go():
            class MyModel(Base, MyCPropMixin):
                __tablename__ = 'foo'

        assert_raises(sa.exc.InvalidRequestError, go)

    def test_table_name_inherited(self):

        class MyMixin:
            @classproperty
            def __tablename__(cls):
                return cls.__name__.lower()
            id = Column(Integer, primary_key=True)

        class MyModel(Base, MyMixin):
            pass

        eq_(MyModel.__table__.name, 'mymodel')

    def test_table_name_not_inherited(self):

        class MyMixin:
            @classproperty
            def __tablename__(cls):
                return cls.__name__.lower()
            id = Column(Integer, primary_key=True)

        class MyModel(Base, MyMixin):
            __tablename__ = 'overridden'

        eq_(MyModel.__table__.name, 'overridden')

    def test_table_name_inheritance_order(self):

        class MyMixin1:
            @classproperty
            def __tablename__(cls):
                return cls.__name__.lower() + '1'

        class MyMixin2:
            @classproperty
            def __tablename__(cls):
                return cls.__name__.lower() + '2'

        class MyModel(Base, MyMixin1, MyMixin2):
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__table__.name, 'mymodel1')

    def test_table_name_dependent_on_subclass(self):

        class MyHistoryMixin:
            @classproperty
            def __tablename__(cls):
                return cls.parent_name + '_changelog'

        class MyModel(Base, MyHistoryMixin):
            parent_name = 'foo'
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__table__.name, 'foo_changelog')

    def test_table_args_inherited(self):

        class MyMixin:
            __table_args__ = {'mysql_engine': 'InnoDB'}

        class MyModel(Base, MyMixin):
            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__table__.kwargs, {'mysql_engine': 'InnoDB'})

    def test_table_args_inherited_descriptor(self):

        class MyMixin:
            @classproperty
            def __table_args__(cls):
                return {'info': cls.__name__}

        class MyModel(Base, MyMixin):
            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__table__.info, 'MyModel')

    def test_table_args_inherited_single_table_inheritance(self):

        class MyMixin:
            __table_args__ = {'mysql_engine': 'InnoDB'}

        class General(Base, MyMixin):
            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)
            type_ = Column(String(50))
            __mapper__args = {'polymorphic_on': type_}

        class Specific(General):
            __mapper_args__ = {'polymorphic_identity': 'specific'}

        assert Specific.__table__ is General.__table__
        eq_(General.__table__.kwargs, {'mysql_engine': 'InnoDB'})

    def test_table_args_overridden(self):

        class MyMixin:
            __table_args__ = {'mysql_engine': 'Foo'}

        class MyModel(Base, MyMixin):
            __tablename__ = 'test'
            __table_args__ = {'mysql_engine': 'InnoDB'}
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__table__.kwargs, {'mysql_engine': 'InnoDB'})

    def test_mapper_args_classproperty(self):

        class ComputedMapperArgs:
            @classproperty
            def __mapper_args__(cls):
                if cls.__name__ == 'Person':
                    return {'polymorphic_on': cls.discriminator}
                else:
                    return {'polymorphic_identity': cls.__name__}

        class Person(Base, ComputedMapperArgs):
            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            discriminator = Column('type', String(50))

        class Engineer(Person):
            pass

        compile_mappers()
        assert class_mapper(Person).polymorphic_on \
            is Person.__table__.c.type
        eq_(class_mapper(Engineer).polymorphic_identity, 'Engineer')

    def test_mapper_args_classproperty_two(self):

        # same as test_mapper_args_classproperty, but we repeat
        # ComputedMapperArgs on both classes for no apparent reason.

        class ComputedMapperArgs:
            @classproperty
            def __mapper_args__(cls):
                if cls.__name__ == 'Person':
                    return {'polymorphic_on': cls.discriminator}
                else:
                    return {'polymorphic_identity': cls.__name__}

        class Person(Base, ComputedMapperArgs):

            __tablename__ = 'people'
            id = Column(Integer, primary_key=True)
            discriminator = Column('type', String(50))

        class Engineer(Person, ComputedMapperArgs):
            pass

        compile_mappers()
        assert class_mapper(Person).polymorphic_on \
            is Person.__table__.c.type
        eq_(class_mapper(Engineer).polymorphic_identity, 'Engineer')

    def test_table_args_composite(self):

        class MyMixin1:

            __table_args__ = {'info': {'baz': 'bob'}}

        class MyMixin2:

            __table_args__ = {'info': {'foo': 'bar'}}

        class MyModel(Base, MyMixin1, MyMixin2):

            __tablename__ = 'test'

            @classproperty
            def __table_args__(self):
                info = {}
                args = dict(info=info)
                info.update(MyMixin1.__table_args__['info'])
                info.update(MyMixin2.__table_args__['info'])
                return args
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__table__.info, {'foo': 'bar', 'baz': 'bob'})

    def test_mapper_args_inherited(self):

        class MyMixin:

            __mapper_args__ = {'always_refresh': True}

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__mapper__.always_refresh, True)

    def test_mapper_args_inherited_descriptor(self):

        class MyMixin:

            @classproperty
            def __mapper_args__(cls):

                # tenuous, but illustrates the problem!

                if cls.__name__ == 'MyModel':
                    return dict(always_refresh=True)
                else:
                    return dict(always_refresh=False)

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__mapper__.always_refresh, True)

    def test_mapper_args_polymorphic_on_inherited(self):

        class MyMixin:

            type_ = Column(String(50))
            __mapper_args__ = {'polymorphic_on': type_}

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)

        col = MyModel.__mapper__.polymorphic_on
        eq_(col.name, 'type_')
        assert col.table is not None

    def test_mapper_args_overridden(self):

        class MyMixin:

            __mapper_args__ = dict(always_refresh=True)

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            __mapper_args__ = dict(always_refresh=False)
            id = Column(Integer, primary_key=True)

        eq_(MyModel.__mapper__.always_refresh, False)

    def test_mapper_args_composite(self):

        class MyMixin1:

            type_ = Column(String(50))
            __mapper_args__ = {'polymorphic_on': type_}

        class MyMixin2:

            __mapper_args__ = {'always_refresh': True}

        class MyModel(Base, MyMixin1, MyMixin2):

            __tablename__ = 'test'

            @classproperty
            def __mapper_args__(self):
                args = {}
                args.update(MyMixin1.__mapper_args__)
                args.update(MyMixin2.__mapper_args__)
                return args
            id = Column(Integer, primary_key=True)

        col = MyModel.__mapper__.polymorphic_on
        eq_(col.name, 'type_')
        assert col.table is not None
        eq_(MyModel.__mapper__.always_refresh, True)

    def test_single_table_no_propagation(self):

        class IdColumn:

            id = Column(Integer, primary_key=True)

        class Generic(Base, IdColumn):

            __tablename__ = 'base'
            discriminator = Column('type', String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)
            value = Column(Integer())

        class Specific(Generic):

            __mapper_args__ = dict(polymorphic_identity='specific')

        assert Specific.__table__ is Generic.__table__
        eq_(Generic.__table__.c.keys(), ['id', 'type', 'value'])
        assert class_mapper(Specific).polymorphic_on \
            is Generic.__table__.c.type
        eq_(class_mapper(Specific).polymorphic_identity, 'specific')

    def test_joined_table_propagation(self):

        class CommonMixin:

            @classproperty
            def __tablename__(cls):
                return cls.__name__.lower()
            __table_args__ = {'mysql_engine': 'InnoDB'}
            timestamp = Column(Integer)
            id = Column(Integer, primary_key=True)

        class Generic(Base, CommonMixin):

            discriminator = Column('python_type', String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)

        class Specific(Generic):

            __mapper_args__ = dict(polymorphic_identity='specific')
            id = Column(Integer, ForeignKey('generic.id'),
                        primary_key=True)

        eq_(Generic.__table__.name, 'generic')
        eq_(Specific.__table__.name, 'specific')
        eq_(Generic.__table__.c.keys(), ['timestamp', 'id',
            'python_type'])
        eq_(Specific.__table__.c.keys(), ['timestamp', 'id'])
        eq_(Generic.__table__.kwargs, {'mysql_engine': 'InnoDB'})
        eq_(Specific.__table__.kwargs, {'mysql_engine': 'InnoDB'})

    def test_some_propagation(self):

        class CommonMixin:

            @classproperty
            def __tablename__(cls):
                return cls.__name__.lower()
            __table_args__ = {'mysql_engine': 'InnoDB'}
            timestamp = Column(Integer)

        class BaseType(Base, CommonMixin):

            discriminator = Column('type', String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)
            id = Column(Integer, primary_key=True)
            value = Column(Integer())

        class Single(BaseType):

            __tablename__ = None
            __mapper_args__ = dict(polymorphic_identity='type1')

        class Joined(BaseType):

            __mapper_args__ = dict(polymorphic_identity='type2')
            id = Column(Integer, ForeignKey('basetype.id'),
                        primary_key=True)

        eq_(BaseType.__table__.name, 'basetype')
        eq_(BaseType.__table__.c.keys(), ['timestamp', 'type', 'id',
            'value'])
        eq_(BaseType.__table__.kwargs, {'mysql_engine': 'InnoDB'})
        assert Single.__table__ is BaseType.__table__
        eq_(Joined.__table__.name, 'joined')
        eq_(Joined.__table__.c.keys(), ['timestamp', 'id'])
        eq_(Joined.__table__.kwargs, {'mysql_engine': 'InnoDB'})

    def test_non_propagating_mixin(self):

        class NoJoinedTableNameMixin:

            @classproperty
            def __tablename__(cls):
                if decl.has_inherited_table(cls):
                    return None
                return cls.__name__.lower()

        class BaseType(Base, NoJoinedTableNameMixin):

            discriminator = Column('type', String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)
            id = Column(Integer, primary_key=True)
            value = Column(Integer())

        class Specific(BaseType):

            __mapper_args__ = dict(polymorphic_identity='specific')

        eq_(BaseType.__table__.name, 'basetype')
        eq_(BaseType.__table__.c.keys(), ['type', 'id', 'value'])
        assert Specific.__table__ is BaseType.__table__
        assert class_mapper(Specific).polymorphic_on \
            is BaseType.__table__.c.type
        eq_(class_mapper(Specific).polymorphic_identity, 'specific')

    def test_non_propagating_mixin_used_for_joined(self):

        class TableNameMixin:

            @classproperty
            def __tablename__(cls):
                if decl.has_inherited_table(cls) and TableNameMixin \
                    not in cls.__bases__:
                    return None
                return cls.__name__.lower()

        class BaseType(Base, TableNameMixin):

            discriminator = Column('type', String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)
            id = Column(Integer, primary_key=True)
            value = Column(Integer())

        class Specific(BaseType, TableNameMixin):

            __mapper_args__ = dict(polymorphic_identity='specific')
            id = Column(Integer, ForeignKey('basetype.id'),
                        primary_key=True)

        eq_(BaseType.__table__.name, 'basetype')
        eq_(BaseType.__table__.c.keys(), ['type', 'id', 'value'])
        eq_(Specific.__table__.name, 'specific')
        eq_(Specific.__table__.c.keys(), ['id'])

    def test_single_back_propagate(self):

        class ColumnMixin:

            timestamp = Column(Integer)

        class BaseType(Base):

            __tablename__ = 'foo'
            discriminator = Column('type', String(50))
            __mapper_args__ = dict(polymorphic_on=discriminator)
            id = Column(Integer, primary_key=True)

        class Specific(BaseType, ColumnMixin):

            __mapper_args__ = dict(polymorphic_identity='specific')

        eq_(BaseType.__table__.c.keys(), ['type', 'id', 'timestamp'])

    def test_table_in_model_and_same_column_in_mixin(self):

        class ColumnMixin:

            data = Column(Integer)

        class Model(Base, ColumnMixin):

            __table__ = Table('foo', Base.metadata, Column('data',
                              Integer), Column('id', Integer,
                              primary_key=True))

        model_col = Model.__table__.c.data
        mixin_col = ColumnMixin.data
        assert model_col is not mixin_col
        eq_(model_col.name, 'data')
        assert model_col.type.__class__ is mixin_col.type.__class__

    def test_table_in_model_and_different_named_column_in_mixin(self):

        class ColumnMixin:

            tada = Column(Integer)

        def go():

            class Model(Base, ColumnMixin):

                __table__ = Table('foo', Base.metadata, Column('data',
                                  Integer), Column('id', Integer,
                                  primary_key=True))

        assert_raises_message(sa.exc.ArgumentError,
                              "Can't add additional column 'tada' when "
                              "specifying __table__", go)

    def test_table_in_model_overrides_different_typed_column_in_mixin(self):

        class ColumnMixin:

            data = Column(String)

        class Model(Base, ColumnMixin):

            __table__ = Table('foo', Base.metadata, Column('data',
                              Integer), Column('id', Integer,
                              primary_key=True))

        model_col = Model.__table__.c.data
        mixin_col = ColumnMixin.data
        assert model_col is not mixin_col
        eq_(model_col.name, 'data')
        assert model_col.type.__class__ is Integer

    def test_mixin_column_ordering(self):

        class Foo(object):

            col1 = Column(Integer)
            col3 = Column(Integer)

        class Bar(object):

            col2 = Column(Integer)
            col4 = Column(Integer)

        class Model(Base, Foo, Bar):

            id = Column(Integer, primary_key=True)
            __tablename__ = 'model'

        eq_(Model.__table__.c.keys(), ['col1', 'col3', 'col2', 'col4',
            'id'])

class DeclarativeMixinPropertyTest(DeclarativeTestBase):

    def test_column_property(self):

        class MyMixin(object):

            @classproperty
            def prop_hoho(cls):
                return column_property(Column('prop', String(50)))

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        class MyOtherModel(Base, MyMixin):

            __tablename__ = 'othertest'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        assert MyModel.__table__.c.prop is not None
        assert MyOtherModel.__table__.c.prop is not None
        assert MyModel.__table__.c.prop \
            is not MyOtherModel.__table__.c.prop
        assert MyModel.prop_hoho.property.columns \
            == [MyModel.__table__.c.prop]
        assert MyOtherModel.prop_hoho.property.columns \
            == [MyOtherModel.__table__.c.prop]
        assert MyModel.prop_hoho.property \
            is not MyOtherModel.prop_hoho.property
        Base.metadata.create_all()
        sess = create_session()
        m1, m2 = MyModel(prop_hoho='foo'), MyOtherModel(prop_hoho='bar')
        sess.add_all([m1, m2])
        sess.flush()
        eq_(sess.query(MyModel).filter(MyModel.prop_hoho == 'foo'
            ).one(), m1)
        eq_(sess.query(MyOtherModel).filter(MyOtherModel.prop_hoho
            == 'bar').one(), m2)

    def test_doc(self):
        """test documentation transfer.
        
        the documentation situation with @classproperty is problematic.
        at least see if mapped subclasses get the doc.
        
        """

        class MyMixin(object):

            @classproperty
            def type_(cls):
                """this is a document."""

                return Column(String(50))

            @classproperty
            def t2(cls):
                """this is another document."""

                return column_property(Column(String(50)))

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)

        compile_mappers()
        eq_(MyModel.type_.__doc__, """this is a document.""")
        eq_(MyModel.t2.__doc__, """this is another document.""")

    def test_column_in_mapper_args(self):

        class MyMixin(object):

            @classproperty
            def type_(cls):
                return Column(String(50))
            __mapper_args__ = {'polymorphic_on': type_}

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            id = Column(Integer, primary_key=True)

        compile_mappers()
        col = MyModel.__mapper__.polymorphic_on
        eq_(col.name, 'type_')
        assert col.table is not None

    def test_deferred(self):

        class MyMixin(object):

            @classproperty
            def data(cls):
                return deferred(Column('data', String(50)))

        class MyModel(Base, MyMixin):

            __tablename__ = 'test'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        Base.metadata.create_all()
        sess = create_session()
        sess.add_all([MyModel(data='d1'), MyModel(data='d2')])
        sess.flush()
        sess.expunge_all()
        d1, d2 = sess.query(MyModel).order_by(MyModel.data)
        assert 'data' not in d1.__dict__
        assert d1.data == 'd1'
        assert 'data' in d1.__dict__

    def _test_relationship(self, usestring):

        class RefTargetMixin(object):

            @classproperty
            def target_id(cls):
                return Column('target_id', ForeignKey('target.id'))
            if usestring:

                @classproperty
                def target(cls):
                    return relationship('Target',
                            primaryjoin='Target.id==%s.target_id'
                            % cls.__name__)
            else:

                @classproperty
                def target(cls):
                    return relationship('Target')

        class Foo(Base, RefTargetMixin):

            __tablename__ = 'foo'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        class Bar(Base, RefTargetMixin):

            __tablename__ = 'bar'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        class Target(Base):

            __tablename__ = 'target'
            id = Column(Integer, primary_key=True,
                        test_needs_autoincrement=True)

        Base.metadata.create_all()
        sess = create_session()
        t1, t2 = Target(), Target()
        f1, f2, b1 = Foo(target=t1), Foo(target=t2), Bar(target=t1)
        sess.add_all([f1, f2, b1])
        sess.flush()
        eq_(sess.query(Foo).filter(Foo.target == t2).one(), f2)
        eq_(sess.query(Bar).filter(Bar.target == t2).first(), None)
        sess.expire_all()
        eq_(f1.target, t1)

    def test_relationship(self):
        self._test_relationship(False)

    def test_relationship_primryjoin(self):
        self._test_relationship(True)
        