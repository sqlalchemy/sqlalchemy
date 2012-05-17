from test.lib.testing import eq_, assert_raises
from sqlalchemy.ext import declarative as decl
from test.lib import testing
from sqlalchemy import MetaData, Integer, String, ForeignKey
from test.lib.schema import Table, Column
from sqlalchemy.orm import relationship, create_session, \
    clear_mappers, \
    Session
from test.lib import fixtures

class DeclarativeReflectionBase(fixtures.TablesTest):
    def setup(self):
        global Base
        Base = decl.declarative_base(testing.db)

class DeclarativeReflectionTest(DeclarativeReflectionBase):

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata, 
            Column('id', Integer,
                primary_key=True, test_needs_autoincrement=True),
              Column('name', String(50)), test_needs_fk=True)
        Table(
            'addresses',
            metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('email', String(50)),
            Column('user_id', Integer, ForeignKey('users.id')),
            test_needs_fk=True,
            )
        Table(
            'imhandles',
            metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('user_id', Integer),
            Column('network', String(50)),
            Column('handle', String(50)),
            test_needs_fk=True,
            )

    def test_basic(self):
        meta = MetaData(testing.db)

        class User(Base, fixtures.ComparableEntity):

            __tablename__ = 'users'
            __autoload__ = True
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)
            addresses = relationship('Address', backref='user')

        class Address(Base, fixtures.ComparableEntity):

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

        class User(Base, fixtures.ComparableEntity):

            __tablename__ = 'users'
            __autoload__ = True
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)
            nom = Column('name', String(50), key='nom')
            addresses = relationship('Address', backref='user')

        class Address(Base, fixtures.ComparableEntity):

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

        class IMHandle(Base, fixtures.ComparableEntity):

            __tablename__ = 'imhandles'
            __autoload__ = True
            if testing.against('oracle', 'firebird'):
                id = Column('id', Integer, primary_key=True,
                            test_needs_autoincrement=True)
            user_id = Column('user_id', Integer, ForeignKey('users.id'))

        class User(Base, fixtures.ComparableEntity):

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

class DeferredReflectBase(DeclarativeReflectionBase):
    def teardown(self):
        super(DeferredReflectBase,self).teardown()
        from sqlalchemy.ext.declarative import _MapperConfig
        _MapperConfig.configs.clear()

class DeferredReflectionTest(DeferredReflectBase):

    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata, 
            Column('id', Integer,
                primary_key=True, test_needs_autoincrement=True),
              Column('name', String(50)), test_needs_fk=True)
        Table(
            'addresses',
            metadata,
            Column('id', Integer, primary_key=True,
                   test_needs_autoincrement=True),
            Column('email', String(50)),
            Column('user_id', Integer, ForeignKey('users.id')),
            test_needs_fk=True,
            )

    def _roundtrip(self):

        User = Base._decl_class_registry['User']
        Address = Base._decl_class_registry['Address']

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

    def test_basic_deferred(self):
        class User(decl.DeferredReflection, fixtures.ComparableEntity, 
                            Base):
            __tablename__ = 'users'
            addresses = relationship("Address", backref="user")

        class Address(decl.DeferredReflection, fixtures.ComparableEntity, 
                            Base):
            __tablename__ = 'addresses'

        decl.DeferredReflection.prepare(testing.db)
        self._roundtrip()

    def test_abstract_base(self):
        class DefBase(decl.DeferredReflection, Base):
            __abstract__ = True

        class OtherDefBase(decl.DeferredReflection, Base):
            __abstract__ = True

        class User(fixtures.ComparableEntity, DefBase):
            __tablename__ = 'users'
            addresses = relationship("Address", backref="user")

        class Address(fixtures.ComparableEntity, DefBase):
            __tablename__ = 'addresses'

        class Fake(OtherDefBase):
            __tablename__ = 'nonexistent'

        DefBase.prepare(testing.db)
        self._roundtrip()
