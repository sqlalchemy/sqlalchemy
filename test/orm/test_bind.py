from sqlalchemy.testing import assert_raises_message
from sqlalchemy import MetaData, Integer
from sqlalchemy.testing.schema import Table
from sqlalchemy.testing.schema import Column
from sqlalchemy.orm import mapper, create_session
import sqlalchemy as sa
from sqlalchemy import testing
from sqlalchemy.testing import fixtures, eq_, engines
from sqlalchemy.orm import relationship, Session, backref, sessionmaker
from test.orm import _fixtures


class BindIntegrationTest(_fixtures.FixtureTest):
    run_inserts = None

    def test_mapped_binds(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        # ensure tables are unbound
        m2 = sa.MetaData()
        users_unbound = users.tometadata(m2)
        addresses_unbound = addresses.tometadata(m2)

        mapper(Address, addresses_unbound)
        mapper(User, users_unbound, properties={
            'addresses': relationship(Address,
                                      backref=backref("user", cascade="all"),
                                      cascade="all")})

        sess = Session(binds={User: self.metadata.bind,
                              Address: self.metadata.bind})

        u1 = User(id=1, name='ed')
        sess.add(u1)
        eq_(sess.query(User).filter(User.id == 1).all(),
            [User(id=1, name='ed')])

        # test expression binding

        sess.execute(users_unbound.insert(), params=dict(id=2,
                                                         name='jack'))
        eq_(sess.execute(users_unbound.select(users_unbound.c.id
                                              == 2)).fetchall(), [(2, 'jack')])

        eq_(sess.execute(users_unbound.select(User.id == 2)).fetchall(),
            [(2, 'jack')])

        sess.execute(users_unbound.delete())
        eq_(sess.execute(users_unbound.select()).fetchall(), [])

        sess.close()

    def test_table_binds(self):
        Address, addresses, users, User = (self.classes.Address,
                                           self.tables.addresses,
                                           self.tables.users,
                                           self.classes.User)

        # ensure tables are unbound
        m2 = sa.MetaData()
        users_unbound = users.tometadata(m2)
        addresses_unbound = addresses.tometadata(m2)

        mapper(Address, addresses_unbound)
        mapper(User, users_unbound, properties={
            'addresses': relationship(Address,
                                      backref=backref("user", cascade="all"),
                                      cascade="all")})

        Session = sessionmaker(binds={users_unbound: self.metadata.bind,
                                      addresses_unbound: self.metadata.bind})
        sess = Session()

        u1 = User(id=1, name='ed')
        sess.add(u1)
        eq_(sess.query(User).filter(User.id == 1).all(),
            [User(id=1, name='ed')])

        sess.execute(users_unbound.insert(), params=dict(id=2, name='jack'))

        eq_(sess.execute(users_unbound.select(users_unbound.c.id
                                              == 2)).fetchall(), [(2, 'jack')])

        eq_(sess.execute(users_unbound.select(User.id == 2)).fetchall(),
            [(2, 'jack')])

        sess.execute(users_unbound.delete())
        eq_(sess.execute(users_unbound.select()).fetchall(), [])

        sess.close()

    def test_bind_from_metadata(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)

        session = create_session()
        session.execute(users.insert(), dict(name='Johnny'))

        assert len(session.query(User).filter_by(name='Johnny').all()) == 1

        session.execute(users.delete())

        assert len(session.query(User).filter_by(name='Johnny').all()) == 0
        session.close()

    def test_bind_arguments(self):
        users, Address, addresses, User = (self.tables.users,
                                           self.classes.Address,
                                           self.tables.addresses,
                                           self.classes.User)

        mapper(User, users)
        mapper(Address, addresses)

        e1 = engines.testing_engine()
        e2 = engines.testing_engine()
        e3 = engines.testing_engine()

        sess = Session(e3)
        sess.bind_mapper(User, e1)
        sess.bind_mapper(Address, e2)

        assert sess.connection().engine is e3
        assert sess.connection(bind=e1).engine is e1
        assert sess.connection(mapper=Address, bind=e1).engine is e1
        assert sess.connection(mapper=Address).engine is e2
        assert sess.connection(clause=addresses.select()).engine is e2
        assert sess.connection(mapper=User,
                               clause=addresses.select()).engine is e1
        assert sess.connection(mapper=User,
                               clause=addresses.select(),
                               bind=e2).engine is e2

        sess.close()

    @engines.close_open_connections
    def test_bound_connection(self):
        users, User = self.tables.users, self.classes.User

        mapper(User, users)
        c = testing.db.connect()
        sess = create_session(bind=c)
        sess.begin()
        transaction = sess.transaction
        u = User(name='u1')
        sess.add(u)
        sess.flush()
        assert transaction._connection_for_bind(testing.db) \
            is transaction._connection_for_bind(c) is c

        assert_raises_message(sa.exc.InvalidRequestError,
                              'Session already has a Connection '
                              'associated',
                              transaction._connection_for_bind,
                              testing.db.connect())
        transaction.rollback()
        assert len(sess.query(User).all()) == 0
        sess.close()

    def test_bound_connection_transactional(self):
        User, users = self.classes.User, self.tables.users

        mapper(User, users)
        c = testing.db.connect()

        sess = create_session(bind=c, autocommit=False)
        u = User(name='u1')
        sess.add(u)
        sess.flush()
        sess.close()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 0

        sess = create_session(bind=c, autocommit=False)
        u = User(name='u2')
        sess.add(u)
        sess.flush()
        sess.commit()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 1
        c.execute("delete from users")
        assert c.scalar("select count(1) from users") == 0

        c = testing.db.connect()

        trans = c.begin()
        sess = create_session(bind=c, autocommit=True)
        u = User(name='u3')
        sess.add(u)
        sess.flush()
        assert c.in_transaction()
        trans.commit()
        assert not c.in_transaction()
        assert c.scalar("select count(1) from users") == 1


class SessionBindTest(fixtures.MappedTest):

    @classmethod
    def define_tables(cls, metadata):
        Table('test_table', metadata,
              Column('id', Integer, primary_key=True,
                     test_needs_autoincrement=True),
              Column('data', Integer))

    @classmethod
    def setup_classes(cls):
        class Foo(cls.Basic):
            pass

    @classmethod
    def setup_mappers(cls):
        test_table, Foo = cls.tables.test_table, cls.classes.Foo

        meta = MetaData()
        test_table.tometadata(meta)

        assert meta.tables['test_table'].bind is None
        mapper(Foo, meta.tables['test_table'])

    def test_session_bind(self):
        Foo = self.classes.Foo

        engine = self.metadata.bind

        for bind in (engine, engine.connect()):
            try:
                sess = create_session(bind=bind)
                assert sess.bind is bind
                f = Foo()
                sess.add(f)
                sess.flush()
                assert sess.query(Foo).get(f.id) is f
            finally:
                if hasattr(bind, 'close'):
                    bind.close()

    def test_session_unbound(self):
        Foo = self.classes.Foo

        sess = create_session()
        sess.add(Foo())
        assert_raises_message(
            sa.exc.UnboundExecutionError,
            ('Could not locate a bind configured on Mapper|Foo|test_table '
             'or this Session'),
            sess.flush)
