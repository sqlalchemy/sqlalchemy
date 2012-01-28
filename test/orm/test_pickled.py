from test.lib.testing import eq_
from sqlalchemy.util import pickle
import sqlalchemy as sa
from test.lib import testing
from test.lib.util import picklers
from test.lib.testing import assert_raises_message
from sqlalchemy import Integer, String, ForeignKey, exc, MetaData
from test.lib.schema import Table, Column
from sqlalchemy.orm import mapper, relationship, create_session, \
                            sessionmaker, attributes, interfaces,\
                            clear_mappers, exc as orm_exc,\
                            configure_mappers, Session, lazyload_all,\
                            lazyload, aliased
from test.lib import fixtures
from test.orm import _fixtures
from test.lib.pickleable import User, Address, Dingaling, Order, \
    Child1, Child2, Parent, Screen, EmailUser


class PickleTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('name', String(30), nullable=False),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('addresses', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('email_address', String(50), nullable=False),
              test_needs_acid=True,
              test_needs_fk=True
        )
        Table('orders', metadata,
                  Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
                  Column('user_id', None, ForeignKey('users.id')),
                  Column('address_id', None, ForeignKey('addresses.id')),
                  Column('description', String(30)),
                  Column('isopen', Integer),
                  test_needs_acid=True,
                  test_needs_fk=True
        )
        Table("dingalings", metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('address_id', None, ForeignKey('addresses.id')),
              Column('data', String(30)),
              test_needs_acid=True,
              test_needs_fk=True
        )


    def test_transient(self):
        users, addresses = (self.tables.users,
                                self.tables.addresses)

        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))

        u2 = pickle.loads(pickle.dumps(u1))
        sess.add(u2)
        sess.flush()

        sess.expunge_all()

        eq_(u1, sess.query(User).get(u2.id))

    def test_no_mappers(self):
        users = self.tables.users


        umapper = mapper(User, users)
        u1 = User(name='ed')
        u1_pickled = pickle.dumps(u1, -1)

        clear_mappers()

        assert_raises_message(
            orm_exc.UnmappedInstanceError,
            "Cannot deserialize object of type <class 'test.lib.pickleable.User'> - no mapper()",
            pickle.loads, u1_pickled)

    def test_no_instrumentation(self):
        users = self.tables.users


        umapper = mapper(User, users)
        u1 = User(name='ed')
        u1_pickled = pickle.dumps(u1, -1)

        clear_mappers()

        umapper = mapper(User, users)

        u1 = pickle.loads(u1_pickled)
        # this fails unless the InstanceState
        # compiles the mapper
        eq_(str(u1), "User(name='ed')")

    def test_serialize_path(self):
        users, addresses = (self.tables.users,
                                self.tables.addresses)

        umapper = mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        amapper = mapper(Address, addresses)

        # this is a "relationship" path with mapper, key, mapper, key
        p1 = (umapper, 'addresses', amapper, 'email_address')
        eq_(
            interfaces.deserialize_path(interfaces.serialize_path(p1)),
            p1
        )

        # this is a "mapper" path with mapper, key, mapper, no key
        # at the end.
        p2 = (umapper, 'addresses', amapper, )
        eq_(
            interfaces.deserialize_path(interfaces.serialize_path(p2)),
            p2
        )

        # test a blank path
        p3 = ()
        eq_(
            interfaces.deserialize_path(interfaces.serialize_path(p3)),
            p3
        )

    def test_class_deferred_cols(self):
        addresses, users = (self.tables.addresses,
                                self.tables.users)

        mapper(User, users, properties={
            'name':sa.orm.deferred(users.c.name),
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses, properties={
            'email_address':sa.orm.deferred(addresses.c.email_address)
        })
        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()
        u1 = sess.query(User).get(u1.id)
        assert 'name' not in u1.__dict__
        assert 'addresses' not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        sess2.add(u2)
        eq_(u2.name, 'ed')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        u2 = sess2.merge(u2, load=False)
        eq_(u2.name, 'ed')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

    def test_instance_lazy_relation_loaders(self):
        users, addresses = (self.tables.users,
                                self.tables.addresses)

        mapper(User, users, properties={
            'addresses':relationship(Address, lazy='noload')
        })
        mapper(Address, addresses)

        sess = Session()
        u1 = User(name='ed', addresses=[
                        Address(
                            email_address='ed@bar.com', 
                        )
                ])

        sess.add(u1)
        sess.commit()
        sess.close()

        u1 = sess.query(User).options(
                                lazyload(User.addresses)
                            ).first()
        u2 = pickle.loads(pickle.dumps(u1))

        sess = Session()
        sess.add(u2)
        assert u2.addresses

    def test_instance_deferred_cols(self):
        users, addresses = (self.tables.users,
                                self.tables.addresses)

        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        u1 = sess.query(User).\
                options(sa.orm.defer('name'), 
                        sa.orm.defer('addresses.email_address')).\
                        get(u1.id)
        assert 'name' not in u1.__dict__
        assert 'addresses' not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        sess2.add(u2)
        eq_(u2.name, 'ed')
        assert 'addresses' not in u2.__dict__
        ad = u2.addresses[0]
        assert 'email_address' not in ad.__dict__
        eq_(ad.email_address, 'ed@bar.com')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        u2 = sess2.merge(u2, load=False)
        eq_(u2.name, 'ed')
        assert 'addresses' not in u2.__dict__
        ad = u2.addresses[0]

        # mapper options now transmit over merge(),
        # new as of 0.6, so email_address is deferred.
        assert 'email_address' not in ad.__dict__

        eq_(ad.email_address, 'ed@bar.com')
        eq_(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

    def test_pickle_protocols(self):
        users, addresses = (self.tables.users,
                                self.tables.addresses)

        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = sessionmaker()()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.commit()

        u1 = sess.query(User).first()
        u1.addresses

        for loads, dumps in picklers():
            u2 = loads(dumps(u1))
            eq_(u1, u2)


    def test_options_with_descriptors(self):
        users, addresses, dingalings = (self.tables.users,
                                self.tables.addresses,
                                self.tables.dingalings)

        mapper(User, users, properties={
            'addresses':relationship(Address, backref="user")
        })
        mapper(Address, addresses, properties={
            'dingaling':relationship(Dingaling)
        })
        mapper(Dingaling, dingalings)
        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.expunge_all()

        for opt in [
            sa.orm.joinedload(User.addresses),
            sa.orm.joinedload("addresses"),
            sa.orm.defer("name"),
            sa.orm.defer(User.name),
            sa.orm.joinedload("addresses", Address.dingaling),
        ]:
            opt2 = pickle.loads(pickle.dumps(opt))
            eq_(opt.key, opt2.key)

        u1 = sess.query(User).options(opt).first()
        u2 = pickle.loads(pickle.dumps(u1))

    def test_collection_setstate(self):
        """test a particular cycle that requires CollectionAdapter 
        to not rely upon InstanceState to deserialize."""

        m = MetaData()
        c1 = Table('c1', m, 
            Column('parent_id', String, 
                        ForeignKey('p.id'), primary_key=True)
        )
        c2 = Table('c2', m,
            Column('parent_id', String, 
                        ForeignKey('p.id'), primary_key=True)
        )
        p = Table('p', m,
            Column('id', String, primary_key=True)
        )

        mapper(Parent, p, properties={
            'children1':relationship(Child1),
            'children2':relationship(Child2)
        })
        mapper(Child1, c1)
        mapper(Child2, c2)

        obj = Parent()
        screen1 = Screen(obj)
        screen1.errors = [obj.children1, obj.children2]
        screen2 = Screen(Child2(), screen1)
        pickle.loads(pickle.dumps(screen2))

    def test_exceptions(self):
        class Foo(object):
            pass
        users = self.tables.users
        mapper(User, users)

        for sa_exc in (
            orm_exc.UnmappedInstanceError(Foo()),
            orm_exc.UnmappedClassError(Foo),
            orm_exc.ObjectDeletedError(attributes.instance_state(User())),
        ):
            for loads, dumps in picklers():
                repickled = loads(dumps(sa_exc))
                eq_(repickled.args[0], sa_exc.args[0])

class PolymorphicDeferredTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
            Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
            Column('name', String(30)),
            Column('type', String(30)))
        Table('email_users', metadata,
            Column('id', Integer, ForeignKey('users.id'), primary_key=True),
            Column('email_address', String(30)))


    def test_polymorphic_deferred(self):
        email_users, users = (self.tables.email_users,
                                self.tables.users,
                                )

        mapper(User, users, polymorphic_identity='user', polymorphic_on=users.c.type)
        mapper(EmailUser, email_users, inherits=User, polymorphic_identity='emailuser')

        eu = EmailUser(name="user1", email_address='foo@bar.com')
        sess = create_session()
        sess.add(eu)
        sess.flush()
        sess.expunge_all()

        eu = sess.query(User).first()
        eu2 = pickle.loads(pickle.dumps(eu))
        sess2 = create_session()
        sess2.add(eu2)
        assert 'email_address' not in eu2.__dict__
        eq_(eu2.email_address, 'foo@bar.com')

class TupleLabelTest(_fixtures.FixtureTest):
    @classmethod
    def setup_classes(cls):
        pass

    @classmethod
    def setup_mappers(cls):
        users, addresses, orders = cls.tables.users, cls.tables.addresses, cls.tables.orders
        mapper(User, users, properties={
            'addresses':relationship(Address, backref='user', order_by=addresses.c.id),
            'orders':relationship(Order, backref='user', order_by=orders.c.id), # o2m, m2o
        })
        mapper(Address, addresses)
        mapper(Order, orders, properties={
            'address':relationship(Address),  # m2o
        })

    def test_tuple_labeling(self):
        users = self.tables.users
        sess = create_session()

        # test pickle + all the protocols !
        for pickled in False, -1, 0, 1, 2:
            for row in sess.query(User, Address).join(User.addresses).all():
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))

                eq_(row.keys(), ['User', 'Address'])
                eq_(row.User, row[0])
                eq_(row.Address, row[1])

            for row in sess.query(User.name, User.id.label('foobar')):
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))
                eq_(row.keys(), ['name', 'foobar'])
                eq_(row.name, row[0])
                eq_(row.foobar, row[1])

            for row in sess.query(User).values(User.name, User.id.label('foobar')):
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))
                eq_(row.keys(), ['name', 'foobar'])
                eq_(row.name, row[0])
                eq_(row.foobar, row[1])

            oalias = aliased(Order)
            for row in sess.query(User, oalias).join(User.orders).all():
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))
                eq_(row.keys(), ['User'])
                eq_(row.User, row[0])

            oalias = aliased(Order, name='orders')
            for row in sess.query(User, oalias).join(oalias, User.orders).all():
                if pickled is not False:
                    row = pickle.loads(pickle.dumps(row, pickled))
                eq_(row.keys(), ['User', 'orders'])
                eq_(row.User, row[0])
                eq_(row.orders, row[1])

            # test here that first col is not labeled, only
            # one name in keys, matches correctly
            for row in sess.query(User.name + 'hoho', User.name):
                eq_(row.keys(), ['name'])
                eq_(row[0], row.name + 'hoho')

            if pickled is not False:
                ret = sess.query(User, Address).join(User.addresses).all()
                pickle.loads(pickle.dumps(ret, pickled))

class CustomSetupTeardownTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table('users', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('name', String(30), nullable=False),
              test_needs_acid=True,
              test_needs_fk=True
        )

        Table('addresses', metadata,
              Column('id', Integer, primary_key=True, test_needs_autoincrement=True),
              Column('user_id', None, ForeignKey('users.id')),
              Column('email_address', String(50), nullable=False),
              test_needs_acid=True,
              test_needs_fk=True
        )
    def test_rebuild_state(self):
        """not much of a 'test', but illustrate how to 
        remove instance-level state before pickling.

        """

        users = self.tables.users

        mapper(User, users)

        u1 = User()
        attributes.manager_of_class(User).teardown_instance(u1)
        assert not u1.__dict__
        u2 = pickle.loads(pickle.dumps(u1))
        attributes.manager_of_class(User).setup_instance(u2)
        assert attributes.instance_state(u2)

