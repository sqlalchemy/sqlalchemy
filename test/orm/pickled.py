import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exceptions
from sqlalchemy.orm import *
from testlib import *
from testlib.fixtures import *
import pickle

class EmailUser(User):
    pass

class PickleTest(FixtureTest):
    keep_mappers = False
    keep_data = False

    def test_transient(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))

        u2 = pickle.loads(pickle.dumps(u1))
        sess.save(u2)
        sess.flush()

        sess.clear()

        self.assertEquals(u1, sess.query(User).get(u2.id))

    def test_class_deferred_cols(self):
        mapper(User, users, properties={
            'name':deferred(users.c.name),
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses, properties={
            'email_address':deferred(addresses.c.email_address)
        })
        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.save(u1)
        sess.flush()
        sess.clear()
        u1 = sess.query(User).get(u1.id)
        assert 'name' not in u1.__dict__
        assert 'addresses' not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        sess2.update(u2)
        self.assertEquals(u2.name, 'ed')
        self.assertEquals(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        u2 = sess2.merge(u2, dont_load=True)
        self.assertEquals(u2.name, 'ed')
        self.assertEquals(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

    def test_instance_deferred_cols(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.save(u1)
        sess.flush()
        sess.clear()

        u1 = sess.query(User).options(defer('name'), defer('addresses.email_address')).get(u1.id)
        assert 'name' not in u1.__dict__
        assert 'addresses' not in u1.__dict__

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        sess2.update(u2)
        self.assertEquals(u2.name, 'ed')
        assert 'addresses' not in u2.__dict__
        ad = u2.addresses[0]
        assert 'email_address' not in ad.__dict__
        self.assertEquals(ad.email_address, 'ed@bar.com')
        self.assertEquals(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))

        u2 = pickle.loads(pickle.dumps(u1))
        sess2 = create_session()
        u2 = sess2.merge(u2, dont_load=True)
        self.assertEquals(u2.name, 'ed')
        assert 'addresses' not in u2.__dict__
        ad = u2.addresses[0]
        assert 'email_address' in ad.__dict__  # mapper options dont transmit over merge() right now
        self.assertEquals(ad.email_address, 'ed@bar.com')
        self.assertEquals(u2, User(name='ed', addresses=[Address(email_address='ed@bar.com')]))


class PolymorphicDeferredTest(ORMTest):
    def define_tables(self, metadata):
        global users, email_users
        users = Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)),
            Column('type', String(30)),
            )
        email_users = Table('email_users', metadata,
            Column('id', Integer, ForeignKey('users.id'), primary_key=True),
            Column('email_address', String(30))
            )

    def test_polymorphic_deferred(self):
        mapper(User, users, polymorphic_identity='user', polymorphic_on=users.c.type, polymorphic_fetch='deferred')
        mapper(EmailUser, email_users, inherits=User, polymorphic_identity='emailuser')

        eu = EmailUser(name="user1", email_address='foo@bar.com')
        sess = create_session()
        sess.save(eu)
        sess.flush()
        sess.clear()

        eu = sess.query(User).first()
        eu2 = pickle.loads(pickle.dumps(eu))
        sess2 = create_session()
        sess2.update(eu2)
        assert 'email_address' not in eu2.__dict__
        self.assertEquals(eu2.email_address, 'foo@bar.com')


if __name__ == '__main__':
    testenv.main()
