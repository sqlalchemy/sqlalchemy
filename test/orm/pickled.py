import testenv; testenv.configure_for_tests()
import pickle
from testlib import sa, testing
from testlib.sa import Table, Column, Integer, String, ForeignKey
from testlib.sa.orm import mapper, relation, create_session
from orm import _base, _fixtures


User, EmailUser = None, None

class PickleTest(_fixtures.FixtureTest):
    run_inserts = None
    
    @testing.resolve_artifact_names
    def test_transient(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))

        u2 = pickle.loads(pickle.dumps(u1))
        sess.add(u2)
        sess.flush()

        sess.clear()

        self.assertEquals(u1, sess.query(User).get(u2.id))

    @testing.resolve_artifact_names
    def test_class_deferred_cols(self):
        mapper(User, users, properties={
            'name':sa.orm.deferred(users.c.name),
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses, properties={
            'email_address':sa.orm.deferred(addresses.c.email_address)
        })
        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
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

    @testing.resolve_artifact_names
    def test_instance_deferred_cols(self):
        mapper(User, users, properties={
            'addresses':relation(Address, backref="user")
        })
        mapper(Address, addresses)

        sess = create_session()
        u1 = User(name='ed')
        u1.addresses.append(Address(email_address='ed@bar.com'))
        sess.add(u1)
        sess.flush()
        sess.clear()

        u1 = sess.query(User).options(sa.orm.defer('name'), sa.orm.defer('addresses.email_address')).get(u1.id)
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


class PolymorphicDeferredTest(_base.MappedTest):
    def define_tables(self, metadata):
        Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(30)),
            Column('type', String(30)))
        Table('email_users', metadata,
            Column('id', Integer, ForeignKey('users.id'), primary_key=True),
            Column('email_address', String(30)))

    def setup_classes(self):
        global User, EmailUser
        class User(_base.BasicEntity):
            pass

        class EmailUser(User):
            pass

    def tearDownAll(self):
        global User, EmailUser
        User, EmailUser = None, None
        _base.MappedTest.tearDownAll(self)

    @testing.resolve_artifact_names
    def test_polymorphic_deferred(self):
        mapper(User, users, polymorphic_identity='user', polymorphic_on=users.c.type)
        mapper(EmailUser, email_users, inherits=User, polymorphic_identity='emailuser')

        eu = EmailUser(name="user1", email_address='foo@bar.com')
        sess = create_session()
        sess.add(eu)
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
