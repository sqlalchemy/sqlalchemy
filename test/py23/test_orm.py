import unittest
from sqlalchemy import *
from sqlalchemy.orm import *

class ORMTest(unittest.TestCase):
    def setUp(self):
        global engine, users, addresses
        engine = create_engine('sqlite://')
        meta = MetaData(engine)
        users = Table('users', meta,
              Column('id', Integer, primary_key=True),
              Column('name', String(255))
        )
        addresses = Table('addresses', meta,
              Column('id', Integer, primary_key=True),
              Column('email', String(255)),
              Column('user_id', Integer, ForeignKey('users.id'))
        )

        meta.create_all()

 
    def tearDown(self):
        engine.dispose()

    def test_orm_roundtrip(self):
        class User(object):
            def __init__(self, name):
                self.name = name
        class Address(object):
            def __init__(self, email):
                self.email = email

        mapper(User, users, properties={
           'addresses':relation(Address, backref='user', cascade="all, delete-orphan")
        })
        mapper(Address, addresses)

        sess = sessionmaker()()
        u1 = User('ed')
        u2 = User('wendy')
        u1.addresses.append(Address('ed@ed.com'))
        u2.addresses.append(Address('wendy@wendy.com'))
        sess.add_all([u1, u2])
        sess.commit()

        self.assertEquals(sess.query(User).order_by(User.name).all(), [u1, u2])

	sess.clear()
        u1 = sess.query(User).get(1)
        self.assertEquals(u1.name, 'ed')
        self.assertEquals(u1.addresses[0].email, 'ed@ed.com')
        sess.delete(u1)
        sess.commit()
        self.assertEquals(sess.query(User).count(), 1)
               

if __name__ == '__main__':
    unittest.main()
