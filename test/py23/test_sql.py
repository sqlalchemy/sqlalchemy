import unittest
from sqlalchemy import *

class SQLTest(unittest.TestCase):
    def setUp(self):
        global engine
        engine = create_engine('sqlite://')

    def tearDown(self):
        engine.dispose()

    def test_select_roundtrip(self):
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
        users.insert().execute(id=1, name='ed')
        users.insert().execute(id=2, name='wendy')
        addresses.insert().execute(id=1, user_id=1, email='ed@foo.com')
        addresses.insert().execute(id=2, user_id=2, email='wendy@foo.com')
        self.assertEquals(users.join(addresses).select().execute().fetchall(), 
           [(1, 'ed', 1, 'ed@foo.com', 1), (2, 'wendy', 2, 'wendy@foo.com', 2)]
        )

        

if __name__ == '__main__':
    unittest.main()
