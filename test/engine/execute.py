
import testbase
import unittest, sys, datetime
import tables
db = testbase.db
from sqlalchemy import *


class ExecuteTest(testbase.PersistTest):
    def setUpAll(self):
        global users, metadata
        metadata = BoundMetaData(testbase.db)
        users = Table('users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
            mysql_engine='InnoDB'
        )
        metadata.create_all()
    
    def tearDown(self):
        testbase.db.connect().execute(users.delete())
    def tearDownAll(self):
        metadata.drop_all()
        
    @testbase.supported('sqlite')
    def test_raw_qmark(self):
        for conn in (testbase.db, testbase.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (?, ?)", (1,"jack"))
            conn.execute("insert into users (user_id, user_name) values (?, ?)", [2,"fred"])
            conn.execute("insert into users (user_id, user_name) values (?, ?)", [3,"ed"], [4,"horse"])
            conn.execute("insert into users (user_id, user_name) values (?, ?)", (5,"barney"), (6,"donkey"))
            conn.execute("insert into users (user_id, user_name) values (?, ?)", 7, 'sally')
            res = conn.execute("select * from users")
            assert res.fetchall() == [(1, "jack"), (2, "fred"), (3, "ed"), (4, "horse"), (5, "barney"), (6, "donkey"), (7, 'sally')]
            conn.execute("delete from users")

    @testbase.supported('mysql', 'postgres')
    def test_raw_sprintf(self):
        for conn in (testbase.db, testbase.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (%s, %s)", [1,"jack"])
            conn.execute("insert into users (user_id, user_name) values (%s, %s)", [2,"ed"], [3,"horse"])
            conn.execute("insert into users (user_id, user_name) values (%s, %s)", 4, 'sally')
            conn.execute("insert into users (user_id) values (%s)", 5)
            res = conn.execute("select * from users")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally'), (5, None)]
            conn.execute("delete from users")

    # pyformat is supported for mysql, but skipping because a few driver
    # versions have a bug that bombs out on this test. (1.2.2b3, 1.2.2c1, 1.2.2)
    @testbase.supported('postgres')
    def test_raw_python(self):
        for conn in (testbase.db, testbase.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", {'id':1, 'name':'jack'})
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", {'id':2, 'name':'ed'}, {'id':3, 'name':'horse'})
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", id=4, name='sally')
            res = conn.execute("select * from users")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally')]
            conn.execute("delete from users")

    @testbase.supported('sqlite')
    def test_raw_named(self):
        for conn in (testbase.db, testbase.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", {'id':1, 'name':'jack'})
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", {'id':2, 'name':'ed'}, {'id':3, 'name':'horse'})
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", id=4, name='sally')
            res = conn.execute("select * from users")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally')]
            conn.execute("delete from users")
        
if __name__ == "__main__":
    testbase.main()        
