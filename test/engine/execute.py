import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy import exceptions
from testlib import *

class ExecuteTest(TestBase):
    def setUpAll(self):
        global users, metadata
        metadata = MetaData(testing.db)
        users = Table('users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        metadata.create_all()

    def tearDown(self):
        testing.db.connect().execute(users.delete())
    def tearDownAll(self):
        metadata.drop_all()

    def test_raw_qmark(self):
        for conn in (testing.db, testing.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (?, ?)", (1,"jack"))
            conn.execute("insert into users (user_id, user_name) values (?, ?)", [2,"fred"])
            conn.execute("insert into users (user_id, user_name) values (?, ?)", [3,"ed"], [4,"horse"])
            conn.execute("insert into users (user_id, user_name) values (?, ?)", (5,"barney"), (6,"donkey"))
            conn.execute("insert into users (user_id, user_name) values (?, ?)", 7, 'sally')
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [(1, "jack"), (2, "fred"), (3, "ed"), (4, "horse"), (5, "barney"), (6, "donkey"), (7, 'sally')]
            conn.execute("delete from users")
    test_raw_qmark = testing.fails_on_everything_except('firebird', 'maxdb', 'sqlite')(test_raw_qmark)

    # some psycopg2 versions bomb this.
    def test_raw_sprintf(self):
        for conn in (testing.db, testing.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (%s, %s)", [1,"jack"])
            conn.execute("insert into users (user_id, user_name) values (%s, %s)", [2,"ed"], [3,"horse"])
            conn.execute("insert into users (user_id, user_name) values (%s, %s)", 4, 'sally')
            conn.execute("insert into users (user_id) values (%s)", 5)
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally'), (5, None)]
            conn.execute("delete from users")
    test_raw_sprintf = testing.fails_on_everything_except('mysql', 'postgres')(test_raw_sprintf)

    # pyformat is supported for mysql, but skipping because a few driver
    # versions have a bug that bombs out on this test. (1.2.2b3, 1.2.2c1, 1.2.2)
    def test_raw_python(self):
        for conn in (testing.db, testing.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", {'id':1, 'name':'jack'})
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", {'id':2, 'name':'ed'}, {'id':3, 'name':'horse'})
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", id=4, name='sally')
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally')]
            conn.execute("delete from users")
    test_raw_python = testing.fails_on_everything_except('postgres')(test_raw_python)
    test_raw_python = testing.unsupported('mysql')(test_raw_python)

    def test_raw_named(self):
        for conn in (testing.db, testing.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", {'id':1, 'name':'jack'})
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", {'id':2, 'name':'ed'}, {'id':3, 'name':'horse'})
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", id=4, name='sally')
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally')]
            conn.execute("delete from users")
    test_raw_named = testing.fails_on_everything_except('sqlite', 'oracle')(test_raw_named)

    def test_exception_wrapping(self):
        for conn in (testing.db, testing.db.connect()):
            try:
                conn.execute("osdjafioajwoejoasfjdoifjowejfoawejqoijwef")
                assert False
            except exceptions.DBAPIError:
                assert True

if __name__ == "__main__":
    testenv.main()
