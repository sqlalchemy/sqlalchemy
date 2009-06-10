from sqlalchemy.test.testing import eq_
import re
from sqlalchemy.interfaces import ConnectionProxy
from sqlalchemy import MetaData, Integer, String, INT, VARCHAR, func, bindparam
from sqlalchemy.test.schema import Table
from sqlalchemy.test.schema import Column
import sqlalchemy as tsa
from sqlalchemy.test import TestBase, testing, engines


users, metadata = None, None
class ExecuteTest(TestBase):
    @classmethod
    def setup_class(cls):
        global users, metadata
        metadata = MetaData(testing.db)
        users = Table('users', metadata,
            Column('user_id', INT, primary_key = True),
            Column('user_name', VARCHAR(20)),
        )
        metadata.create_all()

    def teardown(self):
        testing.db.connect().execute(users.delete())
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on_everything_except('firebird', 'maxdb', 'sqlite')
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

    @testing.fails_on_everything_except('mysql', 'postgres')
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

    # pyformat is supported for mysql, but skipping because a few driver
    # versions have a bug that bombs out on this test. (1.2.2b3, 1.2.2c1, 1.2.2)
    @testing.skip_if(lambda: testing.against('mysql'), 'db-api flaky')
    @testing.fails_on_everything_except('postgres')
    def test_raw_python(self):
        for conn in (testing.db, testing.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", {'id':1, 'name':'jack'})
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", {'id':2, 'name':'ed'}, {'id':3, 'name':'horse'})
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", id=4, name='sally')
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally')]
            conn.execute("delete from users")

    @testing.fails_on_everything_except('sqlite', 'oracle')
    def test_raw_named(self):
        for conn in (testing.db, testing.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", {'id':1, 'name':'jack'})
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", {'id':2, 'name':'ed'}, {'id':3, 'name':'horse'})
            conn.execute("insert into users (user_id, user_name) values (:id, :name)", id=4, name='sally')
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally')]
            conn.execute("delete from users")

    def test_exception_wrapping(self):
        for conn in (testing.db, testing.db.connect()):
            try:
                conn.execute("osdjafioajwoejoasfjdoifjowejfoawejqoijwef")
                assert False
            except tsa.exc.DBAPIError:
                assert True

    @testing.fails_on('mssql', 'rowcount returns -1')
    def test_empty_insert(self):
        """test that execute() interprets [] as a list with no params"""
        result = testing.db.execute(users.insert().values(user_name=bindparam('name')), [])
        eq_(result.rowcount, 1)

class ProxyConnectionTest(TestBase):
    @testing.fails_on('firebird', 'Data type unknown')
    def test_proxy(self):
        
        stmts = []
        cursor_stmts = []
        
        class MyProxy(ConnectionProxy):
            def execute(self, conn, execute, clauseelement, *multiparams, **params):
                stmts.append(
                    (str(clauseelement), params,multiparams)
                )
                return execute(clauseelement, *multiparams, **params)

            def cursor_execute(self, execute, cursor, statement, parameters, context, executemany):
                cursor_stmts.append(
                    (statement, parameters, None)
                )
                return execute(cursor, statement, parameters, context)
        
        def assert_stmts(expected, received):
            for stmt, params, posn in expected:
                if not received:
                    assert False
                while received:
                    teststmt, testparams, testmultiparams = received.pop(0)
                    teststmt = re.compile(r'[\n\t ]+', re.M).sub(' ', teststmt).strip()
                    if teststmt.startswith(stmt) and (testparams==params or testparams==posn):
                        break

        for engine in (
            engines.testing_engine(options=dict(proxy=MyProxy())),
            engines.testing_engine(options=dict(proxy=MyProxy(), strategy='threadlocal'))
        ):
            m = MetaData(engine)

            t1 = Table('t1', m, Column('c1', Integer, primary_key=True), Column('c2', String(50), default=func.lower('Foo'), primary_key=True))

            m.create_all()
            try:
                t1.insert().execute(c1=5, c2='some data')
                t1.insert().execute(c1=6)
                assert engine.execute("select * from t1").fetchall() == [(5, 'some data'), (6, 'foo')]
            finally:
                m.drop_all()
            
            engine.dispose()
            
            compiled = [
                ("CREATE TABLE t1", {}, None),
                ("INSERT INTO t1 (c1, c2)", {'c2': 'some data', 'c1': 5}, None),
                ("INSERT INTO t1 (c1, c2)", {'c1': 6}, None),
                ("select * from t1", {}, None),
                ("DROP TABLE t1", {}, None)
            ]

            if engine.dialect.preexecute_pk_sequences:
                cursor = [
                    ("CREATE TABLE t1", {}, None),
                    ("INSERT INTO t1 (c1, c2)", {'c2': 'some data', 'c1': 5}, [5, 'some data']),
                    ("SELECT lower", {'lower_2':'Foo'}, ['Foo']),
                    ("INSERT INTO t1 (c1, c2)", {'c2': 'foo', 'c1': 6}, [6, 'foo']),
                    ("select * from t1", {}, None),
                    ("DROP TABLE t1", {}, None)
                ]
            else:
                cursor = [
                    ("CREATE TABLE t1", {}, ()),
                    ("INSERT INTO t1 (c1, c2)", {'c2': 'some data', 'c1': 5}, [5, 'some data']),
                    ("INSERT INTO t1 (c1, c2)", {'c1': 6, "lower_2":"Foo"}, [6, "Foo"]),  # bind param name 'lower_2' might be incorrect
                    ("select * from t1", {}, ()),
                    ("DROP TABLE t1", {}, ())
                ]
                
            assert_stmts(compiled, stmts)
            assert_stmts(cursor, cursor_stmts)
    

