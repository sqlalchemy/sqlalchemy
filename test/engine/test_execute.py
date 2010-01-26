from sqlalchemy.test.testing import eq_
import re
from sqlalchemy.interfaces import ConnectionProxy
from sqlalchemy import MetaData, Integer, String, INT, VARCHAR, func, bindparam, select
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
            Column('user_id', INT, primary_key = True, test_needs_autoincrement=True),
            Column('user_name', VARCHAR(20)),
        )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        testing.db.connect().execute(users.delete())
        
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on_everything_except('firebird', 'maxdb', 'sqlite', 'mysql+pyodbc', '+zxjdbc', 'mysql+oursql')
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

    @testing.fails_on_everything_except('mysql+mysqldb', 'postgresql')
    @testing.fails_on('postgresql+zxjdbc', 'sprintf not supported')
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
    @testing.skip_if(lambda: testing.against('mysql+mysqldb'), 'db-api flaky')
    @testing.fails_on_everything_except('postgresql+psycopg2')
    def test_raw_python(self):
        for conn in (testing.db, testing.db.connect()):
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", {'id':1, 'name':'jack'})
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", {'id':2, 'name':'ed'}, {'id':3, 'name':'horse'})
            conn.execute("insert into users (user_id, user_name) values (%(id)s, %(name)s)", id=4, name='sally')
            res = conn.execute("select * from users order by user_id")
            assert res.fetchall() == [(1, "jack"), (2, "ed"), (3, "horse"), (4, 'sally')]
            conn.execute("delete from users")

    @testing.fails_on_everything_except('sqlite', 'oracle+cx_oracle')
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

    def test_empty_insert(self):
        """test that execute() interprets [] as a list with no params"""
        result = testing.db.execute(users.insert().values(user_name=bindparam('name')), [])
        eq_(testing.db.execute(users.select()).fetchall(), [
            (1, None)
        ])

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
                    (str(statement), parameters, None)
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
            engines.testing_engine(options=dict(implicit_returning=False, proxy=MyProxy())),
            engines.testing_engine(options=dict(implicit_returning=False, proxy=MyProxy(), strategy='threadlocal'))
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

            if not testing.against('oracle+zxjdbc'): # or engine.dialect.preexecute_pk_sequences:
                cursor = [
                    ("CREATE TABLE t1", {}, ()),
                    ("INSERT INTO t1 (c1, c2)", {'c2': 'some data', 'c1': 5}, [5, 'some data']),
                    ("SELECT lower", {'lower_2':'Foo'}, ['Foo']),
                    ("INSERT INTO t1 (c1, c2)", {'c2': 'foo', 'c1': 6}, [6, 'foo']),
                    ("select * from t1", {}, ()),
                    ("DROP TABLE t1", {}, ())
                ]
            else:
                insert2_params = [6, 'Foo']
                if testing.against('oracle+zxjdbc'):
                    from sqlalchemy.dialects.oracle.zxjdbc import ReturningParam
                    insert2_params.append(ReturningParam(12))
                cursor = [
                    ("CREATE TABLE t1", {}, ()),
                    ("INSERT INTO t1 (c1, c2)", {'c2': 'some data', 'c1': 5}, [5, 'some data']),
                    ("INSERT INTO t1 (c1, c2)", {'c1': 6, "lower_2":"Foo"}, insert2_params),  # bind param name 'lower_2' might be incorrect
                    ("select * from t1", {}, ()),
                    ("DROP TABLE t1", {}, ())
                ]
                
            assert_stmts(compiled, stmts)
            assert_stmts(cursor, cursor_stmts)
    
    def test_transactional(self):
        track = []
        class TrackProxy(ConnectionProxy):
            def __getattribute__(self, key):
                fn = object.__getattribute__(self, key)
                def go(*arg, **kw):
                    track.append(fn.__name__)
                    return fn(*arg, **kw)
                return go

        engine = engines.testing_engine(options={'proxy':TrackProxy()})
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.rollback()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.commit()
        
        eq_(track, ['begin', 'execute', 'cursor_execute', 
                        'rollback', 'begin', 'execute', 'cursor_execute', 'commit'])
        
    @testing.requires.savepoints
    @testing.requires.two_phase_transactions
    def test_transactional_advanced(self):
        track = []
        class TrackProxy(ConnectionProxy):
            def __getattribute__(self, key):
                fn = object.__getattribute__(self, key)
                def go(*arg, **kw):
                    track.append(fn.__name__)
                    return fn(*arg, **kw)
                return go

        engine = engines.testing_engine(options={'proxy':TrackProxy()})
        conn = engine.connect()
        
        trans = conn.begin()
        trans2 = conn.begin_nested()
        conn.execute(select([1]))
        trans2.rollback()
        trans2 = conn.begin_nested()
        conn.execute(select([1]))
        trans2.commit()
        trans.rollback()
        
        trans = conn.begin_twophase()
        conn.execute(select([1]))
        trans.prepare()
        trans.commit()

        track = [t for t in track if t not in ('cursor_execute', 'execute')]
        eq_(track, ['begin', 'savepoint', 
                    'rollback_savepoint', 'savepoint', 'release_savepoint',
                    'rollback', 'begin_twophase', 
                       'prepare_twophase', 'commit_twophase']
        )

