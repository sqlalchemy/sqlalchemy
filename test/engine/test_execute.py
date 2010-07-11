from sqlalchemy.test.testing import eq_, assert_raises
import re
from sqlalchemy.interfaces import ConnectionProxy
from sqlalchemy import MetaData, Integer, String, INT, VARCHAR, func, \
    bindparam, select
from sqlalchemy.test.schema import Table, Column
import sqlalchemy as tsa
from sqlalchemy.test import TestBase, testing, engines
import logging
from sqlalchemy.dialects.oracle.zxjdbc import ReturningParam

users, metadata = None, None
class ExecuteTest(TestBase):
    @classmethod
    def setup_class(cls):
        global users, users_autoinc, metadata
        metadata = MetaData(testing.db)
        users = Table('users', metadata,
            Column('user_id', INT, primary_key = True, autoincrement=False),
            Column('user_name', VARCHAR(20)),
        )
        users_autoinc = Table('users_autoinc', metadata,
            Column('user_id', INT, primary_key = True,
                                    test_needs_autoincrement=True),
            Column('user_name', VARCHAR(20)),
        )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        testing.db.connect().execute(users.delete())
        
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on_everything_except('firebird', 'maxdb', 
                                        'sqlite', '+pyodbc', 
                                        '+mxodbc', '+zxjdbc', 'mysql+oursql')
    def test_raw_qmark(self):
        for conn in testing.db, testing.db.connect():
            conn.execute('insert into users (user_id, user_name) '
                         'values (?, ?)', (1, 'jack'))
            conn.execute('insert into users (user_id, user_name) '
                         'values (?, ?)', [2, 'fred'])
            conn.execute('insert into users (user_id, user_name) '
                         'values (?, ?)', [3, 'ed'], [4, 'horse'])
            conn.execute('insert into users (user_id, user_name) '
                         'values (?, ?)', (5, 'barney'), (6, 'donkey'))
            conn.execute('insert into users (user_id, user_name) '
                         'values (?, ?)', 7, 'sally')
            res = conn.execute('select * from users order by user_id')
            assert res.fetchall() == [
                (1, 'jack'),
                (2, 'fred'),
                (3, 'ed'),
                (4, 'horse'),
                (5, 'barney'),
                (6, 'donkey'),
                (7, 'sally'),
                ]
            conn.execute('delete from users')

    # some psycopg2 versions bomb this.
    @testing.fails_on_everything_except('mysql+mysqldb',
            'mysql+mysqlconnector', 'postgresql')
    @testing.fails_on('postgresql+zxjdbc', 'sprintf not supported')
    def test_raw_sprintf(self):
        for conn in testing.db, testing.db.connect():
            conn.execute('insert into users (user_id, user_name) '
                         'values (%s, %s)', [1, 'jack'])
            conn.execute('insert into users (user_id, user_name) '
                         'values (%s, %s)', [2, 'ed'], [3, 'horse'])
            conn.execute('insert into users (user_id, user_name) '
                         'values (%s, %s)', 4, 'sally')
            conn.execute('insert into users (user_id) values (%s)', 5)
            res = conn.execute('select * from users order by user_id')
            assert res.fetchall() == [(1, 'jack'), (2, 'ed'), (3,
                    'horse'), (4, 'sally'), (5, None)]
            conn.execute('delete from users')

    # pyformat is supported for mysql, but skipping because a few driver
    # versions have a bug that bombs out on this test. (1.2.2b3,
    # 1.2.2c1, 1.2.2)

    @testing.skip_if(lambda : testing.against('mysql+mysqldb'),
                     'db-api flaky')
    @testing.fails_on_everything_except('postgresql+psycopg2',
            'postgresql+pypostgresql', 'mysql+mysqlconnector')
    def test_raw_python(self):
        for conn in testing.db, testing.db.connect():
            conn.execute('insert into users (user_id, user_name) '
                         'values (%(id)s, %(name)s)', {'id': 1, 'name'
                         : 'jack'})
            conn.execute('insert into users (user_id, user_name) '
                         'values (%(id)s, %(name)s)', {'id': 2, 'name'
                         : 'ed'}, {'id': 3, 'name': 'horse'})
            conn.execute('insert into users (user_id, user_name) '
                         'values (%(id)s, %(name)s)', id=4, name='sally'
                         )
            res = conn.execute('select * from users order by user_id')
            assert res.fetchall() == [(1, 'jack'), (2, 'ed'), (3,
                    'horse'), (4, 'sally')]
            conn.execute('delete from users')

    @testing.fails_on_everything_except('sqlite', 'oracle+cx_oracle')
    def test_raw_named(self):
        for conn in testing.db, testing.db.connect():
            conn.execute('insert into users (user_id, user_name) '
                         'values (:id, :name)', {'id': 1, 'name': 'jack'
                         })
            conn.execute('insert into users (user_id, user_name) '
                         'values (:id, :name)', {'id': 2, 'name': 'ed'
                         }, {'id': 3, 'name': 'horse'})
            conn.execute('insert into users (user_id, user_name) '
                         'values (:id, :name)', id=4, name='sally')
            res = conn.execute('select * from users order by user_id')
            assert res.fetchall() == [(1, 'jack'), (2, 'ed'), (3,
                    'horse'), (4, 'sally')]
            conn.execute('delete from users')

    def test_exception_wrapping(self):
        for conn in testing.db, testing.db.connect():
            try:
                conn.execute('osdjafioajwoejoasfjdoifjowejfoawejqoijwef'
                             )
                assert False
            except tsa.exc.DBAPIError:
                assert True

    def test_empty_insert(self):
        """test that execute() interprets [] as a list with no params"""

        result = \
            testing.db.execute(users_autoinc.insert().
                        values(user_name=bindparam('name')), [])
        eq_(testing.db.execute(users_autoinc.select()).fetchall(), [(1,
            None)])

    def test_engine_level_options(self):
        eng = engines.testing_engine(options={'execution_options'
                : {'foo': 'bar'}})
        conn = eng.contextual_connect()
        eq_(conn._execution_options['foo'], 'bar')
        eq_(conn.execution_options(bat='hoho')._execution_options['foo'
            ], 'bar')
        eq_(conn.execution_options(bat='hoho')._execution_options['bat'
            ], 'hoho')
        eq_(conn.execution_options(foo='hoho')._execution_options['foo'
            ], 'hoho')
        eng.update_execution_options(foo='hoho')
        conn = eng.contextual_connect()
        eq_(conn._execution_options['foo'], 'hoho')
        

class CompiledCacheTest(TestBase):
    @classmethod
    def setup_class(cls):
        global users, metadata
        metadata = MetaData(testing.db)
        users = Table('users', metadata,
            Column('user_id', INT, primary_key=True,
                            test_needs_autoincrement=True),
            Column('user_name', VARCHAR(20)),
        )
        metadata.create_all()

    @engines.close_first
    def teardown(self):
        testing.db.connect().execute(users.delete())
        
    @classmethod
    def teardown_class(cls):
        metadata.drop_all()
    
    def test_cache(self):
        conn = testing.db.connect()
        cache = {}
        cached_conn = conn.execution_options(compiled_cache=cache)
        
        ins = users.insert()
        cached_conn.execute(ins, {'user_name':'u1'})
        cached_conn.execute(ins, {'user_name':'u2'})
        cached_conn.execute(ins, {'user_name':'u3'})
        assert len(cache) == 1
        eq_(conn.execute("select count(1) from users").scalar(), 3)
    
class LogTest(TestBase):
    def _test_logger(self, eng, eng_name, pool_name):
        buf = logging.handlers.BufferingHandler(100)
        logs = [
            logging.getLogger('sqlalchemy.engine'),
            logging.getLogger('sqlalchemy.pool')
        ]
        for log in logs:
            log.addHandler(buf)
        
        eq_(eng.logging_name, eng_name)
        eq_(eng.pool.logging_name, pool_name)
        eng.execute(select([1]))
        for log in logs:
            log.removeHandler(buf)
        
        names = set([b.name for b in buf.buffer])
        assert 'sqlalchemy.engine.base.Engine.%s' % (eng_name,) in names
        assert 'sqlalchemy.pool.%s.%s' % (eng.pool.__class__.__name__,
                pool_name) in names
        
    def test_named_logger(self):
        options = {'echo':'debug', 'echo_pool':'debug',
            'logging_name':'myenginename',
            'pool_logging_name':'mypoolname'
        }
        eng = engines.testing_engine(options=options)
        self._test_logger(eng, "myenginename", "mypoolname")
        
        eng.dispose()
        self._test_logger(eng, "myenginename", "mypoolname")
        

    def test_unnamed_logger(self):
        eng = engines.testing_engine(options={'echo': 'debug',
                'echo_pool': 'debug'})
        self._test_logger(
            eng,
            "0x...%s" % hex(id(eng))[-4:],
            "0x...%s" % hex(id(eng.pool))[-4:],
        )
        
class ResultProxyTest(TestBase):
    def test_nontuple_row(self):
        """ensure the C version of BaseRowProxy handles 
        duck-type-dependent rows."""
        
        from sqlalchemy.engine import RowProxy

        class MyList(object):
            def __init__(self, l):
                self.l = l

            def __len__(self):
                return len(self.l)

            def __getitem__(self, i):
                return list.__getitem__(self.l, i)

        proxy = RowProxy(object(), MyList(['value']), [None], {'key'
                         : (None, 0), 0: (None, 0)})
        eq_(list(proxy), ['value'])
        eq_(proxy[0], 'value')
        eq_(proxy['key'], 'value')

    @testing.provide_metadata
    def test_no_rowcount_on_selects_inserts(self):
        """assert that rowcount is only called on deletes and updates.

        This because cursor.rowcount can be expensive on some dialects
        such as Firebird.

        """

        engine = engines.testing_engine()
        metadata.bind = engine
        
        t = Table('t1', metadata,
            Column('data', String(10))
        )
        metadata.create_all()

        class BreakRowcountMixin(object):
            @property
            def rowcount(self):
                assert False
        
        execution_ctx_cls = engine.dialect.execution_ctx_cls
        engine.dialect.execution_ctx_cls = type("FakeCtx", 
                                            (BreakRowcountMixin, 
                                            execution_ctx_cls), 
                                            {})

        try:
            r = t.insert().execute({'data': 'd1'}, {'data': 'd2'},
                                   {'data': 'd3'})
            eq_(t.select().execute().fetchall(), [('d1', ), ('d2', ),
                ('d3', )])
            assert_raises(AssertionError, t.update().execute, {'data'
                          : 'd4'})
            assert_raises(AssertionError, t.delete().execute)
        finally:
            engine.dialect.execution_ctx_cls = execution_ctx_cls
        
class ProxyConnectionTest(TestBase):

    @testing.fails_on('firebird', 'Data type unknown')
    def test_proxy(self):
        
        stmts = []
        cursor_stmts = []
        
        class MyProxy(ConnectionProxy):
            def execute(
                self,
                conn,
                execute,
                clauseelement,
                *multiparams,
                **params
                ):
                stmts.append((str(clauseelement), params, multiparams))
                return execute(clauseelement, *multiparams, **params)

            def cursor_execute(
                self,
                execute,
                cursor,
                statement,
                parameters,
                context,
                executemany,
                ):
                cursor_stmts.append((str(statement), parameters, None))
                return execute(cursor, statement, parameters, context)
        
        def assert_stmts(expected, received):
            for stmt, params, posn in expected:
                if not received:
                    assert False
                while received:
                    teststmt, testparams, testmultiparams = \
                        received.pop(0)
                    teststmt = re.compile(r'[\n\t ]+', re.M).sub(' ',
                            teststmt).strip()
                    if teststmt.startswith(stmt) and (testparams
                            == params or testparams == posn):
                        break

        for engine in \
            engines.testing_engine(options=dict(implicit_returning=False,
                                   proxy=MyProxy())), \
            engines.testing_engine(options=dict(implicit_returning=False,
                                   proxy=MyProxy(),
                                   strategy='threadlocal')):
            m = MetaData(engine)
            t1 = Table('t1', m, 
                Column('c1', Integer, primary_key=True), 
                Column('c2', String(50), default=func.lower('Foo'),
                                            primary_key=True)
            )
            m.create_all()
            try:
                t1.insert().execute(c1=5, c2='some data')
                t1.insert().execute(c1=6)
                eq_(engine.execute('select * from t1').fetchall(), [(5,
                    'some data'), (6, 'foo')])
            finally:
                m.drop_all()
            engine.dispose()
            compiled = [('CREATE TABLE t1', {}, None),
                        ('INSERT INTO t1 (c1, c2)', {'c2': 'some data',
                        'c1': 5}, None), ('INSERT INTO t1 (c1, c2)',
                        {'c1': 6}, None), ('select * from t1', {},
                        None), ('DROP TABLE t1', {}, None)]
            if not testing.against('oracle+zxjdbc'):  # or engine.dialect.pr
                                                      # eexecute_pk_sequence
                                                      # s:
                cursor = [
                    ('CREATE TABLE t1', {}, ()),
                    ('INSERT INTO t1 (c1, c2)', {'c2': 'some data', 'c1'
                     : 5}, (5, 'some data')),
                    ('SELECT lower', {'lower_2': 'Foo'}, ('Foo', )),
                    ('INSERT INTO t1 (c1, c2)', {'c2': 'foo', 'c1': 6},
                     (6, 'foo')),
                    ('select * from t1', {}, ()),
                    ('DROP TABLE t1', {}, ()),
                    ]
            else:
                insert2_params = 6, 'Foo'
                if testing.against('oracle+zxjdbc'):
                    insert2_params += (ReturningParam(12), )
                cursor = [('CREATE TABLE t1', {}, ()),
                          ('INSERT INTO t1 (c1, c2)', {'c2': 'some data'
                          , 'c1': 5}, (5, 'some data')),
                          ('INSERT INTO t1 (c1, c2)', {'c1': 6,
                          'lower_2': 'Foo'}, insert2_params),
                          ('select * from t1', {}, ()), ('DROP TABLE t1'
                          , {}, ())]  # bind param name 'lower_2' might
                                      # be incorrect
            assert_stmts(compiled, stmts)
            assert_stmts(cursor, cursor_stmts)
    
    def test_options(self):
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
        c2 = conn.execution_options(foo='bar')
        eq_(c2._execution_options, {'foo':'bar'})
        c2.execute(select([1]))
        c3 = c2.execution_options(bar='bat')
        eq_(c3._execution_options, {'foo':'bar', 'bar':'bat'})
        eq_(track, ['execute', 'cursor_execute'])
        
        
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
        
        eq_(track, [
            'begin',
            'execute',
            'cursor_execute',
            'rollback',
            'begin',
            'execute',
            'cursor_execute',
            'commit',
            ])
        
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

