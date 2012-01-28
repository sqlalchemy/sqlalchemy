from test.lib.testing import eq_, assert_raises, assert_raises_message, config
import re
from test.lib.util import picklers
from sqlalchemy.interfaces import ConnectionProxy
from sqlalchemy import MetaData, Integer, String, INT, VARCHAR, func, \
    bindparam, select, event, TypeDecorator, create_engine, Sequence
from sqlalchemy.sql import column, literal
from test.lib.schema import Table, Column
import sqlalchemy as tsa
from test.lib import testing, engines
from test.lib.engines import testing_engine
import logging
from sqlalchemy.dialects.oracle.zxjdbc import ReturningParam
from sqlalchemy.engine import base, default
from sqlalchemy.engine.base import Connection, Engine
from test.lib import fixtures
import StringIO

users, metadata = None, None
class ExecuteTest(fixtures.TestBase):
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
        testing.db.execute(users.delete())

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()

    @testing.fails_on_everything_except('firebird', 'maxdb',
                                        'sqlite', '+pyodbc',
                                        '+mxodbc', '+zxjdbc', 'mysql+oursql',
                                        'informix+informixdb')
    def test_raw_qmark(self):
        def go(conn):
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

        go(testing.db)
        conn = testing.db.connect()
        try:
            go(conn)
        finally:
            conn.close()

    # some psycopg2 versions bomb this.
    @testing.fails_on_everything_except('mysql+mysqldb', 'mysql+pymysql',
            'mysql+mysqlconnector', 'postgresql')
    @testing.fails_on('postgresql+zxjdbc', 'sprintf not supported')
    def test_raw_sprintf(self):
        def go(conn):
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
        go(testing.db)
        conn = testing.db.connect()
        try:
            go(conn)
        finally:
            conn.close()

    # pyformat is supported for mysql, but skipping because a few driver
    # versions have a bug that bombs out on this test. (1.2.2b3,
    # 1.2.2c1, 1.2.2)

    @testing.skip_if(lambda : testing.against('mysql+mysqldb'),
                     'db-api flaky')
    @testing.fails_on_everything_except('postgresql+psycopg2',
            'postgresql+pypostgresql', 'mysql+mysqlconnector', 
            'mysql+pymysql')
    def test_raw_python(self):
        def go(conn):
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
        go(testing.db)
        conn = testing.db.connect()
        try:
            go(conn)
        finally:
            conn.close()

    @testing.fails_on_everything_except('sqlite', 'oracle+cx_oracle', 'informix+informixdb')
    def test_raw_named(self):
        def go(conn):
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
        go(testing.db)
        conn= testing.db.connect()
        try:
            go(conn)
        finally:
            conn.close()

    def test_exception_wrapping_dbapi(self):
        def go(conn):
            assert_raises_message(
                tsa.exc.DBAPIError,
                r"not_a_valid_statement",
                conn.execute, 'not_a_valid_statement'
            )
        go(testing.db)
        conn = testing.db.connect()
        try:
            go(conn)
        finally:
            conn.close()

    def test_exception_wrapping_non_dbapi_statement(self):
        class MyType(TypeDecorator):
            impl = Integer
            def process_bind_param(self, value, dialect):
                raise Exception("nope")

        def _go(conn):
            assert_raises_message(
                tsa.exc.StatementError,
                r"nope \(original cause: Exception: nope\) 'SELECT 1 ",
                conn.execute,
                    select([1]).\
                        where(
                            column('foo') == literal('bar', MyType())
                        )
            )
        _go(testing.db)
        conn = testing.db.connect()
        try:
            _go(conn)
        finally:
            conn.close()

    def test_stmt_exception_pickleable_no_dbapi(self):
        self._test_stmt_exception_pickleable(Exception("hello world"))

    @testing.fails_on("postgresql+psycopg2", 
                "Packages the cursor in the exception")
    @testing.fails_on("mysql+oursql", 
                "Exception doesn't come back exactly the same from pickle")
    @testing.fails_on("oracle+cx_oracle", 
                        "cx_oracle exception seems to be having "
                        "some issue with pickling")
    def test_stmt_exception_pickleable_plus_dbapi(self):
        raw = testing.db.raw_connection()
        the_orig = None
        try:
            try:
                cursor = raw.cursor()
                cursor.execute("SELECTINCORRECT")
            except testing.db.dialect.dbapi.DatabaseError, orig:
                # py3k has "orig" in local scope...
                the_orig = orig
        finally:
            raw.close()
        self._test_stmt_exception_pickleable(the_orig)

    def _test_stmt_exception_pickleable(self, orig):
        for sa_exc in (
            tsa.exc.StatementError("some error", 
                            "select * from table", 
                           {"foo":"bar"}, 
                            orig),
            tsa.exc.InterfaceError("select * from table", 
                            {"foo":"bar"}, 
                            orig),
            tsa.exc.NoReferencedTableError("message", "tname"),
            tsa.exc.NoReferencedColumnError("message", "tname", "cname"),
            tsa.exc.CircularDependencyError("some message", [1, 2, 3], [(1, 2), (3, 4)]),
        ):
            for loads, dumps in picklers():
                repickled = loads(dumps(sa_exc))
                eq_(repickled.args[0], sa_exc.args[0])
                if isinstance(sa_exc, tsa.exc.StatementError):
                    eq_(repickled.params, {"foo":"bar"})
                    eq_(repickled.statement, sa_exc.statement)
                    if hasattr(sa_exc, "connection_invalidated"):
                        eq_(repickled.connection_invalidated, 
                            sa_exc.connection_invalidated)
                    eq_(repickled.orig.args[0], orig.args[0])

    def test_dont_wrap_mixin(self):
        class MyException(Exception, tsa.exc.DontWrapMixin):
            pass

        class MyType(TypeDecorator):
            impl = Integer
            def process_bind_param(self, value, dialect):
                raise MyException("nope")

        def _go(conn):
            assert_raises_message(
                MyException,
                "nope",
                conn.execute,
                    select([1]).\
                        where(
                            column('foo') == literal('bar', MyType())
                        )
            )
        _go(testing.db)
        conn = testing.db.connect()
        try:
            _go(conn)
        finally:
            conn.close()

    def test_empty_insert(self):
        """test that execute() interprets [] as a list with no params"""

        result = \
            testing.db.execute(users_autoinc.insert().
                        values(user_name=bindparam('name')), [])
        eq_(testing.db.execute(users_autoinc.select()).fetchall(), [(1,
            None)])

    @testing.requires.ad_hoc_engines
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

    def test_unicode_test_fails_warning(self):
        class MockCursor(engines.DBAPIProxyCursor):
            def execute(self, stmt, params=None, **kw):
                if "test unicode returns" in stmt:
                    raise self.engine.dialect.dbapi.DatabaseError("boom")
                else:
                    return super(MockCursor, self).execute(stmt, params, **kw)
        eng = engines.proxying_engine(cursor_cls=MockCursor)
        assert_raises_message(
            tsa.exc.SAWarning,
            "Exception attempting to detect unicode returns",
            eng.connect
        )
        assert eng.dialect.returns_unicode_strings in (True, False)
        eng.dispose()

class CompiledCacheTest(fixtures.TestBase):
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
        testing.db.execute(users.delete())

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
        eq_(conn.execute("select count(*) from users").scalar(), 3)

class LogParamsTest(fixtures.TestBase):
    __only_on__ = 'sqlite'
    __requires__ = 'ad_hoc_engines',

    def setup(self):
        self.eng = engines.testing_engine(options={'echo':True})
        self.eng.execute("create table foo (data string)")
        self.buf = logging.handlers.BufferingHandler(100)
        for log in [
            logging.getLogger('sqlalchemy.engine'),
            logging.getLogger('sqlalchemy.pool')
        ]:
            log.addHandler(self.buf)

    def teardown(self):
        self.eng.execute("drop table foo")
        for log in [
            logging.getLogger('sqlalchemy.engine'),
            logging.getLogger('sqlalchemy.pool')
        ]:
            log.removeHandler(self.buf)

    def test_log_large_dict(self):
        self.eng.execute(
            "INSERT INTO foo (data) values (:data)", 
            [{"data":str(i)} for i in xrange(100)]
        )
        eq_(
            self.buf.buffer[1].message,
            "[{'data': '0'}, {'data': '1'}, {'data': '2'}, {'data': '3'}, "
            "{'data': '4'}, {'data': '5'}, {'data': '6'}, {'data': '7'}"
            "  ... displaying 10 of 100 total bound "
            "parameter sets ...  {'data': '98'}, {'data': '99'}]"
        )

    def test_log_large_list(self):
        self.eng.execute(
            "INSERT INTO foo (data) values (?)", 
            [(str(i), ) for i in xrange(100)]
        )
        eq_(
            self.buf.buffer[1].message,
            "[('0',), ('1',), ('2',), ('3',), ('4',), ('5',), "
            "('6',), ('7',)  ... displaying 10 of 100 total "
            "bound parameter sets ...  ('98',), ('99',)]"
        )

    def test_error_large_dict(self):
        assert_raises_message(
            tsa.exc.DBAPIError,
            r".*'INSERT INTO nonexistent \(data\) values \(:data\)' "
            "\[{'data': '0'}, {'data': '1'}, {'data': '2'}, "
            "{'data': '3'}, {'data': '4'}, {'data': '5'}, "
            "{'data': '6'}, {'data': '7'}  ... displaying 10 of "
            "100 total bound parameter sets ...  {'data': '98'}, {'data': '99'}\]",
            lambda: self.eng.execute(
                "INSERT INTO nonexistent (data) values (:data)", 
                [{"data":str(i)} for i in xrange(100)]
            )
        )

    def test_error_large_list(self):
        assert_raises_message(
            tsa.exc.DBAPIError,
            r".*INSERT INTO nonexistent \(data\) values "
            "\(\?\)' \[\('0',\), \('1',\), \('2',\), \('3',\), "
            "\('4',\), \('5',\), \('6',\), \('7',\)  ... displaying "
            "10 of 100 total bound parameter sets ...  "
            "\('98',\), \('99',\)\]",
            lambda: self.eng.execute(
                "INSERT INTO nonexistent (data) values (?)", 
                [(str(i), ) for i in xrange(100)]
            )
        )

class LoggingNameTest(fixtures.TestBase):
    __requires__ = 'ad_hoc_engines',

    def _assert_names_in_execute(self, eng, eng_name, pool_name):
        eng.execute(select([1]))
        assert self.buf.buffer
        for name in [b.name for b in self.buf.buffer]:
            assert name in (
                'sqlalchemy.engine.base.Engine.%s' % eng_name,
                'sqlalchemy.pool.%s.%s' % 
                    (eng.pool.__class__.__name__, pool_name)
            )

    def _assert_no_name_in_execute(self, eng):
        eng.execute(select([1]))
        assert self.buf.buffer
        for name in [b.name for b in self.buf.buffer]:
            assert name in (
                'sqlalchemy.engine.base.Engine',
                'sqlalchemy.pool.%s' % eng.pool.__class__.__name__
            )

    def _named_engine(self, **kw):
        options = {
            'logging_name':'myenginename',
            'pool_logging_name':'mypoolname',
            'echo':True
        }
        options.update(kw)
        return engines.testing_engine(options=options)

    def _unnamed_engine(self, **kw):
        kw.update({'echo':True})
        return engines.testing_engine(options=kw)

    def setup(self):
        self.buf = logging.handlers.BufferingHandler(100)
        for log in [
            logging.getLogger('sqlalchemy.engine'),
            logging.getLogger('sqlalchemy.pool')
        ]:
            log.addHandler(self.buf)

    def teardown(self):
        for log in [
            logging.getLogger('sqlalchemy.engine'),
            logging.getLogger('sqlalchemy.pool')
        ]:
            log.removeHandler(self.buf)

    def test_named_logger_names(self):
        eng = self._named_engine()
        eq_(eng.logging_name, "myenginename")
        eq_(eng.pool.logging_name, "mypoolname")

    def test_named_logger_names_after_dispose(self):
        eng = self._named_engine()
        eng.execute(select([1]))
        eng.dispose()
        eq_(eng.logging_name, "myenginename")
        eq_(eng.pool.logging_name, "mypoolname")

    def test_unnamed_logger_names(self):
        eng = self._unnamed_engine()
        eq_(eng.logging_name, None)
        eq_(eng.pool.logging_name, None)

    def test_named_logger_execute(self):
        eng = self._named_engine()
        self._assert_names_in_execute(eng, "myenginename", "mypoolname")

    def test_named_logger_echoflags_execute(self):
        eng = self._named_engine(echo='debug', echo_pool='debug')
        self._assert_names_in_execute(eng, "myenginename", "mypoolname")

    def test_named_logger_execute_after_dispose(self):
        eng = self._named_engine()
        eng.execute(select([1]))
        eng.dispose()
        self._assert_names_in_execute(eng, "myenginename", "mypoolname")

    def test_unnamed_logger_execute(self):
        eng = self._unnamed_engine()
        self._assert_no_name_in_execute(eng)

    def test_unnamed_logger_echoflags_execute(self):
        eng = self._unnamed_engine(echo='debug', echo_pool='debug')
        self._assert_no_name_in_execute(eng)

class EchoTest(fixtures.TestBase):
    __requires__ = 'ad_hoc_engines',

    def setup(self):
        self.level = logging.getLogger('sqlalchemy.engine').level
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
        self.buf = logging.handlers.BufferingHandler(100)
        logging.getLogger('sqlalchemy.engine').addHandler(self.buf)

    def teardown(self):
        logging.getLogger('sqlalchemy.engine').removeHandler(self.buf)
        logging.getLogger('sqlalchemy.engine').setLevel(self.level)

    def testing_engine(self):
        e = engines.testing_engine()

        # do an initial execute to clear out 'first connect'
        # messages
        e.execute(select([10])).close()
        self.buf.flush()

        return e

    def test_levels(self):
        e1 = engines.testing_engine()

        eq_(e1._should_log_info(), False)
        eq_(e1._should_log_debug(), False)
        eq_(e1.logger.isEnabledFor(logging.INFO), False)
        eq_(e1.logger.getEffectiveLevel(), logging.WARN)

        e1.echo = True
        eq_(e1._should_log_info(), True)
        eq_(e1._should_log_debug(), False)
        eq_(e1.logger.isEnabledFor(logging.INFO), True)
        eq_(e1.logger.getEffectiveLevel(), logging.INFO)

        e1.echo = 'debug'
        eq_(e1._should_log_info(), True)
        eq_(e1._should_log_debug(), True)
        eq_(e1.logger.isEnabledFor(logging.DEBUG), True)
        eq_(e1.logger.getEffectiveLevel(), logging.DEBUG)

        e1.echo = False
        eq_(e1._should_log_info(), False)
        eq_(e1._should_log_debug(), False)
        eq_(e1.logger.isEnabledFor(logging.INFO), False)
        eq_(e1.logger.getEffectiveLevel(), logging.WARN)

    def test_echo_flag_independence(self):
        """test the echo flag's independence to a specific engine."""

        e1 = self.testing_engine()
        e2 = self.testing_engine()

        e1.echo = True
        e1.execute(select([1])).close()
        e2.execute(select([2])).close()

        e1.echo = False
        e1.execute(select([3])).close()
        e2.execute(select([4])).close()

        e2.echo = True
        e1.execute(select([5])).close()
        e2.execute(select([6])).close()

        assert self.buf.buffer[0].getMessage().startswith("SELECT 1")
        assert self.buf.buffer[2].getMessage().startswith("SELECT 6")
        assert len(self.buf.buffer) == 4

class MockStrategyTest(fixtures.TestBase):
    def _engine_fixture(self):
        buf = StringIO.StringIO()
        def dump(sql, *multiparams, **params):
            buf.write(unicode(sql.compile(dialect=engine.dialect)))
        engine = create_engine('postgresql://', strategy='mock', executor=dump)
        return engine, buf

    def test_sequence_not_duped(self):
        engine, buf = self._engine_fixture()
        metadata = MetaData()
        t = Table('testtable', metadata,
           Column('pk', Integer, Sequence('testtable_pk_seq'), primary_key=True)
        )

        t.create(engine)
        t.drop(engine)

        eq_(
            re.findall(r'CREATE (\w+)', buf.getvalue()),
            ["SEQUENCE", "TABLE"]
        )

        eq_(
            re.findall(r'DROP (\w+)', buf.getvalue()),
            ["SEQUENCE", "TABLE"]
        )

class ResultProxyTest(fixtures.TestBase):

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
                         : (None, None, 0), 0: (None, None, 0)})
        eq_(list(proxy), ['value'])
        eq_(proxy[0], 'value')
        eq_(proxy['key'], 'value')

    @testing.provide_metadata
    def test_no_rowcount_on_selects_inserts(self):
        """assert that rowcount is only called on deletes and updates.

        This because cursor.rowcount can be expensive on some dialects
        such as Firebird.

        """

        metadata = self.metadata

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


    @testing.requires.python26
    def test_rowproxy_is_sequence(self):
        import collections
        from sqlalchemy.engine import RowProxy

        row = RowProxy(object(), ['value'], [None], {'key'
                         : (None, None, 0), 0: (None, None, 0)})
        assert isinstance(row, collections.Sequence)

    @testing.requires.cextensions
    def test_row_c_sequence_check(self):
        import csv
        import collections
        from StringIO import StringIO

        metadata = MetaData()
        metadata.bind = 'sqlite://'
        users = Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(40)),
        )
        users.create()

        users.insert().execute(name='Test')
        row = users.select().execute().fetchone()

        s = StringIO()
        writer = csv.writer(s)
        # csv performs PySequenceCheck call
        writer.writerow(row)
        assert s.getvalue().strip() == '1,Test'

class AlternateResultProxyTest(fixtures.TestBase):
    __requires__ = ('sqlite', )

    @classmethod
    def setup_class(cls):
        from sqlalchemy.engine import base, default
        cls.engine = engine = testing_engine('sqlite://')
        m = MetaData()
        cls.table = t = Table('test', m, 
            Column('x', Integer, primary_key=True),
            Column('y', String(50, convert_unicode='force'))
        )
        m.create_all(engine)
        engine.execute(t.insert(), [
            {'x':i, 'y':"t_%d" % i} for i in xrange(1, 12)
        ])

    def _test_proxy(self, cls):
        class ExcCtx(default.DefaultExecutionContext):
            def get_result_proxy(self):
                return cls(self)
        self.engine.dialect.execution_ctx_cls = ExcCtx
        rows = []
        r = self.engine.execute(select([self.table]))
        assert isinstance(r, cls)
        for i in range(5):
            rows.append(r.fetchone())
        eq_(rows, [(i, "t_%d" % i) for i in xrange(1, 6)])

        rows = r.fetchmany(3)
        eq_(rows, [(i, "t_%d" % i) for i in xrange(6, 9)])

        rows = r.fetchall()
        eq_(rows, [(i, "t_%d" % i) for i in xrange(9, 12)])

        r = self.engine.execute(select([self.table]))
        rows = r.fetchmany(None)
        eq_(rows[0], (1, "t_1"))
        # number of rows here could be one, or the whole thing
        assert len(rows) == 1 or len(rows) == 11

        r = self.engine.execute(select([self.table]).limit(1))
        r.fetchone()
        eq_(r.fetchone(), None)

        r = self.engine.execute(select([self.table]).limit(5))
        rows = r.fetchmany(6)
        eq_(rows, [(i, "t_%d" % i) for i in xrange(1, 6)])

    def test_plain(self):
        self._test_proxy(base.ResultProxy)

    def test_buffered_row_result_proxy(self):
        self._test_proxy(base.BufferedRowResultProxy)

    def test_fully_buffered_result_proxy(self):
        self._test_proxy(base.FullyBufferedResultProxy)

    def test_buffered_column_result_proxy(self):
        self._test_proxy(base.BufferedColumnResultProxy)

class EngineEventsTest(fixtures.TestBase):
    __requires__ = 'ad_hoc_engines', 

    def tearDown(self):
        Engine.dispatch._clear()

    def _assert_stmts(self, expected, received):
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

    def test_per_engine_independence(self):
        e1 = testing_engine(config.db_url)
        e2 = testing_engine(config.db_url)

        canary = []
        def before_exec(conn, stmt, *arg):
            canary.append(stmt)
        event.listen(e1, "before_execute", before_exec)
        s1 = select([1])
        s2 = select([2])
        e1.execute(s1)
        e2.execute(s2)
        eq_(canary, [s1])
        event.listen(e2, "before_execute", before_exec)
        e1.execute(s1)
        e2.execute(s2)
        eq_(canary, [s1, s1, s2])

    def test_per_engine_plus_global(self):
        canary = []
        def be1(conn, stmt, *arg):
            canary.append('be1')
        def be2(conn, stmt, *arg):
            canary.append('be2')
        def be3(conn, stmt, *arg):
            canary.append('be3')

        event.listen(Engine, "before_execute", be1)
        e1 = testing_engine(config.db_url)
        e2 = testing_engine(config.db_url)

        event.listen(e1, "before_execute", be2)

        event.listen(Engine, "before_execute", be3)
        e1.connect()
        e2.connect()
        canary[:] = []
        e1.execute(select([1]))
        e2.execute(select([1]))

        eq_(canary, ['be1', 'be3', 'be2', 'be1', 'be3'])

    def test_argument_format_execute(self):
        def before_execute(conn, clauseelement, multiparams, params):
            assert isinstance(multiparams, (list, tuple))
            assert isinstance(params, dict)
        def after_execute(conn, clauseelement, multiparams, params, result):
            assert isinstance(multiparams, (list, tuple))
            assert isinstance(params, dict)
        e1 = testing_engine(config.db_url)
        event.listen(e1, 'before_execute', before_execute)
        event.listen(e1, 'after_execute', after_execute)

        e1.execute(select([1]))
        e1.execute(select([1]).compile(dialect=e1.dialect).statement)
        e1.execute(select([1]).compile(dialect=e1.dialect))
        e1._execute_compiled(select([1]).compile(dialect=e1.dialect), [], {})

    @testing.fails_on('firebird', 'Data type unknown')
    def test_execute_events(self):

        stmts = []
        cursor_stmts = []

        def execute(conn, clauseelement, multiparams,
                                                    params ):
            stmts.append((str(clauseelement), params, multiparams))

        def cursor_execute(conn, cursor, statement, parameters, 
                                context, executemany):
            cursor_stmts.append((str(statement), parameters, None))


        for engine in [
            engines.testing_engine(options=dict(implicit_returning=False)), 
            engines.testing_engine(options=dict(implicit_returning=False,
                                   strategy='threadlocal'))
            ]:
            event.listen(engine, 'before_execute', execute)
            event.listen(engine, 'before_cursor_execute', cursor_execute)

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
            self._assert_stmts(compiled, stmts)
            self._assert_stmts(cursor, cursor_stmts)

    def test_options(self):
        canary = []
        def execute(conn, *args, **kw):
            canary.append('execute')

        def cursor_execute(conn, *args, **kw):
            canary.append('cursor_execute')

        engine = engines.testing_engine()
        event.listen(engine, 'before_execute', execute)
        event.listen(engine, 'before_cursor_execute', cursor_execute)
        conn = engine.connect()
        c2 = conn.execution_options(foo='bar')
        eq_(c2._execution_options, {'foo':'bar'})
        c2.execute(select([1]))
        c3 = c2.execution_options(bar='bat')
        eq_(c3._execution_options, {'foo':'bar', 'bar':'bat'})
        eq_(canary, ['execute', 'cursor_execute'])

    def test_retval_flag(self):
        canary = []
        def tracker(name):
            def go(conn, *args, **kw):
                canary.append(name)
            return go

        def execute(conn, clauseelement, multiparams, params):
            canary.append('execute')
            return clauseelement, multiparams, params

        def cursor_execute(conn, cursor, statement, 
                        parameters, context, executemany):
            canary.append('cursor_execute')
            return statement, parameters

        engine = engines.testing_engine()

        assert_raises(
            tsa.exc.ArgumentError,
            event.listen, engine, "begin", tracker("begin"), retval=True
        )

        event.listen(engine, "before_execute", execute, retval=True)
        event.listen(engine, "before_cursor_execute", cursor_execute, retval=True)
        engine.execute(select([1]))
        eq_(
            canary, ['execute', 'cursor_execute']
        )

    def test_transactional(self):
        canary = []
        def tracker(name):
            def go(conn, *args, **kw):
                canary.append(name)
            return go

        engine = engines.testing_engine()
        event.listen(engine, 'before_execute', tracker('execute'))
        event.listen(engine, 'before_cursor_execute', tracker('cursor_execute'))
        event.listen(engine, 'begin', tracker('begin'))
        event.listen(engine, 'commit', tracker('commit'))
        event.listen(engine, 'rollback', tracker('rollback'))

        conn = engine.connect()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.rollback()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.commit()

        eq_(canary, [
            'begin', 'execute', 'cursor_execute', 'rollback',
            'begin', 'execute', 'cursor_execute', 'commit',
            ])

    @testing.requires.savepoints
    @testing.requires.two_phase_transactions
    def test_transactional_advanced(self):
        canary = []
        def tracker(name):
            def go(*args, **kw):
                canary.append(name)
            return go

        engine = engines.testing_engine()
        for name in ['begin', 'savepoint', 
                    'rollback_savepoint', 'release_savepoint',
                    'rollback', 'begin_twophase', 
                       'prepare_twophase', 'commit_twophase']:
            event.listen(engine, '%s' % name, tracker(name))

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

        eq_(canary, ['begin', 'savepoint', 
                    'rollback_savepoint', 'savepoint', 'release_savepoint',
                    'rollback', 'begin_twophase', 
                       'prepare_twophase', 'commit_twophase']
        )


class ProxyConnectionTest(fixtures.TestBase):
    """These are the same tests as EngineEventsTest, except using
    the deprecated ConnectionProxy interface.

    """
    __requires__ = 'ad_hoc_engines', 

    @testing.uses_deprecated(r'.*Use event.listen')
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

    @testing.uses_deprecated(r'.*Use event.listen')
    def test_options(self):
        canary = []
        class TrackProxy(ConnectionProxy):
            def __getattribute__(self, key):
                fn = object.__getattribute__(self, key)
                def go(*arg, **kw):
                    canary.append(fn.__name__)
                    return fn(*arg, **kw)
                return go
        engine = engines.testing_engine(options={'proxy':TrackProxy()})
        conn = engine.connect()
        c2 = conn.execution_options(foo='bar')
        eq_(c2._execution_options, {'foo':'bar'})
        c2.execute(select([1]))
        c3 = c2.execution_options(bar='bat')
        eq_(c3._execution_options, {'foo':'bar', 'bar':'bat'})
        eq_(canary, ['execute', 'cursor_execute'])


    @testing.uses_deprecated(r'.*Use event.listen')
    def test_transactional(self):
        canary = []
        class TrackProxy(ConnectionProxy):
            def __getattribute__(self, key):
                fn = object.__getattribute__(self, key)
                def go(*arg, **kw):
                    canary.append(fn.__name__)
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

        eq_(canary, [
            'begin', 'execute', 'cursor_execute', 'rollback',
            'begin', 'execute', 'cursor_execute', 'commit',
            ])

    @testing.uses_deprecated(r'.*Use event.listen')
    @testing.requires.savepoints
    @testing.requires.two_phase_transactions
    def test_transactional_advanced(self):
        canary = []
        class TrackProxy(ConnectionProxy):
            def __getattribute__(self, key):
                fn = object.__getattribute__(self, key)
                def go(*arg, **kw):
                    canary.append(fn.__name__)
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

        canary = [t for t in canary if t not in ('cursor_execute', 'execute')]
        eq_(canary, ['begin', 'savepoint', 
                    'rollback_savepoint', 'savepoint', 'release_savepoint',
                    'rollback', 'begin_twophase', 
                       'prepare_twophase', 'commit_twophase']
        )

