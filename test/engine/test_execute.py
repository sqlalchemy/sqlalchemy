# coding: utf-8

from sqlalchemy.testing import eq_, assert_raises, assert_raises_message, \
    config, is_
import re
from sqlalchemy.testing.util import picklers
from sqlalchemy.interfaces import ConnectionProxy
from sqlalchemy import MetaData, Integer, String, INT, VARCHAR, func, \
    bindparam, select, event, TypeDecorator, create_engine, Sequence
from sqlalchemy.sql import column, literal
from sqlalchemy.testing.schema import Table, Column
import sqlalchemy as tsa
from sqlalchemy import testing
from sqlalchemy.testing import engines
from sqlalchemy import util
from sqlalchemy.testing.engines import testing_engine
from sqlalchemy.dialects.oracle.zxjdbc import ReturningParam
from sqlalchemy.engine import result as _result, default
from sqlalchemy.engine.base import Engine
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.mock import Mock, call, patch
from contextlib import contextmanager
from sqlalchemy.util import nested

users, metadata, users_autoinc = None, None, None


class ExecuteTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global users, users_autoinc, metadata
        metadata = MetaData(testing.db)
        users = Table(
            'users', metadata,
            Column('user_id', INT, primary_key=True, autoincrement=False),
            Column('user_name', VARCHAR(20)),
        )
        users_autoinc = Table(
            'users_autoinc', metadata,
            Column(
                'user_id', INT, primary_key=True,
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

    @testing.fails_on("postgresql+pg8000",
            "pg8000 still doesn't allow single % without params")
    def test_no_params_option(self):
        stmt = "SELECT '%'" + testing.db.dialect.statement_compiler(
            testing.db.dialect, None).default_from()

        conn = testing.db.connect()
        result = conn.\
            execution_options(no_parameters=True).\
            scalar(stmt)
        eq_(result, '%')

    @testing.fails_on_everything_except('firebird',
                                        'sqlite', '+pyodbc',
                                        '+mxodbc', '+zxjdbc', 'mysql+oursql')
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
            for multiparam, param in [
                (("jack", "fred"), {}),
                ((["jack", "fred"],), {})
            ]:
                res = conn.execute(
                    "select * from users where user_name=? or "
                    "user_name=? order by user_id",
                    *multiparam, **param)
                assert res.fetchall() == [
                    (1, 'jack'),
                    (2, 'fred')
                ]
            res = conn.execute(
                "select * from users where user_name=?",
                "jack"
            )
            assert res.fetchall() == [(1, 'jack')]
            conn.execute('delete from users')

        go(testing.db)
        conn = testing.db.connect()
        try:
            go(conn)
        finally:
            conn.close()

    # some psycopg2 versions bomb this.
    @testing.fails_on_everything_except(
        'mysql+mysqldb', 'mysql+pymysql',
        'mysql+cymysql', 'mysql+mysqlconnector', 'postgresql')
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
            assert res.fetchall() == [
                (1, 'jack'), (2, 'ed'),
                (3, 'horse'), (4, 'sally'), (5, None)
            ]
            for multiparam, param in [
                (("jack", "ed"), {}),
                ((["jack", "ed"],), {})
            ]:
                res = conn.execute(
                    "select * from users where user_name=%s or "
                    "user_name=%s order by user_id",
                    *multiparam, **param)
                assert res.fetchall() == [
                    (1, 'jack'),
                    (2, 'ed')
                ]
            res = conn.execute(
                "select * from users where user_name=%s",
                "jack"
            )
            assert res.fetchall() == [(1, 'jack')]

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

    @testing.skip_if(
        lambda: testing.against('mysql+mysqldb'), 'db-api flaky')
    @testing.fails_on_everything_except(
        'postgresql+psycopg2',
        'postgresql+pypostgresql', 'mysql+mysqlconnector',
        'mysql+pymysql', 'mysql+cymysql')
    def test_raw_python(self):
        def go(conn):
            conn.execute(
                'insert into users (user_id, user_name) '
                'values (%(id)s, %(name)s)',
                {'id': 1, 'name': 'jack'})
            conn.execute(
                'insert into users (user_id, user_name) '
                'values (%(id)s, %(name)s)',
                {'id': 2, 'name': 'ed'}, {'id': 3, 'name': 'horse'})
            conn.execute(
                'insert into users (user_id, user_name) '
                'values (%(id)s, %(name)s)', id=4, name='sally'
            )
            res = conn.execute('select * from users order by user_id')
            assert res.fetchall() == [
                (1, 'jack'), (2, 'ed'), (3, 'horse'), (4, 'sally')]
            conn.execute('delete from users')
        go(testing.db)
        conn = testing.db.connect()
        try:
            go(conn)
        finally:
            conn.close()

    @testing.fails_on_everything_except('sqlite', 'oracle+cx_oracle')
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
            assert res.fetchall() == [
                (1, 'jack'), (2, 'ed'), (3, 'horse'), (4, 'sally')]
            conn.execute('delete from users')
        go(testing.db)
        conn = testing.db.connect()
        try:
            go(conn)
        finally:
            conn.close()

    @testing.engines.close_open_connections
    def test_exception_wrapping_dbapi(self):
        conn = testing.db.connect()
        for _c in testing.db, conn:
            assert_raises_message(
                tsa.exc.DBAPIError,
                r"not_a_valid_statement",
                _c.execute, 'not_a_valid_statement'
            )

    @testing.requires.sqlite
    def test_exception_wrapping_non_dbapi_error(self):
        e = create_engine('sqlite://')
        e.dialect.is_disconnect = is_disconnect = Mock()

        with e.connect() as c:
            c.connection.cursor = Mock(
                return_value=Mock(
                    execute=Mock(
                        side_effect=TypeError("I'm not a DBAPI error")
                    ))
            )

            assert_raises_message(
                TypeError,
                "I'm not a DBAPI error",
                c.execute, "select "
            )
            eq_(is_disconnect.call_count, 0)

    def test_exception_wrapping_non_standard_dbapi_error(self):
        class DBAPIError(Exception):
            pass

        class OperationalError(DBAPIError):
            pass

        class NonStandardException(OperationalError):
            pass

        with nested(
            patch.object(testing.db.dialect, "dbapi", Mock(Error=DBAPIError)),
            patch.object(
                testing.db.dialect, "is_disconnect",
                lambda *arg: False),
            patch.object(
                testing.db.dialect, "do_execute",
                Mock(side_effect=NonStandardException)),
        ):
            with testing.db.connect() as conn:
                assert_raises(
                    tsa.exc.OperationalError,
                    conn.execute, "select 1"
                )

    def test_exception_wrapping_non_dbapi_statement(self):
        class MyType(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                raise Exception("nope")

        def _go(conn):
            assert_raises_message(
                tsa.exc.StatementError,
                r"nope \(original cause: Exception: nope\) u?'SELECT 1 ",
                conn.execute,
                select([1]).
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

    def test_stmt_exception_non_ascii(self):
        name = util.u('méil')
        with testing.db.connect() as conn:
            assert_raises_message(
                tsa.exc.StatementError,
                util.u(
                    "A value is required for bind parameter 'uname'"
                    r'.*SELECT users.user_name AS .m\\xe9il.') if util.py2k
                else
                util.u(
                    "A value is required for bind parameter 'uname'"
                    '.*SELECT users.user_name AS .méil.'),
                conn.execute,
                select([users.c.user_name.label(name)]).where(
                    users.c.user_name == bindparam("uname")),
                {'uname_incorrect': 'foo'}
            )

    def test_stmt_exception_pickleable_no_dbapi(self):
        self._test_stmt_exception_pickleable(Exception("hello world"))

    @testing.crashes(
        "postgresql+psycopg2",
        "Older versions don't support cursor pickling, newer ones do")
    @testing.fails_on(
        "mysql+oursql",
        "Exception doesn't come back exactly the same from pickle")
    @testing.fails_on(
        "mysql+mysqlconnector",
        "Exception doesn't come back exactly the same from pickle")
    @testing.fails_on(
        "oracle+cx_oracle",
        "cx_oracle exception seems to be having "
        "some issue with pickling")
    def test_stmt_exception_pickleable_plus_dbapi(self):
        raw = testing.db.raw_connection()
        the_orig = None
        try:
            try:
                cursor = raw.cursor()
                cursor.execute("SELECTINCORRECT")
            except testing.db.dialect.dbapi.DatabaseError as orig:
                # py3k has "orig" in local scope...
                the_orig = orig
        finally:
            raw.close()
        self._test_stmt_exception_pickleable(the_orig)

    def _test_stmt_exception_pickleable(self, orig):
        for sa_exc in (
            tsa.exc.StatementError("some error",
                                   "select * from table",
                                   {"foo": "bar"},
                                   orig),
            tsa.exc.InterfaceError("select * from table",
                                   {"foo": "bar"},
                                   orig),
            tsa.exc.NoReferencedTableError("message", "tname"),
            tsa.exc.NoReferencedColumnError("message", "tname", "cname"),
            tsa.exc.CircularDependencyError(
                "some message", [1, 2, 3], [(1, 2), (3, 4)]),
        ):
            for loads, dumps in picklers():
                repickled = loads(dumps(sa_exc))
                eq_(repickled.args[0], sa_exc.args[0])
                if isinstance(sa_exc, tsa.exc.StatementError):
                    eq_(repickled.params, {"foo": "bar"})
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
                select([1]).
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

        testing.db.execute(users_autoinc.insert().
                           values(user_name=bindparam('name', None)), [])
        eq_(testing.db.execute(users_autoinc.select()).fetchall(), [(1, None)])

    @testing.requires.ad_hoc_engines
    def test_engine_level_options(self):
        eng = engines.testing_engine(options={'execution_options':
                                              {'foo': 'bar'}})
        with eng.contextual_connect() as conn:
            eq_(conn._execution_options['foo'], 'bar')
            eq_(
                conn.execution_options(bat='hoho')._execution_options['foo'],
                'bar')
            eq_(
                conn.execution_options(bat='hoho')._execution_options['bat'],
                'hoho')
            eq_(
                conn.execution_options(foo='hoho')._execution_options['foo'],
                'hoho')
            eng.update_execution_options(foo='hoho')
            conn = eng.contextual_connect()
            eq_(conn._execution_options['foo'], 'hoho')

    @testing.requires.ad_hoc_engines
    def test_generative_engine_execution_options(self):
        eng = engines.testing_engine(options={'execution_options':
                                              {'base': 'x1'}})

        eng1 = eng.execution_options(foo="b1")
        eng2 = eng.execution_options(foo="b2")
        eng1a = eng1.execution_options(bar="a1")
        eng2a = eng2.execution_options(foo="b3", bar="a2")

        eq_(eng._execution_options,
            {'base': 'x1'})
        eq_(eng1._execution_options,
            {'base': 'x1', 'foo': 'b1'})
        eq_(eng2._execution_options,
            {'base': 'x1', 'foo': 'b2'})
        eq_(eng1a._execution_options,
            {'base': 'x1', 'foo': 'b1', 'bar': 'a1'})
        eq_(eng2a._execution_options,
            {'base': 'x1', 'foo': 'b3', 'bar': 'a2'})
        is_(eng1a.pool, eng.pool)

        # test pool is shared
        eng2.dispose()
        is_(eng1a.pool, eng2.pool)
        is_(eng.pool, eng2.pool)

    @testing.requires.ad_hoc_engines
    def test_generative_engine_event_dispatch(self):
        canary = []

        def l1(*arg, **kw):
            canary.append("l1")

        def l2(*arg, **kw):
            canary.append("l2")

        def l3(*arg, **kw):
            canary.append("l3")

        eng = engines.testing_engine(options={'execution_options':
                                              {'base': 'x1'}})
        event.listen(eng, "before_execute", l1)

        eng1 = eng.execution_options(foo="b1")
        event.listen(eng, "before_execute", l2)
        event.listen(eng1, "before_execute", l3)

        eng.execute(select([1])).close()
        eng1.execute(select([1])).close()

        eq_(canary, ["l1", "l2", "l3", "l1", "l2"])

    @testing.requires.ad_hoc_engines
    def test_autocommit_option_no_issue_first_connect(self):
        eng = create_engine(testing.db.url)
        eng.update_execution_options(autocommit=True)
        conn = eng.connect()
        eq_(conn._execution_options, {"autocommit": True})
        conn.close()

    @testing.requires.ad_hoc_engines
    def test_dialect_init_uses_options(self):
        eng = create_engine(testing.db.url)

        def my_init(connection):
            connection.execution_options(foo='bar').execute(select([1]))

        with patch.object(eng.dialect, "initialize", my_init):
            conn = eng.connect()
            eq_(conn._execution_options, {})
            conn.close()

    @testing.requires.ad_hoc_engines
    def test_generative_engine_event_dispatch_hasevents(self):
        def l1(*arg, **kw):
            pass
        eng = create_engine(testing.db.url)
        assert not eng._has_events
        event.listen(eng, "before_execute", l1)
        eng2 = eng.execution_options(foo='bar')
        assert eng2._has_events

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

    def test_works_after_dispose(self):
        eng = create_engine(testing.db.url)
        for i in range(3):
            eq_(eng.scalar(select([1])), 1)
            eng.dispose()

    def test_works_after_dispose_testing_engine(self):
        eng = engines.testing_engine()
        for i in range(3):
            eq_(eng.scalar(select([1])), 1)
            eng.dispose()


class ConvenienceExecuteTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        cls.table = Table('exec_test', metadata,
                          Column('a', Integer),
                          Column('b', Integer),
                          test_needs_acid=True
                          )

    def _trans_fn(self, is_transaction=False):
        def go(conn, x, value=None):
            if is_transaction:
                conn = conn.connection
            conn.execute(self.table.insert().values(a=x, b=value))
        return go

    def _trans_rollback_fn(self, is_transaction=False):
        def go(conn, x, value=None):
            if is_transaction:
                conn = conn.connection
            conn.execute(self.table.insert().values(a=x, b=value))
            raise Exception("breakage")
        return go

    def _assert_no_data(self):
        eq_(
            testing.db.scalar(self.table.count()), 0
        )

    def _assert_fn(self, x, value=None):
        eq_(
            testing.db.execute(self.table.select()).fetchall(),
            [(x, value)]
        )

    def test_transaction_engine_ctx_commit(self):
        fn = self._trans_fn()
        ctx = testing.db.begin()
        testing.run_as_contextmanager(ctx, fn, 5, value=8)
        self._assert_fn(5, value=8)

    def test_transaction_engine_ctx_begin_fails(self):
        engine = engines.testing_engine()

        mock_connection = Mock(
            return_value=Mock(
                begin=Mock(side_effect=Exception("boom"))
            )
        )
        engine._connection_cls = mock_connection
        assert_raises(
            Exception,
            engine.begin
        )

        eq_(
            mock_connection.return_value.close.mock_calls,
            [call()]
        )

    def test_transaction_engine_ctx_rollback(self):
        fn = self._trans_rollback_fn()
        ctx = testing.db.begin()
        assert_raises_message(
            Exception,
            "breakage",
            testing.run_as_contextmanager, ctx, fn, 5, value=8
        )
        self._assert_no_data()

    def test_transaction_tlocal_engine_ctx_commit(self):
        fn = self._trans_fn()
        engine = engines.testing_engine(options=dict(
            strategy='threadlocal',
            pool=testing.db.pool))
        ctx = engine.begin()
        testing.run_as_contextmanager(ctx, fn, 5, value=8)
        self._assert_fn(5, value=8)

    def test_transaction_tlocal_engine_ctx_rollback(self):
        fn = self._trans_rollback_fn()
        engine = engines.testing_engine(options=dict(
            strategy='threadlocal',
            pool=testing.db.pool))
        ctx = engine.begin()
        assert_raises_message(
            Exception,
            "breakage",
            testing.run_as_contextmanager, ctx, fn, 5, value=8
        )
        self._assert_no_data()

    def test_transaction_connection_ctx_commit(self):
        fn = self._trans_fn(True)
        conn = testing.db.connect()
        ctx = conn.begin()
        testing.run_as_contextmanager(ctx, fn, 5, value=8)
        self._assert_fn(5, value=8)

    def test_transaction_connection_ctx_rollback(self):
        fn = self._trans_rollback_fn(True)
        conn = testing.db.connect()
        ctx = conn.begin()
        assert_raises_message(
            Exception,
            "breakage",
            testing.run_as_contextmanager, ctx, fn, 5, value=8
        )
        self._assert_no_data()

    def test_connection_as_ctx(self):
        fn = self._trans_fn()
        ctx = testing.db.connect()
        testing.run_as_contextmanager(ctx, fn, 5, value=8)
        # autocommit is on
        self._assert_fn(5, value=8)

    @testing.fails_on('mysql+oursql', "oursql bug ?  getting wrong rowcount")
    def test_connect_as_ctx_noautocommit(self):
        fn = self._trans_fn()
        self._assert_no_data()
        ctx = testing.db.connect().execution_options(autocommit=False)
        testing.run_as_contextmanager(ctx, fn, 5, value=8)
        # autocommit is off
        self._assert_no_data()

    def test_transaction_engine_fn_commit(self):
        fn = self._trans_fn()
        testing.db.transaction(fn, 5, value=8)
        self._assert_fn(5, value=8)

    def test_transaction_engine_fn_rollback(self):
        fn = self._trans_rollback_fn()
        assert_raises_message(
            Exception,
            "breakage",
            testing.db.transaction, fn, 5, value=8
        )
        self._assert_no_data()

    def test_transaction_connection_fn_commit(self):
        fn = self._trans_fn()
        conn = testing.db.connect()
        conn.transaction(fn, 5, value=8)
        self._assert_fn(5, value=8)

    def test_transaction_connection_fn_rollback(self):
        fn = self._trans_rollback_fn()
        conn = testing.db.connect()
        assert_raises(
            Exception,
            conn.transaction, fn, 5, value=8
        )
        self._assert_no_data()


class CompiledCacheTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        global users, metadata
        metadata = MetaData(testing.db)
        users = Table('users', metadata,
                      Column('user_id', INT, primary_key=True,
                             test_needs_autoincrement=True),
                      Column('user_name', VARCHAR(20)),
                      Column("extra_data", VARCHAR(20))
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
        with patch.object(
            ins, "compile",
                Mock(side_effect=ins.compile)) as compile_mock:
            cached_conn.execute(ins, {'user_name': 'u1'})
            cached_conn.execute(ins, {'user_name': 'u2'})
            cached_conn.execute(ins, {'user_name': 'u3'})
        eq_(compile_mock.call_count, 1)
        assert len(cache) == 1
        eq_(conn.execute("select count(*) from users").scalar(), 3)

    def test_keys_independent_of_ordering(self):
        conn = testing.db.connect()
        conn.execute(
            users.insert(),
            {"user_id": 1, "user_name": "u1", "extra_data": "e1"})
        cache = {}
        cached_conn = conn.execution_options(compiled_cache=cache)

        upd = users.update().where(users.c.user_id == bindparam("b_user_id"))

        with patch.object(
            upd, "compile",
                Mock(side_effect=upd.compile)) as compile_mock:
            cached_conn.execute(
                upd, util.OrderedDict([
                    ("b_user_id", 1),
                    ("user_name", "u2"),
                    ("extra_data", "e2")
                ])
            )
            cached_conn.execute(
                upd, util.OrderedDict([
                    ("b_user_id", 1),
                    ("extra_data", "e3"),
                    ("user_name", "u3"),
                ])
            )
            cached_conn.execute(
                upd, util.OrderedDict([
                    ("extra_data", "e4"),
                    ("user_name", "u4"),
                    ("b_user_id", 1),
                ])
            )
        eq_(compile_mock.call_count, 1)
        eq_(len(cache), 1)


class MockStrategyTest(fixtures.TestBase):

    def _engine_fixture(self):
        buf = util.StringIO()

        def dump(sql, *multiparams, **params):
            buf.write(util.text_type(sql.compile(dialect=engine.dialect)))
        engine = create_engine('postgresql://', strategy='mock', executor=dump)
        return engine, buf

    def test_sequence_not_duped(self):
        engine, buf = self._engine_fixture()
        metadata = MetaData()
        t = Table('testtable', metadata,
                  Column(
                      'pk', Integer, Sequence('testtable_pk_seq'), primary_key=True)
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
    __backend__ = True

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

        proxy = RowProxy(object(), MyList(['value']), [None], {
                         'key': (None, None, 0), 0: (None, None, 0)})
        eq_(list(proxy), ['value'])
        eq_(proxy[0], 'value')
        eq_(proxy['key'], 'value')

    @testing.provide_metadata
    def test_no_rowcount_on_selects_inserts(self):
        """assert that rowcount is only called on deletes and updates.

        This because cursor.rowcount may can be expensive on some dialects
        such as Firebird, however many dialects require it be called
        before the cursor is closed.

        """

        metadata = self.metadata

        engine = engines.testing_engine()

        t = Table('t1', metadata,
                  Column('data', String(10))
                  )
        metadata.create_all(engine)

        with patch.object(
                engine.dialect.execution_ctx_cls, "rowcount") as mock_rowcount:
            mock_rowcount.__get__ = Mock()
            engine.execute(t.insert(),
                           {'data': 'd1'},
                           {'data': 'd2'},
                           {'data': 'd3'})

            eq_(len(mock_rowcount.__get__.mock_calls), 0)

            eq_(
                engine.execute(t.select()).fetchall(),
                [('d1', ), ('d2', ), ('d3', )]
            )
            eq_(len(mock_rowcount.__get__.mock_calls), 0)

            engine.execute(t.update(), {'data': 'd4'})

            eq_(len(mock_rowcount.__get__.mock_calls), 1)

            engine.execute(t.delete())
            eq_(len(mock_rowcount.__get__.mock_calls), 2)

    def test_rowproxy_is_sequence(self):
        import collections
        from sqlalchemy.engine import RowProxy

        row = RowProxy(
            object(), ['value'], [None],
            {'key': (None, None, 0), 0: (None, None, 0)})
        assert isinstance(row, collections.Sequence)

    @testing.requires.cextensions
    def test_row_c_sequence_check(self):
        import csv

        metadata = MetaData()
        metadata.bind = 'sqlite://'
        users = Table('users', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String(40)),
                      )
        users.create()

        users.insert().execute(name='Test')
        row = users.select().execute().fetchone()

        s = util.StringIO()
        writer = csv.writer(s)
        # csv performs PySequenceCheck call
        writer.writerow(row)
        assert s.getvalue().strip() == '1,Test'

    @testing.requires.selectone
    def test_empty_accessors(self):
        statements = [
            (
                "select 1",
                [
                    lambda r: r.last_inserted_params(),
                    lambda r: r.last_updated_params(),
                    lambda r: r.prefetch_cols(),
                    lambda r: r.postfetch_cols(),
                    lambda r: r.inserted_primary_key
                ],
                "Statement is not a compiled expression construct."
            ),
            (
                select([1]),
                [
                    lambda r: r.last_inserted_params(),
                    lambda r: r.inserted_primary_key
                ],
                r"Statement is not an insert\(\) expression construct."
            ),
            (
                select([1]),
                [
                    lambda r: r.last_updated_params(),
                ],
                r"Statement is not an update\(\) expression construct."
            ),
            (
                select([1]),
                [
                    lambda r: r.prefetch_cols(),
                    lambda r: r.postfetch_cols()
                ],
                r"Statement is not an insert\(\) "
                r"or update\(\) expression construct."
            ),
        ]

        for stmt, meths, msg in statements:
            r = testing.db.execute(stmt)
            try:
                for meth in meths:
                    assert_raises_message(
                        tsa.exc.InvalidRequestError,
                        msg,
                        meth, r
                    )

            finally:
                r.close()


class ExecutionOptionsTest(fixtures.TestBase):

    def test_dialect_conn_options(self):
        engine = testing_engine("sqlite://", options=dict(_initialize=False))
        engine.dialect = Mock()
        conn = engine.connect()
        c2 = conn.execution_options(foo="bar")
        eq_(
            engine.dialect.set_connection_execution_options.mock_calls,
            [call(c2, {"foo": "bar"})]
        )

    def test_dialect_engine_options(self):
        engine = testing_engine("sqlite://")
        engine.dialect = Mock()
        e2 = engine.execution_options(foo="bar")
        eq_(
            engine.dialect.set_engine_execution_options.mock_calls,
            [call(e2, {"foo": "bar"})]
        )

    def test_dialect_engine_construction_options(self):
        dialect = Mock()
        engine = Engine(Mock(), dialect, Mock(),
                        execution_options={"foo": "bar"})
        eq_(
            dialect.set_engine_execution_options.mock_calls,
            [call(engine, {"foo": "bar"})]
        )

    def test_propagate_engine_to_connection(self):
        engine = testing_engine("sqlite://",
                                options=dict(execution_options={"foo": "bar"}))
        conn = engine.connect()
        eq_(conn._execution_options, {"foo": "bar"})

    def test_propagate_option_engine_to_connection(self):
        e1 = testing_engine("sqlite://",
                            options=dict(execution_options={"foo": "bar"}))
        e2 = e1.execution_options(bat="hoho")
        c1 = e1.connect()
        c2 = e2.connect()
        eq_(c1._execution_options, {"foo": "bar"})
        eq_(c2._execution_options, {"foo": "bar", "bat": "hoho"})


class AlternateResultProxyTest(fixtures.TestBase):
    __requires__ = ('sqlite', )

    @classmethod
    def setup_class(cls):
        cls.engine = engine = testing_engine('sqlite://')
        m = MetaData()
        cls.table = t = Table('test', m,
                              Column('x', Integer, primary_key=True),
                              Column('y', String(50, convert_unicode='force'))
                              )
        m.create_all(engine)
        engine.execute(t.insert(), [
            {'x': i, 'y': "t_%d" % i} for i in range(1, 12)
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
        eq_(rows, [(i, "t_%d" % i) for i in range(1, 6)])

        rows = r.fetchmany(3)
        eq_(rows, [(i, "t_%d" % i) for i in range(6, 9)])

        rows = r.fetchall()
        eq_(rows, [(i, "t_%d" % i) for i in range(9, 12)])

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
        eq_(rows, [(i, "t_%d" % i) for i in range(1, 6)])

    def test_plain(self):
        self._test_proxy(_result.ResultProxy)

    def test_buffered_row_result_proxy(self):
        self._test_proxy(_result.BufferedRowResultProxy)

    def test_fully_buffered_result_proxy(self):
        self._test_proxy(_result.FullyBufferedResultProxy)

    def test_buffered_column_result_proxy(self):
        self._test_proxy(_result.BufferedColumnResultProxy)


class EngineEventsTest(fixtures.TestBase):
    __requires__ = 'ad_hoc_engines',
    __backend__ = True

    def tearDown(self):
        Engine.dispatch._clear()
        Engine._has_events = False

    def _assert_stmts(self, expected, received):
        orig = list(received)
        for stmt, params, posn in expected:
            if not received:
                assert False, "Nothing available for stmt: %s" % stmt
            while received:
                teststmt, testparams, testmultiparams = \
                    received.pop(0)
                teststmt = re.compile(r'[\n\t ]+', re.M).sub(' ',
                                                             teststmt).strip()
                if teststmt.startswith(stmt) and (
                        testparams == params or testparams == posn):
                    break

    def test_per_engine_independence(self):
        e1 = testing_engine(config.db_url)
        e2 = testing_engine(config.db_url)

        canary = Mock()
        event.listen(e1, "before_execute", canary)
        s1 = select([1])
        s2 = select([2])
        e1.execute(s1)
        e2.execute(s2)
        eq_(
            [arg[1][1] for arg in canary.mock_calls], [s1]
        )
        event.listen(e2, "before_execute", canary)
        e1.execute(s1)
        e2.execute(s2)
        eq_([arg[1][1] for arg in canary.mock_calls], [s1, s1, s2])

    def test_per_engine_plus_global(self):
        canary = Mock()
        event.listen(Engine, "before_execute", canary.be1)
        e1 = testing_engine(config.db_url)
        e2 = testing_engine(config.db_url)

        event.listen(e1, "before_execute", canary.be2)

        event.listen(Engine, "before_execute", canary.be3)
        e1.connect()
        e2.connect()

        e1.execute(select([1]))
        eq_(canary.be1.call_count, 1)
        eq_(canary.be2.call_count, 1)

        e2.execute(select([1]))

        eq_(canary.be1.call_count, 2)
        eq_(canary.be2.call_count, 1)
        eq_(canary.be3.call_count, 2)

    def test_per_connection_plus_engine(self):
        canary = Mock()
        e1 = testing_engine(config.db_url)

        event.listen(e1, "before_execute", canary.be1)

        conn = e1.connect()
        event.listen(conn, "before_execute", canary.be2)
        conn.execute(select([1]))

        eq_(canary.be1.call_count, 1)
        eq_(canary.be2.call_count, 1)

        conn._branch().execute(select([1]))
        eq_(canary.be1.call_count, 2)
        eq_(canary.be2.call_count, 2)

    def test_add_event_after_connect(self):
        # new feature as of #2978
        canary = Mock()
        e1 = create_engine(config.db_url)
        assert not e1._has_events

        conn = e1.connect()

        event.listen(e1, "before_execute", canary.be1)
        conn.execute(select([1]))

        eq_(canary.be1.call_count, 1)

        conn._branch().execute(select([1]))
        eq_(canary.be1.call_count, 2)

    def test_force_conn_events_false(self):
        canary = Mock()
        e1 = create_engine(config.db_url)
        assert not e1._has_events

        event.listen(e1, "before_execute", canary.be1)

        conn = e1._connection_cls(e1, connection=e1.raw_connection(),
                                  _has_events=False)

        conn.execute(select([1]))

        eq_(canary.be1.call_count, 0)

        conn._branch().execute(select([1]))
        eq_(canary.be1.call_count, 0)

    def test_cursor_events_ctx_execute_scalar(self):
        canary = Mock()
        e1 = testing_engine(config.db_url)

        event.listen(e1, "before_cursor_execute", canary.bce)
        event.listen(e1, "after_cursor_execute", canary.ace)

        stmt = str(select([1]).compile(dialect=e1.dialect))

        with e1.connect() as conn:
            dialect = conn.dialect

            ctx = dialect.execution_ctx_cls._init_statement(
                dialect, conn, conn.connection, stmt, {})

            ctx._execute_scalar(stmt, Integer())

        eq_(canary.bce.mock_calls,
            [call(conn, ctx.cursor, stmt, ctx.parameters[0], ctx, False)])
        eq_(canary.ace.mock_calls,
            [call(conn, ctx.cursor, stmt, ctx.parameters[0], ctx, False)])

    def test_cursor_events_execute(self):
        canary = Mock()
        e1 = testing_engine(config.db_url)

        event.listen(e1, "before_cursor_execute", canary.bce)
        event.listen(e1, "after_cursor_execute", canary.ace)

        stmt = str(select([1]).compile(dialect=e1.dialect))

        with e1.connect() as conn:

            result = conn.execute(stmt)

        ctx = result.context
        eq_(canary.bce.mock_calls,
            [call(conn, ctx.cursor, stmt, ctx.parameters[0], ctx, False)])
        eq_(canary.ace.mock_calls,
            [call(conn, ctx.cursor, stmt, ctx.parameters[0], ctx, False)])

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
        e1._execute_compiled(select([1]).compile(dialect=e1.dialect), (), {})

    @testing.fails_on('firebird', 'Data type unknown')
    def test_execute_events(self):

        stmts = []
        cursor_stmts = []

        def execute(conn, clauseelement, multiparams,
                    params):
            stmts.append((str(clauseelement), params, multiparams))

        def cursor_execute(conn, cursor, statement, parameters,
                           context, executemany):
            cursor_stmts.append((str(statement), parameters, None))

        for engine in [
                engines.testing_engine(options=dict(implicit_returning=False)),
                engines.testing_engine(options=dict(implicit_returning=False,
                                                    strategy='threadlocal')),
                engines.testing_engine(options=dict(implicit_returning=False)).
            connect()
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
                eq_(
                    engine.execute('select * from t1').fetchall(),
                    [(5, 'some data'), (6, 'foo')])
            finally:
                m.drop_all()

            compiled = [('CREATE TABLE t1', {}, None),
                        ('INSERT INTO t1 (c1, c2)',
                         {'c2': 'some data', 'c1': 5}, None),
                        ('INSERT INTO t1 (c1, c2)',
                         {'c1': 6}, None),
                        ('select * from t1', {}, None),
                        ('DROP TABLE t1', {}, None)]

            # or engine.dialect.preexecute_pk_sequences:
            if not testing.against('oracle+zxjdbc'):
                cursor = [
                    ('CREATE TABLE t1', {}, ()),
                    ('INSERT INTO t1 (c1, c2)', {
                        'c2': 'some data', 'c1': 5},
                        (5, 'some data')),
                    ('SELECT lower', {'lower_2': 'Foo'},
                        ('Foo', )),
                    ('INSERT INTO t1 (c1, c2)',
                     {'c2': 'foo', 'c1': 6},
                     (6, 'foo')),
                    ('select * from t1', {}, ()),
                    ('DROP TABLE t1', {}, ()),
                ]
            else:
                insert2_params = 6, 'Foo'
                if testing.against('oracle+zxjdbc'):
                    insert2_params += (ReturningParam(12), )
                cursor = [('CREATE TABLE t1', {}, ()),
                          ('INSERT INTO t1 (c1, c2)',
                           {'c2': 'some data', 'c1': 5}, (5, 'some data')),
                          ('INSERT INTO t1 (c1, c2)',
                           {'c1': 6, 'lower_2': 'Foo'}, insert2_params),
                          ('select * from t1', {}, ()),
                          ('DROP TABLE t1', {}, ())]
                # bind param name 'lower_2' might
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
        eq_(c2._execution_options, {'foo': 'bar'})
        c2.execute(select([1]))
        c3 = c2.execution_options(bar='bat')
        eq_(c3._execution_options, {'foo': 'bar', 'bar': 'bat'})
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
        event.listen(
            engine, "before_cursor_execute", cursor_execute, retval=True)
        engine.execute(select([1]))
        eq_(
            canary, ['execute', 'cursor_execute']
        )

    def test_engine_connect(self):
        engine = engines.testing_engine()

        tracker = Mock()
        event.listen(engine, "engine_connect", tracker)

        c1 = engine.connect()
        c2 = c1._branch()
        c1.close()
        eq_(
            tracker.mock_calls,
            [call(c1, False), call(c2, True)]
        )

    def test_execution_options(self):
        engine = engines.testing_engine()

        engine_tracker = Mock()
        conn_tracker = Mock()

        event.listen(engine, "set_engine_execution_options", engine_tracker)
        event.listen(engine, "set_connection_execution_options", conn_tracker)

        e2 = engine.execution_options(e1='opt_e1')
        c1 = engine.connect()
        c2 = c1.execution_options(c1='opt_c1')
        c3 = e2.connect()
        c4 = c3.execution_options(c3='opt_c3')
        eq_(
            engine_tracker.mock_calls,
            [call(e2, {'e1': 'opt_e1'})]
        )
        eq_(
            conn_tracker.mock_calls,
            [call(c2, {"c1": "opt_c1"}), call(c4, {"c3": "opt_c3"})]
        )

    @testing.requires.sequences
    @testing.provide_metadata
    def test_cursor_execute(self):
        canary = []

        def tracker(name):
            def go(conn, cursor, statement, parameters, context, executemany):
                canary.append((statement, context))
            return go
        engine = engines.testing_engine()

        t = Table('t', self.metadata,
                  Column('x', Integer, Sequence('t_id_seq'), primary_key=True),
                  implicit_returning=False
                  )
        self.metadata.create_all(engine)
        with engine.begin() as conn:
            event.listen(
                conn, 'before_cursor_execute', tracker('cursor_execute'))
            conn.execute(t.insert())
        # we see the sequence pre-executed in the first call
        assert "t_id_seq" in canary[0][0]
        assert "INSERT" in canary[1][0]
        # same context
        is_(
            canary[0][1], canary[1][1]
        )

    def test_transactional(self):
        canary = []

        def tracker(name):
            def go(conn, *args, **kw):
                canary.append(name)
            return go

        engine = engines.testing_engine()
        event.listen(engine, 'before_execute', tracker('execute'))
        event.listen(
            engine, 'before_cursor_execute', tracker('cursor_execute'))
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

        eq_(
            canary, [
                'begin', 'execute', 'cursor_execute', 'rollback',
                'begin', 'execute', 'cursor_execute', 'commit',
            ])

    def test_transactional_named(self):
        canary = []

        def tracker(name):
            def go(*args, **kw):
                canary.append((name, set(kw)))
            return go

        engine = engines.testing_engine()
        event.listen(engine, 'before_execute', tracker('execute'), named=True)
        event.listen(
            engine, 'before_cursor_execute',
            tracker('cursor_execute'), named=True)
        event.listen(engine, 'begin', tracker('begin'), named=True)
        event.listen(engine, 'commit', tracker('commit'), named=True)
        event.listen(engine, 'rollback', tracker('rollback'), named=True)

        conn = engine.connect()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.rollback()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.commit()

        eq_(
            canary, [
                ('begin', set(['conn', ])),
                ('execute', set([
                    'conn', 'clauseelement', 'multiparams', 'params'])),
                ('cursor_execute', set([
                    'conn', 'cursor', 'executemany',
                    'statement', 'parameters', 'context'])),
                ('rollback', set(['conn', ])), ('begin', set(['conn', ])),
                ('execute', set([
                    'conn', 'clauseelement', 'multiparams', 'params'])),
                ('cursor_execute', set([
                    'conn', 'cursor', 'executemany', 'statement',
                    'parameters', 'context'])),
                ('commit', set(['conn', ]))]
        )

    @testing.requires.savepoints
    @testing.requires.two_phase_transactions
    def test_transactional_advanced(self):
        canary1 = []

        def tracker1(name):
            def go(*args, **kw):
                canary1.append(name)
            return go
        canary2 = []

        def tracker2(name):
            def go(*args, **kw):
                canary2.append(name)
            return go

        engine = engines.testing_engine()
        for name in ['begin', 'savepoint',
                     'rollback_savepoint', 'release_savepoint',
                     'rollback', 'begin_twophase',
                     'prepare_twophase', 'commit_twophase']:
            event.listen(engine, '%s' % name, tracker1(name))

        conn = engine.connect()
        for name in ['begin', 'savepoint',
                     'rollback_savepoint', 'release_savepoint',
                     'rollback', 'begin_twophase',
                     'prepare_twophase', 'commit_twophase']:
            event.listen(conn, '%s' % name, tracker2(name))

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

        eq_(canary1, ['begin', 'savepoint',
                      'rollback_savepoint', 'savepoint', 'release_savepoint',
                      'rollback', 'begin_twophase',
                      'prepare_twophase', 'commit_twophase']
            )
        eq_(canary2, ['begin', 'savepoint',
                      'rollback_savepoint', 'savepoint', 'release_savepoint',
                      'rollback', 'begin_twophase',
                      'prepare_twophase', 'commit_twophase']
            )


class HandleErrorTest(fixtures.TestBase):
    __requires__ = 'ad_hoc_engines',
    __backend__ = True

    def tearDown(self):
        Engine.dispatch._clear()
        Engine._has_events = False

    def test_legacy_dbapi_error(self):
        engine = engines.testing_engine()
        canary = Mock()

        event.listen(engine, "dbapi_error", canary)

        with engine.connect() as conn:
            try:
                conn.execute("SELECT FOO FROM I_DONT_EXIST")
                assert False
            except tsa.exc.DBAPIError as e:
                eq_(canary.mock_calls[0][1][5], e.orig)
                eq_(canary.mock_calls[0][1][2], "SELECT FOO FROM I_DONT_EXIST")

    def test_legacy_dbapi_error_no_ad_hoc_context(self):
        engine = engines.testing_engine()

        listener = Mock(return_value=None)
        event.listen(engine, 'dbapi_error', listener)

        nope = Exception("nope")

        class MyType(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                raise nope

        with engine.connect() as conn:
            assert_raises_message(
                tsa.exc.StatementError,
                r"nope \(original cause: Exception: nope\) u?'SELECT 1 ",
                conn.execute,
                select([1]).where(
                    column('foo') == literal('bar', MyType()))
            )
        # no legacy event
        eq_(listener.mock_calls, [])

    def test_legacy_dbapi_error_non_dbapi_error(self):
        engine = engines.testing_engine()

        listener = Mock(return_value=None)
        event.listen(engine, 'dbapi_error', listener)

        nope = TypeError("I'm not a DBAPI error")
        with engine.connect() as c:
            c.connection.cursor = Mock(
                return_value=Mock(
                    execute=Mock(
                        side_effect=nope
                    ))
            )

            assert_raises_message(
                TypeError,
                "I'm not a DBAPI error",
                c.execute, "select "
            )
        # no legacy event
        eq_(listener.mock_calls, [])

    def test_handle_error(self):
        engine = engines.testing_engine()
        canary = Mock(return_value=None)

        event.listen(engine, "handle_error", canary)

        with engine.connect() as conn:
            try:
                conn.execute("SELECT FOO FROM I_DONT_EXIST")
                assert False
            except tsa.exc.DBAPIError as e:
                ctx = canary.mock_calls[0][1][0]

                eq_(ctx.original_exception, e.orig)
                is_(ctx.sqlalchemy_exception, e)
                eq_(ctx.statement, "SELECT FOO FROM I_DONT_EXIST")

    def test_exception_event_reraise(self):
        engine = engines.testing_engine()

        class MyException(Exception):
            pass

        @event.listens_for(engine, 'handle_error', retval=True)
        def err(context):
            stmt = context.statement
            exception = context.original_exception
            if "ERROR ONE" in str(stmt):
                return MyException("my exception")
            elif "ERROR TWO" in str(stmt):
                return exception
            else:
                return None

        conn = engine.connect()
        # case 1: custom exception
        assert_raises_message(
            MyException,
            "my exception",
            conn.execute, "SELECT 'ERROR ONE' FROM I_DONT_EXIST"
        )
        # case 2: return the DBAPI exception we're given;
        # no wrapping should occur
        assert_raises(
            conn.dialect.dbapi.Error,
            conn.execute, "SELECT 'ERROR TWO' FROM I_DONT_EXIST"
        )
        # case 3: normal wrapping
        assert_raises(
            tsa.exc.DBAPIError,
            conn.execute, "SELECT 'ERROR THREE' FROM I_DONT_EXIST"
        )

    def test_exception_event_reraise_chaining(self):
        engine = engines.testing_engine()

        class MyException1(Exception):
            pass

        class MyException2(Exception):
            pass

        class MyException3(Exception):
            pass

        @event.listens_for(engine, 'handle_error', retval=True)
        def err1(context):
            stmt = context.statement

            if "ERROR ONE" in str(stmt) or "ERROR TWO" in str(stmt) \
                    or "ERROR THREE" in str(stmt):
                return MyException1("my exception")
            elif "ERROR FOUR" in str(stmt):
                raise MyException3("my exception short circuit")

        @event.listens_for(engine, 'handle_error', retval=True)
        def err2(context):
            stmt = context.statement
            if ("ERROR ONE" in str(stmt) or "ERROR FOUR" in str(stmt)) \
                    and isinstance(context.chained_exception, MyException1):
                raise MyException2("my exception chained")
            elif "ERROR TWO" in str(stmt):
                return context.chained_exception
            else:
                return None

        conn = engine.connect()

        with patch.object(engine.
                          dialect.execution_ctx_cls,
                          "handle_dbapi_exception") as patched:
            assert_raises_message(
                MyException2,
                "my exception chained",
                conn.execute, "SELECT 'ERROR ONE' FROM I_DONT_EXIST"
            )
            eq_(patched.call_count, 1)

        with patch.object(engine.
                          dialect.execution_ctx_cls,
                          "handle_dbapi_exception") as patched:
            assert_raises(
                MyException1,
                conn.execute, "SELECT 'ERROR TWO' FROM I_DONT_EXIST"
            )
            eq_(patched.call_count, 1)

        with patch.object(engine.
                          dialect.execution_ctx_cls,
                          "handle_dbapi_exception") as patched:
            # test that non None from err1 isn't cancelled out
            # by err2
            assert_raises(
                MyException1,
                conn.execute, "SELECT 'ERROR THREE' FROM I_DONT_EXIST"
            )
            eq_(patched.call_count, 1)

        with patch.object(engine.
                          dialect.execution_ctx_cls,
                          "handle_dbapi_exception") as patched:
            assert_raises(
                tsa.exc.DBAPIError,
                conn.execute, "SELECT 'ERROR FIVE' FROM I_DONT_EXIST"
            )
            eq_(patched.call_count, 1)

        with patch.object(engine.
                          dialect.execution_ctx_cls,
                          "handle_dbapi_exception") as patched:
            assert_raises_message(
                MyException3,
                "my exception short circuit",
                conn.execute, "SELECT 'ERROR FOUR' FROM I_DONT_EXIST"
            )
            eq_(patched.call_count, 1)

    def test_exception_event_ad_hoc_context(self):
        """test that handle_error is called with a context in
        cases where _handle_dbapi_error() is normally called without
        any context.

        """

        engine = engines.testing_engine()

        listener = Mock(return_value=None)
        event.listen(engine, 'handle_error', listener)

        nope = Exception("nope")

        class MyType(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                raise nope

        with engine.connect() as conn:
            assert_raises_message(
                tsa.exc.StatementError,
                r"nope \(original cause: Exception: nope\) u?'SELECT 1 ",
                conn.execute,
                select([1]).where(
                    column('foo') == literal('bar', MyType()))
            )

        ctx = listener.mock_calls[0][1][0]
        assert ctx.statement.startswith("SELECT 1 ")
        is_(ctx.is_disconnect, False)
        is_(ctx.original_exception, nope)

    def test_exception_event_non_dbapi_error(self):
        """test that dbapi_error is called with a context in
        cases where DBAPI raises an exception that is not a DBAPI
        exception, e.g. internal errors or encoding problems.

        """
        engine = engines.testing_engine()

        listener = Mock(return_value=None)
        event.listen(engine, 'handle_error', listener)

        nope = TypeError("I'm not a DBAPI error")
        with engine.connect() as c:
            c.connection.cursor = Mock(
                return_value=Mock(
                    execute=Mock(
                        side_effect=nope
                    ))
            )

            assert_raises_message(
                TypeError,
                "I'm not a DBAPI error",
                c.execute, "select "
            )
        ctx = listener.mock_calls[0][1][0]
        eq_(ctx.statement, "select ")
        is_(ctx.is_disconnect, False)
        is_(ctx.original_exception, nope)

    def _test_alter_disconnect(self, orig_error, evt_value):
        engine = engines.testing_engine()

        @event.listens_for(engine, "handle_error")
        def evt(ctx):
            ctx.is_disconnect = evt_value

        with patch.object(engine.dialect, "is_disconnect",
                          Mock(return_value=orig_error)):

            with engine.connect() as c:
                try:
                    c.execute("SELECT x FROM nonexistent")
                    assert False
                except tsa.exc.StatementError as st:
                    eq_(st.connection_invalidated, evt_value)

    def test_alter_disconnect_to_true(self):
        self._test_alter_disconnect(False, True)
        self._test_alter_disconnect(True, True)

    def test_alter_disconnect_to_false(self):
        self._test_alter_disconnect(True, False)
        self._test_alter_disconnect(False, False)


class ProxyConnectionTest(fixtures.TestBase):

    """These are the same tests as EngineEventsTest, except using
    the deprecated ConnectionProxy interface.

    """
    __requires__ = 'ad_hoc_engines',
    __prefer_requires__ = 'two_phase_transactions',

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
                    assert False, "Nothing available for stmt: %s" % stmt
                while received:
                    teststmt, testparams, testmultiparams = \
                        received.pop(0)
                    teststmt = re.compile(
                        r'[\n\t ]+', re.M).sub(' ', teststmt).strip()
                    if teststmt.startswith(stmt) and (
                            testparams == params or testparams == posn):
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
                eq_(
                    engine.execute('select * from t1').fetchall(),
                    [(5, 'some data'), (6, 'foo')])
            finally:
                m.drop_all()
            engine.dispose()
            compiled = [('CREATE TABLE t1', {}, None),
                        ('INSERT INTO t1 (c1, c2)', {'c2': 'some data',
                                                     'c1': 5}, None),
                        ('INSERT INTO t1 (c1, c2)', {'c1': 6}, None),
                        ('select * from t1', {}, None),
                        ('DROP TABLE t1', {}, None)]
            if not testing.against('oracle+zxjdbc'):  # or engine.dialect.pr
                                                      # eexecute_pk_sequence
                                                      # s:
                cursor = [
                    ('CREATE TABLE t1', {}, ()),
                    ('INSERT INTO t1 (c1, c2)', {
                     'c2': 'some data', 'c1': 5}, (5, 'some data')),
                    ('SELECT lower', {'lower_2': 'Foo'},
                        ('Foo', )),
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
                          ('INSERT INTO t1 (c1, c2)', {
                           'c2': 'some data', 'c1': 5}, (5, 'some data')),
                          ('INSERT INTO t1 (c1, c2)',
                           {'c1': 6, 'lower_2': 'Foo'}, insert2_params),
                          ('select * from t1', {}, ()),
                          ('DROP TABLE t1', {}, ())]
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
        engine = engines.testing_engine(options={'proxy': TrackProxy()})
        conn = engine.connect()
        c2 = conn.execution_options(foo='bar')
        eq_(c2._execution_options, {'foo': 'bar'})
        c2.execute(select([1]))
        c3 = c2.execution_options(bar='bat')
        eq_(c3._execution_options, {'foo': 'bar', 'bar': 'bat'})
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

        engine = engines.testing_engine(options={'proxy': TrackProxy()})
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.rollback()
        trans = conn.begin()
        conn.execute(select([1]))
        trans.commit()

        eq_(
            canary, [
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

        engine = engines.testing_engine(options={'proxy': TrackProxy()})
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


class DialectEventTest(fixtures.TestBase):

    @contextmanager
    def _run_test(self, retval):
        m1 = Mock()

        m1.do_execute.return_value = retval
        m1.do_executemany.return_value = retval
        m1.do_execute_no_params.return_value = retval
        e = engines.testing_engine(options={"_initialize": False})

        event.listen(e, "do_execute", m1.do_execute)
        event.listen(e, "do_executemany", m1.do_executemany)
        event.listen(e, "do_execute_no_params", m1.do_execute_no_params)

        e.dialect.do_execute = m1.real_do_execute
        e.dialect.do_executemany = m1.real_do_executemany
        e.dialect.do_execute_no_params = m1.real_do_execute_no_params

        def mock_the_cursor(cursor, *arg):
            arg[-1].get_result_proxy = Mock(return_value=Mock(context=arg[-1]))
            return retval

        m1.real_do_execute.side_effect = \
            m1.do_execute.side_effect = mock_the_cursor
        m1.real_do_executemany.side_effect = \
            m1.do_executemany.side_effect = mock_the_cursor
        m1.real_do_execute_no_params.side_effect = \
            m1.do_execute_no_params.side_effect = mock_the_cursor

        with e.connect() as conn:
            yield conn, m1

    def _assert(self, retval, m1, m2, mock_calls):
        eq_(m1.mock_calls, mock_calls)
        if retval:
            eq_(m2.mock_calls, [])
        else:
            eq_(m2.mock_calls, mock_calls)

    def _test_do_execute(self, retval):
        with self._run_test(retval) as (conn, m1):
            result = conn.execute("insert into table foo", {"foo": "bar"})
        self._assert(
            retval,
            m1.do_execute, m1.real_do_execute,
            [call(
                result.context.cursor,
                "insert into table foo",
                {"foo": "bar"}, result.context)]
        )

    def _test_do_executemany(self, retval):
        with self._run_test(retval) as (conn, m1):
            result = conn.execute("insert into table foo",
                                  [{"foo": "bar"}, {"foo": "bar"}])
        self._assert(
            retval,
            m1.do_executemany, m1.real_do_executemany,
            [call(
                result.context.cursor,
                "insert into table foo",
                [{"foo": "bar"}, {"foo": "bar"}], result.context)]
        )

    def _test_do_execute_no_params(self, retval):
        with self._run_test(retval) as (conn, m1):
            result = conn.execution_options(no_parameters=True).\
                execute("insert into table foo")
        self._assert(
            retval,
            m1.do_execute_no_params, m1.real_do_execute_no_params,
            [call(
                result.context.cursor,
                "insert into table foo", result.context)]
        )

    def _test_cursor_execute(self, retval):
        with self._run_test(retval) as (conn, m1):
            dialect = conn.dialect

            stmt = "insert into table foo"
            params = {"foo": "bar"}
            ctx = dialect.execution_ctx_cls._init_statement(
                dialect, conn, conn.connection, stmt, [params])

            conn._cursor_execute(ctx.cursor, stmt, params, ctx)

        self._assert(
            retval,
            m1.do_execute, m1.real_do_execute,
            [call(
                ctx.cursor,
                "insert into table foo",
                {"foo": "bar"}, ctx)]
        )

    def test_do_execute_w_replace(self):
        self._test_do_execute(True)

    def test_do_execute_wo_replace(self):
        self._test_do_execute(False)

    def test_do_executemany_w_replace(self):
        self._test_do_executemany(True)

    def test_do_executemany_wo_replace(self):
        self._test_do_executemany(False)

    def test_do_execute_no_params_w_replace(self):
        self._test_do_execute_no_params(True)

    def test_do_execute_no_params_wo_replace(self):
        self._test_do_execute_no_params(False)

    def test_cursor_execute_w_replace(self):
        self._test_cursor_execute(True)

    def test_cursor_execute_wo_replace(self):
        self._test_cursor_execute(False)
