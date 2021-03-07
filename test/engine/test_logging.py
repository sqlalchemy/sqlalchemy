import logging.handlers
import re

import sqlalchemy as tsa
from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import util
from sqlalchemy.sql import util as sql_util
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import eq_regex
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.util import lazy_gc


def exec_sql(engine, sql, *args, **kwargs):
    with engine.begin() as conn:
        return conn.exec_driver_sql(sql, *args, **kwargs)


class LogParamsTest(fixtures.TestBase):
    __only_on__ = "sqlite"
    __requires__ = ("ad_hoc_engines",)

    def setup_test(self):
        self.eng = engines.testing_engine(options={"echo": True})
        self.no_param_engine = engines.testing_engine(
            options={"echo": True, "hide_parameters": True}
        )
        exec_sql(self.eng, "create table if not exists foo (data string)")
        exec_sql(
            self.no_param_engine,
            "create table if not exists foo (data string)",
        )
        self.buf = logging.handlers.BufferingHandler(100)
        for log in [logging.getLogger("sqlalchemy.engine")]:
            log.addHandler(self.buf)

    def teardown_test(self):
        exec_sql(self.eng, "drop table if exists foo")
        for log in [logging.getLogger("sqlalchemy.engine")]:
            log.removeHandler(self.buf)

    def test_log_large_list_of_dict(self):
        exec_sql(
            self.eng,
            "INSERT INTO foo (data) values (:data)",
            [{"data": str(i)} for i in range(100)],
        )
        eq_(
            self.buf.buffer[2].message,
            "[raw sql] [{'data': '0'}, {'data': '1'}, {'data': '2'}, "
            "{'data': '3'}, "
            "{'data': '4'}, {'data': '5'}, {'data': '6'}, {'data': '7'}"
            "  ... displaying 10 of 100 total bound "
            "parameter sets ...  {'data': '98'}, {'data': '99'}]",
        )

    def test_repr_params_large_list_of_dict(self):
        eq_(
            repr(
                sql_util._repr_params(
                    [{"data": str(i)} for i in range(100)],
                    batches=10,
                    ismulti=True,
                )
            ),
            "[{'data': '0'}, {'data': '1'}, {'data': '2'}, {'data': '3'}, "
            "{'data': '4'}, {'data': '5'}, {'data': '6'}, {'data': '7'}"
            "  ... displaying 10 of 100 total bound "
            "parameter sets ...  {'data': '98'}, {'data': '99'}]",
        )

    def test_log_no_parameters(self):
        exec_sql(
            self.no_param_engine,
            "INSERT INTO foo (data) values (:data)",
            [{"data": str(i)} for i in range(100)],
        )
        eq_(
            self.buf.buffer[2].message,
            "[raw sql] [SQL parameters hidden due to hide_parameters=True]",
        )

    def test_log_large_list_of_tuple(self):
        exec_sql(
            self.eng,
            "INSERT INTO foo (data) values (?)",
            [(str(i),) for i in range(100)],
        )
        eq_(
            self.buf.buffer[2].message,
            "[raw sql] [('0',), ('1',), ('2',), ('3',), ('4',), ('5',), "
            "('6',), ('7',)  ... displaying 10 of 100 total "
            "bound parameter sets ...  ('98',), ('99',)]",
        )

    def test_log_positional_array(self):
        with self.eng.connect() as conn:
            exc_info = assert_raises(
                tsa.exc.DBAPIError,
                conn.execute,
                tsa.text("SELECT * FROM foo WHERE id IN :foo AND bar=:bar"),
                {"foo": [1, 2, 3], "bar": "hi"},
            )

            assert (
                "[SQL: SELECT * FROM foo WHERE id IN ? AND bar=?]\n"
                "[parameters: ([1, 2, 3], 'hi')]\n" in str(exc_info)
            )

            eq_regex(
                self.buf.buffer[1].message,
                r"\[generated .*\] \(\[1, 2, 3\], 'hi'\)",
            )

    def test_repr_params_positional_array(self):
        eq_(
            repr(
                sql_util._repr_params(
                    [[1, 2, 3], 5], batches=10, ismulti=False
                )
            ),
            "[[1, 2, 3], 5]",
        )

    def test_repr_params_unknown_list(self):
        # not known if given multiparams or not.   repr params with
        # straight truncation
        eq_(
            repr(
                sql_util._repr_params(
                    [[i for i in range(300)], 5], batches=10, max_chars=80
                )
            ),
            "[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,  ... "
            "(1315 characters truncated) ... , 293, 294, 295, 296, "
            "297, 298, 299], 5]",
        )

    def test_repr_params_positional_list(self):
        # given non-multi-params in a list.   repr params with
        # per-element truncation, mostly does the exact same thing
        eq_(
            repr(
                sql_util._repr_params(
                    [[i for i in range(300)], 5],
                    batches=10,
                    max_chars=80,
                    ismulti=False,
                )
            ),
            "[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1 ... "
            "(1310 characters truncated) ...  "
            "292, 293, 294, 295, 296, 297, 298, 299], 5]",
        )

    def test_repr_params_named_dict(self):
        # given non-multi-params in a list.   repr params with
        # per-element truncation, mostly does the exact same thing
        params = {"key_%s" % i: i for i in range(10)}
        eq_(
            repr(
                sql_util._repr_params(
                    params, batches=10, max_chars=80, ismulti=False
                )
            ),
            repr(params),
        )

    def test_repr_params_ismulti_named_dict(self):
        # given non-multi-params in a list.   repr params with
        # per-element truncation, mostly does the exact same thing
        param = {"key_%s" % i: i for i in range(10)}
        eq_(
            repr(
                sql_util._repr_params(
                    [param for j in range(50)],
                    batches=5,
                    max_chars=80,
                    ismulti=True,
                )
            ),
            "[%(param)r, %(param)r, %(param)r  ... "
            "displaying 5 of 50 total bound parameter sets ...  "
            "%(param)r, %(param)r]" % {"param": param},
        )

    def test_repr_params_ismulti_list(self):
        # given multi-params in a list.   repr params with
        # per-element truncation, mostly does the exact same thing
        eq_(
            repr(
                sql_util._repr_params(
                    [
                        [[i for i in range(300)], 5],
                        [[i for i in range(300)], 5],
                        [[i for i in range(300)], 5],
                    ],
                    batches=10,
                    max_chars=80,
                    ismulti=True,
                )
            ),
            "[[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1 ... "
            "(1310 characters truncated) ...  292, 293, 294, 295, 296, 297, "
            "298, 299], 5], [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1 ... "
            "(1310 characters truncated) ...  292, 293, 294, 295, 296, 297, "
            "298, 299], 5], [[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 1 ... "
            "(1310 characters truncated) ...  292, 293, 294, 295, 296, 297, "
            "298, 299], 5]]",
        )

    def test_log_large_parameter_single(self):
        import random

        largeparam = "".join(chr(random.randint(52, 85)) for i in range(5000))

        exec_sql(self.eng, "INSERT INTO foo (data) values (?)", (largeparam,))

        eq_(
            self.buf.buffer[2].message,
            "[raw sql] ('%s ... (4702 characters truncated) ... %s',)"
            % (largeparam[0:149], largeparam[-149:]),
        )

    def test_log_large_multi_parameter(self):
        import random

        lp1 = "".join(chr(random.randint(52, 85)) for i in range(5))
        lp2 = "".join(chr(random.randint(52, 85)) for i in range(8))
        lp3 = "".join(chr(random.randint(52, 85)) for i in range(670))

        exec_sql(self.eng, "SELECT ?, ?, ?", (lp1, lp2, lp3))

        eq_(
            self.buf.buffer[2].message,
            "[raw sql] ('%s', '%s', '%s ... (372 characters truncated) "
            "... %s')" % (lp1, lp2, lp3[0:149], lp3[-149:]),
        )

    def test_log_large_parameter_multiple(self):
        import random

        lp1 = "".join(chr(random.randint(52, 85)) for i in range(5000))
        lp2 = "".join(chr(random.randint(52, 85)) for i in range(200))
        lp3 = "".join(chr(random.randint(52, 85)) for i in range(670))

        exec_sql(
            self.eng,
            "INSERT INTO foo (data) values (?)",
            [(lp1,), (lp2,), (lp3,)],
        )

        eq_(
            self.buf.buffer[2].message,
            "[raw sql] [('%s ... (4702 characters truncated) ... %s',), "
            "('%s',), "
            "('%s ... (372 characters truncated) ... %s',)]"
            % (lp1[0:149], lp1[-149:], lp2, lp3[0:149], lp3[-149:]),
        )

    def test_exception_format_dict_param(self):
        exception = tsa.exc.IntegrityError("foo", {"x": "y"}, None)
        eq_regex(
            str(exception),
            r"\(.*.NoneType\) None\n\[SQL: foo\]\n\[parameters: {'x': 'y'}\]",
        )

    def test_exception_format_hide_parameters(self):
        exception = tsa.exc.IntegrityError(
            "foo", {"x": "y"}, None, hide_parameters=True
        )
        eq_regex(
            str(exception),
            r"\(.*.NoneType\) None\n\[SQL: foo\]\n"
            r"\[SQL parameters hidden due to hide_parameters=True\]",
        )

    def test_exception_format_hide_parameters_dbapi_round_trip(self):
        assert_raises_message(
            tsa.exc.DBAPIError,
            r".*INSERT INTO nonexistent \(data\) values \(:data\)\]\n"
            r"\[SQL parameters hidden due to hide_parameters=True\]",
            lambda: exec_sql(
                self.no_param_engine,
                "INSERT INTO nonexistent (data) values (:data)",
                [{"data": str(i)} for i in range(10)],
            ),
        )

    def test_exception_format_hide_parameters_nondbapi_round_trip(self):
        foo = Table("foo", MetaData(), Column("data", String))

        with self.no_param_engine.connect() as conn:
            assert_raises_message(
                tsa.exc.StatementError,
                r"\(sqlalchemy.exc.InvalidRequestError\) A value is required "
                r"for bind parameter 'the_data_2'\n"
                r"\[SQL: SELECT foo.data \nFROM foo \nWHERE "
                r"foo.data = \? OR foo.data = \?\]\n"
                r"\[SQL parameters hidden due to hide_parameters=True\]",
                conn.execute,
                select(foo).where(
                    or_(
                        foo.c.data == bindparam("the_data_1"),
                        foo.c.data == bindparam("the_data_2"),
                    )
                ),
                {"the_data_1": "some data"},
            )

    def test_exception_format_unexpected_parameter(self):
        # test that if the parameters aren't any known type, we just
        # run through repr()
        exception = tsa.exc.IntegrityError("foo", "bar", "bat")
        eq_regex(
            str(exception),
            r"\(.*.str\) bat\n\[SQL: foo\]\n\[parameters: 'bar'\]",
        )

    def test_exception_format_unexpected_member_parameter(self):
        # test that if the parameters aren't any known type, we just
        # run through repr()
        exception = tsa.exc.IntegrityError("foo", ["bar", "bat"], "hoho")
        eq_regex(
            str(exception),
            r"\(.*.str\) hoho\n\[SQL: foo\]\n\[parameters: \['bar', 'bat'\]\]",
        )

    def test_result_large_param(self):
        import random

        largeparam = "".join(chr(random.randint(52, 85)) for i in range(5000))

        self.eng.echo = "debug"
        result = exec_sql(self.eng, "SELECT ?", (largeparam,))

        row = result.first()

        eq_(
            self.buf.buffer[2].message,
            "[raw sql] ('%s ... (4702 characters truncated) ... %s',)"
            % (largeparam[0:149], largeparam[-149:]),
        )

        if util.py3k:
            eq_(
                self.buf.buffer[5].message,
                "Row ('%s ... (4702 characters truncated) ... %s',)"
                % (largeparam[0:149], largeparam[-149:]),
            )
        else:
            eq_(
                self.buf.buffer[5].message,
                "Row (u'%s ... (4703 characters truncated) ... %s',)"
                % (largeparam[0:148], largeparam[-149:]),
            )

        if util.py3k:
            eq_(
                repr(row),
                "('%s ... (4702 characters truncated) ... %s',)"
                % (largeparam[0:149], largeparam[-149:]),
            )
        else:
            eq_(
                repr(row),
                "(u'%s ... (4703 characters truncated) ... %s',)"
                % (largeparam[0:148], largeparam[-149:]),
            )

    def test_error_large_dict(self):
        assert_raises_message(
            tsa.exc.DBAPIError,
            r".*INSERT INTO nonexistent \(data\) values \(:data\)\]\n"
            r"\[parameters: "
            r"\[{'data': '0'}, {'data': '1'}, {'data': '2'}, "
            r"{'data': '3'}, {'data': '4'}, {'data': '5'}, "
            r"{'data': '6'}, {'data': '7'}  ... displaying 10 of "
            r"100 total bound parameter sets ...  {'data': '98'}, "
            r"{'data': '99'}\]",
            lambda: exec_sql(
                self.eng,
                "INSERT INTO nonexistent (data) values (:data)",
                [{"data": str(i)} for i in range(100)],
            ),
        )

    def test_error_large_list(self):
        assert_raises_message(
            tsa.exc.DBAPIError,
            r".*INSERT INTO nonexistent \(data\) values "
            r"\(\?\)\]\n\[parameters: \[\('0',\), \('1',\), \('2',\), "
            r"\('3',\), \('4',\), \('5',\), \('6',\), \('7',\)  "
            r"... displaying "
            r"10 of 100 total bound parameter sets ...  "
            r"\('98',\), \('99',\)\]",
            lambda: exec_sql(
                self.eng,
                "INSERT INTO nonexistent (data) values (?)",
                [(str(i),) for i in range(100)],
            ),
        )


class PoolLoggingTest(fixtures.TestBase):
    def setup_test(self):
        self.existing_level = logging.getLogger("sqlalchemy.pool").level

        self.buf = logging.handlers.BufferingHandler(100)
        for log in [logging.getLogger("sqlalchemy.pool")]:
            log.addHandler(self.buf)

    def teardown_test(self):
        for log in [logging.getLogger("sqlalchemy.pool")]:
            log.removeHandler(self.buf)
        logging.getLogger("sqlalchemy.pool").setLevel(self.existing_level)

    def _queuepool_echo_fixture(self):
        return tsa.pool.QueuePool(creator=mock.Mock(), echo="debug")

    def _queuepool_logging_fixture(self):
        logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)
        return tsa.pool.QueuePool(creator=mock.Mock())

    def _stpool_echo_fixture(self):
        return tsa.pool.SingletonThreadPool(creator=mock.Mock(), echo="debug")

    def _stpool_logging_fixture(self):
        logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)
        return tsa.pool.SingletonThreadPool(creator=mock.Mock())

    def _test_queuepool(self, q, dispose=True):
        conn = q.connect()
        conn.close()
        conn = None

        conn = q.connect()
        conn.close()
        conn = None

        conn = q.connect()
        conn = None
        del conn
        lazy_gc()
        q.dispose()

        eq_(
            [buf.msg for buf in self.buf.buffer],
            [
                "Created new connection %r",
                "Connection %r checked out from pool",
                "Connection %r being returned to pool%s",
                "Connection %s rollback-on-return",
                "Connection %r checked out from pool",
                "Connection %r being returned to pool%s",
                "Connection %s rollback-on-return",
                "Connection %r checked out from pool",
                "Connection %r being returned to pool%s",
                "Connection %s rollback-on-return",
                "Closing connection %r",
            ]
            + (["Pool disposed. %s"] if dispose else []),
        )

    def test_stpool_echo(self):
        q = self._stpool_echo_fixture()
        self._test_queuepool(q, False)

    def test_stpool_logging(self):
        q = self._stpool_logging_fixture()
        self._test_queuepool(q, False)

    @testing.requires.predictable_gc
    def test_queuepool_echo(self):
        q = self._queuepool_echo_fixture()
        self._test_queuepool(q)

    @testing.requires.predictable_gc
    def test_queuepool_logging(self):
        q = self._queuepool_logging_fixture()
        self._test_queuepool(q)


class LoggingNameTest(fixtures.TestBase):
    __requires__ = ("ad_hoc_engines",)

    def _assert_names_in_execute(self, eng, eng_name, pool_name):
        with eng.connect() as conn:
            conn.execute(select(1))
        assert self.buf.buffer
        for name in [b.name for b in self.buf.buffer]:
            assert name in (
                "sqlalchemy.engine.Engine.%s" % eng_name,
                "sqlalchemy.pool.impl.%s.%s"
                % (eng.pool.__class__.__name__, pool_name),
            )

    def _assert_no_name_in_execute(self, eng):
        with eng.connect() as conn:
            conn.execute(select(1))
        assert self.buf.buffer
        for name in [b.name for b in self.buf.buffer]:
            assert name in (
                "sqlalchemy.engine.Engine",
                "sqlalchemy.pool.impl.%s" % eng.pool.__class__.__name__,
            )

    def _named_engine(self, **kw):
        options = {
            "logging_name": "myenginename",
            "pool_logging_name": "mypoolname",
            "echo": True,
        }
        options.update(kw)
        return engines.testing_engine(options=options)

    def _unnamed_engine(self, **kw):
        kw.update({"echo": True})
        return engines.testing_engine(options=kw)

    def setup_test(self):
        self.buf = logging.handlers.BufferingHandler(100)
        for log in [
            logging.getLogger("sqlalchemy.engine"),
            logging.getLogger("sqlalchemy.pool"),
        ]:
            log.addHandler(self.buf)

    def teardown_test(self):
        for log in [
            logging.getLogger("sqlalchemy.engine"),
            logging.getLogger("sqlalchemy.pool"),
        ]:
            log.removeHandler(self.buf)

    def test_named_logger_names(self):
        eng = self._named_engine()
        eq_(eng.logging_name, "myenginename")
        eq_(eng.pool.logging_name, "mypoolname")

    def test_named_logger_names_after_dispose(self):
        eng = self._named_engine()
        with eng.connect() as conn:
            conn.execute(select(1))
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
        eng = self._named_engine(echo="debug", echo_pool="debug")
        self._assert_names_in_execute(eng, "myenginename", "mypoolname")

    def test_named_logger_execute_after_dispose(self):
        eng = self._named_engine()
        with eng.connect() as conn:
            conn.execute(select(1))
        eng.dispose()
        self._assert_names_in_execute(eng, "myenginename", "mypoolname")

    def test_unnamed_logger_execute(self):
        eng = self._unnamed_engine()
        self._assert_no_name_in_execute(eng)

    def test_unnamed_logger_echoflags_execute(self):
        eng = self._unnamed_engine(echo="debug", echo_pool="debug")
        self._assert_no_name_in_execute(eng)


class TransactionContextLoggingTest(fixtures.TestBase):
    @testing.fixture()
    def assert_buf(self, logging_engine):
        buf = logging.handlers.BufferingHandler(100)
        for log in [
            logging.getLogger("sqlalchemy.engine"),
        ]:
            log.addHandler(buf)

        def go(expected):
            assert buf.buffer

            buflines = [rec.msg % rec.args for rec in buf.buffer]

            eq_(buflines, expected)
            buf.flush()

        yield go
        for log in [
            logging.getLogger("sqlalchemy.engine"),
        ]:
            log.removeHandler(buf)

    @testing.fixture()
    def logging_engine(self, testing_engine):
        kw = {"echo": True, "future": True}
        e = testing_engine(options=kw)
        e.connect().close()
        return e

    def test_begin_once_block(self, logging_engine, assert_buf):
        with logging_engine.begin():
            pass

        assert_buf(["BEGIN (implicit)", "COMMIT"])

    def test_commit_as_you_go_block_commit(self, logging_engine, assert_buf):
        with logging_engine.connect() as conn:
            conn.begin()
            conn.commit()

        assert_buf(["BEGIN (implicit)", "COMMIT"])

    def test_commit_as_you_go_block_rollback(self, logging_engine, assert_buf):
        with logging_engine.connect() as conn:
            conn.begin()
            conn.rollback()

        assert_buf(["BEGIN (implicit)", "ROLLBACK"])

    def test_commit_as_you_go_block_commit_autocommit(
        self, logging_engine, assert_buf
    ):
        with logging_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.begin()
            conn.commit()

        assert_buf(
            [
                "BEGIN (implicit)",
                "COMMIT using DBAPI connection.commit(), DBAPI "
                "should ignore due to autocommit mode",
            ]
        )

    def test_commit_as_you_go_block_rollback_autocommit(
        self, logging_engine, assert_buf
    ):
        with logging_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            conn.begin()
            conn.rollback()

        assert_buf(
            [
                "BEGIN (implicit)",
                "ROLLBACK using DBAPI connection.rollback(), DBAPI "
                "should ignore due to autocommit mode",
            ]
        )


class LoggingTokenTest(fixtures.TestBase):
    def setup_test(self):
        self.buf = logging.handlers.BufferingHandler(100)
        for log in [
            logging.getLogger("sqlalchemy.engine"),
        ]:
            log.addHandler(self.buf)

    def teardown_test(self):
        for log in [
            logging.getLogger("sqlalchemy.engine"),
        ]:
            log.removeHandler(self.buf)

    def _assert_token_in_execute(self, conn, token):
        self.buf.flush()
        r = conn.execute(select(1))
        r.all()
        assert self.buf.buffer
        for rec in self.buf.buffer:
            line = rec.msg % rec.args
            assert re.match(r"\[%s\]" % token, line)
        self.buf.flush()

    def _assert_no_tokens_in_execute(self, conn):
        self.buf.flush()
        r = conn.execute(select(1))
        r.all()
        assert self.buf.buffer
        for rec in self.buf.buffer:
            line = rec.msg % rec.args
            assert not re.match(r"\[my_.*?\]", line)
        self.buf.flush()

    @testing.fixture()
    def token_engine(self, testing_engine):
        kw = {"echo": "debug"}
        return testing_engine(options=kw)

    def test_logging_token_option_connection(self, token_engine):
        eng = token_engine

        c1 = eng.connect().execution_options(logging_token="my_name_1")
        c2 = eng.connect().execution_options(logging_token="my_name_2")
        c3 = eng.connect()

        self._assert_token_in_execute(c1, "my_name_1")
        self._assert_token_in_execute(c2, "my_name_2")
        self._assert_no_tokens_in_execute(c3)

        c1.close()
        c2.close()
        c3.close()

    def test_logging_token_option_engine(self, token_engine):
        eng = token_engine

        e1 = eng.execution_options(logging_token="my_name_1")
        e2 = eng.execution_options(logging_token="my_name_2")

        with e1.connect() as c1:
            self._assert_token_in_execute(c1, "my_name_1")

        with e2.connect() as c2:
            self._assert_token_in_execute(c2, "my_name_2")

        with eng.connect() as c3:
            self._assert_no_tokens_in_execute(c3)


class EchoTest(fixtures.TestBase):
    __requires__ = ("ad_hoc_engines",)

    def setup_test(self):
        self.level = logging.getLogger("sqlalchemy.engine").level
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARN)
        self.buf = logging.handlers.BufferingHandler(100)
        logging.getLogger("sqlalchemy.engine").addHandler(self.buf)

    def teardown_test(self):
        logging.getLogger("sqlalchemy.engine").removeHandler(self.buf)
        logging.getLogger("sqlalchemy.engine").setLevel(self.level)

    def _testing_engine(self):
        e = engines.testing_engine()

        # do an initial execute to clear out 'first connect'
        # messages
        with e.connect() as conn:
            conn.execute(select(10)).close()
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

        e1.echo = "debug"
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

        e1 = self._testing_engine()
        e2 = self._testing_engine()

        e1.echo = True

        with e1.connect() as conn:
            conn.execute(select(1)).close()

        with e2.connect() as conn:
            conn.execute(select(2)).close()

        e1.echo = False

        with e1.connect() as conn:
            conn.execute(select(3)).close()
        with e2.connect() as conn:
            conn.execute(select(4)).close()

        e2.echo = True
        with e1.connect() as conn:
            conn.execute(select(5)).close()
        with e2.connect() as conn:
            conn.execute(select(6)).close()

        assert self.buf.buffer[0].getMessage().startswith("SELECT 1")
        assert self.buf.buffer[2].getMessage().startswith("SELECT 6")
        assert len(self.buf.buffer) == 4
