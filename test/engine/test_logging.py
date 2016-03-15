from sqlalchemy.testing import eq_, assert_raises_message, eq_regex
from sqlalchemy import select
import sqlalchemy as tsa
from sqlalchemy.testing import engines
import logging.handlers
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.util import lazy_gc
from sqlalchemy import util


class LogParamsTest(fixtures.TestBase):
    __only_on__ = 'sqlite'
    __requires__ = 'ad_hoc_engines',

    def setup(self):
        self.eng = engines.testing_engine(options={'echo': True})
        self.eng.execute("create table foo (data string)")
        self.buf = logging.handlers.BufferingHandler(100)
        for log in [
            logging.getLogger('sqlalchemy.engine'),
        ]:
            log.addHandler(self.buf)

    def teardown(self):
        self.eng.execute("drop table foo")
        for log in [
            logging.getLogger('sqlalchemy.engine'),
        ]:
            log.removeHandler(self.buf)

    def test_log_large_dict(self):
        self.eng.execute(
            "INSERT INTO foo (data) values (:data)",
            [{"data": str(i)} for i in range(100)]
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
            [(str(i), ) for i in range(100)]
        )
        eq_(
            self.buf.buffer[1].message,
            "[('0',), ('1',), ('2',), ('3',), ('4',), ('5',), "
            "('6',), ('7',)  ... displaying 10 of 100 total "
            "bound parameter sets ...  ('98',), ('99',)]"
        )

    def test_log_large_parameter_single(self):
        import random
        largeparam = ''.join(chr(random.randint(52, 85)) for i in range(5000))

        self.eng.execute(
            "INSERT INTO foo (data) values (?)",
            (largeparam, )
        )

        eq_(
            self.buf.buffer[1].message,
            "('%s ... (4702 characters truncated) ... %s',)" % (
                largeparam[0:149], largeparam[-149:]
            )
        )

    def test_log_large_multi_parameter(self):
        import random
        lp1 = ''.join(chr(random.randint(52, 85)) for i in range(5))
        lp2 = ''.join(chr(random.randint(52, 85)) for i in range(8))
        lp3 = ''.join(chr(random.randint(52, 85)) for i in range(670))

        self.eng.execute(
            "SELECT ?, ?, ?",
            (lp1, lp2, lp3)
        )

        eq_(
            self.buf.buffer[1].message,
            "('%s', '%s', '%s ... (372 characters truncated) ... %s')" % (
                lp1, lp2, lp3[0:149], lp3[-149:]
            )
        )

    def test_log_large_parameter_multiple(self):
        import random
        lp1 = ''.join(chr(random.randint(52, 85)) for i in range(5000))
        lp2 = ''.join(chr(random.randint(52, 85)) for i in range(200))
        lp3 = ''.join(chr(random.randint(52, 85)) for i in range(670))

        self.eng.execute(
            "INSERT INTO foo (data) values (?)",
            [(lp1, ), (lp2, ), (lp3, )]
        )

        eq_(
            self.buf.buffer[1].message,
            "[('%s ... (4702 characters truncated) ... %s',), ('%s',), "
            "('%s ... (372 characters truncated) ... %s',)]" % (
                lp1[0:149], lp1[-149:], lp2, lp3[0:149], lp3[-149:]
            )
        )

    def test_exception_format_dict_param(self):
        exception = tsa.exc.IntegrityError("foo", {"x": "y"}, None)
        eq_regex(
            str(exception),
            r"\(.*.NoneType\) None \[SQL: 'foo'\] \[parameters: {'x': 'y'}\]"
        )

    def test_exception_format_unexpected_parameter(self):
        # test that if the parameters aren't any known type, we just
        # run through repr()
        exception = tsa.exc.IntegrityError("foo", "bar", "bat")
        eq_regex(
            str(exception),
            r"\(.*.str\) bat \[SQL: 'foo'\] \[parameters: 'bar'\]"
        )

    def test_exception_format_unexpected_member_parameter(self):
        # test that if the parameters aren't any known type, we just
        # run through repr()
        exception = tsa.exc.IntegrityError("foo", ["bar", "bat"], "hoho")
        eq_regex(
            str(exception),
            r"\(.*.str\) hoho \[SQL: 'foo'\] \[parameters: \['bar', 'bat'\]\]"
        )

    def test_result_large_param(self):
        import random
        largeparam = ''.join(chr(random.randint(52, 85)) for i in range(5000))

        self.eng.echo = 'debug'
        result = self.eng.execute(
            "SELECT ?",
            (largeparam, )
        )

        row = result.first()

        eq_(
            self.buf.buffer[1].message,
            "('%s ... (4702 characters truncated) ... %s',)" % (
                largeparam[0:149], largeparam[-149:]
            )
        )

        if util.py3k:
            eq_(
                self.buf.buffer[3].message,
                "Row ('%s ... (4702 characters truncated) ... %s',)" % (
                    largeparam[0:149], largeparam[-149:]
                )
            )
        else:
            eq_(
                self.buf.buffer[3].message,
                "Row (u'%s ... (4703 characters truncated) ... %s',)" % (
                    largeparam[0:148], largeparam[-149:]
                )
            )

        if util.py3k:
            eq_(
                repr(row),
                "('%s ... (4702 characters truncated) ... %s',)" % (
                    largeparam[0:149], largeparam[-149:]
                )
            )
        else:
            eq_(
                repr(row),
                "(u'%s ... (4703 characters truncated) ... %s',)" % (
                    largeparam[0:148], largeparam[-149:]
                )
            )

    def test_error_large_dict(self):
        assert_raises_message(
            tsa.exc.DBAPIError,
            r".*'INSERT INTO nonexistent \(data\) values \(:data\)'\] "
            "\[parameters: "
            "\[{'data': '0'}, {'data': '1'}, {'data': '2'}, "
            "{'data': '3'}, {'data': '4'}, {'data': '5'}, "
            "{'data': '6'}, {'data': '7'}  ... displaying 10 of "
            "100 total bound parameter sets ...  {'data': '98'}, {'data': '99'}\]",
            lambda: self.eng.execute(
                "INSERT INTO nonexistent (data) values (:data)",
                [{"data": str(i)} for i in range(100)]
            )
        )

    def test_error_large_list(self):
        assert_raises_message(
            tsa.exc.DBAPIError,
            r".*INSERT INTO nonexistent \(data\) values "
            "\(\?\)'\] \[parameters: \[\('0',\), \('1',\), \('2',\), \('3',\), "
            "\('4',\), \('5',\), \('6',\), \('7',\)  "
            "... displaying "
            "10 of 100 total bound parameter sets ...  "
            "\('98',\), \('99',\)\]",
            lambda: self.eng.execute(
                "INSERT INTO nonexistent (data) values (?)",
                [(str(i), ) for i in range(100)]
            )
        )


class PoolLoggingTest(fixtures.TestBase):
    def setup(self):
        self.existing_level = logging.getLogger("sqlalchemy.pool").level

        self.buf = logging.handlers.BufferingHandler(100)
        for log in [
            logging.getLogger('sqlalchemy.pool')
        ]:
            log.addHandler(self.buf)

    def teardown(self):
        for log in [
            logging.getLogger('sqlalchemy.pool')
        ]:
            log.removeHandler(self.buf)
        logging.getLogger("sqlalchemy.pool").setLevel(self.existing_level)

    def _queuepool_echo_fixture(self):
        return tsa.pool.QueuePool(creator=mock.Mock(), echo='debug')

    def _queuepool_logging_fixture(self):
        logging.getLogger("sqlalchemy.pool").setLevel(logging.DEBUG)
        return tsa.pool.QueuePool(creator=mock.Mock())

    def _stpool_echo_fixture(self):
        return tsa.pool.SingletonThreadPool(creator=mock.Mock(), echo='debug')

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
                'Created new connection %r',
                'Connection %r checked out from pool',
                'Connection %r being returned to pool',
                'Connection %s rollback-on-return%s',
                'Connection %r checked out from pool',
                'Connection %r being returned to pool',
                'Connection %s rollback-on-return%s',
                'Connection %r checked out from pool',
                'Connection %r being returned to pool',
                'Connection %s rollback-on-return%s',
                'Closing connection %r',

            ] + (['Pool disposed. %s'] if dispose else [])
        )

    def test_stpool_echo(self):
        q = self._stpool_echo_fixture()
        self._test_queuepool(q, False)

    def test_stpool_logging(self):
        q = self._stpool_logging_fixture()
        self._test_queuepool(q, False)

    def test_queuepool_echo(self):
        q = self._queuepool_echo_fixture()
        self._test_queuepool(q)

    def test_queuepool_logging(self):
        q = self._queuepool_logging_fixture()
        self._test_queuepool(q)


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
            'logging_name': 'myenginename',
            'pool_logging_name': 'mypoolname',
            'echo': True
        }
        options.update(kw)
        return engines.testing_engine(options=options)

    def _unnamed_engine(self, **kw):
        kw.update({'echo': True})
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

    def _testing_engine(self):
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

        e1 = self._testing_engine()
        e2 = self._testing_engine()

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
