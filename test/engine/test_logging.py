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
import logging.handlers
from sqlalchemy.dialects.oracle.zxjdbc import ReturningParam
from sqlalchemy.engine import result as _result, default
from sqlalchemy.engine.base import Engine
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.mock import Mock, call, patch

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
            [{"data":str(i)} for i in range(100)]
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
                [{"data":str(i)} for i in range(100)]
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
                [(str(i), ) for i in range(100)]
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
