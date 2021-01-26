# coding: utf-8
import datetime
import itertools
import logging
import logging.handlers

from sqlalchemy import BigInteger
from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import Numeric
from sqlalchemy import schema
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import TypeDecorator
from sqlalchemy import util
from sqlalchemy.dialects.postgresql import base as postgresql
from sqlalchemy.dialects.postgresql import psycopg2 as psycopg2_dialect
from sqlalchemy.dialects.postgresql.psycopg2 import EXECUTEMANY_BATCH
from sqlalchemy.dialects.postgresql.psycopg2 import EXECUTEMANY_PLAIN
from sqlalchemy.dialects.postgresql.psycopg2 import EXECUTEMANY_VALUES
from sqlalchemy.engine import cursor as _cursor
from sqlalchemy.engine import engine_from_config
from sqlalchemy.engine import url
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_false
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import assert_raises_message
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy.testing.assertions import AssertsExecutionResults
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import eq_regex
from sqlalchemy.testing.assertions import ne_
from sqlalchemy.util import u
from sqlalchemy.util import ue
from ...engine import test_deprecations

if True:
    from sqlalchemy.dialects.postgresql.psycopg2 import (
        EXECUTEMANY_VALUES_PLUS_BATCH,
    )


class DialectTest(fixtures.TestBase):
    """python-side dialect tests.  """

    def test_version_parsing(self):
        def mock_conn(res):
            return mock.Mock(
                exec_driver_sql=mock.Mock(
                    return_value=mock.Mock(scalar=mock.Mock(return_value=res))
                )
            )

        dialect = postgresql.dialect()
        for string, version in [
            (
                "PostgreSQL 8.3.8 on i686-redhat-linux-gnu, compiled by "
                "GCC gcc (GCC) 4.1.2 20070925 (Red Hat 4.1.2-33)",
                (8, 3, 8),
            ),
            (
                "PostgreSQL 8.5devel on x86_64-unknown-linux-gnu, "
                "compiled by GCC gcc (GCC) 4.4.2, 64-bit",
                (8, 5),
            ),
            (
                "EnterpriseDB 9.1.2.2 on x86_64-unknown-linux-gnu, "
                "compiled by gcc (GCC) 4.1.2 20080704 (Red Hat 4.1.2-50), "
                "64-bit",
                (9, 1, 2),
            ),
            (
                "[PostgreSQL 9.2.4 ] VMware vFabric Postgres 9.2.4.0 "
                "release build 1080137",
                (9, 2, 4),
            ),
            (
                "PostgreSQL 10devel on x86_64-pc-linux-gnu"
                "compiled by gcc (GCC) 6.3.1 20170306, 64-bit",
                (10,),
            ),
            (
                "PostgreSQL 10beta1 on x86_64-pc-linux-gnu, "
                "compiled by gcc (GCC) 4.8.5 20150623 "
                "(Red Hat 4.8.5-11), 64-bit",
                (10,),
            ),
            (
                "PostgreSQL 8.0.2 on i686-pc-linux-gnu, compiled by GCC gcc "
                "(GCC) 3.4.2 20041017 (Red Hat 3.4.2-6.fc3), "
                "Redshift 1.0.12103",
                (8, 0, 2),
            ),
        ]:
            eq_(dialect._get_server_version_info(mock_conn(string)), version)

    @testing.requires.python3
    @testing.requires.psycopg2_compatibility
    def test_pg_dialect_no_native_unicode_in_python3(self, testing_engine):
        with testing.expect_raises_message(
            exc.ArgumentError,
            "psycopg2 native_unicode mode is required under Python 3",
        ):
            testing_engine(options=dict(use_native_unicode=False))

    @testing.requires.python2
    @testing.requires.psycopg2_compatibility
    def test_pg_dialect_no_native_unicode_in_python2(self, testing_engine):
        e = testing_engine(options=dict(use_native_unicode=False))
        with e.connect() as conn:
            eq_(
                conn.exec_driver_sql(u"SELECT '🐍 voix m’a réveillé'").scalar(),
                u"🐍 voix m’a réveillé".encode("utf-8"),
            )

    @testing.requires.python2
    @testing.requires.psycopg2_compatibility
    def test_pg_dialect_use_native_unicode_from_config(self):
        config = {
            "sqlalchemy.url": testing.db.url,
            "sqlalchemy.use_native_unicode": "false",
        }

        e = engine_from_config(config, _initialize=False)
        eq_(e.dialect.use_native_unicode, False)

        config = {
            "sqlalchemy.url": testing.db.url,
            "sqlalchemy.use_native_unicode": "true",
        }

        e = engine_from_config(config, _initialize=False)
        eq_(e.dialect.use_native_unicode, True)

    def test_psycopg2_empty_connection_string(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql://")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [""])
        eq_(cparams, {})

    def test_psycopg2_nonempty_connection_string(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql://host")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, {"host": "host"})

    def test_psycopg2_empty_connection_string_w_query_one(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql:///?service=swh-log")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, {"service": "swh-log"})

    def test_psycopg2_empty_connection_string_w_query_two(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql:///?any_random_thing=yes")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, {"any_random_thing": "yes"})

    def test_psycopg2_nonempty_connection_string_w_query(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql://somehost/?any_random_thing=yes")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, {"host": "somehost", "any_random_thing": "yes"})

    def test_psycopg2_nonempty_connection_string_w_query_two(self):
        dialect = psycopg2_dialect.dialect()
        url_string = "postgresql://USER:PASS@/DB?host=hostA"
        u = url.make_url(url_string)
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams["host"], "hostA")

    def test_psycopg2_nonempty_connection_string_w_query_three(self):
        dialect = psycopg2_dialect.dialect()
        url_string = (
            "postgresql://USER:PASS@/DB"
            "?host=hostA:portA&host=hostB&host=hostC"
        )
        u = url.make_url(url_string)
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams["host"], "hostA:portA,hostB,hostC")


class ExecuteManyMode(object):
    __only_on__ = "postgresql+psycopg2"
    __backend__ = True

    run_create_tables = "each"
    run_deletes = None

    options = None

    @config.fixture()
    def connection(self):
        opts = dict(self.options)
        opts["use_reaper"] = False
        eng = engines.testing_engine(options=opts)

        conn = eng.connect()
        trans = conn.begin()
        yield conn
        if trans.is_active:
            trans.rollback()
        conn.close()
        eng.dispose()

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "data",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", String),
            Column("y", String),
            Column("z", Integer, server_default="5"),
        )

        Table(
            u("Unitéble2"),
            metadata,
            Column(u("méil"), Integer, primary_key=True),
            Column(ue("\u6e2c\u8a66"), Integer),
        )

    def test_insert(self, connection):
        from psycopg2 import extras

        values_page_size = connection.dialect.executemany_values_page_size
        batch_page_size = connection.dialect.executemany_batch_page_size
        if connection.dialect.executemany_mode & EXECUTEMANY_VALUES:
            meth = extras.execute_values
            stmt = "INSERT INTO data (x, y) VALUES %s"
            expected_kwargs = {
                "template": "(%(x)s, %(y)s)",
                "page_size": values_page_size,
                "fetch": False,
            }
        elif connection.dialect.executemany_mode & EXECUTEMANY_BATCH:
            meth = extras.execute_batch
            stmt = "INSERT INTO data (x, y) VALUES (%(x)s, %(y)s)"
            expected_kwargs = {"page_size": batch_page_size}
        else:
            assert False

        with mock.patch.object(
            extras, meth.__name__, side_effect=meth
        ) as mock_exec:
            connection.execute(
                self.tables.data.insert(),
                [
                    {"x": "x1", "y": "y1"},
                    {"x": "x2", "y": "y2"},
                    {"x": "x3", "y": "y3"},
                ],
            )

            eq_(
                connection.execute(select(self.tables.data)).fetchall(),
                [
                    (1, "x1", "y1", 5),
                    (2, "x2", "y2", 5),
                    (3, "x3", "y3", 5),
                ],
            )
        eq_(
            mock_exec.mock_calls,
            [
                mock.call(
                    mock.ANY,
                    stmt,
                    (
                        {"x": "x1", "y": "y1"},
                        {"x": "x2", "y": "y2"},
                        {"x": "x3", "y": "y3"},
                    ),
                    **expected_kwargs
                )
            ],
        )

    def test_insert_no_page_size(self, connection):
        from psycopg2 import extras

        values_page_size = connection.dialect.executemany_values_page_size
        batch_page_size = connection.dialect.executemany_batch_page_size

        if connection.dialect.executemany_mode & EXECUTEMANY_VALUES:
            meth = extras.execute_values
            stmt = "INSERT INTO data (x, y) VALUES %s"
            expected_kwargs = {
                "template": "(%(x)s, %(y)s)",
                "page_size": values_page_size,
                "fetch": False,
            }
        elif connection.dialect.executemany_mode & EXECUTEMANY_BATCH:
            meth = extras.execute_batch
            stmt = "INSERT INTO data (x, y) VALUES (%(x)s, %(y)s)"
            expected_kwargs = {"page_size": batch_page_size}
        else:
            assert False

        with mock.patch.object(
            extras, meth.__name__, side_effect=meth
        ) as mock_exec:
            connection.execute(
                self.tables.data.insert(),
                [
                    {"x": "x1", "y": "y1"},
                    {"x": "x2", "y": "y2"},
                    {"x": "x3", "y": "y3"},
                ],
            )

        eq_(
            mock_exec.mock_calls,
            [
                mock.call(
                    mock.ANY,
                    stmt,
                    (
                        {"x": "x1", "y": "y1"},
                        {"x": "x2", "y": "y2"},
                        {"x": "x3", "y": "y3"},
                    ),
                    **expected_kwargs
                )
            ],
        )

    def test_insert_page_size(self):
        from psycopg2 import extras

        opts = self.options.copy()
        opts["executemany_batch_page_size"] = 500
        opts["executemany_values_page_size"] = 1000

        eng = engines.testing_engine(options=opts)

        if eng.dialect.executemany_mode & EXECUTEMANY_VALUES:
            meth = extras.execute_values
            stmt = "INSERT INTO data (x, y) VALUES %s"
            expected_kwargs = {
                "fetch": False,
                "page_size": 1000,
                "template": "(%(x)s, %(y)s)",
            }
        elif eng.dialect.executemany_mode & EXECUTEMANY_BATCH:
            meth = extras.execute_batch
            stmt = "INSERT INTO data (x, y) VALUES (%(x)s, %(y)s)"
            expected_kwargs = {"page_size": 500}
        else:
            assert False

        with mock.patch.object(
            extras, meth.__name__, side_effect=meth
        ) as mock_exec:
            with eng.begin() as conn:
                conn.execute(
                    self.tables.data.insert(),
                    [
                        {"x": "x1", "y": "y1"},
                        {"x": "x2", "y": "y2"},
                        {"x": "x3", "y": "y3"},
                    ],
                )

        eq_(
            mock_exec.mock_calls,
            [
                mock.call(
                    mock.ANY,
                    stmt,
                    (
                        {"x": "x1", "y": "y1"},
                        {"x": "x2", "y": "y2"},
                        {"x": "x3", "y": "y3"},
                    ),
                    **expected_kwargs
                )
            ],
        )

    def test_insert_unicode_keys(self, connection):
        table = self.tables[u("Unitéble2")]

        stmt = table.insert()

        connection.execute(
            stmt,
            [
                {u("méil"): 1, ue("\u6e2c\u8a66"): 1},
                {u("méil"): 2, ue("\u6e2c\u8a66"): 2},
                {u("méil"): 3, ue("\u6e2c\u8a66"): 3},
            ],
        )

        eq_(connection.execute(table.select()).all(), [(1, 1), (2, 2), (3, 3)])

    def test_update_fallback(self, connection):
        from psycopg2 import extras

        batch_page_size = connection.dialect.executemany_batch_page_size
        meth = extras.execute_batch
        stmt = "UPDATE data SET y=%(yval)s WHERE data.x = %(xval)s"
        expected_kwargs = {"page_size": batch_page_size}

        with mock.patch.object(
            extras, meth.__name__, side_effect=meth
        ) as mock_exec:
            connection.execute(
                self.tables.data.update()
                .where(self.tables.data.c.x == bindparam("xval"))
                .values(y=bindparam("yval")),
                [
                    {"xval": "x1", "yval": "y5"},
                    {"xval": "x3", "yval": "y6"},
                ],
            )

        if connection.dialect.executemany_mode & EXECUTEMANY_BATCH:
            eq_(
                mock_exec.mock_calls,
                [
                    mock.call(
                        mock.ANY,
                        stmt,
                        (
                            {"xval": "x1", "yval": "y5"},
                            {"xval": "x3", "yval": "y6"},
                        ),
                        **expected_kwargs
                    )
                ],
            )
        else:
            eq_(mock_exec.mock_calls, [])

    def test_not_sane_rowcount(self, connection):
        if connection.dialect.executemany_mode & EXECUTEMANY_BATCH:
            assert not connection.dialect.supports_sane_multi_rowcount
        else:
            assert connection.dialect.supports_sane_multi_rowcount

    def test_update(self, connection):
        connection.execute(
            self.tables.data.insert(),
            [
                {"x": "x1", "y": "y1"},
                {"x": "x2", "y": "y2"},
                {"x": "x3", "y": "y3"},
            ],
        )

        connection.execute(
            self.tables.data.update()
            .where(self.tables.data.c.x == bindparam("xval"))
            .values(y=bindparam("yval")),
            [{"xval": "x1", "yval": "y5"}, {"xval": "x3", "yval": "y6"}],
        )
        eq_(
            connection.execute(
                select(self.tables.data).order_by(self.tables.data.c.id)
            ).fetchall(),
            [(1, "x1", "y5", 5), (2, "x2", "y2", 5), (3, "x3", "y6", 5)],
        )


class ExecutemanyBatchModeTest(ExecuteManyMode, fixtures.TablesTest):
    options = {"executemany_mode": "batch"}


class ExecutemanyValuesInsertsTest(ExecuteManyMode, fixtures.TablesTest):
    options = {"executemany_mode": "values_only"}

    def test_insert_returning_values(self, connection):
        """the psycopg2 dialect needs to assemble a fully buffered result
        with the return value of execute_values().

        """
        t = self.tables.data

        conn = connection
        page_size = conn.dialect.executemany_values_page_size or 100
        data = [
            {"x": "x%d" % i, "y": "y%d" % i}
            for i in range(1, page_size * 5 + 27)
        ]
        result = conn.execute(t.insert().returning(t.c.x, t.c.y), data)

        eq_([tup[0] for tup in result.cursor.description], ["x", "y"])
        eq_(result.keys(), ["x", "y"])
        assert t.c.x in result.keys()
        assert t.c.id not in result.keys()
        assert not result._soft_closed
        assert isinstance(
            result.cursor_strategy,
            _cursor.FullyBufferedCursorFetchStrategy,
        )
        assert not result.cursor.closed
        assert not result.closed
        eq_(result.mappings().all(), data)

        assert result._soft_closed
        # assert result.closed
        assert result.cursor is None

    def test_insert_returning_preexecute_pk(self, metadata, connection):
        counter = itertools.count(1)

        t = Table(
            "t",
            self.metadata,
            Column(
                "id",
                Integer,
                primary_key=True,
                default=lambda: util.next(counter),
            ),
            Column("data", Integer),
        )
        metadata.create_all(connection)

        result = connection.execute(
            t.insert().return_defaults(),
            [{"data": 1}, {"data": 2}, {"data": 3}],
        )

        eq_(result.inserted_primary_key_rows, [(1,), (2,), (3,)])

    def test_insert_returning_defaults(self, connection):
        t = self.tables.data

        conn = connection

        result = conn.execute(t.insert(), {"x": "x0", "y": "y0"})
        first_pk = result.inserted_primary_key[0]

        page_size = conn.dialect.executemany_values_page_size or 100
        total_rows = page_size * 5 + 27
        data = [{"x": "x%d" % i, "y": "y%d" % i} for i in range(1, total_rows)]
        result = conn.execute(t.insert().returning(t.c.id, t.c.z), data)

        eq_(
            result.all(),
            [(pk, 5) for pk in range(1 + first_pk, total_rows + first_pk)],
        )

    def test_insert_return_pks_default_values(self, connection):
        """test sending multiple, empty rows into an INSERT and getting primary
        key values back.

        This has to use a format that indicates at least one DEFAULT in
        multiple parameter sets, i.e. "INSERT INTO table (anycol) VALUES
        (DEFAULT) (DEFAULT) (DEFAULT) ... RETURNING col"

        """
        t = self.tables.data

        conn = connection

        result = conn.execute(t.insert(), {"x": "x0", "y": "y0"})
        first_pk = result.inserted_primary_key[0]

        page_size = conn.dialect.executemany_values_page_size or 100
        total_rows = page_size * 5 + 27
        data = [{} for i in range(1, total_rows)]
        result = conn.execute(t.insert().returning(t.c.id), data)

        eq_(
            result.all(),
            [(pk,) for pk in range(1 + first_pk, total_rows + first_pk)],
        )

    def test_insert_w_newlines(self, connection):
        from psycopg2 import extras

        t = self.tables.data

        ins = (
            t.insert()
            .inline()
            .values(
                id=bindparam("id"),
                x=select(literal_column("5"))
                .select_from(self.tables.data)
                .scalar_subquery(),
                y=bindparam("y"),
                z=bindparam("z"),
            )
        )
        # compiled SQL has a newline in it
        eq_(
            str(ins.compile(testing.db)),
            "INSERT INTO data (id, x, y, z) VALUES (%(id)s, "
            "(SELECT 5 \nFROM data), %(y)s, %(z)s)",
        )
        meth = extras.execute_values
        with mock.patch.object(
            extras, "execute_values", side_effect=meth
        ) as mock_exec:

            connection.execute(
                ins,
                [
                    {"id": 1, "y": "y1", "z": 1},
                    {"id": 2, "y": "y2", "z": 2},
                    {"id": 3, "y": "y3", "z": 3},
                ],
            )

        eq_(
            mock_exec.mock_calls,
            [
                mock.call(
                    mock.ANY,
                    "INSERT INTO data (id, x, y, z) VALUES %s",
                    (
                        {"id": 1, "y": "y1", "z": 1},
                        {"id": 2, "y": "y2", "z": 2},
                        {"id": 3, "y": "y3", "z": 3},
                    ),
                    template="(%(id)s, (SELECT 5 \nFROM data), %(y)s, %(z)s)",
                    fetch=False,
                    page_size=connection.dialect.executemany_values_page_size,
                )
            ],
        )

    def test_insert_modified_by_event(self, connection):
        from psycopg2 import extras

        t = self.tables.data

        ins = (
            t.insert()
            .inline()
            .values(
                id=bindparam("id"),
                x=select(literal_column("5"))
                .select_from(self.tables.data)
                .scalar_subquery(),
                y=bindparam("y"),
                z=bindparam("z"),
            )
        )
        # compiled SQL has a newline in it
        eq_(
            str(ins.compile(testing.db)),
            "INSERT INTO data (id, x, y, z) VALUES (%(id)s, "
            "(SELECT 5 \nFROM data), %(y)s, %(z)s)",
        )
        meth = extras.execute_batch
        with mock.patch.object(
            extras, "execute_values"
        ) as mock_values, mock.patch.object(
            extras, "execute_batch", side_effect=meth
        ) as mock_batch:

            # create an event hook that will change the statement to
            # something else, meaning the dialect has to detect that
            # insert_single_values_expr is no longer useful
            @event.listens_for(
                connection, "before_cursor_execute", retval=True
            )
            def before_cursor_execute(
                conn, cursor, statement, parameters, context, executemany
            ):
                statement = (
                    "INSERT INTO data (id, y, z) VALUES "
                    "(%(id)s, %(y)s, %(z)s)"
                )
                return statement, parameters

            connection.execute(
                ins,
                [
                    {"id": 1, "y": "y1", "z": 1},
                    {"id": 2, "y": "y2", "z": 2},
                    {"id": 3, "y": "y3", "z": 3},
                ],
            )

        eq_(mock_values.mock_calls, [])

        if connection.dialect.executemany_mode & EXECUTEMANY_BATCH:
            eq_(
                mock_batch.mock_calls,
                [
                    mock.call(
                        mock.ANY,
                        "INSERT INTO data (id, y, z) VALUES "
                        "(%(id)s, %(y)s, %(z)s)",
                        (
                            {"id": 1, "y": "y1", "z": 1},
                            {"id": 2, "y": "y2", "z": 2},
                            {"id": 3, "y": "y3", "z": 3},
                        ),
                    )
                ],
            )
        else:
            eq_(mock_batch.mock_calls, [])


class ExecutemanyValuesPlusBatchInsertsTest(
    ExecuteManyMode, fixtures.TablesTest
):
    options = {"executemany_mode": "values_plus_batch"}


class ExecutemanyFlagOptionsTest(fixtures.TablesTest):
    __only_on__ = "postgresql+psycopg2"
    __backend__ = True

    def test_executemany_correct_flag_options(self):
        for opt, expected in [
            (None, EXECUTEMANY_PLAIN),
            ("batch", EXECUTEMANY_BATCH),
            ("values_only", EXECUTEMANY_VALUES),
            ("values_plus_batch", EXECUTEMANY_VALUES_PLUS_BATCH),
        ]:
            connection = engines.testing_engine(
                options={"executemany_mode": opt}
            )
            is_(connection.dialect.executemany_mode, expected)

    def test_executemany_wrong_flag_options(self):
        for opt in [1, True, "batch_insert"]:
            assert_raises_message(
                exc.ArgumentError,
                "Invalid value for 'executemany_mode': %r" % opt,
                engines.testing_engine,
                options={"executemany_mode": opt},
            )


class MiscBackendTest(
    fixtures.TestBase, AssertsExecutionResults, AssertsCompiledSQL
):

    __only_on__ = "postgresql"
    __backend__ = True

    @testing.provide_metadata
    def test_date_reflection(self):
        metadata = self.metadata
        Table(
            "pgdate",
            metadata,
            Column("date1", DateTime(timezone=True)),
            Column("date2", DateTime(timezone=False)),
        )
        metadata.create_all(testing.db)
        m2 = MetaData()
        t2 = Table("pgdate", m2, autoload_with=testing.db)
        assert t2.c.date1.type.timezone is True
        assert t2.c.date2.type.timezone is False

    @testing.requires.psycopg2_compatibility
    def test_psycopg2_version(self):
        v = testing.db.dialect.psycopg2_version
        assert testing.db.dialect.dbapi.__version__.startswith(
            ".".join(str(x) for x in v)
        )

    @testing.combinations(
        ((8, 1), False, False),
        ((8, 1), None, False),
        ((11, 5), True, False),
        ((11, 5), False, True),
    )
    def test_backslash_escapes_detection(
        self, version, explicit_setting, expected
    ):
        engine = engines.testing_engine()

        def _server_version(conn):
            return version

        if explicit_setting is not None:

            @event.listens_for(engine, "connect", insert=True)
            @event.listens_for(engine, "first_connect", insert=True)
            def connect(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute(
                    "SET SESSION standard_conforming_strings = %s"
                    % ("off" if not explicit_setting else "on")
                )
                dbapi_connection.commit()

        with mock.patch.object(
            engine.dialect, "_get_server_version_info", _server_version
        ):
            with engine.connect():
                eq_(engine.dialect._backslash_escapes, expected)

    def test_dbapi_autocommit_attribute(self):
        """all the supported DBAPIs have an .autocommit attribute.  make
        sure it works and preserves isolation level.

        This is added in particular to support the asyncpg dialect that
        has a DBAPI compatibility layer.

        """

        with testing.db.connect().execution_options(
            isolation_level="SERIALIZABLE"
        ) as conn:
            dbapi_conn = conn.connection.connection

            is_false(dbapi_conn.autocommit)

            with conn.begin():

                existing_isolation = conn.exec_driver_sql(
                    "show transaction isolation level"
                ).scalar()
                eq_(existing_isolation.upper(), "SERIALIZABLE")

                txid1 = conn.exec_driver_sql("select txid_current()").scalar()
                txid2 = conn.exec_driver_sql("select txid_current()").scalar()
                eq_(txid1, txid2)

            dbapi_conn.autocommit = True

            with conn.begin():
                # magic way to see if we are in autocommit mode from
                # the server's perspective
                txid1 = conn.exec_driver_sql("select txid_current()").scalar()
                txid2 = conn.exec_driver_sql("select txid_current()").scalar()
                ne_(txid1, txid2)

            dbapi_conn.autocommit = False

            with conn.begin():

                existing_isolation = conn.exec_driver_sql(
                    "show transaction isolation level"
                ).scalar()
                eq_(existing_isolation.upper(), "SERIALIZABLE")

                txid1 = conn.exec_driver_sql("select txid_current()").scalar()
                txid2 = conn.exec_driver_sql("select txid_current()").scalar()
                eq_(txid1, txid2)

    def test_readonly_flag_connection(self):
        with testing.db.connect() as conn:
            # asyncpg requires serializable for readonly..
            conn = conn.execution_options(
                isolation_level="SERIALIZABLE", postgresql_readonly=True
            )

            dbapi_conn = conn.connection.connection

            cursor = dbapi_conn.cursor()
            cursor.execute("show transaction_read_only")
            val = cursor.fetchone()[0]
            cursor.close()
            eq_(val, "on")
            is_true(testing.db.dialect.get_readonly(dbapi_conn))

        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("show transaction_read_only")
            val = cursor.fetchone()[0]
        finally:
            cursor.close()
            dbapi_conn.rollback()
        eq_(val, "off")

    def test_deferrable_flag_connection(self):
        with testing.db.connect() as conn:
            # asyncpg but not for deferrable?  which the PG docs actually
            # state.  weird
            conn = conn.execution_options(
                isolation_level="SERIALIZABLE", postgresql_deferrable=True
            )

            dbapi_conn = conn.connection.connection

            cursor = dbapi_conn.cursor()
            cursor.execute("show transaction_deferrable")
            val = cursor.fetchone()[0]
            cursor.close()
            eq_(val, "on")
            is_true(testing.db.dialect.get_deferrable(dbapi_conn))

        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("show transaction_deferrable")
            val = cursor.fetchone()[0]
        finally:
            cursor.close()
            dbapi_conn.rollback()
        eq_(val, "off")

    def test_readonly_flag_engine(self):
        engine = engines.testing_engine(
            options={
                "execution_options": dict(
                    isolation_level="SERIALIZABLE", postgresql_readonly=True
                )
            }
        )
        for i in range(2):
            with engine.connect() as conn:
                dbapi_conn = conn.connection.connection

                cursor = dbapi_conn.cursor()
                cursor.execute("show transaction_read_only")
                val = cursor.fetchone()[0]
                cursor.close()
                eq_(val, "on")

            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("show transaction_read_only")
                val = cursor.fetchone()[0]
            finally:
                cursor.close()
                dbapi_conn.rollback()
            eq_(val, "off")

    def test_deferrable_flag_engine(self):
        engine = engines.testing_engine(
            options={
                "execution_options": dict(
                    isolation_level="SERIALIZABLE", postgresql_deferrable=True
                )
            }
        )

        for i in range(2):
            with engine.connect() as conn:
                # asyncpg but not for deferrable?  which the PG docs actually
                # state.  weird
                dbapi_conn = conn.connection.connection

                cursor = dbapi_conn.cursor()
                cursor.execute("show transaction_deferrable")
                val = cursor.fetchone()[0]
                cursor.close()
                eq_(val, "on")

            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("show transaction_deferrable")
                val = cursor.fetchone()[0]
            finally:
                cursor.close()
                dbapi_conn.rollback()
            eq_(val, "off")

    @testing.requires.psycopg2_compatibility
    def test_psycopg2_non_standard_err(self):
        # note that psycopg2 is sometimes called psycopg2cffi
        # depending on platform
        psycopg2 = testing.db.dialect.dbapi
        TransactionRollbackError = __import__(
            "%s.extensions" % psycopg2.__name__
        ).extensions.TransactionRollbackError

        exception = exc.DBAPIError.instance(
            "some statement",
            {},
            TransactionRollbackError("foo"),
            psycopg2.Error,
        )
        assert isinstance(exception, exc.OperationalError)

    @testing.requires.no_coverage
    @testing.requires.psycopg2_compatibility
    def test_notice_logging(self):
        log = logging.getLogger("sqlalchemy.dialects.postgresql")
        buf = logging.handlers.BufferingHandler(100)
        lev = log.level
        log.addHandler(buf)
        log.setLevel(logging.INFO)
        try:
            conn = testing.db.connect()
            trans = conn.begin()
            try:
                conn.exec_driver_sql(
                    """
CREATE OR REPLACE FUNCTION note(message varchar) RETURNS integer AS $$
BEGIN
  RAISE NOTICE 'notice: %%', message;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""
                )
                conn.exec_driver_sql("SELECT note('hi there')")
                conn.exec_driver_sql("SELECT note('another note')")
            finally:
                trans.rollback()
        finally:
            log.removeHandler(buf)
            log.setLevel(lev)
        msgs = " ".join(b.msg for b in buf.buffer)
        eq_regex(
            msgs,
            "NOTICE:  notice: hi there(\nCONTEXT: .*?)? "
            "NOTICE:  notice: another note(\nCONTEXT: .*?)?",
        )

    @testing.requires.psycopg2_or_pg8000_compatibility
    @engines.close_open_connections
    def test_client_encoding(self):
        c = testing.db.connect()
        current_encoding = c.exec_driver_sql(
            "show client_encoding"
        ).fetchone()[0]
        c.close()

        # attempt to use an encoding that's not
        # already set
        if current_encoding == "UTF8":
            test_encoding = "LATIN1"
        else:
            test_encoding = "UTF8"

        e = engines.testing_engine(options={"client_encoding": test_encoding})
        c = e.connect()
        new_encoding = c.exec_driver_sql("show client_encoding").fetchone()[0]
        eq_(new_encoding, test_encoding)

    @testing.requires.psycopg2_or_pg8000_compatibility
    @engines.close_open_connections
    def test_autocommit_isolation_level(self):
        c = testing.db.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        )
        # If we're really in autocommit mode then we'll get an error saying
        # that the prepared transaction doesn't exist. Otherwise, we'd
        # get an error saying that the command can't be run within a
        # transaction.
        assert_raises_message(
            exc.ProgrammingError,
            'prepared transaction with identifier "gilberte" does not exist',
            c.exec_driver_sql,
            "commit prepared 'gilberte'",
        )

    def test_extract(self, connection):
        fivedaysago = testing.db.scalar(
            select(func.now().op("at time zone")("UTC"))
        ) - datetime.timedelta(days=5)

        for field, exp in (
            ("year", fivedaysago.year),
            ("month", fivedaysago.month),
            ("day", fivedaysago.day),
        ):
            r = connection.execute(
                select(
                    extract(
                        field,
                        func.now().op("at time zone")("UTC")
                        + datetime.timedelta(days=-5),
                    )
                )
            ).scalar()
            eq_(r, exp)

    @testing.provide_metadata
    def test_checksfor_sequence(self, connection):
        meta1 = self.metadata
        seq = Sequence("fooseq")
        t = Table("mytable", meta1, Column("col1", Integer, seq))
        seq.drop(connection)
        connection.execute(text("CREATE SEQUENCE fooseq"))
        t.create(connection, checkfirst=True)

    @testing.provide_metadata
    def test_schema_roundtrips(self, connection):
        meta = self.metadata
        users = Table(
            "users",
            meta,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            schema="test_schema",
        )
        users.create(connection)
        connection.execute(users.insert(), dict(id=1, name="name1"))
        connection.execute(users.insert(), dict(id=2, name="name2"))
        connection.execute(users.insert(), dict(id=3, name="name3"))
        connection.execute(users.insert(), dict(id=4, name="name4"))
        eq_(
            connection.execute(
                users.select().where(users.c.name == "name2")
            ).fetchall(),
            [(2, "name2")],
        )
        eq_(
            connection.execute(
                users.select()
                .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
                .where(users.c.name == "name2")
            ).fetchall(),
            [(2, "name2")],
        )
        connection.execute(users.delete().where(users.c.id == 3))
        eq_(
            connection.execute(
                users.select().where(users.c.name == "name3")
            ).fetchall(),
            [],
        )
        connection.execute(
            users.update().where(users.c.name == "name4"), dict(name="newname")
        )
        eq_(
            connection.execute(
                users.select()
                .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
                .where(users.c.id == 4)
            ).fetchall(),
            [(4, "newname")],
        )

    def test_quoted_name_bindparam_ok(self):
        from sqlalchemy.sql.elements import quoted_name

        with testing.db.connect() as conn:
            eq_(
                conn.scalar(
                    select(
                        cast(
                            literal(quoted_name("some_name", False)),
                            String,
                        )
                    )
                ),
                "some_name",
            )

    @testing.provide_metadata
    def test_preexecute_passivedefault(self, connection):
        """test that when we get a primary key column back from
        reflecting a table which has a default value on it, we pre-
        execute that DefaultClause upon insert."""

        meta = self.metadata
        connection.execute(
            text(
                """
                 CREATE TABLE speedy_users
                 (
                     speedy_user_id   SERIAL     PRIMARY KEY,
                     user_name        VARCHAR    NOT NULL,
                     user_password    VARCHAR    NOT NULL
                 );
                """
            )
        )
        t = Table("speedy_users", meta, autoload_with=connection)
        r = connection.execute(
            t.insert(), dict(user_name="user", user_password="lala")
        )
        eq_(r.inserted_primary_key, (1,))
        result = connection.execute(t.select()).fetchall()
        assert result == [(1, "user", "lala")]
        connection.execute(text("DROP TABLE speedy_users"))

    @testing.requires.psycopg2_or_pg8000_compatibility
    def test_numeric_raise(self, connection):
        stmt = text("select cast('hi' as char) as hi").columns(hi=Numeric)
        assert_raises(exc.InvalidRequestError, connection.execute, stmt)

    @testing.only_on("postgresql+psycopg2")
    def test_serial_integer(self):
        class BITD(TypeDecorator):
            impl = Integer

            def load_dialect_impl(self, dialect):
                if dialect.name == "postgresql":
                    return BigInteger()
                else:
                    return Integer()

        for version, type_, expected in [
            (None, Integer, "SERIAL"),
            (None, BigInteger, "BIGSERIAL"),
            ((9, 1), SmallInteger, "SMALLINT"),
            ((9, 2), SmallInteger, "SMALLSERIAL"),
            (None, postgresql.INTEGER, "SERIAL"),
            (None, postgresql.BIGINT, "BIGSERIAL"),
            (
                None,
                Integer().with_variant(BigInteger(), "postgresql"),
                "BIGSERIAL",
            ),
            (
                None,
                Integer().with_variant(postgresql.BIGINT, "postgresql"),
                "BIGSERIAL",
            ),
            (
                (9, 2),
                Integer().with_variant(SmallInteger, "postgresql"),
                "SMALLSERIAL",
            ),
            (None, BITD(), "BIGSERIAL"),
        ]:
            m = MetaData()

            t = Table("t", m, Column("c", type_, primary_key=True))

            if version:
                dialect = testing.db.dialect.__class__()
                dialect._get_server_version_info = mock.Mock(
                    return_value=version
                )
                dialect.initialize(testing.db.connect())
            else:
                dialect = testing.db.dialect

            ddl_compiler = dialect.ddl_compiler(dialect, schema.CreateTable(t))
            eq_(
                ddl_compiler.get_column_specification(t.c.c),
                "c %s NOT NULL" % expected,
            )

    @testing.requires.psycopg2_compatibility
    def test_initial_transaction_state(self):
        from psycopg2.extensions import STATUS_IN_TRANSACTION

        engine = engines.testing_engine()
        with engine.connect() as conn:
            ne_(conn.connection.status, STATUS_IN_TRANSACTION)


class AutocommitTextTest(test_deprecations.AutocommitTextTest):
    __only_on__ = "postgresql"

    def test_grant(self):
        self._test_keyword("GRANT USAGE ON SCHEMA fooschema TO foorole")

    def test_import_foreign_schema(self):
        self._test_keyword("IMPORT FOREIGN SCHEMA foob")

    def test_refresh_view(self):
        self._test_keyword("REFRESH MATERIALIZED VIEW fooview")

    def test_revoke(self):
        self._test_keyword("REVOKE USAGE ON SCHEMA fooschema FROM foorole")

    def test_truncate(self):
        self._test_keyword("TRUNCATE footable")
