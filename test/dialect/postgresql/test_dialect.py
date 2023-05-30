import dataclasses
import datetime
import logging
import logging.handlers

from sqlalchemy import BigInteger
from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import DDL
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
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
from sqlalchemy.dialects.postgresql import base as postgresql
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import psycopg as psycopg_dialect
from sqlalchemy.dialects.postgresql import psycopg2 as psycopg2_dialect
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.dialects.postgresql.psycopg2 import EXECUTEMANY_VALUES
from sqlalchemy.dialects.postgresql.psycopg2 import (
    EXECUTEMANY_VALUES_PLUS_BATCH,
)
from sqlalchemy.engine import url
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import config
from sqlalchemy.testing import engines
from sqlalchemy.testing import expect_raises_message
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
from sqlalchemy.testing.assertions import expect_raises
from sqlalchemy.testing.assertions import ne_


class DialectTest(fixtures.TestBase):
    """python-side dialect tests."""

    def test_range_constructor(self):
        """test kwonly argments in the range constructor, as we had
        to do dataclasses backwards compat operations"""

        r1 = Range(None, 5)
        eq_(dataclasses.astuple(r1), (None, 5, "[)", False))

        r1 = Range(10, 5, bounds="()")
        eq_(dataclasses.astuple(r1), (10, 5, "()", False))

        with expect_raises(TypeError):
            Range(10, 5, "()")  # type: ignore

        with expect_raises(TypeError):
            Range(None, None, "()", True)  # type: ignore

    def test_range_frozen(self):
        r1 = Range(None, 5)
        eq_(dataclasses.astuple(r1), (None, 5, "[)", False))

        with expect_raises(dataclasses.FrozenInstanceError):
            r1.lower = 8  # type: ignore

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

    @testing.only_on("postgresql")
    def test_ensure_version_is_qualified(
        self, future_connection, testing_engine, metadata
    ):
        default_schema_name = future_connection.dialect.default_schema_name
        event.listen(
            metadata,
            "after_create",
            DDL(
                """
CREATE OR REPLACE FUNCTION %s.version() RETURNS integer AS $$
BEGIN
    return 0;
END;
$$ LANGUAGE plpgsql;"""
                % (default_schema_name,)
            ),
        )
        event.listen(
            metadata,
            "before_drop",
            DDL("DROP FUNCTION %s.version" % (default_schema_name,)),
        )

        metadata.create_all(future_connection)
        future_connection.commit()

        e = testing_engine()

        @event.listens_for(e, "do_connect")
        def receive_do_connect(dialect, conn_rec, cargs, cparams):
            conn = dialect.dbapi.connect(*cargs, **cparams)
            cursor = conn.cursor()
            cursor.execute(
                "set search_path = %s,pg_catalog" % (default_schema_name,)
            )
            cursor.close()
            return conn

        with e.connect():
            pass
        eq_(
            e.dialect.server_version_info,
            future_connection.dialect.server_version_info,
        )

    def test_psycopg2_empty_connection_string(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql+psycopg2://")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [""])
        eq_(cparams, {})

    def test_psycopg2_nonempty_connection_string(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql+psycopg2://host")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, {"host": "host"})

    def test_psycopg2_empty_connection_string_w_query_one(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql+psycopg2:///?service=swh-log")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, {"service": "swh-log"})

    def test_psycopg2_empty_connection_string_w_query_two(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url("postgresql+psycopg2:///?any_random_thing=yes")
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, {"any_random_thing": "yes"})

    def test_psycopg2_nonempty_connection_string_w_query(self):
        dialect = psycopg2_dialect.dialect()
        u = url.make_url(
            "postgresql+psycopg2://somehost/?any_random_thing=yes"
        )
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, {"host": "somehost", "any_random_thing": "yes"})

    @testing.combinations(
        (
            "postgresql+psycopg2://USER:PASS@/DB?host=hostA",
            {
                "dbname": "DB",
                "user": "USER",
                "password": "PASS",
                "host": "hostA",
            },
        ),
        (
            "postgresql+psycopg2://USER:PASS@/DB"
            "?host=hostA&host=hostB&host=hostC",
            {
                "dbname": "DB",
                "user": "USER",
                "password": "PASS",
                "host": "hostA,hostB,hostC",
                "port": ",,",
            },
        ),
        (
            "postgresql+psycopg2://USER:PASS@/DB"
            "?host=hostA&host=hostB:portB&host=hostC:portC",
            {
                "dbname": "DB",
                "user": "USER",
                "password": "PASS",
                "host": "hostA,hostB,hostC",
                "port": ",portB,portC",
            },
        ),
        (
            "postgresql+psycopg2://USER:PASS@/DB?"
            "host=hostA:portA&host=hostB:portB&host=hostC:portC",
            {
                "dbname": "DB",
                "user": "USER",
                "password": "PASS",
                "host": "hostA,hostB,hostC",
                "port": "portA,portB,portC",
            },
        ),
        (
            "postgresql+psycopg2:///"
            "?host=hostA:portA&host=hostB:portB&host=hostC:portC",
            {"host": "hostA,hostB,hostC", "port": "portA,portB,portC"},
        ),
        (
            "postgresql+psycopg2:///"
            "?host=hostA:portA&host=hostB:portB&host=hostC:portC",
            {"host": "hostA,hostB,hostC", "port": "portA,portB,portC"},
        ),
        (
            "postgresql+psycopg2:///"
            "?host=hostA,hostB,hostC&port=portA,portB,portC",
            {"host": "hostA,hostB,hostC", "port": "portA,portB,portC"},
        ),
        argnames="url_string,expected",
    )
    @testing.combinations(
        psycopg2_dialect.dialect(),
        psycopg_dialect.dialect(),
        argnames="dialect",
    )
    def test_psycopg_multi_hosts(self, dialect, url_string, expected):
        u = url.make_url(url_string)
        cargs, cparams = dialect.create_connect_args(u)
        eq_(cargs, [])
        eq_(cparams, expected)

    @testing.combinations(
        "postgresql+psycopg2:///?host=H&host=H&port=5432,5432",
        "postgresql+psycopg2://user:pass@/dbname?host=H&host=H&port=5432,5432",
        argnames="url_string",
    )
    @testing.combinations(
        psycopg2_dialect.dialect(),
        psycopg_dialect.dialect(),
        argnames="dialect",
    )
    def test_psycopg_no_mix_hosts(self, dialect, url_string):
        with expect_raises_message(
            exc.ArgumentError, "Can't mix 'multihost' formats together"
        ):
            u = url.make_url(url_string)
            dialect.create_connect_args(u)

    def test_psycopg2_disconnect(self):
        class Error(Exception):
            pass

        dbapi = mock.Mock()
        dbapi.Error = Error

        dialect = psycopg2_dialect.dialect(dbapi=dbapi)

        for error in [
            # these error messages from libpq: interfaces/libpq/fe-misc.c
            # and interfaces/libpq/fe-secure.c.
            "terminating connection",
            "closed the connection",
            "connection not open",
            "could not receive data from server",
            "could not send data to server",
            # psycopg2 client errors, psycopg2/connection.h,
            # psycopg2/cursor.h
            "connection already closed",
            "cursor already closed",
            # not sure where this path is originally from, it may
            # be obsolete.   It really says "losed", not "closed".
            "losed the connection unexpectedly",
            # these can occur in newer SSL
            "connection has been closed unexpectedly",
            "SSL error: decryption failed or bad record mac",
            "SSL SYSCALL error: Bad file descriptor",
            "SSL SYSCALL error: EOF detected",
            "SSL SYSCALL error: Operation timed out",
            "SSL SYSCALL error: Bad address",
        ]:
            eq_(dialect.is_disconnect(Error(error), None, None), True)

        eq_(dialect.is_disconnect("not an error", None, None), False)


class BackendDialectTest(fixtures.TestBase):
    __backend__ = True

    @testing.only_on(["+psycopg", "+psycopg2"])
    @testing.combinations(
        "host=H:P&host=H:P&host=H:P",
        "host=H:P&host=H&host=H",
        "host=H:P&host=H&host=H:P",
        "host=H&host=H:P&host=H",
        "host=H,H,H&port=P,P,P",
    )
    def test_connect_psycopg_multiple_hosts(self, pattern):
        """test the fix for #4392"""

        tdb_url = testing.db.url

        host = tdb_url.host
        if host == "127.0.0.1":
            host = "localhost"
        port = str(tdb_url.port) if tdb_url.port else "5432"

        query_str = pattern.replace("H", host).replace("P", port)
        url_string = (
            f"{tdb_url.drivername}://{tdb_url.username}:"
            f"{tdb_url.password}@/{tdb_url.database}?{query_str}"
        )

        e = create_engine(url_string)
        with e.connect() as conn:
            eq_(conn.exec_driver_sql("select 1").scalar(), 1)


class PGCodeTest(fixtures.TestBase):
    __only_on__ = "postgresql"

    def test_error_code(self, metadata, connection):
        t = Table("t", metadata, Column("id", Integer, primary_key=True))
        t.create(connection)

        errmsg = assert_raises(
            exc.IntegrityError,
            connection.execute,
            t.insert(),
            [{"id": 1}, {"id": 1}],
        )

        if testing.against("postgresql+pg8000"):
            # TODO: is there another way we're supposed to see this?
            eq_(errmsg.orig.args[0]["C"], "23505")
        elif not testing.against("postgresql+psycopg"):
            eq_(errmsg.orig.pgcode, "23505")

        if testing.against("postgresql+asyncpg") or testing.against(
            "postgresql+psycopg"
        ):
            eq_(errmsg.orig.sqlstate, "23505")


class ExecuteManyMode:
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
            "Unitéble2",
            metadata,
            Column("méil", Integer, primary_key=True),
            Column("\u6e2c\u8a66", Integer),
        )

    def test_insert_unicode_keys(self, connection):
        table = self.tables["Unitéble2"]

        stmt = table.insert()

        connection.execute(
            stmt,
            [
                {"méil": 1, "\u6e2c\u8a66": 1},
                {"méil": 2, "\u6e2c\u8a66": 2},
                {"méil": 3, "\u6e2c\u8a66": 3},
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

        if (
            connection.dialect.executemany_mode
            is EXECUTEMANY_VALUES_PLUS_BATCH
        ):
            eq_(
                mock_exec.mock_calls,
                [
                    mock.call(
                        mock.ANY,
                        stmt,
                        [
                            {"xval": "x1", "yval": "y5"},
                            {"xval": "x3", "yval": "y6"},
                        ],
                        **expected_kwargs,
                    )
                ],
            )
        else:
            eq_(mock_exec.mock_calls, [])

    def test_not_sane_rowcount(self, connection):
        if (
            connection.dialect.executemany_mode
            is EXECUTEMANY_VALUES_PLUS_BATCH
        ):
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


class ExecutemanyValuesPlusBatchInsertsTest(
    ExecuteManyMode, fixtures.TablesTest
):
    options = {"executemany_mode": "values_plus_batch"}


class ExecutemanyValuesInsertsTest(ExecuteManyMode, fixtures.TablesTest):
    options = {"executemany_mode": "values_only"}


class ExecutemanyFlagOptionsTest(fixtures.TablesTest):
    __only_on__ = "postgresql+psycopg2"
    __backend__ = True

    def test_executemany_correct_flag_options(self):
        for opt, expected in [
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

    @testing.only_on("postgresql+psycopg")
    def test_psycopg_version(self):
        v = testing.db.dialect.psycopg_version
        assert testing.db.dialect.dbapi.__version__.startswith(
            ".".join(str(x) for x in v)
        )

    @testing.combinations(
        (True, False),
        (False, True),
    )
    def test_backslash_escapes_detection(self, explicit_setting, expected):
        engine = engines.testing_engine()

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
            dbapi_conn = conn.connection.dbapi_connection

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

    @testing.combinations((True,), (False,), argnames="pre_ping")
    def test_readonly_flag_connection(self, testing_engine, pre_ping):
        if pre_ping:
            engine = testing_engine(options={"pool_pre_ping": True})
        else:
            engine = testing_engine()

        for i in range(2):
            with engine.connect() as conn:
                # asyncpg requires serializable for readonly..
                conn = conn.execution_options(
                    isolation_level="SERIALIZABLE", postgresql_readonly=True
                )

                conn.execute(text("select 1")).scalar()

                dbapi_conn = conn.connection.dbapi_connection

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

    @testing.combinations((True,), (False,), argnames="pre_ping")
    def test_deferrable_flag_connection(self, testing_engine, pre_ping):
        if pre_ping:
            engine = testing_engine(options={"pool_pre_ping": True})
        else:
            engine = testing_engine()

        for i in range(2):
            with engine.connect() as conn:
                # asyncpg but not for deferrable?  which the PG docs actually
                # state.  weird
                conn = conn.execution_options(
                    isolation_level="SERIALIZABLE", postgresql_deferrable=True
                )

                conn.execute(text("Select 1")).scalar()

                dbapi_conn = conn.connection.dbapi_connection

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

    @testing.combinations((True,), (False,), argnames="pre_ping")
    def test_readonly_flag_engine(self, testing_engine, pre_ping):
        engine = testing_engine(
            options={
                "execution_options": dict(
                    isolation_level="SERIALIZABLE", postgresql_readonly=True
                ),
                "pool_pre_ping": pre_ping,
            }
        )
        for i in range(2):
            with engine.connect() as conn:
                conn.execute(text("select 1")).scalar()

                dbapi_conn = conn.connection.dbapi_connection

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

    @testing.combinations((True,), (False,), argnames="autocommit")
    def test_autocommit_pre_ping(self, testing_engine, autocommit):
        engine = testing_engine(
            options={
                "isolation_level": "AUTOCOMMIT"
                if autocommit
                else "SERIALIZABLE",
                "pool_pre_ping": True,
            }
        )
        for i in range(4):
            with engine.connect() as conn:
                conn.execute(text("select 1")).scalar()

                dbapi_conn = conn.connection.dbapi_connection
                eq_(dbapi_conn.autocommit, autocommit)

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
                dbapi_conn = conn.connection.dbapi_connection

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

    @testing.requires.any_psycopg_compatibility
    def test_psycopg_non_standard_err(self):
        # note that psycopg2 is sometimes called psycopg2cffi
        # depending on platform
        psycopg = testing.db.dialect.dbapi
        if psycopg.__version__.startswith("3"):
            TransactionRollbackError = __import__(
                "%s.errors" % psycopg.__name__
            ).errors.TransactionRollback
        else:
            TransactionRollbackError = __import__(
                "%s.extensions" % psycopg.__name__
            ).extensions.TransactionRollbackError

        exception = exc.DBAPIError.instance(
            "some statement",
            {},
            TransactionRollbackError("foo"),
            psycopg.Error,
        )
        assert isinstance(exception, exc.OperationalError)

    @testing.requires.no_coverage
    @testing.requires.any_psycopg_compatibility
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
        msgs = " ".join(b.getMessage() for b in buf.buffer)
        eq_regex(
            msgs,
            "NOTICE: [ ]?notice: hi there(\nCONTEXT: .*?)? "
            "NOTICE: [ ]?notice: another note(\nCONTEXT: .*?)?",
        )

    @testing.requires.psycopg_or_pg8000_compatibility
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

    @testing.requires.psycopg_or_pg8000_compatibility
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
        fivedaysago = connection.execute(
            select(func.now().op("at time zone")("UTC"))
        ).scalar() - datetime.timedelta(days=5)

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

    @testing.combinations(True, False, argnames="implicit_returning")
    def test_sequence_detection_tricky_names(
        self, metadata, connection, implicit_returning
    ):
        for tname, cname in [
            ("tb1" * 30, "abc"),
            ("tb2", "abc" * 30),
            ("tb3" * 30, "abc" * 30),
            ("tb4", "abc"),
        ]:
            t = Table(
                tname[:57],
                metadata,
                Column(cname[:57], Integer, primary_key=True),
                implicit_returning=implicit_returning,
            )
            t.create(connection)
            r = connection.execute(t.insert())
            eq_(r.inserted_primary_key, (1,))

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

    @testing.requires.psycopg_or_pg8000_compatibility
    def test_numeric_raise(self, connection):
        stmt = text("select cast('hi' as char) as hi").columns(hi=Numeric)
        assert_raises(exc.InvalidRequestError, connection.execute, stmt)

    @testing.only_on("postgresql+psycopg2")
    def test_serial_integer(self):
        class BITD(TypeDecorator):
            impl = Integer

            cache_ok = True

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
    def test_initial_transaction_state_psycopg2(self):
        from psycopg2.extensions import STATUS_IN_TRANSACTION

        engine = engines.testing_engine()
        with engine.connect() as conn:
            ne_(conn.connection.status, STATUS_IN_TRANSACTION)

    @testing.only_on("postgresql+psycopg")
    def test_initial_transaction_state_psycopg(self):
        from psycopg.pq import TransactionStatus

        engine = engines.testing_engine()
        with engine.connect() as conn:
            ne_(
                conn.connection.dbapi_connection.info.transaction_status,
                TransactionStatus.INTRANS,
            )

    def test_select_rowcount(self):
        conn = testing.db.connect()
        cursor = conn.exec_driver_sql("SELECT 1")
        eq_(cursor.rowcount, 1)


class Psycopg3Test(fixtures.TestBase):
    __only_on__ = ("postgresql+psycopg",)

    def test_json_correctly_registered(self, testing_engine):
        import json

        def loads(value):
            value = json.loads(value)
            value["x"] = value["x"] + "_loads"
            return value

        def dumps(value):
            value = dict(value)
            value["x"] = "dumps_y"
            return json.dumps(value)

        engine = testing_engine(
            options=dict(json_serializer=dumps, json_deserializer=loads)
        )
        engine2 = testing_engine(
            options=dict(
                json_serializer=json.dumps, json_deserializer=json.loads
            )
        )

        s = select(cast({"key": "value", "x": "q"}, JSONB))
        with engine.begin() as conn:
            eq_(conn.scalar(s), {"key": "value", "x": "dumps_y_loads"})
            with engine.begin() as conn:
                eq_(conn.scalar(s), {"key": "value", "x": "dumps_y_loads"})
                with engine2.begin() as conn:
                    eq_(conn.scalar(s), {"key": "value", "x": "q"})
                with engine.begin() as conn:
                    eq_(conn.scalar(s), {"key": "value", "x": "dumps_y_loads"})

    @testing.requires.hstore
    def test_hstore_correctly_registered(self, testing_engine):
        engine = testing_engine(options=dict(use_native_hstore=True))
        engine2 = testing_engine(options=dict(use_native_hstore=False))

        def rp(self, *a):
            return lambda a: {"a": "b"}

        with mock.patch.object(HSTORE, "result_processor", side_effect=rp):
            s = select(cast({"key": "value", "x": "q"}, HSTORE))
            with engine.begin() as conn:
                eq_(conn.scalar(s), {"key": "value", "x": "q"})
                with engine.begin() as conn:
                    eq_(conn.scalar(s), {"key": "value", "x": "q"})
                    with engine2.begin() as conn:
                        eq_(conn.scalar(s), {"a": "b"})
                    with engine.begin() as conn:
                        eq_(conn.scalar(s), {"key": "value", "x": "q"})

    def test_get_dialect(self):
        u = url.URL.create("postgresql://")
        d = psycopg_dialect.PGDialect_psycopg.get_dialect_cls(u)
        is_(d, psycopg_dialect.PGDialect_psycopg)
        d = psycopg_dialect.PGDialect_psycopg.get_async_dialect_cls(u)
        is_(d, psycopg_dialect.PGDialectAsync_psycopg)
        d = psycopg_dialect.PGDialectAsync_psycopg.get_dialect_cls(u)
        is_(d, psycopg_dialect.PGDialectAsync_psycopg)
        d = psycopg_dialect.PGDialectAsync_psycopg.get_dialect_cls(u)
        is_(d, psycopg_dialect.PGDialectAsync_psycopg)

    def test_async_version(self):
        e = create_engine("postgresql+psycopg_async://")
        is_true(isinstance(e.dialect, psycopg_dialect.PGDialectAsync_psycopg))
