import datetime

from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import Time
from sqlalchemy import true
from sqlalchemy import tuple_
from sqlalchemy import Uuid
from sqlalchemy import values
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import REGCONFIG
from sqlalchemy.sql.expression import type_coerce
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertsql import CursorSQL
from sqlalchemy.testing.assertsql import DialectSQL


class FunctionTypingTest(fixtures.TestBase, AssertsExecutionResults):
    __only_on__ = "postgresql"
    __sparse_driver_backend__ = True

    def test_count_star(self, connection):
        eq_(connection.scalar(func.count("*")), 1)

    def test_count_int(self, connection):
        eq_(connection.scalar(func.count(1)), 1)


class InsertTest(fixtures.TestBase, AssertsExecutionResults):
    __only_on__ = "postgresql"
    __backend__ = True

    @testing.combinations((False,), (True,), argnames="implicit_returning")
    def test_foreignkey_missing_insert(
        self, metadata, connection, implicit_returning
    ):
        Table(
            "t1",
            metadata,
            Column("id", Integer, primary_key=True),
            implicit_returning=implicit_returning,
        )
        t2 = Table(
            "t2",
            metadata,
            Column("id", Integer, ForeignKey("t1.id"), primary_key=True),
            implicit_returning=implicit_returning,
        )

        metadata.create_all(connection)

        # want to ensure that "null value in column "id" violates not-
        # null constraint" is raised (IntegrityError on psycoopg2, but
        # ProgrammingError on pg8000), and not "ProgrammingError:
        # (ProgrammingError) relationship "t2_id_seq" does not exist".
        # the latter corresponds to autoincrement behavior, which is not
        # the case here due to the foreign key.

        with expect_warnings(".*has no Python-side or server-side default.*"):
            assert_raises(
                (exc.IntegrityError, exc.ProgrammingError),
                connection.execute,
                t2.insert(),
            )

    @testing.combinations(True, False, argnames="implicit_returning")
    def test_sequence_insert(self, metadata, connection, implicit_returning):
        table = Table(
            "testtable",
            metadata,
            Column("id", Integer, Sequence("my_seq"), primary_key=True),
            Column("data", String(30)),
            implicit_returning=implicit_returning,
        )
        metadata.create_all(connection)
        if implicit_returning:
            self._assert_data_with_sequence_returning(
                connection, table, "my_seq"
            )
        else:
            self._assert_data_with_sequence(connection, table, "my_seq")

    @testing.combinations(True, False, argnames="implicit_returning")
    def test_opt_sequence_insert(
        self, metadata, connection, implicit_returning
    ):
        table = Table(
            "testtable",
            metadata,
            Column(
                "id",
                Integer,
                Sequence("my_seq", optional=True),
                primary_key=True,
            ),
            Column("data", String(30)),
            implicit_returning=implicit_returning,
        )
        metadata.create_all(connection)
        if implicit_returning:
            self._assert_data_autoincrement_returning(connection, table)
        else:
            self._assert_data_autoincrement(connection, table)

    @testing.combinations(True, False, argnames="implicit_returning")
    def test_autoincrement_insert(
        self, metadata, connection, implicit_returning
    ):
        table = Table(
            "testtable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30)),
            implicit_returning=implicit_returning,
        )
        metadata.create_all(connection)
        if implicit_returning:
            self._assert_data_autoincrement_returning(connection, table)
        else:
            self._assert_data_autoincrement(connection, table)

    @testing.combinations(True, False, argnames="implicit_returning")
    def test_noautoincrement_insert(
        self, metadata, connection, implicit_returning
    ):
        table = Table(
            "testtable",
            metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(30)),
            implicit_returning=implicit_returning,
        )
        metadata.create_all(connection)
        self._assert_data_noautoincrement(connection, table)

    def test_full_cursor_insertmanyvalues_sql(self, metadata, connection):
        """test compilation/ execution of the subquery form including
        the fix for #13015

        The specific form in question for #13015 is only supported by the
        PostgreSQL dialect right now.   MSSQL would also use it for a server
        side function that produces monotonic values, but we have no support
        for that outside of sequence next right now, where SQL Server doesn't
        support invokving the sequence outside of the VALUES tuples.

        """

        my_table = Table(
            "my_table",
            metadata,
            Column("data1", String(50)),
            Column(
                "id",
                Integer,
                Sequence("foo_id_seq", start=1, data_type=Integer),
                primary_key=True,
            ),
            Column("data2", String(50)),
        )

        my_table.create(connection)
        with self.sql_execution_asserter(connection) as assert_:
            connection.execute(
                my_table.insert().returning(
                    my_table.c.data1,
                    my_table.c.id,
                    sort_by_parameter_order=True,
                ),
                [
                    {"data1": f"d1 row {i}", "data2": f"d2 row {i}"}
                    for i in range(10)
                ],
            )

        render_bind_casts = (
            String().dialect_impl(connection.dialect).render_bind_cast
        )

        if render_bind_casts:
            varchar_cast = "::VARCHAR"
        else:
            varchar_cast = ""

        if connection.dialect.paramstyle == "pyformat":
            params = ", ".join(
                f"(%(data1__{i})s{varchar_cast}, %(data2__{i})s"
                f"{varchar_cast}, {i})"
                for i in range(0, 10)
            )
            parameters = {}
            for i in range(10):
                parameters[f"data1__{i}"] = f"d1 row {i}"
                parameters[f"data2__{i}"] = f"d2 row {i}"

        elif connection.dialect.paramstyle == "numeric_dollar":
            params = ", ".join(
                f"(${i * 2 + 1}{varchar_cast}, "
                f"${i * 2 + 2}{varchar_cast}, {i})"
                for i in range(0, 10)
            )
            parameters = ()
            for i in range(10):
                parameters += (f"d1 row {i}", f"d2 row {i}")
        elif connection.dialect.paramstyle == "format":
            params = ", ".join(
                f"(%s{varchar_cast}, %s{varchar_cast}, {i})"
                for i in range(0, 10)
            )
            parameters = ()
            for i in range(10):
                parameters += (f"d1 row {i}", f"d2 row {i}")
        elif connection.dialect.paramstyle == "qmark":
            params = ", ".join(
                f"(?{varchar_cast}, ?{varchar_cast}, {i})"
                for i in range(0, 10)
            )
            parameters = ()
            for i in range(10):
                parameters += (f"d1 row {i}", f"d2 row {i}")
        else:
            assert False

        assert_.assert_(
            CursorSQL(
                "INSERT INTO my_table (data1, id, data2) "
                f"SELECT p0::VARCHAR, nextval('foo_id_seq'), p2::VARCHAR "
                f"FROM (VALUES {params}) "
                "AS imp_sen(p0, p2, sen_counter) ORDER BY sen_counter "
                "RETURNING my_table.data1, my_table.id, my_table.id AS id__1",
                parameters,
            )
        )

    def _ints_and_strs_setinputsizes(self, connection):
        return (
            connection.dialect._bind_typing_render_casts
            and String().dialect_impl(connection.dialect).render_bind_cast
        )

    def _assert_data_autoincrement(self, connection, table):
        """
        invoked by:
        * test_opt_sequence_insert
        * test_autoincrement_insert
        """

        with self.sql_execution_asserter(connection) as asserter:
            conn = connection
            # execute with explicit id

            r = conn.execute(table.insert(), {"id": 30, "data": "d1"})
            eq_(r.inserted_primary_key, (30,))

            # execute with prefetch id

            r = conn.execute(table.insert(), {"data": "d2"})
            eq_(r.inserted_primary_key, (1,))

            # executemany with explicit ids

            conn.execute(
                table.insert(),
                [
                    {"id": 31, "data": "d3"},
                    {"id": 32, "data": "d4"},
                ],
            )

            # executemany, uses SERIAL

            conn.execute(table.insert(), [{"data": "d5"}, {"data": "d6"}])

            # single execute, explicit id, inline

            conn.execute(table.insert().inline(), {"id": 33, "data": "d7"})

            # single execute, inline, uses SERIAL

            conn.execute(table.insert().inline(), {"data": "d8"})

        if self._ints_and_strs_setinputsizes(connection):
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 1, "data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data::VARCHAR)",
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data::VARCHAR)",
                    [{"data": "d8"}],
                ),
            )
        else:
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 1, "data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data)",
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data)",
                    [{"data": "d8"}],
                ),
            )

        eq_(
            conn.execute(table.select()).fetchall(),
            [
                (30, "d1"),
                (1, "d2"),
                (31, "d3"),
                (32, "d4"),
                (2, "d5"),
                (3, "d6"),
                (33, "d7"),
                (4, "d8"),
            ],
        )

        conn.execute(table.delete())

        # test the same series of events using a reflected version of
        # the table

        m2 = MetaData()
        table = Table(
            table.name, m2, autoload_with=connection, implicit_returning=False
        )

        with self.sql_execution_asserter(connection) as asserter:
            conn.execute(table.insert(), {"id": 30, "data": "d1"})
            r = conn.execute(table.insert(), {"data": "d2"})
            eq_(r.inserted_primary_key, (5,))
            conn.execute(
                table.insert(),
                [
                    {"id": 31, "data": "d3"},
                    {"id": 32, "data": "d4"},
                ],
            )
            conn.execute(table.insert(), [{"data": "d5"}, {"data": "d6"}])
            conn.execute(table.insert().inline(), {"id": 33, "data": "d7"})
            conn.execute(table.insert().inline(), {"data": "d8"})

        if self._ints_and_strs_setinputsizes(connection):
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 5, "data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data::VARCHAR)",
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data::VARCHAR)",
                    [{"data": "d8"}],
                ),
            )
        else:
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 5, "data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data)",
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data)",
                    [{"data": "d8"}],
                ),
            )

        eq_(
            conn.execute(table.select()).fetchall(),
            [
                (30, "d1"),
                (5, "d2"),
                (31, "d3"),
                (32, "d4"),
                (6, "d5"),
                (7, "d6"),
                (33, "d7"),
                (8, "d8"),
            ],
        )

    def _assert_data_autoincrement_returning(self, connection, table):
        """
        invoked by:
        * test_opt_sequence_returning_insert
        * test_autoincrement_returning_insert
        """
        with self.sql_execution_asserter(connection) as asserter:
            conn = connection

            # execute with explicit id

            r = conn.execute(table.insert(), {"id": 30, "data": "d1"})
            eq_(r.inserted_primary_key, (30,))

            # execute with prefetch id

            r = conn.execute(table.insert(), {"data": "d2"})
            eq_(r.inserted_primary_key, (1,))

            # executemany with explicit ids

            conn.execute(
                table.insert(),
                [
                    {"id": 31, "data": "d3"},
                    {"id": 32, "data": "d4"},
                ],
            )

            # executemany, uses SERIAL

            conn.execute(table.insert(), [{"data": "d5"}, {"data": "d6"}])

            # single execute, explicit id, inline

            conn.execute(table.insert().inline(), {"id": 33, "data": "d7"})

            # single execute, inline, uses SERIAL

            conn.execute(table.insert().inline(), {"data": "d8"})

        if self._ints_and_strs_setinputsizes(connection):
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES "
                    "(:data::VARCHAR) RETURNING "
                    "testtable.id",
                    {"data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data::VARCHAR)",
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data::VARCHAR)",
                    [{"data": "d8"}],
                ),
            )
        else:
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data) RETURNING "
                    "testtable.id",
                    {"data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data)",
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data)",
                    [{"data": "d8"}],
                ),
            )

        eq_(
            conn.execute(table.select()).fetchall(),
            [
                (30, "d1"),
                (1, "d2"),
                (31, "d3"),
                (32, "d4"),
                (2, "d5"),
                (3, "d6"),
                (33, "d7"),
                (4, "d8"),
            ],
        )
        conn.execute(table.delete())

        # test the same series of events using a reflected version of
        # the table

        m2 = MetaData()
        table = Table(
            table.name,
            m2,
            autoload_with=connection,
            implicit_returning=True,
        )

        with self.sql_execution_asserter(connection) as asserter:
            conn.execute(table.insert(), {"id": 30, "data": "d1"})
            r = conn.execute(table.insert(), {"data": "d2"})
            eq_(r.inserted_primary_key, (5,))
            conn.execute(
                table.insert(),
                [
                    {"id": 31, "data": "d3"},
                    {"id": 32, "data": "d4"},
                ],
            )
            conn.execute(table.insert(), [{"data": "d5"}, {"data": "d6"}])
            conn.execute(table.insert().inline(), {"id": 33, "data": "d7"})
            conn.execute(table.insert().inline(), {"data": "d8"})

        if self._ints_and_strs_setinputsizes(connection):
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES "
                    "(:data::VARCHAR) RETURNING "
                    "testtable.id",
                    {"data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data::VARCHAR)",
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data::VARCHAR)",
                    [{"data": "d8"}],
                ),
            )
        else:
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data) RETURNING "
                    "testtable.id",
                    {"data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data)",
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (data) VALUES (:data)",
                    [{"data": "d8"}],
                ),
            )

        eq_(
            conn.execute(table.select()).fetchall(),
            [
                (30, "d1"),
                (5, "d2"),
                (31, "d3"),
                (32, "d4"),
                (6, "d5"),
                (7, "d6"),
                (33, "d7"),
                (8, "d8"),
            ],
        )

    def _assert_data_with_sequence(self, connection, table, seqname):
        """
        invoked by:
        * test_sequence_insert
        """

        with self.sql_execution_asserter(connection) as asserter:
            conn = connection
            conn.execute(table.insert(), {"id": 30, "data": "d1"})
            conn.execute(table.insert(), {"data": "d2"})
            conn.execute(
                table.insert(),
                [
                    {"id": 31, "data": "d3"},
                    {"id": 32, "data": "d4"},
                ],
            )
            conn.execute(table.insert(), [{"data": "d5"}, {"data": "d6"}])
            conn.execute(table.insert().inline(), {"id": 33, "data": "d7"})
            conn.execute(table.insert().inline(), {"data": "d8"})

        if self._ints_and_strs_setinputsizes(connection):
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 30, "data": "d1"},
                ),
                CursorSQL("select nextval('my_seq')", consume_statement=False),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 1, "data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                    ":data::VARCHAR)" % seqname,
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                    ":data::VARCHAR)" % seqname,
                    [{"data": "d8"}],
                ),
            )
        else:
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 30, "data": "d1"},
                ),
                CursorSQL("select nextval('my_seq')", consume_statement=False),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 1, "data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                    ":data)" % seqname,
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                    ":data)" % seqname,
                    [{"data": "d8"}],
                ),
            )
        eq_(
            conn.execute(table.select()).fetchall(),
            [
                (30, "d1"),
                (1, "d2"),
                (31, "d3"),
                (32, "d4"),
                (2, "d5"),
                (3, "d6"),
                (33, "d7"),
                (4, "d8"),
            ],
        )

    def _assert_data_with_sequence_returning(self, connection, table, seqname):
        """
        invoked by:
        * test_sequence_returning_insert
        """

        with self.sql_execution_asserter(connection) as asserter:
            conn = connection
            conn.execute(table.insert(), {"id": 30, "data": "d1"})
            conn.execute(table.insert(), {"data": "d2"})
            conn.execute(
                table.insert(),
                [
                    {"id": 31, "data": "d3"},
                    {"id": 32, "data": "d4"},
                ],
            )
            conn.execute(table.insert(), [{"data": "d5"}, {"data": "d6"}])
            conn.execute(table.insert().inline(), {"id": 33, "data": "d7"})
            conn.execute(table.insert().inline(), {"data": "d8"})

        if self._ints_and_strs_setinputsizes(connection):
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(nextval('my_seq'), :data::VARCHAR) "
                    "RETURNING testtable.id",
                    {"data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                    ":data::VARCHAR)" % seqname,
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(:id::INTEGER, :data::VARCHAR)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                    ":data::VARCHAR)" % seqname,
                    [{"data": "d8"}],
                ),
            )
        else:
            asserter.assert_(
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    {"id": 30, "data": "d1"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES "
                    "(nextval('my_seq'), :data) RETURNING testtable.id",
                    {"data": "d2"},
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 31, "data": "d3"}, {"id": 32, "data": "d4"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                    ":data)" % seqname,
                    [{"data": "d5"}, {"data": "d6"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (:id, :data)",
                    [{"id": 33, "data": "d7"}],
                ),
                DialectSQL(
                    "INSERT INTO testtable (id, data) VALUES (nextval('%s'), "
                    ":data)" % seqname,
                    [{"data": "d8"}],
                ),
            )

        eq_(
            connection.execute(table.select()).fetchall(),
            [
                (30, "d1"),
                (1, "d2"),
                (31, "d3"),
                (32, "d4"),
                (2, "d5"),
                (3, "d6"),
                (33, "d7"),
                (4, "d8"),
            ],
        )

    def _assert_data_noautoincrement(self, connection, table):
        """
        invoked by:
        * test_noautoincrement_insert
        """

        # turning off the cache because we are checking for compile-time
        # warnings
        connection.execution_options(compiled_cache=None)

        conn = connection
        conn.execute(table.insert(), {"id": 30, "data": "d1"})

        with conn.begin_nested() as nested:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    {"data": "d2"},
                )
            nested.rollback()

        with conn.begin_nested() as nested:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    [{"data": "d2"}, {"data": "d3"}],
                )
            nested.rollback()

        with conn.begin_nested() as nested:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    {"data": "d2"},
                )
            nested.rollback()

        with conn.begin_nested() as nested:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    [{"data": "d2"}, {"data": "d3"}],
                )
            nested.rollback()

        conn.execute(
            table.insert(),
            [{"id": 31, "data": "d2"}, {"id": 32, "data": "d3"}],
        )
        conn.execute(table.insert().inline(), {"id": 33, "data": "d4"})
        eq_(
            conn.execute(table.select()).fetchall(),
            [(30, "d1"), (31, "d2"), (32, "d3"), (33, "d4")],
        )
        conn.execute(table.delete())

        # test the same series of events using a reflected version of
        # the table

        m2 = MetaData()
        table = Table(table.name, m2, autoload_with=connection)
        conn = connection

        conn.execute(table.insert(), {"id": 30, "data": "d1"})

        with conn.begin_nested() as nested:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    {"data": "d2"},
                )
            nested.rollback()

        with conn.begin_nested() as nested:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    [{"data": "d2"}, {"data": "d3"}],
                )
            nested.rollback()

        conn.execute(
            table.insert(),
            [{"id": 31, "data": "d2"}, {"id": 32, "data": "d3"}],
        )
        conn.execute(table.insert().inline(), {"id": 33, "data": "d4"})
        eq_(
            conn.execute(table.select()).fetchall(),
            [(30, "d1"), (31, "d2"), (32, "d3"), (33, "d4")],
        )


class MatchTest(fixtures.TablesTest, AssertsCompiledSQL):
    __only_on__ = "postgresql >= 8.3"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "cattable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("description", String(50)),
        )
        Table(
            "matchtable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("title", String(200)),
            Column("category_id", Integer, ForeignKey("cattable.id")),
        )

    @classmethod
    def insert_data(cls, connection):
        cattable, matchtable = cls.tables("cattable", "matchtable")

        connection.execute(
            cattable.insert(),
            [
                {"id": 1, "description": "Python"},
                {"id": 2, "description": "Ruby"},
            ],
        )
        connection.execute(
            matchtable.insert(),
            [
                {
                    "id": 1,
                    "title": "Agile Web Development with Rails",
                    "category_id": 2,
                },
                {"id": 2, "title": "Dive Into Python", "category_id": 1},
                {
                    "id": 3,
                    "title": "Programming Matz's Ruby",
                    "category_id": 2,
                },
                {
                    "id": 4,
                    "title": "The Definitive Guide to Django",
                    "category_id": 1,
                },
                {"id": 5, "title": "Python in a Nutshell", "category_id": 1},
            ],
        )

    def _strs_render_bind_casts(self, connection):
        return (
            connection.dialect._bind_typing_render_casts
            and String().dialect_impl(connection.dialect).render_bind_cast
        )

    @testing.requires.pyformat_paramstyle
    def test_expression_pyformat(self, connection):
        matchtable = self.tables.matchtable

        if self._strs_render_bind_casts(connection):
            self.assert_compile(
                matchtable.c.title.match("somstr"),
                "matchtable.title @@ plainto_tsquery(%(title_1)s::VARCHAR)",
            )
        else:
            self.assert_compile(
                matchtable.c.title.match("somstr"),
                "matchtable.title @@ plainto_tsquery(%(title_1)s)",
            )

    @testing.only_if("+asyncpg")
    def test_expression_positional(self, connection):
        matchtable = self.tables.matchtable

        if self._strs_render_bind_casts(connection):
            self.assert_compile(
                matchtable.c.title.match("somstr"),
                "matchtable.title @@ plainto_tsquery($1::VARCHAR)",
            )
        else:
            self.assert_compile(
                matchtable.c.title.match("somstr"),
                "matchtable.title @@ plainto_tsquery($1)",
            )

    @testing.combinations(
        (func.to_tsvector,),
        (func.to_tsquery,),
        (func.plainto_tsquery,),
        (func.phraseto_tsquery,),
        (func.websearch_to_tsquery, testing.skip_if("postgresql < 11")),
        argnames="to_ts_func",
    )
    @testing.variation("use_regconfig", [True, False, "literal"])
    def test_to_regconfig_fns(self, connection, to_ts_func, use_regconfig):
        """test #8977"""

        matchtable = self.tables.matchtable

        fn_name = to_ts_func().name

        if use_regconfig.literal:
            regconfig = literal("english", REGCONFIG)
        elif use_regconfig:
            regconfig = "english"
        else:
            regconfig = None

        if regconfig is None:
            if fn_name == "to_tsvector":
                fn = to_ts_func(matchtable.c.title).match("python")
            else:
                fn = func.to_tsvector(matchtable.c.title).op("@@")(
                    to_ts_func("python")
                )
        else:
            if fn_name == "to_tsvector":
                fn = to_ts_func(regconfig, matchtable.c.title).match("python")
            else:
                fn = func.to_tsvector(matchtable.c.title).op("@@")(
                    to_ts_func(regconfig, "python")
                )

        stmt = matchtable.select().where(fn).order_by(matchtable.c.id)
        results = connection.execute(stmt).fetchall()
        eq_([2, 5], [r.id for r in results])

    @testing.variation("use_regconfig", [True, False, "literal"])
    @testing.variation("include_options", [True, False])
    def test_ts_headline(self, connection, use_regconfig, include_options):
        """test #8977"""
        if use_regconfig.literal:
            regconfig = literal("english", REGCONFIG)
        elif use_regconfig:
            regconfig = "english"
        else:
            regconfig = None

        text = (
            "The most common type of search is to find all documents "
            "containing given query terms and return them in order of "
            "their similarity to the query."
        )
        tsquery = func.to_tsquery("english", "query & similarity")

        if regconfig is None:
            if include_options:
                fn = func.ts_headline(
                    text,
                    tsquery,
                    "MaxFragments=10, MaxWords=7, MinWords=3, "
                    "StartSel=<<, StopSel=>>",
                )
            else:
                fn = func.ts_headline(
                    text,
                    tsquery,
                )
        else:
            if include_options:
                fn = func.ts_headline(
                    regconfig,
                    text,
                    tsquery,
                    "MaxFragments=10, MaxWords=7, MinWords=3, "
                    "StartSel=<<, StopSel=>>",
                )
            else:
                fn = func.ts_headline(
                    regconfig,
                    text,
                    tsquery,
                )

        stmt = select(fn)

        if include_options:
            eq_(
                connection.scalar(stmt),
                "documents containing given <<query>> terms and return ... "
                "their <<similarity>> to the <<query>>",
            )
        else:
            eq_(
                connection.scalar(stmt),
                "containing given <b>query</b> terms and return them in "
                "order of their <b>similarity</b> to the <b>query</b>.",
            )

    def test_simple_match(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("python"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_not_match(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select()
            .where(~matchtable.c.title.match("python"))
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([1, 3, 4], [r.id for r in results])

    def test_simple_match_with_apostrophe(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select().where(matchtable.c.title.match("Matz's"))
        ).fetchall()
        eq_([3], [r.id for r in results])

    def test_simple_derivative_match(self, connection):
        matchtable = self.tables.matchtable
        results = connection.execute(
            matchtable.select().where(matchtable.c.title.match("nutshells"))
        ).fetchall()
        eq_([5], [r.id for r in results])

    def test_or_match(self, connection):
        matchtable = self.tables.matchtable
        results1 = connection.execute(
            matchtable.select()
            .where(
                or_(
                    matchtable.c.title.match("nutshells"),
                    matchtable.c.title.match("rubies"),
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([3, 5], [r.id for r in results1])

    def test_or_tsquery(self, connection):
        matchtable = self.tables.matchtable
        results2 = connection.execute(
            matchtable.select()
            .where(
                matchtable.c.title.bool_op("@@")(
                    func.to_tsquery("nutshells | rubies")
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([3, 5], [r.id for r in results2])

    def test_and_match(self, connection):
        matchtable = self.tables.matchtable
        results1 = connection.execute(
            matchtable.select().where(
                and_(
                    matchtable.c.title.match("python"),
                    matchtable.c.title.match("nutshells"),
                )
            )
        ).fetchall()
        eq_([5], [r.id for r in results1])

    def test_and_tsquery(self, connection):
        matchtable = self.tables.matchtable
        results2 = connection.execute(
            matchtable.select().where(
                matchtable.c.title.op("@@")(
                    func.to_tsquery("python & nutshells")
                )
            )
        ).fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self, connection):
        cattable, matchtable = self.tables("cattable", "matchtable")
        results = connection.execute(
            matchtable.select()
            .where(
                and_(
                    cattable.c.id == matchtable.c.category_id,
                    or_(
                        cattable.c.description.match("Ruby"),
                        matchtable.c.title.match("nutshells"),
                    ),
                )
            )
            .order_by(matchtable.c.id)
        ).fetchall()
        eq_([1, 3, 5], [r.id for r in results])


class TupleTest(fixtures.TestBase):
    __only_on__ = "postgresql"
    __backend__ = True

    def test_tuple_containment(self, connection):
        for test, exp in [
            ([("a", "b")], True),
            ([("a", "c")], False),
            ([("f", "q"), ("a", "b")], True),
            ([("f", "q"), ("a", "c")], False),
        ]:
            eq_(
                connection.execute(
                    select(
                        tuple_(
                            literal_column("'a'"), literal_column("'b'")
                        ).in_(
                            [
                                tuple_(
                                    *[
                                        literal_column("'%s'" % letter)
                                        for letter in elem
                                    ]
                                )
                                for elem in test
                            ]
                        )
                    )
                ).scalar(),
                exp,
            )


class ExtractTest(fixtures.TablesTest):
    """The rationale behind this test is that for many years we've had a system
    of embedding type casts into the expressions rendered by visit_extract()
    on the postgresql platform.  The reason for this cast is not clear.
    So here we try to produce a wide range of cases to ensure that these casts
    are not needed; see [ticket:2740].

    """

    __only_on__ = "postgresql"
    __backend__ = True

    run_inserts = "once"
    run_deletes = None

    class TZ(datetime.tzinfo):
        def utcoffset(self, dt):
            return datetime.timedelta(hours=4)

    @classmethod
    def setup_bind(cls):
        from sqlalchemy import event

        eng = engines.testing_engine(options={"scope": "class"})

        @event.listens_for(eng, "connect")
        def connect(dbapi_conn, rec):
            cursor = dbapi_conn.cursor()
            cursor.execute("SET SESSION TIME ZONE 0")
            cursor.close()

        return eng

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("dtme", DateTime),
            Column("dt", Date),
            Column("tm", Time),
            Column("intv", postgresql.INTERVAL),
            Column("dttz", DateTime(timezone=True)),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables.t.insert(),
            {
                "dtme": datetime.datetime(2012, 5, 10, 12, 15, 25),
                "dt": datetime.date(2012, 5, 10),
                "tm": datetime.time(12, 15, 25),
                "intv": datetime.timedelta(seconds=570),
                "dttz": datetime.datetime(
                    2012, 5, 10, 12, 15, 25, tzinfo=cls.TZ()
                ),
            },
        )

    def _test(self, connection, expr, field="all", overrides=None):
        t = self.tables.t

        if field == "all":
            fields = {
                "year": 2012,
                "month": 5,
                "day": 10,
                "epoch": 1336652125.0,
                "hour": 12,
                "minute": 15,
            }
        elif field == "time":
            fields = {"hour": 12, "minute": 15, "second": 25}
        elif field == "date":
            fields = {"year": 2012, "month": 5, "day": 10}
        elif field == "all+tz":
            fields = {
                "year": 2012,
                "month": 5,
                "day": 10,
                "epoch": 1336637725.0,
                "hour": 8,
                "timezone": 0,
            }
        else:
            fields = field

        if overrides:
            fields.update(overrides)

        for field in fields:
            result = connection.execute(
                select(extract(field, expr)).select_from(t)
            ).scalar()
            eq_(result, fields[field])

    def test_one(self, connection):
        t = self.tables.t
        self._test(connection, t.c.dtme, "all")

    def test_two(self, connection):
        t = self.tables.t
        self._test(
            connection,
            t.c.dtme + t.c.intv,
            overrides={"epoch": 1336652695.0, "minute": 24},
        )

    def test_three(self, connection):
        self.tables.t

        actual_ts = self.bind.connect().execute(
            func.current_timestamp()
        ).scalar() - datetime.timedelta(days=5)
        self._test(
            connection,
            func.current_timestamp() - datetime.timedelta(days=5),
            {
                "hour": actual_ts.hour,
                "year": actual_ts.year,
                "month": actual_ts.month,
            },
        )

    def test_four(self, connection):
        t = self.tables.t
        self._test(
            connection,
            datetime.timedelta(days=5) + t.c.dt,
            overrides={
                "day": 15,
                "epoch": 1337040000.0,
                "hour": 0,
                "minute": 0,
            },
        )

    def test_five(self, connection):
        t = self.tables.t
        self._test(
            connection,
            func.coalesce(t.c.dtme, func.current_timestamp()),
            overrides={"epoch": 1336652125.0},
        )

    def test_six(self, connection):
        t = self.tables.t
        self._test(
            connection,
            t.c.tm + datetime.timedelta(seconds=30),
            "time",
            overrides={"second": 55},
        )

    def test_seven(self, connection):
        self._test(
            connection,
            literal(datetime.timedelta(seconds=10))
            - literal(datetime.timedelta(seconds=10)),
            "all",
            overrides={
                "hour": 0,
                "minute": 0,
                "month": 0,
                "year": 0,
                "day": 0,
                "epoch": 0,
            },
        )

    def test_eight(self, connection):
        t = self.tables.t
        self._test(
            connection,
            t.c.tm + datetime.timedelta(seconds=30),
            {"hour": 12, "minute": 15, "second": 55},
        )

    def test_nine(self, connection):
        self._test(connection, text("t.dt + t.tm"))

    def test_ten(self, connection):
        t = self.tables.t
        self._test(connection, t.c.dt + t.c.tm)

    def test_eleven(self, connection):
        self._test(
            connection,
            func.current_timestamp() - func.current_timestamp(),
            {"year": 0, "month": 0, "day": 0, "hour": 0},
        )

    def test_twelve(self, connection):
        t = self.tables.t

        actual_ts = connection.scalar(
            func.current_timestamp()
        ) - datetime.datetime(2012, 5, 10, 12, 15, 25, tzinfo=self.TZ())

        self._test(
            connection,
            func.current_timestamp() - t.c.dttz,
            {"day": actual_ts.days},
        )

    def test_thirteen(self, connection):
        t = self.tables.t
        self._test(connection, t.c.dttz, "all+tz")

    def test_fourteen(self, connection):
        t = self.tables.t
        self._test(connection, t.c.tm, "time")

    def test_fifteen(self, connection):
        t = self.tables.t
        self._test(
            connection,
            datetime.timedelta(days=5) + t.c.dtme,
            overrides={"day": 15, "epoch": 1337084125.0},
        )


class TableValuedRoundTripTest(fixtures.TestBase):
    __backend__ = True
    __only_on__ = "postgresql"

    def test_generate_series_scalar(self, connection):
        x = func.generate_series(1, 2).alias("x")
        y = func.generate_series(1, 2).alias("y")

        stmt = select(x.column, y.column).join_from(x, y, true())

        eq_(connection.execute(stmt).all(), [(1, 1), (1, 2), (2, 1), (2, 2)])

    def test_aggregate_scalar_over_table_valued(self, metadata, connection):
        test = Table(
            "test", metadata, Column("id", Integer), Column("data", JSONB)
        )
        test.create(connection)

        connection.execute(
            test.insert(),
            [
                {"id": 1, "data": {"key": [23.7, 108.17, 55.98]}},
                {"id": 2, "data": {"key": [2.320, 9.55]}},
                {"id": 3, "data": {"key": [10.5, 6]}},
            ],
        )

        elem = (
            func.jsonb_array_elements_text(test.c.data["key"])
            .table_valued("value")
            .alias("elem")
        )

        maxdepth = select(func.max(cast(elem.c.value, Float))).label(
            "maxdepth"
        )

        stmt = select(test.c.id.label("test_id"), maxdepth).order_by(
            "maxdepth"
        )

        eq_(
            connection.execute(stmt).all(), [(2, 9.55), (3, 10.5), (1, 108.17)]
        )

    @testing.fixture
    def assets_transactions(self, metadata, connection):
        assets_transactions = Table(
            "assets_transactions",
            metadata,
            Column("id", Integer),
            Column("contents", JSONB),
        )
        assets_transactions.create(connection)
        connection.execute(
            assets_transactions.insert(),
            [
                {"id": 1, "contents": {"k1": "v1"}},
                {"id": 2, "contents": {"k2": "v2"}},
                {"id": 3, "contents": {"k3": "v3"}},
            ],
        )
        return assets_transactions

    def test_scalar_table_valued(self, assets_transactions, connection):
        stmt = select(
            assets_transactions.c.id,
            func.jsonb_each(
                assets_transactions.c.contents, type_=JSONB
            ).scalar_table_valued("key"),
            func.jsonb_each(
                assets_transactions.c.contents, type_=JSONB
            ).scalar_table_valued("value"),
        )

        eq_(
            connection.execute(stmt).all(),
            [(1, "k1", "v1"), (2, "k2", "v2"), (3, "k3", "v3")],
        )

    def test_table_valued(self, assets_transactions, connection):
        jb = func.jsonb_each(assets_transactions.c.contents).table_valued(
            "key", "value"
        )

        stmt = select(assets_transactions.c.id, jb.c.key, jb.c.value).join(
            jb, true()
        )
        eq_(
            connection.execute(stmt).all(),
            [(1, "k1", "v1"), (2, "k2", "v2"), (3, "k3", "v3")],
        )

    @testing.fixture
    def axy_table(self, metadata, connection):
        a = Table(
            "a",
            metadata,
            Column("id", Integer),
            Column("x", Integer),
            Column("y", Integer),
        )
        a.create(connection)
        connection.execute(
            a.insert(),
            [
                {"id": 1, "x": 5, "y": 4},
                {"id": 2, "x": 15, "y": 3},
                {"id": 3, "x": 7, "y": 9},
            ],
        )

        return a

    def test_function_against_table_record(self, axy_table, connection):
        """
        SELECT row_to_json(anon_1) AS row_to_json_1
        FROM (SELECT a.id AS id, a.x AS x, a.y AS y
        FROM a) AS anon_1

        """

        stmt = select(func.row_to_json(axy_table.table_valued()))

        eq_(
            connection.execute(stmt).scalars().all(),
            [
                {"id": 1, "x": 5, "y": 4},
                {"id": 2, "x": 15, "y": 3},
                {"id": 3, "x": 7, "y": 9},
            ],
        )

    def test_function_against_subq_record(self, axy_table, connection):
        """
        SELECT row_to_json(anon_1) AS row_to_json_1
        FROM (SELECT a.id AS id, a.x AS x, a.y AS y
        FROM a) AS anon_1

        """

        stmt = select(
            func.row_to_json(axy_table.select().subquery().table_valued())
        )

        eq_(
            connection.execute(stmt).scalars().all(),
            [
                {"id": 1, "x": 5, "y": 4},
                {"id": 2, "x": 15, "y": 3},
                {"id": 3, "x": 7, "y": 9},
            ],
        )

    def test_function_against_row_constructor(self, connection):
        stmt = select(func.row_to_json(func.row(1, "foo")))

        eq_(connection.scalar(stmt), {"f1": 1, "f2": "foo"})

    def test_with_ordinality_named(self, connection):
        stmt = select(
            func.generate_series(4, 1, -1)
            .table_valued("gs", with_ordinality="ordinality")
            .render_derived()
        )

        eq_(connection.execute(stmt).all(), [(4, 1), (3, 2), (2, 3), (1, 4)])

    def test_with_ordinality_star(self, connection):
        stmt = select("*").select_from(
            func.generate_series(4, 1, -1).table_valued(
                with_ordinality="ordinality"
            )
        )

        eq_(connection.execute(stmt).all(), [(4, 1), (3, 2), (2, 3), (1, 4)])

    def test_array_empty_with_type(self, connection):
        stmt = select(postgresql.array([], type_=Integer))
        eq_(connection.execute(stmt).all(), [([],)])

    def test_plain_old_unnest(self, connection):
        fn = func.unnest(
            postgresql.array(["one", "two", "three", "four"])
        ).column_valued()

        stmt = select(fn)

        eq_(
            connection.execute(stmt).all(),
            [("one",), ("two",), ("three",), ("four",)],
        )

    def test_unnest_with_ordinality(self, connection):
        array_val = postgresql.array(
            [postgresql.array([14, 41, 7]), postgresql.array([54, 9, 49])]
        )
        stmt = select("*").select_from(
            func.unnest(array_val)
            .table_valued("elts", with_ordinality="num")
            .render_derived()
            .alias("t")
        )
        eq_(
            connection.execute(stmt).all(),
            [(14, 1), (41, 2), (7, 3), (54, 4), (9, 5), (49, 6)],
        )

    def test_unnest_with_ordinality_named(self, connection):
        array_val = postgresql.array(
            [postgresql.array([14, 41, 7]), postgresql.array([54, 9, 49])]
        )

        fn = (
            func.unnest(array_val)
            .table_valued("elts", with_ordinality="num")
            .alias("t")
            .render_derived()
        )

        stmt = select(fn.c.elts, fn.c.num)

        eq_(
            connection.execute(stmt).all(),
            [(14, 1), (41, 2), (7, 3), (54, 4), (9, 5), (49, 6)],
        )

    @testing.combinations(
        (
            type_coerce,
            testing.fails("fails on all drivers"),
        ),
        (
            cast,
            testing.fails("fails on all drivers"),
        ),
        (
            None,
            testing.fails_on_everything_except(
                ["postgresql+psycopg2"],
                "I cannot get this to run at all on other drivers, "
                "even selecting from a table",
            ),
        ),
        argnames="cast_fn",
    )
    def test_render_derived_quoting_text(self, connection, cast_fn):
        value = (
            '[{"CaseSensitive":1,"the % value":"foo"}, '
            '{"CaseSensitive":"2","the % value":"bar"}]'
        )

        if cast_fn:
            value = cast_fn(value, JSON)

        fn = (
            func.json_to_recordset(value)
            .table_valued(
                column("CaseSensitive", Integer), column("the % value", String)
            )
            .render_derived(with_types=True)
        )

        stmt = select(fn.c.CaseSensitive, fn.c["the % value"])

        eq_(connection.execute(stmt).all(), [(1, "foo"), (2, "bar")])

    @testing.combinations(
        (
            type_coerce,
            testing.fails("fails on all drivers"),
        ),
        (
            cast,
            testing.fails("fails on all drivers"),
        ),
        (
            None,
            testing.fails("Fails on all drivers"),
        ),
        argnames="cast_fn",
    )
    def test_render_derived_quoting_text_to_json(self, connection, cast_fn):
        value = (
            '[{"CaseSensitive":1,"the % value":"foo"}, '
            '{"CaseSensitive":"2","the % value":"bar"}]'
        )

        if cast_fn:
            value = cast_fn(value, JSON)

        # why won't this work?!?!?
        # should be exactly json_to_recordset(to_json('string'::text))
        #
        fn = (
            func.json_to_recordset(func.to_json(value))
            .table_valued(
                column("CaseSensitive", Integer), column("the % value", String)
            )
            .render_derived(with_types=True)
        )

        stmt = select(fn.c.CaseSensitive, fn.c["the % value"])

        eq_(connection.execute(stmt).all(), [(1, "foo"), (2, "bar")])

    @testing.combinations(
        (type_coerce,),
        (cast,),
        (None, testing.fails("Fails on all PG backends")),
        argnames="cast_fn",
    )
    def test_render_derived_quoting_straight_json(self, connection, cast_fn):
        # these all work

        value = [
            {"CaseSensitive": 1, "the % value": "foo"},
            {"CaseSensitive": "2", "the % value": "bar"},
        ]

        if cast_fn:
            value = cast_fn(value, JSON)

        fn = (
            func.json_to_recordset(value)  # noqa
            .table_valued(
                column("CaseSensitive", Integer), column("the % value", String)
            )
            .render_derived(with_types=True)
        )

        stmt = select(fn.c.CaseSensitive, fn.c["the % value"])

        eq_(connection.execute(stmt).all(), [(1, "foo"), (2, "bar")])


class JSONUpdateTest(fixtures.TablesTest):
    """round trip tests related to using JSON and JSONB in UPDATE statements
    with PG-specific features

    """

    __only_on__ = "postgresql"
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("uuid", Uuid),
            Column("j", JSON),
            Column("jb", JSONB),
        )

    @classmethod
    def insert_data(cls, connection):
        connection.execute(
            cls.tables["t"].insert(),
            [
                {"id": 1, "uuid": "d24587a1-06d9-41df-b1c3-3f423b97a755"},
                {"id": 2, "uuid": "4b07e1c8-d60c-4ea8-9d01-d7cd01362224"},
            ],
        )

    def test_update_values(self, connection):
        t = self.tables["t"]

        value = values(
            Column("id", Integer),
            Column("uuid", Uuid),
            Column("j", JSON),
            Column("jb", JSONB),
            name="update_data",
        ).data(
            [
                (
                    1,
                    "8b6ec1ec-b979-4d0b-b2ce-9acc6e4c2943",
                    {"foo": 1},
                    {"foo_jb": 1},
                ),
                (
                    2,
                    "a2123bcb-7ea3-420a-8284-1db4b2759d79",
                    {"bar": 2},
                    {"bar_jb": 2},
                ),
            ]
        )
        connection.execute(
            t.update()
            .values(uuid=value.c.uuid, j=value.c.j, jb=value.c.jb)
            .where(t.c.id == value.c.id)
        )

        updated_data = connection.execute(t.select().order_by(t.c.id))
        eq_(
            [(str(row.uuid), row.j, row.jb) for row in updated_data],
            [
                (
                    "8b6ec1ec-b979-4d0b-b2ce-9acc6e4c2943",
                    {"foo": 1},
                    {"foo_jb": 1},
                ),
                (
                    "a2123bcb-7ea3-420a-8284-1db4b2759d79",
                    {"bar": 2},
                    {"bar_jb": 2},
                ),
            ],
        )

    @testing.only_on("postgresql>=14")
    def test_jsonb_element_update_basic(self, connection):
        """Test updating individual JSONB elements with subscript syntax

        test #10927

        """
        t = self.tables["t"]

        # Insert test data with complex JSONB
        connection.execute(
            t.insert(),
            [
                {
                    "id": 10,
                    "jb": {
                        "user": {"name": "Alice", "age": 30},
                        "active": True,
                    },
                },
                {
                    "id": 11,
                    "jb": {
                        "user": {"name": "Bob", "age": 25},
                        "active": False,
                    },
                },
            ],
        )

        # Update specific elements using JSONB subscript syntax
        # This tests the new JSONB subscripting feature from issue #10927
        connection.execute(
            t.update()
            .values({t.c.jb["user"]["name"]: "Alice Updated"})
            .where(t.c.id == 10)
        )

        connection.execute(
            t.update().values({t.c.jb["active"]: True}).where(t.c.id == 11)
        )

        results = connection.execute(
            t.select().where(t.c.id.in_([10, 11])).order_by(t.c.id)
        )

        eq_(
            [row.jb for row in results],
            [
                {"user": {"name": "Alice Updated", "age": 30}, "active": True},
                {"user": {"name": "Bob", "age": 25}, "active": True},
            ],
        )

    @testing.only_on("postgresql>=14")
    def test_jsonb_element_update_multiple_keys(self, connection):
        """Test updating multiple JSONB elements in a single statement

        test #10927

        """
        t = self.tables["t"]

        connection.execute(
            t.insert(),
            {
                "id": 20,
                "jb": {
                    "config": {"theme": "dark", "lang": "en"},
                    "version": 1,
                },
            },
        )

        # Update multiple elements at once
        connection.execute(
            t.update()
            .values({t.c.jb["config"]["theme"]: "light", t.c.jb["version"]: 2})
            .where(t.c.id == 20)
        )

        # Verify the updates
        row = connection.execute(t.select().where(t.c.id == 20)).one()

        eq_(
            row.jb,
            {"config": {"theme": "light", "lang": "en"}, "version": 2},
        )

    @testing.only_on("postgresql>=14")
    def test_jsonb_element_update_array_element(self, connection):
        """Test updating JSONB array elements

        test #10927

        """
        t = self.tables["t"]

        # Insert test data with arrays
        connection.execute(
            t.insert(),
            {
                "id": 30,
                "jb": {
                    "tags": ["python", "sql", "postgres"],
                    "priority": "high",
                },
            },
        )

        # Update array element
        connection.execute(
            t.update()
            .values({t.c.jb["tags"][1]: "postgresql"})
            .where(t.c.id == 30)
        )

        # Verify the update
        row = connection.execute(t.select().where(t.c.id == 30)).fetchone()

        eq_(
            row.jb,
            {"tags": ["python", "postgresql", "postgres"], "priority": "high"},
        )
