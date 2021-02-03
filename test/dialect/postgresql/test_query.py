# coding: utf-8

import datetime

from sqlalchemy import and_
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import Date
from sqlalchemy import DateTime
from sqlalchemy import exc
from sqlalchemy import extract
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
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
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import AssertsExecutionResults
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertsql import CursorSQL
from sqlalchemy.testing.assertsql import DialectSQL


class InsertTest(fixtures.TestBase, AssertsExecutionResults):

    __only_on__ = "postgresql"
    __backend__ = True

    def setup_test(self):
        self.metadata = MetaData()

    def teardown_test(self):
        with testing.db.begin() as conn:
            self.metadata.drop_all(conn)

    @testing.combinations((False,), (True,))
    def test_foreignkey_missing_insert(self, implicit_returning):
        engine = engines.testing_engine(
            options={"implicit_returning": implicit_returning}
        )

        Table("t1", self.metadata, Column("id", Integer, primary_key=True))
        t2 = Table(
            "t2",
            self.metadata,
            Column("id", Integer, ForeignKey("t1.id"), primary_key=True),
        )

        self.metadata.create_all(engine)

        # want to ensure that "null value in column "id" violates not-
        # null constraint" is raised (IntegrityError on psycoopg2, but
        # ProgrammingError on pg8000), and not "ProgrammingError:
        # (ProgrammingError) relationship "t2_id_seq" does not exist".
        # the latter corresponds to autoincrement behavior, which is not
        # the case here due to the foreign key.

        with expect_warnings(".*has no Python-side or server-side default.*"):
            with engine.begin() as conn:
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    t2.insert(),
                )

    def test_sequence_insert(self):
        table = Table(
            "testtable",
            self.metadata,
            Column("id", Integer, Sequence("my_seq"), primary_key=True),
            Column("data", String(30)),
        )
        self.metadata.create_all(testing.db)
        self._assert_data_with_sequence(table, "my_seq")

    @testing.requires.returning
    def test_sequence_returning_insert(self):
        table = Table(
            "testtable",
            self.metadata,
            Column("id", Integer, Sequence("my_seq"), primary_key=True),
            Column("data", String(30)),
        )
        self.metadata.create_all(testing.db)
        self._assert_data_with_sequence_returning(table, "my_seq")

    def test_opt_sequence_insert(self):
        table = Table(
            "testtable",
            self.metadata,
            Column(
                "id",
                Integer,
                Sequence("my_seq", optional=True),
                primary_key=True,
            ),
            Column("data", String(30)),
        )
        self.metadata.create_all(testing.db)
        self._assert_data_autoincrement(table)

    @testing.requires.returning
    def test_opt_sequence_returning_insert(self):
        table = Table(
            "testtable",
            self.metadata,
            Column(
                "id",
                Integer,
                Sequence("my_seq", optional=True),
                primary_key=True,
            ),
            Column("data", String(30)),
        )
        self.metadata.create_all(testing.db)
        self._assert_data_autoincrement_returning(table)

    def test_autoincrement_insert(self):
        table = Table(
            "testtable",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30)),
        )
        self.metadata.create_all(testing.db)
        self._assert_data_autoincrement(table)

    @testing.requires.returning
    def test_autoincrement_returning_insert(self):
        table = Table(
            "testtable",
            self.metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30)),
        )
        self.metadata.create_all(testing.db)
        self._assert_data_autoincrement_returning(table)

    def test_noautoincrement_insert(self):
        table = Table(
            "testtable",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("data", String(30)),
        )
        self.metadata.create_all(testing.db)
        self._assert_data_noautoincrement(table)

    def _assert_data_autoincrement(self, table):
        engine = engines.testing_engine(options={"implicit_returning": False})

        with self.sql_execution_asserter(engine) as asserter:

            with engine.begin() as conn:
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
                "INSERT INTO testtable (data) VALUES (:data)", [{"data": "d8"}]
            ),
        )

        with engine.begin() as conn:
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
        table = Table(table.name, m2, autoload_with=engine)

        with self.sql_execution_asserter(engine) as asserter:
            with engine.begin() as conn:
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
                "INSERT INTO testtable (data) VALUES (:data)", [{"data": "d8"}]
            ),
        )
        with engine.begin() as conn:
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
            conn.execute(table.delete())

    def _assert_data_autoincrement_returning(self, table):
        engine = engines.testing_engine(options={"implicit_returning": True})

        with self.sql_execution_asserter(engine) as asserter:
            with engine.begin() as conn:

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
                "INSERT INTO testtable (data) VALUES (:data)", [{"data": "d8"}]
            ),
        )

        with engine.begin() as conn:
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
        table = Table(table.name, m2, autoload_with=engine)

        with self.sql_execution_asserter(engine) as asserter:
            with engine.begin() as conn:
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
                "INSERT INTO testtable (data) VALUES (:data)", [{"data": "d8"}]
            ),
        )

        with engine.begin() as conn:
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
            conn.execute(table.delete())

    def _assert_data_with_sequence(self, table, seqname):
        engine = engines.testing_engine(options={"implicit_returning": False})

        with self.sql_execution_asserter(engine) as asserter:
            with engine.begin() as conn:
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
        with engine.begin() as conn:
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

        # cant test reflection here since the Sequence must be
        # explicitly specified

    def _assert_data_with_sequence_returning(self, table, seqname):
        engine = engines.testing_engine(options={"implicit_returning": True})

        with self.sql_execution_asserter(engine) as asserter:
            with engine.begin() as conn:
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

        with engine.begin() as conn:
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

            # cant test reflection here since the Sequence must be
            # explicitly specified

    def _assert_data_noautoincrement(self, table):
        engine = engines.testing_engine(options={"implicit_returning": False})

        # turning off the cache because we are checking for compile-time
        # warnings
        engine = engine.execution_options(compiled_cache=None)

        with engine.begin() as conn:
            conn.execute(table.insert(), {"id": 30, "data": "d1"})

        with engine.begin() as conn:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    {"data": "d2"},
                )

        with engine.begin() as conn:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    [{"data": "d2"}, {"data": "d3"}],
                )

        with engine.begin() as conn:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    {"data": "d2"},
                )

        with engine.begin() as conn:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    [{"data": "d2"}, {"data": "d3"}],
                )

        with engine.begin() as conn:
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
        table = Table(table.name, m2, autoload_with=engine)
        with engine.begin() as conn:
            conn.execute(table.insert(), {"id": 30, "data": "d1"})

        with engine.begin() as conn:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    {"data": "d2"},
                )

        with engine.begin() as conn:
            with expect_warnings(
                ".*has no Python-side or server-side default.*"
            ):
                assert_raises(
                    (exc.IntegrityError, exc.ProgrammingError),
                    conn.execute,
                    table.insert(),
                    [{"data": "d2"}, {"data": "d3"}],
                )

        with engine.begin() as conn:
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

    @testing.requires.pyformat_paramstyle
    def test_expression_pyformat(self):
        matchtable = self.tables.matchtable
        self.assert_compile(
            matchtable.c.title.match("somstr"),
            "matchtable.title @@ to_tsquery(%(title_1)s" ")",
        )

    @testing.requires.format_paramstyle
    def test_expression_positional(self):
        matchtable = self.tables.matchtable
        self.assert_compile(
            matchtable.c.title.match("somstr"),
            "matchtable.title @@ to_tsquery(%s)",
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
        results2 = connection.execute(
            matchtable.select()
            .where(matchtable.c.title.match("nutshells | rubies"))
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
        results2 = connection.execute(
            matchtable.select().where(
                matchtable.c.title.match("python & nutshells")
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
    on the postgreql platform.  The reason for this cast is not clear.
    So here we try to produce a wide range of cases to ensure that these casts
    are not needed; see [ticket:2740].

    """

    __only_on__ = "postgresql"
    __backend__ = True

    run_inserts = "once"
    run_deletes = None

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
        # TODO: why does setting hours to anything
        # not affect the TZ in the DB col ?
        class TZ(datetime.tzinfo):
            def utcoffset(self, dt):
                return datetime.timedelta(hours=4)

        connection.execute(
            cls.tables.t.insert(),
            {
                "dtme": datetime.datetime(2012, 5, 10, 12, 15, 25),
                "dt": datetime.date(2012, 5, 10),
                "tm": datetime.time(12, 15, 25),
                "intv": datetime.timedelta(seconds=570),
                "dttz": datetime.datetime(
                    2012, 5, 10, 12, 15, 25, tzinfo=TZ()
                ),
            },
        )

    def _test(self, expr, field="all", overrides=None):
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
            result = self.bind.scalar(
                select(extract(field, expr)).select_from(t)
            )
            eq_(result, fields[field])

    def test_one(self):
        t = self.tables.t
        self._test(t.c.dtme, "all")

    def test_two(self):
        t = self.tables.t
        self._test(
            t.c.dtme + t.c.intv,
            overrides={"epoch": 1336652695.0, "minute": 24},
        )

    def test_three(self):
        self.tables.t

        actual_ts = self.bind.scalar(
            func.current_timestamp()
        ) - datetime.timedelta(days=5)
        self._test(
            func.current_timestamp() - datetime.timedelta(days=5),
            {
                "hour": actual_ts.hour,
                "year": actual_ts.year,
                "month": actual_ts.month,
            },
        )

    def test_four(self):
        t = self.tables.t
        self._test(
            datetime.timedelta(days=5) + t.c.dt,
            overrides={
                "day": 15,
                "epoch": 1337040000.0,
                "hour": 0,
                "minute": 0,
            },
        )

    def test_five(self):
        t = self.tables.t
        self._test(
            func.coalesce(t.c.dtme, func.current_timestamp()),
            overrides={"epoch": 1336652125.0},
        )

    def test_six(self):
        t = self.tables.t
        self._test(
            t.c.tm + datetime.timedelta(seconds=30),
            "time",
            overrides={"second": 55},
        )

    def test_seven(self):
        self._test(
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

    def test_eight(self):
        t = self.tables.t
        self._test(
            t.c.tm + datetime.timedelta(seconds=30),
            {"hour": 12, "minute": 15, "second": 55},
        )

    def test_nine(self):
        self._test(text("t.dt + t.tm"))

    def test_ten(self):
        t = self.tables.t
        self._test(t.c.dt + t.c.tm)

    def test_eleven(self):
        self._test(
            func.current_timestamp() - func.current_timestamp(),
            {"year": 0, "month": 0, "day": 0, "hour": 0},
        )

    def test_twelve(self):
        t = self.tables.t
        actual_ts = self.bind.scalar(func.current_timestamp()).replace(
            tzinfo=None
        ) - datetime.datetime(2012, 5, 10, 12, 15, 25)

        self._test(
            func.current_timestamp()
            - func.coalesce(t.c.dtme, func.current_timestamp()),
            {"day": actual_ts.days},
        )

    def test_thirteen(self):
        t = self.tables.t
        self._test(t.c.dttz, "all+tz")

    def test_fourteen(self):
        t = self.tables.t
        self._test(t.c.tm, "time")

    def test_fifteen(self):
        t = self.tables.t
        self._test(
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
