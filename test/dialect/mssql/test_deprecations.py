# -*- encoding: utf-8
from sqlalchemy import Column
from sqlalchemy import engine_from_config
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy.dialects.mssql import base as mssql
from sqlalchemy.sql import column
from sqlalchemy.sql import table
from sqlalchemy.sql.selectable import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.testing import assertions
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import engines
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.mock import Mock


def _legacy_schema_aliasing_warning():
    return assertions.expect_deprecated("The legacy_schema_aliasing parameter")


class LegacySchemaAliasingTest(fixtures.TestBase, AssertsCompiledSQL):
    """Legacy behavior tried to prevent schema-qualified tables
    from being rendered as dotted names, and were instead aliased.

    This behavior no longer seems to be required.

    """

    def setup_test(self):
        metadata = MetaData()
        self.t1 = table(
            "t1",
            column("a", Integer),
            column("b", String),
            column("c", String),
        )
        self.t2 = Table(
            "t2",
            metadata,
            Column("a", Integer),
            Column("b", Integer),
            Column("c", Integer),
            schema="schema",
        )

    def _assert_sql(self, element, legacy_sql, modern_sql=None):
        dialect = self._legacy_dialect()

        self.assert_compile(element, legacy_sql, dialect=dialect)

        dialect = mssql.dialect()
        self.assert_compile(element, modern_sql or "foob", dialect=dialect)

    def _legacy_dialect(self):
        with _legacy_schema_aliasing_warning():
            return mssql.dialect(legacy_schema_aliasing=True)

    @testing.combinations(
        (
            {
                "sqlalchemy.url": "mssql://foodsn",
                "sqlalchemy.legacy_schema_aliasing": "true",
            },
            True,
        ),
        (
            {
                "sqlalchemy.url": "mssql://foodsn",
                "sqlalchemy.legacy_schema_aliasing": "false",
            },
            False,
        ),
    )
    def test_legacy_schema_flag(self, cfg, expected):
        with testing.expect_deprecated("The legacy_schema_aliasing parameter"):
            e = engine_from_config(
                cfg, module=Mock(version="MS SQL Server 11.0.92")
            )
            is_(e.dialect.legacy_schema_aliasing, expected)

    def test_result_map(self):
        s = self.t2.select()
        c = s.compile(dialect=self._legacy_dialect())
        assert self.t2.c.a in set(c._create_result_map()["a"][1])

    def test_result_map_use_labels(self):
        s = self.t2.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
        c = s.compile(dialect=self._legacy_dialect())
        assert self.t2.c.a in set(c._create_result_map()["schema_t2_a"][1])

    def test_straight_select(self):
        self._assert_sql(
            self.t2.select(),
            "SELECT t2_1.a, t2_1.b, t2_1.c FROM [schema].t2 AS t2_1",
            "SELECT [schema].t2.a, [schema].t2.b, "
            "[schema].t2.c FROM [schema].t2",
        )

    def test_straight_select_use_labels(self):
        self._assert_sql(
            self.t2.select().set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL),
            "SELECT t2_1.a AS schema_t2_a, t2_1.b AS schema_t2_b, "
            "t2_1.c AS schema_t2_c FROM [schema].t2 AS t2_1",
            "SELECT [schema].t2.a AS schema_t2_a, "
            "[schema].t2.b AS schema_t2_b, "
            "[schema].t2.c AS schema_t2_c FROM [schema].t2",
        )

    def test_join_to_schema(self):
        t1, t2 = self.t1, self.t2
        self._assert_sql(
            t1.join(t2, t1.c.a == t2.c.a).select(),
            "SELECT t1.a, t1.b, t1.c, t2_1.a AS a_1, t2_1.b AS b_1, "
            "t2_1.c AS c_1 FROM t1 "
            "JOIN [schema].t2 AS t2_1 ON t2_1.a = t1.a",
            "SELECT t1.a, t1.b, t1.c, [schema].t2.a AS a_1, "
            "[schema].t2.b AS b_1, "
            "[schema].t2.c AS c_1 FROM t1 JOIN [schema].t2 "
            "ON [schema].t2.a = t1.a",
        )

    def test_union_schema_to_non(self):
        t1, t2 = self.t1, self.t2
        s = (
            select(t2.c.a, t2.c.b)
            .set_label_style(LABEL_STYLE_TABLENAME_PLUS_COL)
            .union(
                select(t1.c.a, t1.c.b).set_label_style(
                    LABEL_STYLE_TABLENAME_PLUS_COL
                )
            )
            .alias()
            .select()
        )
        self._assert_sql(
            s,
            "SELECT anon_1.schema_t2_a, anon_1.schema_t2_b FROM "
            "(SELECT t2_1.a AS schema_t2_a, t2_1.b AS schema_t2_b "
            "FROM [schema].t2 AS t2_1 UNION SELECT t1.a AS t1_a, "
            "t1.b AS t1_b FROM t1) AS anon_1",
            "SELECT anon_1.schema_t2_a, anon_1.schema_t2_b FROM "
            "(SELECT [schema].t2.a AS schema_t2_a, [schema].t2.b AS "
            "schema_t2_b FROM [schema].t2 UNION SELECT t1.a AS t1_a, "
            "t1.b AS t1_b FROM t1) AS anon_1",
        )

    def test_column_subquery_to_alias(self):
        a1 = self.t2.alias("a1")
        s = select(self.t2, select(a1.c.a).scalar_subquery())
        self._assert_sql(
            s,
            "SELECT t2_1.a, t2_1.b, t2_1.c, "
            "(SELECT a1.a FROM [schema].t2 AS a1) "
            "AS anon_1 FROM [schema].t2 AS t2_1",
            "SELECT [schema].t2.a, [schema].t2.b, [schema].t2.c, "
            "(SELECT a1.a FROM [schema].t2 AS a1) AS anon_1 FROM [schema].t2",
        )


class LegacySchemaAliasingBackendTest(
    testing.AssertsExecutionResults, fixtures.TestBase
):
    __backend__ = True
    __only_on__ = "mssql"

    @testing.provide_metadata
    def test_insertid_schema(self):
        meta = self.metadata

        with _legacy_schema_aliasing_warning():
            eng = engines.testing_engine(
                options=dict(legacy_schema_aliasing=False)
            )

        tbl = Table(
            "test",
            meta,
            Column("id", Integer, primary_key=True),
            schema=testing.config.test_schema,
        )

        with eng.begin() as conn:
            tbl.create(conn)
            conn.execute(tbl.insert(), {"id": 1})
            eq_(conn.scalar(tbl.select()), 1)

    @testing.provide_metadata
    def test_insertid_schema_legacy(self):
        meta = self.metadata

        tbl = Table(
            "test",
            meta,
            Column("id", Integer, primary_key=True),
            schema=testing.config.test_schema,
        )

        with _legacy_schema_aliasing_warning():
            eng = engines.testing_engine(
                options=dict(legacy_schema_aliasing=True)
            )

        with eng.begin() as conn:

            tbl.create(conn)
            conn.execute(tbl.insert(), {"id": 1})
            eq_(conn.scalar(tbl.select()), 1)

    @testing.provide_metadata
    def test_delete_schema(self, connection):
        meta = self.metadata

        is_(connection.dialect.legacy_schema_aliasing, False)

        tbl = Table(
            "test",
            meta,
            Column("id", Integer, primary_key=True),
            schema=testing.config.test_schema,
        )
        tbl.create(connection)
        connection.execute(tbl.insert(), {"id": 1})
        eq_(connection.scalar(tbl.select()), 1)
        connection.execute(tbl.delete(tbl.c.id == 1))
        eq_(connection.scalar(tbl.select()), None)

    @testing.provide_metadata
    def test_delete_schema_legacy(self):
        meta = self.metadata
        with _legacy_schema_aliasing_warning():
            eng = engines.testing_engine(
                options=dict(legacy_schema_aliasing=True)
            )

        tbl = Table(
            "test",
            meta,
            Column("id", Integer, primary_key=True),
            schema=testing.config.test_schema,
        )

        with eng.begin() as conn:
            tbl.create(conn)
            conn.execute(tbl.insert(), {"id": 1})
            eq_(conn.scalar(tbl.select()), 1)
            conn.execute(tbl.delete(tbl.c.id == 1))
            eq_(conn.scalar(tbl.select()), None)
