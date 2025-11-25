import itertools
from operator import itemgetter
import re

import sqlalchemy as sa
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Identity
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import join
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import Sequence
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import Text
from sqlalchemy import text
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import base as postgresql
from sqlalchemy.dialects.postgresql import DOMAIN
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.dialects.postgresql import INET
from sqlalchemy.dialects.postgresql import INTEGER
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.dialects.postgresql import pg_catalog
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy.engine import ObjectKind
from sqlalchemy.engine import ObjectScope
from sqlalchemy.schema import CreateIndex
from sqlalchemy.sql import ddl as sa_ddl
from sqlalchemy.sql.schema import CheckConstraint
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import config
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import assert_warns
from sqlalchemy.testing.assertions import AssertsExecutionResults
from sqlalchemy.testing.assertions import ComparesIndexes
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import expect_raises
from sqlalchemy.testing.assertions import expect_warnings
from sqlalchemy.testing.assertions import is_
from sqlalchemy.testing.assertions import is_false
from sqlalchemy.testing.assertions import is_true
from sqlalchemy.types import NullType


class ReflectionFixtures:
    @testing.fixture(
        params=[
            ("engine", True),
            ("connection", True),
            ("engine", False),
            ("connection", False),
        ]
    )
    def inspect_fixture(self, request, metadata, testing_engine):
        engine, future = request.param

        eng = testing_engine(future=future)

        conn = eng.connect()

        if engine == "connection":
            yield inspect(eng), conn
        else:
            yield inspect(conn), conn

        conn.close()


class ForeignTableReflectionTest(
    ReflectionFixtures, fixtures.TablesTest, AssertsExecutionResults
):
    """Test reflection on foreign tables"""

    __requires__ = ("postgresql_test_dblink",)
    __only_on__ = "postgresql >= 9.3"
    __sparse_driver_backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        from sqlalchemy.testing import config

        dblink = config.file_config.get(
            "sqla_testing", "postgres_test_db_link"
        )

        Table(
            "testtable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30)),
        )

        for ddl in [
            "CREATE SERVER test_server FOREIGN DATA WRAPPER postgres_fdw "
            "OPTIONS (dbname 'test', host '%s')" % dblink,
            "CREATE USER MAPPING FOR public \
            SERVER test_server options (user 'scott', password 'tiger')",
            "CREATE FOREIGN TABLE test_foreigntable ( "
            "   id          INT, "
            "   data        VARCHAR(30) "
            ") SERVER test_server OPTIONS (table_name 'testtable')",
        ]:
            sa.event.listen(metadata, "after_create", sa.DDL(ddl))

        for ddl in [
            "DROP FOREIGN TABLE test_foreigntable",
            "DROP USER MAPPING FOR public SERVER test_server",
            "DROP SERVER test_server",
        ]:
            sa.event.listen(metadata, "before_drop", sa.DDL(ddl))

    def test_foreign_table_is_reflected(self, connection):
        metadata = MetaData()
        table = Table("test_foreigntable", metadata, autoload_with=connection)
        eq_(
            set(table.columns.keys()),
            {"id", "data"},
            "Columns of reflected foreign table didn't equal expected columns",
        )

    def test_get_foreign_table_names(self, inspect_fixture):
        inspector, conn = inspect_fixture

        ft_names = inspector.get_foreign_table_names()
        eq_(ft_names, ["test_foreigntable"])

    def test_get_table_names_no_foreign(self, connection):
        inspector = inspect(connection)
        names = inspector.get_table_names()
        eq_(names, ["testtable"])


class PartitionedReflectionTest(fixtures.TablesTest, AssertsExecutionResults):
    # partitioned table reflection, issue #4237

    __only_on__ = "postgresql >= 10"
    __sparse_driver_backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        # the actual function isn't reflected yet
        dv = Table(
            "data_values",
            metadata,
            Column("modulus", Integer, nullable=False),
            Column("data", String(30)),
            Column("q", Integer),
            postgresql_partition_by="range(modulus)",
        )

        # looks like this is reflected prior to #4237
        sa.event.listen(
            dv,
            "after_create",
            sa.DDL(
                "CREATE TABLE data_values_4_10 PARTITION OF data_values "
                "FOR VALUES FROM (4) TO (10)"
            ),
        )

        if testing.against("postgresql >= 11"):
            Index("my_index", dv.c.q)

    def test_get_tablenames(self, connection):
        assert {"data_values", "data_values_4_10"}.issubset(
            inspect(connection).get_table_names()
        )

    def test_reflect_cols(self, connection):
        cols = inspect(connection).get_columns("data_values")
        eq_([c["name"] for c in cols], ["modulus", "data", "q"])

    def test_reflect_cols_from_partition(self, connection):
        cols = inspect(connection).get_columns("data_values_4_10")
        eq_([c["name"] for c in cols], ["modulus", "data", "q"])

    @testing.only_on("postgresql >= 11")
    def test_reflect_index(self, connection):
        idx = inspect(connection).get_indexes("data_values")
        eq_(
            idx,
            [
                {
                    "name": "my_index",
                    "unique": False,
                    "column_names": ["q"],
                    "include_columns": [],
                    "dialect_options": {"postgresql_include": []},
                }
            ],
        )

    @testing.only_on("postgresql >= 11")
    def test_reflect_index_from_partition(self, connection):
        idx = inspect(connection).get_indexes("data_values_4_10")
        # note the name appears to be generated by PG, currently
        # 'data_values_4_10_q_idx'
        eq_(
            idx,
            [
                {
                    "column_names": ["q"],
                    "include_columns": [],
                    "dialect_options": {"postgresql_include": []},
                    "name": mock.ANY,
                    "unique": False,
                }
            ],
        )


class MaterializedViewReflectionTest(
    ReflectionFixtures, fixtures.TablesTest, AssertsExecutionResults
):
    """Test reflection on materialized views"""

    __only_on__ = "postgresql >= 9.3"
    __sparse_driver_backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        testtable = Table(
            "testtable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(30)),
        )

        # insert data before we create the view
        @sa.event.listens_for(testtable, "after_create")
        def insert_data(target, connection, **kw):
            connection.execute(target.insert(), {"id": 89, "data": "d1"})

        materialized_view = sa.DDL(
            "CREATE MATERIALIZED VIEW test_mview AS SELECT * FROM testtable"
        )

        plain_view = sa.DDL(
            "CREATE VIEW test_regview AS SELECT data FROM testtable"
        )

        sa.event.listen(testtable, "after_create", plain_view)
        sa.event.listen(testtable, "after_create", materialized_view)
        sa.event.listen(
            testtable,
            "after_create",
            sa.DDL("COMMENT ON VIEW test_regview IS 'regular view comment'"),
        )
        sa.event.listen(
            testtable,
            "after_create",
            sa.DDL(
                "COMMENT ON MATERIALIZED VIEW test_mview "
                "IS 'materialized view comment'"
            ),
        )
        sa.event.listen(
            testtable,
            "after_create",
            sa.DDL("CREATE INDEX mat_index ON test_mview(data DESC)"),
        )

        sa.event.listen(
            testtable,
            "before_drop",
            sa.DDL("DROP MATERIALIZED VIEW test_mview"),
        )
        sa.event.listen(
            testtable, "before_drop", sa.DDL("DROP VIEW test_regview")
        )

    def test_has_type(self, connection):
        insp = inspect(connection)
        is_true(insp.has_type("test_mview"))
        is_true(insp.has_type("test_regview"))
        is_true(insp.has_type("testtable"))

    def test_mview_is_reflected(self, connection):
        metadata = MetaData()
        table = Table("test_mview", metadata, autoload_with=connection)
        eq_(
            set(table.columns.keys()),
            {"id", "data"},
            "Columns of reflected mview didn't equal expected columns",
        )

    def test_mview_select(self, connection):
        metadata = MetaData()
        table = Table("test_mview", metadata, autoload_with=connection)
        eq_(connection.execute(table.select()).fetchall(), [(89, "d1")])

    def test_get_view_names(self, inspect_fixture):
        insp, conn = inspect_fixture
        eq_(set(insp.get_view_names()), {"test_regview"})

    def test_get_materialized_view_names(self, inspect_fixture):
        insp, conn = inspect_fixture
        eq_(set(insp.get_materialized_view_names()), {"test_mview"})

    def test_get_view_names_reflection_cache_ok(self, connection):
        insp = inspect(connection)
        eq_(set(insp.get_view_names()), {"test_regview"})
        eq_(
            set(insp.get_materialized_view_names()),
            {"test_mview"},
        )
        eq_(
            set(insp.get_view_names()).union(
                insp.get_materialized_view_names()
            ),
            {"test_regview", "test_mview"},
        )

    def test_get_view_definition(self, connection):
        insp = inspect(connection)

        def normalize(definition):
            # pg16 returns "SELECT" without qualifying tablename.
            # older pgs include it
            definition = re.sub(
                r"testtable\.(\w+)", lambda m: m.group(1), definition
            )
            return re.sub(r"[\n\t ]+", " ", definition.strip())

        eq_(
            normalize(insp.get_view_definition("test_mview")),
            "SELECT id, data FROM testtable;",
        )
        eq_(
            normalize(insp.get_view_definition("test_regview")),
            "SELECT data FROM testtable;",
        )

    def test_get_view_comment(self, connection):
        insp = inspect(connection)
        eq_(
            insp.get_table_comment("test_regview"),
            {"text": "regular view comment"},
        )
        eq_(
            insp.get_table_comment("test_mview"),
            {"text": "materialized view comment"},
        )

    def test_get_multi_view_comment(self, connection):
        insp = inspect(connection)
        eq_(
            insp.get_multi_table_comment(),
            {(None, "testtable"): {"text": None}},
        )
        plain = {(None, "test_regview"): {"text": "regular view comment"}}
        mat = {(None, "test_mview"): {"text": "materialized view comment"}}
        eq_(insp.get_multi_table_comment(kind=ObjectKind.VIEW), plain)
        eq_(
            insp.get_multi_table_comment(kind=ObjectKind.MATERIALIZED_VIEW),
            mat,
        )
        eq_(
            insp.get_multi_table_comment(kind=ObjectKind.ANY_VIEW),
            {**plain, **mat},
        )
        eq_(
            insp.get_multi_table_comment(
                kind=ObjectKind.ANY_VIEW, scope=ObjectScope.TEMPORARY
            ),
            {},
        )

    def test_get_multi_view_indexes(self, connection):
        insp = inspect(connection)
        eq_(insp.get_multi_indexes(), {(None, "testtable"): []})

        exp = {
            "name": "mat_index",
            "unique": False,
            "column_names": ["data"],
            "column_sorting": {"data": ("desc",)},
        }
        if connection.dialect.server_version_info >= (11, 0):
            exp["include_columns"] = []
            exp["dialect_options"] = {"postgresql_include": []}
        plain = {(None, "test_regview"): []}
        mat = {(None, "test_mview"): [exp]}
        eq_(insp.get_multi_indexes(kind=ObjectKind.VIEW), plain)
        eq_(insp.get_multi_indexes(kind=ObjectKind.MATERIALIZED_VIEW), mat)
        eq_(insp.get_multi_indexes(kind=ObjectKind.ANY_VIEW), {**plain, **mat})
        eq_(
            insp.get_multi_indexes(
                kind=ObjectKind.ANY_VIEW, scope=ObjectScope.TEMPORARY
            ),
            {},
        )


class DomainReflectionTest(fixtures.TestBase, AssertsExecutionResults):
    """Test PostgreSQL domains"""

    __only_on__ = "postgresql > 8.3"
    __sparse_driver_backend__ = True

    # these fixtures are all currently using individual test scope,
    # on a connection that's in a transaction that's rolled back.
    # previously, this test would build up all the domains / tables
    # at the class level and commit them.  PostgreSQL seems to be extremely
    # fast at building up / tearing down domains / schemas etc within an
    # uncommitted transaction so it seems OK to keep these at per-test
    # scope.

    @testing.fixture()
    def broken_nullable_domains(self):
        if not testing.requires.postgresql_working_nullable_domains.enabled:
            config.skip_test(
                "reflection of nullable domains broken on PG 17.0-17.2"
            )

    @testing.fixture()
    def testdomain(self, connection, broken_nullable_domains):
        connection.exec_driver_sql(
            "CREATE DOMAIN testdomain INTEGER NOT NULL DEFAULT 42"
        )
        yield
        connection.exec_driver_sql("DROP DOMAIN testdomain")

    @testing.fixture
    def testtable(self, connection, testdomain):
        connection.exec_driver_sql(
            "CREATE TABLE testtable (question integer, answer testdomain)"
        )
        yield
        connection.exec_driver_sql("DROP TABLE testtable")

    @testing.fixture
    def nullable_domains(self, connection, broken_nullable_domains):
        connection.exec_driver_sql(
            'CREATE DOMAIN nullable_domain AS TEXT COLLATE "C" CHECK '
            "(VALUE IN('FOO', 'BAR'))"
        )
        connection.exec_driver_sql(
            "CREATE DOMAIN not_nullable_domain AS TEXT NOT NULL"
        )
        yield
        connection.exec_driver_sql("DROP DOMAIN nullable_domain")
        connection.exec_driver_sql("DROP DOMAIN not_nullable_domain")

    @testing.fixture
    def nullable_domain_table(self, connection, nullable_domains):
        connection.exec_driver_sql(
            "CREATE TABLE nullable_domain_test "
            "(not_nullable_domain_col nullable_domain not null,"
            "nullable_local not_nullable_domain)"
        )
        yield
        connection.exec_driver_sql("DROP TABLE nullable_domain_test")

    @testing.fixture
    def enum_domain(self, connection):
        connection.exec_driver_sql("CREATE TYPE testtype AS ENUM ('test')")
        connection.exec_driver_sql("CREATE DOMAIN enumdomain AS testtype")
        yield
        connection.exec_driver_sql("drop domain enumdomain")
        connection.exec_driver_sql("drop type testtype")

    @testing.fixture
    def enum_table(self, connection, enum_domain):
        connection.exec_driver_sql(
            "CREATE TABLE enum_test (id integer, data enumdomain)"
        )
        yield
        connection.exec_driver_sql("DROP TABLE enum_test")

    @testing.fixture
    def array_domains(self, connection):
        connection.exec_driver_sql("CREATE DOMAIN arraydomain AS INTEGER[]")
        connection.exec_driver_sql(
            "CREATE DOMAIN arraydomain_2d AS INTEGER[][]"
        )
        connection.exec_driver_sql(
            "CREATE DOMAIN arraydomain_3d AS INTEGER[][][]"
        )
        yield
        connection.exec_driver_sql("DROP DOMAIN arraydomain")
        connection.exec_driver_sql("DROP DOMAIN arraydomain_2d")
        connection.exec_driver_sql("DROP DOMAIN arraydomain_3d")

    @testing.fixture
    def array_table(self, connection, array_domains):
        connection.exec_driver_sql(
            "CREATE TABLE array_test ("
            "id integer, "
            "datas arraydomain, "
            "datass arraydomain_2d, "
            "datasss arraydomain_3d"
            ")"
        )
        yield
        connection.exec_driver_sql("DROP TABLE array_test")

    @testing.fixture
    def some_schema(self, connection):
        connection.exec_driver_sql('CREATE SCHEMA IF NOT EXISTS "SomeSchema"')
        yield
        connection.exec_driver_sql('DROP SCHEMA IF EXISTS "SomeSchema"')

    @testing.fixture
    def quoted_schema_domain(self, connection, some_schema):
        connection.exec_driver_sql(
            'CREATE DOMAIN "SomeSchema"."Quoted.Domain" INTEGER DEFAULT 0'
        )
        yield
        connection.exec_driver_sql('DROP DOMAIN "SomeSchema"."Quoted.Domain"')

    @testing.fixture
    def int_domain(self, connection):
        connection.exec_driver_sql(
            "CREATE DOMAIN my_int AS int CONSTRAINT b_my_int_one CHECK "
            "(VALUE > 1) CONSTRAINT a_my_int_two CHECK (VALUE < 42) "
            "CHECK(VALUE != 22)"
        )
        yield
        connection.exec_driver_sql("DROP DOMAIN my_int")

    @testing.fixture
    def quote_table(self, connection, quoted_schema_domain):
        connection.exec_driver_sql(
            "CREATE TABLE quote_test "
            '(id integer, data "SomeSchema"."Quoted.Domain")'
        )
        yield
        connection.exec_driver_sql("drop table quote_test")

    @testing.fixture
    def testdomain_schema(self, connection):
        connection.exec_driver_sql(
            "CREATE DOMAIN test_schema.testdomain INTEGER DEFAULT 0"
        )
        yield
        connection.exec_driver_sql("DROP DOMAIN test_schema.testdomain")

    @testing.fixture
    def testtable_schema(self, connection, testdomain_schema):
        connection.exec_driver_sql(
            "CREATE TABLE test_schema.testtable(question "
            "integer, answer test_schema.testdomain, anything "
            "integer)"
        )
        yield
        connection.exec_driver_sql("drop table test_schema.testtable")

    @testing.fixture
    def crosschema_table(self, connection, testdomain_schema):
        connection.exec_driver_sql(
            "CREATE TABLE crosschema (question integer, answer "
            f"{config.test_schema}.testdomain)"
        )
        yield
        connection.exec_driver_sql("DROP TABLE crosschema")

    def test_table_is_reflected(self, connection, testtable):
        metadata = MetaData()
        table = Table("testtable", metadata, autoload_with=connection)
        eq_(
            set(table.columns.keys()),
            {"question", "answer"},
            "Columns of reflected table didn't equal expected columns",
        )
        assert isinstance(table.c.answer.type, DOMAIN)
        assert table.c.answer.type.name, "testdomain"
        assert isinstance(table.c.answer.type.data_type, Integer)

    def test_nullable_from_domain(self, connection, nullable_domain_table):
        metadata = MetaData()
        table = Table(
            "nullable_domain_test", metadata, autoload_with=connection
        )
        is_(table.c.not_nullable_domain_col.nullable, False)
        is_(table.c.nullable_local.nullable, False)

    def test_domain_is_reflected(self, connection, testtable):
        metadata = MetaData()
        table = Table("testtable", metadata, autoload_with=connection)
        eq_(
            str(table.columns.answer.server_default.arg),
            "42",
            "Reflected default value didn't equal expected value",
        )
        assert (
            not table.columns.answer.nullable
        ), "Expected reflected column to not be nullable."

    def test_enum_domain_is_reflected(self, connection, enum_table):
        metadata = MetaData()
        table = Table("enum_test", metadata, autoload_with=connection)
        assert isinstance(table.c.data.type, DOMAIN)
        eq_(table.c.data.type.data_type.enums, ["test"])

    def test_array_domain_is_reflected(self, connection, array_table):
        metadata = MetaData()
        table = Table("array_test", metadata, autoload_with=connection)

        def assert_is_integer_array_domain(domain, name):
            # Postgres does not persist the dimensionality of the array.
            # It's always treated as integer[]
            assert isinstance(domain, DOMAIN)
            assert domain.name == name
            assert isinstance(domain.data_type, ARRAY)
            assert isinstance(domain.data_type.item_type, INTEGER)

        array_domain = table.c.datas.type
        assert_is_integer_array_domain(array_domain, "arraydomain")

        array_domain_2d = table.c.datass.type
        assert_is_integer_array_domain(array_domain_2d, "arraydomain_2d")

        array_domain_3d = table.c.datasss.type
        assert_is_integer_array_domain(array_domain_3d, "arraydomain_3d")

    def test_quoted_remote_schema_domain_is_reflected(
        self, connection, quote_table
    ):
        metadata = MetaData()
        table = Table("quote_test", metadata, autoload_with=connection)
        assert isinstance(table.c.data.type, DOMAIN)
        assert table.c.data.type.name, "Quoted.Domain"
        assert isinstance(table.c.data.type.data_type, Integer)

    def test_table_is_reflected_test_schema(
        self, connection, testtable_schema
    ):
        metadata = MetaData()
        table = Table(
            "testtable",
            metadata,
            autoload_with=connection,
            schema=config.test_schema,
        )
        eq_(
            set(table.columns.keys()),
            {"question", "answer", "anything"},
            "Columns of reflected table didn't equal expected columns",
        )
        assert isinstance(table.c.anything.type, Integer)

    def test_schema_domain_is_reflected(self, connection, testtable_schema):
        metadata = MetaData()
        table = Table(
            "testtable",
            metadata,
            autoload_with=connection,
            schema=config.test_schema,
        )
        eq_(
            str(table.columns.answer.server_default.arg),
            "0",
            "Reflected default value didn't equal expected value",
        )
        assert (
            table.columns.answer.nullable
        ), "Expected reflected column to be nullable."

    def test_crosschema_domain_is_reflected(
        self, connection, crosschema_table
    ):
        metadata = MetaData()
        table = Table("crosschema", metadata, autoload_with=connection)
        eq_(
            str(table.columns.answer.server_default.arg),
            "0",
            "Reflected default value didn't equal expected value",
        )
        assert (
            table.columns.answer.nullable
        ), "Expected reflected column to be nullable."

    def test_unknown_types(self, connection, testtable):
        from sqlalchemy.dialects.postgresql import base

        ischema_names = base.PGDialect.ischema_names
        base.PGDialect.ischema_names = {}
        try:
            m2 = MetaData()
            assert_warns(
                exc.SAWarning, Table, "testtable", m2, autoload_with=connection
            )

            @testing.emits_warning("Did not recognize type")
            def warns():
                m3 = MetaData()
                t3 = Table("testtable", m3, autoload_with=connection)
                assert t3.c.answer.type.__class__ == sa.types.NullType

        finally:
            base.PGDialect.ischema_names = ischema_names

    @testing.fixture
    def all_domains(
        self,
        quoted_schema_domain,
        array_domains,
        enum_domain,
        nullable_domains,
        int_domain,
        testdomain,
        testdomain_schema,
    ):
        return {
            "public": [
                {
                    "visible": True,
                    "name": "arraydomain",
                    "schema": "public",
                    "nullable": True,
                    "type": "integer[]",
                    "default": None,
                    "constraints": [],
                    "collation": None,
                },
                {
                    "visible": True,
                    "name": "arraydomain_2d",
                    "schema": "public",
                    "nullable": True,
                    "type": "integer[]",
                    "default": None,
                    "constraints": [],
                    "collation": None,
                },
                {
                    "visible": True,
                    "name": "arraydomain_3d",
                    "schema": "public",
                    "nullable": True,
                    "type": "integer[]",
                    "default": None,
                    "constraints": [],
                    "collation": None,
                },
                {
                    "visible": True,
                    "name": "enumdomain",
                    "schema": "public",
                    "nullable": True,
                    "type": "testtype",
                    "default": None,
                    "constraints": [],
                    "collation": None,
                },
                {
                    "visible": True,
                    "name": "my_int",
                    "schema": "public",
                    "nullable": True,
                    "type": "integer",
                    "default": None,
                    "constraints": [
                        {"check": "VALUE < 42", "name": "a_my_int_two"},
                        {"check": "VALUE > 1", "name": "b_my_int_one"},
                        # autogenerated name by pg
                        {"check": "VALUE <> 22", "name": "my_int_check"},
                    ],
                    "collation": None,
                },
                {
                    "visible": True,
                    "name": "not_nullable_domain",
                    "schema": "public",
                    "nullable": False,
                    "type": "text",
                    "default": None,
                    "constraints": [],
                    "collation": "default",
                },
                {
                    "visible": True,
                    "name": "nullable_domain",
                    "schema": "public",
                    "nullable": True,
                    "type": "text",
                    "default": None,
                    "constraints": [
                        {
                            "check": "VALUE = ANY (ARRAY['FOO'::text, "
                            "'BAR'::text])",
                            # autogenerated name by pg
                            "name": "nullable_domain_check",
                        }
                    ],
                    "collation": "C",
                },
                {
                    "visible": True,
                    "name": "testdomain",
                    "schema": "public",
                    "nullable": False,
                    "type": "integer",
                    "default": "42",
                    "constraints": [],
                    "collation": None,
                },
            ],
            "test_schema": [
                {
                    "visible": False,
                    "name": "testdomain",
                    "schema": "test_schema",
                    "nullable": True,
                    "type": "integer",
                    "default": "0",
                    "constraints": [],
                    "collation": None,
                }
            ],
            "SomeSchema": [
                {
                    "visible": False,
                    "name": "Quoted.Domain",
                    "schema": "SomeSchema",
                    "nullable": True,
                    "type": "integer",
                    "default": "0",
                    "constraints": [],
                    "collation": None,
                }
            ],
        }

    def test_inspect_domains(self, connection, all_domains):
        inspector = inspect(connection)
        domains = inspector.get_domains()

        domain_names = {d["name"] for d in domains}
        expect_domain_names = {d["name"] for d in all_domains["public"]}
        eq_(domain_names, expect_domain_names)

        eq_(domains, all_domains["public"])

    def test_inspect_domains_schema(self, connection, all_domains):
        inspector = inspect(connection)
        eq_(
            inspector.get_domains("test_schema"),
            all_domains["test_schema"],
        )
        eq_(inspector.get_domains("SomeSchema"), all_domains["SomeSchema"])

    def test_inspect_domains_star(self, connection, all_domains):
        inspector = inspect(connection)
        all_ = [d for dl in all_domains.values() for d in dl]
        all_ += inspector.get_domains("information_schema")
        exp = sorted(all_, key=lambda d: (d["schema"], d["name"]))
        domains = inspector.get_domains("*")

        eq_(domains, exp)


class ArrayReflectionTest(fixtures.TablesTest):
    __only_on__ = "postgresql >= 10"
    __sparse_driver_backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "array_table",
            metadata,
            Column("id", INTEGER, primary_key=True),
            Column("datas", ARRAY(INTEGER)),
            Column("datass", ARRAY(INTEGER, dimensions=2)),
            Column("datasss", ARRAY(INTEGER, dimensions=3)),
        )

    def test_array_table_is_reflected(self, connection):
        metadata = MetaData()
        table = Table("array_table", metadata, autoload_with=connection)

        def assert_is_integer_array(data_type):
            assert isinstance(data_type, ARRAY)
            # posgres treats all arrays as one-dimensional arrays
            assert isinstance(data_type.item_type, INTEGER)

        assert_is_integer_array(table.c.datas.type)
        assert_is_integer_array(table.c.datass.type)
        assert_is_integer_array(table.c.datasss.type)


class ReflectionTest(
    ReflectionFixtures, AssertsCompiledSQL, ComparesIndexes, fixtures.TestBase
):
    __only_on__ = "postgresql"
    __sparse_driver_backend__ = True

    def test_reflected_primary_key_order(self, metadata, connection):
        meta1 = metadata
        subject = Table(
            "subject",
            meta1,
            Column("p1", Integer, primary_key=True),
            Column("p2", Integer, primary_key=True),
            PrimaryKeyConstraint("p2", "p1"),
        )
        meta1.create_all(connection)
        meta2 = MetaData()
        subject = Table("subject", meta2, autoload_with=connection)
        eq_(subject.primary_key.columns.keys(), ["p2", "p1"])

    @testing.skip_if(
        "postgresql < 15.0", "on delete with column list not supported"
    )
    def test_reflected_foreign_key_ondelete_column_list(
        self, metadata, connection
    ):
        meta1 = metadata
        pktable = Table(
            "pktable",
            meta1,
            Column("tid", Integer, primary_key=True),
            Column("id", Integer, primary_key=True),
        )
        Table(
            "fktable",
            meta1,
            Column("tid", Integer),
            Column("id", Integer),
            Column("fk_id_del_set_null", Integer),
            Column("fk_id_del_set_default", Integer, server_default=text("0")),
            ForeignKeyConstraint(
                name="fktable_tid_fk_id_del_set_null_fkey",
                columns=["tid", "fk_id_del_set_null"],
                refcolumns=[pktable.c.tid, pktable.c.id],
                ondelete="SET NULL (fk_id_del_set_null)",
            ),
            ForeignKeyConstraint(
                name="fktable_tid_fk_id_del_set_default_fkey",
                columns=["tid", "fk_id_del_set_default"],
                refcolumns=[pktable.c.tid, pktable.c.id],
                ondelete="SET DEFAULT(fk_id_del_set_default)",
            ),
        )

        meta1.create_all(connection)
        meta2 = MetaData()
        fktable = Table("fktable", meta2, autoload_with=connection)
        fkey_set_null = next(
            c
            for c in fktable.foreign_key_constraints
            if c.name == "fktable_tid_fk_id_del_set_null_fkey"
        )
        eq_(fkey_set_null.ondelete, "SET NULL (fk_id_del_set_null)")
        fkey_set_default = next(
            c
            for c in fktable.foreign_key_constraints
            if c.name == "fktable_tid_fk_id_del_set_default_fkey"
        )
        eq_(fkey_set_default.ondelete, "SET DEFAULT (fk_id_del_set_default)")

    def test_pg_weirdchar_reflection(self, metadata, connection):
        meta1 = metadata
        subject = Table(
            "subject", meta1, Column("id$", Integer, primary_key=True)
        )
        referer = Table(
            "referer",
            meta1,
            Column("id", Integer, primary_key=True),
            Column("ref", Integer, ForeignKey("subject.id$")),
        )
        meta1.create_all(connection)
        meta2 = MetaData()
        subject = Table("subject", meta2, autoload_with=connection)
        referer = Table("referer", meta2, autoload_with=connection)
        self.assert_(
            (subject.c["id$"] == referer.c.ref).compare(
                subject.join(referer).onclause
            )
        )

    def test_reflect_default_over_128_chars(self, metadata, connection):
        Table(
            "t",
            metadata,
            Column("x", String(200), server_default="abcd" * 40),
        ).create(connection)

        m = MetaData()
        t = Table("t", m, autoload_with=connection)
        eq_(
            t.c.x.server_default.arg.text,
            "'%s'::character varying" % ("abcd" * 40),
        )

    def test_renamed_sequence_reflection(self, metadata, connection):
        Table("t", metadata, Column("id", Integer, primary_key=True))
        metadata.create_all(connection)
        m2 = MetaData()
        t2 = Table("t", m2, autoload_with=connection, implicit_returning=False)
        eq_(t2.c.id.server_default.arg.text, "nextval('t_id_seq'::regclass)")
        r = connection.execute(t2.insert())
        eq_(r.inserted_primary_key, (1,))

        connection.exec_driver_sql(
            "alter table t_id_seq rename to foobar_id_seq"
        )
        m3 = MetaData()
        t3 = Table("t", m3, autoload_with=connection, implicit_returning=False)
        eq_(
            t3.c.id.server_default.arg.text,
            "nextval('foobar_id_seq'::regclass)",
        )
        r = connection.execute(t3.insert())
        eq_(r.inserted_primary_key, (2,))

    def test_altered_type_autoincrement_pk_reflection(
        self, metadata, connection
    ):
        metadata = metadata
        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        metadata.create_all(connection)

        connection.exec_driver_sql(
            "alter table t alter column id type varchar(50)"
        )
        m2 = MetaData()
        t2 = Table("t", m2, autoload_with=connection)
        eq_(t2.c.id.autoincrement, False)
        eq_(t2.c.x.autoincrement, False)

    def test_renamed_pk_reflection(self, metadata, connection):
        metadata = metadata
        Table("t", metadata, Column("id", Integer, primary_key=True))
        metadata.create_all(connection)
        connection.exec_driver_sql("alter table t rename id to t_id")
        m2 = MetaData()
        t2 = Table("t", m2, autoload_with=connection)
        eq_([c.name for c in t2.primary_key], ["t_id"])

    def test_has_temporary_table(self, metadata, connection):
        assert not inspect(connection).has_table("some_temp_table")
        user_tmp = Table(
            "some_temp_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(50)),
            prefixes=["TEMPORARY"],
        )
        user_tmp.create(connection)
        assert inspect(connection).has_table("some_temp_table")

    def test_cross_schema_reflection_one(self, metadata, connection):
        meta1 = metadata

        users = Table(
            "users",
            meta1,
            Column("user_id", Integer, primary_key=True),
            Column("user_name", String(30), nullable=False),
            schema="test_schema",
        )
        addresses = Table(
            "email_addresses",
            meta1,
            Column("address_id", Integer, primary_key=True),
            Column("remote_user_id", Integer, ForeignKey(users.c.user_id)),
            Column("email_address", String(20)),
            schema="test_schema",
        )
        meta1.create_all(connection)
        meta2 = MetaData()
        addresses = Table(
            "email_addresses",
            meta2,
            autoload_with=connection,
            schema="test_schema",
        )
        users = Table("users", meta2, must_exist=True, schema="test_schema")
        j = join(users, addresses)
        self.assert_(
            (users.c.user_id == addresses.c.remote_user_id).compare(j.onclause)
        )

    def test_cross_schema_reflection_two(self, metadata, connection):
        meta1 = metadata
        subject = Table(
            "subject", meta1, Column("id", Integer, primary_key=True)
        )
        referer = Table(
            "referer",
            meta1,
            Column("id", Integer, primary_key=True),
            Column("ref", Integer, ForeignKey("subject.id")),
            schema="test_schema",
        )
        meta1.create_all(connection)
        meta2 = MetaData()
        subject = Table("subject", meta2, autoload_with=connection)
        referer = Table(
            "referer", meta2, schema="test_schema", autoload_with=connection
        )
        self.assert_(
            (subject.c.id == referer.c.ref).compare(
                subject.join(referer).onclause
            )
        )

    def test_cross_schema_reflection_three(self, metadata, connection):
        meta1 = metadata
        subject = Table(
            "subject",
            meta1,
            Column("id", Integer, primary_key=True),
            schema="test_schema_2",
        )
        referer = Table(
            "referer",
            meta1,
            Column("id", Integer, primary_key=True),
            Column("ref", Integer, ForeignKey("test_schema_2.subject.id")),
            schema="test_schema",
        )
        meta1.create_all(connection)
        meta2 = MetaData()
        subject = Table(
            "subject", meta2, autoload_with=connection, schema="test_schema_2"
        )
        referer = Table(
            "referer", meta2, autoload_with=connection, schema="test_schema"
        )
        self.assert_(
            (subject.c.id == referer.c.ref).compare(
                subject.join(referer).onclause
            )
        )

    def test_cross_schema_reflection_four(self, metadata, connection):
        meta1 = metadata
        subject = Table(
            "subject",
            meta1,
            Column("id", Integer, primary_key=True),
            schema="test_schema_2",
        )
        referer = Table(
            "referer",
            meta1,
            Column("id", Integer, primary_key=True),
            Column("ref", Integer, ForeignKey("test_schema_2.subject.id")),
            schema="test_schema",
        )
        meta1.create_all(connection)

        connection.detach()
        connection.exec_driver_sql(
            "SET search_path TO test_schema, test_schema_2"
        )
        meta2 = MetaData()
        subject = Table(
            "subject",
            meta2,
            autoload_with=connection,
            schema="test_schema_2",
            postgresql_ignore_search_path=True,
        )
        referer = Table(
            "referer",
            meta2,
            autoload_with=connection,
            schema="test_schema",
            postgresql_ignore_search_path=True,
        )
        self.assert_(
            (subject.c.id == referer.c.ref).compare(
                subject.join(referer).onclause
            )
        )

    def test_cross_schema_reflection_five(self, metadata, connection):
        meta1 = metadata

        # we assume 'public'
        default_schema = connection.dialect.default_schema_name
        subject = Table(
            "subject", meta1, Column("id", Integer, primary_key=True)
        )
        referer = Table(
            "referer",
            meta1,
            Column("id", Integer, primary_key=True),
            Column("ref", Integer, ForeignKey("subject.id")),
        )
        meta1.create_all(connection)

        meta2 = MetaData()
        subject = Table(
            "subject",
            meta2,
            autoload_with=connection,
            schema=default_schema,
            postgresql_ignore_search_path=True,
        )
        referer = Table(
            "referer",
            meta2,
            autoload_with=connection,
            schema=default_schema,
            postgresql_ignore_search_path=True,
        )
        assert subject.schema == default_schema
        self.assert_(
            (subject.c.id == referer.c.ref).compare(
                subject.join(referer).onclause
            )
        )

    def test_cross_schema_reflection_six(self, metadata, connection):
        # test that the search path *is* taken into account
        # by default
        meta1 = metadata

        Table(
            "some_table",
            meta1,
            Column("id", Integer, primary_key=True),
            schema="test_schema",
        )
        Table(
            "some_other_table",
            meta1,
            Column("id", Integer, primary_key=True),
            Column("sid", Integer, ForeignKey("test_schema.some_table.id")),
            schema="test_schema_2",
        )
        meta1.create_all(connection)
        connection.detach()

        connection.exec_driver_sql(
            "set search_path to test_schema_2, test_schema, public"
        )

        m1 = MetaData()

        Table("some_table", m1, schema="test_schema", autoload_with=connection)
        t2_schema = Table(
            "some_other_table",
            m1,
            schema="test_schema_2",
            autoload_with=connection,
        )

        t2_no_schema = Table("some_other_table", m1, autoload_with=connection)

        t1_no_schema = Table("some_table", m1, autoload_with=connection)

        m2 = MetaData()
        t1_schema_isp = Table(
            "some_table",
            m2,
            schema="test_schema",
            autoload_with=connection,
            postgresql_ignore_search_path=True,
        )
        t2_schema_isp = Table(
            "some_other_table",
            m2,
            schema="test_schema_2",
            autoload_with=connection,
            postgresql_ignore_search_path=True,
        )

        # t2_schema refers to t1_schema, but since "test_schema"
        # is in the search path, we instead link to t2_no_schema
        assert t2_schema.c.sid.references(t1_no_schema.c.id)

        # the two no_schema tables refer to each other also.
        assert t2_no_schema.c.sid.references(t1_no_schema.c.id)

        # but if we're ignoring search path, then we maintain
        # those explicit schemas vs. what the "default" schema is
        assert t2_schema_isp.c.sid.references(t1_schema_isp.c.id)

    def test_cross_schema_reflection_seven(self, metadata, connection):
        # test that the search path *is* taken into account
        # by default
        meta1 = metadata

        Table(
            "some_table",
            meta1,
            Column("id", Integer, primary_key=True),
            schema="test_schema",
        )
        Table(
            "some_other_table",
            meta1,
            Column("id", Integer, primary_key=True),
            Column("sid", Integer, ForeignKey("test_schema.some_table.id")),
            schema="test_schema_2",
        )
        meta1.create_all(connection)
        connection.detach()

        connection.exec_driver_sql(
            "set search_path to test_schema_2, test_schema, public"
        )
        meta2 = MetaData()
        meta2.reflect(connection, schema="test_schema_2")

        eq_(
            set(meta2.tables),
            {"test_schema_2.some_other_table", "some_table"},
        )

        meta3 = MetaData()
        meta3.reflect(
            connection,
            schema="test_schema_2",
            postgresql_ignore_search_path=True,
        )

        eq_(
            set(meta3.tables),
            {
                "test_schema_2.some_other_table",
                "test_schema.some_table",
            },
        )

    def test_cross_schema_reflection_metadata_uses_schema(
        self, metadata, connection
    ):
        # test [ticket:3716]

        Table(
            "some_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("sid", Integer, ForeignKey("some_other_table.id")),
            schema="test_schema",
        )
        Table(
            "some_other_table",
            metadata,
            Column("id", Integer, primary_key=True),
            schema=None,
        )
        metadata.create_all(connection)
        meta2 = MetaData(schema="test_schema")
        meta2.reflect(connection)

        eq_(
            set(meta2.tables),
            {"some_other_table", "test_schema.some_table"},
        )

    def test_uppercase_lowercase_table(self, metadata, connection):
        a_table = Table("a", metadata, Column("x", Integer))
        A_table = Table("A", metadata, Column("x", Integer))

        a_table.create(connection)
        assert inspect(connection).has_table("a")
        assert not inspect(connection).has_table("A")
        A_table.create(connection, checkfirst=True)
        assert inspect(connection).has_table("A")

    def test_uppercase_lowercase_sequence(self, connection):
        a_seq = Sequence("a")
        A_seq = Sequence("A")

        a_seq.create(connection)
        assert connection.dialect.has_sequence(connection, "a")
        assert not connection.dialect.has_sequence(connection, "A")
        A_seq.create(connection, checkfirst=True)
        assert connection.dialect.has_sequence(connection, "A")

        a_seq.drop(connection)
        A_seq.drop(connection)

    def test_index_reflection(self, metadata, connection):
        """Reflecting expression-based indexes works"""

        Table(
            "party",
            metadata,
            Column("id", String(10), nullable=False),
            Column("name", String(20), index=True),
            Column("aname", String(20)),
            Column("other", String(20)),
        )
        metadata.create_all(connection)
        connection.exec_driver_sql(
            """
            create index idx3 on party
                (lower(name::text), other, lower(aname::text) desc)
            """
        )
        connection.exec_driver_sql(
            "create index idx1 on party ((id || name), (other || id::text))"
        )
        connection.exec_driver_sql(
            "create unique index idx2 on party (id) where name = 'test'"
        )
        connection.exec_driver_sql(
            """
            create index idx4 on party using btree
                (name nulls first, lower(other), aname desc)
                where name != 'foo'
            """
        )
        version = connection.dialect.server_version_info
        if version >= (15,):
            connection.exec_driver_sql(
                """
                create unique index zz_idx5 on party
                    (name desc, upper(other))
                    nulls not distinct
                """
            )

        expected = [
            {
                "name": "idx1",
                "column_names": [None, None],
                "expressions": [
                    "(id::text || name::text)",
                    "(other::text || id::text)",
                ],
                "unique": False,
                "include_columns": [],
                "dialect_options": {"postgresql_include": []},
            },
            {
                "name": "idx2",
                "column_names": ["id"],
                "unique": True,
                "include_columns": [],
                "dialect_options": {
                    "postgresql_include": [],
                    "postgresql_where": "((name)::text = 'test'::text)",
                },
            },
            {
                "name": "idx3",
                "column_names": [None, "other", None],
                "expressions": [
                    "lower(name::text)",
                    "other",
                    "lower(aname::text)",
                ],
                "unique": False,
                "include_columns": [],
                "dialect_options": {"postgresql_include": []},
                "column_sorting": {"lower(aname::text)": ("desc",)},
            },
            {
                "name": "idx4",
                "column_names": ["name", None, "aname"],
                "expressions": ["name", "lower(other::text)", "aname"],
                "unique": False,
                "include_columns": [],
                "dialect_options": {
                    "postgresql_include": [],
                    "postgresql_where": "((name)::text <> 'foo'::text)",
                },
                "column_sorting": {
                    "aname": ("desc",),
                    "name": ("nulls_first",),
                },
            },
            {
                "name": "ix_party_name",
                "column_names": ["name"],
                "unique": False,
                "include_columns": [],
                "dialect_options": {"postgresql_include": []},
            },
        ]
        if version > (15,):
            expected.append(
                {
                    "name": "zz_idx5",
                    "column_names": ["name", None],
                    "expressions": ["name", "upper(other::text)"],
                    "unique": True,
                    "include_columns": [],
                    "dialect_options": {
                        "postgresql_include": [],
                        "postgresql_nulls_not_distinct": True,
                    },
                    "column_sorting": {"name": ("desc",)},
                },
            )

        if version < (11,):
            for index in expected:
                index.pop("include_columns")
                index["dialect_options"].pop("postgresql_include")
                if not index["dialect_options"]:
                    index.pop("dialect_options")

        insp = inspect(connection)
        eq_(insp.get_indexes("party"), expected)

        m2 = MetaData()
        t2 = Table("party", m2, autoload_with=connection)
        self.compare_table_index_with_expected(t2, expected, "postgresql")

    def test_index_reflection_partial(self, metadata, connection):
        """Reflect the filter definition on partial indexes"""

        metadata = metadata

        t1 = Table(
            "table1",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(20)),
            Column("x", Integer),
        )
        Index("idx1", t1.c.id, postgresql_where=t1.c.name == "test")
        Index("idx2", t1.c.id, postgresql_where=t1.c.x >= 5)

        metadata.create_all(connection)

        ind = connection.dialect.get_indexes(connection, t1.name, None)

        partial_definitions = []
        for ix in ind:
            if "dialect_options" in ix:
                partial_definitions.append(
                    ix["dialect_options"]["postgresql_where"]
                )

        eq_(
            sorted(partial_definitions),
            ["((name)::text = 'test'::text)", "(x >= 5)"],
        )

        t2 = Table("table1", MetaData(), autoload_with=connection)
        idx = list(sorted(t2.indexes, key=lambda idx: idx.name))[0]

        self.assert_compile(
            CreateIndex(idx),
            "CREATE INDEX idx1 ON table1 (id) "
            "WHERE ((name)::text = 'test'::text)",
        )

    def test_index_reflection_with_sorting(self, metadata, connection):
        """reflect indexes with sorting options set"""

        t1 = Table(
            "party",
            metadata,
            Column("id", String(10), nullable=False),
            Column("name", String(20)),
            Column("aname", String(20)),
        )

        t1.create(connection)

        # check ASC, DESC options alone
        connection.exec_driver_sql(
            """
            create index idx1 on party
                (id, name ASC, aname DESC)
        """
        )

        # check DESC w/ NULLS options
        connection.exec_driver_sql(
            """
          create index idx2 on party
                (name DESC NULLS FIRST, aname DESC NULLS LAST)
        """
        )

        # check ASC w/ NULLS options
        connection.exec_driver_sql(
            """
          create index idx3 on party
                (name ASC NULLS FIRST, aname ASC NULLS LAST)
        """
        )

        # reflect data
        m2 = MetaData()
        t2 = Table("party", m2, autoload_with=connection)

        eq_(len(t2.indexes), 3)

        # Make sure indexes are in the order we expect them in
        r1, r2, r3 = sorted(t2.indexes, key=lambda idx: idx.name)

        eq_(r1.name, "idx1")
        eq_(r2.name, "idx2")
        eq_(r3.name, "idx3")

        # "ASC NULLS LAST" is implicit default for indexes,
        # and "NULLS FIRST" is implicit default for "DESC".
        # (https://www.postgresql.org/docs/current/indexes-ordering.html)

        def compile_exprs(exprs):
            return list(map(str, exprs))

        eq_(
            compile_exprs([t2.c.id, t2.c.name, t2.c.aname.desc()]),
            compile_exprs(r1.expressions),
        )

        eq_(
            compile_exprs([t2.c.name.desc(), t2.c.aname.desc().nulls_last()]),
            compile_exprs(r2.expressions),
        )

        eq_(
            compile_exprs([t2.c.name.nulls_first(), t2.c.aname]),
            compile_exprs(r3.expressions),
        )

    def test_index_reflection_modified(self, metadata, connection):
        """reflect indexes when a column name has changed - PG 9
        does not update the name of the column in the index def.
        [ticket:2141]

        """

        metadata = metadata

        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        metadata.create_all(connection)
        connection.exec_driver_sql("CREATE INDEX idx1 ON t (x)")
        connection.exec_driver_sql("ALTER TABLE t RENAME COLUMN x to y")

        ind = connection.dialect.get_indexes(connection, "t", None)
        expected = [{"name": "idx1", "unique": False, "column_names": ["y"]}]
        if testing.requires.index_reflects_included_columns.enabled:
            expected[0]["include_columns"] = []
            expected[0]["dialect_options"] = {"postgresql_include": []}

        eq_(ind, expected)

    def test_index_reflection_with_storage_options(self, metadata, connection):
        """reflect indexes with storage options set"""

        metadata = metadata

        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
        )
        metadata.create_all(connection)

        connection.exec_driver_sql(
            "CREATE INDEX idx1 ON t (x) WITH (fillfactor = 50)"
        )

        ind = testing.db.dialect.get_indexes(connection, "t", None)

        expected = [
            {
                "unique": False,
                "column_names": ["x"],
                "name": "idx1",
                "dialect_options": {"postgresql_with": {"fillfactor": "50"}},
            }
        ]
        if testing.requires.index_reflects_included_columns.enabled:
            expected[0]["include_columns"] = []
            expected[0]["dialect_options"]["postgresql_include"] = []
        eq_(ind, expected)

        m = MetaData()
        t1 = Table("t", m, autoload_with=connection)
        eq_(
            list(t1.indexes)[0].dialect_options["postgresql"]["with"],
            {"fillfactor": "50"},
        )

    def test_index_reflection_with_access_method(self, metadata, connection):
        """reflect indexes with storage options set"""

        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", ARRAY(Integer)),
        )
        metadata.create_all(connection)
        connection.exec_driver_sql("CREATE INDEX idx1 ON t USING gin (x)")

        ind = testing.db.dialect.get_indexes(connection, "t", None)
        expected = [
            {
                "unique": False,
                "column_names": ["x"],
                "name": "idx1",
                "dialect_options": {"postgresql_using": "gin"},
            }
        ]
        if testing.requires.index_reflects_included_columns.enabled:
            expected[0]["include_columns"] = []
            expected[0]["dialect_options"]["postgresql_include"] = []
        eq_(ind, expected)
        m = MetaData()
        t1 = Table("t", m, autoload_with=connection)
        eq_(
            list(t1.indexes)[0].dialect_options["postgresql"]["using"],
            "gin",
        )

    def test_index_reflection_with_operator_class(self, metadata, connection):
        """reflect indexes with operator class on columns"""

        Table(
            "t",
            metadata,
            Column("id", Integer, nullable=False),
            Column("name", String),
            Column("alias", String),
            Column("addr1", INET),
            Column("addr2", INET),
        )
        metadata.create_all(connection)

        # 'name' and 'addr1' use a non-default operator, 'addr2' uses the
        # default one, and 'alias' uses no operator.
        connection.exec_driver_sql(
            "CREATE INDEX ix_t ON t USING btree"
            " (name text_pattern_ops, alias, addr1 cidr_ops, addr2 inet_ops)"
        )

        ind = inspect(connection).get_indexes("t", None)
        expected = [
            {
                "unique": False,
                "column_names": ["name", "alias", "addr1", "addr2"],
                "name": "ix_t",
                "dialect_options": {
                    "postgresql_ops": {
                        "addr1": "cidr_ops",
                        "name": "text_pattern_ops",
                    },
                },
            }
        ]
        if connection.dialect.server_version_info >= (11, 0):
            expected[0]["include_columns"] = []
            expected[0]["dialect_options"]["postgresql_include"] = []
        eq_(ind, expected)

        m = MetaData()
        t1 = Table("t", m, autoload_with=connection)
        r_ind = list(t1.indexes)[0]
        eq_(
            r_ind.dialect_options["postgresql"]["ops"],
            {"name": "text_pattern_ops", "addr1": "cidr_ops"},
        )

    @testing.skip_if("postgresql < 15.0", "nullsnotdistinct not supported")
    def test_nullsnotdistinct(self, metadata, connection):
        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", ARRAY(Integer)),
            Column("y", ARRAY(Integer)),
            Index(
                "idx1", "x", unique=True, postgresql_nulls_not_distinct=True
            ),
            UniqueConstraint(
                "y", name="unq1", postgresql_nulls_not_distinct=True
            ),
        )
        metadata.create_all(connection)

        ind = inspect(connection).get_indexes("t", None)
        expected_ind = [
            {
                "unique": True,
                "column_names": ["x"],
                "name": "idx1",
                "dialect_options": {
                    "postgresql_nulls_not_distinct": True,
                    "postgresql_include": [],
                },
                "include_columns": [],
            },
            {
                "unique": True,
                "column_names": ["y"],
                "name": "unq1",
                "dialect_options": {
                    "postgresql_nulls_not_distinct": True,
                    "postgresql_include": [],
                },
                "include_columns": [],
                "duplicates_constraint": "unq1",
            },
        ]
        eq_(ind, expected_ind)

        unq = inspect(connection).get_unique_constraints("t", None)
        expected_unq = [
            {
                "column_names": ["y"],
                "name": "unq1",
                "dialect_options": {
                    "postgresql_include": [],
                    "postgresql_nulls_not_distinct": True,
                },
                "comment": None,
            }
        ]
        eq_(unq, expected_unq)

        m = MetaData()
        t1 = Table("t", m, autoload_with=connection)
        eq_(len(t1.indexes), 1)
        idx_options = list(t1.indexes)[0].dialect_options["postgresql"]
        eq_(idx_options["nulls_not_distinct"], True)

        cst = {c.name: c for c in t1.constraints}
        cst_options = cst["unq1"].dialect_options["postgresql"]
        eq_(cst_options["nulls_not_distinct"], True)

    @testing.skip_if("postgresql < 11.0", "indnkeyatts not supported")
    def test_index_reflection_with_include(self, metadata, connection):
        """reflect indexes with include set"""

        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", ARRAY(Integer)),
            Column("name", String(20)),
            Column("aname", String(20)),
            Column("other", Text()),
        )
        metadata.create_all(connection)
        connection.exec_driver_sql("CREATE INDEX idx1 ON t (x) INCLUDE (name)")
        connection.exec_driver_sql(
            """
            create index idx3 on t
                (lower(name::text), other desc nulls last, lower(aname::text))
                include (id, x)
            """
        )
        connection.exec_driver_sql(
            """
            create unique index idx2 on t using btree
                (lower(other), (id * id)) include (id)
            """
        )

        ind = connection.dialect.get_indexes(connection, "t", None)
        eq_(
            ind,
            [
                {
                    "unique": False,
                    "column_names": ["x"],
                    "include_columns": ["name"],
                    "dialect_options": {"postgresql_include": ["name"]},
                    "name": "idx1",
                },
                {
                    "name": "idx2",
                    "column_names": [None, None],
                    "expressions": ["lower(other)", "(id * id)"],
                    "unique": True,
                    "include_columns": ["id"],
                    "dialect_options": {"postgresql_include": ["id"]},
                },
                {
                    "name": "idx3",
                    "column_names": [None, "other", None],
                    "expressions": [
                        "lower(name::text)",
                        "other",
                        "lower(aname::text)",
                    ],
                    "unique": False,
                    "include_columns": ["id", "x"],
                    "dialect_options": {"postgresql_include": ["id", "x"]},
                    "column_sorting": {
                        "other": ("desc", "nulls_last"),
                    },
                },
            ],
        )

    def test_foreign_key_option_inspection(self, metadata, connection):
        Table(
            "person",
            metadata,
            Column("id", String(length=32), nullable=False, primary_key=True),
            Column(
                "company_id",
                ForeignKey(
                    "company.id",
                    name="person_company_id_fkey",
                    match="FULL",
                    onupdate="RESTRICT",
                    ondelete="RESTRICT",
                    deferrable=True,
                    initially="DEFERRED",
                ),
            ),
        )
        Table(
            "company",
            metadata,
            Column("id", String(length=32), nullable=False, primary_key=True),
            Column("name", String(length=255)),
            Column(
                "industry_id",
                ForeignKey(
                    "industry.id",
                    name="company_industry_id_fkey",
                    onupdate="CASCADE",
                    ondelete="CASCADE",
                    deferrable=False,  # PG default
                    # PG default
                    initially="IMMEDIATE",
                ),
            ),
        )
        Table(
            "industry",
            metadata,
            Column("id", Integer(), nullable=False, primary_key=True),
            Column("name", String(length=255)),
        )
        fk_ref = {
            "person_company_id_fkey": {
                "name": "person_company_id_fkey",
                "constrained_columns": ["company_id"],
                "referred_columns": ["id"],
                "referred_table": "company",
                "referred_schema": None,
                "options": {
                    "onupdate": "RESTRICT",
                    "deferrable": True,
                    "ondelete": "RESTRICT",
                    "initially": "DEFERRED",
                    "match": "FULL",
                },
                "comment": None,
            },
            "company_industry_id_fkey": {
                "name": "company_industry_id_fkey",
                "constrained_columns": ["industry_id"],
                "referred_columns": ["id"],
                "referred_table": "industry",
                "referred_schema": None,
                "options": {"onupdate": "CASCADE", "ondelete": "CASCADE"},
                "comment": None,
            },
        }
        metadata.create_all(connection)
        inspector = inspect(connection)
        fks = inspector.get_foreign_keys(
            "person"
        ) + inspector.get_foreign_keys("company")
        for fk in fks:
            eq_(fk, fk_ref[fk["name"]])

    def test_inspect_enums_schema(self, metadata, connection):
        enum_type = postgresql.ENUM(
            "sad",
            "ok",
            "happy",
            name="mood",
            schema="test_schema",
            metadata=metadata,
        )
        enum_type.create(connection)
        inspector = inspect(connection)
        eq_(
            inspector.get_enums("test_schema"),
            [
                {
                    "visible": False,
                    "name": "mood",
                    "schema": "test_schema",
                    "labels": ["sad", "ok", "happy"],
                }
            ],
        )
        is_true(inspector.has_type("mood", "test_schema"))
        is_true(inspector.has_type("mood", "*"))
        is_false(inspector.has_type("mood"))

    def test_inspect_enums(self, metadata, inspect_fixture):
        inspector, conn = inspect_fixture

        enum_type = postgresql.ENUM(
            "cat", "dog", "rat", name="pet", metadata=metadata
        )
        enum_type.create(conn)
        conn.commit()

        res = [
            {
                "visible": True,
                "labels": ["cat", "dog", "rat"],
                "name": "pet",
                "schema": "public",
            }
        ]
        eq_(inspector.get_enums(), res)
        is_true(inspector.has_type("pet", "*"))
        is_true(inspector.has_type("pet"))
        is_false(inspector.has_type("pet", "test_schema"))

        enum_type.drop(conn)
        conn.commit()
        eq_(inspector.get_enums(), res)
        is_true(inspector.has_type("pet"))
        inspector.clear_cache()
        eq_(inspector.get_enums(), [])
        is_false(inspector.has_type("pet"))

    def test_get_table_oid(self, metadata, connection):
        Table("t1", metadata, Column("col", Integer))
        Table("t1", metadata, Column("col", Integer), schema="test_schema")
        metadata.create_all(connection)
        insp = inspect(connection)
        oid = insp.get_table_oid("t1")
        oid_schema = insp.get_table_oid("t1", schema="test_schema")
        is_true(isinstance(oid, int))
        is_true(isinstance(oid_schema, int))
        is_true(oid != oid_schema)

        with expect_raises(exc.NoSuchTableError):
            insp.get_table_oid("does_not_exist")

        metadata.tables["t1"].drop(connection)
        eq_(insp.get_table_oid("t1"), oid)
        insp.clear_cache()
        with expect_raises(exc.NoSuchTableError):
            insp.get_table_oid("t1")

    def test_inspect_enums_case_sensitive(self, metadata, connection):
        sa.event.listen(
            metadata,
            "before_create",
            sa.DDL('create schema "TestSchema"'),
        )
        sa.event.listen(
            metadata,
            "after_drop",
            sa.DDL('drop schema if exists "TestSchema" cascade'),
        )

        for enum in "lower_case", "UpperCase", "Name.With.Dot":
            for schema in None, "test_schema", "TestSchema":
                postgresql.ENUM(
                    "CapsOne",
                    "CapsTwo",
                    name=enum,
                    schema=schema,
                    metadata=metadata,
                )

        metadata.create_all(connection)
        inspector = inspect(connection)
        for schema in None, "test_schema", "TestSchema":
            eq_(
                sorted(
                    inspector.get_enums(schema=schema), key=itemgetter("name")
                ),
                [
                    {
                        "visible": schema is None,
                        "labels": ["CapsOne", "CapsTwo"],
                        "name": "Name.With.Dot",
                        "schema": "public" if schema is None else schema,
                    },
                    {
                        "visible": schema is None,
                        "labels": ["CapsOne", "CapsTwo"],
                        "name": "UpperCase",
                        "schema": "public" if schema is None else schema,
                    },
                    {
                        "visible": schema is None,
                        "labels": ["CapsOne", "CapsTwo"],
                        "name": "lower_case",
                        "schema": "public" if schema is None else schema,
                    },
                ],
            )

    def test_inspect_enums_case_sensitive_from_table(
        self, metadata, connection
    ):
        sa.event.listen(
            metadata,
            "before_create",
            sa.DDL('create schema "TestSchema"'),
        )
        sa.event.listen(
            metadata,
            "after_drop",
            sa.DDL('drop schema if exists "TestSchema" cascade'),
        )

        counter = itertools.count()
        for enum in "lower_case", "UpperCase", "Name.With.Dot":
            for schema in None, "test_schema", "TestSchema":
                enum_type = postgresql.ENUM(
                    "CapsOne",
                    "CapsTwo",
                    name=enum,
                    metadata=metadata,
                    schema=schema,
                )

                Table(
                    "t%d" % next(counter),
                    metadata,
                    Column("q", enum_type),
                )

        metadata.create_all(connection)

        inspector = inspect(connection)
        counter = itertools.count()
        for enum in "lower_case", "UpperCase", "Name.With.Dot":
            for schema in None, "test_schema", "TestSchema":
                cols = inspector.get_columns("t%d" % next(counter))
                cols[0]["type"] = (
                    cols[0]["type"].schema,
                    cols[0]["type"].name,
                    cols[0]["type"].enums,
                )
                eq_(
                    cols,
                    [
                        {
                            "name": "q",
                            "type": (schema, enum, ["CapsOne", "CapsTwo"]),
                            "nullable": True,
                            "default": None,
                            "autoincrement": False,
                            "comment": None,
                        }
                    ],
                )

    def test_inspect_enums_star(self, metadata, connection):
        enum_type = postgresql.ENUM(
            "cat", "dog", "rat", name="pet", metadata=metadata
        )
        schema_enum_type = postgresql.ENUM(
            "sad",
            "ok",
            "happy",
            name="mood",
            schema="test_schema",
            metadata=metadata,
        )
        enum_type.create(connection)
        schema_enum_type.create(connection)
        inspector = inspect(connection)

        eq_(
            inspector.get_enums(),
            [
                {
                    "visible": True,
                    "labels": ["cat", "dog", "rat"],
                    "name": "pet",
                    "schema": "public",
                }
            ],
        )

        eq_(
            inspector.get_enums("*"),
            [
                {
                    "visible": True,
                    "labels": ["cat", "dog", "rat"],
                    "name": "pet",
                    "schema": "public",
                },
                {
                    "visible": False,
                    "name": "mood",
                    "schema": "test_schema",
                    "labels": ["sad", "ok", "happy"],
                },
            ],
        )

    def test_inspect_enum_empty(self, metadata, connection):
        enum_type = postgresql.ENUM(name="empty", metadata=metadata)
        enum_type.create(connection)
        inspector = inspect(connection)

        eq_(
            inspector.get_enums(),
            [
                {
                    "visible": True,
                    "labels": [],
                    "name": "empty",
                    "schema": "public",
                }
            ],
        )

    def test_inspect_enum_empty_from_table(self, metadata, connection):
        Table(
            "t", metadata, Column("x", postgresql.ENUM(name="empty"))
        ).create(connection)

        t = Table("t", MetaData(), autoload_with=connection)
        eq_(t.c.x.type.enums, [])

    def test_enum_starts_with_interval(self, metadata, connection):
        """Test for #12744"""
        enum_type = postgresql.ENUM("day", "week", name="intervalunit")
        t1 = Table("t1", metadata, Column("col", enum_type))
        t1.create(connection)

        insp = inspect(connection)
        cols = insp.get_columns("t1")
        is_true(isinstance(cols[0]["type"], postgresql.ENUM))
        eq_(cols[0]["type"].enums, ["day", "week"])

    def test_reflection_with_unique_constraint(self, metadata, connection):
        insp = inspect(connection)

        meta = metadata
        uc_table = Table(
            "pgsql_uc",
            meta,
            Column("a", String(10)),
            UniqueConstraint("a", name="uc_a"),
        )

        uc_table.create(connection)

        # PostgreSQL will create an implicit index for a unique
        # constraint.   Separately we get both
        indexes = {i["name"] for i in insp.get_indexes("pgsql_uc")}
        constraints = {
            i["name"] for i in insp.get_unique_constraints("pgsql_uc")
        }

        self.assert_("uc_a" in indexes)
        self.assert_("uc_a" in constraints)

        # reflection corrects for the dupe
        reflected = Table("pgsql_uc", MetaData(), autoload_with=connection)

        indexes = {i.name for i in reflected.indexes}
        constraints = {uc.name for uc in reflected.constraints}

        self.assert_("uc_a" not in indexes)
        self.assert_("uc_a" in constraints)

    @testing.requires.btree_gist
    def test_reflection_with_exclude_constraint(self, metadata, connection):
        m = metadata
        Table(
            "t",
            m,
            Column("id", Integer, primary_key=True),
            Column("period", TSRANGE),
            ExcludeConstraint(("period", "&&"), name="quarters_period_excl"),
        )

        m.create_all(connection)

        insp = inspect(connection)

        # PostgreSQL will create an implicit index for an exclude constraint.
        # we don't reflect the EXCLUDE yet.
        expected = [
            {
                "unique": False,
                "name": "quarters_period_excl",
                "duplicates_constraint": "quarters_period_excl",
                "dialect_options": {"postgresql_using": "gist"},
                "column_names": ["period"],
            }
        ]
        if testing.requires.index_reflects_included_columns.enabled:
            expected[0]["include_columns"] = []
            expected[0]["dialect_options"]["postgresql_include"] = []

        eq_(insp.get_indexes("t"), expected)

        # reflection corrects for the dupe
        reflected = Table("t", MetaData(), autoload_with=connection)

        eq_(set(reflected.indexes), set())

    def test_reflect_unique_index(self, metadata, connection):
        insp = inspect(connection)

        meta = metadata

        # a unique index OTOH we are able to detect is an index
        # and not a unique constraint
        uc_table = Table(
            "pgsql_uc",
            meta,
            Column("a", String(10)),
            Index("ix_a", "a", unique=True),
        )

        uc_table.create(connection)

        indexes = {i["name"]: i for i in insp.get_indexes("pgsql_uc")}
        constraints = {
            i["name"] for i in insp.get_unique_constraints("pgsql_uc")
        }

        self.assert_("ix_a" in indexes)
        assert indexes["ix_a"]["unique"]
        self.assert_("ix_a" not in constraints)

        reflected = Table("pgsql_uc", MetaData(), autoload_with=connection)

        indexes = {i.name: i for i in reflected.indexes}
        constraints = {uc.name for uc in reflected.constraints}

        self.assert_("ix_a" in indexes)
        assert indexes["ix_a"].unique
        self.assert_("ix_a" not in constraints)

    def test_reflect_check_constraint(self, metadata, connection):
        meta = metadata

        udf_create = """\
            CREATE OR REPLACE FUNCTION is_positive(
                x integer DEFAULT '-1'::integer)
                RETURNS boolean
                LANGUAGE 'plpgsql'
                COST 100
                VOLATILE
            AS $BODY$BEGIN
                RETURN x > 0;
            END;$BODY$;
        """
        sa.event.listen(meta, "before_create", sa.DDL(udf_create))
        sa.event.listen(
            meta,
            "after_drop",
            sa.DDL("DROP FUNCTION IF EXISTS is_positive(integer)"),
        )

        Table(
            "pgsql_cc",
            meta,
            Column("a", Integer()),
            Column("b", String),
            CheckConstraint("a > 1 AND a < 5", name="cc1"),
            CheckConstraint("a = 1 OR (a > 2 AND a < 5)", name="cc2"),
            CheckConstraint("is_positive(a)", name="cc3"),
            CheckConstraint("b != 'hi\nim a name   \nyup\n'", name="cc4"),
        )

        meta.create_all(connection)

        reflected = Table("pgsql_cc", MetaData(), autoload_with=connection)

        check_constraints = {
            uc.name: uc.sqltext.text
            for uc in reflected.constraints
            if isinstance(uc, CheckConstraint)
        }

        eq_(
            check_constraints,
            {
                "cc1": "a > 1 AND a < 5",
                "cc2": "a = 1 OR a > 2 AND a < 5",
                "cc3": "is_positive(a)",
                "cc4": "b::text <> 'hi\nim a name   \nyup\n'::text",
            },
        )

    def test_reflect_check_warning(self):
        rows = [("foo", "some name", "NOTCHECK foobar", None)]
        conn = mock.Mock(
            execute=lambda *arg, **kw: mock.MagicMock(
                fetchall=lambda: rows, __iter__=lambda self: iter(rows)
            )
        )
        with testing.expect_warnings(
            "Could not parse CHECK constraint text: 'NOTCHECK foobar'"
        ):
            testing.db.dialect.get_check_constraints(conn, "foo")

    def test_reflect_extra_newlines(self):
        rows = [
            (
                "foo",
                "some name",
                "CHECK (\n(a \nIS\n NOT\n\n NULL\n)\n)",
                None,
            ),
            ("foo", "some other name", "CHECK ((b\nIS\nNOT\nNULL))", None),
            (
                "foo",
                "some CRLF name",
                "CHECK ((c\r\n\r\nIS\r\nNOT\r\nNULL))",
                None,
            ),
            ("foo", "some name", "CHECK (c != 'hi\nim a name\n')", None),
        ]
        conn = mock.Mock(
            execute=lambda *arg, **kw: mock.MagicMock(
                fetchall=lambda: rows, __iter__=lambda self: iter(rows)
            )
        )
        check_constraints = testing.db.dialect.get_check_constraints(
            conn, "foo"
        )
        eq_(
            check_constraints,
            [
                {
                    "name": "some name",
                    "sqltext": "a \nIS\n NOT\n\n NULL\n",
                    "comment": None,
                },
                {
                    "name": "some other name",
                    "sqltext": "b\nIS\nNOT\nNULL",
                    "comment": None,
                },
                {
                    "name": "some CRLF name",
                    "sqltext": "c\r\n\r\nIS\r\nNOT\r\nNULL",
                    "comment": None,
                },
                {
                    "name": "some name",
                    "sqltext": "c != 'hi\nim a name\n'",
                    "comment": None,
                },
            ],
        )

    def test_reflect_with_not_valid_check_constraint(self):
        rows = [
            ("foo", "some name", "CHECK ((a IS NOT NULL)) NOT VALID", None)
        ]
        conn = mock.Mock(
            execute=lambda *arg, **kw: mock.MagicMock(
                fetchall=lambda: rows, __iter__=lambda self: iter(rows)
            )
        )
        check_constraints = testing.db.dialect.get_check_constraints(
            conn, "foo"
        )
        eq_(
            check_constraints,
            [
                {
                    "name": "some name",
                    "sqltext": "a IS NOT NULL",
                    "dialect_options": {"not_valid": True},
                    "comment": None,
                }
            ],
        )

    def test_reflect_with_no_inherit_check_constraint(self):
        rows = [
            ("foo", "some name", "CHECK ((a IS NOT NULL)) NO INHERIT", None),
            (
                "foo",
                "some name",
                "CHECK ((a IS NOT NULL)) NO INHERIT NOT VALID",
                None,
            ),
        ]
        conn = mock.Mock(
            execute=lambda *arg, **kw: mock.MagicMock(
                fetchall=lambda: rows, __iter__=lambda self: iter(rows)
            )
        )
        check_constraints = testing.db.dialect.get_check_constraints(
            conn, "foo"
        )
        eq_(
            check_constraints,
            [
                {
                    "name": "some name",
                    "sqltext": "a IS NOT NULL",
                    "dialect_options": {"no_inherit": True},
                    "comment": None,
                },
                {
                    "name": "some name",
                    "sqltext": "a IS NOT NULL",
                    "dialect_options": {"not_valid": True, "no_inherit": True},
                    "comment": None,
                },
            ],
        )

    def _apply_stm(self, connection, use_map):
        if use_map:
            return connection.execution_options(
                schema_translate_map={
                    None: "foo",
                    testing.config.test_schema: "bar",
                }
            )
        else:
            return connection

    @testing.combinations(True, False, argnames="use_map")
    @testing.combinations(True, False, argnames="schema")
    def test_schema_translate_map(self, metadata, connection, use_map, schema):
        schema = testing.config.test_schema if schema else None
        Table(
            "foo",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("a", Integer, index=True),
            Column(
                "b",
                ForeignKey(f"{schema}.foo.id" if schema else "foo.id"),
                unique=True,
            ),
            CheckConstraint("a>10", name="foo_check"),
            comment="comm",
            schema=schema,
        )
        metadata.create_all(connection)
        if use_map:
            connection = connection.execution_options(
                schema_translate_map={
                    None: "foo",
                    testing.config.test_schema: "bar",
                }
            )
        insp = inspect(connection)
        eq_(
            [c["name"] for c in insp.get_columns("foo", schema=schema)],
            ["id", "a", "b"],
        )
        eq_(
            [
                i["column_names"]
                for i in insp.get_indexes("foo", schema=schema)
            ],
            [["b"], ["a"]],
        )
        eq_(
            insp.get_pk_constraint("foo", schema=schema)[
                "constrained_columns"
            ],
            ["id"],
        )
        eq_(insp.get_table_comment("foo", schema=schema), {"text": "comm"})
        eq_(
            [
                f["constrained_columns"]
                for f in insp.get_foreign_keys("foo", schema=schema)
            ],
            [["b"]],
        )
        eq_(
            [
                c["name"]
                for c in insp.get_check_constraints("foo", schema=schema)
            ],
            ["foo_check"],
        )
        eq_(
            [
                u["column_names"]
                for u in insp.get_unique_constraints("foo", schema=schema)
            ],
            [["b"]],
        )

    def test_reflection_constraint_comments(self, connection, metadata):
        t = Table(
            "foo",
            metadata,
            Column("id", Integer),
            Column("foo_id", ForeignKey("foo.id", name="fk_1")),
            Column("foo_other_id", ForeignKey("foo.id", name="fk_2")),
            CheckConstraint("id>0", name="ch_1"),
            CheckConstraint("id<1000", name="ch_2"),
            PrimaryKeyConstraint("id", name="foo_pk"),
            UniqueConstraint("id", "foo_id", name="un_1"),
            UniqueConstraint("id", "foo_other_id", name="un_2"),
        )
        metadata.create_all(connection)

        def check(elements, exp):
            elements = {c["name"]: c["comment"] for c in elements}
            eq_(elements, exp)

        def all_none():
            insp = inspect(connection)
            is_(insp.get_pk_constraint("foo")["comment"], None)
            check(
                insp.get_check_constraints("foo"), {"ch_1": None, "ch_2": None}
            )
            check(
                insp.get_unique_constraints("foo"),
                {"un_1": None, "un_2": None},
            )
            check(insp.get_foreign_keys("foo"), {"fk_1": None, "fk_2": None})

        all_none()

        c = next(c for c in t.constraints if c.name == "ch_1")
        u = next(c for c in t.constraints if c.name == "un_1")
        f = next(c for c in t.foreign_key_constraints if c.name == "fk_1")
        p = t.primary_key
        c.comment = "cc comment"
        u.comment = "uc comment"
        f.comment = "fc comment"
        p.comment = "pk comment"
        for cst in [c, u, f, p]:
            connection.execute(sa_ddl.SetConstraintComment(cst))

        insp = inspect(connection)
        eq_(insp.get_pk_constraint("foo")["comment"], "pk comment")
        check(
            insp.get_check_constraints("foo"),
            {"ch_1": "cc comment", "ch_2": None},
        )
        check(
            insp.get_unique_constraints("foo"),
            {"un_1": "uc comment", "un_2": None},
        )
        check(
            insp.get_foreign_keys("foo"), {"fk_1": "fc comment", "fk_2": None}
        )

        for cst in [c, u, f, p]:
            connection.execute(sa_ddl.DropConstraintComment(cst))
        all_none()

    @testing.skip_if("postgresql < 11.0", "not supported")
    def test_reflection_constraints_with_include(self, connection, metadata):
        Table(
            "foo",
            metadata,
            Column("id", Integer, nullable=False),
            Column("value", Integer, nullable=False),
            Column("foo", String),
            Column("arr", ARRAY(Integer)),
            Column("bar", SmallInteger),
        )
        metadata.create_all(connection)
        connection.exec_driver_sql(
            "ALTER TABLE foo ADD UNIQUE (id) INCLUDE (value)"
        )
        connection.exec_driver_sql(
            "ALTER TABLE foo "
            "ADD PRIMARY KEY (id) INCLUDE (arr, foo, bar, value)"
        )

        unq = inspect(connection).get_unique_constraints("foo")
        expected_unq = [
            {
                "column_names": ["id"],
                "name": "foo_id_value_key",
                "dialect_options": {
                    "postgresql_nulls_not_distinct": False,
                    "postgresql_include": ["value"],
                },
                "comment": None,
            }
        ]
        eq_(unq, expected_unq)

        pk = inspect(connection).get_pk_constraint("foo")
        expected_pk = {
            "comment": None,
            "constrained_columns": ["id"],
            "dialect_options": {
                "postgresql_include": ["arr", "foo", "bar", "value"]
            },
            "name": "foo_pkey",
        }
        eq_(pk, expected_pk)


class CustomTypeReflectionTest(fixtures.TestBase):
    class NTL:
        def __init__(self, enums, domains):
            self.enums = enums
            self.domains = domains

    class CustomType:
        def __init__(self, arg1=None, arg2=None, collation=None):
            self.arg1 = arg1
            self.arg2 = arg2
            self.collation = collation

    ischema_names = None

    def setup_test(self):
        ischema_names = postgresql.PGDialect.ischema_names
        postgresql.PGDialect.ischema_names = ischema_names.copy()
        self.ischema_names = ischema_names

    def teardown_test(self):
        postgresql.PGDialect.ischema_names = self.ischema_names
        self.ischema_names = None

    def _assert_reflected(self, dialect):
        for sch, args in [
            ("my_custom_type", (None, None)),
            ("my_custom_type()", (None, None)),
            ("my_custom_type(ARG1)", ("ARG1", None)),
            ("my_custom_type(ARG1, ARG2)", ("ARG1", "ARG2")),
        ]:
            row_dict = {
                "name": "colname",
                "table_name": "tblname",
                "format_type": sch,
                "default": None,
                "not_null": False,
                "collation": "cc" if sch == "my_custom_type()" else None,
                "comment": None,
                "generated": "",
                "identity_options": None,
            }
            column_info = dialect._get_columns_info(
                [row_dict], self.NTL({}, {}), "public"
            )
            assert ("public", "tblname") in column_info
            column_info = column_info[("public", "tblname")]
            assert len(column_info) == 1
            column_info = column_info[0]
            assert isinstance(column_info["type"], self.CustomType)
            eq_(column_info["type"].arg1, args[0])
            eq_(column_info["type"].arg2, args[1])
            if sch == "my_custom_type()":
                eq_(column_info["type"].collation, "cc")
            else:
                eq_(column_info["type"].collation, None)

    def test_clslevel(self):
        postgresql.PGDialect.ischema_names["my_custom_type"] = self.CustomType
        dialect = postgresql.PGDialect()
        self._assert_reflected(dialect)

    def test_instancelevel(self):
        dialect = postgresql.PGDialect()
        dialect.ischema_names = dialect.ischema_names.copy()
        dialect.ischema_names["my_custom_type"] = self.CustomType
        self._assert_reflected(dialect)

    def test_no_format_type(self):
        """test #8748"""

        dialect = postgresql.PGDialect()
        dialect.ischema_names = dialect.ischema_names.copy()
        dialect.ischema_names["my_custom_type"] = self.CustomType

        with expect_warnings(
            r"PostgreSQL format_type\(\) returned NULL for column 'colname'"
        ):
            row_dict = {
                "name": "colname",
                "table_name": "tblname",
                "format_type": None,
                "default": None,
                "not_null": False,
                "collation": None,
                "comment": None,
                "generated": "",
                "identity_options": None,
            }
            column_info = dialect._get_columns_info(
                [row_dict], self.NTL({}, {}), "public"
            )
            assert ("public", "tblname") in column_info
            column_info = column_info[("public", "tblname")]
            assert len(column_info) == 1
            column_info = column_info[0]
            assert isinstance(column_info["type"], NullType)


class IntervalReflectionTest(fixtures.TestBase):
    __only_on__ = "postgresql"
    __sparse_driver_backend__ = True

    @testing.combinations(
        ("YEAR",),
        ("MONTH",),
        ("DAY",),
        ("HOUR",),
        ("MINUTE",),
        ("SECOND",),
        ("YEAR TO MONTH",),
        ("DAY TO HOUR",),
        ("DAY TO MINUTE",),
        ("DAY TO SECOND",),
        ("HOUR TO MINUTE",),
        ("HOUR TO SECOND",),
        ("MINUTE TO SECOND",),
        argnames="sym",
    )
    def test_interval_types(self, sym, metadata, connection):
        t = Table(
            "i_test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data1", INTERVAL(fields=sym)),
        )
        t.create(connection)

        columns = {
            rec["name"]: rec
            for rec in inspect(connection).get_columns("i_test")
        }
        assert isinstance(columns["data1"]["type"], INTERVAL)
        eq_(columns["data1"]["type"].fields, sym.lower())
        eq_(columns["data1"]["type"].precision, None)

    def test_interval_precision(self, metadata, connection):
        t = Table(
            "i_test",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data1", INTERVAL(precision=6)),
        )
        t.create(connection)

        columns = {
            rec["name"]: rec
            for rec in inspect(connection).get_columns("i_test")
        }
        assert isinstance(columns["data1"]["type"], INTERVAL)
        eq_(columns["data1"]["type"].fields, None)
        eq_(columns["data1"]["type"].precision, 6)


class IdentityReflectionTest(fixtures.TablesTest):
    __only_on__ = "postgresql"
    __sparse_driver_backend__ = True
    __requires__ = ("identity_columns",)

    _names = ("t1", "T2", "MiXeDCaSe!")

    @classmethod
    def define_tables(cls, metadata):
        for name in cls._names:
            Table(
                name,
                metadata,
                Column(
                    "id1",
                    Integer,
                    Identity(
                        always=True,
                        start=2,
                        increment=3,
                        minvalue=-2,
                        maxvalue=42,
                        cycle=True,
                        cache=4,
                    ),
                ),
                Column("id2", Integer, Identity()),
                Column("id3", BigInteger, Identity()),
                Column("id4", SmallInteger, Identity()),
            )

    @testing.combinations(*_names, argnames="name")
    def test_reflect_identity(self, connection, name):
        insp = inspect(connection)
        default = dict(
            always=False,
            start=1,
            increment=1,
            minvalue=1,
            cycle=False,
            cache=1,
        )
        cols = insp.get_columns(name)
        for col in cols:
            if col["name"] == "id1":
                is_true("identity" in col)
                eq_(
                    col["identity"],
                    dict(
                        always=True,
                        start=2,
                        increment=3,
                        minvalue=-2,
                        maxvalue=42,
                        cycle=True,
                        cache=4,
                    ),
                )
            elif col["name"] == "id2":
                is_true("identity" in col)
                exp = default.copy()
                exp.update(maxvalue=2**31 - 1)
                eq_(col["identity"], exp)
            elif col["name"] == "id3":
                is_true("identity" in col)
                exp = default.copy()
                exp.update(maxvalue=2**63 - 1)
                eq_(col["identity"], exp)
            elif col["name"] == "id4":
                is_true("identity" in col)
                exp = default.copy()
                exp.update(maxvalue=2**15 - 1)
                eq_(col["identity"], exp)


class TestReflectDifficultColTypes(fixtures.TablesTest):
    __only_on__ = "postgresql"
    __sparse_driver_backend__ = True

    def define_tables(metadata):
        Table(
            "sample_table",
            metadata,
            Column("c1", Integer, primary_key=True),
            Column("c2", Integer, unique=True),
            Column("c3", Integer),
            Index("sample_table_index", "c2", "c3"),
        )

    def check_int_list(self, row, key):
        value = row[key]
        is_true(isinstance(value, list))
        is_true(len(value) > 0)
        is_true(all(isinstance(v, int) for v in value))

    def test_pg_index(self, connection):
        insp = inspect(connection)

        pgc_oid = insp.get_table_oid("sample_table")
        cols = [
            col
            for col in pg_catalog.pg_index.c
            if testing.db.dialect.server_version_info
            >= col.info.get("server_version", (0,))
        ]

        stmt = sa.select(*cols).filter_by(indrelid=pgc_oid)
        rows = connection.execute(stmt).mappings().all()
        is_true(len(rows) > 0)
        cols = [
            col
            for col in ["indkey", "indoption", "indclass", "indcollation"]
            if testing.db.dialect.server_version_info
            >= pg_catalog.pg_index.c[col].info.get("server_version", (0,))
        ]
        for row in rows:
            for col in cols:
                self.check_int_list(row, col)

    def test_pg_constraint(self, connection):
        insp = inspect(connection)

        pgc_oid = insp.get_table_oid("sample_table")
        cols = [
            col
            for col in pg_catalog.pg_constraint.c
            if testing.db.dialect.server_version_info
            >= col.info.get("server_version", (0,))
        ]
        stmt = sa.select(*cols).filter_by(conrelid=pgc_oid)
        rows = connection.execute(stmt).mappings().all()
        is_true(len(rows) > 0)
        for row in rows:
            self.check_int_list(row, "conkey")


class TestTableOptionsReflection(fixtures.TestBase):
    __only_on__ = "postgresql"
    __sparse_driver_backend__ = True

    def test_table_inherits(self, metadata, connection):
        def assert_inherits_from(table_name, expect_base_tables):
            table_options = inspect(connection).get_table_options(table_name)
            eq_(
                table_options.get("postgresql_inherits", ()),
                expect_base_tables,
            )

        def assert_column_names(table_name, expect_columns):
            columns = inspect(connection).get_columns(table_name)
            eq_([c["name"] for c in columns], expect_columns)

        Table("base", metadata, Column("id", INTEGER, primary_key=True))
        Table("name_mixin", metadata, Column("name", String(16)))
        Table("single_inherits", metadata, postgresql_inherits="base")
        Table(
            "single_inherits_tuple_arg",
            metadata,
            postgresql_inherits=("base",),
        )
        Table(
            "inherits_mixin",
            metadata,
            postgresql_inherits=("base", "name_mixin"),
        )

        metadata.create_all(connection)

        assert_inherits_from("base", ())
        assert_inherits_from("name_mixin", ())

        assert_inherits_from("single_inherits", ("base",))
        assert_column_names("single_inherits", ["id"])

        assert_inherits_from("single_inherits_tuple_arg", ("base",))

        assert_inherits_from("inherits_mixin", ("base", "name_mixin"))
        assert_column_names("inherits_mixin", ["id", "name"])

    def test_table_storage_params(self, metadata, connection):
        def assert_has_storage_param(table_name, option_key, option_value):
            table_options = inspect(connection).get_table_options(table_name)
            storage_params = table_options["postgresql_with"]
            assert isinstance(storage_params, dict)
            eq_(storage_params[option_key], option_value)

        Table("table_no_storage_params", metadata)
        Table(
            "table_with_fillfactor",
            metadata,
            postgresql_with={"fillfactor": 10},
        )
        Table(
            "table_with_parallel_workers",
            metadata,
            postgresql_with={"parallel_workers": 15},
        )

        metadata.create_all(connection)

        no_params_options = inspect(connection).get_table_options(
            "table_no_storage_params"
        )
        assert "postgresql_with" not in no_params_options

        assert_has_storage_param("table_with_fillfactor", "fillfactor", "10")
        assert_has_storage_param(
            "table_with_parallel_workers", "parallel_workers", "15"
        )

    def test_table_using_default(self, metadata: MetaData, connection):
        Table("table_using_heap", metadata, postgresql_using="heap").create(
            connection
        )
        options = inspect(connection).get_table_options("table_using_heap")
        is_false("postgresql_using" in options)

    def test_table_using_custom(self, metadata: MetaData, connection):
        if not connection.exec_driver_sql(
            "SELECT rolsuper FROM pg_roles WHERE rolname = current_user"
        ).scalar():
            config.skip_test("superuser required for CREATE ACCESS METHOD")
        connection.exec_driver_sql(
            "CREATE ACCESS METHOD myaccessmethod "
            "TYPE TABLE "
            "HANDLER heap_tableam_handler"
        )
        Table(
            "table_using_myaccessmethod",
            metadata,
            postgresql_using="myaccessmethod",
        ).create(connection)

        options = inspect(connection).get_table_options(
            "table_using_myaccessmethod"
        )
        eq_(options["postgresql_using"], "myaccessmethod")
