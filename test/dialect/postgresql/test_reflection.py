# coding: utf-8

import itertools
from operator import itemgetter
import re

import sqlalchemy as sa
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import exc
from sqlalchemy import ForeignKey
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
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import base as postgresql
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.dialects.postgresql import INTEGER
from sqlalchemy.dialects.postgresql import INTERVAL
from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy.schema import CreateIndex
from sqlalchemy.sql.schema import CheckConstraint
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.assertions import assert_raises
from sqlalchemy.testing.assertions import AssertsExecutionResults
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.assertions import is_true


class ForeignTableReflectionTest(fixtures.TablesTest, AssertsExecutionResults):
    """Test reflection on foreign tables"""

    __requires__ = ("postgresql_test_dblink",)
    __only_on__ = "postgresql >= 9.3"
    __backend__ = True

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
            set(["id", "data"]),
            "Columns of reflected foreign table didn't equal expected columns",
        )

    def test_get_foreign_table_names(self, connection):
        inspector = inspect(connection)
        ft_names = inspector.get_foreign_table_names()
        eq_(ft_names, ["test_foreigntable"])

    def test_get_table_names_no_foreign(self, connection):
        inspector = inspect(connection)
        names = inspector.get_table_names()
        eq_(names, ["testtable"])


class PartitionedReflectionTest(fixtures.TablesTest, AssertsExecutionResults):
    # partitioned table reflection, issue #4237

    __only_on__ = "postgresql >= 10"
    __backend__ = True

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
                    "name": mock.ANY,
                    "unique": False,
                }
            ],
        )


class MaterializedViewReflectionTest(
    fixtures.TablesTest, AssertsExecutionResults
):
    """Test reflection on materialized views"""

    __only_on__ = "postgresql >= 9.3"
    __backend__ = True

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
            "CREATE MATERIALIZED VIEW test_mview AS " "SELECT * FROM testtable"
        )

        plain_view = sa.DDL(
            "CREATE VIEW test_regview AS " "SELECT * FROM testtable"
        )

        sa.event.listen(testtable, "after_create", plain_view)
        sa.event.listen(testtable, "after_create", materialized_view)
        sa.event.listen(
            testtable,
            "before_drop",
            sa.DDL("DROP MATERIALIZED VIEW test_mview"),
        )
        sa.event.listen(
            testtable, "before_drop", sa.DDL("DROP VIEW test_regview")
        )

    def test_mview_is_reflected(self, connection):
        metadata = MetaData()
        table = Table("test_mview", metadata, autoload_with=connection)
        eq_(
            set(table.columns.keys()),
            set(["id", "data"]),
            "Columns of reflected mview didn't equal expected columns",
        )

    def test_mview_select(self, connection):
        metadata = MetaData()
        table = Table("test_mview", metadata, autoload_with=connection)
        eq_(connection.execute(table.select()).fetchall(), [(89, "d1")])

    def test_get_view_names(self, connection):
        insp = inspect(connection)
        eq_(set(insp.get_view_names()), set(["test_regview", "test_mview"]))

    def test_get_view_names_plain(self, connection):
        insp = inspect(connection)
        eq_(
            set(insp.get_view_names(include=("plain",))), set(["test_regview"])
        )

    def test_get_view_names_plain_string(self, connection):
        insp = inspect(connection)
        eq_(set(insp.get_view_names(include="plain")), set(["test_regview"]))

    def test_get_view_names_materialized(self, connection):
        insp = inspect(connection)
        eq_(
            set(insp.get_view_names(include=("materialized",))),
            set(["test_mview"]),
        )

    def test_get_view_names_reflection_cache_ok(self, connection):
        insp = inspect(connection)
        eq_(
            set(insp.get_view_names(include=("plain",))), set(["test_regview"])
        )
        eq_(
            set(insp.get_view_names(include=("materialized",))),
            set(["test_mview"]),
        )
        eq_(set(insp.get_view_names()), set(["test_regview", "test_mview"]))

    def test_get_view_names_empty(self, connection):
        insp = inspect(connection)
        assert_raises(ValueError, insp.get_view_names, include=())

    def test_get_view_definition(self, connection):
        insp = inspect(connection)
        eq_(
            re.sub(
                r"[\n\t ]+",
                " ",
                insp.get_view_definition("test_mview").strip(),
            ),
            "SELECT testtable.id, testtable.data FROM testtable;",
        )


class DomainReflectionTest(fixtures.TestBase, AssertsExecutionResults):
    """Test PostgreSQL domains"""

    __only_on__ = "postgresql > 8.3"
    __backend__ = True

    @classmethod
    def setup_test_class(cls):
        with testing.db.begin() as con:
            for ddl in [
                'CREATE SCHEMA "SomeSchema"',
                "CREATE DOMAIN testdomain INTEGER NOT NULL DEFAULT 42",
                "CREATE DOMAIN test_schema.testdomain INTEGER DEFAULT 0",
                "CREATE TYPE testtype AS ENUM ('test')",
                "CREATE DOMAIN enumdomain AS testtype",
                "CREATE DOMAIN arraydomain AS INTEGER[]",
                'CREATE DOMAIN "SomeSchema"."Quoted.Domain" INTEGER DEFAULT 0',
            ]:
                try:
                    con.exec_driver_sql(ddl)
                except exc.DBAPIError as e:
                    if "already exists" not in str(e):
                        raise e
            con.exec_driver_sql(
                "CREATE TABLE testtable (question integer, answer "
                "testdomain)"
            )
            con.exec_driver_sql(
                "CREATE TABLE test_schema.testtable(question "
                "integer, answer test_schema.testdomain, anything "
                "integer)"
            )
            con.exec_driver_sql(
                "CREATE TABLE crosschema (question integer, answer "
                "test_schema.testdomain)"
            )

            con.exec_driver_sql(
                "CREATE TABLE enum_test (id integer, data enumdomain)"
            )

            con.exec_driver_sql(
                "CREATE TABLE array_test (id integer, data arraydomain)"
            )

            con.exec_driver_sql(
                "CREATE TABLE quote_test "
                '(id integer, data "SomeSchema"."Quoted.Domain")'
            )

    @classmethod
    def teardown_test_class(cls):
        with testing.db.begin() as con:
            con.exec_driver_sql("DROP TABLE testtable")
            con.exec_driver_sql("DROP TABLE test_schema.testtable")
            con.exec_driver_sql("DROP TABLE crosschema")
            con.exec_driver_sql("DROP TABLE quote_test")
            con.exec_driver_sql("DROP DOMAIN testdomain")
            con.exec_driver_sql("DROP DOMAIN test_schema.testdomain")
            con.exec_driver_sql("DROP TABLE enum_test")
            con.exec_driver_sql("DROP DOMAIN enumdomain")
            con.exec_driver_sql("DROP TYPE testtype")
            con.exec_driver_sql("DROP TABLE array_test")
            con.exec_driver_sql("DROP DOMAIN arraydomain")
            con.exec_driver_sql('DROP DOMAIN "SomeSchema"."Quoted.Domain"')
            con.exec_driver_sql('DROP SCHEMA "SomeSchema"')

    def test_table_is_reflected(self, connection):
        metadata = MetaData()
        table = Table("testtable", metadata, autoload_with=connection)
        eq_(
            set(table.columns.keys()),
            set(["question", "answer"]),
            "Columns of reflected table didn't equal expected columns",
        )
        assert isinstance(table.c.answer.type, Integer)

    def test_domain_is_reflected(self, connection):
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

    def test_enum_domain_is_reflected(self, connection):
        metadata = MetaData()
        table = Table("enum_test", metadata, autoload_with=connection)
        eq_(table.c.data.type.enums, ["test"])

    def test_array_domain_is_reflected(self, connection):
        metadata = MetaData()
        table = Table("array_test", metadata, autoload_with=connection)
        eq_(table.c.data.type.__class__, ARRAY)
        eq_(table.c.data.type.item_type.__class__, INTEGER)

    def test_quoted_remote_schema_domain_is_reflected(self, connection):
        metadata = MetaData()
        table = Table("quote_test", metadata, autoload_with=connection)
        eq_(table.c.data.type.__class__, INTEGER)

    def test_table_is_reflected_test_schema(self, connection):
        metadata = MetaData()
        table = Table(
            "testtable",
            metadata,
            autoload_with=connection,
            schema="test_schema",
        )
        eq_(
            set(table.columns.keys()),
            set(["question", "answer", "anything"]),
            "Columns of reflected table didn't equal expected columns",
        )
        assert isinstance(table.c.anything.type, Integer)

    def test_schema_domain_is_reflected(self, connection):
        metadata = MetaData()
        table = Table(
            "testtable",
            metadata,
            autoload_with=connection,
            schema="test_schema",
        )
        eq_(
            str(table.columns.answer.server_default.arg),
            "0",
            "Reflected default value didn't equal expected value",
        )
        assert (
            table.columns.answer.nullable
        ), "Expected reflected column to be nullable."

    def test_crosschema_domain_is_reflected(self, connection):
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

    def test_unknown_types(self, connection):
        from sqlalchemy.dialects.postgresql import base

        ischema_names = base.PGDialect.ischema_names
        base.PGDialect.ischema_names = {}
        try:
            m2 = MetaData()
            assert_raises(
                exc.SAWarning, Table, "testtable", m2, autoload_with=connection
            )

            @testing.emits_warning("Did not recognize type")
            def warns():
                m3 = MetaData()
                t3 = Table("testtable", m3, autoload_with=connection)
                assert t3.c.answer.type.__class__ == sa.types.NullType

        finally:
            base.PGDialect.ischema_names = ischema_names


class ReflectionTest(AssertsCompiledSQL, fixtures.TestBase):
    __only_on__ = "postgresql"
    __backend__ = True

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
            set(["test_schema_2.some_other_table", "some_table"]),
        )

        meta3 = MetaData()
        meta3.reflect(
            connection,
            schema="test_schema_2",
            postgresql_ignore_search_path=True,
        )

        eq_(
            set(meta3.tables),
            set(
                [
                    "test_schema_2.some_other_table",
                    "test_schema.some_table",
                ]
            ),
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
            set(["some_other_table", "test_schema.some_table"]),
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
        """Reflecting expression-based indexes should warn"""

        Table(
            "party",
            metadata,
            Column("id", String(10), nullable=False),
            Column("name", String(20), index=True),
            Column("aname", String(20)),
        )
        metadata.create_all(connection)
        connection.exec_driver_sql("create index idx1 on party ((id || name))")
        connection.exec_driver_sql(
            "create unique index idx2 on party (id) where name = 'test'"
        )
        connection.exec_driver_sql(
            """
            create index idx3 on party using btree
                (lower(name::text), lower(aname::text))
            """
        )

        def go():
            m2 = MetaData()
            t2 = Table("party", m2, autoload_with=connection)
            assert len(t2.indexes) == 2

            # Make sure indexes are in the order we expect them in

            tmp = [(idx.name, idx) for idx in t2.indexes]
            tmp.sort()
            r1, r2 = [idx[1] for idx in tmp]
            assert r1.name == "idx2"
            assert r1.unique is True
            assert r2.unique is False
            assert [t2.c.id] == r1.columns
            assert [t2.c.name] == r2.columns

        testing.assert_warnings(
            go,
            [
                "Skipped unsupported reflection of "
                "expression-based index idx1",
                "Skipped unsupported reflection of "
                "expression-based index idx3",
            ],
        )

    def test_index_reflection_partial(self, metadata, connection):
        """Reflect the filter defintion on partial indexes"""

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

        ind = connection.dialect.get_indexes(connection, t1, None)

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
        # (https://www.postgresql.org/docs/11/indexes-ordering.html)

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
        eq_(ind, expected)
        m = MetaData()
        t1 = Table("t", m, autoload_with=connection)
        eq_(
            list(t1.indexes)[0].dialect_options["postgresql"]["using"],
            "gin",
        )

    @testing.skip_if("postgresql < 11.0", "indnkeyatts not supported")
    def test_index_reflection_with_include(self, metadata, connection):
        """reflect indexes with include set"""

        Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", ARRAY(Integer)),
            Column("name", String(20)),
        )
        metadata.create_all(connection)
        connection.exec_driver_sql("CREATE INDEX idx1 ON t (x) INCLUDE (name)")

        # prior to #5205, this would return:
        # [{'column_names': ['x', 'name'],
        #  'name': 'idx1', 'unique': False}]

        ind = connection.dialect.get_indexes(connection, "t", None)
        eq_(
            ind,
            [
                {
                    "unique": False,
                    "column_names": ["x"],
                    "include_columns": ["name"],
                    "name": "idx1",
                }
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
            },
            "company_industry_id_fkey": {
                "name": "company_industry_id_fkey",
                "constrained_columns": ["industry_id"],
                "referred_columns": ["id"],
                "referred_table": "industry",
                "referred_schema": None,
                "options": {"onupdate": "CASCADE", "ondelete": "CASCADE"},
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

    def test_inspect_enums(self, metadata, connection):
        enum_type = postgresql.ENUM(
            "cat", "dog", "rat", name="pet", metadata=metadata
        )
        enum_type.create(connection)
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
        indexes = set(i["name"] for i in insp.get_indexes("pgsql_uc"))
        constraints = set(
            i["name"] for i in insp.get_unique_constraints("pgsql_uc")
        )

        self.assert_("uc_a" in indexes)
        self.assert_("uc_a" in constraints)

        # reflection corrects for the dupe
        reflected = Table("pgsql_uc", MetaData(), autoload_with=connection)

        indexes = set(i.name for i in reflected.indexes)
        constraints = set(uc.name for uc in reflected.constraints)

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

        indexes = dict((i["name"], i) for i in insp.get_indexes("pgsql_uc"))
        constraints = set(
            i["name"] for i in insp.get_unique_constraints("pgsql_uc")
        )

        self.assert_("ix_a" in indexes)
        assert indexes["ix_a"]["unique"]
        self.assert_("ix_a" not in constraints)

        reflected = Table("pgsql_uc", MetaData(), autoload_with=connection)

        indexes = dict((i.name, i) for i in reflected.indexes)
        constraints = set(uc.name for uc in reflected.constraints)

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

        check_constraints = dict(
            (uc.name, uc.sqltext.text)
            for uc in reflected.constraints
            if isinstance(uc, CheckConstraint)
        )

        eq_(
            check_constraints,
            {
                u"cc1": u"(a > 1) AND (a < 5)",
                u"cc2": u"(a = 1) OR ((a > 2) AND (a < 5))",
                u"cc3": u"is_positive(a)",
                u"cc4": u"(b)::text <> 'hi\nim a name   \nyup\n'::text",
            },
        )

    def test_reflect_check_warning(self):
        rows = [("some name", "NOTCHECK foobar")]
        conn = mock.Mock(
            execute=lambda *arg, **kw: mock.MagicMock(
                fetchall=lambda: rows, __iter__=lambda self: iter(rows)
            )
        )
        with mock.patch.object(
            testing.db.dialect, "get_table_oid", lambda *arg, **kw: 1
        ):
            with testing.expect_warnings(
                "Could not parse CHECK constraint text: 'NOTCHECK foobar'"
            ):
                testing.db.dialect.get_check_constraints(conn, "foo")

    def test_reflect_extra_newlines(self):
        rows = [
            ("some name", "CHECK (\n(a \nIS\n NOT\n\n NULL\n)\n)"),
            ("some other name", "CHECK ((b\nIS\nNOT\nNULL))"),
            ("some CRLF name", "CHECK ((c\r\n\r\nIS\r\nNOT\r\nNULL))"),
            ("some name", "CHECK (c != 'hi\nim a name\n')"),
        ]
        conn = mock.Mock(
            execute=lambda *arg, **kw: mock.MagicMock(
                fetchall=lambda: rows, __iter__=lambda self: iter(rows)
            )
        )
        with mock.patch.object(
            testing.db.dialect, "get_table_oid", lambda *arg, **kw: 1
        ):
            check_constraints = testing.db.dialect.get_check_constraints(
                conn, "foo"
            )
            eq_(
                check_constraints,
                [
                    {
                        "name": "some name",
                        "sqltext": "a \nIS\n NOT\n\n NULL\n",
                    },
                    {"name": "some other name", "sqltext": "b\nIS\nNOT\nNULL"},
                    {
                        "name": "some CRLF name",
                        "sqltext": "c\r\n\r\nIS\r\nNOT\r\nNULL",
                    },
                    {"name": "some name", "sqltext": "c != 'hi\nim a name\n'"},
                ],
            )

    def test_reflect_with_not_valid_check_constraint(self):
        rows = [("some name", "CHECK ((a IS NOT NULL)) NOT VALID")]
        conn = mock.Mock(
            execute=lambda *arg, **kw: mock.MagicMock(
                fetchall=lambda: rows, __iter__=lambda self: iter(rows)
            )
        )
        with mock.patch.object(
            testing.db.dialect, "get_table_oid", lambda *arg, **kw: 1
        ):
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
                    }
                ],
            )


class CustomTypeReflectionTest(fixtures.TestBase):
    class CustomType(object):
        def __init__(self, arg1=None, arg2=None):
            self.arg1 = arg1
            self.arg2 = arg2

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
            column_info = dialect._get_column_info(
                "colname", sch, None, False, {}, {}, "public", None, "", None
            )
            assert isinstance(column_info["type"], self.CustomType)
            eq_(column_info["type"].arg1, args[0])
            eq_(column_info["type"].arg2, args[1])

    def test_clslevel(self):
        postgresql.PGDialect.ischema_names["my_custom_type"] = self.CustomType
        dialect = postgresql.PGDialect()
        self._assert_reflected(dialect)

    def test_instancelevel(self):
        dialect = postgresql.PGDialect()
        dialect.ischema_names = dialect.ischema_names.copy()
        dialect.ischema_names["my_custom_type"] = self.CustomType
        self._assert_reflected(dialect)


class IntervalReflectionTest(fixtures.TestBase):
    __only_on__ = "postgresql"
    __backend__ = True

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
    __backend__ = True
    __requires__ = ("identity_columns",)

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
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

    def test_reflect_identity(self, connection):
        insp = inspect(connection)
        default = dict(
            always=False,
            start=1,
            increment=1,
            minvalue=1,
            cycle=False,
            cache=1,
        )
        cols = insp.get_columns("t1")
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
                exp.update(maxvalue=2 ** 31 - 1)
                eq_(col["identity"], exp)
            elif col["name"] == "id3":
                is_true("identity" in col)
                exp = default.copy()
                exp.update(maxvalue=2 ** 63 - 1)
                eq_(col["identity"], exp)
            elif col["name"] == "id4":
                is_true("identity" in col)
                exp = default.copy()
                exp.update(maxvalue=2 ** 15 - 1)
                eq_(col["identity"], exp)
