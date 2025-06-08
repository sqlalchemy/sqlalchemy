import datetime
import decimal
import random

from sqlalchemy import Column
from sqlalchemy import DDL
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Identity
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects.mssql import base
from sqlalchemy.dialects.mssql.information_schema import tables
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateIndex
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import ComparesTables
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing import provision
from sqlalchemy.testing.assertions import is_false


class ReflectionTest(fixtures.TestBase, ComparesTables, AssertsCompiledSQL):
    __only_on__ = "mssql"
    __backend__ = True

    def test_basic_reflection(self, metadata, connection):
        meta = metadata

        users = Table(
            "engine_users",
            meta,
            Column("user_id", types.INT, primary_key=True),
            Column("user_name", types.VARCHAR(20), nullable=False),
            Column("test1", types.CHAR(5), nullable=False),
            Column("test2", types.Float(5), nullable=False),
            Column("test2.5", types.Float(), nullable=False),
            Column("test3", types.Text()),
            Column("test4", types.Numeric, nullable=False),
            Column("test4.5", types.Numeric(10, 2), nullable=False),
            Column("test5", types.DateTime),
            Column(
                "parent_user_id",
                types.Integer,
                ForeignKey("engine_users.user_id"),
            ),
            Column("test6", types.DateTime, nullable=False),
            Column("test7", types.Text()),
            Column("test8", types.LargeBinary()),
            Column("test_passivedefault2", types.Integer, server_default="5"),
            Column("test9", types.BINARY(100)),
            Column("test_numeric", types.Numeric()),
        )

        addresses = Table(
            "engine_email_addresses",
            meta,
            Column("address_id", types.Integer, primary_key=True),
            Column(
                "remote_user_id", types.Integer, ForeignKey(users.c.user_id)
            ),
            Column("email_address", types.String(20)),
        )
        meta.create_all(connection)

        meta2 = MetaData()
        reflected_users = Table(
            "engine_users", meta2, autoload_with=connection
        )
        reflected_addresses = Table(
            "engine_email_addresses",
            meta2,
            autoload_with=connection,
        )
        self.assert_tables_equal(users, reflected_users)
        self.assert_tables_equal(addresses, reflected_addresses)

    @testing.combinations(
        (mssql.XML, "XML"),
        (mssql.IMAGE, "IMAGE"),
        (mssql.MONEY, "MONEY"),
        (mssql.NUMERIC(10, 2), "NUMERIC(10, 2)"),
        (mssql.FLOAT, "FLOAT(53)"),
        (mssql.REAL, "REAL"),
        # FLOAT(5) comes back as REAL
        (mssql.FLOAT(5), "REAL"),
        argnames="type_obj,ddl",
    )
    def test_assorted_types(self, metadata, connection, type_obj, ddl):
        table = Table("type_test", metadata, Column("col1", type_obj))
        table.create(connection)

        m2 = MetaData()
        table2 = Table("type_test", m2, autoload_with=connection)
        self.assert_compile(
            schema.CreateTable(table2),
            "CREATE TABLE type_test (col1 %s NULL)" % ddl,
        )

    def test_identity(self, metadata, connection):
        table = Table(
            "identity_test",
            metadata,
            Column(
                "col1",
                Integer,
                mssql_identity_start=2,
                mssql_identity_increment=3,
                primary_key=True,
            ),
        )
        with testing.expect_deprecated(
            "The dialect options 'mssql_identity_start' and"
        ):
            table.create(connection)

        meta2 = MetaData()
        table2 = Table("identity_test", meta2, autoload_with=connection)
        eq_(table2.c["col1"].dialect_options["mssql"]["identity_start"], None)
        eq_(
            table2.c["col1"].dialect_options["mssql"]["identity_increment"],
            None,
        )
        eq_(table2.c["col1"].identity.start, 2)
        eq_(table2.c["col1"].identity.increment, 3)

    def test_skip_types(self, connection):
        connection.exec_driver_sql(
            "create table foo (id integer primary key, data xml)"
        )
        with mock.patch.object(
            connection.dialect, "ischema_names", {"int": mssql.INTEGER}
        ):
            with testing.expect_warnings(
                "Did not recognize type 'xml' of column 'data'"
            ):
                eq_(
                    inspect(connection).get_columns("foo"),
                    [
                        {
                            "name": "id",
                            "type": testing.eq_type_affinity(sqltypes.INTEGER),
                            "nullable": False,
                            "default": None,
                            "autoincrement": False,
                            "comment": None,
                        },
                        {
                            "name": "data",
                            "type": testing.eq_type_affinity(
                                sqltypes.NullType
                            ),
                            "nullable": True,
                            "default": None,
                            "autoincrement": False,
                            "comment": None,
                        },
                    ],
                )

    def test_cross_schema_fk_pk_name_overlaps(self, metadata, connection):
        # test for issue #4228

        Table(
            "subject",
            metadata,
            Column("id", Integer),
            PrimaryKeyConstraint("id", name="subj_pk"),
            schema=testing.config.test_schema,
        )

        Table(
            "referrer",
            metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "sid",
                ForeignKey(
                    "%s.subject.id" % testing.config.test_schema,
                    name="fk_subject",
                ),
            ),
            schema=testing.config.test_schema,
        )

        Table(
            "subject",
            metadata,
            Column("id", Integer),
            PrimaryKeyConstraint("id", name="subj_pk"),
            schema=testing.config.test_schema_2,
        )

        metadata.create_all(connection)

        insp = inspect(connection)
        eq_(
            insp.get_foreign_keys("referrer", testing.config.test_schema),
            [
                {
                    "name": "fk_subject",
                    "constrained_columns": ["sid"],
                    "referred_schema": "test_schema",
                    "referred_table": "subject",
                    "referred_columns": ["id"],
                    "options": {},
                }
            ],
        )

    def test_table_name_that_is_greater_than_16_chars(
        self, metadata, connection
    ):
        Table(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Index("foo_idx", "foo"),
        )
        metadata.create_all(connection)

        t = Table(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ", MetaData(), autoload_with=connection
        )
        eq_(t.name, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")

    @testing.combinations(
        ("local_temp", "#tmp", True),
        ("global_temp", "##tmp", True),
        ("nonexistent", "#no_es_bueno", False),
        id_="iaa",
        argnames="table_name, exists",
    )
    def test_temporary_table(self, metadata, connection, table_name, exists):
        if exists:
            tt = Table(
                table_name,
                metadata,
                Column("id", Integer, primary_key=True),
                Column("txt", mssql.NVARCHAR(50)),
                Column("dt2", mssql.DATETIME2),
            )
            tt.create(connection)
            connection.execute(
                tt.insert(),
                [
                    {
                        "id": 1,
                        "txt": "foo",
                        "dt2": datetime.datetime(2020, 1, 1, 1, 1, 1),
                    },
                    {
                        "id": 2,
                        "txt": "bar",
                        "dt2": datetime.datetime(2020, 2, 2, 2, 2, 2),
                    },
                ],
            )

        if not exists:
            with expect_raises(exc.NoSuchTableError):
                Table(
                    table_name,
                    metadata,
                    autoload_with=connection,
                )
        else:
            tmp_t = Table(table_name, metadata, autoload_with=connection)
            result = connection.execute(
                tmp_t.select().where(tmp_t.c.id == 2)
            ).fetchall()
            eq_(
                result,
                [(2, "bar", datetime.datetime(2020, 2, 2, 2, 2, 2))],
            )

    @testing.combinations(
        ("local_temp", "#tmp", True),
        ("global_temp", "##tmp", True),
        ("nonexistent", "#no_es_bueno", False),
        id_="iaa",
        argnames="table_name, exists",
    )
    def test_has_table_temporary(
        self, metadata, connection, table_name, exists
    ):
        if exists:
            tt = Table(
                table_name,
                metadata,
                Column("id", Integer),
            )
            tt.create(connection)

        found_it = testing.db.dialect.has_table(connection, table_name)
        eq_(found_it, exists)

    def test_has_table_temp_not_present_but_another_session(self):
        """test #6910"""

        with testing.db.connect() as c1, testing.db.connect() as c2:
            try:
                with c1.begin():
                    c1.exec_driver_sql(
                        "create table #myveryveryuniquetemptablename (a int)"
                    )
                assert not c2.dialect.has_table(
                    c2, "#myveryveryuniquetemptablename"
                )
            finally:
                with c1.begin():
                    c1.exec_driver_sql(
                        "drop table #myveryveryuniquetemptablename"
                    )

    def test_has_table_temp_temp_present_both_sessions(self):
        """test #7168, continues from #6910"""

        with testing.db.connect() as c1, testing.db.connect() as c2:
            try:
                with c1.begin():
                    c1.exec_driver_sql(
                        "create table #myveryveryuniquetemptablename (a int)"
                    )

                with c2.begin():
                    c2.exec_driver_sql(
                        "create table #myveryveryuniquetemptablename (a int)"
                    )

                assert c2.dialect.has_table(
                    c2, "#myveryveryuniquetemptablename"
                )
                c2.rollback()
            finally:
                with c1.begin():
                    c1.exec_driver_sql(
                        "drop table #myveryveryuniquetemptablename"
                    )
                with c2.begin():
                    c2.exec_driver_sql(
                        "drop table #myveryveryuniquetemptablename"
                    )

    @testing.fixture
    def temp_db_alt_collation_fixture(
        self, connection_no_trans, testing_engine
    ):
        temp_db_name = "%s_different_collation" % (
            provision.FOLLOWER_IDENT or "default"
        )
        cnxn = connection_no_trans.execution_options(
            isolation_level="AUTOCOMMIT"
        )
        cnxn.exec_driver_sql(f"DROP DATABASE IF EXISTS {temp_db_name}")
        cnxn.exec_driver_sql(
            f"CREATE DATABASE {temp_db_name} COLLATE Danish_Norwegian_CI_AS"
        )
        eng = testing_engine(
            url=testing.db.url.set(database=temp_db_name),
            options=dict(poolclass=NullPool),
        )

        yield eng

        cnxn.exec_driver_sql(f"DROP DATABASE IF EXISTS {temp_db_name}")

    def test_global_temp_different_collation(
        self, temp_db_alt_collation_fixture
    ):
        """test #8035"""

        tname = f"##foo{random.randint(1, 1000000)}"

        with temp_db_alt_collation_fixture.connect() as conn:
            conn.exec_driver_sql(f"CREATE TABLE {tname} (id int primary key)")
            conn.commit()

            eq_(
                inspect(conn).get_columns(tname),
                [
                    {
                        "name": "id",
                        "type": testing.eq_type_affinity(sqltypes.INTEGER),
                        "nullable": False,
                        "default": None,
                        "autoincrement": False,
                        "comment": None,
                    }
                ],
            )
            Table(tname, MetaData(), autoload_with=conn)

    @testing.combinations(
        ("test_schema"),
        ("[test_schema]"),
        argnames="schema_value",
    )
    @testing.variation(
        "reflection_operation", ["has_table", "reflect_table", "get_columns"]
    )
    def test_has_table_with_single_token_schema(
        self, metadata, connection, schema_value, reflection_operation
    ):
        """test for #9133"""
        tt = Table(
            "test", metadata, Column("id", Integer), schema=schema_value
        )
        tt.create(connection)

        if reflection_operation.has_table:
            is_true(inspect(connection).has_table("test", schema=schema_value))
        elif reflection_operation.reflect_table:
            m2 = MetaData()
            Table("test", m2, autoload_with=connection, schema=schema_value)
        elif reflection_operation.get_columns:
            is_true(
                inspect(connection).get_columns("test", schema=schema_value)
            )
        else:
            reflection_operation.fail()

    def test_db_qualified_items(self, metadata, connection):
        Table("foo", metadata, Column("id", Integer, primary_key=True))
        Table(
            "bar",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo_id", Integer, ForeignKey("foo.id", name="fkfoo")),
        )
        metadata.create_all(connection)

        dbname = connection.exec_driver_sql("select db_name()").scalar()
        owner = connection.exec_driver_sql("SELECT user_name()").scalar()
        referred_schema = "%(dbname)s.%(owner)s" % {
            "dbname": dbname,
            "owner": owner,
        }

        inspector = inspect(connection)
        bar_via_db = inspector.get_foreign_keys("bar", schema=referred_schema)
        eq_(
            bar_via_db,
            [
                {
                    "referred_table": "foo",
                    "referred_columns": ["id"],
                    "referred_schema": referred_schema,
                    "name": "fkfoo",
                    "constrained_columns": ["foo_id"],
                    "options": {},
                }
            ],
        )

        assert inspect(connection).has_table("bar", schema=referred_schema)

        m2 = MetaData()
        Table(
            "bar",
            m2,
            schema=referred_schema,
            autoload_with=connection,
        )
        eq_(m2.tables["%s.foo" % referred_schema].schema, referred_schema)

    def test_fk_on_unique_index(self, metadata, connection):
        # test for issue #7160
        Table(
            "uidx_parent",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("uidx_col1", Integer, nullable=False),
            Column("uidx_col2", Integer, nullable=False),
            Index(
                "UIDX_composite",
                "uidx_col1",
                "uidx_col2",
                unique=True,
            ),
        )

        Table(
            "uidx_child",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("parent_uidx_col1", Integer, nullable=False),
            Column("parent_uidx_col2", Integer, nullable=False),
            ForeignKeyConstraint(
                ["parent_uidx_col1", "parent_uidx_col2"],
                ["uidx_parent.uidx_col1", "uidx_parent.uidx_col2"],
                name="FK_uidx_parent",
            ),
        )

        metadata.create_all(connection)

        inspector = inspect(connection)
        fk_info = inspector.get_foreign_keys("uidx_child")
        eq_(
            fk_info,
            [
                {
                    "referred_table": "uidx_parent",
                    "referred_columns": ["uidx_col1", "uidx_col2"],
                    "referred_schema": None,
                    "name": "FK_uidx_parent",
                    "constrained_columns": [
                        "parent_uidx_col1",
                        "parent_uidx_col2",
                    ],
                    "options": {},
                }
            ],
        )

    def test_indexes_cols(self, metadata, connection):
        t1 = Table("t", metadata, Column("x", Integer), Column("y", Integer))
        Index("foo", t1.c.x, t1.c.y)
        metadata.create_all(connection)

        m2 = MetaData()
        t2 = Table("t", m2, autoload_with=connection)

        eq_(set(list(t2.indexes)[0].columns), {t2.c["x"], t2.c.y})

    def test_indexes_cols_with_commas(self, metadata, connection):
        t1 = Table(
            "t",
            metadata,
            Column("x, col", Integer, key="x"),
            Column("y", Integer),
        )
        Index("foo", t1.c.x, t1.c.y)
        metadata.create_all(connection)

        m2 = MetaData()
        t2 = Table("t", m2, autoload_with=connection)

        eq_(set(list(t2.indexes)[0].columns), {t2.c["x, col"], t2.c.y})

    def test_indexes_cols_with_spaces(self, metadata, connection):
        t1 = Table(
            "t",
            metadata,
            Column("x col", Integer, key="x"),
            Column("y", Integer),
        )
        Index("foo", t1.c.x, t1.c.y)
        metadata.create_all(connection)

        m2 = MetaData()
        t2 = Table("t", m2, autoload_with=connection)

        eq_(set(list(t2.indexes)[0].columns), {t2.c["x col"], t2.c.y})

    def test_indexes_with_filtered(self, metadata, connection):
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        Index("idx_x", t1.c.x, mssql_where=t1.c.x == "test")
        Index("idx_y", t1.c.y, mssql_where=t1.c.y >= 5)
        metadata.create_all(connection)
        ind = testing.db.dialect.get_indexes(connection, "t", None)

        filtered_indexes = []
        for ix in ind:
            if "dialect_options" in ix:
                filtered_indexes.append(ix["dialect_options"]["mssql_where"])

        eq_(sorted(filtered_indexes), ["([x]='test')", "([y]>=(5))"])

        t2 = Table("t", MetaData(), autoload_with=connection)
        idx = list(sorted(t2.indexes, key=lambda idx: idx.name))[0]
        self.assert_compile(
            CreateIndex(idx),
            "CREATE NONCLUSTERED INDEX idx_x ON t (x) WHERE ([x]='test')",
        )

    def test_index_reflection_clustered(self, metadata, connection):
        """
        when the result of get_indexes() is used to build an index it should
        include the CLUSTERED keyword when appropriate
        """
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        Index("idx_x", t1.c.x, mssql_clustered=True)
        Index("idx_y", t1.c.y)
        metadata.create_all(connection)
        ind = testing.db.dialect.get_indexes(connection, "t", None)

        clustered_index = ""
        for ix in ind:
            if ix["dialect_options"]["mssql_clustered"]:
                clustered_index = ix["name"]

        eq_(clustered_index, "idx_x")

        t2 = Table("t", MetaData(), autoload_with=connection)
        idx = list(sorted(t2.indexes, key=lambda idx: idx.name))[0]

        self.assert_compile(
            CreateIndex(idx), "CREATE CLUSTERED INDEX idx_x ON t (x)"
        )

    def test_index_reflection_filtered_and_clustered(
        self, metadata, connection
    ):
        """
        table with one filtered index and one clustered index so each index
        will have different dialect_options keys
        """
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        Index("idx_x", t1.c.x, mssql_clustered=True)
        Index("idx_y", t1.c.y, mssql_where=t1.c.y >= 5)
        metadata.create_all(connection)
        ind = testing.db.dialect.get_indexes(connection, "t", None)

        clustered_index = ""
        for ix in ind:
            if ix["dialect_options"]["mssql_clustered"]:
                clustered_index = ix["name"]
                is_false("mssql_columnstore" in ix["dialect_options"])

        eq_(clustered_index, "idx_x")

        filtered_indexes = []
        for ix in ind:
            if "dialect_options" in ix:
                if "mssql_where" in ix["dialect_options"]:
                    filtered_indexes.append(
                        ix["dialect_options"]["mssql_where"]
                    )

        eq_(sorted(filtered_indexes), ["([y]>=(5))"])

        t2 = Table("t", MetaData(), autoload_with=connection)
        clustered_idx = list(
            sorted(t2.indexes, key=lambda clustered_idx: clustered_idx.name)
        )[0]
        filtered_idx = list(
            sorted(t2.indexes, key=lambda filtered_idx: filtered_idx.name)
        )[1]

        self.assert_compile(
            CreateIndex(clustered_idx), "CREATE CLUSTERED INDEX idx_x ON t (x)"
        )

        self.assert_compile(
            CreateIndex(filtered_idx),
            "CREATE NONCLUSTERED INDEX idx_y ON t (y) WHERE ([y]>=(5))",
        )

    def test_index_reflection_nonclustered(self, metadata, connection):
        """
        one index created by specifying mssql_clustered=False
        one created without specifying mssql_clustered property so it will
        use default of NONCLUSTERED.
        When reflected back mssql_clustered=False should be included in both
        """
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        Index("idx_x", t1.c.x, mssql_clustered=False)
        Index("idx_y", t1.c.y)
        metadata.create_all(connection)
        ind = testing.db.dialect.get_indexes(connection, "t", None)

        for ix in ind:
            assert ix["dialect_options"]["mssql_clustered"] == False
            is_false("mssql_columnstore" in ix["dialect_options"])

        t2 = Table("t", MetaData(), autoload_with=connection)
        idx = list(sorted(t2.indexes, key=lambda idx: idx.name))[0]

        self.assert_compile(
            CreateIndex(idx), "CREATE NONCLUSTERED INDEX idx_x ON t (x)"
        )

    @testing.only_if("mssql>=12")
    def test_index_reflection_colstore_clustered(self, metadata, connection):
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
            Index("idx_x", mssql_clustered=True, mssql_columnstore=True),
        )
        Index("idx_y", t1.c.y)
        metadata.create_all(connection)
        ind = testing.db.dialect.get_indexes(connection, "t", None)

        for ix in ind:
            if ix["name"] == "idx_x":
                is_true(ix["dialect_options"]["mssql_clustered"])
                is_true(ix["dialect_options"]["mssql_columnstore"])
                eq_(ix["dialect_options"]["mssql_include"], [])
                eq_(ix["column_names"], [])
            else:
                is_false(ix["dialect_options"]["mssql_clustered"])
                is_false("mssql_columnstore" in ix["dialect_options"])

        t2 = Table("t", MetaData(), autoload_with=connection)
        idx = list(sorted(t2.indexes, key=lambda idx: idx.name))[0]

        self.assert_compile(
            CreateIndex(idx), "CREATE CLUSTERED COLUMNSTORE INDEX idx_x ON t"
        )

    @testing.only_if("mssql>=11")
    def test_index_reflection_colstore_nonclustered(
        self, metadata, connection
    ):
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        Index("idx_x", t1.c.x, mssql_clustered=False, mssql_columnstore=True)
        Index("idx_y", t1.c.y)
        metadata.create_all(connection)
        ind = testing.db.dialect.get_indexes(connection, "t", None)

        for ix in ind:
            is_false(ix["dialect_options"]["mssql_clustered"])
            if ix["name"] == "idx_x":
                is_true(ix["dialect_options"]["mssql_columnstore"])
                eq_(ix["dialect_options"]["mssql_include"], [])
                eq_(ix["column_names"], ["x"])
            else:
                is_false("mssql_columnstore" in ix["dialect_options"])

        t2 = Table("t", MetaData(), autoload_with=connection)
        idx = list(sorted(t2.indexes, key=lambda idx: idx.name))[0]

        self.assert_compile(
            CreateIndex(idx),
            "CREATE NONCLUSTERED COLUMNSTORE INDEX idx_x ON t (x)",
        )

    @testing.only_if("mssql>=11")
    def test_index_reflection_colstore_nonclustered_none(
        self, metadata, connection
    ):
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        Index("idx_x", t1.c.x, mssql_columnstore=True)
        Index("idx_y", t1.c.y)
        metadata.create_all(connection)
        ind = testing.db.dialect.get_indexes(connection, "t", None)

        for ix in ind:
            is_false(ix["dialect_options"]["mssql_clustered"])
            if ix["name"] == "idx_x":
                is_true(ix["dialect_options"]["mssql_columnstore"])
                eq_(ix["dialect_options"]["mssql_include"], [])
                eq_(ix["column_names"], ["x"])
            else:
                is_false("mssql_columnstore" in ix["dialect_options"])

        t2 = Table("t", MetaData(), autoload_with=connection)
        idx = list(sorted(t2.indexes, key=lambda idx: idx.name))[0]

        self.assert_compile(
            CreateIndex(idx),
            "CREATE NONCLUSTERED COLUMNSTORE INDEX idx_x ON t (x)",
        )

    @testing.only_if("mssql>=11")
    def test_index_reflection_colstore_nonclustered_multicol(
        self, metadata, connection
    ):
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        Index(
            "idx_xid",
            t1.c.x,
            t1.c.id,
            mssql_clustered=False,
            mssql_columnstore=True,
        )
        Index("idx_y", t1.c.y)
        metadata.create_all(connection)
        ind = testing.db.dialect.get_indexes(connection, "t", None)

        for ix in ind:
            is_false(ix["dialect_options"]["mssql_clustered"])
            if ix["name"] == "idx_xid":
                is_true(ix["dialect_options"]["mssql_columnstore"])
                eq_(ix["dialect_options"]["mssql_include"], [])
                eq_(ix["column_names"], ["x", "id"])
            else:
                is_false("mssql_columnstore" in ix["dialect_options"])

        t2 = Table("t", MetaData(), autoload_with=connection)
        idx = list(sorted(t2.indexes, key=lambda idx: idx.name))[0]

        self.assert_compile(
            CreateIndex(idx),
            "CREATE NONCLUSTERED COLUMNSTORE INDEX idx_xid ON t (x, id)",
        )

    def test_primary_key_reflection_clustered(self, metadata, connection):
        """
        A primary key will be clustered by default if no other clustered index
        exists.
        When reflected back, mssql_clustered=True should be present.
        """
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        PrimaryKeyConstraint(t1.c.id, name="pk_t")

        metadata.create_all(connection)
        pk_reflect = testing.db.dialect.get_pk_constraint(
            connection, "t", None
        )

        assert pk_reflect["dialect_options"]["mssql_clustered"] == True

    def test_primary_key_reflection_nonclustered(self, metadata, connection):
        """
        Nonclustered primary key should include mssql_clustered=False
        when reflected back
        """
        t1 = Table(
            "t",
            metadata,
            Column("id", Integer),
            Column("x", types.String(20)),
            Column("y", types.Integer),
        )
        PrimaryKeyConstraint(t1.c.id, name="pk_t", mssql_clustered=False)

        metadata.create_all(connection)
        pk_reflect = testing.db.dialect.get_pk_constraint(
            connection, "t", None
        )

        assert pk_reflect["dialect_options"]["mssql_clustered"] == False

    def test_max_ident_in_varchar_not_present(self, metadata, connection):
        """test [ticket:3504].

        Here we are testing not just that the "max" token comes back
        as None, but also that these types accept "max" as the value
        of "length" on construction, which isn't a directly documented
        pattern however is likely in common use.

        """

        Table(
            "t",
            metadata,
            Column("t1", types.String),
            Column("t2", types.Text("max")),
            Column("t3", types.Text("max")),
            Column("t4", types.LargeBinary("max")),
            Column("t5", types.VARBINARY("max")),
        )
        metadata.create_all(connection)
        for col in inspect(connection).get_columns("t"):
            is_(col["type"].length, None)
            in_("max", str(col["type"].compile(dialect=connection.dialect)))

    @testing.fixture
    def comment_table(self, metadata):
        Table(
            "tbl_with_comments",
            metadata,
            Column(
                "id",
                types.Integer,
                primary_key=True,
                comment="pk comment 🔑",
            ),
            Column("no_comment", types.Integer),
            Column(
                "has_comment",
                types.String(20),
                comment="has the comment § méil 📧",
            ),
            comment="table comment çòé 🐍",
        )
        metadata.create_all(testing.db)

    def test_comments(self, connection, comment_table):
        insp = inspect(connection)
        eq_(
            insp.get_table_comment("tbl_with_comments"),
            {"text": "table comment çòé 🐍"},
        )

        cols = {
            col["name"]: col["comment"]
            for col in insp.get_columns("tbl_with_comments")
        }
        eq_(
            cols,
            {
                "id": "pk comment 🔑",
                "no_comment": None,
                "has_comment": "has the comment § méil 📧",
            },
        )

    def test_comments_not_supported(self, testing_engine, comment_table):
        eng = testing_engine(options={"supports_comments": False})
        insp = inspect(eng)

        with expect_raises_message(
            NotImplementedError,
            "Can't get table comments on current SQL Server version in use",
        ):
            insp.get_table_comment("tbl_with_comments")

        # currently, column comments still reflect normally since we
        # aren't using an fn/sp for that

        cols = {
            col["name"]: col["comment"]
            for col in insp.get_columns("tbl_with_comments")
        }
        eq_(
            cols,
            {
                "id": "pk comment 🔑",
                "no_comment": None,
                "has_comment": "has the comment § méil 📧",
            },
        )

    def test_comments_with_dropped_column(self, metadata, connection):
        """test issue #12654"""

        Table(
            "tbl_with_comments",
            metadata,
            Column(
                "id", types.Integer, primary_key=True, comment="pk comment"
            ),
            Column("foobar", Integer, comment="comment_foobar"),
            Column("foo", Integer, comment="comment_foo"),
            Column(
                "bar",
                Integer,
                comment="comment_bar",
            ),
        )
        metadata.create_all(connection)
        insp = inspect(connection)
        eq_(
            {
                c["name"]: c["comment"]
                for c in insp.get_columns("tbl_with_comments")
            },
            {
                "id": "pk comment",
                "foobar": "comment_foobar",
                "foo": "comment_foo",
                "bar": "comment_bar",
            },
        )

        connection.exec_driver_sql(
            "ALTER TABLE [tbl_with_comments] DROP COLUMN [foobar]"
        )
        insp = inspect(connection)
        eq_(
            {
                c["name"]: c["comment"]
                for c in insp.get_columns("tbl_with_comments")
            },
            {
                "id": "pk comment",
                "foo": "comment_foo",
                "bar": "comment_bar",
            },
        )


class InfoCoerceUnicodeTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_info_unicode_cast_no_2000(self):
        dialect = mssql.dialect()
        dialect.server_version_info = base.MS_2000_VERSION
        stmt = tables.c.table_name == "somename"
        self.assert_compile(
            stmt,
            "[INFORMATION_SCHEMA].[TABLES].[TABLE_NAME] = :table_name_1",
            dialect=dialect,
        )

    def test_info_unicode_cast(self):
        dialect = mssql.dialect()
        dialect.server_version_info = base.MS_2005_VERSION
        stmt = tables.c.table_name == "somename"
        self.assert_compile(
            stmt,
            "[INFORMATION_SCHEMA].[TABLES].[TABLE_NAME] = "
            "CAST(:table_name_1 AS NVARCHAR(max))",
            dialect=dialect,
        )


class ReflectHugeViewTest(fixtures.TablesTest):
    __only_on__ = "mssql"
    __backend__ = True

    # crashes on freetds 0.91, not worth it
    __skip_if__ = (lambda: testing.requires.mssql_freetds.enabled,)

    @classmethod
    def define_tables(cls, metadata):
        col_num = 150

        t = Table(
            "base_table",
            metadata,
            *[
                Column("long_named_column_number_%d" % i, Integer)
                for i in range(col_num)
            ],
        )
        cls.view_str = view_str = (
            "CREATE VIEW huge_named_view AS SELECT %s FROM base_table"
            % (
                ",".join(
                    "long_named_column_number_%d" % i for i in range(col_num)
                )
            )
        )
        assert len(view_str) > 4000

        event.listen(t, "after_create", DDL(view_str))
        event.listen(t, "before_drop", DDL("DROP VIEW huge_named_view"))

    def test_inspect_view_definition(self):
        inspector = inspect(testing.db)
        view_def = inspector.get_view_definition("huge_named_view")
        eq_(view_def, self.view_str)


class OwnerPlusDBTest(fixtures.TestBase):
    def test_default_schema_name_not_interpreted_as_tokenized(self):
        dialect = mssql.dialect()
        dialect.server_version_info = base.MS_2014_VERSION

        mock_connection = mock.Mock(scalar=lambda sql: "Jonah.The.Whale")
        schema_name = dialect._get_default_schema_name(mock_connection)
        eq_(schema_name, "Jonah.The.Whale")
        eq_(
            base._owner_plus_db(dialect, schema_name),
            (None, "Jonah.The.Whale"),
        )

    def test_owner_database_pairs_dont_use_for_same_db(self):
        dialect = mssql.dialect()

        identifier = "my_db.some_schema"
        schema, owner = base._owner_plus_db(dialect, identifier)

        mock_connection = mock.Mock(
            dialect=dialect,
            exec_driver_sql=mock.Mock(
                return_value=mock.Mock(scalar=mock.Mock(return_value="my_db"))
            ),
        )
        mock_lambda = mock.Mock()
        base._switch_db(schema, mock_connection, mock_lambda, "x", y="bar")
        eq_(
            mock_connection.mock_calls,
            [mock.call.exec_driver_sql("select db_name()")],
        )
        eq_(
            mock_connection.exec_driver_sql.return_value.mock_calls,
            [mock.call.scalar()],
        ),
        eq_(mock_lambda.mock_calls, [mock.call("x", y="bar")])

    def test_owner_database_pairs_switch_for_different_db(self):
        dialect = mssql.dialect()

        identifier = "my_other_db.some_schema"
        schema, owner = base._owner_plus_db(dialect, identifier)

        mock_connection = mock.Mock(
            dialect=dialect,
            exec_driver_sql=mock.Mock(
                return_value=mock.Mock(scalar=mock.Mock(return_value="my_db"))
            ),
        )
        mock_lambda = mock.Mock()
        base._switch_db(schema, mock_connection, mock_lambda, "x", y="bar")
        eq_(
            mock_connection.mock_calls,
            [
                mock.call.exec_driver_sql("select db_name()"),
                mock.call.exec_driver_sql("use my_other_db"),
                mock.call.exec_driver_sql("use my_db"),
            ],
            eq_(
                mock_connection.exec_driver_sql.return_value.mock_calls,
                [mock.call.scalar()],
            ),
        )
        eq_(mock_lambda.mock_calls, [mock.call("x", y="bar")])

    @testing.combinations(
        ("foo", None, "foo", "use foo"),
        ("foo.bar", "foo", "bar", "use foo"),
        ("Foo.Bar", "Foo", "Bar", "use [Foo]"),
        ("[Foo.Bar]", None, "Foo.Bar", "use [Foo.Bar]"),
        ("[Foo.Bar].[bat]", "Foo.Bar", "bat", "use [Foo.Bar]"),
        (
            "[foo].]do something; select [foo",
            "foo",
            "do something; select foo",
            "use foo",
        ),
        (
            "something; select [foo].bar",
            "something; select foo",
            "bar",
            "use [something; select foo]",
        ),
        (
            "[abc].[def].[efg].[hij]",
            "[abc].[def].[efg]",
            "hij",
            "use [abc].[def].[efg]",
        ),
        ("abc.def.efg.hij", "abc.def.efg", "hij", "use [abc.def.efg]"),
    )
    def test_owner_database_pairs(
        self, identifier, expected_schema, expected_owner, use_stmt
    ):
        dialect = mssql.dialect()

        schema, owner = base._owner_plus_db(dialect, identifier)

        eq_(owner, expected_owner)
        eq_(schema, expected_schema)

        mock_connection = mock.Mock(
            dialect=dialect,
            exec_driver_sql=mock.Mock(
                return_value=mock.Mock(
                    scalar=mock.Mock(return_value="Some Database")
                )
            ),
        )
        mock_lambda = mock.Mock()
        base._switch_db(schema, mock_connection, mock_lambda, "x", y="bar")
        if schema is None:
            eq_(mock_connection.mock_calls, [])
        else:
            eq_(
                mock_connection.mock_calls,
                [
                    mock.call.exec_driver_sql("select db_name()"),
                    mock.call.exec_driver_sql(use_stmt),
                    mock.call.exec_driver_sql("use [Some Database]"),
                ],
            )
            eq_(
                mock_connection.exec_driver_sql.return_value.mock_calls,
                [mock.call.scalar()],
            )
        eq_(mock_lambda.mock_calls, [mock.call("x", y="bar")])


class IdentityReflectionTest(fixtures.TablesTest):
    __only_on__ = "mssql"
    __backend__ = True
    __requires__ = ("identity_columns",)

    @classmethod
    def define_tables(cls, metadata):
        for i, col in enumerate(
            [
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
                Column(
                    "id3",
                    sqltypes.BigInteger,
                    Identity(start=-9223372036854775808),
                ),
                Column("id4", sqltypes.SmallInteger, Identity()),
                Column("id5", sqltypes.Numeric, Identity()),
            ]
        ):
            Table("t%s" % i, metadata, col)

    def test_reflect_identity(self, connection):
        insp = inspect(connection)
        cols = []
        for t in self.tables_test_metadata.tables.keys():
            cols.extend(insp.get_columns(t))
        for col in cols:
            is_true("dialect_options" not in col)
            is_true("identity" in col)
            if col["name"] == "id1":
                eq_(col["identity"], {"start": 2, "increment": 3})
            elif col["name"] == "id2":
                eq_(col["identity"], {"start": 1, "increment": 1})
                eq_(type(col["identity"]["start"]), int)
                eq_(type(col["identity"]["increment"]), int)
            elif col["name"] == "id3":
                eq_(
                    col["identity"],
                    {"start": -9223372036854775808, "increment": 1},
                )
                eq_(type(col["identity"]["start"]), int)
                eq_(type(col["identity"]["increment"]), int)
            elif col["name"] == "id4":
                eq_(col["identity"], {"start": 1, "increment": 1})
                eq_(type(col["identity"]["start"]), int)
                eq_(type(col["identity"]["increment"]), int)
            elif col["name"] == "id5":
                eq_(col["identity"], {"start": 1, "increment": 1})
                eq_(type(col["identity"]["start"]), decimal.Decimal)
                eq_(type(col["identity"]["increment"]), decimal.Decimal)

    @testing.requires.views
    def test_reflect_views(self, connection):
        connection.exec_driver_sql("CREATE VIEW view1 AS SELECT * FROM t1")
        insp = inspect(connection)
        for col in insp.get_columns("view1"):
            is_true("dialect_options" not in col)
            is_true("identity" in col)
            eq_(col["identity"], {})
