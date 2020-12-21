# -*- encoding: utf-8
import datetime
import decimal

from sqlalchemy import Column
from sqlalchemy import DDL
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy import ForeignKey
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
from sqlalchemy import util
from sqlalchemy.dialects import mssql
from sqlalchemy.dialects.mssql import base
from sqlalchemy.dialects.mssql.information_schema import CoerceUnicode
from sqlalchemy.dialects.mssql.information_schema import tables
from sqlalchemy.schema import CreateIndex
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import ComparesTables
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock


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
                        },
                        {
                            "name": "data",
                            "type": testing.eq_type_affinity(
                                sqltypes.NullType
                            ),
                            "nullable": True,
                            "default": None,
                            "autoincrement": False,
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
                        "txt": u"foo",
                        "dt2": datetime.datetime(2020, 1, 1, 1, 1, 1),
                    },
                    {
                        "id": 2,
                        "txt": u"bar",
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

    def test_indexes_cols(self, metadata, connection):

        t1 = Table("t", metadata, Column("x", Integer), Column("y", Integer))
        Index("foo", t1.c.x, t1.c.y)
        metadata.create_all(connection)

        m2 = MetaData()
        t2 = Table("t", m2, autoload_with=connection)

        eq_(set(list(t2.indexes)[0].columns), set([t2.c["x"], t2.c.y]))

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

        eq_(set(list(t2.indexes)[0].columns), set([t2.c["x, col"], t2.c.y]))

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

        eq_(set(list(t2.indexes)[0].columns), set([t2.c["x col"], t2.c.y]))

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
            CreateIndex(idx), "CREATE INDEX idx_x ON t (x) WHERE ([x]='test')"
        )

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


class InfoCoerceUnicodeTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_info_unicode_coercion(self):

        dialect = mssql.dialect()
        value = CoerceUnicode().bind_processor(dialect)("a string")
        assert isinstance(value, util.text_type)

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
            ]
        )
        cls.view_str = (
            view_str
        ) = "CREATE VIEW huge_named_view AS SELECT %s FROM base_table" % (
            ",".join("long_named_column_number_%d" % i for i in range(col_num))
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
                Column("id3", sqltypes.BigInteger, Identity()),
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
                eq_(col["identity"], {"start": 1, "increment": 1})
                eq_(type(col["identity"]["start"]), util.compat.long_type)
                eq_(type(col["identity"]["increment"]), util.compat.long_type)
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
