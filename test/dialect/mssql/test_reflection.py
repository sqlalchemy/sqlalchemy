# -*- encoding: utf-8
from sqlalchemy import Column
from sqlalchemy import DDL
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import schema
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.databases import mssql
from sqlalchemy.dialects.mssql import base
from sqlalchemy.dialects.mssql.information_schema import CoerceUnicode
from sqlalchemy.dialects.mssql.information_schema import tables
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import ComparesTables
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import in_
from sqlalchemy.testing import is_
from sqlalchemy.testing import mock


class ReflectionTest(fixtures.TestBase, ComparesTables, AssertsCompiledSQL):
    __only_on__ = "mssql"
    __backend__ = True

    @testing.provide_metadata
    def test_basic_reflection(self):
        meta = self.metadata

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
        meta.create_all()

        meta2 = MetaData()
        reflected_users = Table(
            "engine_users", meta2, autoload=True, autoload_with=testing.db
        )
        reflected_addresses = Table(
            "engine_email_addresses",
            meta2,
            autoload=True,
            autoload_with=testing.db,
        )
        self.assert_tables_equal(users, reflected_users)
        self.assert_tables_equal(addresses, reflected_addresses)

    @testing.provide_metadata
    def _test_specific_type(self, type_obj, ddl):
        metadata = self.metadata

        table = Table("type_test", metadata, Column("col1", type_obj))
        table.create()

        m2 = MetaData()
        table2 = Table("type_test", m2, autoload_with=testing.db)
        self.assert_compile(
            schema.CreateTable(table2),
            "CREATE TABLE type_test (col1 %s NULL)" % ddl,
        )

    def test_xml_type(self):
        self._test_specific_type(mssql.XML, "XML")

    def test_image_type(self):
        self._test_specific_type(mssql.IMAGE, "IMAGE")

    def test_money_type(self):
        self._test_specific_type(mssql.MONEY, "MONEY")

    def test_numeric_prec_scale(self):
        self._test_specific_type(mssql.NUMERIC(10, 2), "NUMERIC(10, 2)")

    def test_float(self):
        self._test_specific_type(mssql.FLOAT, "FLOAT(53)")

    def test_real(self):
        self._test_specific_type(mssql.REAL, "REAL")

    def test_float_as_real(self):
        # FLOAT(5) comes back as REAL
        self._test_specific_type(mssql.FLOAT(5), "REAL")

    @testing.provide_metadata
    def test_identity(self):
        metadata = self.metadata
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
        table.create()

        meta2 = MetaData(testing.db)
        table2 = Table("identity_test", meta2, autoload=True)
        eq_(table2.c["col1"].dialect_options["mssql"]["identity_start"], 2)
        eq_(table2.c["col1"].dialect_options["mssql"]["identity_increment"], 3)

    @testing.emits_warning("Did not recognize")
    @testing.provide_metadata
    def test_skip_types(self):
        metadata = self.metadata
        testing.db.execute(
            """
            create table foo (id integer primary key, data xml)
        """
        )
        with mock.patch.object(
            testing.db.dialect, "ischema_names", {"int": mssql.INTEGER}
        ):
            t1 = Table("foo", metadata, autoload=True)
        assert isinstance(t1.c.id.type, Integer)
        assert isinstance(t1.c.data.type, types.NullType)

    @testing.provide_metadata
    def test_cross_schema_fk_pk_name_overlaps(self):
        # test for issue #4228
        metadata = self.metadata

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

        metadata.create_all()

        insp = inspect(testing.db)
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

    @testing.provide_metadata
    def test_table_name_that_is_greater_than_16_chars(self):
        metadata = self.metadata
        Table(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer),
            Index("foo_idx", "foo"),
        )
        metadata.create_all()

        t = Table(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ", MetaData(), autoload_with=testing.db
        )
        eq_(t.name, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")

    @testing.provide_metadata
    def test_db_qualified_items(self):
        metadata = self.metadata
        Table("foo", metadata, Column("id", Integer, primary_key=True))
        Table(
            "bar",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo_id", Integer, ForeignKey("foo.id", name="fkfoo")),
        )
        metadata.create_all()

        dbname = testing.db.scalar("select db_name()")
        owner = testing.db.scalar("SELECT user_name()")
        referred_schema = "%(dbname)s.%(owner)s" % {
            "dbname": dbname,
            "owner": owner,
        }

        inspector = inspect(testing.db)
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

        assert testing.db.has_table("bar", schema=referred_schema)

        m2 = MetaData()
        Table(
            "bar",
            m2,
            schema=referred_schema,
            autoload=True,
            autoload_with=testing.db,
        )
        eq_(m2.tables["%s.foo" % referred_schema].schema, referred_schema)

    @testing.provide_metadata
    def test_indexes_cols(self):
        metadata = self.metadata

        t1 = Table("t", metadata, Column("x", Integer), Column("y", Integer))
        Index("foo", t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table("t", m2, autoload=True, autoload_with=testing.db)

        eq_(set(list(t2.indexes)[0].columns), set([t2.c["x"], t2.c.y]))

    @testing.provide_metadata
    def test_indexes_cols_with_commas(self):
        metadata = self.metadata

        t1 = Table(
            "t",
            metadata,
            Column("x, col", Integer, key="x"),
            Column("y", Integer),
        )
        Index("foo", t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table("t", m2, autoload=True, autoload_with=testing.db)

        eq_(set(list(t2.indexes)[0].columns), set([t2.c["x, col"], t2.c.y]))

    @testing.provide_metadata
    def test_indexes_cols_with_spaces(self):
        metadata = self.metadata

        t1 = Table(
            "t",
            metadata,
            Column("x col", Integer, key="x"),
            Column("y", Integer),
        )
        Index("foo", t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table("t", m2, autoload=True, autoload_with=testing.db)

        eq_(set(list(t2.indexes)[0].columns), set([t2.c["x col"], t2.c.y]))

    @testing.provide_metadata
    def test_max_ident_in_varchar_not_present(self):
        """test [ticket:3504].

        Here we are testing not just that the "max" token comes back
        as None, but also that these types accept "max" as the value
        of "length" on construction, which isn't a directly documented
        pattern however is likely in common use.

        """
        metadata = self.metadata

        Table(
            "t",
            metadata,
            Column("t1", types.String),
            Column("t2", types.Text("max")),
            Column("t3", types.Text("max")),
            Column("t4", types.LargeBinary("max")),
            Column("t5", types.VARBINARY("max")),
        )
        metadata.create_all()
        for col in inspect(testing.db).get_columns("t"):
            is_(col["type"].length, None)
            in_("max", str(col["type"].compile(dialect=testing.db.dialect)))


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


class ReflectHugeViewTest(fixtures.TestBase):
    __only_on__ = "mssql"
    __backend__ = True

    # crashes on freetds 0.91, not worth it
    __skip_if__ = (lambda: testing.requires.mssql_freetds.enabled,)

    def setup(self):
        self.col_num = 150

        self.metadata = MetaData(testing.db)
        t = Table(
            "base_table",
            self.metadata,
            *[
                Column("long_named_column_number_%d" % i, Integer)
                for i in range(self.col_num)
            ]
        )
        self.view_str = (
            view_str
        ) = "CREATE VIEW huge_named_view AS SELECT %s FROM base_table" % (
            ",".join(
                "long_named_column_number_%d" % i for i in range(self.col_num)
            )
        )
        assert len(view_str) > 4000

        event.listen(t, "after_create", DDL(view_str))
        event.listen(t, "before_drop", DDL("DROP VIEW huge_named_view"))

        self.metadata.create_all()

    def teardown(self):
        self.metadata.drop_all()

    def test_inspect_view_definition(self):
        inspector = Inspector.from_engine(testing.db)
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
            dialect=dialect, scalar=mock.Mock(return_value="my_db")
        )
        mock_lambda = mock.Mock()
        base._switch_db(schema, mock_connection, mock_lambda, "x", y="bar")
        eq_(mock_connection.mock_calls, [mock.call.scalar("select db_name()")])
        eq_(mock_lambda.mock_calls, [mock.call("x", y="bar")])

    def test_owner_database_pairs_switch_for_different_db(self):
        dialect = mssql.dialect()

        identifier = "my_other_db.some_schema"
        schema, owner = base._owner_plus_db(dialect, identifier)

        mock_connection = mock.Mock(
            dialect=dialect, scalar=mock.Mock(return_value="my_db")
        )
        mock_lambda = mock.Mock()
        base._switch_db(schema, mock_connection, mock_lambda, "x", y="bar")
        eq_(
            mock_connection.mock_calls,
            [
                mock.call.scalar("select db_name()"),
                mock.call.execute("use my_other_db"),
                mock.call.execute("use my_db"),
            ],
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
            scalar=mock.Mock(return_value="Some  Database"),
        )
        mock_lambda = mock.Mock()
        base._switch_db(schema, mock_connection, mock_lambda, "x", y="bar")
        if schema is None:
            eq_(mock_connection.mock_calls, [])
        else:
            eq_(
                mock_connection.mock_calls,
                [
                    mock.call.scalar("select db_name()"),
                    mock.call.execute(use_stmt),
                    mock.call.execute("use [Some  Database]"),
                ],
            )
        eq_(mock_lambda.mock_calls, [mock.call("x", y="bar")])
