# -*- encoding: utf-8
from sqlalchemy.testing import eq_
from sqlalchemy import *
from sqlalchemy import types, schema, event
from sqlalchemy.databases import mssql
from sqlalchemy.testing import fixtures, AssertsCompiledSQL, \
        ComparesTables
from sqlalchemy import testing
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import util


class ReflectionTest(fixtures.TestBase, ComparesTables):
    __only_on__ = 'mssql'

    @testing.provide_metadata
    def test_basic_reflection(self):
        meta = self.metadata

        users = Table(
            'engine_users',
            meta,
            Column('user_id', types.INT, primary_key=True),
            Column('user_name', types.VARCHAR(20), nullable=False),
            Column('test1', types.CHAR(5), nullable=False),
            Column('test2', types.Float(5), nullable=False),
            Column('test3', types.Text('max')),
            Column('test4', types.Numeric, nullable=False),
            Column('test5', types.DateTime),
            Column('parent_user_id', types.Integer,
                   ForeignKey('engine_users.user_id')),
            Column('test6', types.DateTime, nullable=False),
            Column('test7', types.Text('max')),
            Column('test8', types.LargeBinary('max')),
            Column('test_passivedefault2', types.Integer,
                   server_default='5'),
            Column('test9', types.BINARY(100)),
            Column('test_numeric', types.Numeric()),
            )

        addresses = Table(
            'engine_email_addresses',
            meta,
            Column('address_id', types.Integer, primary_key=True),
            Column('remote_user_id', types.Integer,
                   ForeignKey(users.c.user_id)),
            Column('email_address', types.String(20)),
            )
        meta.create_all()

        meta2 = MetaData()
        reflected_users = Table('engine_users', meta2,
                                autoload=True,
                                autoload_with=testing.db)
        reflected_addresses = Table('engine_email_addresses',
                meta2, autoload=True, autoload_with=testing.db)
        self.assert_tables_equal(users, reflected_users)
        self.assert_tables_equal(addresses, reflected_addresses)

    @testing.provide_metadata
    def test_identity(self):
        metadata = self.metadata
        table = Table(
            'identity_test', metadata,
            Column('col1', Integer, Sequence('fred', 2, 3), primary_key=True)
        )
        table.create()

        meta2 = MetaData(testing.db)
        table2 = Table('identity_test', meta2, autoload=True)
        sequence = isinstance(table2.c['col1'].default, schema.Sequence) \
                                and table2.c['col1'].default
        assert sequence.start == 2
        assert sequence.increment == 3

    @testing.emits_warning("Did not recognize")
    @testing.provide_metadata
    def test_skip_types(self):
        metadata = self.metadata
        testing.db.execute("""
            create table foo (id integer primary key, data xml)
        """)
        t1 = Table('foo', metadata, autoload=True)
        assert isinstance(t1.c.id.type, Integer)
        assert isinstance(t1.c.data.type, types.NullType)


    @testing.provide_metadata
    def test_db_qualified_items(self):
        metadata = self.metadata
        Table('foo', metadata, Column('id', Integer, primary_key=True))
        Table('bar', metadata,
                Column('id', Integer, primary_key=True),
                Column('foo_id', Integer, ForeignKey('foo.id', name="fkfoo"))
            )
        metadata.create_all()

        dbname = testing.db.scalar("select db_name()")
        owner = testing.db.scalar("SELECT user_name()")

        inspector = inspect(testing.db)
        bar_via_db = inspector.get_foreign_keys(
                            "bar", schema="%s.%s" % (dbname, owner))
        eq_(
            bar_via_db,
            [{
                'referred_table': 'foo',
                'referred_columns': ['id'],
                'referred_schema': 'test.dbo',
                'name': 'fkfoo',
                'constrained_columns': ['foo_id']}]
        )

        assert testing.db.has_table("bar", schema="test.dbo")

        m2 = MetaData()
        Table('bar', m2, schema="test.dbo", autoload=True,
                                autoload_with=testing.db)
        eq_(m2.tables["test.dbo.foo"].schema, "test.dbo")


    @testing.provide_metadata
    def test_indexes_cols(self):
        metadata = self.metadata

        t1 = Table('t', metadata, Column('x', Integer), Column('y', Integer))
        Index('foo', t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table('t', m2, autoload=True, autoload_with=testing.db)

        eq_(
            set(list(t2.indexes)[0].columns),
            set([t2.c['x'], t2.c.y])
        )

    @testing.provide_metadata
    def test_indexes_cols_with_commas(self):
        metadata = self.metadata

        t1 = Table('t', metadata,
                        Column('x, col', Integer, key='x'),
                        Column('y', Integer)
                    )
        Index('foo', t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table('t', m2, autoload=True, autoload_with=testing.db)

        eq_(
            set(list(t2.indexes)[0].columns),
            set([t2.c['x, col'], t2.c.y])
        )

    @testing.provide_metadata
    def test_indexes_cols_with_spaces(self):
        metadata = self.metadata

        t1 = Table('t', metadata, Column('x col', Integer, key='x'),
                                    Column('y', Integer))
        Index('foo', t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table('t', m2, autoload=True, autoload_with=testing.db)

        eq_(
            set(list(t2.indexes)[0].columns),
            set([t2.c['x col'], t2.c.y])
        )

from sqlalchemy.dialects.mssql.information_schema import CoerceUnicode, tables
from sqlalchemy.dialects.mssql import base

class InfoCoerceUnicodeTest(fixtures.TestBase, AssertsCompiledSQL):
    def test_info_unicode_coercion(self):

        dialect = mssql.dialect()
        value = CoerceUnicode().bind_processor(dialect)('a string')
        assert isinstance(value, util.text_type)

    def test_info_unicode_cast_no_2000(self):
        dialect = mssql.dialect()
        dialect.server_version_info = base.MS_2000_VERSION
        stmt = tables.c.table_name == 'somename'
        self.assert_compile(
            stmt,
            "[TABLES_1].[TABLE_NAME] = :table_name_1",
            dialect=dialect
        )

    def test_info_unicode_cast(self):
        dialect = mssql.dialect()
        dialect.server_version_info = base.MS_2005_VERSION
        stmt = tables.c.table_name == 'somename'
        self.assert_compile(
            stmt,
            "[TABLES_1].[TABLE_NAME] = CAST(:table_name_1 AS NVARCHAR(max))",
            dialect=dialect
        )

class ReflectHugeViewTest(fixtures.TestBase):
    __only_on__ = 'mssql'

    # crashes on freetds 0.91, not worth it
    __skip_if__ = (
        lambda: testing.requires.mssql_freetds.enabled,
    )

    def setup(self):
        self.col_num = 150

        self.metadata = MetaData(testing.db)
        t = Table('base_table', self.metadata,
                *[
                    Column("long_named_column_number_%d" % i, Integer)
                    for i in range(self.col_num)
                ]
        )
        self.view_str = view_str = \
            "CREATE VIEW huge_named_view AS SELECT %s FROM base_table" % (
            ",".join("long_named_column_number_%d" % i
                        for i in range(self.col_num))
            )
        assert len(view_str) > 4000

        event.listen(t, 'after_create', DDL(view_str) )
        event.listen(t, 'before_drop', DDL("DROP VIEW huge_named_view") )

        self.metadata.create_all()

    def teardown(self):
        self.metadata.drop_all()

    def test_inspect_view_definition(self):
        inspector = Inspector.from_engine(testing.db)
        view_def = inspector.get_view_definition("huge_named_view")
        eq_(view_def, self.view_str)

