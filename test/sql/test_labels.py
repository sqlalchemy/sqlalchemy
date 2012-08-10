from test.lib.testing import assert_raises, assert_raises_message, eq_
from test.lib.engines import testing_engine
from test.lib import fixtures, AssertsCompiledSQL, testing
from sqlalchemy import *
from sqlalchemy import exc as exceptions
from sqlalchemy.engine import default
from sqlalchemy.sql import table, column
from test.lib.schema import Table, Column

IDENT_LENGTH = 29

class LabelTypeTest(fixtures.TestBase):
    def test_type(self):
        m = MetaData()
        t = Table('sometable', m,
            Column('col1', Integer),
            Column('col2', Float))
        assert isinstance(t.c.col1.label('hi').type, Integer)
        assert isinstance(select([t.c.col2]).as_scalar().label('lala').type,
                    Float)

class LongLabelsTest(fixtures.TablesTest, AssertsCompiledSQL):
    run_inserts = 'once'
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        table1 = Table("some_large_named_table", metadata,
            Column("this_is_the_primarykey_column", Integer,
                            primary_key=True,
                            test_needs_autoincrement=True),
            Column("this_is_the_data_column", String(30))
            )

        table2 = Table("table_with_exactly_29_characs", metadata,
            Column("this_is_the_primarykey_column", Integer,
                            primary_key=True,
                            test_needs_autoincrement=True),
            Column("this_is_the_data_column", String(30))
            )
        cls.tables.table1 = table1
        cls.tables.table2 = table2

    @classmethod
    def insert_data(cls):
        table1 = cls.tables.table1
        table2 = cls.tables.table2
        for data in [
            {"this_is_the_primarykey_column":1,
                        "this_is_the_data_column":"data1"},
            {"this_is_the_primarykey_column":2,
                        "this_is_the_data_column":"data2"},
            {"this_is_the_primarykey_column":3,
                        "this_is_the_data_column":"data3"},
            {"this_is_the_primarykey_column":4,
                        "this_is_the_data_column":"data4"}
        ]:
            testing.db.execute(
                table1.insert(),
                **data
            )
        testing.db.execute(
            table2.insert(),
            {"this_is_the_primary_key_column":1,
            "this_is_the_data_column":"data"}
        )

    @classmethod
    def setup_class(cls):
        super(LongLabelsTest, cls).setup_class()
        cls.maxlen = testing.db.dialect.max_identifier_length
        testing.db.dialect.max_identifier_length = IDENT_LENGTH

    @classmethod
    def teardown_class(cls):
        testing.db.dialect.max_identifier_length = cls.maxlen
        super(LongLabelsTest, cls).teardown_class()

    def test_too_long_name_disallowed(self):
        m = MetaData(testing.db)
        t1 = Table("this_name_is_too_long_for_what_were_doing_in_this_test",
                        m, Column('foo', Integer))
        assert_raises(exceptions.IdentifierError, m.create_all)
        assert_raises(exceptions.IdentifierError, m.drop_all)
        assert_raises(exceptions.IdentifierError, t1.create)
        assert_raises(exceptions.IdentifierError, t1.drop)

    def test_basic_result(self):
        table1 = self.tables.table1
        s = table1.select(use_labels=True,
                        order_by=[table1.c.this_is_the_primarykey_column])

        result = [
            (row[table1.c.this_is_the_primarykey_column],
            row[table1.c.this_is_the_data_column])
            for row in testing.db.execute(s)
        ]
        eq_(result, [
            (1, "data1"),
            (2, "data2"),
            (3, "data3"),
            (4, "data4"),
        ])

    def test_result_limit(self):
        table1 = self.tables.table1
        # some dialects such as oracle (and possibly ms-sql
        # in a future version)
        # generate a subquery for limits/offsets.
        # ensure that the generated result map corresponds
        # to the selected table, not
        # the select query
        s = table1.select(use_labels=True,
                        order_by=[table1.c.this_is_the_primarykey_column]).\
                        limit(2)

        result = [
            (row[table1.c.this_is_the_primarykey_column],
            row[table1.c.this_is_the_data_column])
            for row in testing.db.execute(s)
        ]
        eq_(result, [
            (1, "data1"),
            (2, "data2"),
        ])

    @testing.requires.offset
    def test_result_limit_offset(self):
        table1 = self.tables.table1
        s = table1.select(use_labels=True,
                        order_by=[table1.c.this_is_the_primarykey_column]).\
                        limit(2).offset(1)

        result = [
            (row[table1.c.this_is_the_primarykey_column],
            row[table1.c.this_is_the_data_column])
            for row in testing.db.execute(s)
        ]
        eq_(result, [
            (2, "data2"),
            (3, "data3"),
        ])

    def test_table_alias_1(self):
        table2 = self.tables.table2
        if testing.against('oracle'):
            self.assert_compile(
                table2.alias().select(),
                "SELECT table_with_exactly_29_c_1."
                "this_is_the_primarykey_column, "
                "table_with_exactly_29_c_1.this_is_the_data_column "
                "FROM table_with_exactly_29_characs "
                "table_with_exactly_29_c_1"
            )
        else:
            self.assert_compile(
                table2.alias().select(),
                "SELECT table_with_exactly_29_c_1."
                "this_is_the_primarykey_column, "
                "table_with_exactly_29_c_1.this_is_the_data_column "
                "FROM table_with_exactly_29_characs AS "
                "table_with_exactly_29_c_1"
            )

    def test_table_alias_2(self):
        table1 = self.tables.table1
        table2 = self.tables.table2
        ta = table2.alias()
        dialect = default.DefaultDialect()
        dialect.max_identifier_length = IDENT_LENGTH
        self.assert_compile(
            select([table1, ta]).select_from(
                        table1.join(ta,
                            table1.c.this_is_the_data_column==
                            ta.c.this_is_the_data_column)).\
                        where(ta.c.this_is_the_data_column=='data3'),

            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column, "
            "table_with_exactly_29_c_1.this_is_the_primarykey_column, "
            "table_with_exactly_29_c_1.this_is_the_data_column FROM "
            "some_large_named_table JOIN table_with_exactly_29_characs "
            "AS table_with_exactly_29_c_1 ON "
            "some_large_named_table.this_is_the_data_column = "
            "table_with_exactly_29_c_1.this_is_the_data_column "
            "WHERE table_with_exactly_29_c_1.this_is_the_data_column = "
            ":this_is_the_data_column_1",
            dialect=dialect
        )

    def test_table_alias_3(self):
        table2 = self.tables.table2
        eq_(
            testing.db.execute(table2.alias().select()).first(),
            (1, "data")
        )

    def test_colbinds(self):
        table1 = self.tables.table1
        r = table1.select(table1.c.this_is_the_primarykey_column == 4).\
                    execute()
        assert r.fetchall() == [(4, "data4")]

        r = table1.select(or_(
            table1.c.this_is_the_primarykey_column == 4,
            table1.c.this_is_the_primarykey_column == 2
        )).execute()
        assert r.fetchall() == [(2, "data2"), (4, "data4")]

    @testing.provide_metadata
    def test_insert_no_pk(self):
        t = Table("some_other_large_named_table", self.metadata,
            Column("this_is_the_primarykey_column", Integer,
                            Sequence("this_is_some_large_seq"),
                            primary_key=True),
            Column("this_is_the_data_column", String(30))
            )
        t.create(testing.db, checkfirst=True)
        testing.db.execute(t.insert(),
                **{"this_is_the_data_column":"data1"})

    @testing.requires.subqueries
    def test_subquery(self):
        table1 = self.tables.table1
        q = table1.select(table1.c.this_is_the_primarykey_column == 4).\
                        alias('foo')
        eq_(
            list(testing.db.execute(select([q]))),
            [(4, u'data4')]
        )

    @testing.requires.subqueries
    def test_anon_alias(self):
        table1 = self.tables.table1
        compile_dialect = default.DefaultDialect()
        compile_dialect.max_identifier_length = IDENT_LENGTH

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias()
        x = select([q], use_labels=True)

        self.assert_compile(x,
            "SELECT anon_1.this_is_the_primarykey_column AS "
            "anon_1_this_is_the_prim_1, anon_1.this_is_the_data_column "
            "AS anon_1_this_is_the_data_2 "
            "FROM (SELECT some_large_named_table."
            "this_is_the_primarykey_column AS "
            "this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "AS this_is_the_data_column "
            "FROM some_large_named_table "
            "WHERE some_large_named_table.this_is_the_primarykey_column "
            "= :this_is_the_primarykey__1) AS anon_1",
            dialect=compile_dialect)

        eq_(
            list(testing.db.execute(x)),
            [(4, u'data4')]
        )

    def test_adjustable(self):
        table1 = self.tables.table1

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])

        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(x,
            "SELECT foo.this_1, foo.this_2 FROM "
            "(SELECT some_large_named_table."
            "this_is_the_primarykey_column AS this_1, "
            "some_large_named_table.this_is_the_data_column AS this_2 "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = :this_1) AS foo",
            dialect=compile_dialect)

        compile_dialect = default.DefaultDialect(label_length=4)
        self.assert_compile(x, "SELECT foo._1, foo._2 FROM "
            "(SELECT some_large_named_table.this_is_the_primarykey_column "
            "AS _1, some_large_named_table.this_is_the_data_column AS _2 "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = :_1) AS foo",
        dialect=compile_dialect)

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias()
        x = select([q], use_labels=True)

        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(x,
            "SELECT anon_1.this_2 AS anon_1, anon_1.this_4 AS anon_3 FROM "
            "(SELECT some_large_named_table.this_is_the_primarykey_column "
            "AS this_2, some_large_named_table.this_is_the_data_column AS this_4 "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = :this_1) AS anon_1",
            dialect=compile_dialect)

        compile_dialect = default.DefaultDialect(label_length=4)
        self.assert_compile(x, "SELECT _1._2 AS _1, _1._4 AS _3 FROM "
            "(SELECT some_large_named_table.this_is_the_primarykey_column "
            "AS _2, some_large_named_table.this_is_the_data_column AS _4 "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = :_1) AS _1",
            dialect=compile_dialect)

    def test_adjustable_result_schema_column(self):
        table1 = self.tables.table1

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])

        e = testing_engine(options={"label_length":10})
        e.pool = testing.db.pool
        row = e.execute(x).first()
        eq_(row.this_is_the_primarykey_column, 4)
        eq_(row.this_1, 4)
        eq_(row['this_1'], 4)

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias('foo')
        row = e.execute(x).first()
        eq_(row.this_is_the_primarykey_column, 4)
        eq_(row.this_1, 4)

    def test_adjustable_result_lightweight_column(self):

        table1 = table("some_large_named_table",
            column("this_is_the_primarykey_column"),
            column("this_is_the_data_column")
        )

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])

        e = testing_engine(options={"label_length":10})
        e.pool = testing.db.pool
        row = e.execute(x).first()
        eq_(row.this_is_the_primarykey_column, 4)
        eq_(row.this_1, 4)

    def test_table_plus_column_exceeds_length(self):
        """test that the truncation occurs if tablename / colname are only
        greater than the max when concatenated."""

        compile_dialect = default.DefaultDialect(label_length=30)
        m = MetaData()
        a_table = Table(
            'thirty_characters_table_xxxxxx',
            m,
            Column('id', Integer, primary_key=True)
        )

        other_table = Table(
            'other_thirty_characters_table_',
            m,
            Column('id', Integer, primary_key=True),
            Column('thirty_characters_table_id',
                Integer,
                ForeignKey('thirty_characters_table_xxxxxx.id'),
                primary_key=True
            )
        )

        anon = a_table.alias()
        self.assert_compile(
            select([other_table,anon]).
                            select_from(
                                other_table.outerjoin(anon)
                        ).apply_labels(),
            "SELECT other_thirty_characters_table_.id AS "
            "other_thirty_characters__1, "
            "other_thirty_characters_table_.thirty_characters_table_id "
            "AS other_thirty_characters__2, thirty_characters_table__1.id "
            "AS thirty_characters_table__3 "
            "FROM other_thirty_characters_table_ "
            "LEFT OUTER JOIN thirty_characters_table_xxxxxx "
            "AS thirty_characters_table__1 ON "
            "thirty_characters_table__1.id = "
            "other_thirty_characters_table_.thirty_characters_table_id",
            dialect=compile_dialect)

        self.assert_compile(
                select([other_table, anon]).
                    select_from(
                                other_table.outerjoin(anon)
                    ).apply_labels(),
            "SELECT other_thirty_characters_table_.id AS "
            "other_thirty_characters__1, "
            "other_thirty_characters_table_.thirty_characters_table_id "
            "AS other_thirty_characters__2, "
            "thirty_characters_table__1.id AS thirty_characters_table__3 "
            "FROM other_thirty_characters_table_ "
            "LEFT OUTER JOIN thirty_characters_table_xxxxxx "
            "AS thirty_characters_table__1 ON "
            "thirty_characters_table__1.id = "
            "other_thirty_characters_table_.thirty_characters_table_id",
            dialect=compile_dialect
        )
