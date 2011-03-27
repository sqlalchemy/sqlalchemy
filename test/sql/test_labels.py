from test.lib.testing import assert_raises, assert_raises_message
from sqlalchemy import *
from sqlalchemy import exc as exceptions
from test.lib import *
from sqlalchemy.engine import default

IDENT_LENGTH = 29

class LabelTypeTest(fixtures.TestBase):
    def test_type(self):
        m = MetaData()
        t = Table('sometable', m,
            Column('col1', Integer),
            Column('col2', Float))
        assert isinstance(t.c.col1.label('hi').type, Integer)
        assert isinstance(select([t.c.col2]).as_scalar().label('lala').type, Float)

class LongLabelsTest(fixtures.TestBase, AssertsCompiledSQL):
    @classmethod
    def setup_class(cls):
        global metadata, table1, table2, maxlen
        metadata = MetaData(testing.db)
        table1 = Table("some_large_named_table", metadata,
            Column("this_is_the_primarykey_column", Integer, Sequence("this_is_some_large_seq"), primary_key=True),
            Column("this_is_the_data_column", String(30))
            )

        table2 = Table("table_with_exactly_29_characs", metadata,
            Column("this_is_the_primarykey_column", Integer, Sequence("some_seq"), primary_key=True),
            Column("this_is_the_data_column", String(30))
            )

        metadata.create_all()

        maxlen = testing.db.dialect.max_identifier_length
        testing.db.dialect.max_identifier_length = IDENT_LENGTH

    @engines.close_first
    def teardown(self):
        table1.delete().execute()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()
        testing.db.dialect.max_identifier_length = maxlen

    def test_too_long_name_disallowed(self):
        m = MetaData(testing.db)
        t1 = Table("this_name_is_too_long_for_what_were_doing_in_this_test", m, Column('foo', Integer))
        assert_raises(exceptions.IdentifierError, m.create_all)
        assert_raises(exceptions.IdentifierError, m.drop_all)
        assert_raises(exceptions.IdentifierError, t1.create)
        assert_raises(exceptions.IdentifierError, t1.drop)

    def test_result(self):
        table1.insert().execute(**{"this_is_the_primarykey_column":1, "this_is_the_data_column":"data1"})
        table1.insert().execute(**{"this_is_the_primarykey_column":2, "this_is_the_data_column":"data2"})
        table1.insert().execute(**{"this_is_the_primarykey_column":3, "this_is_the_data_column":"data3"})
        table1.insert().execute(**{"this_is_the_primarykey_column":4, "this_is_the_data_column":"data4"})

        s = table1.select(use_labels=True, order_by=[table1.c.this_is_the_primarykey_column])
        r = s.execute()
        result = []
        for row in r:
            result.append((row[table1.c.this_is_the_primarykey_column], row[table1.c.this_is_the_data_column]))
        assert result == [
            (1, "data1"),
            (2, "data2"),
            (3, "data3"),
            (4, "data4"),
        ], repr(result)

        # some dialects such as oracle (and possibly ms-sql in a future version)
        # generate a subquery for limits/offsets.
        # ensure that the generated result map corresponds to the selected table, not
        # the select query
        r = s.limit(2).execute()
        result = []
        for row in r:
            result.append((row[table1.c.this_is_the_primarykey_column], row[table1.c.this_is_the_data_column]))
        assert result == [
            (1, "data1"),
            (2, "data2"),
        ], repr(result)

        @testing.requires.offset
        def go():
            r = s.limit(2).offset(1).execute()
            result = []
            for row in r:
                result.append((row[table1.c.this_is_the_primarykey_column], row[table1.c.this_is_the_data_column]))
            assert result == [
                (2, "data2"),
                (3, "data3"),
            ], repr(result)
        go()

    def test_table_alias_names(self):
        if testing.against('oracle'):
            self.assert_compile(
                table2.alias().select(),
                "SELECT table_with_exactly_29_c_1.this_is_the_primarykey_column, table_with_exactly_29_c_1.this_is_the_data_column FROM table_with_exactly_29_characs table_with_exactly_29_c_1"
            )
        else:
            self.assert_compile(
                table2.alias().select(),
                "SELECT table_with_exactly_29_c_1.this_is_the_primarykey_column, table_with_exactly_29_c_1.this_is_the_data_column FROM table_with_exactly_29_characs AS table_with_exactly_29_c_1"
            )

        ta = table2.alias()
        dialect = default.DefaultDialect()
        dialect.max_identifier_length = IDENT_LENGTH
        self.assert_compile(
            select([table1, ta]).select_from(table1.join(ta, table1.c.this_is_the_data_column==ta.c.this_is_the_data_column)).\
                        where(ta.c.this_is_the_data_column=='data3'),

            "SELECT some_large_named_table.this_is_the_primarykey_column, some_large_named_table.this_is_the_data_column, "
            "table_with_exactly_29_c_1.this_is_the_primarykey_column, table_with_exactly_29_c_1.this_is_the_data_column FROM "
            "some_large_named_table JOIN table_with_exactly_29_characs AS table_with_exactly_29_c_1 ON "
            "some_large_named_table.this_is_the_data_column = table_with_exactly_29_c_1.this_is_the_data_column "
            "WHERE table_with_exactly_29_c_1.this_is_the_data_column = :this_is_the_data_column_1",
            dialect=dialect
        )

        table2.insert().execute(
            {"this_is_the_primarykey_column":1, "this_is_the_data_column":"data1"},
            {"this_is_the_primarykey_column":2, "this_is_the_data_column":"data2"},
            {"this_is_the_primarykey_column":3, "this_is_the_data_column":"data3"},
            {"this_is_the_primarykey_column":4, "this_is_the_data_column":"data4"},
        )

        r = table2.alias().select().execute()
        assert r.fetchall() == [(x, "data%d" % x) for x in range(1, 5)]

    def test_colbinds(self):
        table1.insert().execute(**{"this_is_the_primarykey_column":1, "this_is_the_data_column":"data1"})
        table1.insert().execute(**{"this_is_the_primarykey_column":2, "this_is_the_data_column":"data2"})
        table1.insert().execute(**{"this_is_the_primarykey_column":3, "this_is_the_data_column":"data3"})
        table1.insert().execute(**{"this_is_the_primarykey_column":4, "this_is_the_data_column":"data4"})

        r = table1.select(table1.c.this_is_the_primarykey_column == 4).execute()
        assert r.fetchall() == [(4, "data4")]

        r = table1.select(or_(
            table1.c.this_is_the_primarykey_column == 4,
            table1.c.this_is_the_primarykey_column == 2
        )).execute()
        assert r.fetchall() == [(2, "data2"), (4, "data4")]

    def test_insert_no_pk(self):
        table1.insert().execute(**{"this_is_the_data_column":"data1"})
        table1.insert().execute(**{"this_is_the_data_column":"data2"})
        table1.insert().execute(**{"this_is_the_data_column":"data3"})
        table1.insert().execute(**{"this_is_the_data_column":"data4"})

    @testing.requires.subqueries
    def test_subquery(self):
        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])
        print x.execute().fetchall()

    @testing.requires.subqueries
    def test_anon_alias(self):
        compile_dialect = default.DefaultDialect()
        compile_dialect.max_identifier_length = IDENT_LENGTH

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias()
        x = select([q], use_labels=True)

        self.assert_compile(x, "SELECT anon_1.this_is_the_primarykey_column AS anon_1_this_is_the_prim_1, anon_1.this_is_the_data_column AS anon_1_this_is_the_data_2 "
            "FROM (SELECT some_large_named_table.this_is_the_primarykey_column AS this_is_the_primarykey_column, some_large_named_table.this_is_the_data_column AS this_is_the_data_column "
            "FROM some_large_named_table "
            "WHERE some_large_named_table.this_is_the_primarykey_column = :this_is_the_primarykey__1) AS anon_1", dialect=compile_dialect)

        print x.execute().fetchall()

    def test_adjustable(self):

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])

        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(x, "SELECT foo.this_is_the_primarykey_column, foo.this_is_the_data_column FROM "
            "(SELECT some_large_named_table.this_is_the_primarykey_column AS this_1, some_large_named_table.this_is_the_data_column "
            "AS this_2 FROM some_large_named_table WHERE some_large_named_table.this_is_the_primarykey_column = :this_1) AS foo", dialect=compile_dialect)

        compile_dialect = default.DefaultDialect(label_length=4)
        self.assert_compile(x, "SELECT foo.this_is_the_primarykey_column, foo.this_is_the_data_column FROM "
            "(SELECT some_large_named_table.this_is_the_primarykey_column AS _1, some_large_named_table.this_is_the_data_column AS _2 "
            "FROM some_large_named_table WHERE some_large_named_table.this_is_the_primarykey_column = :_1) AS foo", dialect=compile_dialect)

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias()
        x = select([q], use_labels=True)

        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(x, "SELECT anon_1.this_is_the_primarykey_column AS anon_1, anon_1.this_is_the_data_column AS anon_2 FROM "
            "(SELECT some_large_named_table.this_is_the_primarykey_column AS this_3, some_large_named_table.this_is_the_data_column AS this_4 "
            "FROM some_large_named_table WHERE some_large_named_table.this_is_the_primarykey_column = :this_1) AS anon_1", dialect=compile_dialect)

        compile_dialect = default.DefaultDialect(label_length=4)
        self.assert_compile(x, "SELECT _1.this_is_the_primarykey_column AS _1, _1.this_is_the_data_column AS _2 FROM "
            "(SELECT some_large_named_table.this_is_the_primarykey_column AS _3, some_large_named_table.this_is_the_data_column AS _4 "
            "FROM some_large_named_table WHERE some_large_named_table.this_is_the_primarykey_column = :_1) AS _1", dialect=compile_dialect)


