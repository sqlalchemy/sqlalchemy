import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from testlib import *
from sqlalchemy.engine import default

# TODO: either create a mock dialect with named paramstyle and a short identifier length,
# or find a way to just use sqlite dialect and make those changes

IDENT_LENGTH = 29

class LabelTypeTest(TestBase):
    def test_type(self):
        m = MetaData()
        t = Table('sometable', m,
            Column('col1', Integer),
            Column('col2', Float))
        assert isinstance(t.c.col1.label('hi').type, Integer)
        assert isinstance(select([t.c.col2]).as_scalar().label('lala').type, Float)

class LongLabelsTest(TestBase, AssertsCompiledSQL):
    def setUpAll(self):
        global metadata, table1, maxlen
        metadata = MetaData(testing.db)
        table1 = Table("some_large_named_table", metadata,
            Column("this_is_the_primarykey_column", Integer, Sequence("this_is_some_large_seq"), primary_key=True),
            Column("this_is_the_data_column", String(30))
            )

        metadata.create_all()

        maxlen = testing.db.dialect.max_identifier_length
        testing.db.dialect.max_identifier_length = IDENT_LENGTH

    def tearDown(self):
        table1.delete().execute()

    def tearDownAll(self):
        metadata.drop_all()
        testing.db.dialect.max_identifier_length = maxlen

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
  
        r = s.limit(2).offset(1).execute()
        result = []
        for row in r:
            result.append((row[table1.c.this_is_the_primarykey_column], row[table1.c.this_is_the_data_column]))
        assert result == [
            (2, "data2"),
            (3, "data3"),
        ], repr(result)

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

    def test_subquery(self):
      # this is the test that fails if the "max identifier length" is shorter than the
      # length of the actual columns created, because the column names get truncated.
      # if you try to separate "physical columns" from "labels", and only truncate the labels,
      # the compiler.DefaultCompiler.visit_select() logic which auto-labels columns in a subquery (for the purposes of sqlite compat) breaks the code,
      # since it is creating "labels" on the fly but not affecting derived columns, which think they are
      # still "physical"
      q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias('foo')
      x = select([q])
      print x.execute().fetchall()

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

    def test_oid(self):
        """test that a primary key column compiled as the 'oid' column gets proper length truncation"""
        from sqlalchemy.databases import postgres
        dialect = postgres.PGDialect()
        dialect.max_identifier_length = 30
        tt = table1.select(use_labels=True).alias('foo')
        x = select([tt], use_labels=True, order_by=tt.oid_column).compile(dialect=dialect)
        #print x
        # assert it doesnt end with "ORDER BY foo.some_large_named_table_this_is_the_primarykey_column"
        assert str(x).endswith("""ORDER BY foo.some_large_named_table_t_2""")

if __name__ == '__main__':
    testenv.main()
