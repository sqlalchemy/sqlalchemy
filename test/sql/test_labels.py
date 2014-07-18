from sqlalchemy import exc as exceptions, select, MetaData, Integer, or_
from sqlalchemy.engine import default
from sqlalchemy.sql import table, column
from sqlalchemy.testing import AssertsCompiledSQL, assert_raises, engines,\
    fixtures
from sqlalchemy.testing.schema import Table, Column

IDENT_LENGTH = 29


class MaxIdentTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'DefaultDialect'

    table1 = table('some_large_named_table',
                   column('this_is_the_primarykey_column'),
                   column('this_is_the_data_column')
                   )

    table2 = table('table_with_exactly_29_characs',
                   column('this_is_the_primarykey_column'),
                   column('this_is_the_data_column')
                   )

    def _length_fixture(self, length=IDENT_LENGTH, positional=False):
        dialect = default.DefaultDialect()
        dialect.max_identifier_length = length
        if positional:
            dialect.paramstyle = 'format'
            dialect.positional = True
        return dialect

    def _engine_fixture(self, length=IDENT_LENGTH):
        eng = engines.testing_engine()
        eng.dialect.max_identifier_length = length
        return eng

    def test_table_alias_1(self):
        self.assert_compile(
            self.table2.alias().select(),
            'SELECT '
            'table_with_exactly_29_c_1.'
            'this_is_the_primarykey_column, '
            'table_with_exactly_29_c_1.this_is_the_data_column '
            'FROM '
            'table_with_exactly_29_characs '
            'AS table_with_exactly_29_c_1',
            dialect=self._length_fixture()
        )

    def test_table_alias_2(self):
        table1 = self.table1
        table2 = self.table2
        ta = table2.alias()
        on = table1.c.this_is_the_data_column == ta.c.this_is_the_data_column
        self.assert_compile(
            select([table1, ta]).select_from(table1.join(ta, on)).
            where(ta.c.this_is_the_data_column == 'data3'),
            'SELECT '
            'some_large_named_table.this_is_the_primarykey_column, '
            'some_large_named_table.this_is_the_data_column, '
            'table_with_exactly_29_c_1.this_is_the_primarykey_column, '
            'table_with_exactly_29_c_1.this_is_the_data_column '
            'FROM '
            'some_large_named_table '
            'JOIN '
            'table_with_exactly_29_characs '
            'AS '
            'table_with_exactly_29_c_1 '
            'ON '
            'some_large_named_table.this_is_the_data_column = '
            'table_with_exactly_29_c_1.this_is_the_data_column '
            'WHERE '
            'table_with_exactly_29_c_1.this_is_the_data_column = '
            ':this_is_the_data_column_1',
            dialect=self._length_fixture()
        )

    def test_too_long_name_disallowed(self):
        m = MetaData()
        t = Table('this_name_is_too_long_for_what_were_doing_in_this_test',
                  m, Column('foo', Integer))
        eng = self._engine_fixture()
        methods = (t.create, t.drop, m.create_all, m.drop_all)
        for meth in methods:
            assert_raises(exceptions.IdentifierError, meth, eng)

    def _assert_labeled_table1_select(self, s):
        table1 = self.table1
        compiled = s.compile(dialect=self._length_fixture())

        assert set(compiled.result_map['some_large_named_table__2'][1]).\
            issuperset(
            [
                'some_large_named_table_this_is_the_data_column',
                'some_large_named_table__2',
                table1.c.this_is_the_data_column
            ]
        )

        assert set(compiled.result_map['some_large_named_table__1'][1]).\
            issuperset(
            [
                'some_large_named_table_this_is_the_primarykey_column',
                'some_large_named_table__1',
                table1.c.this_is_the_primarykey_column
            ]
        )

    def test_result_map_use_labels(self):
        table1 = self.table1
        s = table1.select().apply_labels().\
            order_by(table1.c.this_is_the_primarykey_column)

        self._assert_labeled_table1_select(s)

    def test_result_map_limit(self):
        table1 = self.table1
        # some dialects such as oracle (and possibly ms-sql in a future
        # version) generate a subquery for limits/offsets. ensure that the
        # generated result map corresponds to the selected table, not the
        # select query
        s = table1.select(use_labels=True,
                          order_by=[table1.c.this_is_the_primarykey_column]).\
            limit(2)
        self._assert_labeled_table1_select(s)

    def test_result_map_subquery(self):
        table1 = self.table1
        s = table1.select(
            table1.c.this_is_the_primarykey_column == 4).\
            alias('foo')
        s2 = select([s])
        compiled = s2.compile(dialect=self._length_fixture())
        assert \
            set(compiled.result_map['this_is_the_data_column'][1]).\
            issuperset(['this_is_the_data_column',
                        s.c.this_is_the_data_column])
        assert \
            set(compiled.result_map['this_is_the_primarykey_column'][1]).\
            issuperset(['this_is_the_primarykey_column',
                        s.c.this_is_the_primarykey_column])

    def test_result_map_anon_alias(self):
        table1 = self.table1
        dialect = self._length_fixture()

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias()
        s = select([q]).apply_labels()

        self.assert_compile(
            s, 'SELECT '
            'anon_1.this_is_the_primarykey_column '
            'AS anon_1_this_is_the_prim_1, '
            'anon_1.this_is_the_data_column '
            'AS anon_1_this_is_the_data_2 '
            'FROM ('
            'SELECT '
            'some_large_named_table.'
            'this_is_the_primarykey_column '
            'AS this_is_the_primarykey_column, '
            'some_large_named_table.this_is_the_data_column '
            'AS this_is_the_data_column '
            'FROM '
            'some_large_named_table '
            'WHERE '
            'some_large_named_table.this_is_the_primarykey_column '
            '= :this_is_the_primarykey__1'
            ') '
            'AS anon_1', dialect=dialect)
        compiled = s.compile(dialect=dialect)
        assert set(compiled.result_map['anon_1_this_is_the_data_2'][1]).\
            issuperset([
                'anon_1_this_is_the_data_2',
                q.corresponding_column(
                    table1.c.this_is_the_data_column)
            ])

        assert set(compiled.result_map['anon_1_this_is_the_prim_1'][1]).\
            issuperset([
                'anon_1_this_is_the_prim_1',
                q.corresponding_column(
                    table1.c.this_is_the_primarykey_column)
            ])

    def test_column_bind_labels_1(self):
        table1 = self.table1

        s = table1.select(table1.c.this_is_the_primarykey_column == 4)
        self.assert_compile(
            s,
            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = "
            ":this_is_the_primarykey__1",
            checkparams={'this_is_the_primarykey__1': 4},
            dialect=self._length_fixture()
        )

        self.assert_compile(
            s,
            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = "
            "%s",
            checkpositional=(4, ),
            checkparams={'this_is_the_primarykey__1': 4},
            dialect=self._length_fixture(positional=True)
        )

    def test_column_bind_labels_2(self):
        table1 = self.table1

        s = table1.select(or_(
            table1.c.this_is_the_primarykey_column == 4,
            table1.c.this_is_the_primarykey_column == 2
        ))
        self.assert_compile(
            s,
            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = "
            ":this_is_the_primarykey__1 OR "
            "some_large_named_table.this_is_the_primarykey_column = "
            ":this_is_the_primarykey__2",
            checkparams={
                'this_is_the_primarykey__1': 4,
                'this_is_the_primarykey__2': 2
            },
            dialect=self._length_fixture()
        )
        self.assert_compile(
            s,
            "SELECT some_large_named_table.this_is_the_primarykey_column, "
            "some_large_named_table.this_is_the_data_column "
            "FROM some_large_named_table WHERE "
            "some_large_named_table.this_is_the_primarykey_column = "
            "%s OR "
            "some_large_named_table.this_is_the_primarykey_column = "
            "%s",
            checkparams={
                'this_is_the_primarykey__1': 4,
                'this_is_the_primarykey__2': 2
            },
            checkpositional=(4, 2),
            dialect=self._length_fixture(positional=True)
        )


class LabelLengthTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'DefaultDialect'

    table1 = table('some_large_named_table',
                   column('this_is_the_primarykey_column'),
                   column('this_is_the_data_column')
                   )

    table2 = table('table_with_exactly_29_characs',
                   column('this_is_the_primarykey_column'),
                   column('this_is_the_data_column')
                   )

    def test_adjustable_1(self):
        table1 = self.table1
        q = table1.select(
            table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])
        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(
            x, 'SELECT '
            'foo.this_1, foo.this_2 '
            'FROM ('
            'SELECT '
            'some_large_named_table.this_is_the_primarykey_column '
            'AS this_1, '
            'some_large_named_table.this_is_the_data_column '
            'AS this_2 '
            'FROM '
            'some_large_named_table '
            'WHERE '
            'some_large_named_table.this_is_the_primarykey_column '
            '= :this_1'
            ') '
            'AS foo', dialect=compile_dialect)

    def test_adjustable_2(self):
        table1 = self.table1

        q = table1.select(
            table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])

        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(
            x, 'SELECT '
            'foo.this_1, foo.this_2 '
            'FROM ('
            'SELECT '
            'some_large_named_table.this_is_the_primarykey_column '
            'AS this_1, '
            'some_large_named_table.this_is_the_data_column '
            'AS this_2 '
            'FROM '
            'some_large_named_table '
            'WHERE '
            'some_large_named_table.this_is_the_primarykey_column '
            '= :this_1'
            ') '
            'AS foo', dialect=compile_dialect)

    def test_adjustable_3(self):
        table1 = self.table1

        compile_dialect = default.DefaultDialect(label_length=4)
        q = table1.select(
            table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])

        self.assert_compile(
            x, 'SELECT '
            'foo._1, foo._2 '
            'FROM ('
            'SELECT '
            'some_large_named_table.this_is_the_primarykey_column '
            'AS _1, '
            'some_large_named_table.this_is_the_data_column '
            'AS _2 '
            'FROM '
            'some_large_named_table '
            'WHERE '
            'some_large_named_table.this_is_the_primarykey_column '
            '= :_1'
            ') '
            'AS foo', dialect=compile_dialect)

    def test_adjustable_4(self):
        table1 = self.table1

        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias()
        x = select([q], use_labels=True)

        compile_dialect = default.DefaultDialect(label_length=10)
        self.assert_compile(
            x, 'SELECT '
            'anon_1.this_2 AS anon_1, '
            'anon_1.this_4 AS anon_3 '
            'FROM ('
            'SELECT '
            'some_large_named_table.this_is_the_primarykey_column '
            'AS this_2, '
            'some_large_named_table.this_is_the_data_column '
            'AS this_4 '
            'FROM '
            'some_large_named_table '
            'WHERE '
            'some_large_named_table.this_is_the_primarykey_column '
            '= :this_1'
            ') '
            'AS anon_1', dialect=compile_dialect)

    def test_adjustable_5(self):
        table1 = self.table1
        q = table1.select(table1.c.this_is_the_primarykey_column == 4).alias()
        x = select([q], use_labels=True)

        compile_dialect = default.DefaultDialect(label_length=4)
        self.assert_compile(
            x, 'SELECT '
            '_1._2 AS _1, '
            '_1._4 AS _3 '
            'FROM ('
            'SELECT '
            'some_large_named_table.this_is_the_primarykey_column '
            'AS _2, '
            'some_large_named_table.this_is_the_data_column '
            'AS _4 '
            'FROM '
            'some_large_named_table '
            'WHERE '
            'some_large_named_table.this_is_the_primarykey_column '
            '= :_1'
            ') '
            'AS _1', dialect=compile_dialect)

    def test_adjustable_result_schema_column_1(self):
        table1 = self.table1

        q = table1.select(
            table1.c.this_is_the_primarykey_column == 4).apply_labels().\
            alias('foo')

        dialect = default.DefaultDialect(label_length=10)
        compiled = q.compile(dialect=dialect)

        assert set(compiled.result_map['some_2'][1]).issuperset([
            table1.c.this_is_the_data_column,
            'some_large_named_table_this_is_the_data_column',
            'some_2'
        ])

        assert set(compiled.result_map['some_1'][1]).issuperset([
            table1.c.this_is_the_primarykey_column,
            'some_large_named_table_this_is_the_primarykey_column',
            'some_1'
        ])

    def test_adjustable_result_schema_column_2(self):
        table1 = self.table1

        q = table1.select(
            table1.c.this_is_the_primarykey_column == 4).alias('foo')
        x = select([q])

        dialect = default.DefaultDialect(label_length=10)
        compiled = x.compile(dialect=dialect)

        assert set(compiled.result_map['this_2'][1]).issuperset([
            q.corresponding_column(table1.c.this_is_the_data_column),
            'this_is_the_data_column',
            'this_2'])

        assert set(compiled.result_map['this_1'][1]).issuperset([
            q.corresponding_column(table1.c.this_is_the_primarykey_column),
            'this_is_the_primarykey_column',
            'this_1'])

    def test_table_plus_column_exceeds_length(self):
        """test that the truncation only occurs when tablename + colname are
        concatenated, if they are individually under the label length.

        """

        compile_dialect = default.DefaultDialect(label_length=30)
        a_table = table(
            'thirty_characters_table_xxxxxx',
            column('id')
        )

        other_table = table(
            'other_thirty_characters_table_',
            column('id'),
            column('thirty_characters_table_id')
        )

        anon = a_table.alias()

        j1 = other_table.outerjoin(
            anon,
            anon.c.id == other_table.c.thirty_characters_table_id)

        self.assert_compile(
            select([other_table, anon]).
            select_from(j1).apply_labels(),
            'SELECT '
            'other_thirty_characters_table_.id '
            'AS other_thirty_characters__1, '
            'other_thirty_characters_table_.thirty_characters_table_id '
            'AS other_thirty_characters__2, '
            'thirty_characters_table__1.id '
            'AS thirty_characters_table__3 '
            'FROM '
            'other_thirty_characters_table_ '
            'LEFT OUTER JOIN '
            'thirty_characters_table_xxxxxx AS thirty_characters_table__1 '
            'ON thirty_characters_table__1.id = '
            'other_thirty_characters_table_.thirty_characters_table_id',
            dialect=compile_dialect)

    def test_colnames_longer_than_labels_lowercase(self):
        t1 = table('a', column('abcde'))
        self._test_colnames_longer_than_labels(t1)

    def test_colnames_longer_than_labels_uppercase(self):
        m = MetaData()
        t1 = Table('a', m, Column('abcde', Integer))
        self._test_colnames_longer_than_labels(t1)

    def _test_colnames_longer_than_labels(self, t1):
        dialect = default.DefaultDialect(label_length=4)
        a1 = t1.alias(name='asdf')

        # 'abcde' is longer than 4, but rendered as itself
        # needs to have all characters
        s = select([a1])
        self.assert_compile(select([a1]),
                            'SELECT asdf.abcde FROM a AS asdf',
                            dialect=dialect)
        compiled = s.compile(dialect=dialect)
        assert set(compiled.result_map['abcde'][1]).issuperset([
            'abcde', a1.c.abcde, 'abcde'])

        # column still there, but short label
        s = select([a1]).apply_labels()
        self.assert_compile(s,
                            'SELECT asdf.abcde AS _1 FROM a AS asdf',
                            dialect=dialect)
        compiled = s.compile(dialect=dialect)
        assert set(compiled.result_map['_1'][1]).issuperset([
            'asdf_abcde', a1.c.abcde, '_1'])
