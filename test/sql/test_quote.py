from sqlalchemy import MetaData, Table, Column, Integer, select, \
    ForeignKey, Index, CheckConstraint, inspect, column
from sqlalchemy import sql, schema, types as sqltypes
from sqlalchemy.sql import compiler
from sqlalchemy.testing import fixtures, AssertsCompiledSQL, eq_
from sqlalchemy import testing
from sqlalchemy.sql.elements import quoted_name, _anonymous_label
from sqlalchemy.testing.util import picklers
from sqlalchemy.engine import default


class QuoteExecTest(fixtures.TestBase):
    __backend__ = True

    @classmethod
    def setup_class(cls):
        # TODO: figure out which databases/which identifiers allow special
        # characters to be used, such as: spaces, quote characters,
        # punctuation characters, set up tests for those as well.

        global table1, table2
        metadata = MetaData(testing.db)

        table1 = Table('WorstCase1', metadata,
                       Column('lowercase', Integer, primary_key=True),
                       Column('UPPERCASE', Integer),
                       Column('MixedCase', Integer),
                       Column('ASC', Integer, key='a123'))
        table2 = Table('WorstCase2', metadata,
                       Column('desc', Integer, primary_key=True, key='d123'),
                       Column('Union', Integer, key='u123'),
                       Column('MixedCase', Integer))

        table1.create()
        table2.create()

    def teardown(self):
        table1.delete().execute()
        table2.delete().execute()

    @classmethod
    def teardown_class(cls):
        table1.drop()
        table2.drop()

    def test_reflect(self):
        meta2 = MetaData(testing.db)
        t2 = Table('WorstCase1', meta2, autoload=True, quote=True)
        assert 'lowercase' in t2.c

        # indicates the DB returns unquoted names as
        # UPPERCASE, which we then assume are unquoted and go to
        # lower case.  So we cannot accurately reflect quoted UPPERCASE
        # names from a "name normalize" backend, as they cannot be
        # distinguished from case-insensitive/unquoted names.
        if testing.db.dialect.requires_name_normalize:
            assert 'uppercase' in t2.c
        else:
            assert 'UPPERCASE' in t2.c

        # ASC OTOH is a reserved word, which is always quoted, so
        # with that name we keep the quotes on and it stays uppercase
        # regardless.  Seems a little weird, though.
        assert 'ASC' in t2.c

        assert 'MixedCase' in t2.c

    @testing.provide_metadata
    def test_has_table_case_sensitive(self):
        preparer = testing.db.dialect.identifier_preparer
        if testing.db.dialect.requires_name_normalize:
            testing.db.execute("CREATE TABLE TAB1 (id INTEGER)")
        else:
            testing.db.execute("CREATE TABLE tab1 (id INTEGER)")
        testing.db.execute('CREATE TABLE %s (id INTEGER)' %
                           preparer.quote_identifier("tab2"))
        testing.db.execute('CREATE TABLE %s (id INTEGER)' %
                           preparer.quote_identifier("TAB3"))
        testing.db.execute('CREATE TABLE %s (id INTEGER)' %
                           preparer.quote_identifier("TAB4"))

        t1 = Table('tab1', self.metadata,
                   Column('id', Integer, primary_key=True),
                   )
        t2 = Table('tab2', self.metadata,
                   Column('id', Integer, primary_key=True),
                   quote=True
                   )
        t3 = Table('TAB3', self.metadata,
                   Column('id', Integer, primary_key=True),
                   )
        t4 = Table('TAB4', self.metadata,
                   Column('id', Integer, primary_key=True),
                   quote=True)

        insp = inspect(testing.db)
        assert testing.db.has_table(t1.name)
        eq_([c['name'] for c in insp.get_columns(t1.name)], ['id'])

        assert testing.db.has_table(t2.name)
        eq_([c['name'] for c in insp.get_columns(t2.name)], ['id'])

        assert testing.db.has_table(t3.name)
        eq_([c['name'] for c in insp.get_columns(t3.name)], ['id'])

        assert testing.db.has_table(t4.name)
        eq_([c['name'] for c in insp.get_columns(t4.name)], ['id'])

    def test_basic(self):
        table1.insert().execute(
            {'lowercase': 1, 'UPPERCASE': 2, 'MixedCase': 3, 'a123': 4},
            {'lowercase': 2, 'UPPERCASE': 2, 'MixedCase': 3, 'a123': 4},
            {'lowercase': 4, 'UPPERCASE': 3, 'MixedCase': 2, 'a123': 1})
        table2.insert().execute(
            {'d123': 1, 'u123': 2, 'MixedCase': 3},
            {'d123': 2, 'u123': 2, 'MixedCase': 3},
            {'d123': 4, 'u123': 3, 'MixedCase': 2})

        columns = [
            table1.c.lowercase,
            table1.c.UPPERCASE,
            table1.c.MixedCase,
            table1.c.a123
        ]
        result = select(columns).execute().fetchall()
        assert(result == [(1, 2, 3, 4), (2, 2, 3, 4), (4, 3, 2, 1)])

        columns = [
            table2.c.d123,
            table2.c.u123,
            table2.c.MixedCase
        ]
        result = select(columns).execute().fetchall()
        assert(result == [(1, 2, 3), (2, 2, 3), (4, 3, 2)])

    def test_use_labels(self):
        table1.insert().execute(
            {'lowercase': 1, 'UPPERCASE': 2, 'MixedCase': 3, 'a123': 4},
            {'lowercase': 2, 'UPPERCASE': 2, 'MixedCase': 3, 'a123': 4},
            {'lowercase': 4, 'UPPERCASE': 3, 'MixedCase': 2, 'a123': 1})
        table2.insert().execute(
            {'d123': 1, 'u123': 2, 'MixedCase': 3},
            {'d123': 2, 'u123': 2, 'MixedCase': 3},
            {'d123': 4, 'u123': 3, 'MixedCase': 2})

        columns = [
            table1.c.lowercase,
            table1.c.UPPERCASE,
            table1.c.MixedCase,
            table1.c.a123
        ]
        result = select(columns, use_labels=True).execute().fetchall()
        assert(result == [(1, 2, 3, 4), (2, 2, 3, 4), (4, 3, 2, 1)])

        columns = [
            table2.c.d123,
            table2.c.u123,
            table2.c.MixedCase
        ]
        result = select(columns, use_labels=True).execute().fetchall()
        assert(result == [(1, 2, 3), (2, 2, 3), (4, 3, 2)])


class QuoteTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        # TODO: figure out which databases/which identifiers allow special
        # characters to be used, such as: spaces, quote characters,
        # punctuation characters, set up tests for those as well.

        global table1, table2
        metadata = MetaData(testing.db)

        table1 = Table('WorstCase1', metadata,
                       Column('lowercase', Integer, primary_key=True),
                       Column('UPPERCASE', Integer),
                       Column('MixedCase', Integer),
                       Column('ASC', Integer, key='a123'))
        table2 = Table('WorstCase2', metadata,
                       Column('desc', Integer, primary_key=True, key='d123'),
                       Column('Union', Integer, key='u123'),
                       Column('MixedCase', Integer))

    @testing.crashes('oracle', 'FIXME: unknown, verify not fails_on')
    @testing.requires.subqueries
    def test_labels(self):
        """test the quoting of labels.

        If labels aren't quoted, a query in postgresql in particular will
        fail since it produces:

        SELECT
            LaLa.lowercase, LaLa."UPPERCASE", LaLa."MixedCase", LaLa."ASC"
        FROM (
            SELECT DISTINCT
                "WorstCase1".lowercase AS lowercase,
                "WorstCase1"."UPPERCASE" AS UPPERCASE,
                "WorstCase1"."MixedCase" AS MixedCase,
                "WorstCase1"."ASC" AS ASC
            FROM "WorstCase1"
        ) AS LaLa

        where the "UPPERCASE" column of "LaLa" doesn't exist.
        """

        self.assert_compile(
            table1.select(distinct=True).alias('LaLa').select(),
            'SELECT '
            '"LaLa".lowercase, '
            '"LaLa"."UPPERCASE", '
            '"LaLa"."MixedCase", '
            '"LaLa"."ASC" '
            'FROM ('
            'SELECT DISTINCT '
            '"WorstCase1".lowercase AS lowercase, '
            '"WorstCase1"."UPPERCASE" AS "UPPERCASE", '
            '"WorstCase1"."MixedCase" AS "MixedCase", '
            '"WorstCase1"."ASC" AS "ASC" '
            'FROM "WorstCase1"'
            ') AS "LaLa"'
        )

    def test_lower_case_names(self):
        # Create table with quote defaults
        metadata = MetaData()
        t1 = Table('t1', metadata,
                   Column('col1', Integer),
                   schema='foo')

        # Note that the names are not quoted b/c they are all lower case
        result = 'CREATE TABLE foo.t1 (col1 INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

        # Create the same table with quotes set to True now
        metadata = MetaData()
        t1 = Table('t1', metadata,
                   Column('col1', Integer, quote=True),
                   schema='foo', quote=True, quote_schema=True)

        # Note that the names are now quoted
        result = 'CREATE TABLE "foo"."t1" ("col1" INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

    def test_upper_case_names(self):
        # Create table with quote defaults
        metadata = MetaData()
        t1 = Table('TABLE1', metadata,
                   Column('COL1', Integer),
                   schema='FOO')

        # Note that the names are quoted b/c they are not all lower case
        result = 'CREATE TABLE "FOO"."TABLE1" ("COL1" INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

        # Create the same table with quotes set to False now
        metadata = MetaData()
        t1 = Table('TABLE1', metadata,
                   Column('COL1', Integer, quote=False),
                   schema='FOO', quote=False, quote_schema=False)

        # Note that the names are now unquoted
        result = 'CREATE TABLE FOO.TABLE1 (COL1 INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

    def test_mixed_case_names(self):
        # Create table with quote defaults
        metadata = MetaData()
        t1 = Table('Table1', metadata,
                   Column('Col1', Integer),
                   schema='Foo')

        # Note that the names are quoted b/c they are not all lower case
        result = 'CREATE TABLE "Foo"."Table1" ("Col1" INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

        # Create the same table with quotes set to False now
        metadata = MetaData()
        t1 = Table('Table1', metadata,
                   Column('Col1', Integer, quote=False),
                   schema='Foo', quote=False, quote_schema=False)

        # Note that the names are now unquoted
        result = 'CREATE TABLE Foo.Table1 (Col1 INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

    def test_numeric_initial_char(self):
        # Create table with quote defaults
        metadata = MetaData()
        t1 = Table('35table', metadata,
                   Column('25column', Integer),
                   schema='45schema')

        # Note that the names are quoted b/c the initial
        # character is in ['$','0', '1' ... '9']
        result = 'CREATE TABLE "45schema"."35table" ("25column" INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

        # Create the same table with quotes set to False now
        metadata = MetaData()
        t1 = Table('35table', metadata,
                   Column('25column', Integer, quote=False),
                   schema='45schema', quote=False, quote_schema=False)

        # Note that the names are now unquoted
        result = 'CREATE TABLE 45schema.35table (25column INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

    def test_illegal_initial_char(self):
        # Create table with quote defaults
        metadata = MetaData()
        t1 = Table('$table', metadata,
                   Column('$column', Integer),
                   schema='$schema')

        # Note that the names are quoted b/c the initial
        # character is in ['$','0', '1' ... '9']
        result = 'CREATE TABLE "$schema"."$table" ("$column" INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

        # Create the same table with quotes set to False now
        metadata = MetaData()
        t1 = Table('$table', metadata,
                   Column('$column', Integer, quote=False),
                   schema='$schema', quote=False, quote_schema=False)

        # Note that the names are now unquoted
        result = 'CREATE TABLE $schema.$table ($column INTEGER)'
        self.assert_compile(schema.CreateTable(t1), result)

    def test_reserved_words(self):
        # Create table with quote defaults
        metadata = MetaData()
        table = Table('foreign', metadata,
                      Column('col1', Integer),
                      Column('from', Integer),
                      Column('order', Integer),
                      schema='create')

        # Note that the names are quoted b/c they are reserved words
        x = select([table.c.col1, table.c['from'], table.c.order])
        self.assert_compile(x,
                            'SELECT '
                            '"create"."foreign".col1, '
                            '"create"."foreign"."from", '
                            '"create"."foreign"."order" '
                            'FROM "create"."foreign"'
                            )

        # Create the same table with quotes set to False now
        metadata = MetaData()
        table = Table('foreign', metadata,
                      Column('col1', Integer),
                      Column('from', Integer, quote=False),
                      Column('order', Integer, quote=False),
                      schema='create', quote=False, quote_schema=False)

        # Note that the names are now unquoted
        x = select([table.c.col1, table.c['from'], table.c.order])
        self.assert_compile(x,
                            'SELECT '
                            'create.foreign.col1, '
                            'create.foreign.from, '
                            'create.foreign.order '
                            'FROM create.foreign'
                            )

    def test_subquery_one(self):
        # Lower case names, should not quote
        metadata = MetaData()
        t1 = Table('t1', metadata,
                   Column('col1', Integer),
                   schema='foo')
        a = t1.select().alias('anon')
        b = select([1], a.c.col1 == 2, from_obj=a)
        self.assert_compile(b,
                            'SELECT 1 '
                            'FROM ('
                            'SELECT '
                            'foo.t1.col1 AS col1 '
                            'FROM '
                            'foo.t1'
                            ') AS anon '
                            'WHERE anon.col1 = :col1_1'
                            )

    def test_subquery_two(self):
        # Lower case names, quotes on, should quote
        metadata = MetaData()
        t1 = Table('t1', metadata,
                   Column('col1', Integer, quote=True),
                   schema='foo', quote=True, quote_schema=True)
        a = t1.select().alias('anon')
        b = select([1], a.c.col1 == 2, from_obj=a)
        self.assert_compile(b,
                            'SELECT 1 '
                            'FROM ('
                            'SELECT '
                            '"foo"."t1"."col1" AS "col1" '
                            'FROM '
                            '"foo"."t1"'
                            ') AS anon '
                            'WHERE anon."col1" = :col1_1'
                            )

    def test_subquery_three(self):
        # Not lower case names, should quote
        metadata = MetaData()
        t1 = Table('T1', metadata,
                   Column('Col1', Integer),
                   schema='Foo')
        a = t1.select().alias('Anon')
        b = select([1], a.c.Col1 == 2, from_obj=a)
        self.assert_compile(b,
                            'SELECT 1 '
                            'FROM ('
                            'SELECT '
                            '"Foo"."T1"."Col1" AS "Col1" '
                            'FROM '
                            '"Foo"."T1"'
                            ') AS "Anon" '
                            'WHERE '
                            '"Anon"."Col1" = :Col1_1'
                            )

    def test_subquery_four(self):

        # Not lower case names, quotes off, should not quote
        metadata = MetaData()
        t1 = Table('T1', metadata,
                   Column('Col1', Integer, quote=False),
                   schema='Foo', quote=False, quote_schema=False)
        a = t1.select().alias('Anon')
        b = select([1], a.c.Col1 == 2, from_obj=a)
        self.assert_compile(b,
                            'SELECT 1 '
                            'FROM ('
                            'SELECT '
                            'Foo.T1.Col1 AS Col1 '
                            'FROM '
                            'Foo.T1'
                            ') AS "Anon" '
                            'WHERE '
                            '"Anon".Col1 = :Col1_1'
                            )

    def test_simple_order_by_label(self):
        m = MetaData()
        t1 = Table('t1', m, Column('col1', Integer))
        cl = t1.c.col1.label('ShouldQuote')
        self.assert_compile(
            select([cl]).order_by(cl),
            'SELECT t1.col1 AS "ShouldQuote" FROM t1 ORDER BY "ShouldQuote"'
        )

    def test_collate(self):
        self.assert_compile(
            column('foo').collate('utf8'),
            "foo COLLATE utf8"
        )

        self.assert_compile(
            column('foo').collate('fr_FR'),
            'foo COLLATE "fr_FR"',
            dialect="postgresql"
        )

        self.assert_compile(
            column('foo').collate('utf8_GERMAN_ci'),
            'foo COLLATE `utf8_GERMAN_ci`',
            dialect="mysql"
        )

        self.assert_compile(
            column('foo').collate('SQL_Latin1_General_CP1_CI_AS'),
            'foo COLLATE SQL_Latin1_General_CP1_CI_AS',
            dialect="mssql"
        )

    def test_join(self):
        # Lower case names, should not quote
        metadata = MetaData()
        t1 = Table('t1', metadata,
                   Column('col1', Integer))
        t2 = Table('t2', metadata,
                   Column('col1', Integer),
                   Column('t1col1', Integer, ForeignKey('t1.col1')))
        self.assert_compile(t2.join(t1).select(),
                            'SELECT '
                            't2.col1, t2.t1col1, t1.col1 '
                            'FROM '
                            't2 '
                            'JOIN '
                            't1 ON t1.col1 = t2.t1col1'
                            )

        # Lower case names, quotes on, should quote
        metadata = MetaData()
        t1 = Table('t1', metadata,
                   Column('col1', Integer, quote=True),
                   quote=True)
        t2 = Table(
            't2',
            metadata,
            Column(
                'col1',
                Integer,
                quote=True),
            Column(
                't1col1',
                Integer,
                ForeignKey('t1.col1'),
                quote=True),
            quote=True)
        self.assert_compile(t2.join(t1).select(),
                            'SELECT '
                            '"t2"."col1", "t2"."t1col1", "t1"."col1" '
                            'FROM '
                            '"t2" '
                            'JOIN '
                            '"t1" ON "t1"."col1" = "t2"."t1col1"'
                            )

        # Not lower case names, should quote
        metadata = MetaData()
        t1 = Table('T1', metadata,
                   Column('Col1', Integer))
        t2 = Table('T2', metadata,
                   Column('Col1', Integer),
                   Column('T1Col1', Integer, ForeignKey('T1.Col1')))
        self.assert_compile(t2.join(t1).select(),
                            'SELECT '
                            '"T2"."Col1", "T2"."T1Col1", "T1"."Col1" '
                            'FROM '
                            '"T2" '
                            'JOIN '
                            '"T1" ON "T1"."Col1" = "T2"."T1Col1"'
                            )

        # Not lower case names, quotes off, should not quote
        metadata = MetaData()
        t1 = Table('T1', metadata,
                   Column('Col1', Integer, quote=False),
                   quote=False)
        t2 = Table(
            'T2',
            metadata,
            Column(
                'Col1',
                Integer,
                quote=False),
            Column(
                'T1Col1',
                Integer,
                ForeignKey('T1.Col1'),
                quote=False),
            quote=False)
        self.assert_compile(t2.join(t1).select(),
                            'SELECT '
                            'T2.Col1, T2.T1Col1, T1.Col1 '
                            'FROM '
                            'T2 '
                            'JOIN '
                            'T1 ON T1.Col1 = T2.T1Col1'
                            )

    def test_label_and_alias(self):
        # Lower case names, should not quote
        metadata = MetaData()
        table = Table('t1', metadata,
                      Column('col1', Integer))
        x = select([table.c.col1.label('label1')]).alias('alias1')
        self.assert_compile(select([x.c.label1]),
                            'SELECT '
                            'alias1.label1 '
                            'FROM ('
                            'SELECT '
                            't1.col1 AS label1 '
                            'FROM t1'
                            ') AS alias1'
                            )

        # Not lower case names, should quote
        metadata = MetaData()
        table = Table('T1', metadata,
                      Column('Col1', Integer))
        x = select([table.c.Col1.label('Label1')]).alias('Alias1')
        self.assert_compile(select([x.c.Label1]),
                            'SELECT '
                            '"Alias1"."Label1" '
                            'FROM ('
                            'SELECT '
                            '"T1"."Col1" AS "Label1" '
                            'FROM "T1"'
                            ') AS "Alias1"'
                            )

    def test_literal_column_already_with_quotes(self):
        # Lower case names
        metadata = MetaData()
        table = Table('t1', metadata,
                      Column('col1', Integer))

        # Note that 'col1' is already quoted (literal_column)
        columns = [sql.literal_column("'col1'").label('label1')]
        x = select(columns, from_obj=[table]).alias('alias1')
        x = x.select()
        self.assert_compile(x,
                            'SELECT '
                            'alias1.label1 '
                            'FROM ('
                            'SELECT '
                            '\'col1\' AS label1 '
                            'FROM t1'
                            ') AS alias1'
                            )

        # Not lower case names
        metadata = MetaData()
        table = Table('T1', metadata,
                      Column('Col1', Integer))

        # Note that 'Col1' is already quoted (literal_column)
        columns = [sql.literal_column("'Col1'").label('Label1')]
        x = select(columns, from_obj=[table]).alias('Alias1')
        x = x.select()
        self.assert_compile(x,
                            'SELECT '
                            '"Alias1"."Label1" '
                            'FROM ('
                            'SELECT '
                            '\'Col1\' AS "Label1" '
                            'FROM "T1"'
                            ') AS "Alias1"'
                            )

    def test_apply_labels_should_quote(self):
        # Not lower case names, should quote
        metadata = MetaData()
        t1 = Table('T1', metadata,
                   Column('Col1', Integer),
                   schema='Foo')

        self.assert_compile(t1.select().apply_labels(),
                            'SELECT '
                            '"Foo"."T1"."Col1" AS "Foo_T1_Col1" '
                            'FROM '
                            '"Foo"."T1"'
                            )

    def test_apply_labels_shouldnt_quote(self):
        # Not lower case names, quotes off
        metadata = MetaData()
        t1 = Table('T1', metadata,
                   Column('Col1', Integer, quote=False),
                   schema='Foo', quote=False, quote_schema=False)

        # TODO: is this what we really want here ?
        # what if table/schema *are* quoted?
        self.assert_compile(t1.select().apply_labels(),
                            'SELECT '
                            'Foo.T1.Col1 AS Foo_T1_Col1 '
                            'FROM '
                            'Foo.T1'
                            )

    def test_quote_flag_propagate_check_constraint(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer, quote=True))
        CheckConstraint(t.c.x > 5)
        self.assert_compile(
            schema.CreateTable(t),
            "CREATE TABLE t ("
            '"x" INTEGER, '
            'CHECK ("x" > 5)'
            ")"
        )

    def test_quote_flag_propagate_index(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer, quote=True))
        idx = Index("foo", t.c.x)
        self.assert_compile(
            schema.CreateIndex(idx),
            'CREATE INDEX foo ON t ("x")'
        )

    def test_quote_flag_propagate_anon_label(self):
        m = MetaData()
        t = Table('t', m, Column('x', Integer, quote=True))

        self.assert_compile(
            select([t.alias()]).apply_labels(),
            'SELECT t_1."x" AS "t_1_x" FROM t AS t_1'
        )

        t2 = Table('t2', m, Column('x', Integer), quote=True)
        self.assert_compile(
            select([t2.c.x]).apply_labels(),
            'SELECT "t2".x AS "t2_x" FROM "t2"'
        )


class PreparerTest(fixtures.TestBase):

    """Test the db-agnostic quoting services of IdentifierPreparer."""

    def test_unformat(self):
        prep = compiler.IdentifierPreparer(default.DefaultDialect())
        unformat = prep.unformat_identifiers

        def a_eq(have, want):
            if have != want:
                print("Wanted %s" % want)
                print("Received %s" % have)
            self.assert_(have == want)

        a_eq(unformat('foo'), ['foo'])
        a_eq(unformat('"foo"'), ['foo'])
        a_eq(unformat("'foo'"), ["'foo'"])
        a_eq(unformat('foo.bar'), ['foo', 'bar'])
        a_eq(unformat('"foo"."bar"'), ['foo', 'bar'])
        a_eq(unformat('foo."bar"'), ['foo', 'bar'])
        a_eq(unformat('"foo".bar'), ['foo', 'bar'])
        a_eq(unformat('"foo"."b""a""r"."baz"'), ['foo', 'b"a"r', 'baz'])

    def test_unformat_custom(self):

        class Custom(compiler.IdentifierPreparer):

            def __init__(self, dialect):
                super(Custom, self).__init__(
                    dialect, initial_quote='`', final_quote='`')

            def _escape_identifier(self, value):
                return value.replace('`', '``')

            def _unescape_identifier(self, value):
                return value.replace('``', '`')

        prep = Custom(default.DefaultDialect())
        unformat = prep.unformat_identifiers

        def a_eq(have, want):
            if have != want:
                print("Wanted %s" % want)
                print("Received %s" % have)
            self.assert_(have == want)

        a_eq(unformat('foo'), ['foo'])
        a_eq(unformat('`foo`'), ['foo'])
        a_eq(unformat(repr('foo')), ["'foo'"])
        a_eq(unformat('foo.bar'), ['foo', 'bar'])
        a_eq(unformat('`foo`.`bar`'), ['foo', 'bar'])
        a_eq(unformat('foo.`bar`'), ['foo', 'bar'])
        a_eq(unformat('`foo`.bar'), ['foo', 'bar'])
        a_eq(unformat('`foo`.`b``a``r`.`baz`'), ['foo', 'b`a`r', 'baz'])


class QuotedIdentTest(fixtures.TestBase):

    def test_concat_quotetrue(self):
        q1 = quoted_name("x", True)
        self._assert_not_quoted("y" + q1)

    def test_concat_quotefalse(self):
        q1 = quoted_name("x", False)
        self._assert_not_quoted("y" + q1)

    def test_concat_quotenone(self):
        q1 = quoted_name("x", None)
        self._assert_not_quoted("y" + q1)

    def test_rconcat_quotetrue(self):
        q1 = quoted_name("x", True)
        self._assert_not_quoted("y" + q1)

    def test_rconcat_quotefalse(self):
        q1 = quoted_name("x", False)
        self._assert_not_quoted("y" + q1)

    def test_rconcat_quotenone(self):
        q1 = quoted_name("x", None)
        self._assert_not_quoted("y" + q1)

    def test_concat_anon(self):
        q1 = _anonymous_label(quoted_name("x", True))
        assert isinstance(q1, _anonymous_label)
        value = q1 + "y"
        assert isinstance(value, _anonymous_label)
        self._assert_quoted(value, True)

    def test_rconcat_anon(self):
        q1 = _anonymous_label(quoted_name("x", True))
        assert isinstance(q1, _anonymous_label)
        value = "y" + q1
        assert isinstance(value, _anonymous_label)
        self._assert_quoted(value, True)

    def test_coerce_quoted_switch(self):
        q1 = quoted_name("x", False)
        q2 = quoted_name(q1, True)
        eq_(q2.quote, True)

    def test_coerce_quoted_none(self):
        q1 = quoted_name("x", False)
        q2 = quoted_name(q1, None)
        eq_(q2.quote, False)

    def test_coerce_quoted_retain(self):
        q1 = quoted_name("x", False)
        q2 = quoted_name(q1, False)
        eq_(q2.quote, False)

    def test_coerce_none(self):
        q1 = quoted_name(None, False)
        eq_(q1, None)

    def test_apply_map_quoted(self):
        q1 = _anonymous_label(quoted_name("x%s", True))
        q2 = q1.apply_map(('bar'))
        eq_(q2, "xbar")
        eq_(q2.quote, True)

    def test_apply_map_plain(self):
        q1 = _anonymous_label(quoted_name("x%s", None))
        q2 = q1.apply_map(('bar'))
        eq_(q2, "xbar")
        self._assert_not_quoted(q2)

    def test_pickle_quote(self):
        q1 = quoted_name("x", True)
        for loads, dumps in picklers():
            q2 = loads(dumps(q1))
            eq_(str(q1), str(q2))
            eq_(q1.quote, q2.quote)

    def test_pickle_anon_label(self):
        q1 = _anonymous_label(quoted_name("x", True))
        for loads, dumps in picklers():
            q2 = loads(dumps(q1))
            assert isinstance(q2, _anonymous_label)
            eq_(str(q1), str(q2))
            eq_(q1.quote, q2.quote)

    def _assert_quoted(self, value, quote):
        assert isinstance(value, quoted_name)
        eq_(value.quote, quote)

    def _assert_not_quoted(self, value):
        assert not isinstance(value, quoted_name)
