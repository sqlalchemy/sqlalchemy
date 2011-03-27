from sqlalchemy import *
from sqlalchemy import sql, schema
from sqlalchemy.sql import compiler
from test.lib import *

class QuoteTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = 'default'

    @classmethod
    def setup_class(cls):
        # TODO: figure out which databases/which identifiers allow special
        # characters to be used, such as: spaces, quote characters,
        # punctuation characters, set up tests for those as well.
        global table1, table2, table3
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

    def testbasic(self):
        table1.insert().execute({'lowercase':1,'UPPERCASE':2,'MixedCase':3,'a123':4},
                {'lowercase':2,'UPPERCASE':2,'MixedCase':3,'a123':4},
                {'lowercase':4,'UPPERCASE':3,'MixedCase':2,'a123':1})
        table2.insert().execute({'d123':1,'u123':2,'MixedCase':3},
                {'d123':2,'u123':2,'MixedCase':3},
                {'d123':4,'u123':3,'MixedCase':2})

        res1 = select([table1.c.lowercase, table1.c.UPPERCASE, table1.c.MixedCase, table1.c.a123]).execute().fetchall()
        print res1
        assert(res1==[(1,2,3,4),(2,2,3,4),(4,3,2,1)])

        res2 = select([table2.c.d123, table2.c.u123, table2.c.MixedCase]).execute().fetchall()
        print res2
        assert(res2==[(1,2,3),(2,2,3),(4,3,2)])

    def test_numeric(self):
        metadata = MetaData()
        t1 = Table('35table', metadata,
            Column('25column', Integer))
        self.assert_compile(schema.CreateTable(t1), 'CREATE TABLE "35table" ('
            '"25column" INTEGER'
            ')'
        )

    def testreflect(self):
        meta2 = MetaData(testing.db)
        t2 = Table('WorstCase2', meta2, autoload=True, quote=True)
        assert 'MixedCase' in t2.c

    def testlabels(self):
        table1.insert().execute({'lowercase':1,'UPPERCASE':2,'MixedCase':3,'a123':4},
                {'lowercase':2,'UPPERCASE':2,'MixedCase':3,'a123':4},
                {'lowercase':4,'UPPERCASE':3,'MixedCase':2,'a123':1})
        table2.insert().execute({'d123':1,'u123':2,'MixedCase':3},
                {'d123':2,'u123':2,'MixedCase':3},
                {'d123':4,'u123':3,'MixedCase':2})

        res1 = select([table1.c.lowercase, table1.c.UPPERCASE, table1.c.MixedCase, table1.c.a123], use_labels=True).execute().fetchall()
        print res1
        assert(res1==[(1,2,3,4),(2,2,3,4),(4,3,2,1)])

        res2 = select([table2.c.d123, table2.c.u123, table2.c.MixedCase], use_labels=True).execute().fetchall()
        print res2
        assert(res2==[(1,2,3),(2,2,3),(4,3,2)])

    def test_quote_flag(self):
        metadata = MetaData()
        t1 = Table('TableOne', metadata,
            Column('ColumnOne', Integer), schema="FooBar")
        self.assert_compile(t1.select(), '''SELECT "FooBar"."TableOne"."ColumnOne" FROM "FooBar"."TableOne"''')

        metadata = MetaData()
        t1 = Table('t1', metadata,
            Column('col1', Integer, quote=True), quote=True, schema="foo", quote_schema=True)
        self.assert_compile(t1.select(), '''SELECT "foo"."t1"."col1" FROM "foo"."t1"''')

        self.assert_compile(t1.select().apply_labels(), '''SELECT "foo"."t1"."col1" AS "foo_t1_col1" FROM "foo"."t1"''')
        a = t1.select().alias('anon')
        b = select([1], a.c.col1==2, from_obj=a)
        self.assert_compile(b, 
            '''SELECT 1 FROM (SELECT "foo"."t1"."col1" AS "col1" FROM '''\
            '''"foo"."t1") AS anon WHERE anon."col1" = :col1_1'''
        )

        metadata = MetaData()
        t1 = Table('TableOne', metadata,
            Column('ColumnOne', Integer, quote=False), quote=False, schema="FooBar", quote_schema=False)
        self.assert_compile(t1.select(), "SELECT FooBar.TableOne.ColumnOne FROM FooBar.TableOne")

        self.assert_compile(t1.select().apply_labels(), 
            "SELECT FooBar.TableOne.ColumnOne AS "\
            "FooBar_TableOne_ColumnOne FROM FooBar.TableOne"   # TODO: is this what we really want here ?  what if table/schema 
                                                               # *are* quoted?
        )

        a = t1.select().alias('anon')
        b = select([1], a.c.ColumnOne==2, from_obj=a)
        self.assert_compile(b, 
            "SELECT 1 FROM (SELECT FooBar.TableOne.ColumnOne AS "\
            "ColumnOne FROM FooBar.TableOne) AS anon WHERE anon.ColumnOne = :ColumnOne_1"
        )



    def test_table_quote_flag(self):
        metadata = MetaData()
        t1 = Table('TableOne', metadata,
                   Column('id', Integer),
                   quote=False)
        t2 = Table('TableTwo', metadata,
                   Column('id', Integer),
                   Column('t1_id', Integer, ForeignKey('TableOne.id')),
                   quote=False)

        self.assert_compile(
            t2.join(t1).select(),
            "SELECT TableTwo.id, TableTwo.t1_id, TableOne.id "
            "FROM TableTwo JOIN TableOne ON TableOne.id = TableTwo.t1_id")

    @testing.crashes('oracle', 'FIXME: unknown, verify not fails_on')
    @testing.requires.subqueries
    def testlabels(self):
        """test the quoting of labels.

        if labels arent quoted, a query in postgresql in particular will fail since it produces:

        SELECT LaLa.lowercase, LaLa."UPPERCASE", LaLa."MixedCase", LaLa."ASC"
        FROM (SELECT DISTINCT "WorstCase1".lowercase AS lowercase, 
                "WorstCase1"."UPPERCASE" AS UPPERCASE, 
                "WorstCase1"."MixedCase" AS MixedCase, "WorstCase1"."ASC" AS ASC \nFROM "WorstCase1") AS LaLa

        where the "UPPERCASE" column of "LaLa" doesnt exist.
        """

        x = table1.select(distinct=True).alias("LaLa").select().scalar()

    def testlabels2(self):
        metadata = MetaData()
        table = Table("ImATable", metadata,
            Column("col1", Integer))
        x = select([table.c.col1.label("ImATable_col1")]).alias("SomeAlias")
        self.assert_compile(select([x.c.ImATable_col1]),
            '''SELECT "SomeAlias"."ImATable_col1" FROM (SELECT "ImATable".col1 AS "ImATable_col1" FROM "ImATable") AS "SomeAlias"''')

        # note that 'foo' and 'FooCol' are literals already quoted
        x = select([sql.literal_column("'foo'").label("somelabel")], from_obj=[table]).alias("AnAlias")
        x = x.select()
        self.assert_compile(x,
            '''SELECT "AnAlias".somelabel FROM (SELECT 'foo' AS somelabel FROM "ImATable") AS "AnAlias"''')

        x = select([sql.literal_column("'FooCol'").label("SomeLabel")], from_obj=[table])
        x = x.select()
        self.assert_compile(x,
            '''SELECT "SomeLabel" FROM (SELECT 'FooCol' AS "SomeLabel" FROM "ImATable")''')

    def test_reserved_words(self):
        metadata = MetaData()
        table = Table("ImATable", metadata,
            Column("col1", Integer),
            Column("from", Integer, key="morf"),
            Column("louisville", Integer),
            Column("order", Integer))
        x = select([table.c.col1, table.c.morf, table.c.louisville, table.c.order])

        self.assert_compile(x, 
            '''SELECT "ImATable".col1, "ImATable"."from", "ImATable".louisville, "ImATable"."order" FROM "ImATable"''')
        

class PreparerTest(fixtures.TestBase):
    """Test the db-agnostic quoting services of IdentifierPreparer."""

    def test_unformat(self):
        prep = compiler.IdentifierPreparer(None)
        unformat = prep.unformat_identifiers

        def a_eq(have, want):
            if have != want:
                print "Wanted %s" % want
                print "Received %s" % have
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
                super(Custom, self).__init__(dialect, initial_quote='`',
                                             final_quote='`')
            def _escape_identifier(self, value):
                return value.replace('`', '``')
            def _unescape_identifier(self, value):
                return value.replace('``', '`')

        prep = Custom(None)
        unformat = prep.unformat_identifiers

        def a_eq(have, want):
            if have != want:
                print "Wanted %s" % want
                print "Received %s" % have
            self.assert_(have == want)

        a_eq(unformat('foo'), ['foo'])
        a_eq(unformat('`foo`'), ['foo'])
        a_eq(unformat(`'foo'`), ["'foo'"])
        a_eq(unformat('foo.bar'), ['foo', 'bar'])
        a_eq(unformat('`foo`.`bar`'), ['foo', 'bar'])
        a_eq(unformat('foo.`bar`'), ['foo', 'bar'])
        a_eq(unformat('`foo`.bar'), ['foo', 'bar'])
        a_eq(unformat('`foo`.`b``a``r`.`baz`'), ['foo', 'b`a`r', 'baz'])

