import testenv; testenv.configure_for_tests()
import datetime, os, pickleable, re
from sqlalchemy import *
from sqlalchemy import types, exc
from sqlalchemy.orm import *
from sqlalchemy.sql import table, column
from sqlalchemy.databases import mssql
import sqlalchemy.engine.url as url
from testlib import *
from testlib.testing import eq_


class CompileTest(TestBase, AssertsCompiledSQL):
    __dialect__ = mssql.MSSQLDialect()

    def test_insert(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.insert(), "INSERT INTO sometable (somecolumn) VALUES (:somecolumn)")

    def test_update(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.update(t.c.somecolumn==7), "UPDATE sometable SET somecolumn=:somecolumn WHERE sometable.somecolumn = :somecolumn_1", dict(somecolumn=10))

    def test_in_with_subqueries(self):
        """Test that when using subqueries in a binary expression
        the == and != are changed to IN and NOT IN respectively.

        """

        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.select().where(t.c.somecolumn==t.select()), "SELECT sometable.somecolumn FROM sometable WHERE sometable.somecolumn IN (SELECT sometable.somecolumn FROM sometable)")
        self.assert_compile(t.select().where(t.c.somecolumn!=t.select()), "SELECT sometable.somecolumn FROM sometable WHERE sometable.somecolumn NOT IN (SELECT sometable.somecolumn FROM sometable)")

    def test_count(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.count(), "SELECT count(sometable.somecolumn) AS tbl_row_count FROM sometable")

    def test_noorderby_insubquery(self):
        """test that the ms-sql dialect removes ORDER BY clauses from subqueries"""

        table1 = table('mytable',
            column('myid', Integer),
            column('name', String),
            column('description', String),
        )

        q = select([table1.c.myid], order_by=[table1.c.myid]).alias('foo')
        crit = q.c.myid == table1.c.myid
        self.assert_compile(select(['*'], crit), """SELECT * FROM (SELECT mytable.myid AS myid FROM mytable) AS foo, mytable WHERE foo.myid = mytable.myid""")

    def test_aliases_schemas(self):
        metadata = MetaData()
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String),
            column('description', String),
        )

        table4 = Table(
            'remotetable', metadata,
            Column('rem_id', Integer, primary_key=True),
            Column('datatype_id', Integer),
            Column('value', String(20)),
            schema = 'remote_owner'
        )

        s = table4.select()
        c = s.compile(dialect=self.__dialect__)
        assert table4.c.rem_id in set(c.result_map['rem_id'][1])

        s = table4.select(use_labels=True)
        c = s.compile(dialect=self.__dialect__)
        print c.result_map
        assert table4.c.rem_id in set(c.result_map['remote_owner_remotetable_rem_id'][1])

        self.assert_compile(table4.select(), "SELECT remotetable_1.rem_id, remotetable_1.datatype_id, remotetable_1.value FROM remote_owner.remotetable AS remotetable_1")
        
        self.assert_compile(table4.select(use_labels=True), "SELECT remotetable_1.rem_id AS remote_owner_remotetable_rem_id, remotetable_1.datatype_id AS remote_owner_remotetable_datatype_id, remotetable_1.value AS remote_owner_remotetable_value FROM remote_owner.remotetable AS remotetable_1")

        self.assert_compile(table1.join(table4, table1.c.myid==table4.c.rem_id).select(), "SELECT mytable.myid, mytable.name, mytable.description, remotetable_1.rem_id, remotetable_1.datatype_id, remotetable_1.value FROM mytable JOIN remote_owner.remotetable AS remotetable_1 ON remotetable_1.rem_id = mytable.myid")

    def test_delete_schema(self):
        metadata = MetaData()
        tbl = Table('test', metadata, Column('id', Integer, primary_key=True), schema='paj')
        self.assert_compile(tbl.delete(tbl.c.id == 1), "DELETE FROM paj.test WHERE paj.test.id = :id_1")

        s = select([tbl.c.id]).where(tbl.c.id==1)
        self.assert_compile(tbl.delete().where(tbl.c.id==(s)), "DELETE FROM paj.test WHERE paj.test.id IN (SELECT test_1.id FROM paj.test AS test_1 WHERE test_1.id = :id_1)")

    def test_union(self):
        t1 = table('t1',
            column('col1'),
            column('col2'),
            column('col3'),
            column('col4')
            )
        t2 = table('t2',
            column('col1'),
            column('col2'),
            column('col3'),
            column('col4'))

        (s1, s2) = (
                    select([t1.c.col3.label('col3'), t1.c.col4.label('col4')], t1.c.col2.in_(["t1col2r1", "t1col2r2"])),
            select([t2.c.col3.label('col3'), t2.c.col4.label('col4')], t2.c.col2.in_(["t2col2r2", "t2col2r3"]))
        )
        u = union(s1, s2, order_by=['col3', 'col4'])
        self.assert_compile(u, "SELECT t1.col3 AS col3, t1.col4 AS col4 FROM t1 WHERE t1.col2 IN (:col2_1, :col2_2) "\
        "UNION SELECT t2.col3 AS col3, t2.col4 AS col4 FROM t2 WHERE t2.col2 IN (:col2_3, :col2_4) ORDER BY col3, col4")

        self.assert_compile(u.alias('bar').select(), "SELECT bar.col3, bar.col4 FROM (SELECT t1.col3 AS col3, t1.col4 AS col4 FROM t1 WHERE "\
        "t1.col2 IN (:col2_1, :col2_2) UNION SELECT t2.col3 AS col3, t2.col4 AS col4 FROM t2 WHERE t2.col2 IN (:col2_3, :col2_4)) AS bar")

    def test_function(self):
        self.assert_compile(func.foo(1, 2), "foo(:foo_1, :foo_2)")
        self.assert_compile(func.current_time(), "CURRENT_TIME")
        self.assert_compile(func.foo(), "foo()")

        m = MetaData()
        t = Table('sometable', m, Column('col1', Integer), Column('col2', Integer))
        self.assert_compile(select([func.max(t.c.col1)]), "SELECT max(sometable.col1) AS max_1 FROM sometable")

    def test_function_overrides(self):
        self.assert_compile(func.current_date(), "GETDATE()")
        self.assert_compile(func.length(3), "LEN(:length_1)")

class ReflectionTest(TestBase):
    __only_on__ = 'mssql'

    def testidentity(self):
        meta = MetaData(testing.db)
        table = Table(
            'identity_test', meta,
            Column('col1', Integer, Sequence('fred', 2, 3), primary_key=True)
        )
        table.create()

        meta2 = MetaData(testing.db)
        try:
            table2 = Table('identity_test', meta2, autoload=True)
            assert table2.c['col1'].sequence.start == 2
            assert table2.c['col1'].sequence.increment == 3
        finally:
            table.drop()


class QueryTest(TestBase):
    __only_on__ = 'mssql'

    def test_fetchid_trigger(self):
        meta = MetaData(testing.db)
        t1 = Table('t1', meta,
                Column('id', Integer, Sequence('fred', 100, 1), primary_key=True),
                Column('descr', String(200)))
        t2 = Table('t2', meta,
                Column('id', Integer, Sequence('fred', 200, 1), primary_key=True),
                Column('descr', String(200)))
        meta.create_all()
        con = testing.db.connect()
        con.execute("""create trigger paj on t1 for insert as
            insert into t2 (descr) select descr from inserted""")

        try:
            tr = con.begin()
            r = con.execute(t2.insert(), descr='hello')
            self.assert_(r.last_inserted_ids() == [200])
            r = con.execute(t1.insert(), descr='hello')
            self.assert_(r.last_inserted_ids() == [100])

        finally:
            tr.commit()
            con.execute("""drop trigger paj""")
            meta.drop_all()

    def test_insertid_schema(self):
        meta = MetaData(testing.db)
        con = testing.db.connect()
        con.execute('create schema paj')
        tbl = Table('test', meta, Column('id', Integer, primary_key=True), schema='paj')
        tbl.create()
        try:
            tbl.insert().execute({'id':1})
        finally:
            tbl.drop()
            con.execute('drop schema paj')

    def test_delete_schema(self):
        meta = MetaData(testing.db)
        con = testing.db.connect()
        con.execute('create schema paj')
        tbl = Table('test', meta, Column('id', Integer, primary_key=True), schema='paj')
        tbl.create()
        try:
            tbl.insert().execute({'id':1})
            tbl.delete(tbl.c.id == 1).execute()
        finally:
            tbl.drop()
            con.execute('drop schema paj')

    def test_insertid_reserved(self):
        meta = MetaData(testing.db)
        table = Table(
            'select', meta,
            Column('col', Integer, primary_key=True)
        )
        table.create()

        meta2 = MetaData(testing.db)
        try:
            table.insert().execute(col=7)
        finally:
            table.drop()


class Foo(object):
    def __init__(self, **kw):
        for k in kw:
            setattr(self, k, kw[k])

class GenerativeQueryTest(TestBase):
    __only_on__ = 'mssql'

    def setUpAll(self):
        global foo, metadata
        metadata = MetaData(testing.db)
        foo = Table('foo', metadata,
                    Column('id', Integer, Sequence('foo_id_seq'),
                           primary_key=True),
                    Column('bar', Integer),
                    Column('range', Integer))

        mapper(Foo, foo)
        metadata.create_all()

        sess = create_session(bind=testing.db)
        for i in range(100):
            sess.save(Foo(bar=i, range=i%10))
        sess.flush()

    def tearDownAll(self):
        metadata.drop_all()
        clear_mappers()

    def test_slice_mssql(self):
        sess = create_session(bind=testing.db)
        query = sess.query(Foo)
        orig = query.all()
        assert list(query[:10]) == orig[:10]
        assert list(query[:10]) == orig[:10]


class SchemaTest(TestBase):

    def setUp(self):
        t = Table('sometable', MetaData(),
            Column('pk_column', Integer),
            Column('test_column', String)
        )
        self.column = t.c.test_column

    def test_that_mssql_default_nullability_emits_null(self):
        schemagenerator = \
            mssql.MSSQLDialect().schemagenerator(mssql.MSSQLDialect(), None)
        column_specification = \
            schemagenerator.get_column_specification(self.column)
        eq_("test_column VARCHAR NULL", column_specification)

    def test_that_mssql_none_nullability_does_not_emit_nullability(self):
        schemagenerator = \
            mssql.MSSQLDialect().schemagenerator(mssql.MSSQLDialect(), None)
        self.column.nullable = None
        column_specification = \
            schemagenerator.get_column_specification(self.column)
        eq_("test_column VARCHAR", column_specification)

    def test_that_mssql_specified_nullable_emits_null(self):
        schemagenerator = \
            mssql.MSSQLDialect().schemagenerator(mssql.MSSQLDialect(), None)
        self.column.nullable = True
        column_specification = \
            schemagenerator.get_column_specification(self.column)
        eq_("test_column VARCHAR NULL", column_specification)

    def test_that_mssql_specified_not_nullable_emits_not_null(self):
        schemagenerator = \
            mssql.MSSQLDialect().schemagenerator(mssql.MSSQLDialect(), None)
        self.column.nullable = False
        column_specification = \
            schemagenerator.get_column_specification(self.column)
        eq_("test_column VARCHAR NOT NULL", column_specification)


def full_text_search_missing():
    """Test if full text search is not implemented and return False if 
    it is and True otherwise."""

    try:
        connection = testing.db.connect()
        try:
            connection.execute("CREATE FULLTEXT CATALOG Catalog AS DEFAULT")
            return False
        except:
            return True
    finally:
        connection.close()

class MatchTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'mssql'
    __skip_if__ = (full_text_search_missing, )

    def setUpAll(self):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)
        
        cattable = Table('cattable', metadata,
            Column('id', Integer),
            Column('description', String(50)),
            PrimaryKeyConstraint('id', name='PK_cattable'),
        )
        matchtable = Table('matchtable', metadata,
            Column('id', Integer),
            Column('title', String(200)),
            Column('category_id', Integer, ForeignKey('cattable.id')),
            PrimaryKeyConstraint('id', name='PK_matchtable'),
        )
        DDL("""CREATE FULLTEXT INDEX 
                       ON cattable (description) 
                       KEY INDEX PK_cattable"""
                   ).execute_at('after-create', matchtable)
        DDL("""CREATE FULLTEXT INDEX 
                       ON matchtable (title) 
                       KEY INDEX PK_matchtable"""
                   ).execute_at('after-create', matchtable)
        metadata.create_all()

        cattable.insert().execute([
            {'id': 1, 'description': 'Python'},
            {'id': 2, 'description': 'Ruby'},
        ])
        matchtable.insert().execute([
            {'id': 1, 'title': 'Agile Web Development with Rails', 'category_id': 2},
            {'id': 2, 'title': 'Dive Into Python', 'category_id': 1},
            {'id': 3, 'title': 'Programming Matz''s Ruby', 'category_id': 2},
            {'id': 4, 'title': 'The Definitive Guide to Django', 'category_id': 1},
            {'id': 5, 'title': 'Python in a Nutshell', 'category_id': 1}
        ])
        DDL("WAITFOR DELAY '00:00:05'").execute(bind=engines.testing_engine())

    def tearDownAll(self):
        metadata.drop_all()
        connection = testing.db.connect()
        connection.execute("DROP FULLTEXT CATALOG Catalog")
        connection.close()

    def test_expression(self):
        self.assert_compile(matchtable.c.title.match('somstr'), "CONTAINS (matchtable.title, ?)")

    def test_simple_match(self):
        results = matchtable.select().where(matchtable.c.title.match('python')).order_by(matchtable.c.id).execute().fetchall()
        self.assertEquals([2, 5], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = matchtable.select().where(matchtable.c.title.match('"Matz''s"')).execute().fetchall()
        self.assertEquals([3], [r.id for r in results])

    def test_simple_prefix_match(self):
        results = matchtable.select().where(matchtable.c.title.match('"nut*"')).execute().fetchall()
        self.assertEquals([5], [r.id for r in results])

    def test_simple_inflectional_match(self):
        results = matchtable.select().where(matchtable.c.title.match('FORMSOF(INFLECTIONAL, "dives")')).execute().fetchall()
        self.assertEquals([2], [r.id for r in results])

    def test_or_match(self):
        results1 = matchtable.select().where(or_(matchtable.c.title.match('nutshell'), 
                                                 matchtable.c.title.match('ruby'))
                                            ).order_by(matchtable.c.id).execute().fetchall()
        self.assertEquals([3, 5], [r.id for r in results1])
        results2 = matchtable.select().where(matchtable.c.title.match('nutshell OR ruby'), 
                                            ).order_by(matchtable.c.id).execute().fetchall()
        self.assertEquals([3, 5], [r.id for r in results2])    

    def test_and_match(self):
        results1 = matchtable.select().where(and_(matchtable.c.title.match('python'), 
                                                  matchtable.c.title.match('nutshell'))
                                            ).execute().fetchall()
        self.assertEquals([5], [r.id for r in results1])
        results2 = matchtable.select().where(matchtable.c.title.match('python AND nutshell'), 
                                            ).execute().fetchall()
        self.assertEquals([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = matchtable.select().where(and_(cattable.c.id==matchtable.c.category_id, 
                                            or_(cattable.c.description.match('Ruby'), 
                                                matchtable.c.title.match('nutshell')))
                                           ).order_by(matchtable.c.id).execute().fetchall()
        self.assertEquals([1, 3, 5], [r.id for r in results])


class ParseConnectTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'mssql'

    def test_pyodbc_connect_dsn_trusted(self):
        u = url.make_url('mssql://mydsn')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['dsn=mydsn;TrustedConnection=Yes'], {}], connection)

    def test_pyodbc_connect_old_style_dsn_trusted(self):
        u = url.make_url('mssql:///?dsn=mydsn')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['dsn=mydsn;TrustedConnection=Yes'], {}], connection)

    def test_pyodbc_connect_dsn_non_trusted(self):
        u = url.make_url('mssql://username:password@mydsn')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['dsn=mydsn;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_dsn_extra(self):
        u = url.make_url('mssql://username:password@mydsn/?LANGUAGE=us_english&foo=bar')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['dsn=mydsn;UID=username;PWD=password;LANGUAGE=us_english;foo=bar'], {}], connection)

    def test_pyodbc_connect(self):
        u = url.make_url('mssql://username:password@hostspec/database')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['DRIVER={SQL Server};Server=hostspec;Database=database;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_comma_port(self):
        u = url.make_url('mssql://username:password@hostspec:12345/database')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['DRIVER={SQL Server};Server=hostspec,12345;Database=database;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_config_port(self):
        u = url.make_url('mssql://username:password@hostspec/database?port=12345')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['DRIVER={SQL Server};Server=hostspec;Database=database;UID=username;PWD=password;port=12345'], {}], connection)

    def test_pyodbc_extra_connect(self):
        u = url.make_url('mssql://username:password@hostspec/database?LANGUAGE=us_english&foo=bar')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['DRIVER={SQL Server};Server=hostspec;Database=database;UID=username;PWD=password;foo=bar;LANGUAGE=us_english'], {}], connection)

    def test_pyodbc_odbc_connect(self):
        u = url.make_url('mssql:///?odbc_connect=DRIVER%3D%7BSQL+Server%7D%3BServer%3Dhostspec%3BDatabase%3Ddatabase%3BUID%3Dusername%3BPWD%3Dpassword')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['DRIVER={SQL Server};Server=hostspec;Database=database;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_odbc_connect_with_dsn(self):
        u = url.make_url('mssql:///?odbc_connect=dsn%3Dmydsn%3BDatabase%3Ddatabase%3BUID%3Dusername%3BPWD%3Dpassword')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['dsn=mydsn;Database=database;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_odbc_connect_ignores_other_values(self):
        u = url.make_url('mssql://userdiff:passdiff@localhost/dbdiff?odbc_connect=DRIVER%3D%7BSQL+Server%7D%3BServer%3Dhostspec%3BDatabase%3Ddatabase%3BUID%3Dusername%3BPWD%3Dpassword')
        dialect = mssql.MSSQLDialect_pyodbc()
        connection = dialect.create_connect_args(u)
        self.assertEquals([['DRIVER={SQL Server};Server=hostspec;Database=database;UID=username;PWD=password'], {}], connection)


class TypesTest(TestBase):
    __only_on__ = 'mssql'

    def setUpAll(self):
        global numeric_table, metadata
        metadata = MetaData(testing.db)
        numeric_table = Table('numeric_table', metadata,
            Column('id', Integer, Sequence('numeric_id_seq', optional=True), primary_key=True),
            Column('numericcol', Numeric(asdecimal=False))
        )
        metadata.create_all()

    def tearDownAll(self):
        metadata.drop_all()

    def tearDown(self):
        numeric_table.delete().execute()

    def test_decimal_e_notation(self):
        from decimal import Decimal

        try:
            numeric_table.insert().execute(numericcol=Decimal('4.1'))
            numeric_table.insert().execute(numericcol=Decimal('1E-1'))
            numeric_table.insert().execute(numericcol=Decimal('1E-2'))
            numeric_table.insert().execute(numericcol=Decimal('1E-3'))
            numeric_table.insert().execute(numericcol=Decimal('1E-4'))
            numeric_table.insert().execute(numericcol=Decimal('1E-5'))
            numeric_table.insert().execute(numericcol=Decimal('1E-6'))
            numeric_table.insert().execute(numericcol=Decimal('1E-7'))
            numeric_table.insert().execute(numericcol=Decimal('1E-8'))
            numeric_table.insert().execute(numericcol=10000)
        except Exception, e:
            raise e


class TypesTest2(TestBase, AssertsExecutionResults):
    "Test Microsoft SQL Server column types"

    __only_on__ = 'mssql'

    def test_money(self):
        "Exercise type specification for money types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSMoney, [], {},
             'MONEY'),
            (mssql.MSSmallMoney, [], {},
             'SMALLMONEY'),
           ]

        table_args = ['test_mssql_money', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw), nullable=None))

        money_table = Table(*table_args)
        gen = testing.db.dialect.schemagenerator(testing.db.dialect, testing.db, None, None)

        for col in money_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            money_table.create(checkfirst=True)
            assert True
        except:
            raise
        money_table.drop()

    def test_dates(self):
        "Exercise type specification for date types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSDateTime, [], {},
             'DATETIME', []),

            (mssql.MSDate, [], {},
             'DATE', ['>=', (10,)]),
            (mssql.MSDate, [], {},
             'DATETIME', ['<', (10,)], mssql.MSDateTime),

            (mssql.MSTime, [], {},
             'TIME', ['>=', (10,)]),
            (mssql.MSTime, [1], {},
             'TIME(1)', ['>=', (10,)]),
            (mssql.MSTime, [], {},
             'DATETIME', ['<', (10,)], mssql.MSDateTime),

            (mssql.MSSmallDateTime, [], {},
             'SMALLDATETIME', []),

            (mssql.MSDateTimeOffset, [], {},
             'DATETIMEOFFSET', ['>=', (10,)]),
            (mssql.MSDateTimeOffset, [1], {},
             'DATETIMEOFFSET(1)', ['>=', (10,)]),

            (mssql.MSDateTime2, [], {},
             'DATETIME2', ['>=', (10,)]),
            (mssql.MSDateTime2, [1], {},
             'DATETIME2(1)', ['>=', (10,)]),

            ]

        table_args = ['test_mssql_dates', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res, requires = spec[0:5]
            if (requires and testing._is_excluded('mssql', *requires)) or not requires:
                table_args.append(Column('c%s' % index, type_(*args, **kw), nullable=None))

        dates_table = Table(*table_args)
        gen = testing.db.dialect.schemagenerator(testing.db.dialect, testing.db, None, None)

        for col in dates_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            dates_table.create(checkfirst=True)
            assert True
        except:
            raise

        reflected_dates = Table('test_mssql_dates', MetaData(testing.db), autoload=True)
        for col in reflected_dates.c:
            index = int(col.name[1:])
            testing.eq_(testing.db.dialect.type_descriptor(col.type).__class__,
                len(columns[index]) > 5 and columns[index][5] or columns[index][0])
        dates_table.drop()

    def test_dates2(self):
        meta = MetaData(testing.db)
        t = Table('test_dates', meta,
                  Column('id', Integer,
                         Sequence('datetest_id_seq', optional=True),
                         primary_key=True),
                  Column('adate', Date),
                  Column('atime', Time),
                  Column('adatetime', DateTime))
        t.create(checkfirst=True)
        try:
            d1 = datetime.date(2007, 10, 30)
            t1 = datetime.time(11, 2, 32)
            d2 = datetime.datetime(2007, 10, 30, 11, 2, 32)
            t.insert().execute(adate=d1, adatetime=d2, atime=t1)
            t.insert().execute(adate=d2, adatetime=d2, atime=d2)

            x = t.select().execute().fetchall()[0]
            self.assert_(x.adate.__class__ == datetime.date)
            self.assert_(x.atime.__class__ == datetime.time)
            self.assert_(x.adatetime.__class__ == datetime.datetime)

            t.delete().execute()

            t.insert().execute(adate=d1, adatetime=d2, atime=t1)

            self.assertEquals(select([t.c.adate, t.c.atime, t.c.adatetime], t.c.adate==d1).execute().fetchall(), [(d1, t1, d2)])

        finally:
            t.drop(checkfirst=True)

    def test_binary(self):
        "Exercise type specification for binary types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSBinary, [], {},
             'BINARY'),
            (mssql.MSBinary, [10], {},
             'BINARY(10)'),

            (mssql.MSVarBinary, [], {},
             'VARBINARY'),
            (mssql.MSVarBinary, [10], {},
             'VARBINARY(10)'),

            (mssql.MSImage, [], {},
             'IMAGE'),

            (types.Binary, [], {},
             'IMAGE'),
            (types.Binary, [10], {},
             'BINARY(10)')
            ]

        table_args = ['test_mssql_binary', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw), nullable=None))

        binary_table = Table(*table_args)
        gen = testing.db.dialect.schemagenerator(testing.db.dialect, testing.db, None, None)

        for col in binary_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            binary_table.create(checkfirst=True)
            assert True
        except:
            raise

        reflected_binary = Table('test_mssql_binary', MetaData(testing.db), autoload=True)
        for col in reflected_binary.c:
            # don't test the MSGenericBinary since it's a special case and
            # reflected it will map to a MSImage or MSBinary depending
            if not testing.db.dialect.type_descriptor(binary_table.c[col.name].type).__class__ == mssql.MSGenericBinary:
                testing.eq_(testing.db.dialect.type_descriptor(col.type).__class__,
                    testing.db.dialect.type_descriptor(binary_table.c[col.name].type).__class__)
            if binary_table.c[col.name].type.length:
                testing.eq_(col.type.length, binary_table.c[col.name].type.length)
        binary_table.drop()

    def test_boolean(self):
        "Exercise type specification for boolean type."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSBoolean, [], {},
             'BIT'),
           ]

        table_args = ['test_mssql_boolean', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw), nullable=None))

        boolean_table = Table(*table_args)
        gen = testing.db.dialect.schemagenerator(testing.db.dialect, testing.db, None, None)

        for col in boolean_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            boolean_table.create(checkfirst=True)
            assert True
        except:
            raise
        boolean_table.drop()

    def test_numeric(self):
        "Exercise type specification and options for numeric types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSNumeric, [], {},
             'NUMERIC(10, 2)'),
            (mssql.MSNumeric, [None], {},
             'NUMERIC'),
            (mssql.MSNumeric, [12], {},
             'NUMERIC(12, 2)'),
            (mssql.MSNumeric, [12, 4], {},
             'NUMERIC(12, 4)'),

            (mssql.MSFloat, [], {},
             'FLOAT(10)'),
            (mssql.MSFloat, [None], {},
             'FLOAT'),
            (mssql.MSFloat, [12], {},
             'FLOAT(12)'),
            (mssql.MSReal, [], {},
             'REAL'),

            (mssql.MSInteger, [], {},
             'INTEGER'),
            (mssql.MSBigInteger, [], {},
             'BIGINT'),
            (mssql.MSTinyInteger, [], {},
             'TINYINT'),
            (mssql.MSSmallInteger, [], {},
             'SMALLINT'),
           ]

        table_args = ['test_mssql_numeric', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw), nullable=None))

        numeric_table = Table(*table_args)
        gen = testing.db.dialect.schemagenerator(testing.db.dialect, testing.db, None, None)

        for col in numeric_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            numeric_table.create(checkfirst=True)
            assert True
        except:
            raise
        numeric_table.drop()

    def test_char(self):
        """Exercise COLLATE-ish options on string types."""

        # modify the text_as_varchar setting since we are not testing that behavior here
        text_as_varchar = testing.db.dialect.text_as_varchar
        testing.db.dialect.text_as_varchar = False

        columns = [
            (mssql.MSChar, [], {},
             'CHAR'),
            (mssql.MSChar, [1], {},
             'CHAR(1)'),
            (mssql.MSChar, [1], {'collation': 'Latin1_General_CI_AS'},
             'CHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSNChar, [], {},
             'NCHAR'),
            (mssql.MSNChar, [1], {},
             'NCHAR(1)'),
            (mssql.MSNChar, [1], {'collation': 'Latin1_General_CI_AS'},
             'NCHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSString, [], {},
             'VARCHAR'),
            (mssql.MSString, [1], {},
             'VARCHAR(1)'),
            (mssql.MSString, [1], {'collation': 'Latin1_General_CI_AS'},
             'VARCHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSNVarchar, [], {},
             'NVARCHAR'),
            (mssql.MSNVarchar, [1], {},
             'NVARCHAR(1)'),
            (mssql.MSNVarchar, [1], {'collation': 'Latin1_General_CI_AS'},
             'NVARCHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSText, [], {},
             'TEXT'),
            (mssql.MSText, [], {'collation': 'Latin1_General_CI_AS'},
             'TEXT COLLATE Latin1_General_CI_AS'),

            (mssql.MSNText, [], {},
             'NTEXT'),
            (mssql.MSNText, [], {'collation': 'Latin1_General_CI_AS'},
             'NTEXT COLLATE Latin1_General_CI_AS'),
           ]

        table_args = ['test_mssql_charset', MetaData(testing.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw), nullable=None))

        charset_table = Table(*table_args)
        gen = testing.db.dialect.schemagenerator(testing.db.dialect, testing.db, None, None)

        for col in charset_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            charset_table.create(checkfirst=True)
            assert True
        except:
            raise
        charset_table.drop()

        testing.db.dialect.text_as_varchar = text_as_varchar

    def test_timestamp(self):
        """Exercise TIMESTAMP column."""

        meta = MetaData(testing.db)

        try:
            columns = [
                (TIMESTAMP,
                 'TIMESTAMP'),
                (mssql.MSTimeStamp,
                 'TIMESTAMP'),
                ]
            for idx, (spec, expected) in enumerate(columns):
                t = Table('mssql_ts%s' % idx, meta,
                          Column('id', Integer, primary_key=True),
                          Column('t', spec, nullable=None))
                testing.eq_(colspec(t.c.t), "t %s" % expected)
                self.assert_(repr(t.c.t))
                try:
                    t.create(checkfirst=True)
                    assert True
                except:
                    raise
                t.drop()
        finally:
            meta.drop_all()

    def test_autoincrement(self):
        meta = MetaData(testing.db)
        try:
            Table('ai_1', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True))
            Table('ai_2', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True))
            Table('ai_3', meta,
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_4', meta,
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_n2', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False))
            Table('ai_5', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, DefaultClause('0'),
                         primary_key=True, autoincrement=False))
            Table('ai_6', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_7', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('o2', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_8', meta,
                  Column('o1', String(1), DefaultClause('x'),
                         primary_key=True),
                  Column('o2', String(1), DefaultClause('x'),
                         primary_key=True))
            meta.create_all()

            table_names = ['ai_1', 'ai_2', 'ai_3', 'ai_4',
                           'ai_5', 'ai_6', 'ai_7', 'ai_8']
            mr = MetaData(testing.db)
            mr.reflect(only=table_names)

            for tbl in [mr.tables[name] for name in table_names]:
                for c in tbl.c:
                    if c.name.startswith('int_y'):
                        assert c.autoincrement
                    elif c.name.startswith('int_n'):
                        assert not c.autoincrement
                tbl.insert().execute()
                if 'int_y' in tbl.c:
                    assert select([tbl.c.int_y]).scalar() == 1
                    assert list(tbl.select().execute().fetchone()).count(1) == 1
                else:
                    assert 1 not in list(tbl.select().execute().fetchone())
        finally:
            meta.drop_all()

def colspec(c):
    return testing.db.dialect.schemagenerator(testing.db.dialect,
        testing.db, None, None).get_column_specification(c)


class BinaryTest(TestBase, AssertsExecutionResults):
    """Test the Binary and VarBinary types"""
    def setUpAll(self):
        global binary_table, MyPickleType

        class MyPickleType(types.TypeDecorator):
            impl = PickleType

            def process_bind_param(self, value, dialect):
                if value:
                    value.stuff = 'this is modified stuff'
                return value

            def process_result_value(self, value, dialect):
                if value:
                    value.stuff = 'this is the right stuff'
                return value

        binary_table = Table('binary_table', MetaData(testing.db),
        Column('primary_id', Integer, Sequence('binary_id_seq', optional=True), primary_key=True),
        Column('data', mssql.MSVarBinary(8000)),
        Column('data_image', mssql.MSImage),
        Column('data_slice', Binary(100)),
        Column('misc', String(30)),
        # construct PickleType with non-native pickle module, since cPickle uses relative module
        # loading and confuses this test's parent package 'sql' with the 'sqlalchemy.sql' package relative
        # to the 'types' module
        Column('pickled', PickleType),
        Column('mypickle', MyPickleType)
        )
        binary_table.create()

    def tearDown(self):
        binary_table.delete().execute()

    def tearDownAll(self):
        binary_table.drop()

    def test_binary(self):
        testobj1 = pickleable.Foo('im foo 1')
        testobj2 = pickleable.Foo('im foo 2')
        testobj3 = pickleable.Foo('im foo 3')

        stream1 =self.load_stream('binary_data_one.dat')
        stream2 =self.load_stream('binary_data_two.dat')
        binary_table.insert().execute(primary_id=1, misc='binary_data_one.dat', data=stream1, data_image=stream1, data_slice=stream1[0:100], pickled=testobj1, mypickle=testobj3)
        binary_table.insert().execute(primary_id=2, misc='binary_data_two.dat', data=stream2, data_image=stream2, data_slice=stream2[0:99], pickled=testobj2)
        binary_table.insert().execute(primary_id=3, misc='binary_data_two.dat', data=None, data_image=None, data_slice=stream2[0:99], pickled=None)

        for stmt in (
            binary_table.select(order_by=binary_table.c.primary_id),
            text("select * from binary_table order by binary_table.primary_id", typemap={'pickled':PickleType, 'mypickle':MyPickleType}, bind=testing.db)
        ):
            l = stmt.execute().fetchall()
            self.assertEquals(list(stream1), list(l[0]['data']))

            paddedstream = list(stream1[0:100])
            paddedstream.extend(['\x00'] * (100 - len(paddedstream)))
            self.assertEquals(paddedstream, list(l[0]['data_slice']))

            self.assertEquals(list(stream2), list(l[1]['data']))
            self.assertEquals(list(stream2), list(l[1]['data_image']))
            self.assertEquals(testobj1, l[0]['pickled'])
            self.assertEquals(testobj2, l[1]['pickled'])
            self.assertEquals(testobj3.moredata, l[0]['mypickle'].moredata)
            self.assertEquals(l[0]['mypickle'].stuff, 'this is the right stuff')

    def load_stream(self, name, len=3000):
        f = os.path.join(os.path.dirname(testenv.__file__), name)
        return file(f).read(len)


if __name__ == "__main__":
    testenv.main()
