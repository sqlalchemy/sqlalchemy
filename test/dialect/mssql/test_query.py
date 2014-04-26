# -*- encoding: utf-8
from sqlalchemy.testing import eq_, engines
from sqlalchemy import *
from sqlalchemy.sql import table, column
from sqlalchemy.databases import mssql
from sqlalchemy.testing import fixtures, AssertsCompiledSQL
from sqlalchemy import testing
from sqlalchemy.util import ue
from sqlalchemy import util



class SchemaAliasingTest(fixtures.TestBase, AssertsCompiledSQL):
    """SQL server cannot reference schema-qualified tables in a SELECT statement, they
    must be aliased.
    """
    __dialect__ = mssql.dialect()

    def setup(self):
        metadata = MetaData()
        self.t1 = table('t1',
            column('a', Integer),
            column('b', String),
            column('c', String),
        )
        self.t2 = Table(
            't2', metadata,
            Column("a", Integer),
            Column("b", Integer),
            Column("c", Integer),
            schema = 'schema'
        )

    def test_result_map(self):
        s = self.t2.select()
        c = s.compile(dialect=self.__dialect__)
        assert self.t2.c.a in set(c.result_map['a'][1])

    def test_result_map_use_labels(self):
        s = self.t2.select(use_labels=True)
        c = s.compile(dialect=self.__dialect__)
        assert self.t2.c.a in set(c.result_map['schema_t2_a'][1])

    def test_straight_select(self):
        self.assert_compile(self.t2.select(),
            "SELECT t2_1.a, t2_1.b, t2_1.c FROM [schema].t2 AS t2_1"
        )

    def test_straight_select_use_labels(self):
        self.assert_compile(
            self.t2.select(use_labels=True),
            "SELECT t2_1.a AS schema_t2_a, t2_1.b AS schema_t2_b, "
            "t2_1.c AS schema_t2_c FROM [schema].t2 AS t2_1"
        )

    def test_join_to_schema(self):
        t1, t2 = self.t1, self.t2
        self.assert_compile(
            t1.join(t2, t1.c.a==t2.c.a).select(),
            "SELECT t1.a, t1.b, t1.c, t2_1.a, t2_1.b, t2_1.c FROM t1 "
            "JOIN [schema].t2 AS t2_1 ON t2_1.a = t1.a"
        )

    def test_union_schema_to_non(self):
        t1, t2 = self.t1, self.t2
        s = select([t2.c.a, t2.c.b]).apply_labels().\
                union(
                    select([t1.c.a, t1.c.b]).apply_labels()
                ).alias().select()
        self.assert_compile(
            s,
            "SELECT anon_1.schema_t2_a, anon_1.schema_t2_b FROM "
            "(SELECT t2_1.a AS schema_t2_a, t2_1.b AS schema_t2_b "
            "FROM [schema].t2 AS t2_1 UNION SELECT t1.a AS t1_a, "
            "t1.b AS t1_b FROM t1) AS anon_1"
        )

    def test_column_subquery_to_alias(self):
        a1 = self.t2.alias('a1')
        s = select([self.t2, select([a1.c.a]).as_scalar()])
        self.assert_compile(
            s,
            "SELECT t2_1.a, t2_1.b, t2_1.c, "
            "(SELECT a1.a FROM [schema].t2 AS a1) "
            "AS anon_1 FROM [schema].t2 AS t2_1"

        )

class IdentityInsertTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = 'mssql'
    __dialect__ = mssql.MSDialect()

    @classmethod
    def setup_class(cls):
        global metadata, cattable
        metadata = MetaData(testing.db)

        cattable = Table('cattable', metadata,
            Column('id', Integer),
            Column('description', String(50)),
            PrimaryKeyConstraint('id', name='PK_cattable'),
        )

    def setup(self):
        metadata.create_all()

    def teardown(self):
        metadata.drop_all()

    def test_compiled(self):
        self.assert_compile(cattable.insert().values(id=9,
                            description='Python'),
                            'INSERT INTO cattable (id, description) '
                            'VALUES (:id, :description)')

    def test_execute(self):
        cattable.insert().values(id=9, description='Python').execute()

        cats = cattable.select().order_by(cattable.c.id).execute()
        eq_([(9, 'Python')], list(cats))

        result = cattable.insert().values(description='PHP').execute()
        eq_([10], result.inserted_primary_key)
        lastcat = cattable.select().order_by(desc(cattable.c.id)).execute()
        eq_((10, 'PHP'), lastcat.first())

    def test_executemany(self):
        cattable.insert().execute([{'id': 89, 'description': 'Python'},
                                  {'id': 8, 'description': 'Ruby'},
                                  {'id': 3, 'description': 'Perl'},
                                  {'id': 1, 'description': 'Java'}])
        cats = cattable.select().order_by(cattable.c.id).execute()
        eq_([(1, 'Java'), (3, 'Perl'), (8, 'Ruby'), (89, 'Python')],
            list(cats))
        cattable.insert().execute([{'description': 'PHP'},
                                  {'description': 'Smalltalk'}])
        lastcats = \
            cattable.select().order_by(desc(cattable.c.id)).limit(2).execute()
        eq_([(91, 'Smalltalk'), (90, 'PHP')], list(lastcats))

class QueryUnicodeTest(fixtures.TestBase):

    __only_on__ = 'mssql'

    def test_convert_unicode(self):
        meta = MetaData(testing.db)
        t1 = Table('unitest_table', meta, Column('id', Integer,
                   primary_key=True), Column('descr',
                   mssql.MSText(convert_unicode=True)))
        meta.create_all()
        con = testing.db.connect()

        # encode in UTF-8 (sting object) because this is the default
        # dialect encoding

        con.execute(ue("insert into unitest_table values ('bien u\
                    umang\xc3\xa9')").encode('UTF-8'))
        try:
            r = t1.select().execute().first()
            assert isinstance(r[1], util.text_type), \
                '%s is %s instead of unicode, working on %s' % (r[1],
                    type(r[1]), meta.bind)
        finally:
            meta.drop_all()

from sqlalchemy.testing.assertsql import ExactSQL
class QueryTest(testing.AssertsExecutionResults, fixtures.TestBase):
    __only_on__ = 'mssql'

    def test_fetchid_trigger(self):
        """
        Verify identity return value on inserting to a trigger table.

        MSSQL's OUTPUT INSERTED clause does not work for the
        case of a table having an identity (autoincrement)
        primary key column, and which also has a trigger configured
        to fire upon each insert and subsequently perform an
        insert into a different table.

        SQLALchemy's MSSQL dialect by default will attempt to
        use an OUTPUT_INSERTED clause, which in this case will
        raise the following error:

        ProgrammingError: (ProgrammingError) ('42000', 334,
        "[Microsoft][SQL Server Native Client 10.0][SQL Server]The
        target table 't1' of the DML statement cannot have any enabled
        triggers if the statement contains an OUTPUT clause without
        INTO clause.", 7748) 'INSERT INTO t1 (descr) OUTPUT inserted.id
        VALUES (?)' ('hello',)

        This test verifies a workaround, which is to rely on the
        older SCOPE_IDENTITY() call, which still works for this scenario.
        To enable the workaround, the Table must be instantiated
        with the init parameter 'implicit_returning = False'.
        """

        #todo: this same test needs to be tried in a multithreaded context
        #      with multiple threads inserting to the same table.
        #todo: check whether this error also occurs with clients other
        #      than the SQL Server Native Client. Maybe an assert_raises
        #      test should be written.
        meta = MetaData(testing.db)
        t1 = Table('t1', meta,
                Column('id', Integer, Sequence('fred', 100, 1),
                                primary_key=True),
                Column('descr', String(200)),
                # the following flag will prevent the
                # MSSQLCompiler.returning_clause from getting called,
                # though the ExecutionContext will still have a
                # _select_lastrowid, so the SELECT SCOPE_IDENTITY() will
                # hopefully be called instead.
                implicit_returning = False
                )
        t2 = Table('t2', meta,
                Column('id', Integer, Sequence('fred', 200, 1),
                                primary_key=True),
                Column('descr', String(200)))
        meta.create_all()
        con = testing.db.connect()
        con.execute("""create trigger paj on t1 for insert as
            insert into t2 (descr) select descr from inserted""")

        try:
            tr = con.begin()
            r = con.execute(t2.insert(), descr='hello')
            self.assert_(r.inserted_primary_key == [200])
            r = con.execute(t1.insert(), descr='hello')
            self.assert_(r.inserted_primary_key == [100])

        finally:
            tr.commit()
            con.execute("""drop trigger paj""")
            meta.drop_all()

    @testing.fails_on_everything_except('mssql+pyodbc', 'pyodbc-specific feature')
    @testing.provide_metadata
    def test_disable_scope_identity(self):
        engine = engines.testing_engine(options={"use_scope_identity": False})
        metadata = self.metadata
        metadata.bind = engine
        t1 = Table('t1', metadata,
                Column('id', Integer, primary_key=True),
                implicit_returning=False
        )
        metadata.create_all()

        self.assert_sql_execution(
                testing.db,
                lambda: engine.execute(t1.insert()),
                ExactSQL("INSERT INTO t1 DEFAULT VALUES"),
                # we don't have an event for
                # "SELECT @@IDENTITY" part here.
                # this will be in 0.8 with #2459
        )
        assert not engine.dialect.use_scope_identity

    def test_insertid_schema(self):
        meta = MetaData(testing.db)
        con = testing.db.connect()
        con.execute('create schema paj')
        tbl = Table('test', meta,
                    Column('id', Integer, primary_key=True), schema='paj')
        tbl.create()
        try:
            tbl.insert().execute({'id':1})
        finally:
            tbl.drop()
            con.execute('drop schema paj')

    def test_returning_no_autoinc(self):
        meta = MetaData(testing.db)
        table = Table('t1', meta, Column('id', Integer,
                      primary_key=True), Column('data', String(50)))
        table.create()
        try:
            result = table.insert().values(id=1,
                    data=func.lower('SomeString'
                    )).returning(table.c.id, table.c.data).execute()
            eq_(result.fetchall(), [(1, 'somestring')])
        finally:

            # this will hang if the "SET IDENTITY_INSERT t1 OFF" occurs
            # before the result is fetched

            table.drop()

    def test_delete_schema(self):
        meta = MetaData(testing.db)
        con = testing.db.connect()
        con.execute('create schema paj')
        tbl = Table('test', meta, Column('id', Integer,
                    primary_key=True), schema='paj')
        tbl.create()
        try:
            tbl.insert().execute({'id': 1})
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


def full_text_search_missing():
    """Test if full text search is not implemented and return False if
    it is and True otherwise."""

    try:
        connection = testing.db.connect()
        try:
            connection.execute('CREATE FULLTEXT CATALOG Catalog AS '
                               'DEFAULT')
            return False
        except:
            return True
    finally:
        connection.close()

class MatchTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = 'mssql'
    __skip_if__ = full_text_search_missing,

    @classmethod
    def setup_class(cls):
        global metadata, cattable, matchtable
        metadata = MetaData(testing.db)
        cattable = Table('cattable', metadata, Column('id', Integer),
                         Column('description', String(50)),
                         PrimaryKeyConstraint('id', name='PK_cattable'))
        matchtable = Table(
            'matchtable',
            metadata,
            Column('id', Integer),
            Column('title', String(200)),
            Column('category_id', Integer, ForeignKey('cattable.id')),
            PrimaryKeyConstraint('id', name='PK_matchtable'),
            )
        DDL("""CREATE FULLTEXT INDEX
                       ON cattable (description)
                       KEY INDEX PK_cattable""").execute_at('after-create'
                , matchtable)
        DDL("""CREATE FULLTEXT INDEX
                       ON matchtable (title)
                       KEY INDEX PK_matchtable""").execute_at('after-create'
                , matchtable)
        metadata.create_all()
        cattable.insert().execute([{'id': 1, 'description': 'Python'},
                                  {'id': 2, 'description': 'Ruby'}])
        matchtable.insert().execute([{'id': 1, 'title'
                                    : 'Agile Web Development with Rails'
                                    , 'category_id': 2}, {'id': 2,
                                    'title': 'Dive Into Python',
                                    'category_id': 1}, {'id': 3, 'title'
                                    : "Programming Matz's Ruby",
                                    'category_id': 2}, {'id': 4, 'title'
                                    : 'The Definitive Guide to Django',
                                    'category_id': 1}, {'id': 5, 'title'
                                    : 'Python in a Nutshell',
                                    'category_id': 1}])
        DDL("WAITFOR DELAY '00:00:05'"
            ).execute(bind=engines.testing_engine())

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()
        connection = testing.db.connect()
        connection.execute("DROP FULLTEXT CATALOG Catalog")
        connection.close()

    def test_expression(self):
        self.assert_compile(matchtable.c.title.match('somstr'),
                            'CONTAINS (matchtable.title, ?)')

    def test_simple_match(self):
        results = \
            matchtable.select().where(matchtable.c.title.match('python'
                )).order_by(matchtable.c.id).execute().fetchall()
        eq_([2, 5], [r.id for r in results])

    def test_simple_match_with_apostrophe(self):
        results = \
            matchtable.select().where(matchtable.c.title.match("Matz's"
                )).execute().fetchall()
        eq_([3], [r.id for r in results])

    def test_simple_prefix_match(self):
        results = \
            matchtable.select().where(matchtable.c.title.match('"nut*"'
                )).execute().fetchall()
        eq_([5], [r.id for r in results])

    def test_simple_inflectional_match(self):
        results = \
            matchtable.select().where(
                matchtable.c.title.match('FORMSOF(INFLECTIONAL, "dives")'
                )).execute().fetchall()
        eq_([2], [r.id for r in results])

    def test_or_match(self):
        results1 = \
            matchtable.select().where(or_(matchtable.c.title.match('nutshell'
                ), matchtable.c.title.match('ruby'
                ))).order_by(matchtable.c.id).execute().fetchall()
        eq_([3, 5], [r.id for r in results1])
        results2 = \
            matchtable.select().where(
                matchtable.c.title.match('nutshell OR ruby'
                )).order_by(matchtable.c.id).execute().fetchall()
        eq_([3, 5], [r.id for r in results2])

    def test_and_match(self):
        results1 = \
            matchtable.select().where(and_(matchtable.c.title.match('python'
                ), matchtable.c.title.match('nutshell'
                ))).execute().fetchall()
        eq_([5], [r.id for r in results1])
        results2 = \
            matchtable.select().where(
                matchtable.c.title.match('python AND nutshell'
                )).execute().fetchall()
        eq_([5], [r.id for r in results2])

    def test_match_across_joins(self):
        results = matchtable.select().where(and_(cattable.c.id
                == matchtable.c.category_id,
                or_(cattable.c.description.match('Ruby'),
                matchtable.c.title.match('nutshell'
                )))).order_by(matchtable.c.id).execute().fetchall()
        eq_([1, 3, 5], [r.id for r in results])


