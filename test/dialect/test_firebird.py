from sqlalchemy.test.testing import eq_
from sqlalchemy import *
from sqlalchemy.databases import firebird
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import table, column
from sqlalchemy.test import *


class DomainReflectionTest(TestBase, AssertsExecutionResults):
    "Test Firebird domains"

    __only_on__ = 'firebird'

    @classmethod
    def setup_class(cls):
        con = testing.db.connect()
        try:
            con.execute('CREATE DOMAIN int_domain AS INTEGER DEFAULT 42 NOT NULL')
            con.execute('CREATE DOMAIN str_domain AS VARCHAR(255)')
            con.execute('CREATE DOMAIN rem_domain AS BLOB SUB_TYPE TEXT')
            con.execute('CREATE DOMAIN img_domain AS BLOB SUB_TYPE BINARY')
        except ProgrammingError, e:
            if not "attempt to store duplicate value" in str(e):
                raise e
        con.execute('''CREATE GENERATOR gen_testtable_id''')
        con.execute('''CREATE TABLE testtable (question int_domain,
                                               answer str_domain DEFAULT 'no answer',
                                               remark rem_domain DEFAULT '',
                                               photo img_domain,
                                               d date,
                                               t time,
                                               dt timestamp)''')
        con.execute('''ALTER TABLE testtable
                       ADD CONSTRAINT testtable_pk PRIMARY KEY (question)''')
        con.execute('''CREATE TRIGGER testtable_autoid FOR testtable
                       ACTIVE BEFORE INSERT AS
                       BEGIN
                         IF (NEW.question IS NULL) THEN
                           NEW.question = gen_id(gen_testtable_id, 1);
                       END''')

    @classmethod
    def teardown_class(cls):
        con = testing.db.connect()
        con.execute('DROP TABLE testtable')
        con.execute('DROP DOMAIN int_domain')
        con.execute('DROP DOMAIN str_domain')
        con.execute('DROP DOMAIN rem_domain')
        con.execute('DROP DOMAIN img_domain')
        con.execute('DROP GENERATOR gen_testtable_id')

    def test_table_is_reflected(self):
        metadata = MetaData(testing.db)
        table = Table('testtable', metadata, autoload=True)
        eq_(set(table.columns.keys()),
                          set(['question', 'answer', 'remark', 'photo', 'd', 't', 'dt']),
                          "Columns of reflected table didn't equal expected columns")
        eq_(table.c.question.primary_key, True)
        eq_(table.c.question.sequence.name, 'gen_testtable_id')
        eq_(table.c.question.type.__class__, firebird.FBInteger)
        eq_(table.c.question.server_default.arg.text, "42")
        eq_(table.c.answer.type.__class__, firebird.FBString)
        eq_(table.c.answer.server_default.arg.text, "'no answer'")
        eq_(table.c.remark.type.__class__, firebird.FBText)
        eq_(table.c.remark.server_default.arg.text, "''")
        eq_(table.c.photo.type.__class__, firebird.FBBinary)
        # The following assume a Dialect 3 database
        eq_(table.c.d.type.__class__, firebird.FBDate)
        eq_(table.c.t.type.__class__, firebird.FBTime)
        eq_(table.c.dt.type.__class__, firebird.FBDateTime)


class CompileTest(TestBase, AssertsCompiledSQL):
    __dialect__ = firebird.FBDialect()

    def test_alias(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t.alias()])
        self.assert_compile(s, "SELECT sometable_1.col1, sometable_1.col2 FROM sometable sometable_1")

    def test_function(self):
        self.assert_compile(func.foo(1, 2), "foo(:foo_1, :foo_2)")
        self.assert_compile(func.current_time(), "CURRENT_TIME")
        self.assert_compile(func.foo(), "foo")

        m = MetaData()
        t = Table('sometable', m, Column('col1', Integer), Column('col2', Integer))
        self.assert_compile(select([func.max(t.c.col1)]), "SELECT max(sometable.col1) AS max_1 FROM sometable")

    def test_substring(self):
        self.assert_compile(func.substring('abc', 1, 2), "SUBSTRING(:substring_1 FROM :substring_2 FOR :substring_3)")
        self.assert_compile(func.substring('abc', 1), "SUBSTRING(:substring_1 FROM :substring_2)")

    def test_update_returning(self):
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String(128)),
            column('description', String(128)),
        )

        u = update(table1, values=dict(name='foo'), firebird_returning=[table1.c.myid, table1.c.name])
        self.assert_compile(u, "UPDATE mytable SET name=:name RETURNING mytable.myid, mytable.name")

        u = update(table1, values=dict(name='foo'), firebird_returning=[table1])
        self.assert_compile(u, "UPDATE mytable SET name=:name "\
            "RETURNING mytable.myid, mytable.name, mytable.description")

        u = update(table1, values=dict(name='foo'), firebird_returning=[func.length(table1.c.name)])
        self.assert_compile(u, "UPDATE mytable SET name=:name RETURNING char_length(mytable.name)")

    def test_insert_returning(self):
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String(128)),
            column('description', String(128)),
        )

        i = insert(table1, values=dict(name='foo'), firebird_returning=[table1.c.myid, table1.c.name])
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (:name) RETURNING mytable.myid, mytable.name")

        i = insert(table1, values=dict(name='foo'), firebird_returning=[table1])
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (:name) "\
            "RETURNING mytable.myid, mytable.name, mytable.description")

        i = insert(table1, values=dict(name='foo'), firebird_returning=[func.length(table1.c.name)])
        self.assert_compile(i, "INSERT INTO mytable (name) VALUES (:name) RETURNING char_length(mytable.name)")


class ReturningTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'firebird'

    @testing.exclude('firebird', '<', (2, 1), '2.1+ feature')
    def test_update_returning(self):
        meta = MetaData(testing.db)
        table = Table('tables', meta,
            Column('id', Integer, Sequence('gen_tables_id'), primary_key=True),
            Column('persons', Integer),
            Column('full', Boolean)
        )
        table.create()
        try:
            table.insert().execute([{'persons': 5, 'full': False}, {'persons': 3, 'full': False}])

            result = table.update(table.c.persons > 4, dict(full=True), firebird_returning=[table.c.id]).execute()
            eq_(result.fetchall(), [(1,)])

            result2 = select([table.c.id, table.c.full]).order_by(table.c.id).execute()
            eq_(result2.fetchall(), [(1,True),(2,False)])
        finally:
            table.drop()

    @testing.exclude('firebird', '<', (2, 0), '2.0+ feature')
    def test_insert_returning(self):
        meta = MetaData(testing.db)
        table = Table('tables', meta,
            Column('id', Integer, Sequence('gen_tables_id'), primary_key=True),
            Column('persons', Integer),
            Column('full', Boolean)
        )
        table.create()
        try:
            result = table.insert(firebird_returning=[table.c.id]).execute({'persons': 1, 'full': False})

            eq_(result.fetchall(), [(1,)])

            # Multiple inserts only return the last row
            result2 = table.insert(firebird_returning=[table]).execute(
                 [{'persons': 2, 'full': False}, {'persons': 3, 'full': True}])

            eq_(result2.fetchall(), [(3,3,True)])

            result3 = table.insert(firebird_returning=[table.c.id]).execute({'persons': 4, 'full': False})
            eq_([dict(row) for row in result3], [{'ID':4}])

            result4 = testing.db.execute('insert into tables (id, persons, "full") values (5, 10, 1) returning persons')
            eq_([dict(row) for row in result4], [{'PERSONS': 10}])
        finally:
            table.drop()

    @testing.exclude('firebird', '<', (2, 1), '2.1+ feature')
    def test_delete_returning(self):
        meta = MetaData(testing.db)
        table = Table('tables', meta,
            Column('id', Integer, Sequence('gen_tables_id'), primary_key=True),
            Column('persons', Integer),
            Column('full', Boolean)
        )
        table.create()
        try:
            table.insert().execute([{'persons': 5, 'full': False}, {'persons': 3, 'full': False}])

            result = table.delete(table.c.persons > 4, firebird_returning=[table.c.id]).execute()
            eq_(result.fetchall(), [(1,)])

            result2 = select([table.c.id, table.c.full]).order_by(table.c.id).execute()
            eq_(result2.fetchall(), [(2,False),])
        finally:
            table.drop()


class MiscFBTests(TestBase):
    __only_on__ = 'firebird'

    def test_strlen(self):
        # On FB the length() function is implemented by an external
        # UDF, strlen().  Various SA tests fail because they pass a
        # parameter to it, and that does not work (it always results
        # the maximum string length the UDF was declared to accept).
        # This test checks that at least it works ok in other cases.

        meta = MetaData(testing.db)
        t = Table('t1', meta,
            Column('id', Integer, Sequence('t1idseq'), primary_key=True),
            Column('name', String(10))
        )
        meta.create_all()
        try:
            t.insert(values=dict(name='dante')).execute()
            t.insert(values=dict(name='alighieri')).execute()
            select([func.count(t.c.id)],func.length(t.c.name)==5).execute().fetchone()[0] == 1
        finally:
            meta.drop_all()

    def test_server_version_info(self):
        version = testing.db.dialect.server_version_info(testing.db.connect())
        assert len(version) == 3, "Got strange version info: %s" % repr(version)


