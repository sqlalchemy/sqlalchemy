import testbase
from sqlalchemy import *
from sqlalchemy.databases import firebird
from sqlalchemy.exceptions import ProgrammingError
from sqlalchemy.sql import table, column
from testlib import *

class BasicTest(AssertMixin):
    # A simple import of the database/ module should work on all systems.
    def test_import(self):
        # we got this far, right?
        return True


class DomainReflectionTest(AssertMixin):
    "Test Firebird domains"

    @testing.supported('firebird')
    def setUpAll(self):
        con = testbase.db.connect()
        try:
            con.execute('CREATE DOMAIN int_domain AS INTEGER DEFAULT 42 NOT NULL')
            con.execute('CREATE DOMAIN str_domain AS VARCHAR(255)')
            con.execute('CREATE DOMAIN rem_domain AS BLOB SUB_TYPE TEXT')
            con.execute('CREATE DOMAIN img_domain AS BLOB SUB_TYPE BINARY')
        except ProgrammingError, e:
            if not "attempt to store duplicate value" in str(e):
                raise e
        con.execute('''CREATE TABLE testtable (question int_domain,
                                               answer str_domain,
                                               remark rem_domain,
                                               photo img_domain,
                                               d date,
                                               t time,
                                               dt timestamp)''')

    @testing.supported('firebird')
    def tearDownAll(self):
        con = testbase.db.connect()
        con.execute('DROP TABLE testtable')
        con.execute('DROP DOMAIN int_domain')
        con.execute('DROP DOMAIN str_domain')
        con.execute('DROP DOMAIN rem_domain')
        con.execute('DROP DOMAIN img_domain')

    @testing.supported('firebird')
    def test_table_is_reflected(self):
        metadata = MetaData(testbase.db)
        table = Table('testtable', metadata, autoload=True)
        self.assertEquals(set(table.columns.keys()),
                          set(['question', 'answer', 'remark', 'photo', 'd', 't', 'dt']),
                          "Columns of reflected table didn't equal expected columns")
        self.assertEquals(table.c.question.type.__class__, firebird.FBInteger)
        self.assertEquals(table.c.answer.type.__class__, firebird.FBString)
        self.assertEquals(table.c.remark.type.__class__, firebird.FBText)
        self.assertEquals(table.c.photo.type.__class__, firebird.FBBinary)
        # The following assume a Dialect 3 database
        self.assertEquals(table.c.d.type.__class__, firebird.FBDate)
        self.assertEquals(table.c.t.type.__class__, firebird.FBTime)
        self.assertEquals(table.c.dt.type.__class__, firebird.FBDateTime)


class CompileTest(SQLCompileTest):
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
        self.assert_compile(select([func.max(t.c.col1)]), "SELECT max(sometable.col1) FROM sometable")

        
if __name__ == '__main__':
    testbase.main()
