import testbase
from sqlalchemy import *
from sqlalchemy.sql import table, column
from sqlalchemy.databases import oracle
from testlib import *


class OutParamTest(AssertMixin):
    __only_on__ = 'oracle'

    def setUpAll(self):
        testbase.db.execute("""
create or replace procedure foo(x_in IN number, x_out OUT number, y_out OUT number) IS
  retval number;
    begin
    retval := 6;
    x_out := 10;
    y_out := x_in * 15;
    end;
        """)

    def test_out_params(self):
        result = testbase.db.execute(text("begin foo(:x, :y, :z); end;", bindparams=[bindparam('x', Numeric), outparam('y', Numeric), outparam('z', Numeric)]), x=5)
        assert result.out_parameters == {'y':10, 'z':75}, result.out_parameters
        print result.out_parameters

    def tearDownAll(self):
         testbase.db.execute("DROP PROCEDURE foo")


class CompileTest(SQLCompileTest):
    __dialect__ = oracle.OracleDialect()

    def test_subquery(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t])
        s = select([s.c.col1, s.c.col2])

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 FROM sometable)")

    def test_limit(self):
        t = table('sometable', column('col1'), column('col2'))

        s = select([t]).limit(10).offset(20)

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2, "
            "ROW_NUMBER() OVER (ORDER BY sometable.rowid) AS ora_rn FROM sometable) WHERE ora_rn>20 AND ora_rn<=30"
        )

        s = select([s.c.col1, s.c.col2])

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2, ROW_NUMBER() OVER (ORDER BY sometable.rowid) AS ora_rn FROM sometable) WHERE ora_rn>20 AND ora_rn<=30)")

        # testing this twice to ensure oracle doesn't modify the original statement
        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2, ROW_NUMBER() OVER (ORDER BY sometable.rowid) AS ora_rn FROM sometable) WHERE ora_rn>20 AND ora_rn<=30)")

        s = select([t]).limit(10).offset(20).order_by(t.c.col2)

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, "
            "sometable.col2 AS col2, ROW_NUMBER() OVER (ORDER BY sometable.col2) AS ora_rn FROM sometable) WHERE ora_rn>20 AND ora_rn<=30")

    def test_outer_join(self):
        table1 = table('mytable',
            column('myid', Integer),
            column('name', String),
            column('description', String),
        )

        table2 = table(
            'myothertable',
            column('otherid', Integer),
            column('othername', String),
        )

        table3 = table(
            'thirdtable',
            column('userid', Integer),
            column('otherstuff', String),
        )

        query = select(
                [table1, table2],
                or_(
                    table1.c.name == 'fred',
                    table1.c.myid == 10,
                    table2.c.othername != 'jack',
                    "EXISTS (select yay from foo where boo = lar)"
                ),
                from_obj = [ outerjoin(table1, table2, table1.c.myid == table2.c.otherid) ]
                )
        self.assert_compile(query,
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername \
FROM mytable, myothertable WHERE mytable.myid = myothertable.otherid(+) AND \
(mytable.name = :mytable_name_1 OR mytable.myid = :mytable_myid_1 OR \
myothertable.othername != :myothertable_othername_1 OR EXISTS (select yay from foo where boo = lar))",
            dialect=oracle.OracleDialect(use_ansi = False))

        query = table1.outerjoin(table2, table1.c.myid==table2.c.otherid).outerjoin(table3, table3.c.userid==table2.c.otherid)
        self.assert_compile(query.select(), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable LEFT OUTER JOIN myothertable ON mytable.myid = myothertable.otherid LEFT OUTER JOIN thirdtable ON thirdtable.userid = myothertable.otherid")
        self.assert_compile(query.select(), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable, myothertable, thirdtable WHERE mytable.myid = myothertable.otherid(+) AND thirdtable.userid(+) = myothertable.otherid", dialect=oracle.dialect(use_ansi=False))

        query = table1.join(table2, table1.c.myid==table2.c.otherid).join(table3, table3.c.userid==table2.c.otherid)
        self.assert_compile(query.select(), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable, myothertable, thirdtable WHERE mytable.myid = myothertable.otherid AND thirdtable.userid = myothertable.otherid", dialect=oracle.dialect(use_ansi=False))

    def test_alias_outer_join(self):
        address_types = table('address_types',
                    column('id'),
                    column('name'),
                    )
        addresses = table('addresses',
                column('id'),
                column('user_id'),
                column('address_type_id'),
                column('email_address')
            )
        at_alias = address_types.alias()

        s = select([at_alias, addresses]).\
            select_from(addresses.outerjoin(at_alias, addresses.c.address_type_id==at_alias.c.id)).\
            where(addresses.c.user_id==7).\
            order_by(addresses.oid_column, address_types.oid_column)
        self.assert_compile(s, "SELECT address_types_1.id, address_types_1.name, addresses.id, addresses.user_id, "
            "addresses.address_type_id, addresses.email_address FROM addresses LEFT OUTER JOIN address_types address_types_1 "
            "ON addresses.address_type_id = address_types_1.id WHERE addresses.user_id = :addresses_user_id_1 ORDER BY addresses.rowid, "
            "address_types.rowid")

class TypesTest(SQLCompileTest):
    __only_on__ = 'oracle'

    def test_no_clobs_for_string_params(self):
        """test that simple string params get a DBAPI type of VARCHAR, not CLOB.
        this is to prevent setinputsizes from setting up cx_oracle.CLOBs on
        string-based bind params [ticket:793]."""

        class FakeDBAPI(object):
            def __getattr__(self, attr):
                return attr

        dialect = oracle.OracleDialect()
        dbapi = FakeDBAPI()

        b = bindparam("foo", "hello world!")
        assert b.type.dialect_impl(dialect).get_dbapi_type(dbapi) == 'STRING'

        b = bindparam("foo", u"hello world!")
        assert b.type.dialect_impl(dialect).get_dbapi_type(dbapi) == 'STRING'

    def test_reflect_raw(self):
        types_table = Table(
        'all_types', MetaData(testbase.db),
            Column('owner', String(30), primary_key=True),
            Column('type_name', String(30), primary_key=True),
            autoload=True,
            )
        [[row[k] for k in row.keys()] for row in types_table.select().execute().fetchall()]

    def test_longstring(self):
        metadata = MetaData(testbase.db)
        testbase.db.execute("""
        CREATE TABLE Z_TEST
        (
          ID        NUMERIC(22) PRIMARY KEY,
          ADD_USER  VARCHAR2(20)  NOT NULL
        )
        """)
        try:
            t = Table("z_test", metadata, autoload=True)
            t.insert().execute(id=1.0, add_user='foobar')
            assert t.select().execute().fetchall() == [(1, 'foobar')]
        finally:
            testbase.db.execute("DROP TABLE Z_TEST")

class SequenceTest(SQLCompileTest):
    def test_basic(self):
        seq = Sequence("my_seq_no_schema")
        dialect = oracle.OracleDialect()
        assert dialect.identifier_preparer.format_sequence(seq) == "my_seq_no_schema"

        seq = Sequence("my_seq", schema="some_schema")
        assert dialect.identifier_preparer.format_sequence(seq) == "some_schema.my_seq"

        seq = Sequence("My_Seq", schema="Some_Schema")
        assert dialect.identifier_preparer.format_sequence(seq) == '"Some_Schema"."My_Seq"'


if __name__ == '__main__':
    testbase.main()
