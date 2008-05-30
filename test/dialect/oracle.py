import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.sql import table, column
from sqlalchemy.databases import oracle
from testlib import *
from testlib.engines import testing_engine
import os


class OutParamTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'oracle'

    def setUpAll(self):
        testing.db.execute("""
create or replace procedure foo(x_in IN number, x_out OUT number, y_out OUT number) IS
  retval number;
    begin
    retval := 6;
    x_out := 10;
    y_out := x_in * 15;
    end;
        """)

    def test_out_params(self):
        result = testing.db.execute(text("begin foo(:x, :y, :z); end;", bindparams=[bindparam('x', Numeric), outparam('y', Numeric), outparam('z', Numeric)]), x=5)
        assert result.out_parameters == {'y':10, 'z':75}, result.out_parameters
        print result.out_parameters

    def tearDownAll(self):
         testing.db.execute("DROP PROCEDURE foo")


class CompileTest(TestBase, AssertsCompiledSQL):
    __dialect__ = oracle.OracleDialect()

    def test_owner(self):
        meta  = MetaData()
        parent = Table('parent', meta, Column('id', Integer, primary_key=True), 
           Column('name', String(50)),
           owner='ed')
        child = Table('child', meta, Column('id', Integer, primary_key=True),
           Column('parent_id', Integer, ForeignKey('ed.parent.id')),
           owner = 'ed')

        self.assert_compile(parent.join(child), "ed.parent JOIN ed.child ON ed.parent.id = ed.child.parent_id")

    def test_subquery(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t])
        s = select([s.c.col1, s.c.col2])

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 FROM sometable)")

    def test_limit(self):
        t = table('sometable', column('col1'), column('col2'))

        s = select([t])
        c = s.compile(dialect=oracle.OracleDialect())
        assert t.c.col1 in set(c.result_map['col1'][1])
        
        s = select([t]).limit(10).offset(20)

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2, "
            "ROW_NUMBER() OVER (ORDER BY sometable.rowid) AS ora_rn FROM sometable) WHERE ora_rn>20 AND ora_rn<=30"
        )

        # assert that despite the subquery, the columns from the table,
        # not the select, get put into the "result_map"
        c = s.compile(dialect=oracle.OracleDialect())
        assert t.c.col1 in set(c.result_map['col1'][1])
        
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
FROM mytable, myothertable WHERE \
(mytable.name = :name_1 OR mytable.myid = :myid_1 OR \
myothertable.othername != :othername_1 OR EXISTS (select yay from foo where boo = lar)) \
AND mytable.myid = myothertable.otherid(+)",
            dialect=oracle.OracleDialect(use_ansi = False))

        query = table1.outerjoin(table2, table1.c.myid==table2.c.otherid).outerjoin(table3, table3.c.userid==table2.c.otherid)
        self.assert_compile(query.select(), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable LEFT OUTER JOIN myothertable ON mytable.myid = myothertable.otherid LEFT OUTER JOIN thirdtable ON thirdtable.userid = myothertable.otherid")
        self.assert_compile(query.select(), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable, myothertable, thirdtable WHERE mytable.myid = myothertable.otherid(+) AND thirdtable.userid(+) = myothertable.otherid", dialect=oracle.dialect(use_ansi=False))

        query = table1.join(table2, table1.c.myid==table2.c.otherid).join(table3, table3.c.userid==table2.c.otherid)
        self.assert_compile(query.select(), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable, myothertable, thirdtable WHERE mytable.myid = myothertable.otherid AND thirdtable.userid = myothertable.otherid", dialect=oracle.dialect(use_ansi=False))

        query = table1.join(table2, table1.c.myid==table2.c.otherid).outerjoin(table3, table3.c.userid==table2.c.otherid)
        self.assert_compile(query.select().order_by(table1.oid_column).limit(10).offset(5), "SELECT myid, name, description, otherid, othername, userid, \
otherstuff FROM (SELECT mytable.myid AS myid, mytable.name AS name, \
mytable.description AS description, myothertable.otherid AS otherid, \
myothertable.othername AS othername, thirdtable.userid AS userid, \
thirdtable.otherstuff AS otherstuff, ROW_NUMBER() OVER (ORDER BY mytable.rowid) AS ora_rn \
FROM mytable, myothertable, thirdtable WHERE mytable.myid = myothertable.otherid AND thirdtable.userid(+) = myothertable.otherid) \
WHERE ora_rn>5 AND ora_rn<=15", dialect=oracle.dialect(use_ansi=False))

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
            "ON addresses.address_type_id = address_types_1.id WHERE addresses.user_id = :user_id_1 ORDER BY addresses.rowid, "
            "address_types.rowid")

class MultiSchemaTest(TestBase, AssertsCompiledSQL):
    """instructions:

       1. create a user 'ed' in the oracle database.
       2. in 'ed', issue the following statements:
           create table parent(id integer primary key, data varchar2(50));
           create table child(id integer primary key, data varchar2(50), parent_id integer references parent(id));
           create synonym ptable for parent;
           create synonym ctable for child;
           grant all on parent to scott;  (or to whoever you run the oracle tests as)
           grant all on child to scott;  (same)
           grant all on ptable to scott;
           grant all on ctable to scott;

    """

    __only_on__ = 'oracle'

    def test_create_same_names_explicit_schema(self):
        schema = testing.db.dialect.get_default_schema_name(testing.db.connect())
        meta = MetaData(testing.db)
        parent = Table('parent', meta, 
            Column('pid', Integer, primary_key=True),
            schema=schema
        )
        child = Table('child', meta, 
            Column('cid', Integer, primary_key=True),
            Column('pid', Integer, ForeignKey('scott.parent.pid')),
            schema=schema
        )
        meta.create_all()
        try:
            parent.insert().execute({'pid':1})
            child.insert().execute({'cid':1, 'pid':1})
            self.assertEquals(child.select().execute().fetchall(), [(1, 1)])
        finally:
            meta.drop_all()

    def test_create_same_names_implicit_schema(self):
        meta = MetaData(testing.db)
        parent = Table('parent', meta, 
            Column('pid', Integer, primary_key=True),
        )
        child = Table('child', meta, 
            Column('cid', Integer, primary_key=True),
            Column('pid', Integer, ForeignKey('parent.pid')),
        )
        meta.create_all()
        try:
            parent.insert().execute({'pid':1})
            child.insert().execute({'cid':1, 'pid':1})
            self.assertEquals(child.select().execute().fetchall(), [(1, 1)])
        finally:
            meta.drop_all()


    def test_reflect_alt_owner_explicit(self):
        meta = MetaData(testing.db)
        parent = Table('parent', meta, autoload=True, schema='ed')
        child = Table('child', meta, autoload=True, schema='ed')

        self.assert_compile(parent.join(child), "ed.parent JOIN ed.child ON ed.parent.id = ed.child.parent_id")
        select([parent, child]).select_from(parent.join(child)).execute().fetchall()

    def test_reflect_local_to_remote(self):
        testing.db.execute("CREATE TABLE localtable (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES ed.parent(id))")
        try:
            meta = MetaData(testing.db)
            lcl = Table('localtable', meta, autoload=True)
            parent = meta.tables['ed.parent']
            self.assert_compile(parent.join(lcl), "ed.parent JOIN localtable ON ed.parent.id = localtable.parent_id")
            select([parent, lcl]).select_from(parent.join(lcl)).execute().fetchall()
        finally:
            testing.db.execute("DROP TABLE localtable")

    def test_reflect_alt_owner_implicit(self):
        meta = MetaData(testing.db)
        parent = Table('parent', meta, autoload=True, schema='ed')
        child = Table('child', meta, autoload=True, schema='ed')

        self.assert_compile(parent.join(child), "ed.parent JOIN ed.child ON ed.parent.id = ed.child.parent_id")
        select([parent, child]).select_from(parent.join(child)).execute().fetchall()
      
    def test_reflect_alt_owner_synonyms(self):
        testing.db.execute("CREATE TABLE localtable (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES ed.ptable(id))")
        try:
            meta = MetaData(testing.db)
            lcl = Table('localtable', meta, autoload=True, oracle_resolve_synonyms=True)
            parent = meta.tables['ed.ptable']
            self.assert_compile(parent.join(lcl), "ed.ptable JOIN localtable ON ed.ptable.id = localtable.parent_id")
            select([parent, lcl]).select_from(parent.join(lcl)).execute().fetchall()
        finally:
            testing.db.execute("DROP TABLE localtable")
 
    def test_reflect_remote_synonyms(self):
        meta = MetaData(testing.db)
        parent = Table('ptable', meta, autoload=True, schema='ed', oracle_resolve_synonyms=True)
        child = Table('ctable', meta, autoload=True, schema='ed', oracle_resolve_synonyms=True)
        self.assert_compile(parent.join(child), "ed.ptable JOIN ed.ctable ON ed.ptable.id = ed.ctable.parent_id")
        select([parent, child]).select_from(parent.join(child)).execute().fetchall()

 
class TypesTest(TestBase, AssertsCompiledSQL):
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
        'all_types', MetaData(testing.db),
            Column('owner', String(30), primary_key=True),
            Column('type_name', String(30), primary_key=True),
            autoload=True, oracle_resolve_synonyms=True
            )
        [[row[k] for k in row.keys()] for row in types_table.select().execute().fetchall()]

    def test_longstring(self):
        metadata = MetaData(testing.db)
        testing.db.execute("""
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
            testing.db.execute("DROP TABLE Z_TEST")

class BufferedColumnTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'oracle'

    def setUpAll(self):
        global binary_table, stream, meta
        meta = MetaData(testing.db)
        binary_table = Table('binary_table', meta, 
           Column('id', Integer, primary_key=True),
           Column('data', Binary)
        )
        meta.create_all()
        stream = os.path.join(os.path.dirname(testenv.__file__), 'binary_data_one.dat')
        stream = file(stream).read(12000)

        for i in range(1, 11):
            binary_table.insert().execute(id=i, data=stream)

    def tearDownAll(self):
        meta.drop_all()

    def test_fetch(self):
        self.assertEquals(
            binary_table.select().execute().fetchall() ,
            [(i, stream) for i in range(1, 11)], 
        )

    def test_fetch_single_arraysize(self):
        eng = testing_engine(options={'arraysize':1})
        self.assertEquals(
            eng.execute(binary_table.select()).fetchall(),
            [(i, stream) for i in range(1, 11)], 
        )

class SequenceTest(TestBase, AssertsCompiledSQL):
    def test_basic(self):
        seq = Sequence("my_seq_no_schema")
        dialect = oracle.OracleDialect()
        assert dialect.identifier_preparer.format_sequence(seq) == "my_seq_no_schema"

        seq = Sequence("my_seq", schema="some_schema")
        assert dialect.identifier_preparer.format_sequence(seq) == "some_schema.my_seq"

        seq = Sequence("My_Seq", schema="Some_Schema")
        assert dialect.identifier_preparer.format_sequence(seq) == '"Some_Schema"."My_Seq"'


if __name__ == '__main__':
    testenv.main()
