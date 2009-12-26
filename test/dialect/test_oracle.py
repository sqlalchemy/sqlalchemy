# coding: utf-8

from sqlalchemy.test.testing import eq_
from sqlalchemy import *
from sqlalchemy.sql import table, column
from sqlalchemy.databases import oracle
from sqlalchemy.test import *
from sqlalchemy.test.testing import eq_
from sqlalchemy.test.engines import testing_engine
from sqlalchemy.engine import default
import os


class OutParamTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'oracle'

    @classmethod
    def setup_class(cls):
        testing.db.execute("""
create or replace procedure foo(x_in IN number, x_out OUT number, y_out OUT number, z_out OUT varchar) IS
  retval number;
    begin
    retval := 6;
    x_out := 10;
    y_out := x_in * 15;
    z_out := NULL;
    end;
        """)

    def test_out_params(self):
        result = testing.db.execute(text("begin foo(:x_in, :x_out, :y_out, :z_out); end;", bindparams=[bindparam('x_in', Numeric), outparam('x_out', Numeric), outparam('y_out', Numeric), outparam('z_out', String)]), x_in=5)
        assert result.out_parameters == {'x_out':10, 'y_out':75, 'z_out':None}, result.out_parameters

    @classmethod
    def teardown_class(cls):
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

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable) WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1"
        )

        # assert that despite the subquery, the columns from the table,
        # not the select, get put into the "result_map"
        c = s.compile(dialect=oracle.OracleDialect())
        assert t.c.col1 in set(c.result_map['col1'][1])
        
        s = select([s.c.col1, s.c.col2])

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 FROM sometable) WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1)")

        # testing this twice to ensure oracle doesn't modify the original statement
        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 FROM sometable) WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1)")

        s = select([t]).limit(10).offset(20).order_by(t.c.col2)

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM "
            "AS ora_rn FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 FROM sometable "
            "ORDER BY sometable.col2) WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1")
    
    def test_long_labels(self):
        dialect = default.DefaultDialect()
        dialect.max_identifier_length = 30
        
        ora_dialect = oracle.dialect()
        
        m = MetaData()
        a_table = Table(
            'thirty_characters_table_xxxxxx',
            m,
            Column('id', Integer, primary_key=True)
        )

        other_table = Table(
            'other_thirty_characters_table_',
            m,
            Column('id', Integer, primary_key=True),
            Column('thirty_characters_table_id',
                Integer,
                ForeignKey('thirty_characters_table_xxxxxx.id'),
                primary_key=True
            )
        )
        
        anon = a_table.alias()
        self.assert_compile(
        
            select([other_table, anon]).select_from(
                other_table.outerjoin(anon)
            ).apply_labels(),
            "SELECT other_thirty_characters_table_.id AS other_thirty_characters__1, "
            "other_thirty_characters_table_.thirty_characters_table_id AS other_thirty_characters__2, "
            "thirty_characters_table__1.id AS thirty_characters_table__3 FROM other_thirty_characters_table_ "
            "LEFT OUTER JOIN thirty_characters_table_xxxxxx AS thirty_characters_table__1 ON "
            "thirty_characters_table__1.id = other_thirty_characters_table_.thirty_characters_table_id",
            dialect=dialect
        )
        self.assert_compile(
        
            select([other_table, anon]).select_from(
                other_table.outerjoin(anon)
            ).apply_labels(),
            "SELECT other_thirty_characters_table_.id AS other_thirty_characters__1, "
            "other_thirty_characters_table_.thirty_characters_table_id AS other_thirty_characters__2, "
            "thirty_characters_table__1.id AS thirty_characters_table__3 FROM other_thirty_characters_table_ "
            "LEFT OUTER JOIN thirty_characters_table_xxxxxx thirty_characters_table__1 ON "
            "thirty_characters_table__1.id = other_thirty_characters_table_.thirty_characters_table_id",
            dialect=ora_dialect
        )
        
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
        self.assert_compile(query.select(), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable, myothertable, thirdtable WHERE thirdtable.userid(+) = myothertable.otherid AND mytable.myid = myothertable.otherid(+)", dialect=oracle.dialect(use_ansi=False))

        query = table1.join(table2, table1.c.myid==table2.c.otherid).join(table3, table3.c.userid==table2.c.otherid)
        self.assert_compile(query.select(), "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, myothertable.othername, thirdtable.userid, thirdtable.otherstuff FROM mytable, myothertable, thirdtable WHERE thirdtable.userid = myothertable.otherid AND mytable.myid = myothertable.otherid", dialect=oracle.dialect(use_ansi=False))

        query = table1.join(table2, table1.c.myid==table2.c.otherid).outerjoin(table3, table3.c.userid==table2.c.otherid)

        self.assert_compile(query.select().order_by(table1.c.name).limit(10).offset(5), 
        
            "SELECT myid, name, description, otherid, othername, userid, "
            "otherstuff FROM (SELECT myid, name, description, "
            "otherid, othername, userid, otherstuff, ROWNUM AS ora_rn FROM (SELECT "
            "mytable.myid AS myid, mytable.name AS name, mytable.description AS description, "
            "myothertable.otherid AS otherid, myothertable.othername AS othername, "
            "thirdtable.userid AS userid, thirdtable.otherstuff AS otherstuff FROM mytable, "
            "myothertable, thirdtable WHERE thirdtable.userid(+) = myothertable.otherid AND "
            "mytable.myid = myothertable.otherid ORDER BY mytable.name) WHERE "
            "ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1", dialect=oracle.dialect(use_ansi=False))

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
            order_by(addresses.c.id, address_types.c.id)
        self.assert_compile(s, "SELECT address_types_1.id, address_types_1.name, addresses.id, addresses.user_id, "
            "addresses.address_type_id, addresses.email_address FROM addresses LEFT OUTER JOIN address_types address_types_1 "
            "ON addresses.address_type_id = address_types_1.id WHERE addresses.user_id = :user_id_1 ORDER BY addresses.id, "
            "address_types.id")

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
            eq_(child.select().execute().fetchall(), [(1, 1)])
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
            eq_(child.select().execute().fetchall(), [(1, 1)])
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

    def test_reflect_nvarchar(self):
        metadata = MetaData(testing.db)
        t = Table('t', metadata,
            Column('data', oracle.OracleNVarchar(255))
        )
        metadata.create_all()
        try:
            m2 = MetaData(testing.db)
            t2 = Table('t', m2, autoload=True)
            assert isinstance(t2.c.data.type, oracle.OracleNVarchar)
            data = u'm’a réveillé.'
            t2.insert().execute(data=data)
            eq_(t2.select().execute().fetchone()['data'], data)
        finally:
            metadata.drop_all()
        
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

    def test_raw_lobs(self):
        engine = testing_engine(options=dict(auto_convert_lobs=False))
        metadata = MetaData()
        t = Table("z_test", metadata, Column('id', Integer, primary_key=True), 
                 Column('data', Text), Column('bindata', Binary))
        t.create(engine)
        try:
            engine.execute(t.insert(), id=1, data='this is text', bindata='this is binary')
            row = engine.execute(t.select()).fetchone()
            eq_(row['data'].read(), 'this is text')
            eq_(row['bindata'].read(), 'this is binary')
        finally:
            t.drop(engine)
            
class DontReflectIOTTest(TestBase):
    """test that index overflow tables aren't included in table_names."""
   
    __only_on__ = 'oracle'
 
    def setup(self):
        testing.db.execute("""
        CREATE TABLE admin_docindex(
                token char(20), 
                doc_id NUMBER,
                token_frequency NUMBER,
                token_offsets VARCHAR2(2000),
                CONSTRAINT pk_admin_docindex PRIMARY KEY (token, doc_id))
            ORGANIZATION INDEX 
            TABLESPACE users
            PCTTHRESHOLD 20
            OVERFLOW TABLESPACE users
        """)
    
    def teardown(self):
        testing.db.execute("drop table admin_docindex")
    
    def test_reflect_all(self):
        m = MetaData(testing.db)
        m.reflect()
        eq_(
            set(t.name for t in m.tables.values()),
            set(['admin_docindex'])
        )
        
class BufferedColumnTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'oracle'

    @classmethod
    def setup_class(cls):
        global binary_table, stream, meta
        meta = MetaData(testing.db)
        binary_table = Table('binary_table', meta, 
           Column('id', Integer, primary_key=True),
           Column('data', Binary)
        )
        meta.create_all()
        
        stream = os.path.join(os.path.dirname(__file__), "..", 'binary_data_one.dat')
        stream = file(stream).read(12000)

        for i in range(1, 11):
            binary_table.insert().execute(id=i, data=stream)

    @classmethod
    def teardown_class(cls):
        meta.drop_all()

    def test_fetch(self):
        eq_(
            binary_table.select().execute().fetchall() ,
            [(i, stream) for i in range(1, 11)], 
        )

    def test_fetch_single_arraysize(self):
        eng = testing_engine(options={'arraysize':1})
        eq_(
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

class ExecuteTest(TestBase):
    __only_on__ = 'oracle'
    def test_basic(self):
        assert testing.db.execute("/*+ this is a comment */ SELECT 1 FROM DUAL").fetchall() == [(1,)]

