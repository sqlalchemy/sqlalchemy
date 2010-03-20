# coding: utf-8

from sqlalchemy.test.testing import eq_
from sqlalchemy import *
from sqlalchemy import types as sqltypes, exc
from sqlalchemy.sql import table, column
from sqlalchemy.test import *
from sqlalchemy.test.testing import eq_, assert_raises
from sqlalchemy.test.engines import testing_engine
from sqlalchemy.dialects.oracle import cx_oracle, base as oracle
from sqlalchemy.engine import default
from sqlalchemy.util import jython
from decimal import Decimal
import datetime
import os


class OutParamTest(TestBase, AssertsExecutionResults):
    __only_on__ = 'oracle+cx_oracle'

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
        result = testing.db.execute(text("begin foo(:x_in, :x_out, :y_out, :z_out); end;", 
                        bindparams=[
                                bindparam('x_in', Numeric), 
                                outparam('x_out', Integer), 
                                outparam('y_out', Numeric), 
                                outparam('z_out', String)]), 
                                x_in=5)
        eq_(
            result.out_parameters,
            {'x_out':10, 'y_out':75, 'z_out':None}
        )
        assert isinstance(result.out_parameters['x_out'], int)

    @classmethod
    def teardown_class(cls):
         testing.db.execute("DROP PROCEDURE foo")


class CompileTest(TestBase, AssertsCompiledSQL):
    __dialect__ = oracle.OracleDialect()

    def test_owner(self):
        meta  = MetaData()
        parent = Table('parent', meta, Column('id', Integer, primary_key=True), 
           Column('name', String(50)),
           schema='ed')
        child = Table('child', meta, Column('id', Integer, primary_key=True),
           Column('parent_id', Integer, ForeignKey('ed.parent.id')),
           schema = 'ed')

        self.assert_compile(
                    parent.join(child), 
                    "ed.parent JOIN ed.child ON ed.parent.id = ed.child.parent_id")

    def test_subquery(self):
        t = table('sometable', column('col1'), column('col2'))
        s = select([t])
        s = select([s.c.col1, s.c.col2])

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT "
                                "sometable.col1 AS col1, sometable.col2 "
                                "AS col2 FROM sometable)")

    def test_limit(self):
        t = table('sometable', column('col1'), column('col2'))

        s = select([t])
        c = s.compile(dialect=oracle.OracleDialect())
        assert t.c.col1 in set(c.result_map['col1'][1])
        
        s = select([t]).limit(10).offset(20)

        self.assert_compile(s, 
            "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
            "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
            "FROM sometable) WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1"
        )

        # assert that despite the subquery, the columns from the table,
        # not the select, get put into the "result_map"
        c = s.compile(dialect=oracle.OracleDialect())
        assert t.c.col1 in set(c.result_map['col1'][1])
        
        s = select([s.c.col1, s.c.col2])

        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT col1, col2 FROM "
                        "(SELECT col1, col2, ROWNUM AS ora_rn FROM (SELECT "
                        "sometable.col1 AS col1, sometable.col2 AS col2 FROM "
                        "sometable) WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1)")

        # testing this twice to ensure oracle doesn't modify the original statement
        self.assert_compile(s, "SELECT col1, col2 FROM (SELECT col1, col2 FROM "
                        "(SELECT col1, col2, ROWNUM AS ora_rn FROM (SELECT "
                        "sometable.col1 AS col1, sometable.col2 AS col2 FROM sometable) "
                        "WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1)")

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
            "other_thirty_characters_table_.thirty_characters_table_id AS "
            "other_thirty_characters__2, "
            "thirty_characters_table__1.id AS thirty_characters_table__3 FROM "
            "other_thirty_characters_table_ "
            "LEFT OUTER JOIN thirty_characters_table_xxxxxx AS thirty_characters_table__1 "
            "ON thirty_characters_table__1.id = "
            "other_thirty_characters_table_.thirty_characters_table_id",
            dialect=dialect
        )
        self.assert_compile(
        
            select([other_table, anon]).select_from(
                other_table.outerjoin(anon)
            ).apply_labels(),
            "SELECT other_thirty_characters_table_.id AS other_thirty_characters__1, "
            "other_thirty_characters_table_.thirty_characters_table_id AS "
            "other_thirty_characters__2, "
            "thirty_characters_table__1.id AS thirty_characters_table__3 FROM "
            "other_thirty_characters_table_ "
            "LEFT OUTER JOIN thirty_characters_table_xxxxxx thirty_characters_table__1 ON "
            "thirty_characters_table__1.id = "
            "other_thirty_characters_table_.thirty_characters_table_id",
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
            "SELECT mytable.myid, mytable.name, mytable.description, myothertable.otherid, "
                "myothertable.othername FROM mytable, myothertable WHERE "
                "(mytable.name = :name_1 OR mytable.myid = :myid_1 OR "
                "myothertable.othername != :othername_1 OR EXISTS (select yay "
                "from foo where boo = lar)) "
                "AND mytable.myid = myothertable.otherid(+)",
            dialect=oracle.OracleDialect(use_ansi = False))

        query = table1.outerjoin(table2, table1.c.myid==table2.c.otherid).\
                            outerjoin(table3, table3.c.userid==table2.c.otherid)
        self.assert_compile(query.select(), 
                    "SELECT mytable.myid, mytable.name, mytable.description, "
                    "myothertable.otherid, myothertable.othername, thirdtable.userid,"
                    " thirdtable.otherstuff "
                    "FROM mytable LEFT OUTER JOIN myothertable ON mytable.myid ="
                    " myothertable.otherid LEFT OUTER "
                    "JOIN thirdtable ON thirdtable.userid = myothertable.otherid")

        self.assert_compile(query.select(), 
                    "SELECT mytable.myid, mytable.name, mytable.description, "
                    "myothertable.otherid, myothertable.othername, thirdtable.userid,"
                    " thirdtable.otherstuff FROM "
                    "mytable, myothertable, thirdtable WHERE thirdtable.userid(+) ="
                    " myothertable.otherid AND "
                    "mytable.myid = myothertable.otherid(+)",
                     dialect=oracle.dialect(use_ansi=False))

        query = table1.join(table2, table1.c.myid==table2.c.otherid).\
                    join(table3, table3.c.userid==table2.c.otherid)
        self.assert_compile(query.select(), 
            "SELECT mytable.myid, mytable.name, mytable.description, "
            "myothertable.otherid, myothertable.othername, thirdtable.userid, "
            "thirdtable.otherstuff FROM "
            "mytable, myothertable, thirdtable WHERE thirdtable.userid = "
            "myothertable.otherid AND "
            "mytable.myid = myothertable.otherid", dialect=oracle.dialect(use_ansi=False))

        query = table1.join(table2, table1.c.myid==table2.c.otherid).\
                    outerjoin(table3, table3.c.userid==table2.c.otherid)

        self.assert_compile(query.select().order_by(table1.c.name).limit(10).offset(5), 
        
            "SELECT myid, name, description, otherid, othername, userid, "
            "otherstuff FROM (SELECT myid, name, description, "
            "otherid, othername, userid, otherstuff, "
            "ROWNUM AS ora_rn FROM (SELECT "
            "mytable.myid AS myid, mytable.name AS name, "
            "mytable.description AS description, "
            "myothertable.otherid AS otherid, myothertable.othername "
            "AS othername, "
            "thirdtable.userid AS userid, thirdtable.otherstuff AS "
            "otherstuff FROM mytable, "
            "myothertable, thirdtable WHERE thirdtable.userid(+) = "
            "myothertable.otherid AND "
            "mytable.myid = myothertable.otherid ORDER BY "
            "mytable.name) WHERE "
            "ROWNUM <= :ROWNUM_1) WHERE ora_rn > :ora_rn_1", 
            dialect=oracle.dialect(use_ansi=False))

        subq = select([table1]).\
                    select_from(
                            table1.outerjoin(table2, table1.c.myid==table2.c.otherid)
                        ).alias()
        q = select([table3]).select_from(
                            table3.outerjoin(subq, table3.c.userid==subq.c.myid)
                            )

        self.assert_compile(q, "SELECT thirdtable.userid, thirdtable.otherstuff "
                        "FROM thirdtable LEFT OUTER JOIN (SELECT mytable.myid AS "
                        "myid, mytable.name"
                        " AS name, mytable.description AS description "
                        "FROM mytable LEFT OUTER JOIN myothertable ON mytable.myid = "           
                        "myothertable.otherid) anon_1 ON thirdtable.userid = anon_1.myid",
                        dialect=oracle.dialect(use_ansi=True))
    
        self.assert_compile(q, "SELECT thirdtable.userid, thirdtable.otherstuff "
                        "FROM thirdtable, (SELECT mytable.myid AS myid, mytable.name AS name, "
                        "mytable.description AS description FROM mytable, myothertable "
                        "WHERE mytable.myid = myothertable.otherid(+)) anon_1 "
                        "WHERE thirdtable.userid = anon_1.myid(+)", 
                        dialect=oracle.dialect(use_ansi=False))
        
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
            select_from(
                        addresses.outerjoin(at_alias, 
                                    addresses.c.address_type_id==at_alias.c.id)
                        ).\
            where(addresses.c.user_id==7).\
            order_by(addresses.c.id, address_types.c.id)
        self.assert_compile(s, 
                "SELECT address_types_1.id, address_types_1.name, addresses.id, "
                "addresses.user_id, "
                "addresses.address_type_id, addresses.email_address FROM addresses "
                "LEFT OUTER JOIN address_types address_types_1 "
                "ON addresses.address_type_id = address_types_1.id WHERE "
                "addresses.user_id = :user_id_1 ORDER BY addresses.id, "
                "address_types.id")

    def test_compound(self):
        t1 = table('t1', column('c1'), column('c2'), column('c3'), )
        t2 = table('t2', column('c1'), column('c2'), column('c3'), )
        self.assert_compile(
            union(t1.select(), t2.select()),
            "SELECT t1.c1, t1.c2, t1.c3 FROM t1 UNION SELECT t2.c1, t2.c2, t2.c3 FROM t2"
        )
        self.assert_compile(
            except_(t1.select(), t2.select()),
            "SELECT t1.c1, t1.c2, t1.c3 FROM t1 MINUS SELECT t2.c1, t2.c2, t2.c3 FROM t2"
        )
        
class MultiSchemaTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'oracle'
    
    @classmethod
    def setup_class(cls):
        # currently assuming full DBA privs for the user.
        # don't really know how else to go here unless
        # we connect as the other user.
        
        for stmt in """
create table test_schema.parent(
    id integer primary key, 
    data varchar2(50)
);
                
create table test_schema.child(
    id integer primary key,
    data varchar2(50), 
    parent_id integer references test_schema.parent(id)
);

create synonym test_schema.ptable for test_schema.parent;
create synonym test_schema.ctable for test_schema.child;

-- can't make a ref from local schema to the 
-- remote schema's table without this, 
-- *and* cant give yourself a grant !  
-- so we give it to public.  ideas welcome. 
grant references on test_schema.parent to public;
grant references on test_schema.child to public;
""".split(";"):
            if stmt.strip():
                testing.db.execute(stmt)
        
    @classmethod
    def teardown_class(cls):
        for stmt in """
drop table test_schema.child;
drop table test_schema.parent;
drop synonym test_schema.ctable;
drop synonym test_schema.ptable;
""".split(";"):
            if stmt.strip():
                testing.db.execute(stmt)
        
    def test_create_same_names_explicit_schema(self):
        schema = testing.db.dialect.default_schema_name
        meta = MetaData(testing.db)
        parent = Table('parent', meta, 
            Column('pid', Integer, primary_key=True),
            schema=schema
        )
        child = Table('child', meta, 
            Column('cid', Integer, primary_key=True),
            Column('pid', Integer, ForeignKey('%s.parent.pid' % schema)),
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
        parent = Table('parent', meta, autoload=True, schema='test_schema')
        child = Table('child', meta, autoload=True, schema='test_schema')

        self.assert_compile(parent.join(child), 
                "test_schema.parent JOIN test_schema.child ON "
                "test_schema.parent.id = test_schema.child.parent_id")
        select([parent, child]).\
                select_from(parent.join(child)).\
                execute().fetchall()

    def test_reflect_local_to_remote(self):
        testing.db.execute("CREATE TABLE localtable "
                            "(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES"
                            " test_schema.parent(id))")
        try:
            meta = MetaData(testing.db)
            lcl = Table('localtable', meta, autoload=True)
            parent = meta.tables['test_schema.parent']
            self.assert_compile(parent.join(lcl), 
                    "test_schema.parent JOIN localtable ON "
                    "test_schema.parent.id = localtable.parent_id")
            select([parent, lcl]).\
                    select_from(parent.join(lcl)).\
                    execute().fetchall()
        finally:
            testing.db.execute("DROP TABLE localtable")

    def test_reflect_alt_owner_implicit(self):
        meta = MetaData(testing.db)
        parent = Table('parent', meta, autoload=True, schema='test_schema')
        child = Table('child', meta, autoload=True, schema='test_schema')

        self.assert_compile(parent.join(child), 
                    "test_schema.parent JOIN test_schema.child ON "
                    "test_schema.parent.id = test_schema.child.parent_id")
        select([parent, child]).select_from(parent.join(child)).execute().fetchall()
      
    def test_reflect_alt_owner_synonyms(self):
        testing.db.execute("CREATE TABLE localtable "
                            "(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES"
                            " test_schema.ptable(id))")
        try:
            meta = MetaData(testing.db)
            lcl = Table('localtable', meta, autoload=True, oracle_resolve_synonyms=True)
            parent = meta.tables['test_schema.ptable']
            self.assert_compile(parent.join(lcl), 
                    "test_schema.ptable JOIN localtable ON "
                    "test_schema.ptable.id = localtable.parent_id")
            select([parent, lcl]).select_from(parent.join(lcl)).execute().fetchall()
        finally:
            testing.db.execute("DROP TABLE localtable")
 
    def test_reflect_remote_synonyms(self):
        meta = MetaData(testing.db)
        parent = Table('ptable', meta, autoload=True, 
                            schema='test_schema', 
                            oracle_resolve_synonyms=True)
        child = Table('ctable', meta, autoload=True, 
                            schema='test_schema', 
                            oracle_resolve_synonyms=True)
        self.assert_compile(parent.join(child), 
                    "test_schema.ptable JOIN test_schema.ctable ON "
                    "test_schema.ptable.id = test_schema.ctable.parent_id")
        select([parent, child]).select_from(parent.join(child)).execute().fetchall()

class ConstraintTest(TestBase):
    __only_on__ = 'oracle'
    
    def setup(self):
        global metadata
        metadata = MetaData(testing.db)
        
        foo = Table('foo', metadata,
                Column('id', Integer, primary_key=True),
        )
        foo.create(checkfirst=True)
    
    def teardown(self):
        metadata.drop_all()

    def test_oracle_has_no_on_update_cascade(self):
        bar = Table('bar', metadata,
                Column('id', Integer, primary_key=True),
                Column('foo_id', Integer, ForeignKey('foo.id', onupdate="CASCADE"))
        )
        assert_raises(exc.SAWarning, bar.create)

        bat = Table('bat', metadata,
                Column('id', Integer, primary_key=True),
                Column('foo_id', Integer),
                ForeignKeyConstraint(['foo_id'], ['foo.id'], onupdate="CASCADE")
        )
        assert_raises(exc.SAWarning, bat.create)
        
        
class TypesTest(TestBase, AssertsCompiledSQL):
    __only_on__ = 'oracle'
    __dialect__ = oracle.OracleDialect()

    def test_no_clobs_for_string_params(self):
        """test that simple string params get a DBAPI type of 
        VARCHAR, not CLOB. This is to prevent setinputsizes 
        from setting up cx_oracle.CLOBs on
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
    
    @testing.fails_on('+zxjdbc', 'zxjdbc lacks the FIXED_CHAR dbapi type')
    def test_fixed_char(self):
        m = MetaData(testing.db)
        t = Table('t1', m, 
            Column('id', Integer, primary_key=True),
            Column('data', CHAR(30), nullable=False)
        )
        
        t.create()
        try:
            t.insert().execute(
                dict(id=1, data="value 1"),
                dict(id=2, data="value 2"),
                dict(id=3, data="value 3")
            )

            eq_(t.select().where(t.c.data=='value 2').execute().fetchall(), 
                [(2, 'value 2                       ')]
                )
                
            m2 = MetaData(testing.db)
            t2 = Table('t1', m2, autoload=True)
            assert type(t2.c.data.type) is CHAR
            eq_(t2.select().where(t2.c.data=='value 2').execute().fetchall(), 
                [(2, 'value 2                       ')]
                )
            
        finally:
            t.drop()
        
    def test_type_adapt(self):
        dialect = cx_oracle.dialect()

        for start, test in [
            (Date(), cx_oracle._OracleDate),
            (oracle.OracleRaw(), cx_oracle._OracleRaw),
            (String(), String),
            (VARCHAR(), cx_oracle._OracleString),
            (DATE(), DATE),
            (String(50), cx_oracle._OracleString),
            (Unicode(), cx_oracle._OracleNVarChar),
            (Text(), cx_oracle._OracleText),
            (UnicodeText(), cx_oracle._OracleUnicodeText),
            (NCHAR(), cx_oracle._OracleNVarChar),
            (oracle.RAW(50), cx_oracle._OracleRaw),
        ]:
            assert isinstance(start.dialect_impl(dialect), test), \
                    "wanted %r got %r" % (test, start.dialect_impl(dialect))

    @testing.requires.returning
    def test_int_not_float(self):
        m = MetaData(testing.db)
        t1 = Table('t1', m, Column('foo', Integer))
        t1.create()
        try:
            r = t1.insert().values(foo=5).returning(t1.c.foo).execute()
            x = r.scalar()
            assert x == 5
            assert isinstance(x, int)

            x = t1.select().scalar()
            assert x == 5
            assert isinstance(x, int)
        finally:
            t1.drop()
    
    @testing.fails_on('+zxjdbc', 
                    'Not yet known how to pass values of the INTERVAL type')
    def test_interval(self):

        for type_, expected in [
            (oracle.INTERVAL(), "INTERVAL DAY TO SECOND"),
            (
                oracle.INTERVAL(day_precision=3), 
                "INTERVAL DAY(3) TO SECOND"
            ),
            (
                oracle.INTERVAL(second_precision=5), 
                "INTERVAL DAY TO SECOND(5)"
            ),
            (
                oracle.INTERVAL(day_precision=2, second_precision=5), 
                "INTERVAL DAY(2) TO SECOND(5)"
            ),
        ]:
            self.assert_compile(type_, expected)
        
        metadata = MetaData(testing.db)
        interval_table = Table("intervaltable", metadata,
            Column("id", Integer, primary_key=True, test_needs_autoincrement=True),
            Column("day_interval", oracle.INTERVAL(day_precision=3)),
            )
        metadata.create_all()
        try:
            interval_table.insert().execute(
                day_interval=datetime.timedelta(days=35, seconds=5743),
            )
            row = interval_table.select().execute().first()
            eq_(row['day_interval'], datetime.timedelta(days=35, seconds=5743))
        finally:
            metadata.drop_all()
        
    def test_numerics(self):
        m = MetaData(testing.db)
        t1 = Table('t1', m, 
            Column('intcol', Integer),
            Column('numericcol', Numeric(precision=9, scale=2)),
            Column('floatcol1', Float()),
            Column('floatcol2', FLOAT()),
            Column('doubleprec', oracle.DOUBLE_PRECISION),
            Column('numbercol1', oracle.NUMBER(9)),
            Column('numbercol2', oracle.NUMBER(9, 3)),
            Column('numbercol3', oracle.NUMBER),
            
        )
        t1.create()
        try:
            t1.insert().execute(
                intcol=1, 
                numericcol=5.2, 
                floatcol1=6.5, 
                floatcol2 = 8.5,
                doubleprec = 9.5, 
                numbercol1=12,
                numbercol2=14.85,
                numbercol3=15.76
                )
            
            m2 = MetaData(testing.db)
            t2 = Table('t1', m2, autoload=True)

            for row in (
                t1.select().execute().first(),
                t2.select().execute().first() 
            ):
                for i, (val, type_) in enumerate((
                    (1, int),
                    (Decimal("5.2"), Decimal),
                    (6.5, float),
                    (8.5, float),
                    (9.5, float),
                    (12, int),
                    (Decimal("14.85"), Decimal),
                    (15.76, float),
                )):
                    eq_(row[i], val)
                    assert isinstance(row[i], type_)

        finally:
            t1.drop()
    
    def test_reflect_dates(self):
        metadata = MetaData(testing.db)
        Table(
            "date_types", metadata,
            Column('d1', DATE),
            Column('d2', TIMESTAMP),
            Column('d3', TIMESTAMP(timezone=True)),
            Column('d4', oracle.INTERVAL(second_precision=5)),
        )
        metadata.create_all()
        try:
            m = MetaData(testing.db)
            t1 = Table(
                "date_types", m,
                autoload=True)
            assert isinstance(t1.c.d1.type, DATE)
            assert isinstance(t1.c.d2.type, TIMESTAMP)
            assert not t1.c.d2.type.timezone
            assert isinstance(t1.c.d3.type, TIMESTAMP)
            assert t1.c.d3.type.timezone
            assert isinstance(t1.c.d4.type, oracle.INTERVAL)
            
        finally:
            metadata.drop_all()
        
    def test_reflect_raw(self):
        types_table = Table('all_types', MetaData(testing.db),
            Column('owner', String(30), primary_key=True),
            Column('type_name', String(30), primary_key=True),
            autoload=True, oracle_resolve_synonyms=True
            )
        for row in types_table.select().execute().fetchall():
            [row[k] for k in row.keys()]

    def test_reflect_nvarchar(self):
        metadata = MetaData(testing.db)
        t = Table('t', metadata,
            Column('data', sqltypes.NVARCHAR(255))
        )
        metadata.create_all()
        try:
            m2 = MetaData(testing.db)
            t2 = Table('t', m2, autoload=True)
            assert isinstance(t2.c.data.type, sqltypes.NVARCHAR)

            if testing.against('oracle+cx_oracle'):
                # nvarchar returns unicode natively.  cx_oracle
                # _OracleNVarChar type should be at play here.
                assert isinstance(
                    t2.c.data.type.dialect_impl(testing.db.dialect), 
                    cx_oracle._OracleNVarChar)

            data = u'm’a réveillé.'
            t2.insert().execute(data=data)
            res = t2.select().execute().first()['data']
            eq_(res, data)
            assert isinstance(res, unicode)
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

    @testing.fails_on('+zxjdbc', 'auto_convert_lobs not applicable')
    def test_raw_lobs(self):
        engine = testing_engine(options=dict(auto_convert_lobs=False))
        metadata = MetaData()
        t = Table("z_test", metadata, Column('id', Integer, primary_key=True), 
                 Column('data', Text), Column('bindata', LargeBinary))
        t.create(engine)
        try:
            engine.execute(t.insert(), id=1, 
                                        data='this is text', 
                                        bindata='this is binary')
            row = engine.execute(t.select()).first()
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
           Column('data', LargeBinary)
        )
        meta.create_all()
        stream = os.path.join(
                        os.path.dirname(__file__), "..", 
                        'binary_data_one.dat')
        stream = file(stream).read(12000)

        for i in range(1, 11):
            binary_table.insert().execute(id=i, data=stream)

    @classmethod
    def teardown_class(cls):
        meta.drop_all()

    def test_fetch(self):
        result = binary_table.select().execute().fetchall()
        eq_(result, [(i, stream) for i in range(1, 11)])

    @testing.fails_on('+zxjdbc', 'FIXME: zxjdbc should support this')
    def test_fetch_single_arraysize(self):
        eng = testing_engine(options={'arraysize':1})
        result = eng.execute(binary_table.select()).fetchall()
        eq_(result, [(i, stream) for i in range(1, 11)])

class UnsupportedIndexReflectTest(TestBase):
    __only_on__ = 'oracle'
    
    def setup(self):
        global metadata
        metadata = MetaData(testing.db)
        t1 = Table('test_index_reflect', metadata, 
                    Column('data', String(20), primary_key=True)
                )
        metadata.create_all()
    
    def teardown(self):
        metadata.drop_all()
        
    def test_reflect_functional_index(self):
        testing.db.execute("CREATE INDEX DATA_IDX ON TEST_INDEX_REFLECT (UPPER(DATA))")
        m2 = MetaData(testing.db)
        t2 = Table('test_index_reflect', m2, autoload=True)
        
        
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
        eq_(
            testing.db.execute("/*+ this is a comment */ SELECT 1 FROM DUAL").fetchall(),
            [(1,)]
        )

