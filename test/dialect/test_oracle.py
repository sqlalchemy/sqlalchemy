# coding: utf-8

from test.lib.testing import eq_
from sqlalchemy import *
from sqlalchemy import types as sqltypes, exc
from sqlalchemy.sql import table, column
from test.lib import *
from test.lib.testing import eq_, assert_raises, assert_raises_message
from test.lib.engines import testing_engine
from sqlalchemy.dialects.oracle import cx_oracle, base as oracle
from sqlalchemy.engine import default
from sqlalchemy.util import jython
from sqlalchemy.util.compat import decimal
import datetime
import os


class OutParamTest(fixtures.TestBase, AssertsExecutionResults):
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
        result = \
            testing.db.execute(text('begin foo(:x_in, :x_out, :y_out, '
                               ':z_out); end;',
                               bindparams=[bindparam('x_in', Float),
                               outparam('x_out', Integer),
                               outparam('y_out', Float),
                               outparam('z_out', String)]), x_in=5)
        eq_(result.out_parameters, {'x_out': 10, 'y_out': 75, 'z_out'
            : None})
        assert isinstance(result.out_parameters['x_out'], int)

    @classmethod
    def teardown_class(cls):
         testing.db.execute("DROP PROCEDURE foo")


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = oracle.OracleDialect()

    def test_owner(self):
        meta = MetaData()
        parent = Table('parent', meta, Column('id', Integer,
                       primary_key=True), Column('name', String(50)),
                       schema='ed')
        child = Table('child', meta, Column('id', Integer,
                      primary_key=True), Column('parent_id', Integer,
                      ForeignKey('ed.parent.id')), schema='ed')
        self.assert_compile(parent.join(child),
                            'ed.parent JOIN ed.child ON ed.parent.id = '
                            'ed.child.parent_id')

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
                            'SELECT col1, col2 FROM (SELECT col1, '
                            'col2, ROWNUM AS ora_rn FROM (SELECT '
                            'sometable.col1 AS col1, sometable.col2 AS '
                            'col2 FROM sometable) WHERE ROWNUM <= '
                            ':ROWNUM_1) WHERE ora_rn > :ora_rn_1')

        c = s.compile(dialect=oracle.OracleDialect())
        assert t.c.col1 in set(c.result_map['col1'][1])
        s = select([s.c.col1, s.c.col2])
        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT col1, col2 '
                            'FROM (SELECT col1, col2, ROWNUM AS ora_rn '
                            'FROM (SELECT sometable.col1 AS col1, '
                            'sometable.col2 AS col2 FROM sometable) '
                            'WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > '
                            ':ora_rn_1)')

        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT col1, col2 '
                            'FROM (SELECT col1, col2, ROWNUM AS ora_rn '
                            'FROM (SELECT sometable.col1 AS col1, '
                            'sometable.col2 AS col2 FROM sometable) '
                            'WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > '
                            ':ora_rn_1)')

        s = select([t]).limit(10).offset(20).order_by(t.c.col2)
        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT col1, '
                            'col2, ROWNUM AS ora_rn FROM (SELECT '
                            'sometable.col1 AS col1, sometable.col2 AS '
                            'col2 FROM sometable ORDER BY '
                            'sometable.col2) WHERE ROWNUM <= '
                            ':ROWNUM_1) WHERE ora_rn > :ora_rn_1')
        s = select([t], for_update=True).limit(10).order_by(t.c.col2)
        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT '
                            'sometable.col1 AS col1, sometable.col2 AS '
                            'col2 FROM sometable ORDER BY '
                            'sometable.col2) WHERE ROWNUM <= :ROWNUM_1 '
                            'FOR UPDATE')

        s = select([t],
                   for_update=True).limit(10).offset(20).order_by(t.c.col2)
        self.assert_compile(s,
                            'SELECT col1, col2 FROM (SELECT col1, '
                            'col2, ROWNUM AS ora_rn FROM (SELECT '
                            'sometable.col1 AS col1, sometable.col2 AS '
                            'col2 FROM sometable ORDER BY '
                            'sometable.col2) WHERE ROWNUM <= '
                            ':ROWNUM_1) WHERE ora_rn > :ora_rn_1 FOR '
                            'UPDATE')

    def test_use_binds_for_limits_disabled(self):
        t = table('sometable', column('col1'), column('col2'))
        dialect = oracle.OracleDialect(use_binds_for_limits = False)

        self.assert_compile(select([t]).limit(10),
                "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, "
                "sometable.col2 AS col2 FROM sometable) WHERE ROWNUM <= 10",
                dialect=dialect)

        self.assert_compile(select([t]).offset(10),
                "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
                "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
                "FROM sometable)) WHERE ora_rn > 10",
                dialect=dialect)

        self.assert_compile(select([t]).limit(10).offset(10),
                "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
                "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
                "FROM sometable) WHERE ROWNUM <= 20) WHERE ora_rn > 10",
                dialect=dialect)

    def test_use_binds_for_limits_enabled(self):
        t = table('sometable', column('col1'), column('col2'))
        dialect = oracle.OracleDialect(use_binds_for_limits = True)

        self.assert_compile(select([t]).limit(10),
                "SELECT col1, col2 FROM (SELECT sometable.col1 AS col1, "
                "sometable.col2 AS col2 FROM sometable) WHERE ROWNUM "
                "<= :ROWNUM_1",
                dialect=dialect)

        self.assert_compile(select([t]).offset(10),
                "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
                "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
                "FROM sometable)) WHERE ora_rn > :ora_rn_1",
                dialect=dialect)

        self.assert_compile(select([t]).limit(10).offset(10),
                "SELECT col1, col2 FROM (SELECT col1, col2, ROWNUM AS ora_rn "
                "FROM (SELECT sometable.col1 AS col1, sometable.col2 AS col2 "
                "FROM sometable) WHERE ROWNUM <= :ROWNUM_1) WHERE ora_rn > "
                ":ora_rn_1",
                dialect=dialect)

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
        self.assert_compile(select([other_table,
                            anon]).
                            select_from(
                                other_table.outerjoin(anon)).apply_labels(),
                            'SELECT other_thirty_characters_table_.id '
                            'AS other_thirty_characters__1, '
                            'other_thirty_characters_table_.thirty_char'
                            'acters_table_id AS other_thirty_characters'
                            '__2, thirty_characters_table__1.id AS '
                            'thirty_characters_table__3 FROM '
                            'other_thirty_characters_table_ LEFT OUTER '
                            'JOIN thirty_characters_table_xxxxxx AS '
                            'thirty_characters_table__1 ON '
                            'thirty_characters_table__1.id = '
                            'other_thirty_characters_table_.thirty_char'
                            'acters_table_id', dialect=dialect)
        self.assert_compile(select([other_table,
                            anon]).select_from(
                                other_table.outerjoin(anon)).apply_labels(),
                            'SELECT other_thirty_characters_table_.id '
                            'AS other_thirty_characters__1, '
                            'other_thirty_characters_table_.thirty_char'
                            'acters_table_id AS other_thirty_characters'
                            '__2, thirty_characters_table__1.id AS '
                            'thirty_characters_table__3 FROM '
                            'other_thirty_characters_table_ LEFT OUTER '
                            'JOIN thirty_characters_table_xxxxxx '
                            'thirty_characters_table__1 ON '
                            'thirty_characters_table__1.id = '
                            'other_thirty_characters_table_.thirty_char'
                            'acters_table_id', dialect=ora_dialect)

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

        query = select([table1, table2], or_(table1.c.name == 'fred',
                       table1.c.myid == 10, table2.c.othername != 'jack'
                       , 'EXISTS (select yay from foo where boo = lar)'
                       ), from_obj=[outerjoin(table1, table2,
                       table1.c.myid == table2.c.otherid)])
        self.assert_compile(query,
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, myothertable.otherid,'
                            ' myothertable.othername FROM mytable, '
                            'myothertable WHERE (mytable.name = '
                            ':name_1 OR mytable.myid = :myid_1 OR '
                            'myothertable.othername != :othername_1 OR '
                            'EXISTS (select yay from foo where boo = '
                            'lar)) AND mytable.myid = '
                            'myothertable.otherid(+)',
                            dialect=oracle.OracleDialect(use_ansi=False))
        query = table1.outerjoin(table2, table1.c.myid
                                 == table2.c.otherid).outerjoin(table3,
                table3.c.userid == table2.c.otherid)
        self.assert_compile(query.select(),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, myothertable.otherid,'
                            ' myothertable.othername, '
                            'thirdtable.userid, thirdtable.otherstuff '
                            'FROM mytable LEFT OUTER JOIN myothertable '
                            'ON mytable.myid = myothertable.otherid '
                            'LEFT OUTER JOIN thirdtable ON '
                            'thirdtable.userid = myothertable.otherid')

        self.assert_compile(query.select(),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, myothertable.otherid,'
                            ' myothertable.othername, '
                            'thirdtable.userid, thirdtable.otherstuff '
                            'FROM mytable, myothertable, thirdtable '
                            'WHERE thirdtable.userid(+) = '
                            'myothertable.otherid AND mytable.myid = '
                            'myothertable.otherid(+)',
                            dialect=oracle.dialect(use_ansi=False))
        query = table1.join(table2, table1.c.myid
                            == table2.c.otherid).join(table3,
                table3.c.userid == table2.c.otherid)
        self.assert_compile(query.select(),
                            'SELECT mytable.myid, mytable.name, '
                            'mytable.description, myothertable.otherid,'
                            ' myothertable.othername, '
                            'thirdtable.userid, thirdtable.otherstuff '
                            'FROM mytable, myothertable, thirdtable '
                            'WHERE thirdtable.userid = '
                            'myothertable.otherid AND mytable.myid = '
                            'myothertable.otherid',
                            dialect=oracle.dialect(use_ansi=False))
        query = table1.join(table2, table1.c.myid
                            == table2.c.otherid).outerjoin(table3,
                table3.c.userid == table2.c.otherid)
        self.assert_compile(query.select().order_by(table1.c.name).
                        limit(10).offset(5),
                            'SELECT myid, name, description, otherid, '
                            'othername, userid, otherstuff FROM '
                            '(SELECT myid, name, description, otherid, '
                            'othername, userid, otherstuff, ROWNUM AS '
                            'ora_rn FROM (SELECT mytable.myid AS myid, '
                            'mytable.name AS name, mytable.description '
                            'AS description, myothertable.otherid AS '
                            'otherid, myothertable.othername AS '
                            'othername, thirdtable.userid AS userid, '
                            'thirdtable.otherstuff AS otherstuff FROM '
                            'mytable, myothertable, thirdtable WHERE '
                            'thirdtable.userid(+) = '
                            'myothertable.otherid AND mytable.myid = '
                            'myothertable.otherid ORDER BY '
                            'mytable.name) WHERE ROWNUM <= :ROWNUM_1) '
                            'WHERE ora_rn > :ora_rn_1',
                            dialect=oracle.dialect(use_ansi=False))

        subq = select([table1]).select_from(table1.outerjoin(table2,
                table1.c.myid == table2.c.otherid)).alias()
        q = select([table3]).select_from(table3.outerjoin(subq,
                table3.c.userid == subq.c.myid))

        self.assert_compile(q,
                            'SELECT thirdtable.userid, '
                            'thirdtable.otherstuff FROM thirdtable '
                            'LEFT OUTER JOIN (SELECT mytable.myid AS '
                            'myid, mytable.name AS name, '
                            'mytable.description AS description FROM '
                            'mytable LEFT OUTER JOIN myothertable ON '
                            'mytable.myid = myothertable.otherid) '
                            'anon_1 ON thirdtable.userid = anon_1.myid'
                            , dialect=oracle.dialect(use_ansi=True))

        self.assert_compile(q,
                            'SELECT thirdtable.userid, '
                            'thirdtable.otherstuff FROM thirdtable, '
                            '(SELECT mytable.myid AS myid, '
                            'mytable.name AS name, mytable.description '
                            'AS description FROM mytable, myothertable '
                            'WHERE mytable.myid = myothertable.otherid('
                            '+)) anon_1 WHERE thirdtable.userid = '
                            'anon_1.myid(+)',
                            dialect=oracle.dialect(use_ansi=False))

        q = select([table1.c.name]).where(table1.c.name == 'foo')
        self.assert_compile(q,
                            'SELECT mytable.name FROM mytable WHERE '
                            'mytable.name = :name_1',
                            dialect=oracle.dialect(use_ansi=False))
        subq = select([table3.c.otherstuff]).where(table3.c.otherstuff
                == table1.c.name).label('bar')
        q = select([table1.c.name, subq])
        self.assert_compile(q,
                            'SELECT mytable.name, (SELECT '
                            'thirdtable.otherstuff FROM thirdtable '
                            'WHERE thirdtable.otherstuff = '
                            'mytable.name) AS bar FROM mytable',
                            dialect=oracle.dialect(use_ansi=False))


    def test_alias_outer_join(self):
        address_types = table('address_types', column('id'),
                              column('name'))
        addresses = table('addresses', column('id'), column('user_id'),
                          column('address_type_id'),
                          column('email_address'))
        at_alias = address_types.alias()
        s = select([at_alias,
                   addresses]).select_from(addresses.outerjoin(at_alias,
                addresses.c.address_type_id
                == at_alias.c.id)).where(addresses.c.user_id
                == 7).order_by(addresses.c.id, address_types.c.id)
        self.assert_compile(s,
                            'SELECT address_types_1.id, '
                            'address_types_1.name, addresses.id, '
                            'addresses.user_id, addresses.address_type_'
                            'id, addresses.email_address FROM '
                            'addresses LEFT OUTER JOIN address_types '
                            'address_types_1 ON addresses.address_type_'
                            'id = address_types_1.id WHERE '
                            'addresses.user_id = :user_id_1 ORDER BY '
                            'addresses.id, address_types.id')

    def test_compound(self):
        t1 = table('t1', column('c1'), column('c2'), column('c3'))
        t2 = table('t2', column('c1'), column('c2'), column('c3'))
        self.assert_compile(union(t1.select(), t2.select()),
                            'SELECT t1.c1, t1.c2, t1.c3 FROM t1 UNION '
                            'SELECT t2.c1, t2.c2, t2.c3 FROM t2')
        self.assert_compile(except_(t1.select(), t2.select()),
                            'SELECT t1.c1, t1.c2, t1.c3 FROM t1 MINUS '
                            'SELECT t2.c1, t2.c2, t2.c3 FROM t2')

    def test_no_paren_fns(self):
        for fn, expected in [
            (func.uid(), "uid"),
            (func.UID(), "UID"),
            (func.sysdate(), "sysdate"),
            (func.row_number(), "row_number()"),
            (func.rank(), "rank()"),
            (func.now(), "CURRENT_TIMESTAMP"),
            (func.current_timestamp(), "CURRENT_TIMESTAMP"),
            (func.user(), "USER"),
        ]:
            self.assert_compile(fn, expected)

class CompatFlagsTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = 'oracle'

    def test_ora8_flags(self):
        def server_version_info(self):
            return (8, 2, 5)

        dialect = oracle.dialect(dbapi=testing.db.dialect.dbapi)
        dialect._get_server_version_info = server_version_info

        # before connect, assume modern DB
        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi

        dialect.initialize(testing.db.connect())
        assert not dialect.implicit_returning
        assert not dialect._supports_char_length
        assert not dialect._supports_nchar
        assert not dialect.use_ansi
        self.assert_compile(String(50),"VARCHAR2(50)",dialect=dialect)
        self.assert_compile(Unicode(50),"VARCHAR2(50)",dialect=dialect)
        self.assert_compile(UnicodeText(),"CLOB",dialect=dialect)

        dialect = oracle.dialect(implicit_returning=True, 
                                    dbapi=testing.db.dialect.dbapi)
        dialect._get_server_version_info = server_version_info
        dialect.initialize(testing.db.connect())
        assert dialect.implicit_returning


    def test_default_flags(self):
        """test with no initialization or server version info"""

        dialect = oracle.dialect(dbapi=testing.db.dialect.dbapi)
        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi
        self.assert_compile(String(50),"VARCHAR2(50 CHAR)",dialect=dialect)
        self.assert_compile(Unicode(50),"NVARCHAR2(50)",dialect=dialect)
        self.assert_compile(UnicodeText(),"NCLOB",dialect=dialect)

    def test_ora10_flags(self):
        def server_version_info(self):
            return (10, 2, 5)
        dialect = oracle.dialect(dbapi=testing.db.dialect.dbapi)
        dialect._get_server_version_info = server_version_info
        dialect.initialize(testing.db.connect())
        assert dialect._supports_char_length
        assert dialect._supports_nchar
        assert dialect.use_ansi
        self.assert_compile(String(50),"VARCHAR2(50 CHAR)",dialect=dialect)
        self.assert_compile(Unicode(50),"NVARCHAR2(50)",dialect=dialect)
        self.assert_compile(UnicodeText(),"NCLOB",dialect=dialect)


class MultiSchemaTest(fixtures.TestBase, AssertsCompiledSQL):
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
        testing.db.execute('CREATE TABLE localtable (id INTEGER '
                           'PRIMARY KEY, parent_id INTEGER REFERENCES '
                           'test_schema.parent(id))')
        try:
            meta = MetaData(testing.db)
            lcl = Table('localtable', meta, autoload=True)
            parent = meta.tables['test_schema.parent']
            self.assert_compile(parent.join(lcl),
                                'test_schema.parent JOIN localtable ON '
                                'test_schema.parent.id = '
                                'localtable.parent_id')
            select([parent,
                   lcl]).select_from(parent.join(lcl)).execute().fetchall()
        finally:
            testing.db.execute('DROP TABLE localtable')

    def test_reflect_alt_owner_implicit(self):
        meta = MetaData(testing.db)
        parent = Table('parent', meta, autoload=True,
                       schema='test_schema')
        child = Table('child', meta, autoload=True, schema='test_schema'
                      )
        self.assert_compile(parent.join(child),
                            'test_schema.parent JOIN test_schema.child '
                            'ON test_schema.parent.id = '
                            'test_schema.child.parent_id')
        select([parent,
               child]).select_from(parent.join(child)).execute().fetchall()

    def test_reflect_alt_owner_synonyms(self):
        testing.db.execute('CREATE TABLE localtable (id INTEGER '
                           'PRIMARY KEY, parent_id INTEGER REFERENCES '
                           'test_schema.ptable(id))')
        try:
            meta = MetaData(testing.db)
            lcl = Table('localtable', meta, autoload=True,
                        oracle_resolve_synonyms=True)
            parent = meta.tables['test_schema.ptable']
            self.assert_compile(parent.join(lcl),
                                'test_schema.ptable JOIN localtable ON '
                                'test_schema.ptable.id = '
                                'localtable.parent_id')
            select([parent,
                   lcl]).select_from(parent.join(lcl)).execute().fetchall()
        finally:
            testing.db.execute('DROP TABLE localtable')

    def test_reflect_remote_synonyms(self):
        meta = MetaData(testing.db)
        parent = Table('ptable', meta, autoload=True,
                       schema='test_schema',
                       oracle_resolve_synonyms=True)
        child = Table('ctable', meta, autoload=True,
                      schema='test_schema',
                      oracle_resolve_synonyms=True)
        self.assert_compile(parent.join(child),
                            'test_schema.ptable JOIN '
                            'test_schema.ctable ON test_schema.ptable.i'
                            'd = test_schema.ctable.parent_id')
        select([parent,
               child]).select_from(parent.join(child)).execute().fetchall()

class ConstraintTest(fixtures.TestBase):

    __only_on__ = 'oracle'

    def setup(self):
        global metadata
        metadata = MetaData(testing.db)
        foo = Table('foo', metadata, Column('id', Integer,
                    primary_key=True))
        foo.create(checkfirst=True)

    def teardown(self):
        metadata.drop_all()

    def test_oracle_has_no_on_update_cascade(self):
        bar = Table('bar', metadata, Column('id', Integer,
                    primary_key=True), Column('foo_id', Integer,
                    ForeignKey('foo.id', onupdate='CASCADE')))
        assert_raises(exc.SAWarning, bar.create)
        bat = Table('bat', metadata, Column('id', Integer,
                    primary_key=True), Column('foo_id', Integer),
                    ForeignKeyConstraint(['foo_id'], ['foo.id'],
                    onupdate='CASCADE'))
        assert_raises(exc.SAWarning, bat.create)

class TypesTest(fixtures.TestBase, AssertsCompiledSQL):
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

    @testing.provide_metadata
    def test_rowid(self):
        metadata = self.metadata
        t = Table('t1', metadata,
            Column('x', Integer)
        )
        t.create()
        t.insert().execute(x=5)
        s1 = select([t])
        s2 = select([column('rowid')]).select_from(s1)
        rowid = s2.scalar()

        # the ROWID type is not really needed here,
        # as cx_oracle just treats it as a string,
        # but we want to make sure the ROWID works...
        rowid_col= column('rowid', oracle.ROWID)
        s3 = select([t.c.x, rowid_col]).\
                    where(rowid_col == cast(rowid, oracle.ROWID))
        eq_(s3.select().execute().fetchall(),
        [(5, rowid)]
        )

    @testing.fails_on('+zxjdbc',
                      'Not yet known how to pass values of the '
                      'INTERVAL type')
    def test_interval(self):
        for type_, expected in [(oracle.INTERVAL(),
                                'INTERVAL DAY TO SECOND'),
                                (oracle.INTERVAL(day_precision=3),
                                'INTERVAL DAY(3) TO SECOND'),
                                (oracle.INTERVAL(second_precision=5),
                                'INTERVAL DAY TO SECOND(5)'),
                                (oracle.INTERVAL(day_precision=2,
                                second_precision=5),
                                'INTERVAL DAY(2) TO SECOND(5)')]:
            self.assert_compile(type_, expected)
        metadata = MetaData(testing.db)
        interval_table = Table('intervaltable', metadata, Column('id',
                               Integer, primary_key=True,
                               test_needs_autoincrement=True),
                               Column('day_interval',
                               oracle.INTERVAL(day_precision=3)))
        metadata.create_all()
        try:
            interval_table.insert().\
                execute(day_interval=datetime.timedelta(days=35,
                    seconds=5743))
            row = interval_table.select().execute().first()
            eq_(row['day_interval'], datetime.timedelta(days=35,
                seconds=5743))
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
                    (decimal.Decimal("5.2"), decimal.Decimal),
                    (6.5, float),
                    (8.5, float),
                    (9.5, float),
                    (12, int),
                    (decimal.Decimal("14.85"), decimal.Decimal),
                    (15.76, float),
                )):
                    eq_(row[i], val)
                    assert isinstance(row[i], type_), '%r is not %r' \
                        % (row[i], type_)

        finally:
            t1.drop()

    @testing.provide_metadata
    def test_numerics_broken_inspection(self):
        """Numeric scenarios where Oracle type info is 'broken',
        returning us precision, scale of the form (0, 0) or (0, -127).
        We convert to Decimal and let int()/float() processors take over.

        """

        metadata = self.metadata

        # this test requires cx_oracle 5

        foo = Table('foo', metadata,
            Column('idata', Integer),
            Column('ndata', Numeric(20, 2)),
            Column('ndata2', Numeric(20, 2)),
            Column('nidata', Numeric(5, 0)),
            Column('fdata', Float()),
        )
        foo.create()

        foo.insert().execute(
            {'idata':5, 'ndata':decimal.Decimal("45.6"), 'ndata2':decimal.Decimal("45.0"), 
                    'nidata':decimal.Decimal('53'), 'fdata':45.68392},
        )

        stmt = """
        SELECT 
            idata,
            ndata,
            ndata2,
            nidata,
            fdata
        FROM foo
        """


        row = testing.db.execute(stmt).fetchall()[0]
        eq_([type(x) for x in row], [int, decimal.Decimal, decimal.Decimal, int, float])
        eq_(
            row, 
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'), 53, 45.683920000000001)
        )

        # with a nested subquery, 
        # both Numeric values that don't have decimal places, regardless
        # of their originating type, come back as ints with no useful
        # typing information beyond "numeric".  So native handler
        # must convert to int.
        # this means our Decimal converters need to run no matter what.
        # totally sucks.

        stmt = """
        SELECT 
            (SELECT (SELECT idata FROM foo) FROM DUAL) AS idata,
            (SELECT CAST((SELECT ndata FROM foo) AS NUMERIC(20, 2)) FROM DUAL)
             AS ndata,
             (SELECT CAST((SELECT ndata2 FROM foo) AS NUMERIC(20, 2)) FROM DUAL)
              AS ndata2,
             (SELECT CAST((SELECT nidata FROM foo) AS NUMERIC(5, 0)) FROM DUAL)
              AS nidata,
            (SELECT CAST((SELECT fdata FROM foo) AS FLOAT) FROM DUAL) AS fdata
        FROM dual
        """
        row = testing.db.execute(stmt).fetchall()[0]
        eq_([type(x) for x in row], [int, decimal.Decimal, int, int, decimal.Decimal])
        eq_(
            row, 
            (5, decimal.Decimal('45.6'), 45, 53, decimal.Decimal('45.68392'))
        )

        row = testing.db.execute(text(stmt, 
                                typemap={
                                        'idata':Integer(), 
                                        'ndata':Numeric(20, 2), 
                                        'ndata2':Numeric(20, 2), 
                                        'nidata':Numeric(5, 0),
                                        'fdata':Float()
                                })).fetchall()[0]
        eq_([type(x) for x in row], [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float])
        eq_(row, 
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'), decimal.Decimal('53'), 45.683920000000001)
        )

        stmt = """
        SELECT 
                anon_1.idata AS anon_1_idata,
                anon_1.ndata AS anon_1_ndata,
                anon_1.ndata2 AS anon_1_ndata2,
                anon_1.nidata AS anon_1_nidata,
                anon_1.fdata AS anon_1_fdata
        FROM (SELECT idata, ndata, ndata2, nidata, fdata
        FROM (
            SELECT 
                (SELECT (SELECT idata FROM foo) FROM DUAL) AS idata,
                (SELECT CAST((SELECT ndata FROM foo) AS NUMERIC(20, 2)) 
                FROM DUAL) AS ndata,
                (SELECT CAST((SELECT ndata2 FROM foo) AS NUMERIC(20, 2)) 
                FROM DUAL) AS ndata2,
                (SELECT CAST((SELECT nidata FROM foo) AS NUMERIC(5, 0)) 
                FROM DUAL) AS nidata,
                (SELECT CAST((SELECT fdata FROM foo) AS FLOAT) FROM DUAL) 
                AS fdata
            FROM dual
        )
        WHERE ROWNUM >= 0) anon_1
        """
        row =testing.db.execute(stmt).fetchall()[0]
        eq_([type(x) for x in row], [int, decimal.Decimal, int, int, decimal.Decimal])
        eq_(row, (5, decimal.Decimal('45.6'), 45, 53, decimal.Decimal('45.68392')))

        row = testing.db.execute(text(stmt, 
                                typemap={
                                        'anon_1_idata':Integer(), 
                                        'anon_1_ndata':Numeric(20, 2), 
                                        'anon_1_ndata2':Numeric(20, 2), 
                                        'anon_1_nidata':Numeric(5, 0), 
                                        'anon_1_fdata':Float()
                                })).fetchall()[0]
        eq_([type(x) for x in row], [int, decimal.Decimal, decimal.Decimal, decimal.Decimal, float])
        eq_(row, 
            (5, decimal.Decimal('45.6'), decimal.Decimal('45'), decimal.Decimal('53'), 45.683920000000001)
        )

        row = testing.db.execute(text(stmt, 
                                typemap={
                                        'anon_1_idata':Integer(), 
                                        'anon_1_ndata':Numeric(20, 2, asdecimal=False), 
                                        'anon_1_ndata2':Numeric(20, 2, asdecimal=False), 
                                        'anon_1_nidata':Numeric(5, 0, asdecimal=False), 
                                        'anon_1_fdata':Float(asdecimal=True)
                                })).fetchall()[0]
        eq_([type(x) for x in row], [int, float, float, float, decimal.Decimal])
        eq_(row, 
            (5, 45.6, 45, 53, decimal.Decimal('45.68392'))
        )


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

    def test_reflect_all_types_schema(self):
        types_table = Table('all_types', MetaData(testing.db),
            Column('owner', String(30), primary_key=True),
            Column('type_name', String(30), primary_key=True),
            autoload=True, oracle_resolve_synonyms=True
            )
        for row in types_table.select().execute().fetchall():
            [row[k] for k in row.keys()]

    def test_raw_compile(self):
        self.assert_compile(oracle.RAW(), "RAW")
        self.assert_compile(oracle.RAW(35), "RAW(35)")

    @testing.provide_metadata
    def test_raw_roundtrip(self):
        metadata = self.metadata
        raw_table = Table('raw', metadata,
            Column('id', Integer, primary_key=True),
            Column('data', oracle.RAW(35))
        )
        metadata.create_all()
        testing.db.execute(raw_table.insert(), id=1, data="ABCDEF")
        eq_(
            testing.db.execute(raw_table.select()).first(),
            (1, "ABCDEF")
        )

    @testing.provide_metadata
    def test_reflect_nvarchar(self):
        metadata = self.metadata
        t = Table('t', metadata,
            Column('data', sqltypes.NVARCHAR(255))
        )
        metadata.create_all()
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

    def test_char_length(self):
        self.assert_compile(VARCHAR(50),"VARCHAR(50 CHAR)")

        oracle8dialect = oracle.dialect()
        oracle8dialect.server_version_info = (8, 0)
        self.assert_compile(VARCHAR(50),"VARCHAR(50)",dialect=oracle8dialect)

        self.assert_compile(NVARCHAR(50),"NVARCHAR2(50)")
        self.assert_compile(CHAR(50),"CHAR(50)")

        metadata = MetaData(testing.db)
        t1 = Table('t1', metadata,
              Column("c1", VARCHAR(50)),
              Column("c2", NVARCHAR(250)),
              Column("c3", CHAR(200))
        )
        t1.create()
        try:
            m2 = MetaData(testing.db)
            t2 = Table('t1', m2, autoload=True)
            eq_(t2.c.c1.type.length, 50)
            eq_(t2.c.c2.type.length, 250)
            eq_(t2.c.c3.type.length, 200)
        finally:
            t1.drop()

    def test_varchar_types(self):
        dialect = oracle.dialect()
        for typ, exp in [
            (String(50), "VARCHAR2(50 CHAR)"),
            (Unicode(50), "NVARCHAR2(50)"),
            (NVARCHAR(50), "NVARCHAR2(50)"),
            (VARCHAR(50), "VARCHAR(50 CHAR)"),
            (oracle.NVARCHAR2(50), "NVARCHAR2(50)"),
            (oracle.VARCHAR2(50), "VARCHAR2(50 CHAR)"),
        ]:
            self.assert_compile(typ, exp, dialect=dialect)

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
    def test_lobs_without_convert(self):
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

class EuroNumericTest(fixtures.TestBase):
    """test the numeric output_type_handler when using non-US locale for NLS_LANG."""

    __only_on__ = 'oracle+cx_oracle'

    def setup(self):
        self.old_nls_lang = os.environ.get('NLS_LANG', False)
        os.environ['NLS_LANG'] = "GERMAN"
        self.engine = testing_engine()

    def teardown(self):
        if self.old_nls_lang is not False:
            os.environ['NLS_LANG'] = self.old_nls_lang
        else:
            del os.environ['NLS_LANG']
        self.engine.dispose()

    @testing.provide_metadata
    def test_output_type_handler(self):
        metadata = self.metadata
        for stmt, exp, kw in [
            ("SELECT 0.1 FROM DUAL", decimal.Decimal("0.1"), {}),
            ("SELECT 15 FROM DUAL", 15, {}),
            ("SELECT CAST(15 AS NUMERIC(3, 1)) FROM DUAL", decimal.Decimal("15"), {}),
            ("SELECT CAST(0.1 AS NUMERIC(5, 2)) FROM DUAL", decimal.Decimal("0.1"), {}),
            ("SELECT :num FROM DUAL", decimal.Decimal("2.5"), {'num':decimal.Decimal("2.5")})
        ]:
            test_exp = self.engine.scalar(stmt, **kw)
            eq_(
                test_exp,
                exp
            )
            assert type(test_exp) is type(exp)


class DontReflectIOTTest(fixtures.TestBase):
    """test that index overflow tables aren't included in
    table_names."""

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

class BufferedColumnTest(fixtures.TestBase, AssertsCompiledSQL):
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

class UnsupportedIndexReflectTest(fixtures.TestBase):
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
        testing.db.execute('CREATE INDEX DATA_IDX ON '
                           'TEST_INDEX_REFLECT (UPPER(DATA))')
        m2 = MetaData(testing.db)
        t2 = Table('test_index_reflect', m2, autoload=True)

class RoundTripIndexTest(fixtures.TestBase):
    __only_on__ = 'oracle'

    def test_basic(self):
        engine = testing.db
        metadata = MetaData(engine)

        table=Table("sometable", metadata,
            Column("id_a", Unicode(255), primary_key=True),
            Column("id_b", Unicode(255), primary_key=True, unique=True),
            Column("group", Unicode(255), primary_key=True),
            Column("col", Unicode(255)),
            UniqueConstraint('col','group'),
        )

        # "group" is a keyword, so lower case
        normalind = Index('tableind', table.c.id_b, table.c.group) 

        # create
        metadata.create_all()
        try:
            # round trip, create from reflection
            mirror = MetaData(engine)
            mirror.reflect()
            metadata.drop_all()
            mirror.create_all()

            # inspect the reflected creation
            inspect = MetaData(engine)
            inspect.reflect()

            def obj_definition(obj):
                return obj.__class__, tuple([c.name for c in
                        obj.columns]), getattr(obj, 'unique', None)

            # find what the primary k constraint name should be
            primaryconsname = engine.execute(
               text("""SELECT constraint_name
                   FROM all_constraints
                   WHERE table_name = :table_name
                   AND owner = :owner
                   AND constraint_type = 'P' """),
               table_name=table.name.upper(),
               owner=engine.url.username.upper()).fetchall()[0][0]

            reflectedtable = inspect.tables[table.name]

            # make a dictionary of the reflected objects:

            reflected = dict([(obj_definition(i), i) for i in
                             reflectedtable.indexes
                             | reflectedtable.constraints])

            # assert we got primary key constraint and its name, Error
            # if not in dict

            assert reflected[(PrimaryKeyConstraint, ('id_a', 'id_b',
                             'group'), None)].name.upper() \
                == primaryconsname.upper()

            # Error if not in dict

            assert reflected[(Index, ('id_b', 'group'), False)].name \
                == normalind.name
            assert (Index, ('id_b', ), True) in reflected
            assert (Index, ('col', 'group'), True) in reflected
            assert len(reflectedtable.constraints) == 1
            assert len(reflectedtable.indexes) == 3

        finally:
            metadata.drop_all()



class SequenceTest(fixtures.TestBase, AssertsCompiledSQL):

    def test_basic(self):
        seq = Sequence('my_seq_no_schema')
        dialect = oracle.OracleDialect()
        assert dialect.identifier_preparer.format_sequence(seq) \
            == 'my_seq_no_schema'
        seq = Sequence('my_seq', schema='some_schema')
        assert dialect.identifier_preparer.format_sequence(seq) \
            == 'some_schema.my_seq'
        seq = Sequence('My_Seq', schema='Some_Schema')
        assert dialect.identifier_preparer.format_sequence(seq) \
            == '"Some_Schema"."My_Seq"'


class ExecuteTest(fixtures.TestBase):

    __only_on__ = 'oracle'

    def test_basic(self):
        eq_(testing.db.execute('/*+ this is a comment */ SELECT 1 FROM '
            'DUAL').fetchall(), [(1, )])

    def test_sequences_are_integers(self):
        seq = Sequence('foo_seq')
        seq.create(testing.db)
        try:
            val = testing.db.execute(seq)
            eq_(val, 1)
            assert type(val) is int
        finally:
            seq.drop(testing.db)

    @testing.provide_metadata
    def test_limit_offset_for_update(self):
        metadata = self.metadata
        # oracle can't actually do the ROWNUM thing with FOR UPDATE
        # very well.

        t = Table('t1', metadata, Column('id', Integer, primary_key=True),
            Column('data', Integer)
        )
        metadata.create_all()

        t.insert().execute(
            {'id':1, 'data':1},
            {'id':2, 'data':7},
            {'id':3, 'data':12},
            {'id':4, 'data':15},
            {'id':5, 'data':32},
        )

        # here, we can't use ORDER BY.
        eq_(
            t.select(for_update=True).limit(2).execute().fetchall(),
            [(1, 1),
             (2, 7)]
        )

        # here, its impossible.  But we'd prefer it to raise ORA-02014
        # instead of issuing a syntax error.
        assert_raises_message(
            exc.DatabaseError,
            "ORA-02014",
            t.select(for_update=True).limit(2).offset(3).execute
        )


class UnicodeSchemaTest(fixtures.TestBase):
    __only_on__ = 'oracle'

    @testing.provide_metadata
    def test_quoted_column_non_unicode(self):
        metadata = self.metadata
        table=Table("atable", metadata,
            Column("_underscorecolumn", Unicode(255), primary_key=True),
        )
        metadata.create_all()

        table.insert().execute(
            {'_underscorecolumn': u'’é'},
        )
        result = testing.db.execute(
            table.select().where(table.c._underscorecolumn==u'’é')
        ).scalar()
        eq_(result, u'’é')

    @testing.provide_metadata
    def test_quoted_column_unicode(self):
        metadata = self.metadata
        table=Table("atable", metadata,
            Column(u"méil", Unicode(255), primary_key=True),
        )
        metadata.create_all()

        table.insert().execute(
            {u'méil': u'’é'},
        )
        result = testing.db.execute(
            table.select().where(table.c[u'méil']==u'’é')
        ).scalar()
        eq_(result, u'’é')


