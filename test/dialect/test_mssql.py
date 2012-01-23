# -*- encoding: utf-8
from test.lib.testing import eq_
import datetime
import os
import re
import warnings
from sqlalchemy import *
from sqlalchemy import types, exc, schema, event
from sqlalchemy.orm import *
from sqlalchemy.sql import table, column
from sqlalchemy.databases import mssql
from sqlalchemy.dialects.mssql import pyodbc, mxodbc, pymssql
from sqlalchemy.dialects.mssql.base import TIME
from sqlalchemy.engine import url
from test.lib import *
from test.lib.testing import eq_, emits_warning_on, \
    assert_raises_message
from sqlalchemy.util.compat import decimal
from sqlalchemy.engine.reflection import Inspector

class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mssql.dialect()

    def test_select(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.select(),
                            'SELECT sometable.somecolumn FROM sometable')

    def test_select_with_nolock(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.select().with_hint(t, 'WITH (NOLOCK)'),
                            'SELECT sometable.somecolumn FROM sometable WITH (NOLOCK)')

    def test_join_with_hint (self):
        t1 = table('t1',
            column('a', Integer),
            column('b', String),
            column('c', String),
        )
        t2 = table('t2',
            column("a", Integer),
            column("b", Integer),
            column("c", Integer),
        )
        join = t1.join(t2, t1.c.a==t2.c.a).\
                        select().with_hint(t1, 'WITH (NOLOCK)')
        self.assert_compile(
            join,
            'SELECT t1.a, t1.b, t1.c, t2.a, t2.b, t2.c '
            'FROM t1 WITH (NOLOCK) JOIN t2 ON t1.a = t2.a'
        )

    def test_insert(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.insert(),
                            'INSERT INTO sometable (somecolumn) VALUES '
                            '(:somecolumn)')

    def test_update(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.update(t.c.somecolumn == 7),
                            'UPDATE sometable SET somecolumn=:somecolum'
                            'n WHERE sometable.somecolumn = '
                            ':somecolumn_1', dict(somecolumn=10))

    # TODO: should this be for *all* MS-SQL dialects ?
    def test_mxodbc_binds(self):
        """mxodbc uses MS-SQL native binds, which aren't allowed in
        various places."""

        mxodbc_dialect = mxodbc.dialect()
        t = table('sometable', column('foo'))

        for expr, compile in [
            (
                select([literal("x"), literal("y")]), 
                "SELECT 'x' AS anon_1, 'y' AS anon_2",
            ),
            (
                select([t]).where(t.c.foo.in_(['x', 'y', 'z'])),
                "SELECT sometable.foo FROM sometable WHERE sometable.foo "
                "IN ('x', 'y', 'z')",
            ),
            (
                func.foobar("x", "y", 4, 5),
                "foobar('x', 'y', 4, 5)",
            ),
            (
                select([t]).where(func.len('xyz') > func.len(t.c.foo)),
                "SELECT sometable.foo FROM sometable WHERE len('xyz') > "
                "len(sometable.foo)",
            )
        ]:
            self.assert_compile(expr, compile, dialect=mxodbc_dialect)

    @testing.uses_deprecated
    def test_in_with_subqueries(self):
        """Test that when using subqueries in a binary expression
        the == and != are changed to IN and NOT IN respectively.

        """

        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.select().where(t.c.somecolumn
                            == t.select()),
                            'SELECT sometable.somecolumn FROM '
                            'sometable WHERE sometable.somecolumn IN '
                            '(SELECT sometable.somecolumn FROM '
                            'sometable)')
        self.assert_compile(t.select().where(t.c.somecolumn
                            != t.select()),
                            'SELECT sometable.somecolumn FROM '
                            'sometable WHERE sometable.somecolumn NOT '
                            'IN (SELECT sometable.somecolumn FROM '
                            'sometable)')

    def test_count(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.count(),
                            'SELECT count(sometable.somecolumn) AS '
                            'tbl_row_count FROM sometable')

    def test_noorderby_insubquery(self):
        """test that the ms-sql dialect removes ORDER BY clauses from
        subqueries"""

        table1 = table('mytable',
            column('myid', Integer),
            column('name', String),
            column('description', String),
        )

        q = select([table1.c.myid],
                   order_by=[table1.c.myid]).alias('foo')
        crit = q.c.myid == table1.c.myid
        self.assert_compile(select(['*'], crit),
                            "SELECT * FROM (SELECT mytable.myid AS "
                            "myid FROM mytable) AS foo, mytable WHERE "
                            "foo.myid = mytable.myid")



    def test_delete_schema(self):
        metadata = MetaData()
        tbl = Table('test', metadata, Column('id', Integer,
                    primary_key=True), schema='paj')
        self.assert_compile(tbl.delete(tbl.c.id == 1),
                            'DELETE FROM paj.test WHERE paj.test.id = '
                            ':id_1')
        s = select([tbl.c.id]).where(tbl.c.id == 1)
        self.assert_compile(tbl.delete().where(tbl.c.id.in_(s)),
                            'DELETE FROM paj.test WHERE paj.test.id IN '
                            '(SELECT test_1.id FROM paj.test AS test_1 '
                            'WHERE test_1.id = :id_1)')

    def test_delete_schema_multipart(self):
        metadata = MetaData()
        tbl = Table('test', metadata, Column('id', Integer,
                    primary_key=True), schema='banana.paj')
        self.assert_compile(tbl.delete(tbl.c.id == 1),
                            'DELETE FROM banana.paj.test WHERE '
                            'banana.paj.test.id = :id_1')
        s = select([tbl.c.id]).where(tbl.c.id == 1)
        self.assert_compile(tbl.delete().where(tbl.c.id.in_(s)),
                            'DELETE FROM banana.paj.test WHERE '
                            'banana.paj.test.id IN (SELECT test_1.id '
                            'FROM banana.paj.test AS test_1 WHERE '
                            'test_1.id = :id_1)')

    def test_delete_schema_multipart_needs_quoting(self):
        metadata = MetaData()
        tbl = Table('test', metadata, Column('id', Integer,
                    primary_key=True), schema='banana split.paj')
        self.assert_compile(tbl.delete(tbl.c.id == 1),
                            'DELETE FROM [banana split].paj.test WHERE '
                            '[banana split].paj.test.id = :id_1')
        s = select([tbl.c.id]).where(tbl.c.id == 1)
        self.assert_compile(tbl.delete().where(tbl.c.id.in_(s)),
                            'DELETE FROM [banana split].paj.test WHERE '
                            '[banana split].paj.test.id IN (SELECT '
                            'test_1.id FROM [banana split].paj.test AS '
                            'test_1 WHERE test_1.id = :id_1)')

    def test_delete_schema_multipart_both_need_quoting(self):
        metadata = MetaData()
        tbl = Table('test', metadata, Column('id', Integer,
                    primary_key=True),
                    schema='banana split.paj with a space')
        self.assert_compile(tbl.delete(tbl.c.id == 1),
                            'DELETE FROM [banana split].[paj with a '
                            'space].test WHERE [banana split].[paj '
                            'with a space].test.id = :id_1')
        s = select([tbl.c.id]).where(tbl.c.id == 1)
        self.assert_compile(tbl.delete().where(tbl.c.id.in_(s)),
                            'DELETE FROM [banana split].[paj with a '
                            'space].test WHERE [banana split].[paj '
                            'with a space].test.id IN (SELECT '
                            'test_1.id FROM [banana split].[paj with a '
                            'space].test AS test_1 WHERE test_1.id = '
                            ':id_1)')

    def test_union(self):
        t1 = table('t1', column('col1'), column('col2'), column('col3'
                   ), column('col4'))
        t2 = table('t2', column('col1'), column('col2'), column('col3'
                   ), column('col4'))
        s1, s2 = select([t1.c.col3.label('col3'), t1.c.col4.label('col4'
                        )], t1.c.col2.in_(['t1col2r1', 't1col2r2'])), \
            select([t2.c.col3.label('col3'), t2.c.col4.label('col4')],
                   t2.c.col2.in_(['t2col2r2', 't2col2r3']))
        u = union(s1, s2, order_by=['col3', 'col4'])
        self.assert_compile(u,
                            'SELECT t1.col3 AS col3, t1.col4 AS col4 '
                            'FROM t1 WHERE t1.col2 IN (:col2_1, '
                            ':col2_2) UNION SELECT t2.col3 AS col3, '
                            't2.col4 AS col4 FROM t2 WHERE t2.col2 IN '
                            '(:col2_3, :col2_4) ORDER BY col3, col4')
        self.assert_compile(u.alias('bar').select(),
                            'SELECT bar.col3, bar.col4 FROM (SELECT '
                            't1.col3 AS col3, t1.col4 AS col4 FROM t1 '
                            'WHERE t1.col2 IN (:col2_1, :col2_2) UNION '
                            'SELECT t2.col3 AS col3, t2.col4 AS col4 '
                            'FROM t2 WHERE t2.col2 IN (:col2_3, '
                            ':col2_4)) AS bar')

    def test_function(self):
        self.assert_compile(func.foo(1, 2), 'foo(:foo_1, :foo_2)')
        self.assert_compile(func.current_time(), 'CURRENT_TIME')
        self.assert_compile(func.foo(), 'foo()')
        m = MetaData()
        t = Table('sometable', m, Column('col1', Integer), Column('col2'
                  , Integer))
        self.assert_compile(select([func.max(t.c.col1)]),
                            'SELECT max(sometable.col1) AS max_1 FROM '
                            'sometable')

    def test_function_overrides(self):
        self.assert_compile(func.current_date(), "GETDATE()")
        self.assert_compile(func.length(3), "LEN(:length_1)")

    def test_extract(self):
        t = table('t', column('col1'))

        for field in 'day', 'month', 'year':
            self.assert_compile(
                select([extract(field, t.c.col1)]),
                'SELECT DATEPART("%s", t.col1) AS anon_1 FROM t' % field)

    def test_update_returning(self):
        table1 = table('mytable', column('myid', Integer), column('name'
                       , String(128)), column('description',
                       String(128)))
        u = update(table1, values=dict(name='foo'
                   )).returning(table1.c.myid, table1.c.name)
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name OUTPUT '
                            'inserted.myid, inserted.name')
        u = update(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name OUTPUT '
                            'inserted.myid, inserted.name, '
                            'inserted.description')
        u = update(table1, values=dict(name='foo'
                   )).returning(table1).where(table1.c.name == 'bar')
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name OUTPUT '
                            'inserted.myid, inserted.name, '
                            'inserted.description WHERE mytable.name = '
                            ':name_1')
        u = update(table1, values=dict(name='foo'
                   )).returning(func.length(table1.c.name))
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name OUTPUT '
                            'LEN(inserted.name) AS length_1')

    def test_delete_returning(self):
        table1 = table('mytable', column('myid', Integer), column('name'
                       , String(128)), column('description',
                       String(128)))
        d = delete(table1).returning(table1.c.myid, table1.c.name)
        self.assert_compile(d,
                            'DELETE FROM mytable OUTPUT deleted.myid, '
                            'deleted.name')
        d = delete(table1).where(table1.c.name == 'bar'
                                 ).returning(table1.c.myid,
                table1.c.name)
        self.assert_compile(d,
                            'DELETE FROM mytable OUTPUT deleted.myid, '
                            'deleted.name WHERE mytable.name = :name_1')

    def test_insert_returning(self):
        table1 = table('mytable', column('myid', Integer), column('name'
                       , String(128)), column('description',
                       String(128)))
        i = insert(table1, values=dict(name='foo'
                   )).returning(table1.c.myid, table1.c.name)
        self.assert_compile(i,
                            'INSERT INTO mytable (name) OUTPUT '
                            'inserted.myid, inserted.name VALUES '
                            '(:name)')
        i = insert(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(i,
                            'INSERT INTO mytable (name) OUTPUT '
                            'inserted.myid, inserted.name, '
                            'inserted.description VALUES (:name)')
        i = insert(table1, values=dict(name='foo'
                   )).returning(func.length(table1.c.name))
        self.assert_compile(i,
                            'INSERT INTO mytable (name) OUTPUT '
                            'LEN(inserted.name) AS length_1 VALUES '
                            '(:name)')

    def test_limit_using_top(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x==5).order_by(t.c.y).limit(10)

        self.assert_compile(
            s,
            "SELECT TOP 10 t.x, t.y FROM t WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={u'x_1': 5}
        )

    def test_limit_zero_using_top(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x==5).order_by(t.c.y).limit(0)

        self.assert_compile(
            s,
            "SELECT TOP 0 t.x, t.y FROM t WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={u'x_1': 5}
        )

    def test_offset_using_window(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x==5).order_by(t.c.y).offset(20)

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.y FROM (SELECT t.x AS x, t.y "
            "AS y, ROW_NUMBER() OVER (ORDER BY t.y) AS "
            "mssql_rn FROM t WHERE t.x = :x_1) AS "
            "anon_1 WHERE mssql_rn > :mssql_rn_1",
            checkparams={u'mssql_rn_1': 20, u'x_1': 5}
        )

    def test_limit_offset_using_window(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x==5).order_by(t.c.y).limit(10).offset(20)

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.y "
            "FROM (SELECT t.x AS x, t.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY t.y) AS mssql_rn "
            "FROM t "
            "WHERE t.x = :x_1) AS anon_1 "
            "WHERE mssql_rn > :mssql_rn_1 AND mssql_rn <= :mssql_rn_2",
            checkparams={u'mssql_rn_1': 20, u'mssql_rn_2': 30, u'x_1': 5}
        )

    def test_limit_zero_offset_using_window(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x==5).order_by(t.c.y).limit(0).offset(0)

        # render the LIMIT of zero, but not the OFFSET
        # of zero, so produces TOP 0
        self.assert_compile(
            s,
            "SELECT TOP 0 t.x, t.y FROM t "
            "WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={u'x_1': 5}
        )


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
            "SELECT t1.a, t1.b, t1.c, t2_1.a, t2_1.b, t2_1.c FROM t1 JOIN [schema].t2 AS t2_1 ON t2_1.a = t1.a"
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


class ReflectionTest(fixtures.TestBase, ComparesTables):
    __only_on__ = 'mssql'

    def test_basic_reflection(self):
        meta = MetaData(testing.db)

        users = Table(
            'engine_users',
            meta,
            Column('user_id', types.INT, primary_key=True),
            Column('user_name', types.VARCHAR(20), nullable=False),
            Column('test1', types.CHAR(5), nullable=False),
            Column('test2', types.Float(5), nullable=False),
            Column('test3', types.Text),
            Column('test4', types.Numeric, nullable=False),
            Column('test5', types.DateTime),
            Column('parent_user_id', types.Integer,
                   ForeignKey('engine_users.user_id')),
            Column('test6', types.DateTime, nullable=False),
            Column('test7', types.Text),
            Column('test8', types.LargeBinary),
            Column('test_passivedefault2', types.Integer,
                   server_default='5'),
            Column('test9', types.BINARY(100)),
            Column('test_numeric', types.Numeric()),
            test_needs_fk=True,
            )

        addresses = Table(
            'engine_email_addresses',
            meta,
            Column('address_id', types.Integer, primary_key=True),
            Column('remote_user_id', types.Integer,
                   ForeignKey(users.c.user_id)),
            Column('email_address', types.String(20)),
            test_needs_fk=True,
            )
        meta.create_all()

        try:
            meta2 = MetaData()
            reflected_users = Table('engine_users', meta2,
                                    autoload=True,
                                    autoload_with=testing.db)
            reflected_addresses = Table('engine_email_addresses',
                    meta2, autoload=True, autoload_with=testing.db)
            self.assert_tables_equal(users, reflected_users)
            self.assert_tables_equal(addresses, reflected_addresses)
        finally:
            meta.drop_all()

    @testing.provide_metadata
    def test_identity(self):
        metadata = self.metadata
        table = Table(
            'identity_test', metadata,
            Column('col1', Integer, Sequence('fred', 2, 3), primary_key=True)
        )
        table.create()

        meta2 = MetaData(testing.db)
        table2 = Table('identity_test', meta2, autoload=True)
        sequence = isinstance(table2.c['col1'].default, schema.Sequence) \
                                and table2.c['col1'].default
        assert sequence.start == 2
        assert sequence.increment == 3

    @testing.emits_warning("Did not recognize")
    @testing.provide_metadata
    def test_skip_types(self):
        metadata = self.metadata
        testing.db.execute("""
            create table foo (id integer primary key, data xml)
        """)
        t1 = Table('foo', metadata, autoload=True)
        assert isinstance(t1.c.id.type, Integer)
        assert isinstance(t1.c.data.type, types.NullType)

    @testing.provide_metadata
    def test_indexes_cols(self):
        metadata = self.metadata

        t1 = Table('t', metadata, Column('x', Integer), Column('y', Integer))
        Index('foo', t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table('t', m2, autoload=True, autoload_with=testing.db)

        eq_(
            set(list(t2.indexes)[0].columns),
            set([t2.c['x'], t2.c.y])
        )

    @testing.provide_metadata
    def test_indexes_cols_with_commas(self):
        metadata = self.metadata

        t1 = Table('t', metadata, 
                        Column('x, col', Integer, key='x'), 
                        Column('y', Integer)
                    )
        Index('foo', t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table('t', m2, autoload=True, autoload_with=testing.db)

        eq_(
            set(list(t2.indexes)[0].columns),
            set([t2.c['x, col'], t2.c.y])
        )

    @testing.provide_metadata
    def test_indexes_cols_with_spaces(self):
        metadata = self.metadata

        t1 = Table('t', metadata, Column('x col', Integer, key='x'), 
                                    Column('y', Integer))
        Index('foo', t1.c.x, t1.c.y)
        metadata.create_all()

        m2 = MetaData()
        t2 = Table('t', m2, autoload=True, autoload_with=testing.db)

        eq_(
            set(list(t2.indexes)[0].columns),
            set([t2.c['x col'], t2.c.y])
        )

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

        con.execute(u"insert into unitest_table values ('bien u\
                    umang\xc3\xa9')".encode('UTF-8'))
        try:
            r = t1.select().execute().first()
            assert isinstance(r[1], unicode), \
                '%s is %s instead of unicode, working on %s' % (r[1],
                    type(r[1]), meta.bind)
        finally:
            meta.drop_all()

class QueryTest(fixtures.TestBase):
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

class GenerativeQueryTest(fixtures.TestBase):
    __only_on__ = 'mssql'

    @classmethod
    def setup_class(cls):
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
            sess.add(Foo(bar=i, range=i%10))
        sess.flush()

    @classmethod
    def teardown_class(cls):
        metadata.drop_all()
        clear_mappers()

    def test_slice_mssql(self):
        sess = create_session(bind=testing.db)
        query = sess.query(Foo)
        orig = query.all()
        assert list(query[:10]) == orig[:10]
        assert list(query[:10]) == orig[:10]


class SchemaTest(fixtures.TestBase):

    def setup(self):
        t = Table('sometable', MetaData(),
            Column('pk_column', Integer),
            Column('test_column', String)
        )
        self.column = t.c.test_column

        dialect = mssql.dialect()
        self.ddl_compiler = dialect.ddl_compiler(dialect,
                schema.CreateTable(t))

    def _column_spec(self):
        return self.ddl_compiler.get_column_specification(self.column)

    def test_that_mssql_default_nullability_emits_null(self):
        eq_("test_column VARCHAR(max) NULL", self._column_spec())

    def test_that_mssql_none_nullability_does_not_emit_nullability(self):
        self.column.nullable = None
        eq_("test_column VARCHAR(max)", self._column_spec())

    def test_that_mssql_specified_nullable_emits_null(self):
        self.column.nullable = True
        eq_("test_column VARCHAR(max) NULL", self._column_spec())

    def test_that_mssql_specified_not_nullable_emits_not_null(self):
        self.column.nullable = False
        eq_("test_column VARCHAR(max) NOT NULL", self._column_spec())


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


class ParseConnectTest(fixtures.TestBase, AssertsCompiledSQL):
    @classmethod
    def setup_class(cls):
        global dialect
        dialect = pyodbc.dialect()

    def test_pyodbc_connect_dsn_trusted(self):
        u = url.make_url('mssql://mydsn')
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;Trusted_Connection=Yes'], {}], connection)

    def test_pyodbc_connect_old_style_dsn_trusted(self):
        u = url.make_url('mssql:///?dsn=mydsn')
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;Trusted_Connection=Yes'], {}], connection)

    def test_pyodbc_connect_dsn_non_trusted(self):
        u = url.make_url('mssql://username:password@mydsn')
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_dsn_extra(self):
        u = \
            url.make_url('mssql://username:password@mydsn/?LANGUAGE=us_'
                         'english&foo=bar')
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;UID=username;PWD=password;LANGUAGE=us_english;'
            'foo=bar'], {}], connection)

    def test_pyodbc_connect(self):
        u = url.make_url('mssql://username:password@hostspec/database')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec;Database=database;UI'
            'D=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_comma_port(self):
        u = \
            url.make_url('mssql://username:password@hostspec:12345/data'
                         'base')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec,12345;Database=datab'
            'ase;UID=username;PWD=password'], {}], connection)

    def test_pyodbc_connect_config_port(self):
        u = \
            url.make_url('mssql://username:password@hostspec/database?p'
                         'ort=12345')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec;Database=database;UI'
            'D=username;PWD=password;port=12345'], {}], connection)

    def test_pyodbc_extra_connect(self):
        u = \
            url.make_url('mssql://username:password@hostspec/database?L'
                         'ANGUAGE=us_english&foo=bar')
        connection = dialect.create_connect_args(u)
        eq_(connection[1], {})
        eq_(connection[0][0]
            in ('DRIVER={SQL Server};Server=hostspec;Database=database;'
            'UID=username;PWD=password;foo=bar;LANGUAGE=us_english',
            'DRIVER={SQL Server};Server=hostspec;Database=database;UID='
            'username;PWD=password;LANGUAGE=us_english;foo=bar'), True)

    def test_pyodbc_odbc_connect(self):
        u = \
            url.make_url('mssql:///?odbc_connect=DRIVER%3D%7BSQL+Server'
                         '%7D%3BServer%3Dhostspec%3BDatabase%3Ddatabase'
                         '%3BUID%3Dusername%3BPWD%3Dpassword')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec;Database=database;UI'
            'D=username;PWD=password'], {}], connection)

    def test_pyodbc_odbc_connect_with_dsn(self):
        u = \
            url.make_url('mssql:///?odbc_connect=dsn%3Dmydsn%3BDatabase'
                         '%3Ddatabase%3BUID%3Dusername%3BPWD%3Dpassword'
                         )
        connection = dialect.create_connect_args(u)
        eq_([['dsn=mydsn;Database=database;UID=username;PWD=password'],
            {}], connection)

    def test_pyodbc_odbc_connect_ignores_other_values(self):
        u = \
            url.make_url('mssql://userdiff:passdiff@localhost/dbdiff?od'
                         'bc_connect=DRIVER%3D%7BSQL+Server%7D%3BServer'
                         '%3Dhostspec%3BDatabase%3Ddatabase%3BUID%3Duse'
                         'rname%3BPWD%3Dpassword')
        connection = dialect.create_connect_args(u)
        eq_([['DRIVER={SQL Server};Server=hostspec;Database=database;UI'
            'D=username;PWD=password'], {}], connection)

    def test_pymssql_port_setting(self):
        dialect = pymssql.dialect()

        u = \
            url.make_url('mssql+pymssql://scott:tiger@somehost/test')
        connection = dialect.create_connect_args(u)
        eq_(
            [[], {'host': 'somehost', 'password': 'tiger', 
                    'user': 'scott', 'database': 'test'}], connection
        )

        u = \
            url.make_url('mssql+pymssql://scott:tiger@somehost:5000/test')
        connection = dialect.create_connect_args(u)
        eq_(
            [[], {'host': 'somehost:5000', 'password': 'tiger', 
                    'user': 'scott', 'database': 'test'}], connection
        )

    @testing.only_on(['mssql+pyodbc', 'mssql+pymssql'], "FreeTDS specific test")
    def test_bad_freetds_warning(self):
        engine = engines.testing_engine()

        def _bad_version(connection):
            return 95, 10, 255

        engine.dialect._get_server_version_info = _bad_version
        assert_raises_message(exc.SAWarning,
                              'Unrecognized server version info',
                              engine.connect)

class TimeTypeTest(fixtures.TestBase):

    def test_result_processor_no_microseconds(self):
        expected = datetime.time(12, 34, 56)
        self._assert_result_processor(expected, '12:34:56')

    def test_result_processor_too_many_microseconds(self):
        # microsecond must be in 0..999999, should truncate (6 vs 7 digits)
        expected = datetime.time(12, 34, 56, 123456)
        self._assert_result_processor(expected, '12:34:56.1234567')

    def _assert_result_processor(self, expected, value):
        mssql_time_type = TIME()
        result_processor = mssql_time_type.result_processor(None, None)
        eq_(expected, result_processor(value))

class TypesTest(fixtures.TestBase, AssertsExecutionResults, ComparesTables):
    __only_on__ = 'mssql'

    @classmethod
    def setup_class(cls):
        global metadata
        metadata = MetaData(testing.db)

    def teardown(self):
        metadata.drop_all()

    @testing.fails_on_everything_except('mssql+pyodbc',
            'this is some pyodbc-specific feature')
    def test_decimal_notation(self):
        numeric_table = Table('numeric_table', metadata, Column('id',
                              Integer, Sequence('numeric_id_seq',
                              optional=True), primary_key=True),
                              Column('numericcol',
                              Numeric(precision=38, scale=20,
                              asdecimal=True)))
        metadata.create_all()
        test_items = [decimal.Decimal(d) for d in (
            '1500000.00000000000000000000',
            '-1500000.00000000000000000000',
            '1500000',
            '0.0000000000000000002',
            '0.2',
            '-0.0000000000000000002',
            '-2E-2',
            '156666.458923543',
            '-156666.458923543',
            '1',
            '-1',
            '-1234',
            '1234',
            '2E-12',
            '4E8',
            '3E-6',
            '3E-7',
            '4.1',
            '1E-1',
            '1E-2',
            '1E-3',
            '1E-4',
            '1E-5',
            '1E-6',
            '1E-7',
            '1E-1',
            '1E-8',
            '0.2732E2',
            '-0.2432E2',
            '4.35656E2',
            '-02452E-2',
            '45125E-2',
            '1234.58965E-2',
            '1.521E+15',
            '-1E-25',
            '1E-25',
            '1254E-25',
            '-1203E-25',
            '0',
            '-0.00',
            '-0',
            '4585E12',
            '000000000000000000012',
            '000000000000.32E12',
            '00000000000000.1E+12',
            '000000000000.2E-32',
            )]

        for value in test_items:
            numeric_table.insert().execute(numericcol=value)

        for value in select([numeric_table.c.numericcol]).execute():
            assert value[0] in test_items, "%r not in test_items" % value[0]

    def test_float(self):
        float_table = Table('float_table', metadata, Column('id',
                            Integer, Sequence('numeric_id_seq',
                            optional=True), primary_key=True),
                            Column('floatcol', Float()))
        metadata.create_all()
        try:
            test_items = [float(d) for d in (
                '1500000.00000000000000000000',
                '-1500000.00000000000000000000',
                '1500000',
                '0.0000000000000000002',
                '0.2',
                '-0.0000000000000000002',
                '156666.458923543',
                '-156666.458923543',
                '1',
                '-1',
                '1234',
                '2E-12',
                '4E8',
                '3E-6',
                '3E-7',
                '4.1',
                '1E-1',
                '1E-2',
                '1E-3',
                '1E-4',
                '1E-5',
                '1E-6',
                '1E-7',
                '1E-8',
                )]
            for value in test_items:
                float_table.insert().execute(floatcol=value)
        except Exception, e:
            raise e

    def test_money(self):
        """Exercise type specification for money types."""

        columns = [(mssql.MSMoney, [], {}, 'MONEY'),
                   (mssql.MSSmallMoney, [], {}, 'SMALLMONEY')]
        table_args = ['test_mssql_money', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw),
                              nullable=None))
        money_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect,
                                   schema.CreateTable(money_table))
        for col in money_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col), '%s %s'
                        % (col.name, columns[index][3]))
            self.assert_(repr(col))
        try:
            money_table.create(checkfirst=True)
            assert True
        except:
            raise
        money_table.drop()

    # todo this should suppress warnings, but it does not
    @emits_warning_on('mssql+mxodbc', r'.*does not have any indexes.*')
    def test_dates(self):
        "Exercise type specification for date types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSDateTime, [], {},
             'DATETIME', []),

            (types.DATE, [], {},
             'DATE', ['>=', (10,)]),
            (types.Date, [], {},
             'DATE', ['>=', (10,)]),
            (types.Date, [], {},
             'DATETIME', ['<', (10,)], mssql.MSDateTime),
            (mssql.MSDate, [], {},
             'DATE', ['>=', (10,)]),
            (mssql.MSDate, [], {},
             'DATETIME', ['<', (10,)], mssql.MSDateTime),

            (types.TIME, [], {},
             'TIME', ['>=', (10,)]),
            (types.Time, [], {},
             'TIME', ['>=', (10,)]),
            (mssql.MSTime, [], {},
             'TIME', ['>=', (10,)]),
            (mssql.MSTime, [1], {},
             'TIME(1)', ['>=', (10,)]),
            (types.Time, [], {},
             'DATETIME', ['<', (10,)], mssql.MSDateTime),
            (mssql.MSTime, [], {},
             'TIME', ['>=', (10,)]),

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

        table_args = ['test_mssql_dates', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res, requires = spec[0:5]
            if requires and testing._is_excluded('mssql', *requires) \
                or not requires:
                c = Column('c%s' % index, type_(*args,
                                  **kw), nullable=None)
                testing.db.dialect.type_descriptor(c.type)
                table_args.append(c)
        dates_table = Table(*table_args)
        gen = testing.db.dialect.ddl_compiler(testing.db.dialect,
                schema.CreateTable(dates_table))
        for col in dates_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col), '%s %s'
                        % (col.name, columns[index][3]))
            self.assert_(repr(col))
        dates_table.create(checkfirst=True)
        reflected_dates = Table('test_mssql_dates',
                                MetaData(testing.db), autoload=True)
        for col in reflected_dates.c:
            self.assert_types_base(col, dates_table.c[col.key])

    def test_date_roundtrip(self):
        t = Table('test_dates', metadata,
                    Column('id', Integer,
                           Sequence('datetest_id_seq', optional=True),
                           primary_key=True),
                    Column('adate', Date),
                    Column('atime', Time),
                    Column('adatetime', DateTime))
        metadata.create_all()
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

        eq_(select([t.c.adate, t.c.atime, t.c.adatetime], t.c.adate
            == d1).execute().fetchall(), [(d1, t1, d2)])

    @emits_warning_on('mssql+mxodbc', r'.*does not have any indexes.*')
    def test_binary(self):
        "Exercise type specification for binary types."

        columns = [
            # column type, args, kwargs, expected ddl
            (mssql.MSBinary, [], {},
             'BINARY'),
            (mssql.MSBinary, [10], {},
             'BINARY(10)'),

            (types.BINARY, [], {},
             'BINARY'),
            (types.BINARY, [10], {},
             'BINARY(10)'),

            (mssql.MSVarBinary, [], {},
             'VARBINARY(max)'),
            (mssql.MSVarBinary, [10], {},
             'VARBINARY(10)'),

            (types.VARBINARY, [10], {},
             'VARBINARY(10)'),
            (types.VARBINARY, [], {},
             'VARBINARY(max)'),

            (mssql.MSImage, [], {},
             'IMAGE'),

            (mssql.IMAGE, [], {},
             'IMAGE'),

            (types.LargeBinary, [], {},
             'IMAGE'),
        ]

        table_args = ['test_mssql_binary', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw),
                              nullable=None))
        binary_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect,
                                   schema.CreateTable(binary_table))
        for col in binary_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col), '%s %s'
                        % (col.name, columns[index][3]))
            self.assert_(repr(col))
        metadata.create_all()
        reflected_binary = Table('test_mssql_binary',
                                 MetaData(testing.db), autoload=True)
        for col in reflected_binary.c:
            c1 = testing.db.dialect.type_descriptor(col.type).__class__
            c2 = \
                testing.db.dialect.type_descriptor(
                    binary_table.c[col.name].type).__class__
            assert issubclass(c1, c2), '%r is not a subclass of %r' \
                % (c1, c2)
            if binary_table.c[col.name].type.length:
                testing.eq_(col.type.length,
                            binary_table.c[col.name].type.length)

    def test_boolean(self):
        "Exercise type specification for boolean type."

        columns = [
            # column type, args, kwargs, expected ddl
            (Boolean, [], {},
             'BIT'),
           ]

        table_args = ['test_mssql_boolean', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column('c%s' % index, type_(*args, **kw), nullable=None))

        boolean_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(boolean_table))

        for col in boolean_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        metadata.create_all()

    def test_numeric(self):
        "Exercise type specification and options for numeric types."

        columns = [
            # column type, args, kwargs, expected ddl
            (types.NUMERIC, [], {},
             'NUMERIC'),
            (types.NUMERIC, [None], {},
             'NUMERIC'),
            (types.NUMERIC, [12, 4], {},
             'NUMERIC(12, 4)'),

            (types.Float, [], {},
             'FLOAT'),
            (types.Float, [None], {},
             'FLOAT'),
            (types.Float, [12], {},
             'FLOAT(12)'),
            (mssql.MSReal, [], {},
             'REAL'),

            (types.Integer, [], {},
             'INTEGER'),
            (types.BigInteger, [], {},
             'BIGINT'),
            (mssql.MSTinyInteger, [], {},
             'TINYINT'),
            (types.SmallInteger, [], {},
             'SMALLINT'),
           ]

        table_args = ['test_mssql_numeric', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column('c%s' % index, type_(*args, **kw), nullable=None))

        numeric_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(numeric_table))

        for col in numeric_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        metadata.create_all()

    def test_char(self):
        """Exercise COLLATE-ish options on string types."""

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
             'VARCHAR(max)'),
            (mssql.MSString, [1], {},
             'VARCHAR(1)'),
            (mssql.MSString, [1], {'collation': 'Latin1_General_CI_AS'},
             'VARCHAR(1) COLLATE Latin1_General_CI_AS'),

            (mssql.MSNVarchar, [], {},
             'NVARCHAR(max)'),
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

        table_args = ['test_mssql_charset', metadata]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(
                Column('c%s' % index, type_(*args, **kw), nullable=None))

        charset_table = Table(*table_args)
        dialect = mssql.dialect()
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(charset_table))

        for col in charset_table.c:
            index = int(col.name[1:])
            testing.eq_(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        metadata.create_all()

    def test_timestamp(self):
        """Exercise TIMESTAMP column."""

        dialect = mssql.dialect()

        spec, expected = (TIMESTAMP,'TIMESTAMP')
        t = Table('mssql_ts', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('t', spec, nullable=None))
        gen = dialect.ddl_compiler(dialect, schema.CreateTable(t))
        testing.eq_(gen.get_column_specification(t.c.t), "t %s" % expected)
        self.assert_(repr(t.c.t))
        t.create(checkfirst=True)

    def test_autoincrement(self):
        Table('ai_1', metadata,
               Column('int_y', Integer, primary_key=True),
               Column('int_n', Integer, DefaultClause('0'),
                      primary_key=True, autoincrement=False))
        Table('ai_2', metadata,
               Column('int_y', Integer, primary_key=True),
               Column('int_n', Integer, DefaultClause('0'),
                      primary_key=True, autoincrement=False))
        Table('ai_3', metadata,
               Column('int_n', Integer, DefaultClause('0'),
                      primary_key=True, autoincrement=False),
               Column('int_y', Integer, primary_key=True))
        Table('ai_4', metadata,
               Column('int_n', Integer, DefaultClause('0'),
                      primary_key=True, autoincrement=False),
               Column('int_n2', Integer, DefaultClause('0'),
                      primary_key=True, autoincrement=False))
        Table('ai_5', metadata,
               Column('int_y', Integer, primary_key=True),
               Column('int_n', Integer, DefaultClause('0'),
                      primary_key=True, autoincrement=False))
        Table('ai_6', metadata,
               Column('o1', String(1), DefaultClause('x'),
                      primary_key=True),
               Column('int_y', Integer, primary_key=True))
        Table('ai_7', metadata,
               Column('o1', String(1), DefaultClause('x'),
                      primary_key=True),
               Column('o2', String(1), DefaultClause('x'),
                      primary_key=True),
               Column('int_y', Integer, primary_key=True))
        Table('ai_8', metadata,
               Column('o1', String(1), DefaultClause('x'),
                      primary_key=True),
               Column('o2', String(1), DefaultClause('x'),
                      primary_key=True))
        metadata.create_all()

        table_names = ['ai_1', 'ai_2', 'ai_3', 'ai_4',
                        'ai_5', 'ai_6', 'ai_7', 'ai_8']
        mr = MetaData(testing.db)

        for name in table_names:
            tbl = Table(name, mr, autoload=True)
            tbl = metadata.tables[name]
            for c in tbl.c:
                if c.name.startswith('int_y'):
                    assert c.autoincrement, name
                    assert tbl._autoincrement_column is c, name
                elif c.name.startswith('int_n'):
                    assert not c.autoincrement, name
                    assert tbl._autoincrement_column is not c, name

            # mxodbc can't handle scope_identity() with DEFAULT VALUES

            if testing.db.driver == 'mxodbc':
                eng = \
                    [engines.testing_engine(options={'implicit_returning'
                     : True})]
            else:
                eng = \
                    [engines.testing_engine(options={'implicit_returning'
                     : False}),
                     engines.testing_engine(options={'implicit_returning'
                     : True})]

            for counter, engine in enumerate(eng):
                engine.execute(tbl.insert())
                if 'int_y' in tbl.c:
                    assert engine.scalar(select([tbl.c.int_y])) \
                        == counter + 1
                    assert list(engine.execute(tbl.select()).first()).\
                            count(counter + 1) == 1
                else:
                    assert 1 \
                        not in list(engine.execute(tbl.select()).first())
                engine.execute(tbl.delete())

class BinaryTest(fixtures.TestBase, AssertsExecutionResults):
    """Test the Binary and VarBinary types"""

    __only_on__ = 'mssql'

    @classmethod
    def setup_class(cls):
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

        binary_table = Table(
            'binary_table',
            MetaData(testing.db),
            Column('primary_id', Integer, Sequence('binary_id_seq',
                   optional=True), primary_key=True),
            Column('data', mssql.MSVarBinary(8000)),
            Column('data_image', mssql.MSImage),
            Column('data_slice', types.BINARY(100)),
            Column('misc', String(30)),
            Column('pickled', PickleType),
            Column('mypickle', MyPickleType),
            )
        binary_table.create()

    def teardown(self):
        binary_table.delete().execute()

    @classmethod
    def teardown_class(cls):
        binary_table.drop()

    def test_binary(self):
        testobj1 = pickleable.Foo('im foo 1')
        testobj2 = pickleable.Foo('im foo 2')
        testobj3 = pickleable.Foo('im foo 3')
        stream1 = self.load_stream('binary_data_one.dat')
        stream2 = self.load_stream('binary_data_two.dat')
        binary_table.insert().execute(
            primary_id=1,
            misc='binary_data_one.dat',
            data=stream1,
            data_image=stream1,
            data_slice=stream1[0:100],
            pickled=testobj1,
            mypickle=testobj3,
            )
        binary_table.insert().execute(
            primary_id=2,
            misc='binary_data_two.dat',
            data=stream2,
            data_image=stream2,
            data_slice=stream2[0:99],
            pickled=testobj2,
            )

        # TODO: pyodbc does not seem to accept "None" for a VARBINARY
        # column (data=None). error:  [Microsoft][ODBC SQL Server
        # Driver][SQL Server]Implicit conversion from data type varchar
        # to varbinary is not allowed. Use the CONVERT function to run
        # this query. (257) binary_table.insert().execute(primary_id=3,
        # misc='binary_data_two.dat', data=None, data_image=None,
        # data_slice=stream2[0:99], pickled=None)

        binary_table.insert().execute(primary_id=3,
                misc='binary_data_two.dat', data_image=None,
                data_slice=stream2[0:99], pickled=None)
        for stmt in \
            binary_table.select(order_by=binary_table.c.primary_id), \
            text('select * from binary_table order by '
                 'binary_table.primary_id',
                 typemap=dict(data=mssql.MSVarBinary(8000),
                 data_image=mssql.MSImage,
                 data_slice=types.BINARY(100), pickled=PickleType,
                 mypickle=MyPickleType), bind=testing.db):
            l = stmt.execute().fetchall()
            eq_(list(stream1), list(l[0]['data']))
            paddedstream = list(stream1[0:100])
            paddedstream.extend(['\x00'] * (100 - len(paddedstream)))
            eq_(paddedstream, list(l[0]['data_slice']))
            eq_(list(stream2), list(l[1]['data']))
            eq_(list(stream2), list(l[1]['data_image']))
            eq_(testobj1, l[0]['pickled'])
            eq_(testobj2, l[1]['pickled'])
            eq_(testobj3.moredata, l[0]['mypickle'].moredata)
            eq_(l[0]['mypickle'].stuff, 'this is the right stuff')

    def load_stream(self, name, len=3000):
        fp = open(os.path.join(os.path.dirname(__file__), "..", name), 'rb')
        stream = fp.read(len)
        fp.close()
        return stream


class ReflectHugeViewTest(fixtures.TestBase):
    __only_on__ = 'mssql'

    def setup(self):
        self.col_num = 150

        self.metadata = MetaData(testing.db)
        t = Table('base_table', self.metadata,
                *[
                    Column("long_named_column_number_%d" % i, Integer)
                    for i in xrange(self.col_num)
                ]
        )
        self.view_str = view_str = \
            "CREATE VIEW huge_named_view AS SELECT %s FROM base_table" % (
            ",".join("long_named_column_number_%d" % i 
                        for i in xrange(self.col_num))
            )
        assert len(view_str) > 4000

        event.listen(t, 'after_create', DDL(view_str) )
        event.listen(t, 'before_drop', DDL("DROP VIEW huge_named_view") )

        self.metadata.create_all()

    def teardown(self):
        self.metadata.drop_all()

    def test_inspect_view_definition(self):
        inspector = Inspector.from_engine(testing.db)
        view_def = inspector.get_view_definition("huge_named_view")
        eq_(view_def, self.view_str)

