# -*- encoding: utf-8
from sqlalchemy.testing import eq_, is_
from sqlalchemy import schema
from sqlalchemy.sql import table, column
from sqlalchemy.databases import mssql
from sqlalchemy.dialects.mssql import mxodbc
from sqlalchemy.testing import fixtures, AssertsCompiledSQL
from sqlalchemy import sql
from sqlalchemy import Integer, String, Table, Column, select, MetaData,\
    update, delete, insert, extract, union, func, PrimaryKeyConstraint, \
    UniqueConstraint, Index, Sequence, literal


class CompileTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = mssql.dialect(legacy_schema_aliasing=False)

    def test_true_false(self):
        self.assert_compile(
            sql.false(), "0"
        )
        self.assert_compile(
            sql.true(),
            "1"
        )

    def test_select(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.select(),
                            'SELECT sometable.somecolumn FROM sometable')

    def test_select_with_nolock(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(
            t.select().with_hint(t, 'WITH (NOLOCK)'),
            'SELECT sometable.somecolumn FROM sometable WITH (NOLOCK)')

    def test_select_with_nolock_schema(self):
        m = MetaData()
        t = Table('sometable', m, Column('somecolumn', Integer),
                  schema='test_schema')
        self.assert_compile(
            t.select().with_hint(t, 'WITH (NOLOCK)'),
            'SELECT test_schema.sometable.somecolumn '
            'FROM test_schema.sometable WITH (NOLOCK)')

    def test_join_with_hint(self):
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
        join = t1.join(t2, t1.c.a == t2.c.a).\
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

    def test_insert_hint(self):
        t = table('sometable', column('somecolumn'))
        for targ in (None, t):
            for darg in ("*", "mssql"):
                self.assert_compile(
                    t.insert().
                    values(somecolumn="x").
                    with_hint("WITH (PAGLOCK)",
                              selectable=targ,
                              dialect_name=darg),
                    "INSERT INTO sometable WITH (PAGLOCK) "
                    "(somecolumn) VALUES (:somecolumn)"
                )

    def test_update_hint(self):
        t = table('sometable', column('somecolumn'))
        for targ in (None, t):
            for darg in ("*", "mssql"):
                self.assert_compile(
                    t.update().where(t.c.somecolumn == "q").
                    values(somecolumn="x").
                    with_hint("WITH (PAGLOCK)",
                              selectable=targ,
                              dialect_name=darg),
                    "UPDATE sometable WITH (PAGLOCK) "
                    "SET somecolumn=:somecolumn "
                    "WHERE sometable.somecolumn = :somecolumn_1"
                )

    def test_update_exclude_hint(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(
            t.update().where(t.c.somecolumn == "q").
            values(somecolumn="x").
            with_hint("XYZ", "mysql"),
            "UPDATE sometable SET somecolumn=:somecolumn "
            "WHERE sometable.somecolumn = :somecolumn_1"
        )

    def test_delete_hint(self):
        t = table('sometable', column('somecolumn'))
        for targ in (None, t):
            for darg in ("*", "mssql"):
                self.assert_compile(
                    t.delete().where(t.c.somecolumn == "q").
                    with_hint("WITH (PAGLOCK)",
                              selectable=targ,
                              dialect_name=darg),
                    "DELETE FROM sometable WITH (PAGLOCK) "
                    "WHERE sometable.somecolumn = :somecolumn_1"
                )

    def test_delete_exclude_hint(self):
        t = table('sometable', column('somecolumn'))
        self.assert_compile(
            t.delete().
            where(t.c.somecolumn == "q").
            with_hint("XYZ", dialect_name="mysql"),
            "DELETE FROM sometable WHERE "
            "sometable.somecolumn = :somecolumn_1"
        )

    def test_update_from_hint(self):
        t = table('sometable', column('somecolumn'))
        t2 = table('othertable', column('somecolumn'))
        for darg in ("*", "mssql"):
            self.assert_compile(
                t.update().where(t.c.somecolumn == t2.c.somecolumn).
                values(somecolumn="x").
                with_hint("WITH (PAGLOCK)",
                          selectable=t2,
                          dialect_name=darg),
                "UPDATE sometable SET somecolumn=:somecolumn "
                "FROM sometable, othertable WITH (PAGLOCK) "
                "WHERE sometable.somecolumn = othertable.somecolumn"
            )

    def test_update_to_select_schema(self):
        meta = MetaData()
        table = Table(
            "sometable", meta,
            Column("sym", String),
            Column("val", Integer),
            schema="schema"
        )
        other = Table(
            "#other", meta,
            Column("sym", String),
            Column("newval", Integer)
        )
        stmt = table.update().values(
            val=select([other.c.newval]).
            where(table.c.sym == other.c.sym).as_scalar())

        self.assert_compile(
            stmt,
            "UPDATE [schema].sometable SET val="
            "(SELECT [#other].newval FROM [#other] "
            "WHERE [schema].sometable.sym = [#other].sym)",
        )

        stmt = table.update().values(val=other.c.newval).\
            where(table.c.sym == other.c.sym)
        self.assert_compile(
            stmt,
            "UPDATE [schema].sometable SET val="
            "[#other].newval FROM [schema].sometable, "
            "[#other] WHERE [schema].sometable.sym = [#other].sym",
        )

    # TODO: not supported yet.
    # def test_delete_from_hint(self):
    #    t = table('sometable', column('somecolumn'))
    #    t2 = table('othertable', column('somecolumn'))
    #    for darg in ("*", "mssql"):
    #        self.assert_compile(
    #            t.delete().where(t.c.somecolumn==t2.c.somecolumn).
    #                    with_hint("WITH (PAGLOCK)",
    #                            selectable=t2,
    #                            dialect_name=darg),
    #            ""
    #        )

    def test_strict_binds(self):
        """test the 'strict' compiler binds."""

        from sqlalchemy.dialects.mssql.base import MSSQLStrictCompiler
        mxodbc_dialect = mxodbc.dialect()
        mxodbc_dialect.statement_compiler = MSSQLStrictCompiler

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
                t.c.foo.in_([None]),
                "sometable.foo IN (NULL)"
            )
        ]:
            self.assert_compile(expr, compile, dialect=mxodbc_dialect)

    def test_in_with_subqueries(self):
        """Test removal of legacy behavior that converted "x==subquery"
        to use IN.

        """

        t = table('sometable', column('somecolumn'))
        self.assert_compile(t.select().where(t.c.somecolumn
                                             == t.select()),
                            'SELECT sometable.somecolumn FROM '
                            'sometable WHERE sometable.somecolumn = '
                            '(SELECT sometable.somecolumn FROM '
                            'sometable)')
        self.assert_compile(t.select().where(t.c.somecolumn
                                             != t.select()),
                            'SELECT sometable.somecolumn FROM '
                            'sometable WHERE sometable.somecolumn != '
                            '(SELECT sometable.somecolumn FROM '
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
                            '(SELECT paj.test.id FROM paj.test '
                            'WHERE paj.test.id = :id_1)')

    def test_delete_schema_multipart(self):
        metadata = MetaData()
        tbl = Table(
            'test', metadata,
            Column('id', Integer,
                   primary_key=True),
            schema='banana.paj')
        self.assert_compile(tbl.delete(tbl.c.id == 1),
                            'DELETE FROM banana.paj.test WHERE '
                            'banana.paj.test.id = :id_1')
        s = select([tbl.c.id]).where(tbl.c.id == 1)
        self.assert_compile(tbl.delete().where(tbl.c.id.in_(s)),
                            'DELETE FROM banana.paj.test WHERE '
                            'banana.paj.test.id IN (SELECT banana.paj.test.id '
                            'FROM banana.paj.test WHERE '
                            'banana.paj.test.id = :id_1)')

    def test_delete_schema_multipart_needs_quoting(self):
        metadata = MetaData()
        tbl = Table(
            'test', metadata,
            Column('id', Integer, primary_key=True),
            schema='banana split.paj')
        self.assert_compile(tbl.delete(tbl.c.id == 1),
                            'DELETE FROM [banana split].paj.test WHERE '
                            '[banana split].paj.test.id = :id_1')
        s = select([tbl.c.id]).where(tbl.c.id == 1)
        self.assert_compile(tbl.delete().where(tbl.c.id.in_(s)),
                            'DELETE FROM [banana split].paj.test WHERE '
                            '[banana split].paj.test.id IN ('

                            'SELECT [banana split].paj.test.id FROM '
                            '[banana split].paj.test WHERE '
                            '[banana split].paj.test.id = :id_1)')

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
        self.assert_compile(
            tbl.delete().where(tbl.c.id.in_(s)),
            "DELETE FROM [banana split].[paj with a space].test "
            "WHERE [banana split].[paj with a space].test.id IN "
            "(SELECT [banana split].[paj with a space].test.id "
            "FROM [banana split].[paj with a space].test "
            "WHERE [banana split].[paj with a space].test.id = :id_1)"
        )

    def test_union(self):
        t1 = table(
            't1', column('col1'), column('col2'),
            column('col3'), column('col4'))
        t2 = table(
            't2', column('col1'), column('col2'),
            column('col3'), column('col4'))
        s1, s2 = select(
            [t1.c.col3.label('col3'), t1.c.col4.label('col4')],
            t1.c.col2.in_(['t1col2r1', 't1col2r2'])), \
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
        t = Table(
            'sometable', m, Column('col1', Integer), Column('col2', Integer))
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
                'SELECT DATEPART(%s, t.col1) AS anon_1 FROM t' % field)

    def test_update_returning(self):
        table1 = table(
            'mytable',
            column('myid', Integer),
            column('name', String(128)),
            column('description', String(128)))
        u = update(
            table1,
            values=dict(name='foo')).returning(table1.c.myid, table1.c.name)
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name OUTPUT '
                            'inserted.myid, inserted.name')
        u = update(table1, values=dict(name='foo')).returning(table1)
        self.assert_compile(u,
                            'UPDATE mytable SET name=:name OUTPUT '
                            'inserted.myid, inserted.name, '
                            'inserted.description')
        u = update(
            table1,
            values=dict(
                name='foo')).returning(table1).where(table1.c.name == 'bar')
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
        table1 = table(
            'mytable', column('myid', Integer),
            column('name', String(128)), column('description', String(128)))
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
        table1 = table(
            'mytable', column('myid', Integer),
            column('name', String(128)), column('description', String(128)))
        i = insert(
            table1,
            values=dict(name='foo')).returning(table1.c.myid, table1.c.name)
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

        s = select([t]).where(t.c.x == 5).order_by(t.c.y).limit(10)

        self.assert_compile(
            s,
            "SELECT TOP 10 t.x, t.y FROM t WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={'x_1': 5}
        )

    def test_limit_zero_using_top(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x == 5).order_by(t.c.y).limit(0)

        self.assert_compile(
            s,
            "SELECT TOP 0 t.x, t.y FROM t WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={'x_1': 5}
        )
        c = s.compile(dialect=mssql.MSDialect())
        eq_(len(c._result_columns), 2)
        assert t.c.x in set(c._create_result_map()['x'][1])

    def test_offset_using_window(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x == 5).order_by(t.c.y).offset(20)

        # test that the select is not altered with subsequent compile
        # calls
        for i in range(2):
            self.assert_compile(
                s,
                "SELECT anon_1.x, anon_1.y FROM (SELECT t.x AS x, t.y "
                "AS y, ROW_NUMBER() OVER (ORDER BY t.y) AS "
                "mssql_rn FROM t WHERE t.x = :x_1) AS "
                "anon_1 WHERE mssql_rn > :param_1",
                checkparams={'param_1': 20, 'x_1': 5}
            )

            c = s.compile(dialect=mssql.MSDialect())
            eq_(len(c._result_columns), 2)
            assert t.c.x in set(c._create_result_map()['x'][1])

    def test_limit_offset_using_window(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x == 5).order_by(t.c.y).limit(10).offset(20)

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.y "
            "FROM (SELECT t.x AS x, t.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY t.y) AS mssql_rn "
            "FROM t "
            "WHERE t.x = :x_1) AS anon_1 "
            "WHERE mssql_rn > :param_1 AND mssql_rn <= :param_2 + :param_1",
            checkparams={'param_1': 20, 'param_2': 10, 'x_1': 5}
        )
        c = s.compile(dialect=mssql.MSDialect())
        eq_(len(c._result_columns), 2)
        assert t.c.x in set(c._create_result_map()['x'][1])
        assert t.c.y in set(c._create_result_map()['y'][1])

    def test_limit_offset_w_ambiguous_cols(self):
        t = table('t', column('x', Integer), column('y', Integer))

        cols = [t.c.x, t.c.x.label('q'), t.c.x.label('p'), t.c.y]
        s = select(cols).where(t.c.x == 5).order_by(t.c.y).limit(10).offset(20)

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.q, anon_1.p, anon_1.y "
            "FROM (SELECT t.x AS x, t.x AS q, t.x AS p, t.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY t.y) AS mssql_rn "
            "FROM t "
            "WHERE t.x = :x_1) AS anon_1 "
            "WHERE mssql_rn > :param_1 AND mssql_rn <= :param_2 + :param_1",
            checkparams={'param_1': 20, 'param_2': 10, 'x_1': 5}
        )
        c = s.compile(dialect=mssql.MSDialect())
        eq_(len(c._result_columns), 4)

        result_map = c._create_result_map()

        for col in cols:
            is_(result_map[col.key][1][0], col)

    def test_limit_offset_with_correlated_order_by(self):
        t1 = table('t1', column('x', Integer), column('y', Integer))
        t2 = table('t2', column('x', Integer), column('y', Integer))

        order_by = select([t2.c.y]).where(t1.c.x == t2.c.x).as_scalar()
        s = select([t1]).where(t1.c.x == 5).order_by(order_by) \
            .limit(10).offset(20)

        self.assert_compile(
            s,
            "SELECT anon_1.x, anon_1.y "
            "FROM (SELECT t1.x AS x, t1.y AS y, "
            "ROW_NUMBER() OVER (ORDER BY "
            "(SELECT t2.y FROM t2 WHERE t1.x = t2.x)"
            ") AS mssql_rn "
            "FROM t1 "
            "WHERE t1.x = :x_1) AS anon_1 "
            "WHERE mssql_rn > :param_1 AND mssql_rn <= :param_2 + :param_1",
            checkparams={'param_1': 20, 'param_2': 10, 'x_1': 5}
        )

        c = s.compile(dialect=mssql.MSDialect())
        eq_(len(c._result_columns), 2)
        assert t1.c.x in set(c._create_result_map()['x'][1])
        assert t1.c.y in set(c._create_result_map()['y'][1])

    def test_offset_dont_misapply_labelreference(self):
        m = MetaData()

        t = Table('t', m, Column('x', Integer))

        expr1 = func.foo(t.c.x).label('x')
        expr2 = func.foo(t.c.x).label('y')

        stmt1 = select([expr1]).order_by(expr1.desc()).offset(1)
        stmt2 = select([expr2]).order_by(expr2.desc()).offset(1)

        self.assert_compile(
            stmt1,
            "SELECT anon_1.x FROM (SELECT foo(t.x) AS x, "
            "ROW_NUMBER() OVER (ORDER BY foo(t.x) DESC) AS mssql_rn FROM t) "
            "AS anon_1 WHERE mssql_rn > :param_1"
        )

        self.assert_compile(
            stmt2,
            "SELECT anon_1.y FROM (SELECT foo(t.x) AS y, "
            "ROW_NUMBER() OVER (ORDER BY foo(t.x) DESC) AS mssql_rn FROM t) "
            "AS anon_1 WHERE mssql_rn > :param_1"
        )

    def test_limit_zero_offset_using_window(self):
        t = table('t', column('x', Integer), column('y', Integer))

        s = select([t]).where(t.c.x == 5).order_by(t.c.y).limit(0).offset(0)

        # render the LIMIT of zero, but not the OFFSET
        # of zero, so produces TOP 0
        self.assert_compile(
            s,
            "SELECT TOP 0 t.x, t.y FROM t "
            "WHERE t.x = :x_1 ORDER BY t.y",
            checkparams={'x_1': 5}
        )

    def test_sequence_start_0(self):
        metadata = MetaData()
        tbl = Table('test', metadata,
                    Column('id', Integer, Sequence('', 0), primary_key=True))
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(0,1), "
            "PRIMARY KEY (id))"
        )

    def test_sequence_non_primary_key(self):
        metadata = MetaData()
        tbl = Table('test', metadata,
                    Column('id', Integer, Sequence(''), primary_key=False))
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(1,1))"
        )

    def test_sequence_ignore_nullability(self):
        metadata = MetaData()
        tbl = Table('test', metadata,
                    Column('id', Integer, Sequence(''), nullable=True))
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (id INTEGER NOT NULL IDENTITY(1,1))"
        )

    def test_table_pkc_clustering(self):
        metadata = MetaData()
        tbl = Table('test', metadata,
                    Column('x', Integer, autoincrement=False),
                    Column('y', Integer, autoincrement=False),
                    PrimaryKeyConstraint("x", "y", mssql_clustered=True))
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (x INTEGER NOT NULL, y INTEGER NOT NULL, "
            "PRIMARY KEY CLUSTERED (x, y))"
        )

    def test_table_uc_clustering(self):
        metadata = MetaData()
        tbl = Table('test', metadata,
                    Column('x', Integer, autoincrement=False),
                    Column('y', Integer, autoincrement=False),
                    PrimaryKeyConstraint("x"),
                    UniqueConstraint("y", mssql_clustered=True))
        self.assert_compile(
            schema.CreateTable(tbl),
            "CREATE TABLE test (x INTEGER NOT NULL, y INTEGER NULL, "
            "PRIMARY KEY (x), UNIQUE CLUSTERED (y))"
        )

    def test_index_clustering(self):
        metadata = MetaData()
        tbl = Table('test', metadata,
                    Column('id', Integer))
        idx = Index("foo", tbl.c.id, mssql_clustered=True)
        self.assert_compile(schema.CreateIndex(idx),
                            "CREATE CLUSTERED INDEX foo ON test (id)"
                            )

    def test_index_ordering(self):
        metadata = MetaData()
        tbl = Table(
            'test', metadata,
            Column('x', Integer), Column('y', Integer), Column('z', Integer))
        idx = Index("foo", tbl.c.x.desc(), "y")
        self.assert_compile(schema.CreateIndex(idx),
                            "CREATE INDEX foo ON test (x DESC, y)"
                            )

    def test_create_index_expr(self):
        m = MetaData()
        t1 = Table('foo', m,
                   Column('x', Integer)
                   )
        self.assert_compile(
            schema.CreateIndex(Index("bar", t1.c.x > 5)),
            "CREATE INDEX bar ON foo (x > 5)"
        )

    def test_drop_index_w_schema(self):
        m = MetaData()
        t1 = Table('foo', m,
                   Column('x', Integer),
                   schema='bar'
                   )
        self.assert_compile(
            schema.DropIndex(Index("idx_foo", t1.c.x)),
            "DROP INDEX idx_foo ON bar.foo"
        )

    def test_index_extra_include_1(self):
        metadata = MetaData()
        tbl = Table(
            'test', metadata,
            Column('x', Integer), Column('y', Integer), Column('z', Integer))
        idx = Index("foo", tbl.c.x, mssql_include=['y'])
        self.assert_compile(schema.CreateIndex(idx),
                            "CREATE INDEX foo ON test (x) INCLUDE (y)"
                            )

    def test_index_extra_include_2(self):
        metadata = MetaData()
        tbl = Table(
            'test', metadata,
            Column('x', Integer), Column('y', Integer), Column('z', Integer))
        idx = Index("foo", tbl.c.x, mssql_include=[tbl.c.y])
        self.assert_compile(schema.CreateIndex(idx),
                            "CREATE INDEX foo ON test (x) INCLUDE (y)"
                            )


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
