#! coding:utf-8

from sqlalchemy import Column, Integer, MetaData, String, Table,\
    bindparam, exc, func, insert, select, column, text, table
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.engine import default
from sqlalchemy.testing import AssertsCompiledSQL,\
    assert_raises_message, fixtures, eq_
from sqlalchemy.sql import crud

class _InsertTestBase(object):

    @classmethod
    def define_tables(cls, metadata):
        Table('mytable', metadata,
              Column('myid', Integer),
              Column('name', String(30)),
              Column('description', String(30)))
        Table('myothertable', metadata,
              Column('otherid', Integer, primary_key=True),
              Column('othername', String(30)))
        Table('table_w_defaults', metadata,
              Column('id', Integer, primary_key=True),
              Column('x', Integer, default=10),
              Column('y', Integer, server_default=text('5')),
              Column('z', Integer, default=lambda: 10)
            )


class InsertTest(_InsertTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_generic_insert_bind_params_all_columns(self):
        table1 = self.tables.mytable

        self.assert_compile(insert(table1),
                            'INSERT INTO mytable (myid, name, description) '
                            'VALUES (:myid, :name, :description)')

    def test_insert_with_values_dict(self):
        table1 = self.tables.mytable

        checkparams = {
            'myid': 3,
            'name': 'jack'
        }

        self.assert_compile(
            insert(
                table1,
                dict(
                    myid=3,
                    name='jack')),
            'INSERT INTO mytable (myid, name) VALUES (:myid, :name)',
            checkparams=checkparams)

    def test_unconsumed_names_kwargs(self):
        t = table("t", column("x"), column("y"))
        assert_raises_message(
            exc.CompileError,
            "Unconsumed column names: z",
            t.insert().values(x=5, z=5).compile,
        )

    def test_bindparam_name_no_consume_error(self):
        t = table("t", column("x"), column("y"))
        # bindparam names don't get counted
        i = t.insert().values(x=3 + bindparam('x2'))
        self.assert_compile(
            i,
            "INSERT INTO t (x) VALUES ((:param_1 + :x2))"
        )

        # even if in the params list
        i = t.insert().values(x=3 + bindparam('x2'))
        self.assert_compile(
            i,
            "INSERT INTO t (x) VALUES ((:param_1 + :x2))",
            params={"x2": 1}
        )

    def test_unconsumed_names_values_dict(self):
        table1 = self.tables.mytable

        checkparams = {
            'myid': 3,
            'name': 'jack',
            'unknowncol': 'oops'
        }

        stmt = insert(table1, values=checkparams)
        assert_raises_message(
            exc.CompileError,
            'Unconsumed column names: unknowncol',
            stmt.compile,
            dialect=postgresql.dialect()
        )

    def test_unconsumed_names_multi_values_dict(self):
        table1 = self.tables.mytable

        checkparams = [{
            'myid': 3,
            'name': 'jack',
            'unknowncol': 'oops'
        }, {
            'myid': 4,
            'name': 'someone',
            'unknowncol': 'oops'
        }]

        stmt = insert(table1, values=checkparams)
        assert_raises_message(
            exc.CompileError,
            'Unconsumed column names: unknowncol',
            stmt.compile,
            dialect=postgresql.dialect()
        )

    def test_insert_with_values_tuple(self):
        table1 = self.tables.mytable

        checkparams = {
            'myid': 3,
            'name': 'jack',
            'description': 'mydescription'
        }

        self.assert_compile(insert(table1, (3, 'jack', 'mydescription')),
                            'INSERT INTO mytable (myid, name, description) '
                            'VALUES (:myid, :name, :description)',
                            checkparams=checkparams)

    def test_insert_with_values_func(self):
        table1 = self.tables.mytable

        self.assert_compile(insert(table1, values=dict(myid=func.lala())),
                            'INSERT INTO mytable (myid) VALUES (lala())')

    def test_insert_with_user_supplied_bind_params(self):
        table1 = self.tables.mytable

        values = {
            table1.c.myid: bindparam('userid'),
            table1.c.name: bindparam('username')
        }

        self.assert_compile(
            insert(
                table1,
                values),
            'INSERT INTO mytable (myid, name) VALUES (:userid, :username)')

    def test_insert_values(self):
        table1 = self.tables.mytable

        values1 = {table1.c.myid: bindparam('userid')}
        values2 = {table1.c.name: bindparam('username')}

        self.assert_compile(
            insert(
                table1,
                values=values1).values(values2),
            'INSERT INTO mytable (myid, name) VALUES (:userid, :username)')

    def test_prefix_with(self):
        table1 = self.tables.mytable

        stmt = table1.insert().\
            prefix_with('A', 'B', dialect='mysql').\
            prefix_with('C', 'D')

        self.assert_compile(
            stmt,
            'INSERT C D INTO mytable (myid, name, description) '
            'VALUES (:myid, :name, :description)')

        self.assert_compile(
            stmt,
            'INSERT A B C D INTO mytable (myid, name, description) '
            'VALUES (%s, %s, %s)',
            dialect=mysql.dialect())

    def test_inline_default(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('foo', Integer, default=func.foobar()))

        self.assert_compile(table.insert(values={}, inline=True),
                            'INSERT INTO sometable (foo) VALUES (foobar())')

        self.assert_compile(
            table.insert(
                inline=True),
            'INSERT INTO sometable (foo) VALUES (foobar())',
            params={})

    def test_insert_returning_not_in_default(self):
        table1 = self.tables.mytable

        stmt = table1.insert().returning(table1.c.myid)
        assert_raises_message(
            exc.CompileError,
            "RETURNING is not supported by this dialect's statement compiler.",
            stmt.compile,
            dialect=default.DefaultDialect()
        )

    def test_insert_from_select_returning(self):
        table1 = self.tables.mytable
        sel = select([table1.c.myid, table1.c.name]).where(
            table1.c.name == 'foo')
        ins = self.tables.myothertable.insert().\
            from_select(("otherid", "othername"), sel).returning(
                self.tables.myothertable.c.otherid
            )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = %(name_1)s RETURNING myothertable.otherid",
            checkparams={"name_1": "foo"},
            dialect="postgresql"
        )

    def test_insert_from_select_select(self):
        table1 = self.tables.mytable
        sel = select([table1.c.myid, table1.c.name]).where(
            table1.c.name == 'foo')
        ins = self.tables.myothertable.insert().\
            from_select(("otherid", "othername"), sel)
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"}
        )

    def test_insert_from_select_cte_one(self):
        table1 = self.tables.mytable

        cte = select([table1.c.name]).where(table1.c.name == 'bar').cte()

        sel = select([table1.c.myid, table1.c.name]).where(
            table1.c.name == cte.c.name)

        ins = self.tables.myothertable.insert().\
            from_select(("otherid", "othername"), sel)
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) WITH anon_1 AS "
            "(SELECT mytable.name AS name FROM mytable "
            "WHERE mytable.name = :name_1) "
            "SELECT mytable.myid, mytable.name FROM mytable, anon_1 "
            "WHERE mytable.name = anon_1.name",
            checkparams={"name_1": "bar"}
        )

    def test_insert_from_select_cte_two(self):
        table1 = self.tables.mytable

        cte = table1.select().cte("c")
        stmt = cte.select()
        ins = table1.insert().from_select(table1.c, stmt)

        self.assert_compile(
            ins,
            "INSERT INTO mytable (myid, name, description) "
            "WITH c AS (SELECT mytable.myid AS myid, mytable.name AS name, "
            "mytable.description AS description FROM mytable) "
            "SELECT c.myid, c.name, c.description FROM c"
        )

    def test_insert_from_select_select_alt_ordering(self):
        table1 = self.tables.mytable
        sel = select([table1.c.name, table1.c.myid]).where(
            table1.c.name == 'foo')
        ins = self.tables.myothertable.insert().\
            from_select(("othername", "otherid"), sel)
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername, otherid) "
            "SELECT mytable.name, mytable.myid FROM mytable "
            "WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"}
        )

    def test_insert_from_select_no_defaults(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('foo', Integer, default=func.foobar()))
        table1 = self.tables.mytable
        sel = select([table1.c.myid]).where(table1.c.name == 'foo')
        ins = table.insert().\
            from_select(["id"], sel, include_defaults=False)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id) SELECT mytable.myid "
            "FROM mytable WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"}
        )

    def test_insert_from_select_with_sql_defaults(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('foo', Integer, default=func.foobar()))
        table1 = self.tables.mytable
        sel = select([table1.c.myid]).where(table1.c.name == 'foo')
        ins = table.insert().\
            from_select(["id"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo) SELECT "
            "mytable.myid, foobar() AS foobar_1 "
            "FROM mytable WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"}
        )

    def test_insert_from_select_with_python_defaults(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('foo', Integer, default=12))
        table1 = self.tables.mytable
        sel = select([table1.c.myid]).where(table1.c.name == 'foo')
        ins = table.insert().\
            from_select(["id"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo) SELECT "
            "mytable.myid, :foo AS anon_1 "
            "FROM mytable WHERE mytable.name = :name_1",
            # value filled in at execution time
            checkparams={"name_1": "foo", "foo": None}
        )

    def test_insert_from_select_override_defaults(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('foo', Integer, default=12))
        table1 = self.tables.mytable
        sel = select(
            [table1.c.myid, table1.c.myid.label('q')]).where(
            table1.c.name == 'foo')
        ins = table.insert().\
            from_select(["id", "foo"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo) SELECT "
            "mytable.myid, mytable.myid AS q "
            "FROM mytable WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"}
        )

    def test_insert_from_select_fn_defaults(self):
        metadata = MetaData()

        def foo(ctx):
            return 12

        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('foo', Integer, default=foo))
        table1 = self.tables.mytable
        sel = select(
            [table1.c.myid]).where(
            table1.c.name == 'foo')
        ins = table.insert().\
            from_select(["id"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo) SELECT "
            "mytable.myid, :foo AS anon_1 "
            "FROM mytable WHERE mytable.name = :name_1",
            # value filled in at execution time
            checkparams={"name_1": "foo", "foo": None}
        )

    def test_insert_from_select_dont_mutate_raw_columns(self):
        # test [ticket:3603]
        from sqlalchemy import table
        table_ = table(
            'mytable',
            Column('foo', String),
            Column('bar', String, default='baz'),
        )

        stmt = select([table_.c.foo])
        insert = table_.insert().from_select(['foo'], stmt)

        self.assert_compile(stmt, "SELECT mytable.foo FROM mytable")
        self.assert_compile(
            insert,
            "INSERT INTO mytable (foo, bar) "
            "SELECT mytable.foo, :bar AS anon_1 FROM mytable"
        )
        self.assert_compile(stmt, "SELECT mytable.foo FROM mytable")
        self.assert_compile(
            insert,
            "INSERT INTO mytable (foo, bar) "
            "SELECT mytable.foo, :bar AS anon_1 FROM mytable"
        )


    def test_insert_mix_select_values_exception(self):
        table1 = self.tables.mytable
        sel = select([table1.c.myid, table1.c.name]).where(
            table1.c.name == 'foo')
        ins = self.tables.myothertable.insert().\
            from_select(("otherid", "othername"), sel)
        assert_raises_message(
            exc.InvalidRequestError,
            "This construct already inserts from a SELECT",
            ins.values, othername="5"
        )

    def test_insert_mix_values_select_exception(self):
        table1 = self.tables.mytable
        sel = select([table1.c.myid, table1.c.name]).where(
            table1.c.name == 'foo')
        ins = self.tables.myothertable.insert().values(othername="5")
        assert_raises_message(
            exc.InvalidRequestError,
            "This construct already inserts value expressions",
            ins.from_select, ("otherid", "othername"), sel
        )

    def test_insert_from_select_table(self):
        table1 = self.tables.mytable
        ins = self.tables.myothertable.insert().\
            from_select(("otherid", "othername"), table1)
        # note we aren't checking the number of columns right now
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable",
            checkparams={}
        )

    def test_insert_from_select_union(self):
        mytable = self.tables.mytable

        name = column('name')
        description = column('desc')
        sel = select(
            [name, mytable.c.description],
        ).union(
            select([name, description])
        )
        ins = mytable.insert().\
            from_select(
                [mytable.c.name, mytable.c.description], sel)
        self.assert_compile(
            ins,
            "INSERT INTO mytable (name, description) "
            "SELECT name, mytable.description FROM mytable "
            'UNION SELECT name, "desc"'
        )

    def test_insert_from_select_col_values(self):
        table1 = self.tables.mytable
        table2 = self.tables.myothertable
        sel = select([table1.c.myid, table1.c.name]).where(
            table1.c.name == 'foo')
        ins = table2.insert().\
            from_select((table2.c.otherid, table2.c.othername), sel)
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"}
        )


class InsertImplicitReturningTest(
        _InsertTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = postgresql.dialect(implicit_returning=True)

    def test_insert_select(self):
        table1 = self.tables.mytable
        sel = select([table1.c.myid, table1.c.name]).where(
            table1.c.name == 'foo')
        ins = self.tables.myothertable.insert().\
            from_select(("otherid", "othername"), sel)
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = %(name_1)s",
            checkparams={"name_1": "foo"}
        )

    def test_insert_select_return_defaults(self):
        table1 = self.tables.mytable
        sel = select([table1.c.myid, table1.c.name]).where(
            table1.c.name == 'foo')
        ins = self.tables.myothertable.insert().\
            from_select(("otherid", "othername"), sel).\
            return_defaults(self.tables.myothertable.c.otherid)
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = %(name_1)s",
            checkparams={"name_1": "foo"}
        )

    def test_insert_multiple_values(self):
        ins = self.tables.myothertable.insert().values([
            {"othername": "foo"},
            {"othername": "bar"},
        ])
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) "
            "VALUES (%(othername_0)s), "
            "(%(othername_1)s)",
            checkparams={
                'othername_1': 'bar',
                'othername_0': 'foo'}
        )

    def test_insert_multiple_values_return_defaults(self):
        # TODO: not sure if this should raise an
        # error or what
        ins = self.tables.myothertable.insert().values([
            {"othername": "foo"},
            {"othername": "bar"},
        ]).return_defaults(self.tables.myothertable.c.otherid)
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) "
            "VALUES (%(othername_0)s), "
            "(%(othername_1)s)",
            checkparams={
                'othername_1': 'bar',
                'othername_0': 'foo'}
        )

    def test_insert_single_list_values(self):
        ins = self.tables.myothertable.insert().values([
            {"othername": "foo"},
        ])
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) "
            "VALUES (%(othername_0)s)",
            checkparams={'othername_0': 'foo'}
        )

    def test_insert_single_element_values(self):
        ins = self.tables.myothertable.insert().values(
            {"othername": "foo"},
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) "
            "VALUES (%(othername)s) RETURNING myothertable.otherid",
            checkparams={'othername': 'foo'}
        )


class EmptyTest(_InsertTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_empty_insert_default(self):
        table1 = self.tables.mytable

        stmt = table1.insert().values({})  # hide from 2to3
        self.assert_compile(stmt, 'INSERT INTO mytable () VALUES ()')

    def test_supports_empty_insert_true(self):
        table1 = self.tables.mytable

        dialect = default.DefaultDialect()
        dialect.supports_empty_insert = dialect.supports_default_values = True

        stmt = table1.insert().values({})  # hide from 2to3
        self.assert_compile(stmt,
                            'INSERT INTO mytable DEFAULT VALUES',
                            dialect=dialect)

    def test_supports_empty_insert_false(self):
        table1 = self.tables.mytable

        dialect = default.DefaultDialect()
        dialect.supports_empty_insert = dialect.supports_default_values = False

        stmt = table1.insert().values({})  # hide from 2to3
        assert_raises_message(
            exc.CompileError,
            "The 'default' dialect with current database version "
            "settings does not support empty inserts.",
            stmt.compile,
            dialect=dialect)

    def _test_insert_with_empty_collection_values(self, collection):
        table1 = self.tables.mytable

        ins = table1.insert().values(collection)

        self.assert_compile(ins,
                            'INSERT INTO mytable () VALUES ()',
                            checkparams={})

        # empty dict populates on next values call
        self.assert_compile(ins.values(myid=3),
                            'INSERT INTO mytable (myid) VALUES (:myid)',
                            checkparams={'myid': 3})

    def test_insert_with_empty_list_values(self):
        self._test_insert_with_empty_collection_values([])

    def test_insert_with_empty_dict_values(self):
        self._test_insert_with_empty_collection_values({})

    def test_insert_with_empty_tuple_values(self):
        self._test_insert_with_empty_collection_values(())


class MultirowTest(_InsertTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = 'default'

    def test_not_supported(self):
        table1 = self.tables.mytable

        dialect = default.DefaultDialect()
        stmt = table1.insert().values([{'myid': 1}, {'myid': 2}])
        assert_raises_message(
            exc.CompileError,
            "The 'default' dialect with current database version settings "
            "does not support in-place multirow inserts.",
            stmt.compile, dialect=dialect)

    def test_named(self):
        table1 = self.tables.mytable

        values = [
            {'myid': 1, 'name': 'a', 'description': 'b'},
            {'myid': 2, 'name': 'c', 'description': 'd'},
            {'myid': 3, 'name': 'e', 'description': 'f'}
        ]

        checkparams = {
            'myid_0': 1,
            'myid_1': 2,
            'myid_2': 3,
            'name_0': 'a',
            'name_1': 'c',
            'name_2': 'e',
            'description_0': 'b',
            'description_1': 'd',
            'description_2': 'f',
        }

        dialect = default.DefaultDialect()
        dialect.supports_multivalues_insert = True

        self.assert_compile(
            table1.insert().values(values),
            'INSERT INTO mytable (myid, name, description) VALUES '
            '(:myid_0, :name_0, :description_0), '
            '(:myid_1, :name_1, :description_1), '
            '(:myid_2, :name_2, :description_2)',
            checkparams=checkparams,
            dialect=dialect)

    def test_positional(self):
        table1 = self.tables.mytable

        values = [
            {'myid': 1, 'name': 'a', 'description': 'b'},
            {'myid': 2, 'name': 'c', 'description': 'd'},
            {'myid': 3, 'name': 'e', 'description': 'f'}
        ]

        checkpositional = (1, 'a', 'b', 2, 'c', 'd', 3, 'e', 'f')

        dialect = default.DefaultDialect()
        dialect.supports_multivalues_insert = True
        dialect.paramstyle = 'format'
        dialect.positional = True

        self.assert_compile(
            table1.insert().values(values),
            'INSERT INTO mytable (myid, name, description) VALUES '
            '(%s, %s, %s), (%s, %s, %s), (%s, %s, %s)',
            checkpositional=checkpositional,
            dialect=dialect)

    def test_positional_w_defaults(self):
        table1 = self.tables.table_w_defaults

        values = [
            {'id': 1},
            {'id': 2},
            {'id': 3}
        ]

        checkpositional = (1, None, None, 2, None, None, 3, None, None)

        dialect = default.DefaultDialect()
        dialect.supports_multivalues_insert = True
        dialect.paramstyle = 'format'
        dialect.positional = True

        self.assert_compile(
            table1.insert().values(values),
            "INSERT INTO table_w_defaults (id, x, z) VALUES "
            "(%s, %s, %s), (%s, %s, %s), (%s, %s, %s)",
            checkpositional=checkpositional,
            check_prefetch=[
                table1.c.x, table1.c.z,
                crud._multiparam_column(table1.c.x, 0),
                crud._multiparam_column(table1.c.z, 0),
                crud._multiparam_column(table1.c.x, 1),
                crud._multiparam_column(table1.c.z, 1)
            ],
            dialect=dialect)

    def test_inline_default(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('data', String),
                      Column('foo', Integer, default=func.foobar()))

        values = [
            {'id': 1, 'data': 'data1'},
            {'id': 2, 'data': 'data2', 'foo': 'plainfoo'},
            {'id': 3, 'data': 'data3'},
        ]

        checkparams = {
            'id_0': 1,
            'id_1': 2,
            'id_2': 3,
            'data_0': 'data1',
            'data_1': 'data2',
            'data_2': 'data3',
            'foo_1': 'plainfoo',
        }

        self.assert_compile(
            table.insert().values(values),
            'INSERT INTO sometable (id, data, foo) VALUES '
            '(%(id_0)s, %(data_0)s, foobar()), '
            '(%(id_1)s, %(data_1)s, %(foo_1)s), '
            '(%(id_2)s, %(data_2)s, foobar())',
            checkparams=checkparams,
            dialect=postgresql.dialect())

    def test_python_scalar_default(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('data', String),
                      Column('foo', Integer, default=10))

        values = [
            {'id': 1, 'data': 'data1'},
            {'id': 2, 'data': 'data2', 'foo': 15},
            {'id': 3, 'data': 'data3'},
        ]

        checkparams = {
            'id_0': 1,
            'id_1': 2,
            'id_2': 3,
            'data_0': 'data1',
            'data_1': 'data2',
            'data_2': 'data3',
            'foo': None,  # evaluated later
            'foo_1': 15,
            'foo_2': None  # evaluated later
        }

        stmt = table.insert().values(values)

        eq_(
            dict([
                (k, v.type._type_affinity)
                for (k, v) in
                stmt.compile(dialect=postgresql.dialect()).binds.items()]),
            {
                'foo': Integer, 'data_2': String, 'id_0': Integer,
                'id_2': Integer, 'foo_1': Integer, 'data_1': String,
                'id_1': Integer, 'foo_2': Integer, 'data_0': String}
        )

        self.assert_compile(
            stmt,
            'INSERT INTO sometable (id, data, foo) VALUES '
            '(%(id_0)s, %(data_0)s, %(foo)s), '
            '(%(id_1)s, %(data_1)s, %(foo_1)s), '
            '(%(id_2)s, %(data_2)s, %(foo_2)s)',
            checkparams=checkparams,
            dialect=postgresql.dialect())

    def test_python_fn_default(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('data', String),
                      Column('foo', Integer, default=lambda: 10))

        values = [
            {'id': 1, 'data': 'data1'},
            {'id': 2, 'data': 'data2', 'foo': 15},
            {'id': 3, 'data': 'data3'},
        ]

        checkparams = {
            'id_0': 1,
            'id_1': 2,
            'id_2': 3,
            'data_0': 'data1',
            'data_1': 'data2',
            'data_2': 'data3',
            'foo': None,  # evaluated later
            'foo_1': 15,
            'foo_2': None,  # evaluated later
        }

        stmt = table.insert().values(values)
        eq_(
            dict([
                (k, v.type._type_affinity)
                for (k, v) in
                stmt.compile(dialect=postgresql.dialect()).binds.items()]),
            {
                'foo': Integer, 'data_2': String, 'id_0': Integer,
                'id_2': Integer, 'foo_1': Integer, 'data_1': String,
                'id_1': Integer, 'foo_2': Integer, 'data_0': String}
        )

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (id, data, foo) VALUES "
            "(%(id_0)s, %(data_0)s, %(foo)s), "
            "(%(id_1)s, %(data_1)s, %(foo_1)s), "
            "(%(id_2)s, %(data_2)s, %(foo_2)s)",
            checkparams=checkparams,
            dialect=postgresql.dialect())

    def test_sql_functions(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('data', String),
                      Column('foo', Integer))

        values = [
            {"id": 1, "data": "foo", "foo": func.foob()},
            {"id": 2, "data": "bar", "foo": func.foob()},
            {"id": 3, "data": "bar", "foo": func.bar()},
            {"id": 4, "data": "bar", "foo": 15},
            {"id": 5, "data": "bar", "foo": func.foob()},
        ]
        checkparams = {
            'id_0': 1,
            'data_0': 'foo',

            'id_1': 2,
            'data_1': 'bar',

            'id_2': 3,
            'data_2': 'bar',

            'id_3': 4,
            'data_3': 'bar',
            'foo_3': 15,

            'id_4': 5,
            'data_4': 'bar'
        }

        self.assert_compile(
            table.insert().values(values),
            "INSERT INTO sometable (id, data, foo) VALUES "
            "(%(id_0)s, %(data_0)s, foob()), "
            "(%(id_1)s, %(data_1)s, foob()), "
            "(%(id_2)s, %(data_2)s, bar()), "
            "(%(id_3)s, %(data_3)s, %(foo_3)s), "
            "(%(id_4)s, %(data_4)s, foob())",
            checkparams=checkparams,
            dialect=postgresql.dialect())

    def test_server_default(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('data', String),
                      Column('foo', Integer, server_default=func.foobar()))

        values = [
            {'id': 1, 'data': 'data1'},
            {'id': 2, 'data': 'data2', 'foo': 'plainfoo'},
            {'id': 3, 'data': 'data3'},
        ]

        checkparams = {
            'id_0': 1,
            'id_1': 2,
            'id_2': 3,
            'data_0': 'data1',
            'data_1': 'data2',
            'data_2': 'data3',
        }

        self.assert_compile(
            table.insert().values(values),
            'INSERT INTO sometable (id, data) VALUES '
            '(%(id_0)s, %(data_0)s), '
            '(%(id_1)s, %(data_1)s), '
            '(%(id_2)s, %(data_2)s)',
            checkparams=checkparams,
            dialect=postgresql.dialect())

    def test_server_default_absent_value(self):
        metadata = MetaData()
        table = Table('sometable', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('data', String),
                      Column('foo', Integer, server_default=func.foobar()))

        values = [
            {'id': 1, 'data': 'data1', 'foo': 'plainfoo'},
            {'id': 2, 'data': 'data2'},
            {'id': 3, 'data': 'data3', 'foo': 'otherfoo'},
        ]

        assert_raises_message(
            exc.CompileError,
            "INSERT value for column sometable.foo is explicitly rendered "
            "as a boundparameter in the VALUES clause; a Python-side value or "
            "SQL expression is required",
            table.insert().values(values).compile
        )
