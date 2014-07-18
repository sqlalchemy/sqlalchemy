#! coding:utf-8

from sqlalchemy import Column, Integer, MetaData, String, Table,\
    bindparam, exc, func, insert, select
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.engine import default
from sqlalchemy.testing import AssertsCompiledSQL,\
    assert_raises_message, fixtures


class _InsertTestBase(object):

    @classmethod
    def define_tables(cls, metadata):
        Table('mytable', metadata,
              Column('myid', Integer),
              Column('name', String(30)),
              Column('description', String(30)))
        Table('myothertable', metadata,
              Column('otherid', Integer),
              Column('othername', String(30)))


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

    def test_insert_from_select_select_no_defaults(self):
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
            "INSERT INTO sometable (id) SELECT mytable.myid "
            "FROM mytable WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"}
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

        name = 'name'
        description = 'desc'
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
            "UNION SELECT name, desc"
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

        checkparams = {
            'id_0': 1,
            'id_1': 2,
            'id_2': 3,
            'data_0': 'data1',
            'data_1': 'data2',
            'data_2': 'data3',
            'foo_0': 'plainfoo',
            'foo_2': 'otherfoo',
        }

        # note the effect here is that the first set of params
        # takes effect for the rest of them, when one is absent
        self.assert_compile(
            table.insert().values(values),
            'INSERT INTO sometable (id, data, foo) VALUES '
            '(%(id_0)s, %(data_0)s, %(foo_0)s), '
            '(%(id_1)s, %(data_1)s, %(foo_0)s), '
            '(%(id_2)s, %(data_2)s, %(foo_2)s)',
            checkparams=checkparams,
            dialect=postgresql.dialect())
