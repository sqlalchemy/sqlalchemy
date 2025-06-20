from __future__ import annotations

from typing import Tuple

from sqlalchemy import bindparam
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Sequence
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.engine import default
from sqlalchemy.sql import crud
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import config
from sqlalchemy.testing import eq_
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import expect_warnings
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.provision import normalize_sequence


class ORMExpr:
    def __init__(self, col):
        self.col = col

    def __clause_element__(self):
        return self.col


class _InsertTestBase:
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "mytable",
            metadata,
            Column("myid", Integer),
            Column("name", String(30)),
            Column("description", String(30)),
        )
        Table(
            "myothertable",
            metadata,
            Column("otherid", Integer, primary_key=True),
            Column("othername", String(30)),
        )
        Table(
            "table_w_defaults",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer, default=10),
            Column("y", Integer, server_default=text("5")),
            Column("z", Integer, default=lambda: 10),
        )


class InsertTest(_InsertTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = "default_enhanced"

    @testing.combinations(
        ((), ("z",), ()),
        (("x",), (), ()),
        (("x",), ("y",), ("x", "y")),
        (("x", "y"), ("y",), ("x", "y")),
    )
    def test_return_defaults_generative(
        self,
        initial_keys: Tuple[str, ...],
        second_keys: Tuple[str, ...],
        expected_keys: Tuple[str, ...],
    ):
        t = table("foo", column("x"), column("y"), column("z"))

        initial_cols = tuple(t.c[initial_keys])
        second_cols = tuple(t.c[second_keys])
        expected = set(t.c[expected_keys])

        stmt = t.insert().return_defaults(*initial_cols)
        eq_(stmt._return_defaults_columns, initial_cols)
        stmt = stmt.return_defaults(*second_cols)
        assert isinstance(stmt._return_defaults_columns, tuple)
        eq_(set(stmt._return_defaults_columns), expected)

    @testing.variation("add_values", ["before", "after"])
    @testing.variation("multi_values", [True, False])
    @testing.variation("sort_by_parameter_order", [True, False])
    def test_sort_by_parameter_ordering_parameter_no_multi_values(
        self, add_values, multi_values, sort_by_parameter_order
    ):
        t = table("foo", column("x"), column("y"), column("z"))
        stmt = insert(t)

        if add_values.before:
            if multi_values:
                stmt = stmt.values([{"y": 6}, {"y": 7}])
            else:
                stmt = stmt.values(y=6)

        stmt = stmt.returning(
            t.c.x, sort_by_parameter_order=bool(sort_by_parameter_order)
        )

        if add_values.after:
            if multi_values:
                stmt = stmt.values([{"y": 6}, {"y": 7}])
            else:
                stmt = stmt.values(y=6)

        if multi_values:
            if sort_by_parameter_order:
                with expect_raises_message(
                    exc.CompileError,
                    "RETURNING cannot be determinstically sorted "
                    "when using an INSERT",
                ):
                    stmt.compile()
            else:
                self.assert_compile(
                    stmt,
                    "INSERT INTO foo (y) VALUES (:y_m0), (:y_m1) "
                    "RETURNING foo.x",
                )
        else:
            self.assert_compile(
                stmt,
                "INSERT INTO foo (y) VALUES (:y) RETURNING foo.x",
            )

    def test_binds_that_match_columns(self):
        """test bind params named after column names
        replace the normal SET/VALUES generation.

        See also test_compiler.py::CrudParamOverlapTest

        """

        t = table("foo", column("x"), column("y"))

        i = t.insert().values(x=3 + bindparam("x"))
        self.assert_compile(i, "INSERT INTO foo (x) VALUES ((:param_1 + :x))")
        self.assert_compile(
            i,
            "INSERT INTO foo (x, y) VALUES ((:param_1 + :x), :y)",
            params={"x": 1, "y": 2},
        )

        i = t.insert().values(x=bindparam("y"))
        self.assert_compile(i, "INSERT INTO foo (x) VALUES (:y)")

        i = t.insert().values(x=bindparam("y"), y=5)
        assert_raises(exc.CompileError, i.compile)

        i = t.insert().values(x=3 + bindparam("y"), y=5)
        assert_raises(exc.CompileError, i.compile)

        i = t.insert().values(x=3 + bindparam("x2"))
        self.assert_compile(i, "INSERT INTO foo (x) VALUES ((:param_1 + :x2))")
        self.assert_compile(
            i, "INSERT INTO foo (x) VALUES ((:param_1 + :x2))", params={}
        )
        self.assert_compile(
            i,
            "INSERT INTO foo (x, y) VALUES ((:param_1 + :x2), :y)",
            params={"x": 1, "y": 2},
        )
        self.assert_compile(
            i,
            "INSERT INTO foo (x, y) VALUES ((:param_1 + :x2), :y)",
            params={"x2": 1, "y": 2},
        )

    def test_insert_literal_binds(self):
        table1 = self.tables.mytable
        stmt = table1.insert().values(myid=3, name="jack")

        self.assert_compile(
            stmt,
            "INSERT INTO mytable (myid, name) VALUES (3, 'jack')",
            literal_binds=True,
        )

    def test_insert_literal_binds_sequence_notimplemented(self):
        table = Table(
            "x",
            MetaData(),
            Column(
                "y", Integer, normalize_sequence(config, Sequence("y_seq"))
            ),
        )
        dialect = default.DefaultDialect()
        dialect.supports_sequences = True

        stmt = table.insert().values(myid=3, name="jack")

        assert_raises(
            NotImplementedError,
            stmt.compile,
            compile_kwargs=dict(literal_binds=True),
            dialect=dialect,
        )

    def test_inline_defaults(self):
        m = MetaData()
        foo = Table("foo", m, Column("id", Integer))

        t = Table(
            "test",
            m,
            Column("col1", Integer, default=func.foo(1)),
            Column(
                "col2",
                Integer,
                default=select(func.coalesce(func.max(foo.c.id))),
            ),
        )

        self.assert_compile(
            t.insert().values({}),
            "INSERT INTO test (col1, col2) VALUES (foo(:foo_1), "
            "(SELECT coalesce(max(foo.id)) AS coalesce_1 FROM "
            "foo))",
        )

        self.assert_compile(
            t.insert().inline().values({}),
            "INSERT INTO test (col1, col2) VALUES (foo(:foo_1), "
            "(SELECT coalesce(max(foo.id)) AS coalesce_1 FROM "
            "foo))",
        )

    def test_generic_insert_bind_params_all_columns(self):
        table1 = self.tables.mytable

        self.assert_compile(
            insert(table1),
            "INSERT INTO mytable (myid, name, description) "
            "VALUES (:myid, :name, :description)",
        )

    def test_insert_with_values_dict(self):
        table1 = self.tables.mytable

        checkparams = {"myid": 3, "name": "jack"}

        self.assert_compile(
            insert(table1).values(myid=3, name="jack"),
            "INSERT INTO mytable (myid, name) VALUES (:myid, :name)",
            checkparams=checkparams,
        )

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
        i = t.insert().values(x=3 + bindparam("x2"))
        self.assert_compile(i, "INSERT INTO t (x) VALUES ((:param_1 + :x2))")

        # even if in the params list
        i = t.insert().values(x=3 + bindparam("x2"))
        self.assert_compile(
            i, "INSERT INTO t (x) VALUES ((:param_1 + :x2))", params={"x2": 1}
        )

    def test_unconsumed_names_values_dict(self):
        table1 = self.tables.mytable

        checkparams = {"myid": 3, "name": "jack", "unknowncol": "oops"}

        stmt = insert(table1).values(checkparams)
        assert_raises_message(
            exc.CompileError,
            "Unconsumed column names: unknowncol",
            stmt.compile,
            dialect=postgresql.dialect(),
        )

    def test_unconsumed_names_multi_values_dict(self):
        table1 = self.tables.mytable

        checkparams = [
            {"myid": 3, "name": "jack", "unknowncol": "oops"},
            {"myid": 4, "name": "someone", "unknowncol": "oops"},
        ]

        stmt = insert(table1).values(checkparams)
        assert_raises_message(
            exc.CompileError,
            "Unconsumed column names: unknowncol",
            stmt.compile,
            dialect=postgresql.dialect(),
        )

    def test_insert_with_values_tuple(self):
        table1 = self.tables.mytable

        checkparams = {
            "myid": 3,
            "name": "jack",
            "description": "mydescription",
        }

        self.assert_compile(
            insert(table1).values([3, "jack", "mydescription"]),
            "INSERT INTO mytable (myid, name, description) "
            "VALUES (:myid, :name, :description)",
            checkparams=checkparams,
        )

    def test_insert_with_values_func(self):
        table1 = self.tables.mytable

        self.assert_compile(
            insert(table1).values(myid=func.lala()),
            "INSERT INTO mytable (myid) VALUES (lala())",
        )

    def test_insert_with_user_supplied_bind_params(self):
        table1 = self.tables.mytable

        values = {
            table1.c.myid: bindparam("userid"),
            table1.c.name: bindparam("username"),
        }

        self.assert_compile(
            insert(table1).values(values),
            "INSERT INTO mytable (myid, name) VALUES (:userid, :username)",
        )

    def test_insert_values_multiple(self):
        table1 = self.tables.mytable

        values1 = {table1.c.myid: bindparam("userid")}
        values2 = {table1.c.name: bindparam("username")}

        self.assert_compile(
            insert(table1).values(values1).values(values2),
            "INSERT INTO mytable (myid, name) VALUES (:userid, :username)",
        )

    def test_prefix_with(self):
        table1 = self.tables.mytable

        stmt = (
            table1.insert()
            .prefix_with("A", "B", dialect="mysql")
            .prefix_with("C", "D")
        )

        self.assert_compile(
            stmt,
            "INSERT C D INTO mytable (myid, name, description) "
            "VALUES (:myid, :name, :description)",
        )

        self.assert_compile(
            stmt,
            "INSERT A B C D INTO mytable (myid, name, description) "
            "VALUES (%s, %s, %s)",
            dialect=mysql.dialect(),
        )

    def test_inline_default(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default=func.foobar()),
        )

        self.assert_compile(
            table.insert().values(),
            "INSERT INTO sometable (foo) VALUES (foobar())",
        )

        self.assert_compile(
            table.insert(),
            "INSERT INTO sometable (foo) VALUES (foobar())",
            params={},
        )

        self.assert_compile(
            table.insert().values().inline(),
            "INSERT INTO sometable (foo) VALUES (foobar())",
        )

        self.assert_compile(
            table.insert().inline(),
            "INSERT INTO sometable (foo) VALUES (foobar())",
            params={},
        )

    def test_insert_returning_not_in_default(self):
        table1 = self.tables.mytable

        stmt = table1.insert().returning(table1.c.myid)
        self.assert_compile(
            stmt,
            "INSERT INTO mytable (myid, name, description) "
            "VALUES (:myid, :name, :description) RETURNING mytable.myid",
            dialect=default.DefaultDialect(),
        )

    def test_insert_from_select_returning(self):
        table1 = self.tables.mytable
        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == "foo"
        )
        ins = (
            self.tables.myothertable.insert()
            .from_select(("otherid", "othername"), sel)
            .returning(self.tables.myothertable.c.otherid)
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = %(name_1)s RETURNING myothertable.otherid",
            checkparams={"name_1": "foo"},
            dialect="postgresql",
        )

    def test_insert_from_select_select(self):
        table1 = self.tables.mytable
        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == "foo"
        )
        ins = self.tables.myothertable.insert().from_select(
            ("otherid", "othername"), sel
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"},
        )

    def test_insert_from_select_seq(self):
        m = MetaData()

        t1 = Table(
            "t",
            m,
            Column(
                "id",
                Integer,
                normalize_sequence(config, Sequence("id_seq")),
                primary_key=True,
            ),
            Column("data", String),
        )

        stmt = t1.insert().from_select(("data",), select(t1.c.data))

        self.assert_compile(
            stmt,
            "INSERT INTO t (data, id) SELECT t.data, "
            "nextval('id_seq') AS next_value_1 FROM t",
            dialect=postgresql.dialect(),
        )

    def test_insert_seq_pk_multi_values(self):
        """test #6361"""

        m = MetaData()

        t1 = Table(
            "t",
            m,
            Column(
                "id",
                Integer,
                normalize_sequence(config, Sequence("id_seq")),
                primary_key=True,
            ),
            Column("data", String),
        )

        stmt = t1.insert().values(
            [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}]
        )

        self.assert_compile(
            stmt,
            "INSERT INTO t (id, data) VALUES (nextval('id_seq'), "
            "%(data_m0)s), (nextval('id_seq'), %(data_m1)s), "
            "(nextval('id_seq'), %(data_m2)s)",
            dialect=postgresql.dialect(),
        )

    def test_insert_seq_non_pk_multi_values(self):
        """test #6361"""

        m = MetaData()

        t1 = Table(
            "t",
            m,
            Column("id", Integer, primary_key=True),
            Column(
                "counter", normalize_sequence(config, Sequence("counter_seq"))
            ),
            Column("data", String),
        )

        stmt = t1.insert().values(
            [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}]
        )

        self.assert_compile(
            stmt,
            "INSERT INTO t (counter, data) VALUES (nextval('counter_seq'), "
            "%(data_m0)s), (nextval('counter_seq'), %(data_m1)s), "
            "(nextval('counter_seq'), %(data_m2)s)",
            dialect=postgresql.dialect(),
        )

    @testing.variation("paramstyle", ["pg", "qmark", "numeric", "dollar"])
    def test_heterogeneous_multi_values(self, paramstyle):
        """for #6047, originally I thought we'd take any insert().values()
        and be able to convert it to a "many" style execution that we can
        cache.

        however, this test shows that we cannot, at least not in the
        general case, because SQL expressions are not guaranteed to be in
        the same position each time, therefore each ``VALUES`` clause is not
        of the same structure.

        """

        m = MetaData()

        t1 = Table(
            "t",
            m,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
            Column("y", Integer),
            Column("z", Integer),
        )

        stmt = t1.insert().values(
            [
                {"x": 1, "y": func.sum(1, 2), "z": 2},
                {"x": func.sum(1, 2), "y": 2, "z": 3},
                {"x": func.sum(1, 2), "y": 2, "z": func.foo(10)},
            ]
        )

        pos_par = (
            1,
            1,
            2,
            2,
            1,
            2,
            2,
            3,
            1,
            2,
            2,
            10,
        )

        # SQL expressions in the params at arbitrary locations means
        # we have to scan them at compile time, and the shape of the bound
        # parameters is not predictable.   so for #6047 where I originally
        # thought all of values() could be rewritten, this makes it not
        # really worth it.
        if paramstyle.pg:
            self.assert_compile(
                stmt,
                "INSERT INTO t (x, y, z) VALUES "
                "(%(x_m0)s, sum(%(sum_1)s, %(sum_2)s), %(z_m0)s), "
                "(sum(%(sum_3)s, %(sum_4)s), %(y_m1)s, %(z_m1)s), "
                "(sum(%(sum_5)s, %(sum_6)s), %(y_m2)s, foo(%(foo_1)s))",
                checkparams={
                    "x_m0": 1,
                    "sum_1": 1,
                    "sum_2": 2,
                    "z_m0": 2,
                    "sum_3": 1,
                    "sum_4": 2,
                    "y_m1": 2,
                    "z_m1": 3,
                    "sum_5": 1,
                    "sum_6": 2,
                    "y_m2": 2,
                    "foo_1": 10,
                },
                dialect=postgresql.dialect(),
            )
        elif paramstyle.qmark:
            self.assert_compile(
                stmt,
                "INSERT INTO t (x, y, z) VALUES "
                "(?, sum(?, ?), ?), "
                "(sum(?, ?), ?, ?), "
                "(sum(?, ?), ?, foo(?))",
                checkpositional=pos_par,
                dialect=sqlite.dialect(),
            )
        elif paramstyle.numeric:
            self.assert_compile(
                stmt,
                "INSERT INTO t (x, y, z) VALUES "
                "(:1, sum(:2, :3), :4), "
                "(sum(:5, :6), :7, :8), "
                "(sum(:9, :10), :11, foo(:12))",
                checkpositional=pos_par,
                dialect=sqlite.dialect(paramstyle="numeric"),
            )
        elif paramstyle.dollar:
            self.assert_compile(
                stmt,
                "INSERT INTO t (x, y, z) VALUES "
                "($1, sum($2, $3), $4), "
                "(sum($5, $6), $7, $8), "
                "(sum($9, $10), $11, foo($12))",
                checkpositional=pos_par,
                dialect=sqlite.dialect(paramstyle="numeric_dollar"),
            )
        else:
            paramstyle.fail()

    def test_insert_seq_pk_multi_values_seq_not_supported(self):
        m = MetaData()

        t1 = Table(
            "t",
            m,
            Column(
                "id",
                Integer,
                normalize_sequence(config, Sequence("id_seq")),
                primary_key=True,
            ),
            Column("data", String),
        )

        stmt = t1.insert().values(
            [{"data": "d1"}, {"data": "d2"}, {"data": "d3"}]
        )

        self.assert_compile(
            stmt,
            "INSERT INTO t (data) VALUES (?), (?), (?)",
            dialect=sqlite.dialect(),
        )

    def test_insert_from_select_cte_one(self):
        table1 = self.tables.mytable

        cte = select(table1.c.name).where(table1.c.name == "bar").cte()

        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == cte.c.name
        )

        ins = self.tables.myothertable.insert().from_select(
            ("otherid", "othername"), sel
        )
        self.assert_compile(
            ins,
            "WITH anon_1 AS "
            "(SELECT mytable.name AS name FROM mytable "
            "WHERE mytable.name = :name_1) "
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable, anon_1 "
            "WHERE mytable.name = anon_1.name",
            checkparams={"name_1": "bar"},
        )

    def test_insert_from_select_cte_follows_insert_one(self):
        dialect = default.DefaultDialect()
        dialect.cte_follows_insert = True

        table1 = self.tables.mytable

        cte = select(table1.c.name).where(table1.c.name == "bar").cte()

        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == cte.c.name
        )

        ins = self.tables.myothertable.insert().from_select(
            ("otherid", "othername"), sel
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "WITH anon_1 AS "
            "(SELECT mytable.name AS name FROM mytable "
            "WHERE mytable.name = :name_1) "
            "SELECT mytable.myid, mytable.name FROM mytable, anon_1 "
            "WHERE mytable.name = anon_1.name",
            checkparams={"name_1": "bar"},
            dialect=dialect,
        )

    def test_insert_from_select_cte_two(self):
        table1 = self.tables.mytable

        cte = table1.select().cte("c")
        stmt = cte.select()
        ins = table1.insert().from_select(table1.c, stmt)

        self.assert_compile(
            ins,
            "WITH c AS (SELECT mytable.myid AS myid, mytable.name AS name, "
            "mytable.description AS description FROM mytable) "
            "INSERT INTO mytable (myid, name, description) "
            "SELECT c.myid, c.name, c.description FROM c",
        )

    def test_insert_from_select_cte_follows_insert_two(self):
        dialect = default.DefaultDialect()
        dialect.cte_follows_insert = True
        table1 = self.tables.mytable

        cte = table1.select().cte("c")
        stmt = cte.select()
        ins = table1.insert().from_select(table1.c, stmt)

        self.assert_compile(
            ins,
            "INSERT INTO mytable (myid, name, description) "
            "WITH c AS (SELECT mytable.myid AS myid, mytable.name AS name, "
            "mytable.description AS description FROM mytable) "
            "SELECT c.myid, c.name, c.description FROM c",
            dialect=dialect,
        )

    def test_insert_from_select_select_alt_ordering(self):
        table1 = self.tables.mytable
        sel = select(table1.c.name, table1.c.myid).where(
            table1.c.name == "foo"
        )
        ins = self.tables.myothertable.insert().from_select(
            ("othername", "otherid"), sel
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername, otherid) "
            "SELECT mytable.name, mytable.myid FROM mytable "
            "WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"},
        )

    def test_insert_from_select_no_defaults(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default=func.foobar()),
        )
        table1 = self.tables.mytable
        sel = select(table1.c.myid).where(table1.c.name == "foo")
        ins = table.insert().from_select(["id"], sel, include_defaults=False)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id) SELECT mytable.myid "
            "FROM mytable WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"},
        )

    def test_insert_from_select_with_sql_defaults(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default=func.foobar()),
        )
        table1 = self.tables.mytable
        sel = select(table1.c.myid).where(table1.c.name == "foo")
        ins = table.insert().from_select(["id"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo) SELECT "
            "mytable.myid, foobar() AS foobar_1 "
            "FROM mytable WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"},
        )

    def test_insert_from_select_with_python_defaults(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default=12),
        )
        table1 = self.tables.mytable
        sel = select(table1.c.myid).where(table1.c.name == "foo")
        ins = table.insert().from_select(["id"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo) SELECT "
            "mytable.myid, :foo AS anon_1 "
            "FROM mytable WHERE mytable.name = :name_1",
            # value filled in at execution time
            checkparams={"name_1": "foo", "foo": None},
        )

    def test_insert_from_select_override_defaults(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default=12),
        )
        table1 = self.tables.mytable
        sel = select(table1.c.myid, table1.c.myid.label("q")).where(
            table1.c.name == "foo"
        )
        ins = table.insert().from_select(["id", "foo"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo) SELECT "
            "mytable.myid, mytable.myid AS q "
            "FROM mytable WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"},
        )

    def test_insert_from_select_fn_defaults(self):
        metadata = MetaData()

        def foo(ctx):
            return 12

        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default=foo),
        )
        table1 = self.tables.mytable
        sel = select(table1.c.myid).where(table1.c.name == "foo")
        ins = table.insert().from_select(["id"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo) SELECT "
            "mytable.myid, :foo AS anon_1 "
            "FROM mytable WHERE mytable.name = :name_1",
            # value filled in at execution time
            checkparams={"name_1": "foo", "foo": None},
        )

    def test_insert_from_select_fn_defaults_compound(self):
        """test #8073"""

        metadata = MetaData()

        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default="foo"),
            Column("bar", Integer, default="bar"),
        )
        table1 = self.tables.mytable
        sel = (
            select(table1.c.myid)
            .where(table1.c.name == "foo")
            .union(select(table1.c.myid).where(table1.c.name == "foo"))
        )
        ins = table.insert().from_select(["id"], sel)
        with expect_raises_message(
            exc.CompileError,
            r"Can't extend statement for INSERT..FROM SELECT to include "
            r"additional default-holding column\(s\) 'foo', 'bar'.  "
            r"Convert the selectable to a subquery\(\) first, or pass "
            r"include_defaults=False to Insert.from_select\(\) to skip these "
            r"columns.",
        ):
            ins.compile()

    def test_insert_from_select_fn_defaults_compound_subquery(self):
        """test #8073"""

        metadata = MetaData()

        def foo(ctx):
            return 12

        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("foo", Integer, default="foo"),
            Column("bar", Integer, default="bar"),
        )
        table1 = self.tables.mytable
        sel = (
            select(table1.c.myid)
            .where(table1.c.name == "foo")
            .union(select(table1.c.myid).where(table1.c.name == "foo"))
            .subquery()
        )

        ins = table.insert().from_select(["id"], sel)
        self.assert_compile(
            ins,
            "INSERT INTO sometable (id, foo, bar) SELECT anon_1.myid, "
            ":foo AS anon_2, :bar AS anon_3 FROM "
            "(SELECT mytable.myid AS myid FROM mytable "
            "WHERE mytable.name = :name_1 UNION "
            "SELECT mytable.myid AS myid FROM mytable "
            "WHERE mytable.name = :name_2) AS anon_1",
            checkparams={
                "foo": None,
                "bar": None,
                "name_1": "foo",
                "name_2": "foo",
            },
        )

    def test_insert_from_select_dont_mutate_raw_columns(self):
        # test [ticket:3603]
        from sqlalchemy import table

        table_ = table(
            "mytable",
            Column("foo", String),
            Column("bar", String, default="baz"),
        )

        stmt = select(table_.c.foo)
        insert = table_.insert().from_select(["foo"], stmt)

        self.assert_compile(stmt, "SELECT mytable.foo FROM mytable")
        self.assert_compile(
            insert,
            "INSERT INTO mytable (foo, bar) "
            "SELECT mytable.foo, :bar AS anon_1 FROM mytable",
        )
        self.assert_compile(stmt, "SELECT mytable.foo FROM mytable")
        self.assert_compile(
            insert,
            "INSERT INTO mytable (foo, bar) "
            "SELECT mytable.foo, :bar AS anon_1 FROM mytable",
        )

    def test_insert_mix_select_values_exception(self):
        table1 = self.tables.mytable
        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == "foo"
        )
        ins = self.tables.myothertable.insert().from_select(
            ("otherid", "othername"), sel
        )
        assert_raises_message(
            exc.InvalidRequestError,
            "This construct already inserts from a SELECT",
            ins.values,
            othername="5",
        )

    def test_insert_mix_values_select_exception(self):
        table1 = self.tables.mytable
        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == "foo"
        )
        ins = self.tables.myothertable.insert().values(othername="5")
        assert_raises_message(
            exc.InvalidRequestError,
            "This construct already inserts value expressions",
            ins.from_select,
            ("otherid", "othername"),
            sel,
        )

    def test_insert_from_select_table(self):
        table1 = self.tables.mytable
        ins = self.tables.myothertable.insert().from_select(
            ("otherid", "othername"), table1
        )
        # note we aren't checking the number of columns right now
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name, mytable.description "
            "FROM mytable",
            checkparams={},
        )

    def test_insert_from_select_union(self):
        mytable = self.tables.mytable

        name = column("name")
        description = column("desc")
        sel = select(name, mytable.c.description).union(
            select(name, description)
        )
        ins = mytable.insert().from_select(
            [mytable.c.name, mytable.c.description], sel
        )
        self.assert_compile(
            ins,
            "INSERT INTO mytable (name, description) "
            "SELECT name, mytable.description FROM mytable "
            'UNION SELECT name, "desc"',
        )

    def test_insert_from_select_col_values(self):
        table1 = self.tables.mytable
        table2 = self.tables.myothertable
        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == "foo"
        )
        ins = table2.insert().from_select(
            (table2.c.otherid, table2.c.othername), sel
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = :name_1",
            checkparams={"name_1": "foo"},
        )

    def test_anticipate_no_pk_composite_pk(self):
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, primary_key=True),
            Column("y", Integer, primary_key=True),
        )

        with expect_warnings(
            "Column 't.y' is marked as a member.*"
            "Note that as of SQLAlchemy 1.1,"
        ):
            self.assert_compile(
                t.insert(), "INSERT INTO t (x) VALUES (:x)", params={"x": 5}
            )

    def test_anticipate_no_pk_composite_pk_implicit_returning(self):
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, primary_key=True),
            Column("y", Integer, primary_key=True),
        )
        d = postgresql.dialect()
        d.implicit_returning = True

        with expect_warnings(
            "Column 't.y' is marked as a member.*"
            "Note that as of SQLAlchemy 1.1,"
        ):
            self.assert_compile(
                t.insert(),
                "INSERT INTO t (x) VALUES (%(x)s)",
                params={"x": 5},
                dialect=d,
            )

    def test_anticipate_no_pk_composite_pk_prefetch(self):
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, primary_key=True),
            Column("y", Integer, primary_key=True),
        )
        d = postgresql.dialect()
        d.implicit_returning = False
        with expect_warnings(
            "Column 't.y' is marked as a member.*"
            "Note that as of SQLAlchemy 1.1,"
        ):
            self.assert_compile(
                t.insert(),
                "INSERT INTO t (x) VALUES (%(x)s)",
                params={"x": 5},
                dialect=d,
            )

    def test_anticipate_nullable_composite_pk(self):
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, primary_key=True),
            Column("y", Integer, primary_key=True, nullable=True),
        )
        self.assert_compile(
            t.insert(), "INSERT INTO t (x) VALUES (:x)", params={"x": 5}
        )

    def test_anticipate_no_pk_non_composite_pk(self):
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, primary_key=True, autoincrement=False),
            Column("q", Integer),
        )
        with expect_warnings(
            "Column 't.x' is marked as a member.*may not store NULL.$"
        ):
            self.assert_compile(
                t.insert(), "INSERT INTO t (q) VALUES (:q)", params={"q": 5}
            )

    def test_anticipate_no_pk_non_composite_pk_implicit_returning(self):
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, primary_key=True, autoincrement=False),
            Column("q", Integer),
        )
        d = postgresql.dialect()
        d.implicit_returning = True
        with expect_warnings(
            "Column 't.x' is marked as a member.*may not store NULL.$"
        ):
            self.assert_compile(
                t.insert(),
                "INSERT INTO t (q) VALUES (%(q)s)",
                params={"q": 5},
                dialect=d,
            )

    def test_anticipate_no_pk_non_composite_pk_prefetch(self):
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, primary_key=True, autoincrement=False),
            Column("q", Integer),
        )
        d = postgresql.dialect()
        d.implicit_returning = False

        with expect_warnings(
            "Column 't.x' is marked as a member.*may not store NULL.$"
        ):
            self.assert_compile(
                t.insert(),
                "INSERT INTO t (q) VALUES (%(q)s)",
                params={"q": 5},
                dialect=d,
            )

    def test_anticipate_no_pk_lower_case_table(self):
        t = table(
            "t",
            Column("id", Integer, primary_key=True, autoincrement=False),
            Column("notpk", String(10), nullable=True),
        )
        with expect_warnings(
            "Column 't.id' is marked as a member.*may not store NULL.$"
        ):
            self.assert_compile(
                t.insert(),
                "INSERT INTO t () VALUES ()",
                params={},
                supports_default_values=False,
            )


class InsertImplicitReturningTest(
    _InsertTestBase, fixtures.TablesTest, AssertsCompiledSQL
):
    __dialect__ = postgresql.dialect(implicit_returning=True)

    def test_insert_select(self):
        table1 = self.tables.mytable
        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == "foo"
        )
        ins = self.tables.myothertable.insert().from_select(
            ("otherid", "othername"), sel
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = %(name_1)s",
            checkparams={"name_1": "foo"},
        )

    def test_insert_select_return_defaults(self):
        table1 = self.tables.mytable
        sel = select(table1.c.myid, table1.c.name).where(
            table1.c.name == "foo"
        )
        ins = (
            self.tables.myothertable.insert()
            .from_select(("otherid", "othername"), sel)
            .return_defaults(self.tables.myothertable.c.otherid)
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (otherid, othername) "
            "SELECT mytable.myid, mytable.name FROM mytable "
            "WHERE mytable.name = %(name_1)s",
            checkparams={"name_1": "foo"},
        )

    @testing.combinations(
        True, False, argnames="insert_null_still_autoincrements"
    )
    @testing.combinations("values", "params", "nothing", argnames="paramtype")
    def test_explicit_null_implicit_returning_still_renders(
        self, paramtype, insert_null_still_autoincrements
    ):
        """test for future support of #7998 with RETURNING"""
        t = Table(
            "t",
            MetaData(),
            Column("x", Integer, primary_key=True),
            Column("q", Integer),
        )

        dialect = postgresql.dialect()
        dialect.insert_null_pk_still_autoincrements = (
            insert_null_still_autoincrements
        )

        if paramtype == "values":
            # for values present, we now have an extra check for this
            stmt = t.insert().values(x=None, q=5)
            if insert_null_still_autoincrements:
                expected = (
                    "INSERT INTO t (x, q) VALUES (%(x)s, %(q)s) RETURNING t.x"
                )
            else:
                expected = "INSERT INTO t (x, q) VALUES (%(x)s, %(q)s)"
            params = None
        elif paramtype == "params":
            # for params, compiler doesnt have the value available to look
            # at.  we assume non-NULL
            stmt = t.insert()
            if insert_null_still_autoincrements:
                expected = (
                    "INSERT INTO t (x, q) VALUES (%(x)s, %(q)s) RETURNING t.x"
                )
            else:
                expected = "INSERT INTO t (x, q) VALUES (%(x)s, %(q)s)"
            params = {"x": None, "q": 5}
        elif paramtype == "nothing":
            # no params, we assume full INSERT.  this kind of compilation
            # doesn't actually happen during execution since there are always
            # parameters or values
            stmt = t.insert()
            expected = "INSERT INTO t (x, q) VALUES (%(x)s, %(q)s)"
            params = None

        self.assert_compile(stmt, expected, params=params, dialect=dialect)

    def test_insert_multiple_values(self):
        ins = self.tables.myothertable.insert().values(
            [{"othername": "foo"}, {"othername": "bar"}]
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) "
            "VALUES (%(othername_m0)s), "
            "(%(othername_m1)s)",
            checkparams={"othername_m1": "bar", "othername_m0": "foo"},
        )

    def test_insert_multiple_values_literal_binds(self):
        ins = self.tables.myothertable.insert().values(
            [{"othername": "foo"}, {"othername": "bar"}]
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) VALUES ('foo'), ('bar')",
            checkparams={},
            literal_binds=True,
        )

    def test_insert_multiple_values_return_defaults(self):
        # TODO: not sure if this should raise an
        # error or what
        ins = (
            self.tables.myothertable.insert()
            .values([{"othername": "foo"}, {"othername": "bar"}])
            .return_defaults(self.tables.myothertable.c.otherid)
        )
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) "
            "VALUES (%(othername_m0)s), "
            "(%(othername_m1)s)",
            checkparams={"othername_m1": "bar", "othername_m0": "foo"},
        )

    def test_insert_single_list_values(self):
        ins = self.tables.myothertable.insert().values([{"othername": "foo"}])
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) "
            "VALUES (%(othername_m0)s)",
            checkparams={"othername_m0": "foo"},
        )

    def test_insert_single_element_values(self):
        ins = self.tables.myothertable.insert().values({"othername": "foo"})
        self.assert_compile(
            ins,
            "INSERT INTO myothertable (othername) "
            "VALUES (%(othername)s) RETURNING myothertable.otherid",
            checkparams={"othername": "foo"},
        )


class EmptyTest(_InsertTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_empty_insert_default(self):
        table1 = self.tables.mytable

        stmt = table1.insert().values({})  # hide from 2to3
        self.assert_compile(
            stmt,
            "INSERT INTO mytable () VALUES ()",
            supports_default_values=False,
        )

    def test_supports_empty_insert_true(self):
        table1 = self.tables.mytable

        stmt = table1.insert().values({})
        self.assert_compile(
            stmt,
            "INSERT INTO mytable DEFAULT VALUES",
            supports_default_values=True,
        )

    def test_supports_empty_insert_true_executemany_mode(self):
        table1 = self.tables.mytable

        dialect = default.DefaultDialect()
        dialect.supports_empty_insert = False
        dialect.supports_default_values = True
        dialect.supports_default_metavalue = True

        stmt = table1.insert().values({})
        self.assert_compile(
            stmt,
            "INSERT INTO mytable (myid) VALUES (DEFAULT)",
            dialect=dialect,
            for_executemany=True,
        )

        dialect.supports_default_metavalue = False
        self.assert_compile(
            stmt,
            "INSERT INTO mytable DEFAULT VALUES",
            dialect=dialect,
            for_executemany=True,
        )

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
            dialect=dialect,
        )

    @testing.combinations(([],), ({},), ((),))
    def test_insert_with_empty_collection_values(self, collection):
        table1 = self.tables.mytable

        ins = table1.insert().values(collection)

        self.assert_compile(
            ins,
            "INSERT INTO mytable () VALUES ()",
            checkparams={},
            supports_default_values=False,
        )

        # empty dict populates on next values call
        self.assert_compile(
            ins.values(myid=3),
            "INSERT INTO mytable (myid) VALUES (:myid)",
            checkparams={"myid": 3},
        )


class MultirowTest(_InsertTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_not_supported(self):
        table1 = self.tables.mytable

        dialect = default.DefaultDialect()
        stmt = table1.insert().values([{"myid": 1}, {"myid": 2}])
        assert_raises_message(
            exc.CompileError,
            "The 'default' dialect with current database version settings "
            "does not support in-place multirow inserts.",
            stmt.compile,
            dialect=dialect,
        )

    def test_named(self):
        table1 = self.tables.mytable

        values = [
            {"myid": 1, "name": "a", "description": "b"},
            {"myid": 2, "name": "c", "description": "d"},
            {"myid": 3, "name": "e", "description": "f"},
        ]

        checkparams = {
            "myid_m0": 1,
            "myid_m1": 2,
            "myid_m2": 3,
            "name_m0": "a",
            "name_m1": "c",
            "name_m2": "e",
            "description_m0": "b",
            "description_m1": "d",
            "description_m2": "f",
        }

        dialect = default.DefaultDialect()
        dialect.supports_multivalues_insert = True

        self.assert_compile(
            table1.insert().values(values),
            "INSERT INTO mytable (myid, name, description) VALUES "
            "(:myid_m0, :name_m0, :description_m0), "
            "(:myid_m1, :name_m1, :description_m1), "
            "(:myid_m2, :name_m2, :description_m2)",
            checkparams=checkparams,
            dialect=dialect,
        )

    @testing.combinations(("strings",), ("columns",), ("inspectables",))
    def test_named_with_column_objects(self, column_style):
        table1 = self.tables.mytable

        if column_style == "strings":
            myid, name, description = "myid", "name", "description"

        elif column_style == "columns":
            myid, name, description = (
                table1.c.myid,
                table1.c.name,
                table1.c.description,
            )
        elif column_style == "inspectables":
            myid, name, description = (
                ORMExpr(table1.c.myid),
                ORMExpr(table1.c.name),
                ORMExpr(table1.c.description),
            )
        else:
            assert False

        values = [
            {myid: 1, name: "a", description: "b"},
            {myid: 2, name: "c", description: "d"},
            {myid: 3, name: "e", description: "f"},
        ]

        checkparams = {
            "myid_m0": 1,
            "myid_m1": 2,
            "myid_m2": 3,
            "name_m0": "a",
            "name_m1": "c",
            "name_m2": "e",
            "description_m0": "b",
            "description_m1": "d",
            "description_m2": "f",
        }

        dialect = default.DefaultDialect()
        dialect.supports_multivalues_insert = True

        self.assert_compile(
            table1.insert().values(values),
            "INSERT INTO mytable (myid, name, description) VALUES "
            "(:myid_m0, :name_m0, :description_m0), "
            "(:myid_m1, :name_m1, :description_m1), "
            "(:myid_m2, :name_m2, :description_m2)",
            checkparams=checkparams,
            dialect=dialect,
        )

    def test_positional(self):
        table1 = self.tables.mytable

        values = [
            {"myid": 1, "name": "a", "description": "b"},
            {"myid": 2, "name": "c", "description": "d"},
            {"myid": 3, "name": "e", "description": "f"},
        ]

        checkpositional = (1, "a", "b", 2, "c", "d", 3, "e", "f")

        dialect = default.DefaultDialect()
        dialect.supports_multivalues_insert = True
        dialect.paramstyle = "format"
        dialect.positional = True

        self.assert_compile(
            table1.insert().values(values),
            "INSERT INTO mytable (myid, name, description) VALUES "
            "(%s, %s, %s), (%s, %s, %s), (%s, %s, %s)",
            checkpositional=checkpositional,
            dialect=dialect,
        )

    def test_positional_w_defaults(self):
        table1 = self.tables.table_w_defaults

        values = [{"id": 1}, {"id": 2}, {"id": 3}]

        checkpositional = (1, None, None, 2, None, None, 3, None, None)

        dialect = default.DefaultDialect()
        dialect.supports_multivalues_insert = True
        dialect.paramstyle = "format"
        dialect.positional = True

        self.assert_compile(
            table1.insert().values(values),
            "INSERT INTO table_w_defaults (id, x, z) VALUES "
            "(%s, %s, %s), (%s, %s, %s), (%s, %s, %s)",
            checkpositional=checkpositional,
            check_prefetch=[
                table1.c.x,
                table1.c.z,
                crud._multiparam_column(table1.c.x, 0),
                crud._multiparam_column(table1.c.z, 0),
                crud._multiparam_column(table1.c.x, 1),
                crud._multiparam_column(table1.c.z, 1),
            ],
            dialect=dialect,
        )

    def test_mix_single_and_multi_single_first(self):
        table1 = self.tables.mytable

        stmt = table1.insert().values(myid=1, name="d1")
        stmt = stmt.values(
            [{"myid": 2, "name": "d2"}, {"myid": 3, "name": "d3"}]
        )

        assert_raises_message(
            exc.InvalidRequestError,
            "Can't mix single and multiple VALUES formats in one "
            "INSERT statement",
            stmt.compile,
        )

    def test_mix_single_and_multi_multi_first(self):
        table1 = self.tables.mytable

        stmt = table1.insert().values(
            [{"myid": 2, "name": "d2"}, {"myid": 3, "name": "d3"}]
        )

        stmt = stmt.values(myid=1, name="d1")

        assert_raises_message(
            exc.InvalidRequestError,
            "Can't mix single and multiple VALUES formats in one "
            "INSERT statement",
            stmt.compile,
        )

    def test_multi_multi(self):
        table1 = self.tables.mytable

        stmt = table1.insert().values([{"myid": 1, "name": "d1"}])

        stmt = stmt.values(
            [{"myid": 2, "name": "d2"}, {"myid": 3, "name": "d3"}]
        )

        self.assert_compile(
            stmt,
            "INSERT INTO mytable (myid, name) VALUES (%(myid_m0)s, "
            "%(name_m0)s), (%(myid_m1)s, %(name_m1)s), (%(myid_m2)s, "
            "%(name_m2)s)",
            checkparams={
                "myid_m0": 1,
                "name_m0": "d1",
                "myid_m1": 2,
                "name_m1": "d2",
                "myid_m2": 3,
                "name_m2": "d3",
            },
            dialect=postgresql.dialect(),
        )

    def test_inline_default(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
            Column("foo", Integer, default=func.foobar()),
        )

        values = [
            {"id": 1, "data": "data1"},
            {"id": 2, "data": "data2", "foo": "plainfoo"},
            {"id": 3, "data": "data3"},
        ]

        checkparams = {
            "id_m0": 1,
            "id_m1": 2,
            "id_m2": 3,
            "data_m0": "data1",
            "data_m1": "data2",
            "data_m2": "data3",
            "foo_m1": "plainfoo",
        }

        self.assert_compile(
            table.insert().values(values),
            "INSERT INTO sometable (id, data, foo) VALUES "
            "(%(id_m0)s, %(data_m0)s, foobar()), "
            "(%(id_m1)s, %(data_m1)s, %(foo_m1)s), "
            "(%(id_m2)s, %(data_m2)s, foobar())",
            checkparams=checkparams,
            dialect=postgresql.dialect(),
        )

    @testing.combinations(("strings",), ("columns",), ("inspectables",))
    def test_python_scalar_default(self, key_type):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
            Column("foo", Integer, default=10),
        )

        if key_type == "strings":
            id_, data, foo = "id", "data", "foo"
        elif key_type == "columns":
            id_, data, foo = table.c.id, table.c.data, table.c.foo
        elif key_type == "inspectables":
            id_, data, foo = (
                ORMExpr(table.c.id),
                ORMExpr(table.c.data),
                ORMExpr(table.c.foo),
            )
        else:
            assert False

        values = [
            {id_: 1, data: "data1"},
            {id_: 2, data: "data2", foo: 15},
            {id_: 3, data: "data3"},
        ]

        checkparams = {
            "id_m0": 1,
            "id_m1": 2,
            "id_m2": 3,
            "data_m0": "data1",
            "data_m1": "data2",
            "data_m2": "data3",
            "foo": None,  # evaluated later
            "foo_m1": 15,
            "foo_m2": None,  # evaluated later
        }

        stmt = table.insert().values(values)

        eq_(
            {
                k: v.type._type_affinity
                for (k, v) in stmt.compile(
                    dialect=postgresql.dialect()
                ).binds.items()
            },
            {
                "foo": Integer,
                "data_m2": String,
                "id_m0": Integer,
                "id_m2": Integer,
                "foo_m1": Integer,
                "data_m1": String,
                "id_m1": Integer,
                "foo_m2": Integer,
                "data_m0": String,
            },
        )

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (id, data, foo) VALUES "
            "(%(id_m0)s, %(data_m0)s, %(foo)s), "
            "(%(id_m1)s, %(data_m1)s, %(foo_m1)s), "
            "(%(id_m2)s, %(data_m2)s, %(foo_m2)s)",
            checkparams=checkparams,
            dialect=postgresql.dialect(),
        )

    def test_sql_expression_pk_autoinc_lastinserted(self):
        # test that postfetch isn't invoked for a SQL expression
        # in a primary key column.  the DB either needs to support a lastrowid
        # that can return it, or RETURNING.  [ticket:3133]
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
        )

        stmt = table.insert().return_defaults().values(id=func.foobar())
        dialect = sqlite.dialect()
        dialect.insert_returning = False
        compiled = stmt.compile(dialect=dialect, column_keys=["data"])
        eq_(compiled.postfetch, [])
        eq_(compiled.implicit_returning, [])

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (id, data) VALUES (foobar(), ?)",
            checkparams={"data": "foo"},
            params={"data": "foo"},
            dialect=dialect,
        )

    def test_sql_expression_pk_autoinc_returning(self):
        # test that return_defaults() works with a primary key where we are
        # sending a SQL expression, and we need to get the server-calculated
        # value back.  [ticket:3133]
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
        )

        stmt = table.insert().return_defaults().values(id=func.foobar())
        returning_dialect = postgresql.dialect()
        returning_dialect.implicit_returning = True
        compiled = stmt.compile(
            dialect=returning_dialect, column_keys=["data"]
        )
        eq_(compiled.postfetch, [])
        eq_(compiled.implicit_returning, [table.c.id])

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (id, data) VALUES "
            "(foobar(), %(data)s) RETURNING sometable.id",
            checkparams={"data": "foo"},
            params={"data": "foo"},
            dialect=returning_dialect,
        )

    def test_sql_expression_pk_noautoinc_returning(self):
        # test that return_defaults() works with a primary key where we are
        # sending a SQL expression, and we need to get the server-calculated
        # value back.  [ticket:3133]
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, autoincrement=False, primary_key=True),
            Column("data", String),
        )

        stmt = table.insert().return_defaults().values(id=func.foobar())
        returning_dialect = postgresql.dialect()
        returning_dialect.implicit_returning = True
        compiled = stmt.compile(
            dialect=returning_dialect, column_keys=["data"]
        )
        eq_(compiled.postfetch, [])
        eq_(compiled.implicit_returning, [table.c.id])

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (id, data) VALUES "
            "(foobar(), %(data)s) RETURNING sometable.id",
            checkparams={"data": "foo"},
            params={"data": "foo"},
            dialect=returning_dialect,
        )

    def test_python_fn_default(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
            Column("foo", Integer, default=lambda: 10),
        )

        values = [
            {"id": 1, "data": "data1"},
            {"id": 2, "data": "data2", "foo": 15},
            {"id": 3, "data": "data3"},
        ]

        checkparams = {
            "id_m0": 1,
            "id_m1": 2,
            "id_m2": 3,
            "data_m0": "data1",
            "data_m1": "data2",
            "data_m2": "data3",
            "foo": None,  # evaluated later
            "foo_m1": 15,
            "foo_m2": None,  # evaluated later
        }

        stmt = table.insert().values(values)
        eq_(
            {
                k: v.type._type_affinity
                for (k, v) in stmt.compile(
                    dialect=postgresql.dialect()
                ).binds.items()
            },
            {
                "foo": Integer,
                "data_m2": String,
                "id_m0": Integer,
                "id_m2": Integer,
                "foo_m1": Integer,
                "data_m1": String,
                "id_m1": Integer,
                "foo_m2": Integer,
                "data_m0": String,
            },
        )

        self.assert_compile(
            stmt,
            "INSERT INTO sometable (id, data, foo) VALUES "
            "(%(id_m0)s, %(data_m0)s, %(foo)s), "
            "(%(id_m1)s, %(data_m1)s, %(foo_m1)s), "
            "(%(id_m2)s, %(data_m2)s, %(foo_m2)s)",
            checkparams=checkparams,
            dialect=postgresql.dialect(),
        )

    def test_sql_functions(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
            Column("foo", Integer),
        )

        values = [
            {"id": 1, "data": "foo", "foo": func.foob()},
            {"id": 2, "data": "bar", "foo": func.foob()},
            {"id": 3, "data": "bar", "foo": func.bar()},
            {"id": 4, "data": "bar", "foo": 15},
            {"id": 5, "data": "bar", "foo": func.foob()},
        ]
        checkparams = {
            "id_m0": 1,
            "data_m0": "foo",
            "id_m1": 2,
            "data_m1": "bar",
            "id_m2": 3,
            "data_m2": "bar",
            "id_m3": 4,
            "data_m3": "bar",
            "foo_m3": 15,
            "id_m4": 5,
            "data_m4": "bar",
        }

        self.assert_compile(
            table.insert().values(values),
            "INSERT INTO sometable (id, data, foo) VALUES "
            "(%(id_m0)s, %(data_m0)s, foob()), "
            "(%(id_m1)s, %(data_m1)s, foob()), "
            "(%(id_m2)s, %(data_m2)s, bar()), "
            "(%(id_m3)s, %(data_m3)s, %(foo_m3)s), "
            "(%(id_m4)s, %(data_m4)s, foob())",
            checkparams=checkparams,
            dialect=postgresql.dialect(),
        )

    def test_server_default(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
            Column("foo", Integer, server_default=func.foobar()),
        )

        values = [
            {"id": 1, "data": "data1"},
            {"id": 2, "data": "data2", "foo": "plainfoo"},
            {"id": 3, "data": "data3"},
        ]

        checkparams = {
            "id_m0": 1,
            "id_m1": 2,
            "id_m2": 3,
            "data_m0": "data1",
            "data_m1": "data2",
            "data_m2": "data3",
        }

        self.assert_compile(
            table.insert().values(values),
            "INSERT INTO sometable (id, data) VALUES "
            "(%(id_m0)s, %(data_m0)s), "
            "(%(id_m1)s, %(data_m1)s), "
            "(%(id_m2)s, %(data_m2)s)",
            checkparams=checkparams,
            dialect=postgresql.dialect(),
        )

    def test_server_default_absent_value(self):
        metadata = MetaData()
        table = Table(
            "sometable",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String),
            Column("foo", Integer, server_default=func.foobar()),
        )

        values = [
            {"id": 1, "data": "data1", "foo": "plainfoo"},
            {"id": 2, "data": "data2"},
            {"id": 3, "data": "data3", "foo": "otherfoo"},
        ]

        assert_raises_message(
            exc.CompileError,
            "INSERT value for column sometable.foo is explicitly rendered "
            "as a boundparameter in the VALUES clause; a Python-side value or "
            "SQL expression is required",
            table.insert().values(values).compile,
        )
