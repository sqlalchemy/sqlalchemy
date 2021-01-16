import itertools
import random

from sqlalchemy import bindparam
from sqlalchemy import column
from sqlalchemy import exc
from sqlalchemy import exists
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy import util
from sqlalchemy.dialects import mysql
from sqlalchemy.engine import default
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BooleanClauseList
from sqlalchemy.testing import assert_raises
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import mock
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class _UpdateFromTestBase(object):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "mytable",
            metadata,
            Column("myid", Integer),
            Column("name", String(30)),
            Column("description", String(50)),
        )
        Table(
            "myothertable",
            metadata,
            Column("otherid", Integer),
            Column("othername", String(30)),
        )
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
        )
        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("name", String(30), nullable=False),
            Column("email_address", String(50), nullable=False),
        )
        Table(
            "dingalings",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("address_id", None, ForeignKey("addresses.id")),
            Column("data", String(30)),
        )
        Table(
            "update_w_default",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("x", Integer),
            Column("ycol", Integer, key="y"),
            Column("data", String(30), onupdate=lambda: "hi"),
        )

    @classmethod
    def fixtures(cls):
        return dict(
            users=(
                ("id", "name"),
                (7, "jack"),
                (8, "ed"),
                (9, "fred"),
                (10, "chuck"),
            ),
            addresses=(
                ("id", "user_id", "name", "email_address"),
                (1, 7, "x", "jack@bean.com"),
                (2, 8, "x", "ed@wood.com"),
                (3, 8, "x", "ed@bettyboop.com"),
                (4, 8, "x", "ed@lala.com"),
                (5, 9, "x", "fred@fred.com"),
            ),
            dingalings=(
                ("id", "address_id", "data"),
                (1, 2, "ding 1/2"),
                (2, 5, "ding 2/5"),
            ),
        )


class UpdateTest(_UpdateFromTestBase, fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = "default_enhanced"

    def test_update_literal_binds(self):
        table1 = self.tables.mytable

        stmt = (
            table1.update().values(name="jack").where(table1.c.name == "jill")
        )

        self.assert_compile(
            stmt,
            "UPDATE mytable SET name='jack' WHERE mytable.name = 'jill'",
            literal_binds=True,
        )

    def test_update_custom_key_thing(self):
        table1 = self.tables.mytable

        class Thing(object):
            def __clause_element__(self):
                return table1.c.name

        stmt = (
            table1.update()
            .values({Thing(): "jack"})
            .where(table1.c.name == "jill")
        )

        self.assert_compile(
            stmt,
            "UPDATE mytable SET name='jack' WHERE mytable.name = 'jill'",
            literal_binds=True,
        )

    def test_update_ordered_custom_key_thing(self):
        table1 = self.tables.mytable

        class Thing(object):
            def __clause_element__(self):
                return table1.c.name

        stmt = (
            table1.update()
            .ordered_values((Thing(), "jack"))
            .where(table1.c.name == "jill")
        )

        self.assert_compile(
            stmt,
            "UPDATE mytable SET name='jack' WHERE mytable.name = 'jill'",
            literal_binds=True,
        )

    def test_update_broken_custom_key_thing(self):
        table1 = self.tables.mytable

        class Thing(object):
            def __clause_element__(self):
                return 5

        assert_raises_message(
            exc.ArgumentError,
            "SET/VALUES column expression or string key expected, got .*Thing",
            table1.update().values,
            {Thing(): "jack"},
        )

    def test_update_ordered_broken_custom_key_thing(self):
        table1 = self.tables.mytable

        class Thing(object):
            def __clause_element__(self):
                return 5

        assert_raises_message(
            exc.ArgumentError,
            "SET/VALUES column expression or string key expected, got .*Thing",
            table1.update().ordered_values,
            (Thing(), "jack"),
        )

    def test_correlated_update_one(self):
        table1 = self.tables.mytable

        # test against a straight text subquery
        u = update(table1).values(
            {
                table1.c.name: text(
                    "(select name from mytable where id=mytable.id)"
                )
            }
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=(select name from mytable "
            "where id=mytable.id)",
        )

    def test_correlated_update_two(self):
        table1 = self.tables.mytable

        mt = table1.alias()
        u = update(table1).values(
            {
                table1.c.name: select(mt.c.name)
                .where(mt.c.myid == table1.c.myid)
                .scalar_subquery()
            }
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=(SELECT mytable_1.name FROM "
            "mytable AS mytable_1 WHERE "
            "mytable_1.myid = mytable.myid)",
        )

    def test_correlated_update_three(self):
        table1 = self.tables.mytable
        table2 = self.tables.myothertable

        # test against a regular constructed subquery
        s = (
            select(table2)
            .where(table2.c.otherid == table1.c.myid)
            .scalar_subquery()
        )
        u = (
            update(table1)
            .where(table1.c.name == "jack")
            .values({table1.c.name: s})
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=(SELECT myothertable.otherid, "
            "myothertable.othername FROM myothertable WHERE "
            "myothertable.otherid = mytable.myid) "
            "WHERE mytable.name = :name_1",
        )

    def test_correlated_update_four(self):
        table1 = self.tables.mytable
        table2 = self.tables.myothertable

        # test a non-correlated WHERE clause
        s = select(table2.c.othername).where(table2.c.otherid == 7)
        u = update(table1).where(table1.c.name == s.scalar_subquery())
        self.assert_compile(
            u,
            "UPDATE mytable SET myid=:myid, name=:name, "
            "description=:description WHERE mytable.name = "
            "(SELECT myothertable.othername FROM myothertable "
            "WHERE myothertable.otherid = :otherid_1)",
        )

    def test_correlated_update_five(self):
        table1 = self.tables.mytable
        table2 = self.tables.myothertable

        # test one that is actually correlated...
        s = select(table2.c.othername).where(table2.c.otherid == table1.c.myid)
        u = table1.update().where(table1.c.name == s.scalar_subquery())
        self.assert_compile(
            u,
            "UPDATE mytable SET myid=:myid, name=:name, "
            "description=:description WHERE mytable.name = "
            "(SELECT myothertable.othername FROM myothertable "
            "WHERE myothertable.otherid = mytable.myid)",
        )

    def test_correlated_update_six(self):
        table1 = self.tables.mytable
        table2 = self.tables.myothertable

        # test correlated FROM implicit in WHERE and SET clauses
        u = (
            table1.update()
            .values(name=table2.c.othername)
            .where(table2.c.otherid == table1.c.myid)
        )
        self.assert_compile(
            u,
            "UPDATE mytable SET name=myothertable.othername "
            "FROM myothertable WHERE myothertable.otherid = mytable.myid",
        )

    def test_correlated_update_seven(self):
        table1 = self.tables.mytable
        table2 = self.tables.myothertable

        u = (
            table1.update()
            .values(name="foo")
            .where(table2.c.otherid == table1.c.myid)
        )

        # this is the "default_enhanced" compiler.  there's no UPDATE FROM
        # in the base compiler.
        # See also test/dialect/mssql/test_compiler->test_update_from().
        self.assert_compile(
            u,
            "UPDATE mytable SET name=:name "
            "FROM myothertable WHERE myothertable.otherid = mytable.myid",
        )

    def test_binds_that_match_columns(self):
        """test bind params named after column names
        replace the normal SET/VALUES generation."""

        t = table("foo", column("x"), column("y"))

        u = t.update().where(t.c.x == bindparam("x"))

        assert_raises(exc.CompileError, u.compile)

        self.assert_compile(u, "UPDATE foo SET  WHERE foo.x = :x", params={})

        assert_raises(exc.CompileError, u.values(x=7).compile)

        self.assert_compile(
            u.values(y=7), "UPDATE foo SET y=:y WHERE foo.x = :x"
        )

        assert_raises(
            exc.CompileError, u.values(x=7).compile, column_keys=["x", "y"]
        )
        assert_raises(exc.CompileError, u.compile, column_keys=["x", "y"])

        self.assert_compile(
            u.values(x=3 + bindparam("x")),
            "UPDATE foo SET x=(:param_1 + :x) WHERE foo.x = :x",
        )

        self.assert_compile(
            u.values(x=3 + bindparam("x")),
            "UPDATE foo SET x=(:param_1 + :x) WHERE foo.x = :x",
            params={"x": 1},
        )

        self.assert_compile(
            u.values(x=3 + bindparam("x")),
            "UPDATE foo SET x=(:param_1 + :x), y=:y WHERE foo.x = :x",
            params={"x": 1, "y": 2},
        )

    def test_labels_no_collision(self):

        t = table("foo", column("id"), column("foo_id"))

        self.assert_compile(
            t.update().where(t.c.id == 5),
            "UPDATE foo SET id=:id, foo_id=:foo_id WHERE foo.id = :id_1",
        )

        self.assert_compile(
            t.update().where(t.c.id == bindparam(key=t.c.id._label)),
            "UPDATE foo SET id=:id, foo_id=:foo_id WHERE foo.id = :foo_id_1",
        )

    def test_labels_no_collision_index(self):
        """test for [ticket:4911] """

        t = Table(
            "foo",
            MetaData(),
            Column("id", Integer, index=True),
            Column("foo_id", Integer),
        )

        self.assert_compile(
            t.update().where(t.c.id == 5),
            "UPDATE foo SET id=:id, foo_id=:foo_id WHERE foo.id = :id_1",
        )

        self.assert_compile(
            t.update().where(t.c.id == bindparam(key=t.c.id._label)),
            "UPDATE foo SET id=:id, foo_id=:foo_id WHERE foo.id = :foo_id_1",
        )

    def test_inline_defaults(self):
        m = MetaData()
        foo = Table("foo", m, Column("id", Integer))

        t = Table(
            "test",
            m,
            Column("col1", Integer, onupdate=func.foo(1)),
            Column(
                "col2",
                Integer,
                onupdate=select(func.coalesce(func.max(foo.c.id))),
            ),
            Column("col3", String(30)),
        )

        self.assert_compile(
            t.update().values({"col3": "foo"}),
            "UPDATE test SET col1=foo(:foo_1), col2=(SELECT "
            "coalesce(max(foo.id)) AS coalesce_1 FROM foo), "
            "col3=:col3",
        )

        self.assert_compile(
            t.update().inline().values({"col3": "foo"}),
            "UPDATE test SET col1=foo(:foo_1), col2=(SELECT "
            "coalesce(max(foo.id)) AS coalesce_1 FROM foo), "
            "col3=:col3",
        )

    def test_update_1(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1).where(table1.c.myid == 7),
            "UPDATE mytable SET name=:name WHERE mytable.myid = :myid_1",
            params={table1.c.name: "fred"},
        )

    def test_update_2(self):
        table1 = self.tables.mytable

        self.assert_compile(
            table1.update()
            .where(table1.c.myid == 7)
            .values({table1.c.myid: 5}),
            "UPDATE mytable SET myid=:myid WHERE mytable.myid = :myid_1",
            checkparams={"myid": 5, "myid_1": 7},
        )

    def test_update_3(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1).where(table1.c.myid == 7),
            "UPDATE mytable SET name=:name WHERE mytable.myid = :myid_1",
            params={"name": "fred"},
        )

    def test_update_4(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1).values({table1.c.name: table1.c.myid}),
            "UPDATE mytable SET name=mytable.myid",
        )

    def test_update_5(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1)
            .where(table1.c.name == bindparam("crit"))
            .values(
                {table1.c.name: "hi"},
            ),
            "UPDATE mytable SET name=:name WHERE mytable.name = :crit",
            params={"crit": "notthere"},
            checkparams={"crit": "notthere", "name": "hi"},
        )

    def test_update_6(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1)
            .where(table1.c.myid == 12)
            .values(
                {table1.c.name: table1.c.myid},
            ),
            "UPDATE mytable "
            "SET name=mytable.myid, description=:description "
            "WHERE mytable.myid = :myid_1",
            params={"description": "test"},
            checkparams={"description": "test", "myid_1": 12},
        )

    def test_update_7(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1)
            .where(table1.c.myid == 12)
            .values({table1.c.myid: 9}),
            "UPDATE mytable "
            "SET myid=:myid, description=:description "
            "WHERE mytable.myid = :myid_1",
            params={"myid_1": 12, "myid": 9, "description": "test"},
        )

    def test_update_8(self):
        table1 = self.tables.mytable

        self.assert_compile(
            update(table1).where(table1.c.myid == 12),
            "UPDATE mytable SET myid=:myid WHERE mytable.myid = :myid_1",
            params={"myid": 18},
            checkparams={"myid": 18, "myid_1": 12},
        )

    def test_update_9(self):
        table1 = self.tables.mytable

        s = (
            table1.update()
            .where(table1.c.myid == 12)
            .values({table1.c.name: "lala"})
        )
        c = s.compile(column_keys=["id", "name"])
        eq_(str(s), str(c))

    def test_update_10(self):
        table1 = self.tables.mytable

        v1 = {table1.c.name: table1.c.myid}
        v2 = {table1.c.name: table1.c.name + "foo"}
        self.assert_compile(
            update(table1).where(table1.c.myid == 12).values(v1).values(v2),
            "UPDATE mytable "
            "SET "
            "name=(mytable.name || :name_1), "
            "description=:description "
            "WHERE mytable.myid = :myid_1",
            params={"description": "test"},
        )

    def test_update_11(self):
        table1 = self.tables.mytable

        values = {
            table1.c.name: table1.c.name + "lala",
            table1.c.myid: func.do_stuff(table1.c.myid, literal("hoho")),
        }

        self.assert_compile(
            update(table1)
            .where(
                (table1.c.myid == func.hoho(4))
                & (
                    table1.c.name
                    == literal("foo") + table1.c.name + literal("lala")
                )
            )
            .values(values),
            "UPDATE mytable "
            "SET "
            "myid=do_stuff(mytable.myid, :param_1), "
            "name=(mytable.name || :name_1) "
            "WHERE "
            "mytable.myid = hoho(:hoho_1) AND "
            "mytable.name = :param_2 || mytable.name || :param_3",
        )

    def test_unconsumed_names_kwargs(self):
        t = table("t", column("x"), column("y"))

        assert_raises_message(
            exc.CompileError,
            "Unconsumed column names: z",
            t.update().values(x=5, z=5).compile,
        )

    def test_unconsumed_names_values_dict(self):
        t = table("t", column("x"), column("y"))
        t2 = table("t2", column("q"), column("z"))

        assert_raises_message(
            exc.CompileError,
            "Unconsumed column names: j",
            t.update()
            .values(x=5, j=7)
            .values({t2.c.z: 5})
            .where(t.c.x == t2.c.q)
            .compile,
        )

    def test_unconsumed_names_kwargs_w_keys(self):
        t = table("t", column("x"), column("y"))

        assert_raises_message(
            exc.CompileError,
            "Unconsumed column names: j",
            t.update().values(x=5, j=7).compile,
            column_keys=["j"],
        )

    def test_update_ordered_parameters_newstyle_1(self):
        table1 = self.tables.mytable

        # Confirm that we can pass values as list value pairs
        # note these are ordered *differently* from table.c
        values = [
            (table1.c.name, table1.c.name + "lala"),
            (table1.c.myid, func.do_stuff(table1.c.myid, literal("hoho"))),
        ]
        self.assert_compile(
            update(table1)
            .where(
                (table1.c.myid == func.hoho(4))
                & (
                    table1.c.name
                    == literal("foo") + table1.c.name + literal("lala")
                )
            )
            .ordered_values(*values),
            "UPDATE mytable "
            "SET "
            "name=(mytable.name || :name_1), "
            "myid=do_stuff(mytable.myid, :param_1) "
            "WHERE "
            "mytable.myid = hoho(:hoho_1) AND "
            "mytable.name = :param_2 || mytable.name || :param_3",
        )

    def test_update_ordered_parameters_newstyle_2(self):
        table1 = self.tables.mytable

        # Confirm that we can pass values as list value pairs
        # note these are ordered *differently* from table.c
        values = [
            (table1.c.name, table1.c.name + "lala"),
            ("description", "some desc"),
            (table1.c.myid, func.do_stuff(table1.c.myid, literal("hoho"))),
        ]
        self.assert_compile(
            update(table1)
            .where(
                (table1.c.myid == func.hoho(4))
                & (
                    table1.c.name
                    == literal("foo") + table1.c.name + literal("lala")
                ),
            )
            .ordered_values(*values),
            "UPDATE mytable "
            "SET "
            "name=(mytable.name || :name_1), "
            "description=:description, "
            "myid=do_stuff(mytable.myid, :param_1) "
            "WHERE "
            "mytable.myid = hoho(:hoho_1) AND "
            "mytable.name = :param_2 || mytable.name || :param_3",
        )

    def test_update_ordered_parameters_multiple(self):
        table1 = self.tables.mytable

        stmt = update(table1)

        stmt = stmt.ordered_values(("name", "somename"))

        assert_raises_message(
            exc.ArgumentError,
            "This statement already has ordered values present",
            stmt.ordered_values,
            ("myid", 10),
        )

    def test_update_ordered_then_nonordered(self):
        table1 = self.tables.mytable

        stmt = table1.update().ordered_values(("myid", 1), ("name", "d1"))

        assert_raises_message(
            exc.InvalidRequestError,
            "This statement already has ordered values present",
            stmt.values,
            {"myid": 2, "name": "d2"},
        )

    def test_update_no_multiple_parameters_allowed(self):
        table1 = self.tables.mytable

        stmt = table1.update().values(
            [{"myid": 1, "name": "n1"}, {"myid": 2, "name": "n2"}]
        )

        assert_raises_message(
            exc.InvalidRequestError,
            "UPDATE construct does not support multiple parameter sets.",
            stmt.compile,
        )

    def test_update_ordereddict(self):
        table1 = self.tables.mytable

        # Confirm that ordered dicts are treated as normal dicts,
        # columns sorted in table order
        values = util.OrderedDict(
            (
                (table1.c.name, table1.c.name + "lala"),
                (table1.c.myid, func.do_stuff(table1.c.myid, literal("hoho"))),
            )
        )

        self.assert_compile(
            update(table1)
            .where(
                (table1.c.myid == func.hoho(4))
                & (
                    table1.c.name
                    == literal("foo") + table1.c.name + literal("lala")
                ),
            )
            .values(values),
            "UPDATE mytable "
            "SET "
            "myid=do_stuff(mytable.myid, :param_1), "
            "name=(mytable.name || :name_1) "
            "WHERE "
            "mytable.myid = hoho(:hoho_1) AND "
            "mytable.name = :param_2 || mytable.name || :param_3",
        )

    def test_where_empty(self):
        table1 = self.tables.mytable
        self.assert_compile(
            table1.update().where(
                BooleanClauseList._construct_raw(operators.and_)
            ),
            "UPDATE mytable SET myid=:myid, name=:name, "
            "description=:description",
        )
        self.assert_compile(
            table1.update().where(
                BooleanClauseList._construct_raw(operators.or_)
            ),
            "UPDATE mytable SET myid=:myid, name=:name, "
            "description=:description",
        )

    def test_prefix_with(self):
        table1 = self.tables.mytable

        stmt = (
            table1.update()
            .prefix_with("A", "B", dialect="mysql")
            .prefix_with("C", "D")
        )

        self.assert_compile(
            stmt,
            "UPDATE C D mytable SET myid=:myid, name=:name, "
            "description=:description",
        )

        self.assert_compile(
            stmt,
            "UPDATE A B C D mytable SET myid=%s, name=%s, description=%s",
            dialect=mysql.dialect(),
        )

    def test_update_to_expression_one(self):
        """test update from an expression.

        this logic is triggered currently by a left side that doesn't
        have a key.  The current supported use case is updating the index
        of a PostgreSQL ARRAY type.

        """
        table1 = self.tables.mytable
        expr = func.foo(table1.c.myid)
        eq_(expr.key, None)
        self.assert_compile(
            table1.update().values({expr: "bar"}),
            "UPDATE mytable SET foo(myid)=:param_1",
        )

    @testing.fixture
    def randomized_param_order_update(self):
        from sqlalchemy.sql.dml import UpdateDMLState

        super_process_ordered_values = UpdateDMLState._process_ordered_values

        # this fixture is needed for Python 3.6 and above to work around
        # dictionaries being insert-ordered.  in python 2.7 the previous
        # logic fails pretty easily without this fixture.
        def _process_ordered_values(self, statement):
            super_process_ordered_values(self, statement)

            tuples = list(self._dict_parameters.items())
            random.shuffle(tuples)
            self._dict_parameters = dict(tuples)

        dialect = default.StrCompileDialect()
        dialect.paramstyle = "qmark"
        dialect.positional = True

        with mock.patch.object(
            UpdateDMLState, "_process_ordered_values", _process_ordered_values
        ):
            yield

    def random_update_order_parameters():
        from sqlalchemy import ARRAY

        t = table(
            "foo",
            column("data1", ARRAY(Integer)),
            column("data2", ARRAY(Integer)),
            column("data3", ARRAY(Integer)),
            column("data4", ARRAY(Integer)),
        )

        idx_to_value = [
            (t.c.data1, 5, 7),
            (t.c.data2, 10, 18),
            (t.c.data3, 8, 4),
            (t.c.data4, 12, 14),
        ]

        def combinations():
            while True:
                random.shuffle(idx_to_value)
                yield list(idx_to_value)

        return testing.combinations(
            *[
                (t, combination)
                for i, combination in zip(range(10), combinations())
            ],
            argnames="t, idx_to_value"
        )

    @random_update_order_parameters()
    def test_update_to_expression_two(
        self, randomized_param_order_update, t, idx_to_value
    ):
        """test update from an expression.

        this logic is triggered currently by a left side that doesn't
        have a key.  The current supported use case is updating the index
        of a PostgreSQL ARRAY type.

        """

        dialect = default.StrCompileDialect()
        dialect.paramstyle = "qmark"
        dialect.positional = True

        stmt = t.update().ordered_values(
            *[(col[idx], val) for col, idx, val in idx_to_value]
        )

        self.assert_compile(
            stmt,
            "UPDATE foo SET %s"
            % (
                ", ".join(
                    "%s[?]=?" % col.key for col, idx, val in idx_to_value
                )
            ),
            dialect=dialect,
            checkpositional=tuple(
                itertools.chain.from_iterable(
                    (idx, val) for col, idx, val in idx_to_value
                )
            ),
        )

    def test_update_to_expression_three(self):
        # this test is from test_defaults but exercises a particular
        # parameter ordering issue
        metadata = MetaData()

        q = Table(
            "q",
            metadata,
            Column("x", Integer, default=2),
            Column("y", Integer, onupdate=5),
            Column("z", Integer),
        )

        p = Table(
            "p",
            metadata,
            Column("s", Integer),
            Column("t", Integer),
            Column("u", Integer, onupdate=1),
        )

        cte = (
            q.update().where(q.c.z == 1).values(x=7).returning(q.c.z).cte("c")
        )
        stmt = select(p.c.s, cte.c.z).where(p.c.s == cte.c.z)

        dialect = default.StrCompileDialect()
        dialect.paramstyle = "qmark"
        dialect.positional = True

        self.assert_compile(
            stmt,
            "WITH c AS (UPDATE q SET x=?, y=? WHERE q.z = ? RETURNING q.z) "
            "SELECT p.s, c.z FROM p, c WHERE p.s = c.z",
            checkpositional=(7, None, 1),
            dialect=dialect,
        )

    def test_update_bound_ordering(self):
        """test that bound parameters between the UPDATE and FROM clauses
        order correctly in different SQL compilation scenarios.

        """
        table1 = self.tables.mytable
        table2 = self.tables.myothertable
        sel = select(table2).where(table2.c.otherid == 5).alias()
        upd = (
            table1.update()
            .where(table1.c.name == sel.c.othername)
            .values(name="foo")
        )

        dialect = default.StrCompileDialect()
        dialect.positional = True
        self.assert_compile(
            upd,
            "UPDATE mytable SET name=:name FROM (SELECT "
            "myothertable.otherid AS otherid, "
            "myothertable.othername AS othername "
            "FROM myothertable "
            "WHERE myothertable.otherid = :otherid_1) AS anon_1 "
            "WHERE mytable.name = anon_1.othername",
            checkpositional=("foo", 5),
            dialect=dialect,
        )

        self.assert_compile(
            upd,
            "UPDATE mytable, (SELECT myothertable.otherid AS otherid, "
            "myothertable.othername AS othername "
            "FROM myothertable "
            "WHERE myothertable.otherid = %s) AS anon_1 SET mytable.name=%s "
            "WHERE mytable.name = anon_1.othername",
            checkpositional=(5, "foo"),
            dialect=mysql.dialect(),
        )


class UpdateFromCompileTest(
    _UpdateFromTestBase, fixtures.TablesTest, AssertsCompiledSQL
):
    __dialect__ = "default_enhanced"

    run_create_tables = run_inserts = run_deletes = None

    def test_alias_one(self):
        table1 = self.tables.mytable
        talias1 = table1.alias("t1")

        # this case is nonsensical.  the UPDATE is entirely
        # against the alias, but we name the table-bound column
        # in values.   The behavior here isn't really defined
        self.assert_compile(
            update(talias1)
            .where(talias1.c.myid == 7)
            .values({table1.c.name: "fred"}),
            "UPDATE mytable AS t1 "
            "SET name=:name "
            "WHERE t1.myid = :myid_1",
        )

    def test_alias_two(self):
        table1 = self.tables.mytable
        talias1 = table1.alias("t1")

        # Here, compared to
        # test_alias_one(), here we actually have UPDATE..FROM,
        # which is causing the "table1.c.name" param to be handled
        # as an "extra table", hence we see the full table name rendered.
        self.assert_compile(
            update(talias1)
            .where(table1.c.myid == 7)
            .values({table1.c.name: "fred"}),
            "UPDATE mytable AS t1 "
            "SET name=:mytable_name "
            "FROM mytable "
            "WHERE mytable.myid = :myid_1",
            checkparams={"mytable_name": "fred", "myid_1": 7},
        )

    def test_alias_two_mysql(self):
        table1 = self.tables.mytable
        talias1 = table1.alias("t1")

        self.assert_compile(
            update(talias1)
            .where(table1.c.myid == 7)
            .values({table1.c.name: "fred"}),
            "UPDATE mytable AS t1, mytable SET mytable.name=%s "
            "WHERE mytable.myid = %s",
            checkparams={"mytable_name": "fred", "myid_1": 7},
            dialect="mysql",
        )

    def test_update_from_multitable_same_name_mysql(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.assert_compile(
            users.update()
            .values(name="newname")
            .values({addresses.c.name: "new address"})
            .where(users.c.id == addresses.c.user_id),
            "UPDATE users, addresses SET addresses.name=%s, "
            "users.name=%s WHERE users.id = addresses.user_id",
            checkparams={"addresses_name": "new address", "name": "newname"},
            dialect="mysql",
        )

    def test_update_from_join_mysql_whereclause(self):
        users, addresses = self.tables.users, self.tables.addresses

        j = users.join(addresses)
        self.assert_compile(
            update(j)
            .values(name="newname")
            .where(addresses.c.email_address == "e1"),
            ""
            "UPDATE users "
            "INNER JOIN addresses ON users.id = addresses.user_id "
            "SET users.name=%s "
            "WHERE "
            "addresses.email_address = %s",
            checkparams={"email_address_1": "e1", "name": "newname"},
            dialect=mysql.dialect(),
        )

    def test_update_from_join_mysql_no_whereclause_one(self):
        users, addresses = self.tables.users, self.tables.addresses

        j = users.join(addresses)
        self.assert_compile(
            update(j).values(name="newname"),
            ""
            "UPDATE users "
            "INNER JOIN addresses ON users.id = addresses.user_id "
            "SET users.name=%s",
            checkparams={"name": "newname"},
            dialect=mysql.dialect(),
        )

    def test_update_from_join_mysql_no_whereclause_two(self):
        users, addresses = self.tables.users, self.tables.addresses

        j = users.join(addresses)
        self.assert_compile(
            update(j).values({users.c.name: addresses.c.email_address}),
            ""
            "UPDATE users "
            "INNER JOIN addresses ON users.id = addresses.user_id "
            "SET users.name=addresses.email_address",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_update_from_join_mysql_no_whereclause_three(self):
        users, addresses, dingalings = (
            self.tables.users,
            self.tables.addresses,
            self.tables.dingalings,
        )

        j = users.join(addresses).join(dingalings)
        self.assert_compile(
            update(j).values({users.c.name: dingalings.c.id}),
            ""
            "UPDATE users "
            "INNER JOIN addresses ON users.id = addresses.user_id "
            "INNER JOIN dingalings ON addresses.id = dingalings.address_id "
            "SET users.name=dingalings.id",
            checkparams={},
            dialect=mysql.dialect(),
        )

    def test_update_from_join_mysql_no_whereclause_four(self):
        users, addresses, dingalings = (
            self.tables.users,
            self.tables.addresses,
            self.tables.dingalings,
        )

        j = users.join(addresses).join(dingalings)

        self.assert_compile(
            update(j).values(name="foo"),
            ""
            "UPDATE users "
            "INNER JOIN addresses ON users.id = addresses.user_id "
            "INNER JOIN dingalings ON addresses.id = dingalings.address_id "
            "SET users.name=%s",
            checkparams={"name": "foo"},
            dialect=mysql.dialect(),
        )

    def test_render_table(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.assert_compile(
            users.update()
            .values(name="newname")
            .where(users.c.id == addresses.c.user_id)
            .where(addresses.c.email_address == "e1"),
            "UPDATE users "
            "SET name=:name FROM addresses "
            "WHERE "
            "users.id = addresses.user_id AND "
            "addresses.email_address = :email_address_1",
            checkparams={"email_address_1": "e1", "name": "newname"},
        )

    def test_render_multi_table(self):
        users = self.tables.users
        addresses = self.tables.addresses
        dingalings = self.tables.dingalings

        checkparams = {"email_address_1": "e1", "id_1": 2, "name": "newname"}

        self.assert_compile(
            users.update()
            .values(name="newname")
            .where(users.c.id == addresses.c.user_id)
            .where(addresses.c.email_address == "e1")
            .where(addresses.c.id == dingalings.c.address_id)
            .where(dingalings.c.id == 2),
            "UPDATE users "
            "SET name=:name "
            "FROM addresses, dingalings "
            "WHERE "
            "users.id = addresses.user_id AND "
            "addresses.email_address = :email_address_1 AND "
            "addresses.id = dingalings.address_id AND "
            "dingalings.id = :id_1",
            checkparams=checkparams,
        )

    def test_render_table_mysql(self):
        users, addresses = self.tables.users, self.tables.addresses

        self.assert_compile(
            users.update()
            .values(name="newname")
            .where(users.c.id == addresses.c.user_id)
            .where(addresses.c.email_address == "e1"),
            "UPDATE users, addresses "
            "SET users.name=%s "
            "WHERE "
            "users.id = addresses.user_id AND "
            "addresses.email_address = %s",
            checkparams={"email_address_1": "e1", "name": "newname"},
            dialect=mysql.dialect(),
        )

    def test_render_subquery(self):
        users, addresses = self.tables.users, self.tables.addresses

        checkparams = {"email_address_1": "e1", "id_1": 7, "name": "newname"}

        subq = (
            select(
                addresses.c.id, addresses.c.user_id, addresses.c.email_address
            )
            .where(addresses.c.id == 7)
            .alias()
        )
        self.assert_compile(
            users.update()
            .values(name="newname")
            .where(users.c.id == subq.c.user_id)
            .where(subq.c.email_address == "e1"),
            "UPDATE users "
            "SET name=:name FROM ("
            "SELECT "
            "addresses.id AS id, "
            "addresses.user_id AS user_id, "
            "addresses.email_address AS email_address "
            "FROM addresses "
            "WHERE addresses.id = :id_1"
            ") AS anon_1 "
            "WHERE users.id = anon_1.user_id "
            "AND anon_1.email_address = :email_address_1",
            checkparams=checkparams,
        )

    def test_correlation_to_extra(self):
        users, addresses = self.tables.users, self.tables.addresses

        stmt = (
            users.update()
            .values(name="newname")
            .where(users.c.id == addresses.c.user_id)
            .where(
                ~exists()
                .where(addresses.c.user_id == users.c.id)
                .where(addresses.c.email_address == "foo")
                .correlate(addresses)
            )
        )

        self.assert_compile(
            stmt,
            "UPDATE users SET name=:name FROM addresses WHERE "
            "users.id = addresses.user_id AND NOT "
            "(EXISTS (SELECT * FROM users WHERE addresses.user_id = users.id "
            "AND addresses.email_address = :email_address_1))",
        )

    def test_dont_correlate_to_extra(self):
        users, addresses = self.tables.users, self.tables.addresses

        stmt = (
            users.update()
            .values(name="newname")
            .where(users.c.id == addresses.c.user_id)
            .where(
                ~exists()
                .where(addresses.c.user_id == users.c.id)
                .where(addresses.c.email_address == "foo")
                .correlate()
            )
        )

        self.assert_compile(
            stmt,
            "UPDATE users SET name=:name FROM addresses WHERE "
            "users.id = addresses.user_id AND NOT "
            "(EXISTS (SELECT * FROM addresses, users "
            "WHERE addresses.user_id = users.id "
            "AND addresses.email_address = :email_address_1))",
        )

    def test_autocorrelate_error(self):
        users, addresses = self.tables.users, self.tables.addresses

        stmt = (
            users.update()
            .values(name="newname")
            .where(users.c.id == addresses.c.user_id)
            .where(
                ~exists()
                .where(addresses.c.user_id == users.c.id)
                .where(addresses.c.email_address == "foo")
            )
        )

        assert_raises_message(
            exc.InvalidRequestError,
            ".*returned no FROM clauses due to auto-correlation.*",
            stmt.compile,
            dialect=default.StrCompileDialect(),
        )


class UpdateFromRoundTripTest(_UpdateFromTestBase, fixtures.TablesTest):
    __backend__ = True

    @testing.requires.update_from
    def test_exec_two_table(self, connection):
        users, addresses = self.tables.users, self.tables.addresses

        connection.execute(
            addresses.update()
            .values(email_address=users.c.name)
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
        )

        expected = [
            (1, 7, "x", "jack@bean.com"),
            (2, 8, "x", "ed"),
            (3, 8, "x", "ed"),
            (4, 8, "x", "ed"),
            (5, 9, "x", "fred@fred.com"),
        ]
        self._assert_addresses(connection, addresses, expected)

    @testing.requires.update_from
    def test_exec_two_table_plus_alias(self, connection):
        users, addresses = self.tables.users, self.tables.addresses

        a1 = addresses.alias()
        connection.execute(
            addresses.update()
            .values(email_address=users.c.name)
            .where(users.c.id == a1.c.user_id)
            .where(users.c.name == "ed")
            .where(a1.c.id == addresses.c.id)
        )

        expected = [
            (1, 7, "x", "jack@bean.com"),
            (2, 8, "x", "ed"),
            (3, 8, "x", "ed"),
            (4, 8, "x", "ed"),
            (5, 9, "x", "fred@fred.com"),
        ]
        self._assert_addresses(connection, addresses, expected)

    @testing.requires.update_from
    def test_exec_three_table(self, connection):
        users = self.tables.users
        addresses = self.tables.addresses
        dingalings = self.tables.dingalings

        connection.execute(
            addresses.update()
            .values(email_address=users.c.name)
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
            .where(addresses.c.id == dingalings.c.address_id)
            .where(dingalings.c.id == 1)
        )

        expected = [
            (1, 7, "x", "jack@bean.com"),
            (2, 8, "x", "ed"),
            (3, 8, "x", "ed@bettyboop.com"),
            (4, 8, "x", "ed@lala.com"),
            (5, 9, "x", "fred@fred.com"),
        ]
        self._assert_addresses(connection, addresses, expected)

    @testing.only_on("mysql", "Multi table update")
    def test_exec_multitable(self, connection):
        users, addresses = self.tables.users, self.tables.addresses

        values = {addresses.c.email_address: "updated", users.c.name: "ed2"}

        connection.execute(
            addresses.update()
            .values(values)
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
        )

        expected = [
            (1, 7, "x", "jack@bean.com"),
            (2, 8, "x", "updated"),
            (3, 8, "x", "updated"),
            (4, 8, "x", "updated"),
            (5, 9, "x", "fred@fred.com"),
        ]
        self._assert_addresses(connection, addresses, expected)

        expected = [(7, "jack"), (8, "ed2"), (9, "fred"), (10, "chuck")]
        self._assert_users(connection, users, expected)

    @testing.only_on("mysql", "Multi table update")
    def test_exec_join_multitable(self, connection):
        users, addresses = self.tables.users, self.tables.addresses

        values = {addresses.c.email_address: "updated", users.c.name: "ed2"}

        connection.execute(
            update(users.join(addresses))
            .values(values)
            .where(users.c.name == "ed")
        )

        expected = [
            (1, 7, "x", "jack@bean.com"),
            (2, 8, "x", "updated"),
            (3, 8, "x", "updated"),
            (4, 8, "x", "updated"),
            (5, 9, "x", "fred@fred.com"),
        ]
        self._assert_addresses(connection, addresses, expected)

        expected = [(7, "jack"), (8, "ed2"), (9, "fred"), (10, "chuck")]
        self._assert_users(connection, users, expected)

    @testing.only_on("mysql", "Multi table update")
    def test_exec_multitable_same_name(self, connection):
        users, addresses = self.tables.users, self.tables.addresses

        values = {addresses.c.name: "ad_ed2", users.c.name: "ed2"}

        connection.execute(
            addresses.update()
            .values(values)
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
        )

        expected = [
            (1, 7, "x", "jack@bean.com"),
            (2, 8, "ad_ed2", "ed@wood.com"),
            (3, 8, "ad_ed2", "ed@bettyboop.com"),
            (4, 8, "ad_ed2", "ed@lala.com"),
            (5, 9, "x", "fred@fred.com"),
        ]
        self._assert_addresses(connection, addresses, expected)

        expected = [(7, "jack"), (8, "ed2"), (9, "fred"), (10, "chuck")]
        self._assert_users(connection, users, expected)

    def _assert_addresses(self, connection, addresses, expected):
        stmt = addresses.select().order_by(addresses.c.id)
        eq_(connection.execute(stmt).fetchall(), expected)

    def _assert_users(self, connection, users, expected):
        stmt = users.select().order_by(users.c.id)
        eq_(connection.execute(stmt).fetchall(), expected)


class UpdateFromMultiTableUpdateDefaultsTest(
    _UpdateFromTestBase, fixtures.TablesTest
):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30), nullable=False),
            Column("some_update", String(30), onupdate="im the update"),
        )

        Table(
            "addresses",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("email_address", String(50), nullable=False),
        )

        Table(
            "foobar",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_id", None, ForeignKey("users.id")),
            Column("data", String(30)),
            Column("some_update", String(30), onupdate="im the other update"),
        )

    @classmethod
    def fixtures(cls):
        return dict(
            users=(
                ("id", "name", "some_update"),
                (8, "ed", "value"),
                (9, "fred", "value"),
            ),
            addresses=(
                ("id", "user_id", "email_address"),
                (2, 8, "ed@wood.com"),
                (3, 8, "ed@bettyboop.com"),
                (4, 9, "fred@fred.com"),
            ),
            foobar=(
                ("id", "user_id", "data"),
                (2, 8, "d1"),
                (3, 8, "d2"),
                (4, 9, "d3"),
            ),
        )

    @testing.only_on("mysql", "Multi table update")
    def test_defaults_second_table(self, connection):
        users, addresses = self.tables.users, self.tables.addresses

        values = {addresses.c.email_address: "updated", users.c.name: "ed2"}

        ret = connection.execute(
            addresses.update()
            .values(values)
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
        )

        eq_(set(ret.prefetch_cols()), set([users.c.some_update]))

        expected = [
            (2, 8, "updated"),
            (3, 8, "updated"),
            (4, 9, "fred@fred.com"),
        ]
        self._assert_addresses(connection, addresses, expected)

        expected = [(8, "ed2", "im the update"), (9, "fred", "value")]
        self._assert_users(connection, users, expected)

    @testing.only_on("mysql", "Multi table update")
    def test_defaults_second_table_same_name(self, connection):
        users, foobar = self.tables.users, self.tables.foobar

        values = {foobar.c.data: foobar.c.data + "a", users.c.name: "ed2"}

        ret = connection.execute(
            users.update()
            .values(values)
            .where(users.c.id == foobar.c.user_id)
            .where(users.c.name == "ed")
        )

        eq_(
            set(ret.prefetch_cols()),
            set([users.c.some_update, foobar.c.some_update]),
        )

        expected = [
            (2, 8, "d1a", "im the other update"),
            (3, 8, "d2a", "im the other update"),
            (4, 9, "d3", None),
        ]
        self._assert_foobar(connection, foobar, expected)

        expected = [(8, "ed2", "im the update"), (9, "fred", "value")]
        self._assert_users(connection, users, expected)

    @testing.only_on("mysql", "Multi table update")
    def test_no_defaults_second_table(self, connection):
        users, addresses = self.tables.users, self.tables.addresses

        ret = connection.execute(
            addresses.update()
            .values({"email_address": users.c.name})
            .where(users.c.id == addresses.c.user_id)
            .where(users.c.name == "ed")
        )

        eq_(ret.prefetch_cols(), [])

        expected = [(2, 8, "ed"), (3, 8, "ed"), (4, 9, "fred@fred.com")]
        self._assert_addresses(connection, addresses, expected)

        # users table not actually updated, so no onupdate
        expected = [(8, "ed", "value"), (9, "fred", "value")]
        self._assert_users(connection, users, expected)

    def _assert_foobar(self, connection, foobar, expected):
        stmt = foobar.select().order_by(foobar.c.id)
        eq_(connection.execute(stmt).fetchall(), expected)

    def _assert_addresses(self, connection, addresses, expected):
        stmt = addresses.select().order_by(addresses.c.id)
        eq_(connection.execute(stmt).fetchall(), expected)

    def _assert_users(self, connection, users, expected):
        stmt = users.select().order_by(users.c.id)
        eq_(connection.execute(stmt).fetchall(), expected)
