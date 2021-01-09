from sqlalchemy import and_
from sqlalchemy import asc
from sqlalchemy import bindparam
from sqlalchemy import cast
from sqlalchemy import desc
from sqlalchemy import exc
from sqlalchemy import except_
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import INT
from sqlalchemy import Integer
from sqlalchemy import intersect
from sqlalchemy import literal
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import not_
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import sql
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import TypeDecorator
from sqlalchemy import union
from sqlalchemy import union_all
from sqlalchemy import VARCHAR
from sqlalchemy.engine import default
from sqlalchemy.sql import LABEL_STYLE_TABLENAME_PLUS_COL
from sqlalchemy.sql.selectable import LABEL_STYLE_NONE
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class QueryTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column(
                "user_id", INT, primary_key=True, test_needs_autoincrement=True
            ),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )
        Table(
            "addresses",
            metadata,
            Column(
                "address_id",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("user_id", Integer, ForeignKey("users.user_id")),
            Column("address", String(30)),
            test_needs_acid=True,
        )

        Table(
            "u2",
            metadata,
            Column("user_id", INT, primary_key=True),
            Column("user_name", VARCHAR(20)),
            test_needs_acid=True,
        )

    @testing.fails_on(
        "firebird", "kinterbasdb doesn't send full type information"
    )
    def test_order_by_label(self, connection):
        """test that a label within an ORDER BY works on each backend.

        This test should be modified to support [ticket:1068] when that ticket
        is implemented.  For now, you need to put the actual string in the
        ORDER BY.

        """

        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9, "user_name": "fred"},
            ],
        )

        concat = ("test: " + users.c.user_name).label("thedata")
        eq_(
            connection.execute(select(concat).order_by("thedata")).fetchall(),
            [("test: ed",), ("test: fred",), ("test: jack",)],
        )

        eq_(
            connection.execute(select(concat).order_by("thedata")).fetchall(),
            [("test: ed",), ("test: fred",), ("test: jack",)],
        )

        concat = ("test: " + users.c.user_name).label("thedata")
        eq_(
            connection.execute(
                select(concat).order_by(desc("thedata"))
            ).fetchall(),
            [("test: jack",), ("test: fred",), ("test: ed",)],
        )

    @testing.requires.order_by_label_with_expression
    def test_order_by_label_compound(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9, "user_name": "fred"},
            ],
        )

        concat = ("test: " + users.c.user_name).label("thedata")
        eq_(
            connection.execute(
                select(concat).order_by(literal_column("thedata") + "x")
            ).fetchall(),
            [("test: ed",), ("test: fred",), ("test: jack",)],
        )

    @testing.requires.boolean_col_expressions
    def test_or_and_as_columns(self, connection):
        true, false = literal(True), literal(False)

        eq_(connection.execute(select(and_(true, false))).scalar(), False)
        eq_(connection.execute(select(and_(true, true))).scalar(), True)
        eq_(connection.execute(select(or_(true, false))).scalar(), True)
        eq_(connection.execute(select(or_(false, false))).scalar(), False)
        eq_(
            connection.execute(select(not_(or_(false, false)))).scalar(),
            True,
        )

        row = connection.execute(
            select(or_(false, false).label("x"), and_(true, false).label("y"))
        ).first()
        assert row.x == False  # noqa
        assert row.y == False  # noqa

        row = connection.execute(
            select(or_(true, false).label("x"), and_(true, false).label("y"))
        ).first()
        assert row.x == True  # noqa
        assert row.y == False  # noqa

    def test_select_tuple(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            {"user_id": 1, "user_name": "apples"},
        )

        assert_raises_message(
            exc.CompileError,
            r"Most backends don't support SELECTing from a tuple\(\) object.",
            connection.execute,
            select(tuple_(users.c.user_id, users.c.user_name)),
        )

    def test_like_ops(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [
                {"user_id": 1, "user_name": "apples"},
                {"user_id": 2, "user_name": "oranges"},
                {"user_id": 3, "user_name": "bananas"},
                {"user_id": 4, "user_name": "legumes"},
                {"user_id": 5, "user_name": "hi % there"},
            ],
        )

        for expr, result in (
            (
                select(users.c.user_id).where(
                    users.c.user_name.startswith("apple")
                ),
                [(1,)],
            ),
            (
                select(users.c.user_id).where(
                    users.c.user_name.contains("i % t")
                ),
                [(5,)],
            ),
            (
                select(users.c.user_id).where(
                    users.c.user_name.endswith("anas")
                ),
                [(3,)],
            ),
            (
                select(users.c.user_id).where(
                    users.c.user_name.contains("i % t", escape="&")
                ),
                [(5,)],
            ),
        ):
            eq_(connection.execute(expr).fetchall(), result)

    @testing.requires.mod_operator_as_percent_sign
    @testing.emits_warning(".*now automatically escapes.*")
    def test_percents_in_text(self, connection):
        for expr, result in (
            (text("select 6 % 10"), 6),
            (text("select 17 % 10"), 7),
            (text("select '%'"), "%"),
            (text("select '%%'"), "%%"),
            (text("select '%%%'"), "%%%"),
            (text("select 'hello % world'"), "hello % world"),
        ):
            eq_(connection.scalar(expr), result)

    def test_ilike(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [
                {"user_id": 1, "user_name": "one"},
                {"user_id": 2, "user_name": "TwO"},
                {"user_id": 3, "user_name": "ONE"},
                {"user_id": 4, "user_name": "OnE"},
            ],
        )

        eq_(
            connection.execute(
                select(users.c.user_id).where(users.c.user_name.ilike("one"))
            ).fetchall(),
            [(1,), (3,), (4,)],
        )

        eq_(
            connection.execute(
                select(users.c.user_id).where(users.c.user_name.ilike("TWO"))
            ).fetchall(),
            [(2,)],
        )

        if testing.against("postgresql"):
            eq_(
                connection.execute(
                    select(users.c.user_id).where(
                        users.c.user_name.like("one")
                    )
                ).fetchall(),
                [(1,)],
            )
            eq_(
                connection.execute(
                    select(users.c.user_id).where(
                        users.c.user_name.like("TWO")
                    )
                ).fetchall(),
                [],
            )

    def test_compiled_execute(self, connection):
        users = self.tables.users
        connection.execute(users.insert(), dict(user_id=7, user_name="jack"))
        s = (
            select(users)
            .where(users.c.user_id == bindparam("id"))
            .compile(connection)
        )
        eq_(connection.execute(s, dict(id=7)).first()._mapping["user_id"], 7)

    def test_compiled_insert_execute(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert().compile(connection),
            dict(user_id=7, user_name="jack"),
        )
        s = (
            select(users)
            .where(users.c.user_id == bindparam("id"))
            .compile(connection)
        )
        eq_(connection.execute(s, dict(id=7)).first()._mapping["user_id"], 7)

    def test_repeated_bindparams(self, connection):
        """Tests that a BindParam can be used more than once.

        This should be run for DB-APIs with both positional and named
        paramstyles.
        """
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=7, user_name="jack"))
        connection.execute(users.insert(), dict(user_id=8, user_name="fred"))

        u = bindparam("userid")
        s = users.select(and_(users.c.user_name == u, users.c.user_name == u))
        r = connection.execute(s, dict(userid="fred")).fetchall()
        assert len(r) == 1

    def test_bindparam_detection(self):
        dialect = default.DefaultDialect(paramstyle="qmark")

        def prep(q):
            return str(sql.text(q).compile(dialect=dialect))

        def a_eq(got, wanted):
            if got != wanted:
                print("Wanted %s" % wanted)
                print("Received %s" % got)
            self.assert_(got == wanted, got)

        a_eq(prep("select foo"), "select foo")
        a_eq(prep("time='12:30:00'"), "time='12:30:00'")
        a_eq(prep("time='12:30:00'"), "time='12:30:00'")
        a_eq(prep(":this:that"), ":this:that")
        a_eq(prep(":this :that"), "? ?")
        a_eq(prep("(:this),(:that :other)"), "(?),(? ?)")
        a_eq(prep("(:this),(:that:other)"), "(?),(:that:other)")
        a_eq(prep("(:this),(:that,:other)"), "(?),(?,?)")
        a_eq(prep("(:that_:other)"), "(:that_:other)")
        a_eq(prep("(:that_ :other)"), "(? ?)")
        a_eq(prep("(:that_other)"), "(?)")
        a_eq(prep("(:that$other)"), "(?)")
        a_eq(prep("(:that$:other)"), "(:that$:other)")
        a_eq(prep(".:that$ :other."), ".? ?.")

        a_eq(prep(r"select \foo"), r"select \foo")
        a_eq(prep(r"time='12\:30:00'"), r"time='12\:30:00'")
        a_eq(prep(r":this \:that"), "? :that")
        a_eq(prep(r"(\:that$other)"), "(:that$other)")
        a_eq(prep(r".\:that$ :other."), ".:that$ ?.")

    @testing.requires.standalone_binds
    def test_select_from_bindparam(self, connection):
        """Test result row processing when selecting from a plain bind
        param."""

        class MyInteger(TypeDecorator):
            impl = Integer

            def process_bind_param(self, value, dialect):
                return int(value[4:])

            def process_result_value(self, value, dialect):
                return "INT_%d" % value

        eq_(
            connection.scalar(select(cast("INT_5", type_=MyInteger))),
            "INT_5",
        )
        eq_(
            connection.scalar(
                select(cast("INT_5", type_=MyInteger).label("foo"))
            ),
            "INT_5",
        )

    def test_order_by(self, connection):
        """Exercises ORDER BY clause generation.

        Tests simple, compound, aliased and DESC clauses.
        """

        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1, user_name="c"))
        connection.execute(users.insert(), dict(user_id=2, user_name="b"))
        connection.execute(users.insert(), dict(user_id=3, user_name="a"))

        def a_eq(executable, wanted):
            got = list(connection.execute(executable))
            eq_(got, wanted)

        for labels in False, True:
            label_style = (
                LABEL_STYLE_NONE
                if labels is False
                else LABEL_STYLE_TABLENAME_PLUS_COL
            )

            def go(stmt):
                if labels:
                    stmt = stmt.set_label_style(label_style)
                return stmt

            a_eq(
                users.select(order_by=[users.c.user_id]).set_label_style(
                    label_style
                ),
                [(1, "c"), (2, "b"), (3, "a")],
            )

            a_eq(
                users.select(
                    order_by=[users.c.user_name, users.c.user_id],
                ).set_label_style(label_style),
                [(3, "a"), (2, "b"), (1, "c")],
            )

            a_eq(
                go(
                    select(users.c.user_id.label("foo")).order_by(
                        users.c.user_id
                    )
                ),
                [(1,), (2,), (3,)],
            )

            a_eq(
                go(
                    select(
                        users.c.user_id.label("foo"), users.c.user_name
                    ).order_by(users.c.user_name, users.c.user_id),
                ),
                [(3, "a"), (2, "b"), (1, "c")],
            )

            a_eq(
                users.select(
                    distinct=True,
                    order_by=[users.c.user_id],
                ).set_label_style(label_style),
                [(1, "c"), (2, "b"), (3, "a")],
            )

            a_eq(
                go(
                    select(users.c.user_id.label("foo"))
                    .distinct()
                    .order_by(users.c.user_id),
                ),
                [(1,), (2,), (3,)],
            )

            a_eq(
                go(
                    select(
                        users.c.user_id.label("a"),
                        users.c.user_id.label("b"),
                        users.c.user_name,
                    ).order_by(users.c.user_id),
                ),
                [(1, 1, "c"), (2, 2, "b"), (3, 3, "a")],
            )

            a_eq(
                users.select(
                    distinct=True,
                    order_by=[desc(users.c.user_id)],
                ).set_label_style(label_style),
                [(3, "a"), (2, "b"), (1, "c")],
            )

            a_eq(
                go(
                    select(users.c.user_id.label("foo"))
                    .distinct()
                    .order_by(users.c.user_id.desc()),
                ),
                [(3,), (2,), (1,)],
            )

    @testing.requires.nullsordering
    def test_order_by_nulls(self, connection):
        """Exercises ORDER BY clause generation.

        Tests simple, compound, aliased and DESC clauses.
        """

        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=1))
        connection.execute(users.insert(), dict(user_id=2, user_name="b"))
        connection.execute(users.insert(), dict(user_id=3, user_name="a"))

        def a_eq(executable, wanted):
            got = list(connection.execute(executable))
            eq_(got, wanted)

        for labels in False, True:
            label_style = (
                LABEL_STYLE_NONE
                if labels is False
                else LABEL_STYLE_TABLENAME_PLUS_COL
            )
            a_eq(
                users.select(
                    order_by=[users.c.user_name.nulls_first()],
                ).set_label_style(label_style),
                [(1, None), (3, "a"), (2, "b")],
            )

            a_eq(
                users.select(
                    order_by=[users.c.user_name.nulls_last()],
                ).set_label_style(label_style),
                [(3, "a"), (2, "b"), (1, None)],
            )

            a_eq(
                users.select(
                    order_by=[asc(users.c.user_name).nulls_first()],
                ).set_label_style(label_style),
                [(1, None), (3, "a"), (2, "b")],
            )

            a_eq(
                users.select(
                    order_by=[asc(users.c.user_name).nulls_last()],
                ).set_label_style(label_style),
                [(3, "a"), (2, "b"), (1, None)],
            )

            a_eq(
                users.select(
                    order_by=[users.c.user_name.desc().nulls_first()],
                ).set_label_style(label_style),
                [(1, None), (2, "b"), (3, "a")],
            )

            a_eq(
                users.select(
                    order_by=[users.c.user_name.desc().nulls_last()],
                ).set_label_style(label_style),
                [(2, "b"), (3, "a"), (1, None)],
            )

            a_eq(
                users.select(
                    order_by=[desc(users.c.user_name).nulls_first()],
                ).set_label_style(label_style),
                [(1, None), (2, "b"), (3, "a")],
            )

            a_eq(
                users.select(
                    order_by=[desc(users.c.user_name).nulls_last()],
                ).set_label_style(label_style),
                [(2, "b"), (3, "a"), (1, None)],
            )

            a_eq(
                users.select(
                    order_by=[
                        users.c.user_name.nulls_first(),
                        users.c.user_id,
                    ],
                ).set_label_style(label_style),
                [(1, None), (3, "a"), (2, "b")],
            )

            a_eq(
                users.select(
                    order_by=[users.c.user_name.nulls_last(), users.c.user_id],
                ).set_label_style(label_style),
                [(3, "a"), (2, "b"), (1, None)],
            )

    def test_in_filtering(self, connection):
        """test the behavior of the in_() function."""
        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=7, user_name="jack"))
        connection.execute(users.insert(), dict(user_id=8, user_name="fred"))
        connection.execute(users.insert(), dict(user_id=9, user_name=None))

        s = users.select(users.c.user_name.in_([]))
        r = connection.execute(s).fetchall()
        # No username is in empty set
        assert len(r) == 0

        s = users.select(not_(users.c.user_name.in_([])))
        r = connection.execute(s).fetchall()
        assert len(r) == 3

        s = users.select(users.c.user_name.in_(["jack", "fred"]))
        r = connection.execute(s).fetchall()
        assert len(r) == 2

        s = users.select(not_(users.c.user_name.in_(["jack", "fred"])))
        r = connection.execute(s).fetchall()
        # Null values are not outside any set
        assert len(r) == 0

    def test_expanding_in(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [
                dict(user_id=7, user_name="jack"),
                dict(user_id=8, user_name="fred"),
                dict(user_id=9, user_name=None),
            ],
        )

        stmt = (
            select(users)
            .where(users.c.user_name.in_(bindparam("uname", expanding=True)))
            .order_by(users.c.user_id)
        )

        eq_(
            connection.execute(stmt, {"uname": ["jack"]}).fetchall(),
            [(7, "jack")],
        )

        eq_(
            connection.execute(stmt, {"uname": ["jack", "fred"]}).fetchall(),
            [(7, "jack"), (8, "fred")],
        )

        eq_(connection.execute(stmt, {"uname": []}).fetchall(), [])

        assert_raises_message(
            exc.StatementError,
            "'expanding' parameters can't be used with executemany()",
            connection.execute,
            users.update().where(
                users.c.user_name.in_(bindparam("uname", expanding=True))
            ),
            [{"uname": ["fred"]}, {"uname": ["ed"]}],
        )

    @testing.requires.no_quoting_special_bind_names
    def test_expanding_in_special_chars(self, connection):
        users = self.tables.users
        connection.execute(
            users.insert(),
            [
                dict(user_id=7, user_name="jack"),
                dict(user_id=8, user_name="fred"),
            ],
        )

        stmt = (
            select(users)
            .where(users.c.user_name.in_(bindparam("u35", expanding=True)))
            .where(users.c.user_id == bindparam("u46"))
            .order_by(users.c.user_id)
        )

        eq_(
            connection.execute(
                stmt, {"u35": ["jack", "fred"], "u46": 7}
            ).fetchall(),
            [(7, "jack")],
        )

        stmt = (
            select(users)
            .where(users.c.user_name.in_(bindparam("u.35", expanding=True)))
            .where(users.c.user_id == bindparam("u.46"))
            .order_by(users.c.user_id)
        )

        eq_(
            connection.execute(
                stmt, {"u.35": ["jack", "fred"], "u.46": 7}
            ).fetchall(),
            [(7, "jack")],
        )

    def test_expanding_in_multiple(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=7, user_name="jack"),
                dict(user_id=8, user_name="fred"),
                dict(user_id=9, user_name="ed"),
            ],
        )

        stmt = (
            select(users)
            .where(users.c.user_name.in_(bindparam("uname", expanding=True)))
            .where(users.c.user_id.in_(bindparam("userid", expanding=True)))
            .order_by(users.c.user_id)
        )

        eq_(
            connection.execute(
                stmt, {"uname": ["jack", "fred", "ed"], "userid": [8, 9]}
            ).fetchall(),
            [(8, "fred"), (9, "ed")],
        )

    def test_expanding_in_repeated(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=7, user_name="jack"),
                dict(user_id=8, user_name="fred"),
                dict(user_id=9, user_name="ed"),
            ],
        )

        stmt = (
            select(users)
            .where(
                users.c.user_name.in_(bindparam("uname", expanding=True))
                | users.c.user_name.in_(bindparam("uname2", expanding=True))
            )
            .where(users.c.user_id == 8)
        )
        stmt = stmt.union(
            select(users)
            .where(
                users.c.user_name.in_(bindparam("uname", expanding=True))
                | users.c.user_name.in_(bindparam("uname2", expanding=True))
            )
            .where(users.c.user_id == 9)
        ).order_by("user_id")

        eq_(
            connection.execute(
                stmt,
                {
                    "uname": ["jack", "fred"],
                    "uname2": ["ed"],
                    "userid": [8, 9],
                },
            ).fetchall(),
            [(8, "fred"), (9, "ed")],
        )

    @testing.requires.tuple_in
    def test_expanding_in_composite(self, connection):
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                dict(user_id=7, user_name="jack"),
                dict(user_id=8, user_name="fred"),
                dict(user_id=9, user_name=None),
            ],
        )

        stmt = (
            select(users)
            .where(
                tuple_(users.c.user_id, users.c.user_name).in_(
                    bindparam("uname", expanding=True)
                )
            )
            .order_by(users.c.user_id)
        )

        eq_(
            connection.execute(stmt, {"uname": [(7, "jack")]}).fetchall(),
            [(7, "jack")],
        )

        eq_(
            connection.execute(
                stmt, {"uname": [(7, "jack"), (8, "fred")]}
            ).fetchall(),
            [(7, "jack"), (8, "fred")],
        )

    def test_expanding_in_dont_alter_compiled(self, connection):
        """test for issue #5048 """

        class NameWithProcess(TypeDecorator):
            impl = String

            def process_bind_param(self, value, dialect):
                return value[3:]

        users = Table(
            "users",
            MetaData(),
            Column("user_id", Integer, primary_key=True),
            Column("user_name", NameWithProcess()),
        )

        connection.execute(
            users.insert(),
            [
                dict(user_id=7, user_name="AB jack"),
                dict(user_id=8, user_name="BE fred"),
                dict(user_id=9, user_name="GP ed"),
            ],
        )

        stmt = (
            select(users)
            .where(users.c.user_name.in_(bindparam("uname", expanding=True)))
            .order_by(users.c.user_id)
        )

        compiled = stmt.compile(testing.db)
        eq_(len(compiled._bind_processors), 1)

        eq_(
            connection.execute(
                compiled, {"uname": ["HJ jack", "RR fred"]}
            ).fetchall(),
            [(7, "jack"), (8, "fred")],
        )

        eq_(len(compiled._bind_processors), 1)

    @testing.fails_on("firebird", "uses sql-92 rules")
    @testing.fails_on("sybase", "uses sql-92 rules")
    @testing.skip_if(["mssql"])
    def test_bind_in(self, connection):
        """test calling IN against a bind parameter.

        this isn't allowed on several platforms since we
        generate ? = ?.

        """

        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=7, user_name="jack"))
        connection.execute(users.insert(), dict(user_id=8, user_name="fred"))
        connection.execute(users.insert(), dict(user_id=9, user_name=None))

        u = bindparam("search_key", type_=String)

        s = users.select(not_(u.in_([])))
        r = connection.execute(s, dict(search_key="john")).fetchall()
        assert len(r) == 3
        r = connection.execute(s, dict(search_key=None)).fetchall()
        assert len(r) == 3

    def test_literal_in(self, connection):
        """similar to test_bind_in but use a bind with a value."""

        users = self.tables.users

        connection.execute(users.insert(), dict(user_id=7, user_name="jack"))
        connection.execute(users.insert(), dict(user_id=8, user_name="fred"))
        connection.execute(users.insert(), dict(user_id=9, user_name=None))

        s = users.select(not_(literal("john").in_([])))
        r = connection.execute(s).fetchall()
        assert len(r) == 3

    @testing.requires.boolean_col_expressions
    def test_empty_in_filtering_static(self, connection):
        """test the behavior of the in_() function when
        comparing against an empty collection, specifically
        that a proper boolean value is generated.

        """
        users = self.tables.users

        connection.execute(
            users.insert(),
            [
                {"user_id": 7, "user_name": "jack"},
                {"user_id": 8, "user_name": "ed"},
                {"user_id": 9, "user_name": None},
            ],
        )

        s = users.select(users.c.user_name.in_([]) == True)  # noqa
        r = connection.execute(s).fetchall()
        assert len(r) == 0
        s = users.select(users.c.user_name.in_([]) == False)  # noqa
        r = connection.execute(s).fetchall()
        assert len(r) == 3
        s = users.select(users.c.user_name.in_([]) == None)  # noqa
        r = connection.execute(s).fetchall()
        assert len(r) == 0


class RequiredBindTest(fixtures.TablesTest):
    run_create_tables = None
    run_deletes = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "foo",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("data", String(50)),
            Column("x", Integer),
        )

    def _assert_raises(self, stmt, params):
        with testing.db.connect() as conn:
            assert_raises_message(
                exc.StatementError,
                "A value is required for bind parameter 'x'",
                conn.execute,
                stmt,
                params,
            )

    def test_insert(self):
        stmt = self.tables.foo.insert().values(
            x=bindparam("x"), data=bindparam("data")
        )
        self._assert_raises(stmt, {"data": "data"})

    def test_select_where(self):
        stmt = (
            select(self.tables.foo)
            .where(self.tables.foo.c.data == bindparam("data"))
            .where(self.tables.foo.c.x == bindparam("x"))
        )
        self._assert_raises(stmt, {"data": "data"})

    @testing.requires.standalone_binds
    def test_select_columns(self):
        stmt = select(bindparam("data"), bindparam("x"))
        self._assert_raises(stmt, {"data": "data"})

    def test_text(self):
        stmt = text("select * from foo where x=:x and data=:data1")
        self._assert_raises(stmt, {"data1": "data"})

    def test_required_flag(self):
        is_(bindparam("foo").required, True)
        is_(bindparam("foo", required=False).required, False)
        is_(bindparam("foo", "bar").required, False)
        is_(bindparam("foo", "bar", required=True).required, True)

        def c():
            return None

        is_(bindparam("foo", callable_=c, required=True).required, True)
        is_(bindparam("foo", callable_=c).required, False)
        is_(bindparam("foo", callable_=c, required=False).required, False)


class LimitTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "users",
            metadata,
            Column("user_id", INT, primary_key=True),
            Column("user_name", VARCHAR(20)),
        )
        Table(
            "addresses",
            metadata,
            Column("address_id", Integer, primary_key=True),
            Column("user_id", Integer, ForeignKey("users.user_id")),
            Column("address", String(30)),
        )

    @classmethod
    def insert_data(cls, connection):
        users, addresses = cls.tables("users", "addresses")
        conn = connection
        conn.execute(users.insert(), dict(user_id=1, user_name="john"))
        conn.execute(
            addresses.insert(), dict(address_id=1, user_id=1, address="addr1")
        )
        conn.execute(users.insert(), dict(user_id=2, user_name="jack"))
        conn.execute(
            addresses.insert(), dict(address_id=2, user_id=2, address="addr1")
        )
        conn.execute(users.insert(), dict(user_id=3, user_name="ed"))
        conn.execute(
            addresses.insert(), dict(address_id=3, user_id=3, address="addr2")
        )
        conn.execute(users.insert(), dict(user_id=4, user_name="wendy"))
        conn.execute(
            addresses.insert(), dict(address_id=4, user_id=4, address="addr3")
        )
        conn.execute(users.insert(), dict(user_id=5, user_name="laura"))
        conn.execute(
            addresses.insert(), dict(address_id=5, user_id=5, address="addr4")
        )
        conn.execute(users.insert(), dict(user_id=6, user_name="ralph"))
        conn.execute(
            addresses.insert(), dict(address_id=6, user_id=6, address="addr5")
        )
        conn.execute(users.insert(), dict(user_id=7, user_name="fido"))
        conn.execute(
            addresses.insert(), dict(address_id=7, user_id=7, address="addr5")
        )

    def test_select_limit(self, connection):
        users, addresses = self.tables("users", "addresses")
        r = connection.execute(
            users.select(limit=3, order_by=[users.c.user_id])
        ).fetchall()
        self.assert_(r == [(1, "john"), (2, "jack"), (3, "ed")], repr(r))

    @testing.requires.offset
    def test_select_limit_offset(self, connection):
        """Test the interaction between limit and offset"""

        users, addresses = self.tables("users", "addresses")

        r = connection.execute(
            users.select(limit=3, offset=2, order_by=[users.c.user_id])
        ).fetchall()
        self.assert_(r == [(3, "ed"), (4, "wendy"), (5, "laura")])
        r = connection.execute(
            users.select(offset=5, order_by=[users.c.user_id])
        ).fetchall()
        self.assert_(r == [(6, "ralph"), (7, "fido")])

    def test_select_distinct_limit(self, connection):
        """Test the interaction between limit and distinct"""

        users, addresses = self.tables("users", "addresses")

        r = sorted(
            [
                x[0]
                for x in connection.execute(
                    select(addresses.c.address).distinct().limit(3)
                )
            ]
        )
        self.assert_(len(r) == 3, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2], repr(r))

    @testing.requires.offset
    def test_select_distinct_offset(self, connection):
        """Test the interaction between distinct and offset"""

        users, addresses = self.tables("users", "addresses")

        r = sorted(
            [
                x[0]
                for x in connection.execute(
                    select(addresses.c.address)
                    .distinct()
                    .offset(1)
                    .order_by(addresses.c.address)
                ).fetchall()
            ]
        )
        eq_(len(r), 4)
        self.assert_(r[0] != r[1] and r[1] != r[2] and r[2] != [3], repr(r))

    @testing.requires.offset
    def test_select_distinct_limit_offset(self, connection):
        """Test the interaction between limit and limit/offset"""

        users, addresses = self.tables("users", "addresses")

        r = connection.execute(
            select(addresses.c.address)
            .order_by(addresses.c.address)
            .distinct()
            .offset(2)
            .limit(3)
        ).fetchall()
        self.assert_(len(r) == 3, repr(r))
        self.assert_(r[0] != r[1] and r[1] != r[2], repr(r))


class CompoundTest(fixtures.TablesTest):

    """test compound statements like UNION, INTERSECT, particularly their
    ability to nest on different databases."""

    __backend__ = True

    run_inserts = "each"

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column(
                "col1",
                Integer,
                test_needs_autoincrement=True,
                primary_key=True,
            ),
            Column("col2", String(30)),
            Column("col3", String(40)),
            Column("col4", String(30)),
        )
        Table(
            "t2",
            metadata,
            Column(
                "col1",
                Integer,
                test_needs_autoincrement=True,
                primary_key=True,
            ),
            Column("col2", String(30)),
            Column("col3", String(40)),
            Column("col4", String(30)),
        )
        Table(
            "t3",
            metadata,
            Column(
                "col1",
                Integer,
                test_needs_autoincrement=True,
                primary_key=True,
            ),
            Column("col2", String(30)),
            Column("col3", String(40)),
            Column("col4", String(30)),
        )

    @classmethod
    def insert_data(cls, connection):
        t1, t2, t3 = cls.tables("t1", "t2", "t3")
        conn = connection
        conn.execute(
            t1.insert(),
            [
                dict(col2="t1col2r1", col3="aaa", col4="aaa"),
                dict(col2="t1col2r2", col3="bbb", col4="bbb"),
                dict(col2="t1col2r3", col3="ccc", col4="ccc"),
            ],
        )
        conn.execute(
            t2.insert(),
            [
                dict(col2="t2col2r1", col3="aaa", col4="bbb"),
                dict(col2="t2col2r2", col3="bbb", col4="ccc"),
                dict(col2="t2col2r3", col3="ccc", col4="aaa"),
            ],
        )
        conn.execute(
            t3.insert(),
            [
                dict(col2="t3col2r1", col3="aaa", col4="ccc"),
                dict(col2="t3col2r2", col3="bbb", col4="aaa"),
                dict(col2="t3col2r3", col3="ccc", col4="bbb"),
            ],
        )

    def _fetchall_sorted(self, executed):
        return sorted([tuple(row) for row in executed.fetchall()])

    @testing.requires.subqueries
    def test_union(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")
        (s1, s2) = (
            select(t1.c.col3.label("col3"), t1.c.col4.label("col4")).where(
                t1.c.col2.in_(["t1col2r1", "t1col2r2"]),
            ),
            select(t2.c.col3.label("col3"), t2.c.col4.label("col4")).where(
                t2.c.col2.in_(["t2col2r2", "t2col2r3"]),
            ),
        )
        u = union(s1, s2)

        wanted = [
            ("aaa", "aaa"),
            ("bbb", "bbb"),
            ("bbb", "ccc"),
            ("ccc", "aaa"),
        ]
        found1 = self._fetchall_sorted(connection.execute(u))
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(
            connection.execute(u.alias("bar").select())
        )
        eq_(found2, wanted)

    @testing.fails_on("firebird", "doesn't like ORDER BY with UNIONs")
    def test_union_ordered(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        (s1, s2) = (
            select(t1.c.col3.label("col3"), t1.c.col4.label("col4")).where(
                t1.c.col2.in_(["t1col2r1", "t1col2r2"]),
            ),
            select(t2.c.col3.label("col3"), t2.c.col4.label("col4")).where(
                t2.c.col2.in_(["t2col2r2", "t2col2r3"]),
            ),
        )
        u = union(s1, s2, order_by=["col3", "col4"])

        wanted = [
            ("aaa", "aaa"),
            ("bbb", "bbb"),
            ("bbb", "ccc"),
            ("ccc", "aaa"),
        ]
        eq_(connection.execute(u).fetchall(), wanted)

    @testing.fails_on("firebird", "doesn't like ORDER BY with UNIONs")
    @testing.requires.subqueries
    def test_union_ordered_alias(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        (s1, s2) = (
            select(t1.c.col3.label("col3"), t1.c.col4.label("col4")).where(
                t1.c.col2.in_(["t1col2r1", "t1col2r2"]),
            ),
            select(t2.c.col3.label("col3"), t2.c.col4.label("col4")).where(
                t2.c.col2.in_(["t2col2r2", "t2col2r3"]),
            ),
        )
        u = union(s1, s2, order_by=["col3", "col4"])

        wanted = [
            ("aaa", "aaa"),
            ("bbb", "bbb"),
            ("bbb", "ccc"),
            ("ccc", "aaa"),
        ]
        eq_(connection.execute(u.alias("bar").select()).fetchall(), wanted)

    @testing.crashes("oracle", "FIXME: unknown, verify not fails_on")
    @testing.fails_on(
        "firebird",
        "has trouble extracting anonymous column from union subquery",
    )
    @testing.fails_on(
        testing.requires._mysql_not_mariadb_104, "FIXME: unknown"
    )
    @testing.fails_on("sqlite", "FIXME: unknown")
    def test_union_all(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        e = union_all(
            select(t1.c.col3),
            union(select(t1.c.col3), select(t1.c.col3)),
        )

        wanted = [("aaa",), ("aaa",), ("bbb",), ("bbb",), ("ccc",), ("ccc",)]
        found1 = self._fetchall_sorted(connection.execute(e))
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(
            connection.execute(e.alias("foo").select())
        )
        eq_(found2, wanted)

    def test_union_all_lightweight(self, connection):
        """like test_union_all, but breaks the sub-union into
        a subquery with an explicit column reference on the outside,
        more palatable to a wider variety of engines.

        """

        t1, t2, t3 = self.tables("t1", "t2", "t3")

        u = union(select(t1.c.col3), select(t1.c.col3)).alias()

        e = union_all(select(t1.c.col3), select(u.c.col3))

        wanted = [("aaa",), ("aaa",), ("bbb",), ("bbb",), ("ccc",), ("ccc",)]
        found1 = self._fetchall_sorted(connection.execute(e))
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(
            connection.execute(e.alias("foo").select())
        )
        eq_(found2, wanted)

    @testing.requires.intersect
    def test_intersect(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        i = intersect(
            select(t2.c.col3, t2.c.col4),
            select(t2.c.col3, t2.c.col4).where(t2.c.col4 == t3.c.col3),
        )

        wanted = [("aaa", "bbb"), ("bbb", "ccc"), ("ccc", "aaa")]

        found1 = self._fetchall_sorted(connection.execute(i))
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(
            connection.execute(i.alias("bar").select())
        )
        eq_(found2, wanted)

    @testing.requires.except_
    @testing.fails_on("sqlite", "Can't handle this style of nesting")
    def test_except_style1(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        e = except_(
            union(
                select(t1.c.col3, t1.c.col4),
                select(t2.c.col3, t2.c.col4),
                select(t3.c.col3, t3.c.col4),
            ),
            select(t2.c.col3, t2.c.col4),
        )

        wanted = [
            ("aaa", "aaa"),
            ("aaa", "ccc"),
            ("bbb", "aaa"),
            ("bbb", "bbb"),
            ("ccc", "bbb"),
            ("ccc", "ccc"),
        ]

        found = self._fetchall_sorted(connection.execute(e.alias().select()))
        eq_(found, wanted)

    @testing.requires.except_
    def test_except_style2(self, connection):
        # same as style1, but add alias().select() to the except_().
        # sqlite can handle it now.

        t1, t2, t3 = self.tables("t1", "t2", "t3")

        e = except_(
            union(
                select(t1.c.col3, t1.c.col4),
                select(t2.c.col3, t2.c.col4),
                select(t3.c.col3, t3.c.col4),
            )
            .alias()
            .select(),
            select(t2.c.col3, t2.c.col4),
        )

        wanted = [
            ("aaa", "aaa"),
            ("aaa", "ccc"),
            ("bbb", "aaa"),
            ("bbb", "bbb"),
            ("ccc", "bbb"),
            ("ccc", "ccc"),
        ]

        found1 = self._fetchall_sorted(connection.execute(e))
        eq_(found1, wanted)

        found2 = self._fetchall_sorted(connection.execute(e.alias().select()))
        eq_(found2, wanted)

    @testing.fails_on(
        ["sqlite", testing.requires._mysql_not_mariadb_104],
        "Can't handle this style of nesting",
    )
    @testing.requires.except_
    def test_except_style3(self, connection):
        # aaa, bbb, ccc - (aaa, bbb, ccc - (ccc)) = ccc
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        e = except_(
            select(t1.c.col3),  # aaa, bbb, ccc
            except_(
                select(t2.c.col3),  # aaa, bbb, ccc
                select(t3.c.col3).where(t3.c.col3 == "ccc"),  # ccc
            ),
        )
        eq_(connection.execute(e).fetchall(), [("ccc",)])
        eq_(connection.execute(e.alias("foo").select()).fetchall(), [("ccc",)])

    @testing.requires.except_
    def test_except_style4(self, connection):
        # aaa, bbb, ccc - (aaa, bbb, ccc - (ccc)) = ccc
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        e = except_(
            select(t1.c.col3),  # aaa, bbb, ccc
            except_(
                select(t2.c.col3),  # aaa, bbb, ccc
                select(t3.c.col3).where(t3.c.col3 == "ccc"),  # ccc
            )
            .alias()
            .select(),
        )

        eq_(connection.execute(e).fetchall(), [("ccc",)])
        eq_(connection.execute(e.alias().select()).fetchall(), [("ccc",)])

    @testing.requires.intersect
    @testing.fails_on(
        ["sqlite", testing.requires._mysql_not_mariadb_104],
        "sqlite can't handle leading parenthesis",
    )
    def test_intersect_unions(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        u = intersect(
            union(select(t1.c.col3, t1.c.col4), select(t3.c.col3, t3.c.col4)),
            union(select(t2.c.col3, t2.c.col4), select(t3.c.col3, t3.c.col4))
            .alias()
            .select(),
        )
        wanted = [("aaa", "ccc"), ("bbb", "aaa"), ("ccc", "bbb")]
        found = self._fetchall_sorted(connection.execute(u))

        eq_(found, wanted)

    @testing.requires.intersect
    def test_intersect_unions_2(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        u = intersect(
            union(select(t1.c.col3, t1.c.col4), select(t3.c.col3, t3.c.col4))
            .alias()
            .select(),
            union(select(t2.c.col3, t2.c.col4), select(t3.c.col3, t3.c.col4))
            .alias()
            .select(),
        )
        wanted = [("aaa", "ccc"), ("bbb", "aaa"), ("ccc", "bbb")]
        found = self._fetchall_sorted(connection.execute(u))

        eq_(found, wanted)

    @testing.requires.intersect
    def test_intersect_unions_3(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        u = intersect(
            select(t2.c.col3, t2.c.col4),
            union(
                select(t1.c.col3, t1.c.col4),
                select(t2.c.col3, t2.c.col4),
                select(t3.c.col3, t3.c.col4),
            )
            .alias()
            .select(),
        )
        wanted = [("aaa", "bbb"), ("bbb", "ccc"), ("ccc", "aaa")]
        found = self._fetchall_sorted(connection.execute(u))

        eq_(found, wanted)

    @testing.requires.intersect
    def test_composite_alias(self, connection):
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        ua = intersect(
            select(t2.c.col3, t2.c.col4),
            union(
                select(t1.c.col3, t1.c.col4),
                select(t2.c.col3, t2.c.col4),
                select(t3.c.col3, t3.c.col4),
            )
            .alias()
            .select(),
        ).alias()

        wanted = [("aaa", "bbb"), ("bbb", "ccc"), ("ccc", "aaa")]
        found = self._fetchall_sorted(connection.execute(ua.select()))
        eq_(found, wanted)


class JoinTest(fixtures.TablesTest):

    """Tests join execution.

    The compiled SQL emitted by the dialect might be ANSI joins or
    theta joins ('old oracle style', with (+) for OUTER).  This test
    tries to exercise join syntax and uncover any inconsistencies in
    `JOIN rhs ON lhs.col=rhs.col` vs `rhs.col=lhs.col`.  At least one
    database seems to be sensitive to this.
    """

    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "t1",
            metadata,
            Column("t1_id", Integer, primary_key=True),
            Column("name", String(32)),
        )
        Table(
            "t2",
            metadata,
            Column("t2_id", Integer, primary_key=True),
            Column("t1_id", Integer, ForeignKey("t1.t1_id")),
            Column("name", String(32)),
        )
        Table(
            "t3",
            metadata,
            Column("t3_id", Integer, primary_key=True),
            Column("t2_id", Integer, ForeignKey("t2.t2_id")),
            Column("name", String(32)),
        )

    @classmethod
    def insert_data(cls, connection):
        conn = connection
        # t1.10 -> t2.20 -> t3.30
        # t1.11 -> t2.21
        # t1.12
        t1, t2, t3 = cls.tables("t1", "t2", "t3")

        conn.execute(
            t1.insert(),
            [
                {"t1_id": 10, "name": "t1 #10"},
                {"t1_id": 11, "name": "t1 #11"},
                {"t1_id": 12, "name": "t1 #12"},
            ],
        )
        conn.execute(
            t2.insert(),
            [
                {"t2_id": 20, "t1_id": 10, "name": "t2 #20"},
                {"t2_id": 21, "t1_id": 11, "name": "t2 #21"},
            ],
        )
        conn.execute(
            t3.insert(), [{"t3_id": 30, "t2_id": 20, "name": "t3 #30"}]
        )

    def assertRows(self, statement, expected):
        """Execute a statement and assert that rows returned equal expected."""
        with testing.db.connect() as conn:
            found = sorted(
                [tuple(row) for row in conn.execute(statement).fetchall()]
            )
            eq_(found, sorted(expected))

    def test_join_x1(self):
        """Joins t1->t2."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t1.c.t1_id == t2.c.t1_id, t2.c.t1_id == t1.c.t1_id):
            expr = select(t1.c.t1_id, t2.c.t2_id).select_from(
                t1.join(t2, criteria)
            )
            self.assertRows(expr, [(10, 20), (11, 21)])

    def test_join_x2(self):
        """Joins t1->t2->t3."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t1.c.t1_id == t2.c.t1_id, t2.c.t1_id == t1.c.t1_id):
            expr = select(t1.c.t1_id, t2.c.t2_id).select_from(
                t1.join(t2, criteria)
            )
            self.assertRows(expr, [(10, 20), (11, 21)])

    def test_outerjoin_x1(self):
        """Outer joins t1->t2."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(t1.c.t1_id, t2.c.t2_id).select_from(
                t1.join(t2).join(t3, criteria)
            )
            self.assertRows(expr, [(10, 20)])

    def test_outerjoin_x2(self):
        """Outer joins t1->t2,t3."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id).select_from(
                t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                    t3, criteria
                )
            )
            self.assertRows(
                expr, [(10, 20, 30), (11, 21, None), (12, None, None)]
            )

    def test_outerjoin_where_x2_t1(self):
        """Outer joins t1->t2,t3, where on t1."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(t1.c.name == "t1 #10")
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(t1.c.t1_id < 12)
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t2(self):
        """Outer joins t1->t2,t3, where on t2."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(t2.c.name == "t2 #20")
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(t2.c.t2_id < 29)
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t3(self):
        """Outer joins t1->t2,t3, where on t3."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(t3.c.name == "t3 #30")
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(t3.c.t3_id < 39)
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30)])

    def test_outerjoin_where_x2_t1t3(self):
        """Outer joins t1->t2,t3, where on t1 and t3."""

        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(and_(t1.c.name == "t1 #10", t3.c.name == "t3 #30"))
                .select_from(
                    t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                        t3, criteria
                    )
                )
            )

            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(and_(t1.c.t1_id < 19, t3.c.t3_id < 39))
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30)])

    def test_outerjoin_where_x2_t1t2(self):
        """Outer joins t1->t2,t3, where on t1 and t2."""

        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(and_(t1.c.name == "t1 #10", t2.c.name == "t2 #20"))
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(and_(t1.c.t1_id < 12, t2.c.t2_id < 39))
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_outerjoin_where_x2_t1t2t3(self):
        """Outer joins t1->t2,t3, where on t1, t2 and t3."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(
                    and_(
                        t1.c.name == "t1 #10",
                        t2.c.name == "t2 #20",
                        t3.c.name == "t3 #30",
                    )
                )
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(and_(t1.c.t1_id < 19, t2.c.t2_id < 29, t3.c.t3_id < 39))
                .select_from(
                    (
                        t1.outerjoin(t2, t1.c.t1_id == t2.c.t1_id).outerjoin(
                            t3, criteria
                        )
                    )
                )
            )
            self.assertRows(expr, [(10, 20, 30)])

    def test_mixed(self):
        """Joins t1->t2, outer t2->t3."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id).select_from(
                (t1.join(t2).outerjoin(t3, criteria)),
            )
            print(expr)
            self.assertRows(expr, [(10, 20, 30), (11, 21, None)])

    def test_mixed_where(self):
        """Joins t1->t2, outer t2->t3, plus a where on each table in turn."""
        t1, t2, t3 = self.tables("t1", "t2", "t3")

        for criteria in (t2.c.t2_id == t3.c.t2_id, t3.c.t2_id == t2.c.t2_id):
            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(
                    t1.c.name == "t1 #10",
                )
                .select_from((t1.join(t2).outerjoin(t3, criteria)))
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(
                    t2.c.name == "t2 #20",
                )
                .select_from((t1.join(t2).outerjoin(t3, criteria)))
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(
                    t3.c.name == "t3 #30",
                )
                .select_from((t1.join(t2).outerjoin(t3, criteria)))
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(
                    and_(t1.c.name == "t1 #10", t2.c.name == "t2 #20"),
                )
                .select_from((t1.join(t2).outerjoin(t3, criteria)))
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(
                    and_(t2.c.name == "t2 #20", t3.c.name == "t3 #30"),
                )
                .select_from((t1.join(t2).outerjoin(t3, criteria)))
            )
            self.assertRows(expr, [(10, 20, 30)])

            expr = (
                select(t1.c.t1_id, t2.c.t2_id, t3.c.t3_id)
                .where(
                    and_(
                        t1.c.name == "t1 #10",
                        t2.c.name == "t2 #20",
                        t3.c.name == "t3 #30",
                    ),
                )
                .select_from((t1.join(t2).outerjoin(t3, criteria)))
            )
            self.assertRows(expr, [(10, 20, 30)])


class OperatorTest(fixtures.TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "flds",
            metadata,
            Column(
                "idcol",
                Integer,
                primary_key=True,
                test_needs_autoincrement=True,
            ),
            Column("intcol", Integer),
            Column("strcol", String(50)),
        )

    @classmethod
    def insert_data(cls, connection):
        flds = cls.tables.flds
        connection.execute(
            flds.insert(),
            [dict(intcol=5, strcol="foo"), dict(intcol=13, strcol="bar")],
        )

    # TODO: seems like more tests warranted for this setup.
    def test_modulo(self, connection):
        flds = self.tables.flds

        eq_(
            connection.execute(
                select(flds.c.intcol % 3).order_by(flds.c.idcol)
            ).fetchall(),
            [(2,), (1,)],
        )

    @testing.requires.window_functions
    def test_over(self, connection):
        flds = self.tables.flds

        eq_(
            connection.execute(
                select(
                    flds.c.intcol,
                    func.row_number().over(order_by=flds.c.strcol),
                )
            ).fetchall(),
            [(13, 1), (5, 2)],
        )
