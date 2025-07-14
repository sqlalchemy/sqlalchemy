from sqlalchemy import alias
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import Enum
from sqlalchemy import exc
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy import testing
from sqlalchemy import true
from sqlalchemy import values
from sqlalchemy.engine import default
from sqlalchemy.sql import func
from sqlalchemy.sql.compiler import FROM_LINTING
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_not
from sqlalchemy.util import OrderedDict


class ValuesTest(fixtures.TablesTest, AssertsCompiledSQL):
    __dialect__ = default.DefaultDialect(supports_native_boolean=True)

    run_setup_bind = None

    run_create_tables = None

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "people",
            metadata,
            Column("people_id", Integer, primary_key=True),
            Column("age", Integer),
            Column("name", String(30)),
        )
        Table(
            "bookcases",
            metadata,
            Column("bookcase_id", Integer, primary_key=True),
            Column(
                "bookcase_owner_id", Integer, ForeignKey("people.people_id")
            ),
            Column("bookcase_shelves", Integer),
            Column("bookcase_width", Integer),
        )
        Table(
            "books",
            metadata,
            Column("book_id", Integer, primary_key=True),
            Column(
                "bookcase_id", Integer, ForeignKey("bookcases.bookcase_id")
            ),
            Column("book_owner_id", Integer, ForeignKey("people.people_id")),
            Column("book_weight", Integer),
        )

    def test_wrong_number_of_elements(self):
        v1 = values(
            column("CaseSensitive", Integer),
            column("has spaces", String),
            name="Spaces and Cases",
        ).data([(1, "textA", 99), (2, "textB", 88)])

        with expect_raises_message(
            exc.ArgumentError,
            r"Wrong number of elements for 2-tuple: \(1, 'textA', 99\)",
        ):
            str(v1)

    @testing.fixture
    def _auto_proxy_fixture(self):
        c1 = column("q", Integer)
        c2 = column("p", Integer)
        t = table("t", c1)  # noqa: F841

        v1 = values(c1, c2).data([(1, 2), (3, 4)])

        return c1, c2, t, v1

    def test_auto_proxy_col_ownership(self, _auto_proxy_fixture):
        """test #10280"""

        c1, c2, t, v1 = _auto_proxy_fixture

        is_(c2, v1.c.p)
        is_not(c1, v1.c.q)

    def test_auto_proxy_select_c_col(self, _auto_proxy_fixture):
        """test #10280"""

        c1, c2, t, v1 = _auto_proxy_fixture
        self.assert_compile(select(t.c.q), "SELECT t.q FROM t")
        self.assert_compile(
            select(v1.c.q),
            "SELECT q FROM (VALUES (:param_1, :param_2), "
            "(:param_3, :param_4))",
            checkparams={
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4,
            },
        )

    def test_auto_proxy_select_direct_col(self, _auto_proxy_fixture):
        """test #10280"""

        c1, c2, t, v1 = _auto_proxy_fixture
        self.assert_compile(select(c1), "SELECT t.q FROM t")

        # for VALUES, the column does not have its set_parent called up front.
        # this is to make the construction of values() faster, as the values.c
        # use case is not required in order to use the construct
        self.assert_compile(select(c2), "SELECT p")

        # once we call v.c, then it's set up.
        # patch for #10280 added an extra step to make sure this works
        # even after the previous compile is called.
        # is this how it should work?  not sure, just testing how it is
        # right now
        v1.c.p

        self.assert_compile(
            select(c2),
            "SELECT p FROM (VALUES (:param_1, :param_2), "
            "(:param_3, :param_4))",
            checkparams={
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4,
            },
        )

    def test_auto_proxy_make_new_values(self, _auto_proxy_fixture):
        """test #10280"""

        c1, c2, t, v1 = _auto_proxy_fixture

        self.assert_compile(
            select(v1.c.p),
            "SELECT p FROM (VALUES (:param_1, :param_2), "
            "(:param_3, :param_4))",
            checkparams={
                "param_1": 1,
                "param_2": 2,
                "param_3": 3,
                "param_4": 4,
            },
        )

        v2 = values(c1, c2).data([(5, 6)])
        self.assert_compile(
            select(v2.c.p),
            "SELECT p FROM (VALUES (:param_1, :param_2))",
            checkparams={"param_1": 5, "param_2": 6},
        )

    def test_column_quoting(self):
        v1 = values(
            column("CaseSensitive", Integer),
            column("has spaces", String),
            column("number", Integer),
            name="Spaces and Cases",
        ).data([(1, "textA", 99), (2, "textB", 88)])
        self.assert_compile(
            select(v1),
            'SELECT "Spaces and Cases"."CaseSensitive", '
            '"Spaces and Cases"."has spaces", "Spaces and Cases".number FROM '
            "(VALUES (:param_1, :param_2, :param_3), "
            "(:param_4, :param_5, :param_6)) "
            'AS "Spaces and Cases" ("CaseSensitive", "has spaces", number)',
        )

    def test_values_in_scalar_subq(self):
        """test #9772"""

        people = self.tables.people
        table_value_constructor = values(
            Column("v1", Integer), name="tvc"
        ).data(
            [
                (people.c.people_id,),
                (people.c.age,),
                (people.c.name,),
            ]
        )

        maximum = select(func.max(table_value_constructor.c.v1))
        maximum_subquery = maximum.scalar_subquery()
        query = select(people.c.people_id, maximum_subquery)
        self.assert_compile(
            query,
            "SELECT people.people_id, "
            "(SELECT max(tvc.v1) AS max_1 FROM "
            "(VALUES (people.people_id), (people.age), (people.name)) "
            "AS tvc (v1)) AS anon_1 FROM people",
        )

    def test_values_in_select_cte_params(self):
        cte1 = select(
            values(
                column("col1", String),
                column("col2", Integer),
                name="temp_table",
            ).data([("a", 2), ("b", 3)])
        ).cte("cte1")

        cte2 = select(cte1.c.col1).where(cte1.c.col1 == "q").cte("cte2")
        stmt = select(cte2.c.col1)

        dialect = default.DefaultDialect()
        dialect.positional = True
        dialect.paramstyle = "numeric"
        self.assert_compile(
            stmt,
            "WITH cte1 AS (SELECT temp_table.col1 AS col1, "
            "temp_table.col2 AS col2 FROM (VALUES (:1, :2), (:3, :4)) AS "
            "temp_table (col1, col2)), "
            "cte2 AS "
            "(SELECT cte1.col1 AS col1 FROM cte1 WHERE cte1.col1 = :5) "
            "SELECT cte2.col1 FROM cte2",
            checkpositional=("a", 2, "b", 3, "q"),
            dialect=dialect,
        )

        self.assert_compile(
            stmt,
            "WITH cte1 AS (SELECT temp_table.col1 AS col1, "
            "temp_table.col2 AS col2 FROM (VALUES ('a', 2), ('b', 3)) "
            "AS temp_table (col1, col2)), "
            "cte2 AS "
            "(SELECT cte1.col1 AS col1 FROM cte1 WHERE cte1.col1 = 'q') "
            "SELECT cte2.col1 FROM cte2",
            literal_binds=True,
            dialect=dialect,
        )

    def test_values_in_select_cte_literal_binds(self):
        cte1 = select(
            values(
                column("col1", String),
                column("col2", Integer),
                name="temp_table",
                literal_binds=True,
            ).data([("a", 2), ("b", 3)])
        ).cte("cte1")

        cte2 = select(cte1.c.col1).where(cte1.c.col1 == "q").cte("cte2")
        stmt = select(cte2.c.col1)

        self.assert_compile(
            stmt,
            "WITH cte1 AS (SELECT temp_table.col1 AS col1, "
            "temp_table.col2 AS col2 FROM (VALUES ('a', 2), ('b', 3)) "
            "AS temp_table (col1, col2)), "
            "cte2 AS "
            "(SELECT cte1.col1 AS col1 FROM cte1 WHERE cte1.col1 = :col1_1) "
            "SELECT cte2.col1 FROM cte2",
            checkparams={"col1_1": "q"},
        )

    @testing.variation("values_named", [True, False])
    @testing.variation("cte_named", [True, False])
    @testing.variation("literal_binds", [True, False])
    def test_direct_cte(self, values_named, cte_named, literal_binds):
        """test #12734"""

        cte1 = (
            values(
                column("col1", String),
                column("col2", Integer),
                literal_binds=bool(literal_binds),
                name="some name" if values_named else None,
            )
            .data([("a", 2), ("b", 3)])
            .cte("cte1" if cte_named else None)
        )

        stmt = select(cte1.c.col1)

        if cte_named:
            cte_name = "cte1"
        elif values_named:
            cte_name = "some_name_1"
        else:
            cte_name = "anon_1"

        if literal_binds:
            params = "('a', 2), ('b', 3)"
        else:
            params = "(:param_1, :param_2), (:param_3, :param_4)"
        self.assert_compile(
            stmt,
            f"WITH {cte_name}(col1, col2) AS "
            f"(VALUES {params}) "
            f"SELECT {cte_name}.col1 FROM {cte_name}",
            checkparams=(
                {
                    "param_1": "a",
                    "param_2": 2,
                    "param_3": "b",
                    "param_4": 3,
                }
                if not literal_binds
                else {}
            ),
        )

    def test_add_cte_one(self):
        cte1 = (
            values(
                column("col1", String),
                column("col2", Integer),
                name="some_name",
            ).data([("a", 2), ("b", 3)])
        ).add_cte(select(1).cte())

        self.assert_compile(
            cte1.select(),
            "WITH anon_1 AS (SELECT 1) "
            "SELECT some_name.col1, some_name.col2 FROM "
            "(VALUES (:param_1, :param_2), "
            "(:param_3, :param_4)) AS some_name (col1, col2)",
        )

    def test_add_cte_two(self):
        cte1 = (
            (
                values(
                    column("col1", String),
                    column("col2", Integer),
                    name="some_name",
                ).data([("a", 2), ("b", 3)])
            )
            .add_cte(select(1).cte())
            .cte()
        )

        self.assert_compile(
            cte1.select(),
            "WITH anon_1 AS (SELECT 1), some_name_1(col1, col2) AS "
            "(VALUES (:param_1, :param_2), (:param_3, :param_4)) "
            "SELECT some_name_1.col1, some_name_1.col2 FROM some_name_1",
        )

    def test_no_cte_with_lateral(self):
        values_ = (
            values(
                column("col1", String),
                column("col2", Integer),
                name="some_name",
            )
            .data([("a", 2), ("b", 3)])
            .lateral()
        )

        cte = values_.cte()

        with expect_raises_message(
            exc.CompileError,
            "Can't use a LATERAL VALUES expression inside of a CTE",
        ):
            cte.select().compile()

    @testing.fixture
    def literal_parameter_fixture(self):
        def go(literal_binds, omit=None):
            cols = [
                column("mykey", Integer),
                column("mytext", String),
                column("myint", Integer),
            ]
            if omit:
                for idx in omit:
                    cols[idx] = column(cols[idx].name)

            return values(
                *cols, name="myvalues", literal_binds=literal_binds
            ).data([(1, "textA", 99), (2, "textB", 88)])

        return go

    @testing.fixture
    def tricky_types_parameter_fixture(self):
        class SomeEnum:
            # Implements PEP 435 in the minimal fashion needed by SQLAlchemy
            __members__ = OrderedDict()

            def __init__(self, name, value, alias=None):
                self.name = name
                self.value = value
                self.__members__[name] = self
                setattr(self.__class__, name, self)
                if alias:
                    self.__members__[alias] = self
                    setattr(self.__class__, alias, self)

        one = SomeEnum("one", 1)
        two = SomeEnum("two", 2)

        class MumPyString(str):
            """some kind of string, can't imagine where such a thing might
            be found

            """

        class MumPyNumber(int):
            """some kind of int, can't imagine where such a thing might
            be found

            """

        def go(literal_binds, omit=None):
            cols = [
                column("mykey", Integer),
                column("mytext", String),
                column("myenum", Enum(SomeEnum)),
            ]
            if omit:
                for idx in omit:
                    cols[idx] = column(cols[idx].name)

            return values(
                *cols, name="myvalues", literal_binds=literal_binds
            ).data(
                [
                    (MumPyNumber(1), MumPyString("textA"), one),
                    (MumPyNumber(2), MumPyString("textB"), two),
                ]
            )

        return go

    def test_bound_parameters(self, literal_parameter_fixture):
        literal_parameter_fixture = literal_parameter_fixture(False)

        stmt = select(literal_parameter_fixture)

        self.assert_compile(
            stmt,
            "SELECT myvalues.mykey, myvalues.mytext, myvalues.myint FROM "
            "(VALUES (:param_1, :param_2, :param_3), "
            "(:param_4, :param_5, :param_6)"
            ") AS myvalues (mykey, mytext, myint)",
            checkparams={
                "param_1": 1,
                "param_2": "textA",
                "param_3": 99,
                "param_4": 2,
                "param_5": "textB",
                "param_6": 88,
            },
        )

    def test_literal_parameters(self, literal_parameter_fixture):
        literal_parameter_fixture = literal_parameter_fixture(True)

        stmt = select(literal_parameter_fixture)

        self.assert_compile(
            stmt,
            "SELECT myvalues.mykey, myvalues.mytext, myvalues.myint FROM "
            "(VALUES (1, 'textA', 99), (2, 'textB', 88)"
            ") AS myvalues (mykey, mytext, myint)",
            checkparams={},
        )

    def test_literal_parameters_not_every_type_given(
        self, literal_parameter_fixture
    ):
        literal_parameter_fixture = literal_parameter_fixture(True, omit=(1,))

        stmt = select(literal_parameter_fixture)

        self.assert_compile(
            stmt,
            "SELECT myvalues.mykey, myvalues.mytext, myvalues.myint FROM "
            "(VALUES (1, 'textA', 99), (2, 'textB', 88)"
            ") AS myvalues (mykey, mytext, myint)",
            checkparams={},
        )

    def test_use_cols_tricky_not_every_type_given(
        self, tricky_types_parameter_fixture
    ):
        literal_parameter_fixture = tricky_types_parameter_fixture(
            True, omit=(1,)
        )

        stmt = select(literal_parameter_fixture)

        with expect_raises_message(
            exc.CompileError,
            r"No literal value renderer is available for literal "
            r"value \"'textA'\" with datatype NULL",
        ):
            str(stmt)

    def test_use_cols_for_types(self, tricky_types_parameter_fixture):
        literal_parameter_fixture = tricky_types_parameter_fixture(True)

        stmt = select(literal_parameter_fixture)

        self.assert_compile(
            stmt,
            "SELECT myvalues.mykey, myvalues.mytext, myvalues.myenum FROM "
            "(VALUES (1, 'textA', 'one'), (2, 'textB', 'two')"
            ") AS myvalues (mykey, mytext, myenum)",
            checkparams={},
        )

    def test_anon_alias(self):
        people = self.tables.people
        values_ = (
            values(
                column("bookcase_id", Integer),
                column("bookcase_owner_id", Integer),
            )
            .data([(1, 1), (2, 1), (3, 2), (3, 3)])
            .alias()
        )
        stmt = select(people, values_).select_from(
            people.join(
                values_, values_.c.bookcase_owner_id == people.c.people_id
            )
        )
        self.assert_compile(
            stmt,
            "SELECT people.people_id, people.age, people.name, "
            "anon_1.bookcase_id, anon_1.bookcase_owner_id FROM people "
            "JOIN (VALUES (:param_1, :param_2), (:param_3, :param_4), "
            "(:param_5, :param_6), (:param_7, :param_8)) AS anon_1 "
            "(bookcase_id, bookcase_owner_id) "
            "ON people.people_id = anon_1.bookcase_owner_id",
        )

    def test_with_join_unnamed(self):
        people = self.tables.people
        values_ = values(
            column("column1", Integer),
            column("column2", Integer),
        ).data([(1, 1), (2, 1), (3, 2), (3, 3)])
        stmt = select(people, values_).select_from(
            people.join(values_, values_.c.column2 == people.c.people_id)
        )
        self.assert_compile(
            stmt,
            "SELECT people.people_id, people.age, people.name, column1, "
            "column2 FROM people JOIN (VALUES (:param_1, :param_2), "
            "(:param_3, :param_4), (:param_5, :param_6), "
            "(:param_7, :param_8)) "
            "ON people.people_id = column2",
            checkparams={
                "param_1": 1,
                "param_2": 1,
                "param_3": 2,
                "param_4": 1,
                "param_5": 3,
                "param_6": 2,
                "param_7": 3,
                "param_8": 3,
            },
        )

    def test_with_join_named(self):
        people = self.tables.people
        values_ = values(
            column("bookcase_id", Integer),
            column("bookcase_owner_id", Integer),
            name="bookcases",
        ).data([(1, 1), (2, 1), (3, 2), (3, 3)])
        stmt = select(people, values_).select_from(
            people.join(
                values_, values_.c.bookcase_owner_id == people.c.people_id
            )
        )
        self.assert_compile(
            stmt,
            "SELECT people.people_id, people.age, people.name, "
            "bookcases.bookcase_id, bookcases.bookcase_owner_id FROM people "
            "JOIN (VALUES (:param_1, :param_2), (:param_3, :param_4), "
            "(:param_5, :param_6), (:param_7, :param_8)) AS bookcases "
            "(bookcase_id, bookcase_owner_id) "
            "ON people.people_id = bookcases.bookcase_owner_id",
            checkparams={
                "param_1": 1,
                "param_2": 1,
                "param_3": 2,
                "param_4": 1,
                "param_5": 3,
                "param_6": 2,
                "param_7": 3,
                "param_8": 3,
            },
        )

    def test_with_aliased_join(self):
        people = self.tables.people
        values_ = (
            values(
                column("bookcase_id", Integer),
                column("bookcase_owner_id", Integer),
            )
            .data([(1, 1), (2, 1), (3, 2), (3, 3)])
            .alias("bookcases")
        )

        stmt = select(people, values_).select_from(
            people.join(
                values_, values_.c.bookcase_owner_id == people.c.people_id
            )
        )
        self.assert_compile(
            stmt,
            "SELECT people.people_id, people.age, people.name, "
            "bookcases.bookcase_id, bookcases.bookcase_owner_id FROM people "
            "JOIN (VALUES (:param_1, :param_2), (:param_3, :param_4), "
            "(:param_5, :param_6), (:param_7, :param_8)) AS bookcases "
            "(bookcase_id, bookcase_owner_id) "
            "ON people.people_id = bookcases.bookcase_owner_id",
            checkparams={
                "param_1": 1,
                "param_2": 1,
                "param_3": 2,
                "param_4": 1,
                "param_5": 3,
                "param_6": 2,
                "param_7": 3,
                "param_8": 3,
            },
        )

    def test_with_standalone_aliased_join(self):
        people = self.tables.people
        values_ = values(
            column("bookcase_id", Integer),
            column("bookcase_owner_id", Integer),
        ).data([(1, 1), (2, 1), (3, 2), (3, 3)])
        values_ = alias(values_, "bookcases")

        stmt = select(people, values_).select_from(
            people.join(
                values_, values_.c.bookcase_owner_id == people.c.people_id
            )
        )
        self.assert_compile(
            stmt,
            "SELECT people.people_id, people.age, people.name, "
            "bookcases.bookcase_id, bookcases.bookcase_owner_id FROM people "
            "JOIN (VALUES (:param_1, :param_2), (:param_3, :param_4), "
            "(:param_5, :param_6), (:param_7, :param_8)) AS bookcases "
            "(bookcase_id, bookcase_owner_id) "
            "ON people.people_id = bookcases.bookcase_owner_id",
            checkparams={
                "param_1": 1,
                "param_2": 1,
                "param_3": 2,
                "param_4": 1,
                "param_5": 3,
                "param_6": 2,
                "param_7": 3,
                "param_8": 3,
            },
        )

    def test_lateral(self):
        people = self.tables.people
        values_ = (
            values(
                column("bookcase_id", Integer),
                column("bookcase_owner_id", Integer),
                name="bookcases",
            )
            .data([(1, 1), (2, 1), (3, 2), (3, 3)])
            .lateral()
        )
        stmt = select(people, values_).select_from(
            people.join(values_, true())
        )
        self.assert_compile(
            stmt,
            "SELECT people.people_id, people.age, people.name, "
            "bookcases.bookcase_id, bookcases.bookcase_owner_id FROM people "
            "JOIN LATERAL (VALUES (:param_1, :param_2), (:param_3, :param_4), "
            "(:param_5, :param_6), (:param_7, :param_8)) AS bookcases "
            "(bookcase_id, bookcase_owner_id) "
            "ON true",
            checkparams={
                "param_1": 1,
                "param_2": 1,
                "param_3": 2,
                "param_4": 1,
                "param_5": 3,
                "param_6": 2,
                "param_7": 3,
                "param_8": 3,
            },
        )

    def test_from_linting_named(self):
        people = self.tables.people
        values_ = values(
            column("bookcase_id", Integer),
            column("bookcase_owner_id", Integer),
            name="bookcases",
        ).data([(1, 1), (2, 1), (3, 2), (3, 3)])
        stmt = select(people, values_)

        with testing.expect_warnings(
            r"SELECT statement has a cartesian product between FROM "
            r'element\(s\) "(?:bookcases|people)" and '
            r'FROM element "(?:people|bookcases)"'
        ):
            stmt.compile(linting=FROM_LINTING)

    def test_from_linting_unnamed(self):
        people = self.tables.people
        values_ = values(
            column("bookcase_id", Integer),
            column("bookcase_owner_id", Integer),
        ).data([(1, 1), (2, 1), (3, 2), (3, 3)])
        stmt = select(people, values_)

        with testing.expect_warnings(
            r"SELECT statement has a cartesian product between FROM "
            r'element\(s\) "(?:\(unnamed VALUES element\)|people)" and '
            r'FROM element "(?:people|\(unnamed VALUES element\))"'
        ):
            stmt.compile(linting=FROM_LINTING)
