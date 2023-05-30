from sqlalchemy import and_
from sqlalchemy import case
from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import literal_column
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import testing
from sqlalchemy import text
from sqlalchemy.sql import column
from sqlalchemy.sql import table
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures


info_table = None


class CaseTest(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    @classmethod
    def setup_test_class(cls):
        metadata = MetaData()
        global info_table
        info_table = Table(
            "infos",
            metadata,
            Column("pk", Integer, primary_key=True),
            Column("info", String(30)),
        )

        with testing.db.begin() as conn:
            info_table.create(conn)

            conn.execute(
                info_table.insert(),
                [
                    {"pk": 1, "info": "pk_1_data"},
                    {"pk": 2, "info": "pk_2_data"},
                    {"pk": 3, "info": "pk_3_data"},
                    {"pk": 4, "info": "pk_4_data"},
                    {"pk": 5, "info": "pk_5_data"},
                    {"pk": 6, "info": "pk_6_data"},
                ],
            )

    @classmethod
    def teardown_test_class(cls):
        with testing.db.begin() as conn:
            info_table.drop(conn)

    @testing.requires.subqueries
    def test_case(self, connection):
        inner = select(
            case(
                (info_table.c.pk < 3, "lessthan3"),
                (and_(info_table.c.pk >= 3, info_table.c.pk < 7), "gt3"),
            ).label("x"),
            info_table.c.pk,
            info_table.c.info,
        ).select_from(info_table)

        inner_result = connection.execute(inner).all()

        # Outputs:
        # lessthan3 1 pk_1_data
        # lessthan3 2 pk_2_data
        # gt3 3 pk_3_data
        # gt3 4 pk_4_data
        # gt3 5 pk_5_data
        # gt3 6 pk_6_data
        eq_(
            inner_result,
            [
                ("lessthan3", 1, "pk_1_data"),
                ("lessthan3", 2, "pk_2_data"),
                ("gt3", 3, "pk_3_data"),
                ("gt3", 4, "pk_4_data"),
                ("gt3", 5, "pk_5_data"),
                ("gt3", 6, "pk_6_data"),
            ],
        )

        outer = select(inner.alias("q_inner"))

        outer_result = connection.execute(outer).all()

        assert outer_result == [
            ("lessthan3", 1, "pk_1_data"),
            ("lessthan3", 2, "pk_2_data"),
            ("gt3", 3, "pk_3_data"),
            ("gt3", 4, "pk_4_data"),
            ("gt3", 5, "pk_5_data"),
            ("gt3", 6, "pk_6_data"),
        ]

        w_else = select(
            case(
                [info_table.c.pk < 3, cast(3, Integer)],
                [and_(info_table.c.pk >= 3, info_table.c.pk < 6), 6],
                else_=0,
            ).label("x"),
            info_table.c.pk,
            info_table.c.info,
        ).select_from(info_table)

        else_result = connection.execute(w_else).all()

        eq_(
            else_result,
            [
                (3, 1, "pk_1_data"),
                (3, 2, "pk_2_data"),
                (6, 3, "pk_3_data"),
                (6, 4, "pk_4_data"),
                (6, 5, "pk_5_data"),
                (0, 6, "pk_6_data"),
            ],
        )

    def test_literal_interpretation_one(self):
        """note this is modified as of #7287 to accept strings, tuples
        and other literal values as input
        where they are interpreted as bound values just like any other
        expression.

        Previously, an exception would be raised that the literal was
        ambiguous.


        """
        self.assert_compile(
            case(("x", "y")),
            "CASE WHEN :param_1 THEN :param_2 END",
            checkparams={"param_1": "x", "param_2": "y"},
        )

    def test_literal_interpretation_two(self):
        """note this is modified as of #7287 to accept strings, tuples
        and other literal values as input
        where they are interpreted as bound values just like any other
        expression.

        Previously, an exception would be raised that the literal was
        ambiguous.


        """
        self.assert_compile(
            case(
                (("x", "y"), "z"),
            ),
            "CASE WHEN :param_1 THEN :param_2 END",
            checkparams={"param_1": ("x", "y"), "param_2": "z"},
        )

    def test_literal_interpretation_two_point_five(self):
        """note this is modified as of #7287 to accept strings, tuples
        and other literal values as input
        where they are interpreted as bound values just like any other
        expression.

        Previously, an exception would be raised that the literal was
        ambiguous.


        """
        self.assert_compile(
            case(
                (12, "z"),
            ),
            "CASE WHEN :param_1 THEN :param_2 END",
            checkparams={"param_1": 12, "param_2": "z"},
        )

    def test_literal_interpretation_three(self):
        t = table("test", column("col1"))

        self.assert_compile(
            case(("x", "y"), value=t.c.col1),
            "CASE test.col1 WHEN :param_1 THEN :param_2 END",
        )
        self.assert_compile(
            case((t.c.col1 == 7, "y"), else_="z"),
            "CASE WHEN (test.col1 = :col1_1) THEN :param_1 ELSE :param_2 END",
        )

    @testing.combinations(
        (
            (lambda t: ({"x": "y"}, t.c.col1, None)),
            "CASE test.col1 WHEN :param_1 THEN :param_2 END",
        ),
        (
            (lambda t: ({"x": "y", "p": "q"}, t.c.col1, None)),
            "CASE test.col1 WHEN :param_1 THEN :param_2 "
            "WHEN :param_3 THEN :param_4 END",
        ),
        (
            (lambda t: ({t.c.col1 == 7: "x"}, None, 10)),
            "CASE WHEN (test.col1 = :col1_1) THEN :param_1 ELSE :param_2 END",
        ),
        (
            (lambda t: ({t.c.col1 == 7: "x", t.c.col1 == 10: "y"}, None, 10)),
            "CASE WHEN (test.col1 = :col1_1) THEN :param_1 "
            "WHEN (test.col1 = :col1_2) THEN :param_2 ELSE :param_3 END",
        ),
        argnames="test_case, expected",
    )
    def test_when_dicts(self, test_case, expected):
        t = table("test", column("col1"))

        when_dict, value, else_ = testing.resolve_lambda(test_case, t=t)

        self.assert_compile(
            case(when_dict, value=value, else_=else_), expected
        )

    def test_text_doesnt_explode(self, connection):
        for s in [
            select(
                case(
                    (info_table.c.info == "pk_4_data", text("'yes'")),
                    else_=text("'no'"),
                )
            ).order_by(info_table.c.info),
            select(
                case(
                    (
                        info_table.c.info == "pk_4_data",
                        literal_column("'yes'"),
                    ),
                    else_=literal_column("'no'"),
                )
            ).order_by(info_table.c.info),
        ]:
            eq_(
                connection.execute(s).all(),
                [("no",), ("no",), ("no",), ("yes",), ("no",), ("no",)],
            )

    def test_text_doenst_explode_even_in_whenlist(self):
        """test #7287"""
        self.assert_compile(
            case(
                (text(":case = 'upper'"), func.upper(literal_column("q"))),
                else_=func.lower(literal_column("q")),
            ),
            "CASE WHEN :case = 'upper' THEN upper(q) ELSE lower(q) END",
        )

    def testcase_with_dict(self):
        query = select(
            case(
                {
                    info_table.c.pk < 3: "lessthan3",
                    info_table.c.pk >= 3: "gt3",
                },
                else_="other",
            ),
            info_table.c.pk,
            info_table.c.info,
        ).select_from(info_table)
        eq_(
            query.execute().fetchall(),
            [
                ("lessthan3", 1, "pk_1_data"),
                ("lessthan3", 2, "pk_2_data"),
                ("gt3", 3, "pk_3_data"),
                ("gt3", 4, "pk_4_data"),
                ("gt3", 5, "pk_5_data"),
                ("gt3", 6, "pk_6_data"),
            ],
        )

        simple_query = (
            select(
                case(
                    {1: "one", 2: "two"}, value=info_table.c.pk, else_="other"
                ),
                info_table.c.pk,
            )
            .where(info_table.c.pk < 4)
            .select_from(info_table)
        )

        assert simple_query.execute().fetchall() == [
            ("one", 1),
            ("two", 2),
            ("other", 3),
        ]
