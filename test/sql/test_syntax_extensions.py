from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import Table
from sqlalchemy import table
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql import coercions
from sqlalchemy.sql import roles
from sqlalchemy.sql import util as sql_util
from sqlalchemy.sql.base import SyntaxExtension
from sqlalchemy.sql.dml import Delete
from sqlalchemy.sql.dml import Update
from sqlalchemy.sql.visitors import _TraverseInternalsType
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import expect_raises_message
from sqlalchemy.testing import fixtures


class PostSelectClause(SyntaxExtension, ClauseElement):
    _traverse_internals = []

    def apply_to_select(self, select_stmt):
        select_stmt.apply_syntax_extension_point(
            lambda existing: [*existing, self],
            "post_select",
        )


class PreColumnsClause(SyntaxExtension, ClauseElement):
    _traverse_internals = []

    def apply_to_select(self, select_stmt):
        select_stmt.apply_syntax_extension_point(
            lambda existing: [*existing, self],
            "pre_columns",
        )


class PostCriteriaClause(SyntaxExtension, ClauseElement):
    _traverse_internals = []

    def apply_to_select(self, select_stmt):
        select_stmt.apply_syntax_extension_point(
            lambda existing: [*existing, self],
            "post_criteria",
        )

    def apply_to_update(self, update_stmt: Update) -> None:
        update_stmt.apply_syntax_extension_point(
            lambda existing: [self], "post_criteria"
        )

    def apply_to_delete(self, delete_stmt: Delete) -> None:
        delete_stmt.apply_syntax_extension_point(
            lambda existing: [self], "post_criteria"
        )


class PostCriteriaClause2(SyntaxExtension, ClauseElement):
    _traverse_internals = []

    def apply_to_select(self, select_stmt):
        select_stmt.apply_syntax_extension_point(
            self.append_replacing_same_type,
            "post_criteria",
        )


class PostCriteriaClause3(SyntaxExtension, ClauseElement):
    _traverse_internals = []

    def apply_to_select(self, select_stmt):
        select_stmt.apply_syntax_extension_point(
            lambda existing: [self],
            "post_criteria",
        )


class PostBodyClause(SyntaxExtension, ClauseElement):
    _traverse_internals = []

    def apply_to_select(self, select_stmt):
        select_stmt.apply_syntax_extension_point(
            lambda existing: [self],
            "post_body",
        )


class PostValuesClause(SyntaxExtension, ClauseElement):
    _traverse_internals = []

    def apply_to_insert(self, insert_stmt):
        insert_stmt.apply_syntax_extension_point(
            lambda existing: [self],
            "post_values",
        )


class ColumnExpressionExt(SyntaxExtension, ClauseElement):
    _traverse_internals: _TraverseInternalsType = [
        ("_exprs", InternalTraversal.dp_clauseelement_tuple),
    ]

    def __init__(self, *exprs):
        self._exprs = tuple(
            coercions.expect(roles.ByOfRole, e, apply_propagate_attrs=self)
            for e in exprs
        )

    def apply_to_select(self, select_stmt):
        select_stmt.apply_syntax_extension_point(
            lambda existing: [*existing, self],
            "post_select",
        )


@compiles(PostSelectClause)
def _compile_psk(element, compiler, **kw):
    return "POST SELECT KEYWORD"


@compiles(PreColumnsClause)
def _compile_pcc(element, compiler, **kw):
    return "PRE COLUMNS"


@compiles(PostCriteriaClause)
def _compile_psc(element, compiler, **kw):
    return "POST CRITERIA"


@compiles(PostCriteriaClause2)
def _compile_psc2(element, compiler, **kw):
    return "2 POST CRITERIA 2"


@compiles(PostCriteriaClause3)
def _compile_psc3(element, compiler, **kw):
    return "3 POST CRITERIA 3"


@compiles(PostBodyClause)
def _compile_psb(element, compiler, **kw):
    return "POST SELECT BODY"


@compiles(PostValuesClause)
def _compile_pvc(element, compiler, **kw):
    return "POST VALUES"


@compiles(ColumnExpressionExt)
def _compile_cee(element, compiler, **kw):
    inner = ", ".join(compiler.process(elem, **kw) for elem in element._exprs)
    return f"COLUMN EXPRESSIONS ({inner})"


class TestExtensionPoints(fixtures.TestBase, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_illegal_section(self):
        class SomeExtension(SyntaxExtension, ClauseElement):
            _traverse_internals = []

            def apply_to_select(self, select_stmt):
                select_stmt.apply_syntax_extension_point(
                    lambda existing: [self],
                    "not_present",
                )

        with expect_raises_message(
            ValueError,
            r"Unknown position 'not_present' for <class .*Select'> "
            "construct; known positions: "
            "'post_select', 'pre_columns', 'post_criteria', 'post_body'",
        ):
            select(column("q")).ext(SomeExtension())

    def test_select_post_select_clause(self):
        self.assert_compile(
            select(column("a"), column("b"))
            .ext(PostSelectClause())
            .where(column("q") == 5),
            "SELECT POST SELECT KEYWORD a, b WHERE q = :q_1",
        )

    def test_select_pre_columns_clause(self):
        self.assert_compile(
            select(column("a"), column("b"))
            .ext(PreColumnsClause())
            .where(column("q") == 5)
            .distinct(),
            "SELECT DISTINCT PRE COLUMNS a, b WHERE q = :q_1",
        )

    def test_select_post_criteria_clause(self):
        self.assert_compile(
            select(column("a"), column("b"))
            .ext(PostCriteriaClause())
            .where(column("q") == 5)
            .having(column("z") == 10)
            .order_by(column("r")),
            "SELECT a, b WHERE q = :q_1 HAVING z = :z_1 "
            "POST CRITERIA ORDER BY r",
        )

    def test_select_post_criteria_clause_multiple(self):
        self.assert_compile(
            select(column("a"), column("b"))
            .ext(PostCriteriaClause())
            .ext(PostCriteriaClause2())
            .where(column("q") == 5)
            .having(column("z") == 10)
            .order_by(column("r")),
            "SELECT a, b WHERE q = :q_1 HAVING z = :z_1 "
            "POST CRITERIA 2 POST CRITERIA 2 ORDER BY r",
        )

    def test_select_post_criteria_clause_multiple2(self):
        stmt = (
            select(column("a"), column("b"))
            .ext(PostCriteriaClause())
            .ext(PostCriteriaClause())
            .ext(PostCriteriaClause2())
            .ext(PostCriteriaClause2())
            .where(column("q") == 5)
            .having(column("z") == 10)
            .order_by(column("r"))
        )
        # PostCriteriaClause2 is here only once
        self.assert_compile(
            stmt,
            "SELECT a, b WHERE q = :q_1 HAVING z = :z_1 "
            "POST CRITERIA POST CRITERIA 2 POST CRITERIA 2 ORDER BY r",
        )
        # now there is only PostCriteriaClause3
        self.assert_compile(
            stmt.ext(PostCriteriaClause3()),
            "SELECT a, b WHERE q = :q_1 HAVING z = :z_1 "
            "3 POST CRITERIA 3 ORDER BY r",
        )

    def test_select_post_select_body(self):
        self.assert_compile(
            select(column("a"), column("b"))
            .ext(PostBodyClause())
            .where(column("q") == 5)
            .having(column("z") == 10)
            .order_by(column("r"))
            .limit(15),
            "SELECT a, b WHERE q = :q_1 HAVING z = :z_1 "
            "ORDER BY r LIMIT :param_1 POST SELECT BODY",
        )

    def test_insert_post_values(self):
        t = table("t", column("a"), column("b"))
        self.assert_compile(
            t.insert().ext(PostValuesClause()),
            "INSERT INTO t (a, b) VALUES (:a, :b) POST VALUES",
        )

    def test_update_post_criteria(self):
        t = table("t", column("a"), column("b"))
        self.assert_compile(
            t.update().ext(PostCriteriaClause()).where(t.c.a == "hi"),
            "UPDATE t SET a=:a, b=:b WHERE t.a = :a_1 POST CRITERIA",
        )

    def test_delete_post_criteria(self):
        t = table("t", column("a"), column("b"))
        self.assert_compile(
            t.delete().ext(PostCriteriaClause()).where(t.c.a == "hi"),
            "DELETE FROM t WHERE t.a = :a_1 POST CRITERIA",
        )


class TestExpressionExtensions(
    fixtures.CacheKeyFixture, fixtures.TestBase, AssertsCompiledSQL
):
    __dialect__ = "default"

    def test_render(self):
        t = Table(
            "t1", MetaData(), Column("c1", Integer), Column("c2", Integer)
        )

        stmt = select(t).ext(ColumnExpressionExt(t.c.c1, t.c.c2))
        self.assert_compile(
            stmt,
            "SELECT COLUMN EXPRESSIONS (t1.c1, t1.c2) t1.c1, t1.c2 FROM t1",
        )

    def test_adaptation(self):
        t = Table(
            "t1", MetaData(), Column("c1", Integer), Column("c2", Integer)
        )

        s1 = select(t).subquery()
        s2 = select(t).ext(ColumnExpressionExt(t.c.c1, t.c.c2))
        s3 = sql_util.ClauseAdapter(s1).traverse(s2)

        self.assert_compile(
            s3,
            "SELECT COLUMN EXPRESSIONS (anon_1.c1, anon_1.c2) "
            "anon_1.c1, anon_1.c2 FROM "
            "(SELECT t1.c1 AS c1, t1.c2 AS c2 FROM t1) AS anon_1",
        )

    def test_compare(self):
        t = Table(
            "t1", MetaData(), Column("c1", Integer), Column("c2", Integer)
        )

        self._run_compare_fixture(
            lambda: (
                select(t).ext(ColumnExpressionExt(t.c.c1, t.c.c2)),
                select(t).ext(ColumnExpressionExt(t.c.c1)),
                select(t),
            )
        )
