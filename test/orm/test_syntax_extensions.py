from __future__ import annotations

from typing import Any

from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql import coercions
from sqlalchemy.sql import roles
from sqlalchemy.sql._typing import _ColumnExpressionArgument
from sqlalchemy.sql.base import SyntaxExtension
from sqlalchemy.sql.dml import Delete
from sqlalchemy.sql.dml import Update
from sqlalchemy.sql.visitors import _TraverseInternalsType
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import eq_
from .test_query import QueryTest


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
            lambda existing: [*existing, self],
            "post_criteria",
        )


class PostCriteriaClauseCols(PostCriteriaClause):
    _traverse_internals: _TraverseInternalsType = [
        ("exprs", InternalTraversal.dp_clauseelement_tuple),
    ]

    def __init__(self, *exprs: _ColumnExpressionArgument[Any]):
        self.exprs = tuple(
            coercions.expect(roles.ByOfRole, e, apply_propagate_attrs=self)
            for e in exprs
        )


class PostCriteriaClauseColsNoProp(PostCriteriaClause):
    _traverse_internals: _TraverseInternalsType = [
        ("exprs", InternalTraversal.dp_clauseelement_tuple),
    ]

    def __init__(self, *exprs: _ColumnExpressionArgument[Any]):
        self.exprs = tuple(coercions.expect(roles.ByOfRole, e) for e in exprs)


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


@compiles(PostCriteriaClauseCols)
def _compile_psc_cols(element, compiler, **kw):
    return f"""PC COLS ({
        ', '.join(compiler.process(expr, **kw) for expr in element.exprs)
    })"""


@compiles(PostBodyClause)
def _compile_psb(element, compiler, **kw):
    return "POST SELECT BODY"


@compiles(PostValuesClause)
def _compile_pvc(element, compiler, **kw):
    return "POST VALUES"


class TestExtensionPoints(QueryTest, AssertsCompiledSQL):
    __dialect__ = "default"

    def test_select_post_select_clause(self):
        User = self.classes.User

        stmt = select(User).ext(PostSelectClause()).where(User.name == "x")
        self.assert_compile(
            stmt,
            "SELECT POST SELECT KEYWORD users.id, users.name "
            "FROM users WHERE users.name = :name_1",
        )

    def test_select_pre_columns_clause(self):
        User = self.classes.User

        stmt = select(User).ext(PreColumnsClause()).where(User.name == "x")
        self.assert_compile(
            stmt,
            "SELECT PRE COLUMNS users.id, users.name FROM users "
            "WHERE users.name = :name_1",
        )

    def test_select_post_criteria_clause(self):
        User = self.classes.User

        stmt = select(User).ext(PostCriteriaClause()).where(User.name == "x")
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "WHERE users.name = :name_1 POST CRITERIA",
        )

    def test_select_post_criteria_clause_multiple(self):
        User = self.classes.User

        stmt = (
            select(User)
            .ext(PostCriteriaClause())
            .ext(PostCriteriaClause2())
            .where(User.name == "x")
        )
        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "WHERE users.name = :name_1 POST CRITERIA 2 POST CRITERIA 2",
        )

    def test_select_post_select_body(self):
        User = self.classes.User

        stmt = select(User).ext(PostBodyClause()).where(User.name == "x")

        self.assert_compile(
            stmt,
            "SELECT users.id, users.name FROM users "
            "WHERE users.name = :name_1 POST SELECT BODY",
        )

    def test_insert_post_values(self):
        User = self.classes.User

        self.assert_compile(
            insert(User).ext(PostValuesClause()),
            "INSERT INTO users (id, name) VALUES (:id, :name) POST VALUES",
        )

    def test_update_post_criteria(self):
        User = self.classes.User

        self.assert_compile(
            update(User).ext(PostCriteriaClause()).where(User.name == "hi"),
            "UPDATE users SET id=:id, name=:name "
            "WHERE users.name = :name_1 POST CRITERIA",
        )

    @testing.combinations(
        (lambda User: select(1).ext(PostCriteriaClauseCols(User.id)), True),
        (
            lambda User: select(1).ext(PostCriteriaClauseColsNoProp(User.id)),
            False,
        ),
        (
            lambda User, users: users.update().ext(
                PostCriteriaClauseCols(User.id)
            ),
            True,
        ),
        (
            lambda User, users: users.delete().ext(
                PostCriteriaClauseCols(User.id)
            ),
            True,
        ),
        (lambda User, users: users.delete(), False),
    )
    def test_propagate_attrs(self, stmt, expected):
        User = self.classes.User
        user_table = self.tables.users

        stmt = testing.resolve_lambda(stmt, User=User, users=user_table)

        if expected:
            eq_(
                stmt._propagate_attrs,
                {
                    "compile_state_plugin": "orm",
                    "plugin_subject": inspect(User),
                },
            )
        else:
            eq_(stmt._propagate_attrs, {})
