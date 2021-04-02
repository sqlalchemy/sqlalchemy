from sqlalchemy import exc
from sqlalchemy import String
from sqlalchemy.dialects.mysql import base as mysql
from sqlalchemy.dialects.mysql import match
from sqlalchemy.sql import column
from sqlalchemy.sql import table
# from sqlalchemy.sql.expression import literal_column
from sqlalchemy.testing import assert_raises_message
from sqlalchemy.testing import AssertsCompiledSQL
from sqlalchemy.testing import fixtures


class MatchExpressionTest(fixtures.TestBase, AssertsCompiledSQL):

    __dialect__ = mysql.dialect()

    matcheble = table(
        "user",
        column("firstname", String),
        column("lastname", String),
    )

    def test_match_expression(self):
        firstname = self.matcheble.c.firstname
        lastname = self.matcheble.c.lastname

        expr = match(firstname, lastname, against="John Connor")

        self.assert_compile(
            expr,
            "MATCH (user.firstname, user.lastname) AGAINST (%s)",
            dialect=self.__dialect__,
        )

        self.assert_compile(
            expr.in_boolean_mode,
            "MATCH (user.firstname, user.lastname) AGAINST "
            "(%s IN BOOLEAN MODE)",
            dialect=self.__dialect__,
        )

        self.assert_compile(
            expr.in_natural_language_mode,
            "MATCH (user.firstname, user.lastname) AGAINST "
            "(%s IN NATURAL LANGUAGE MODE)",
            dialect=self.__dialect__,
        )

        self.assert_compile(
            expr.with_query_expansion,
            "MATCH (user.firstname, user.lastname) AGAINST "
            "(%s WITH QUERY EXPANSION)",
            dialect=self.__dialect__,
        )

        self.assert_compile(
            expr.in_natural_language_mode.with_query_expansion,
            "MATCH (user.firstname, user.lastname) AGAINST "
            "(%s IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)",
            dialect=self.__dialect__,
        )

    def test_match_expression_fails(self):
        firstname = self.matcheble.c.firstname
        lastname = self.matcheble.c.lastname

        assert_raises_message(
            exc.CompileError,
            "Can not match with no columns",
            match,
            against="John Connor",
        )

        expr = match(firstname, lastname, against="John Connor")

        msg = "Flag combination does not make sence: " \
            "mysql_boolean_mode=%s, " \
            "mysql_natural_language=%s, " \
            "mysql_query_expansion=%s"

        assert_raises_message(
            exc.CompileError,
            msg % (True, False, True),
            expr.in_boolean_mode.with_query_expansion
            .compile,
            dialect=self.__dialect__,
        )

        assert_raises_message(
            exc.CompileError,
            msg % (True, True, False),
            expr.in_boolean_mode.in_natural_language_mode
            .compile,
            dialect=self.__dialect__,
        )

        assert_raises_message(
            exc.CompileError,
            msg % (True, True, True),
            expr.in_boolean_mode.in_natural_language_mode.with_query_expansion
            .compile,
            dialect=self.__dialect__,
        )
