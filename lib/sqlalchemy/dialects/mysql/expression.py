from functools import wraps

from sqlalchemy import exc
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import elements
from sqlalchemy.sql import operators
from sqlalchemy.util import immutabledict


def property_enables_flag(flag_name):
    def wrapper(target):
        @property
        @wraps(target)
        def inner(self):
            update = {flag_name: True}
            new_flags = self.flags.union(update)

            return match_(
                self.clause,
                against=self.against,
                flags=new_flags,
            )

        return inner

    return wrapper


class match_(elements.ColumnElement):
    """Produce a ``MATCH (X, Y) AGAINST ('TEXT')`` clause.

    E.g.::

        from sqlalchemy import desc
        from sqlalchemy.mysql.dialects.mysql import match_

        match_expr = match_(
            users_table.c.firstname,
            users_table.c.lastname,
            against="John Connor",
        )

        stmt = select(users_table)\
            .where(match_expr.in_boolean_mode)\
            .order_by(desc(match_expr))

    Would produce SQL resembling::

        SELECT id, firstname, lastname
        FROM user
        WHERE MATCH(firstname, lastname) AGAINST (:param_1 IN BOOLEAN MODE)
        ORDER BY MATCH(firstname, lastname) AGAINST (:param_2) DESC

    The :func:`.match_` function is a standalone version of the
    :meth:`_expression.ColumnElement.match_` method available on all
    SQL expressions, as when :meth:`_expression.ColumnElement.match_` is
    used, but allows to pass multiple columns

    All positional arguments passed to :func:`.match_`, typically should be a
     :class:`_expression.ColumnElement` instances

    :param: against typically scalar expression to be coerced into a ``str``

    :param: flags optional ``dict``. Use properties ``in_boolean_mode``,
     ``in_natural_language_mode`` and ``with_query_expansion`` to control it::

        match_expr = match_(
            users_table.c.firstname,
            users_table.c.lastname,
            against="John Connor",
        )

        print(match_expr)

        # MATCH(firstname, lastname) AGAINST (:param_1)

        print(match_expr.in_boolean_mode)

        # MATCH(firstname, lastname) AGAINST (:param_1 IN BOOLEAN MODE)

        print(match_expr.in_natural_language_mode.with_query_expansion)

        # MATCH(firstname, lastname) AGAINST
        # (:param_1 IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)

    :property: ``in_boolean_mode`` returns new ``match_`` object with
     set to ``True`` the ``mysql_boolean_mode`` flag

    :property: ``in_natural_language_mode`` returns new ``match_`` object with
     set to ``True`` the ``mysql_natural_language`` flag

    :property: ``with_query_expansion`` returns new ``match_`` object with
     set to ``True`` the ``mysql_query_expansion`` flag

    .. versionadded:: 1.4.20

    .. seealso::

        :meth:`_expression.ColumnElement.match_`

    """

    default_flags = immutabledict(
        mysql_boolean_mode=False,
        mysql_natural_language=False,
        mysql_query_expansion=False,
    )

    def __init__(self, *clauselist, **kwargs):
        clauselist_len = len(clauselist)
        if clauselist_len == 0:
            raise exc.CompileError("Can not match with no columns")
        elif clauselist_len == 1:
            self.clause = clauselist[0]
        else:
            clause = elements.BooleanClauseList._construct_raw(
                operators.comma_op,
                clauses=clauselist,
            )
            clause.group = False
            self.clause = clause

        against = kwargs.get("against")
        flags = kwargs.get("flags")

        if against is None:
            raise exc.CompileError("Can not match without against")

        self.against = against
        self.flags = flags or self.default_flags

    @property_enables_flag("mysql_boolean_mode")
    def in_boolean_mode(self):
        pass

    @property_enables_flag("mysql_natural_language")
    def in_natural_language_mode(self):
        pass

    @property_enables_flag("mysql_query_expansion")
    def with_query_expansion(self):
        pass


@compiles(match_, "mysql")
def visit_match(element, compiler, **kw):
    target = element.clause.match(element.against, **element.flags)

    return compiler.process(target, **kw)
