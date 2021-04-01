from functools import wraps

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import (
    ColumnElement,
    ClauseElementBatch,
)


def property_enables_flag(flag_name):
    def wrapper(target):
        @property
        @wraps(target)
        def inner(self):
            new_flags = self.flags.copy()
            new_flags[flag_name] = True

            return match(
                self.clause,
                against=self.against,
                flags=new_flags,
            )

        return inner
    return wrapper


class match(ColumnElement):
    """Produce a ``MATCH (X, Y) AGAINST ('TEXT')`` clause.

    E.g.::

        from sqlalchemy import desc
        from sqlalchemy.mysql.dialects.mysql.expression import match

        match_expr = match(
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

    The :func:`.match` function is a standalone version of the
    :meth:`_expression.ColumnElement.match` method available on all
    SQL expressions, as when :meth:`_expression.ColumnElement.match` is
    used, but allows to pass multiple columns

    All positional arguments passed to :func:`.match`, typically should be a
     :class:`_expression.ColumnElement` instances

    :param against: typically scalar expression to be coerced into a ``str``

    :param flags: optional ``dict``

     .. versionadded:: 1.4.4

    .. seealso::

        :meth:`_expression.ColumnElement.match`

    """

    default_flags = {
        'mysql_boolean_mode': False,
        'mysql_natural_language': False,
        'mysql_query_expansion': False,
    }

    def __init__(self, *clauselist, against, flags=None):
        if len(clauselist) == 1:
            self.clause = clauselist[0]
        else:
            self.clause = ClauseElementBatch(*clauselist, group=False)

        self.against = against
        self.flags = flags or self.default_flags.copy()

    @property_enables_flag('mysql_boolean_mode')
    def in_boolean_mode(self): ...

    @property_enables_flag('mysql_natural_language')
    def in_natural_language_mode(self): ...

    @property_enables_flag('mysql_query_expansion')
    def with_query_expansion(self): ...


@compiles(match, "mysql")
def visit_match(element: match, compiler, **kw):
    target = element.clause.match(
        element.against,
        **element.flags
    )

    return compiler.process(target, **kw)
