from functools import wraps

from sqlalchemy import exc
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import (
    ColumnElement,
    BooleanClauseList,
)


def property_enables_flag(flag_name):
    def wrapper(target):
        @property
        @wraps(target)
        def inner(self):
            new_flags = self.flags.copy()
            new_flags[flag_name] = True

            return match_(
                self.clause,
                against=self.against,
                flags=new_flags,
            )

        return inner
    return wrapper


class match_(ColumnElement):
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

    :param: against typically scalar expression to be coerced into a ``str``

    :param: flags optional ``dict``. Use properties ``in_boolean_mode``,
     ``in_natural_language_mode`` and ``with_query_expansion`` to control it:

        match_expr = match(
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

    :property: ``in_boolean_mode`` returns new ``match`` object with
     set to ``True`` the ``mysql_boolean_mode`` flag

    :property: ``in_natural_language_mode`` returns new ``match`` object with
     set to ``True`` the ``mysql_natural_language`` flag

    :property: ``with_query_expansion`` returns new ``match`` object with
     set to ``True`` the ``mysql_query_expansion`` flag

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
        clauselist_len = len(clauselist)
        if clauselist_len == 0:
            raise exc.CompileError("Can not match with no columns")
        elif clauselist_len == 1:
            self.clause = clauselist[0]
        else:
            clause = BooleanClauseList._construct_raw(
                operators.comma_op,
                clauses=clauselist,
            )
            clause.group = False
            self.clause = clause

        self.against = against
        self.flags = flags or self.default_flags.copy()

    @property_enables_flag('mysql_boolean_mode')
    def in_boolean_mode(self): pass

    @property_enables_flag('mysql_natural_language')
    def in_natural_language_mode(self): pass

    @property_enables_flag('mysql_query_expansion')
    def with_query_expansion(self): pass



@compiles(match_, "mysql")
def visit_match(element, compiler, **kw):
    target = element.clause.match(
        element.against,
        **element.flags
    )

    return compiler.process(target, **kw)
