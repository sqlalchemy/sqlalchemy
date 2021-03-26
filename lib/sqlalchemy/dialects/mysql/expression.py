from ...sql.elements import ClauseElementBatch


def match(*clauselist, against, modifier=None, **kwargs):
    """Produce a ``MATCH (X, Y) AGAINST ('TEXT')`` clause.

    E.g.::

        from sqlalchemy.mysql.dialects.mysql.expression import match

        from sqlalchemy.mysql.dialects.mysql.expression_enum \
            import MatchExpressionModifier

        match_columns_where = match(
            users_table.c.firstname,
            users_table.c.lastname,
            against="John Connor",
            modifier=MatchExpressionModifier.in_boolean_mode,
        )

        match_columns_order = match(
            users_table.c.firstname,
            users_table.c.lastname,
            against="John Connor",
        )

        stmt = select(users_table)\
            .where(match_columns_where)\
            .order_by(match_columns_order)

    Would produce SQL resembling::

        SELECT id, firstname, lastname FROM user
        WHERE MATCH(firstname, lastname)
            AGAINST (:param_1 IN BOOLEAN MODE)
        ORDER BY MATCH(firstname, lastname) AGAINST (:param_2)

    The :func:`.match` function is a standalone version of the
    :meth:`_expression.ColumnElement.match` method available on all
    SQL expressions, as when :meth:`_expression.ColumnElement.match` is
    used, but allows to pass multiple columns

    All positional arguments passed to :func:`.match`, should
    be :class:`_expression.ColumnElement` subclass.

    :param clauselist: a column iterator, typically a
     :class:`_expression.ColumnElement` instances or alternatively a Python
     scalar expression to be coerced into a column expression,
     serving as the ``MATCH`` side of expression.

    :param modifier: ``None`` or member of
     :class:`.expression_enum.MatchExpressionModifier`.

    :

     .. versionadded:: 1.4.4

    .. seealso::

        :meth:`_expression.ColumnElement.match`

    """

    clause_batch = ClauseElementBatch(*clauselist, group=False)
    return clause_batch.match(against, modifier=modifier, **kwargs)
