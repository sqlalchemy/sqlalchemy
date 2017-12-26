# sql/operators.py
# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Defines operators used in SQL expressions."""

from .. import util

from operator import (
    and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg,
    getitem, lshift, rshift, contains
)

if util.py2k:
    from operator import div
else:
    div = truediv


class Operators(object):
    """Base of comparison and logical operators.

    Implements base methods
    :meth:`~sqlalchemy.sql.operators.Operators.operate` and
    :meth:`~sqlalchemy.sql.operators.Operators.reverse_operate`, as well as
    :meth:`~sqlalchemy.sql.operators.Operators.__and__`,
    :meth:`~sqlalchemy.sql.operators.Operators.__or__`,
    :meth:`~sqlalchemy.sql.operators.Operators.__invert__`.

    Usually is used via its most common subclass
    :class:`.ColumnOperators`.

    """
    __slots__ = ()

    def __and__(self, other):
        """Implement the ``&`` operator.

        When used with SQL expressions, results in an
        AND operation, equivalent to
        :func:`~.expression.and_`, that is::

            a & b

        is equivalent to::

            from sqlalchemy import and_
            and_(a, b)

        Care should be taken when using ``&`` regarding
        operator precedence; the ``&`` operator has the highest precedence.
        The operands should be enclosed in parenthesis if they contain
        further sub expressions::

            (a == 2) & (b == 4)

        """
        return self.operate(and_, other)

    def __or__(self, other):
        """Implement the ``|`` operator.

        When used with SQL expressions, results in an
        OR operation, equivalent to
        :func:`~.expression.or_`, that is::

            a | b

        is equivalent to::

            from sqlalchemy import or_
            or_(a, b)

        Care should be taken when using ``|`` regarding
        operator precedence; the ``|`` operator has the highest precedence.
        The operands should be enclosed in parenthesis if they contain
        further sub expressions::

            (a == 2) | (b == 4)

        """
        return self.operate(or_, other)

    def __invert__(self):
        """Implement the ``~`` operator.

        When used with SQL expressions, results in a
        NOT operation, equivalent to
        :func:`~.expression.not_`, that is::

            ~a

        is equivalent to::

            from sqlalchemy import not_
            not_(a)

        """
        return self.operate(inv)

    def op(
            self, opstring, precedence=0, is_comparison=False,
            return_type=None):
        """produce a generic operator function.

        e.g.::

          somecolumn.op("*")(5)

        produces::

          somecolumn * 5

        This function can also be used to make bitwise operators explicit. For
        example::

          somecolumn.op('&')(0xff)

        is a bitwise AND of the value in ``somecolumn``.

        :param operator: a string which will be output as the infix operator
          between this element and the expression passed to the
          generated function.

        :param precedence: precedence to apply to the operator, when
         parenthesizing expressions.  A lower number will cause the expression
         to be parenthesized when applied against another operator with
         higher precedence.  The default value of ``0`` is lower than all
         operators except for the comma (``,``) and ``AS`` operators.
         A value of 100 will be higher or equal to all operators, and -100
         will be lower than or equal to all operators.

         .. versionadded:: 0.8 - added the 'precedence' argument.

        :param is_comparison: if True, the operator will be considered as a
         "comparison" operator, that is which evaluates to a boolean
         true/false value, like ``==``, ``>``, etc.  This flag should be set
         so that ORM relationships can establish that the operator is a
         comparison operator when used in a custom join condition.

         .. versionadded:: 0.9.2 - added the
            :paramref:`.Operators.op.is_comparison` flag.

        :param return_type: a :class:`.TypeEngine` class or object that will
          force the return type of an expression produced by this operator
          to be of that type.   By default, operators that specify
          :paramref:`.Operators.op.is_comparison` will resolve to
          :class:`.Boolean`, and those that do not will be of the same
          type as the left-hand operand.

          .. versionadded:: 1.2.0b3 - added the
             :paramref:`.Operators.op.return_type` argument.

        .. seealso::

            :ref:`types_operators`

            :ref:`relationship_custom_operator`

        """
        operator = custom_op(opstring, precedence, is_comparison, return_type)

        def against(other):
            return operator(self, other)
        return against

    def bool_op(self, opstring, precedence=0):
        """Return a custom boolean operator.

        This method is shorthand for calling
        :meth:`.Operators.op` and passing the
        :paramref:`.Operators.op.is_comparison`
        flag with True.

        .. versionadded:: 1.2.0b3

        .. seealso::

            :meth:`.Operators.op`

        """
        return self.op(opstring, precedence=precedence, is_comparison=True)

    def operate(self, op, *other, **kwargs):
        r"""Operate on an argument.

        This is the lowest level of operation, raises
        :class:`NotImplementedError` by default.

        Overriding this on a subclass can allow common
        behavior to be applied to all operations.
        For example, overriding :class:`.ColumnOperators`
        to apply ``func.lower()`` to the left and right
        side::

            class MyComparator(ColumnOperators):
                def operate(self, op, other):
                    return op(func.lower(self), func.lower(other))

        :param op:  Operator callable.
        :param \*other: the 'other' side of the operation. Will
         be a single scalar for most operations.
        :param \**kwargs: modifiers.  These may be passed by special
         operators such as :meth:`ColumnOperators.contains`.


        """
        raise NotImplementedError(str(op))

    def reverse_operate(self, op, other, **kwargs):
        """Reverse operate on an argument.

        Usage is the same as :meth:`operate`.

        """
        raise NotImplementedError(str(op))


class custom_op(object):
    """Represent a 'custom' operator.

    :class:`.custom_op` is normally instantiated when the
    :meth:`.Operators.op` or :meth:`.Operators.bool_op` methods
    are used to create a custom operator callable.  The class can also be
    used directly when programmatically constructing expressions.   E.g.
    to represent the "factorial" operation::

        from sqlalchemy.sql import UnaryExpression
        from sqlalchemy.sql import operators
        from sqlalchemy import Numeric

        unary = UnaryExpression(table.c.somecolumn,
                modifier=operators.custom_op("!"),
                type_=Numeric)


    .. seealso::

        :meth:`.Operators.op`

        :meth:`.Operators.bool_op`

    """
    __name__ = 'custom_op'

    def __init__(
            self, opstring, precedence=0, is_comparison=False,
            return_type=None, natural_self_precedent=False,
            eager_grouping=False):
        self.opstring = opstring
        self.precedence = precedence
        self.is_comparison = is_comparison
        self.natural_self_precedent = natural_self_precedent
        self.eager_grouping = eager_grouping
        self.return_type = (
            return_type._to_instance(return_type) if return_type else None
        )

    def __eq__(self, other):
        return isinstance(other, custom_op) and \
            other.opstring == self.opstring

    def __hash__(self):
        return id(self)

    def __call__(self, left, right, **kw):
        return left.operate(self, right, **kw)


class ColumnOperators(Operators):
    """Defines boolean, comparison, and other operators for
    :class:`.ColumnElement` expressions.

    By default, all methods call down to
    :meth:`.operate` or :meth:`.reverse_operate`,
    passing in the appropriate operator function from the
    Python builtin ``operator`` module or
    a SQLAlchemy-specific operator function from
    :mod:`sqlalchemy.expression.operators`.   For example
    the ``__eq__`` function::

        def __eq__(self, other):
            return self.operate(operators.eq, other)

    Where ``operators.eq`` is essentially::

        def eq(a, b):
            return a == b

    The core column expression unit :class:`.ColumnElement`
    overrides :meth:`.Operators.operate` and others
    to return further :class:`.ColumnElement` constructs,
    so that the ``==`` operation above is replaced by a clause
    construct.

    See also:

    :ref:`types_operators`

    :attr:`.TypeEngine.comparator_factory`

    :class:`.ColumnOperators`

    :class:`.PropComparator`

    """

    __slots__ = ()

    timetuple = None
    """Hack, allows datetime objects to be compared on the LHS."""

    def __lt__(self, other):
        """Implement the ``<`` operator.

        In a column context, produces the clause ``a < b``.

        """
        return self.operate(lt, other)

    def __le__(self, other):
        """Implement the ``<=`` operator.

        In a column context, produces the clause ``a <= b``.

        """
        return self.operate(le, other)

    __hash__ = Operators.__hash__

    def __eq__(self, other):
        """Implement the ``==`` operator.

        In a column context, produces the clause ``a = b``.
        If the target is ``None``, produces ``a IS NULL``.

        """
        return self.operate(eq, other)

    def __ne__(self, other):
        """Implement the ``!=`` operator.

        In a column context, produces the clause ``a != b``.
        If the target is ``None``, produces ``a IS NOT NULL``.

        """
        return self.operate(ne, other)

    def is_distinct_from(self, other):
        """Implement the ``IS DISTINCT FROM`` operator.

        Renders "a IS DISTINCT FROM b" on most platforms;
        on some such as SQLite may render "a IS NOT b".

        .. versionadded:: 1.1

        """
        return self.operate(is_distinct_from, other)

    def isnot_distinct_from(self, other):
        """Implement the ``IS NOT DISTINCT FROM`` operator.

        Renders "a IS NOT DISTINCT FROM b" on most platforms;
        on some such as SQLite may render "a IS b".

        .. versionadded:: 1.1

        """
        return self.operate(isnot_distinct_from, other)

    def __gt__(self, other):
        """Implement the ``>`` operator.

        In a column context, produces the clause ``a > b``.

        """
        return self.operate(gt, other)

    def __ge__(self, other):
        """Implement the ``>=`` operator.

        In a column context, produces the clause ``a >= b``.

        """
        return self.operate(ge, other)

    def __neg__(self):
        """Implement the ``-`` operator.

        In a column context, produces the clause ``-a``.

        """
        return self.operate(neg)

    def __contains__(self, other):
        return self.operate(contains, other)

    def __getitem__(self, index):
        """Implement the [] operator.

        This can be used by some database-specific types
        such as PostgreSQL ARRAY and HSTORE.

        """
        return self.operate(getitem, index)

    def __lshift__(self, other):
        """implement the << operator.

        Not used by SQLAlchemy core, this is provided
        for custom operator systems which want to use
        << as an extension point.
        """
        return self.operate(lshift, other)

    def __rshift__(self, other):
        """implement the >> operator.

        Not used by SQLAlchemy core, this is provided
        for custom operator systems which want to use
        >> as an extension point.
        """
        return self.operate(rshift, other)

    def concat(self, other):
        """Implement the 'concat' operator.

        In a column context, produces the clause ``a || b``,
        or uses the ``concat()`` operator on MySQL.

        """
        return self.operate(concat_op, other)

    def like(self, other, escape=None):
        r"""Implement the ``like`` operator.

        In a column context, produces the expression::

            a LIKE other

        E.g.::

            stmt = select([sometable]).\
                where(sometable.c.column.like("%foobar%"))

        :param other: expression to be compared
        :param escape: optional escape character, renders the ``ESCAPE``
          keyword, e.g.::

            somecolumn.like("foo/%bar", escape="/")

        .. seealso::

            :meth:`.ColumnOperators.ilike`

        """
        return self.operate(like_op, other, escape=escape)

    def ilike(self, other, escape=None):
        r"""Implement the ``ilike`` operator, e.g. case insensitive LIKE.

        In a column context, produces an expression either of the form::

            lower(a) LIKE lower(other)

        Or on backends that support the ILIKE operator::

            a ILIKE other

        E.g.::

            stmt = select([sometable]).\
                where(sometable.c.column.ilike("%foobar%"))

        :param other: expression to be compared
        :param escape: optional escape character, renders the ``ESCAPE``
          keyword, e.g.::

            somecolumn.ilike("foo/%bar", escape="/")

        .. seealso::

            :meth:`.ColumnOperators.like`

        """
        return self.operate(ilike_op, other, escape=escape)

    def in_(self, other):
        """Implement the ``in`` operator.

        In a column context, produces the clause ``a IN other``.
        "other" may be a tuple/list of column expressions,
        or a :func:`~.expression.select` construct.

        In the case that ``other`` is an empty sequence, the compiler
        produces an "empty in" expression.   This defaults to the
        expression "1 != 1" to produce false in all cases.  The
        :paramref:`.create_engine.empty_in_strategy` may be used to
        alter this behavior.

        .. versionchanged:: 1.2  The :meth:`.ColumnOperators.in_` and
           :meth:`.ColumnOperators.notin_` operators
           now produce a "static" expression for an empty IN sequence
           by default.

        """
        return self.operate(in_op, other)

    def notin_(self, other):
        """implement the ``NOT IN`` operator.

        This is equivalent to using negation with
        :meth:`.ColumnOperators.in_`, i.e. ``~x.in_(y)``.

        In the case that ``other`` is an empty sequence, the compiler
        produces an "empty not in" expression.   This defaults to the
        expression "1 = 1" to produce true in all cases.  The
        :paramref:`.create_engine.empty_in_strategy` may be used to
        alter this behavior.

        .. versionchanged:: 1.2  The :meth:`.ColumnOperators.in_` and
           :meth:`.ColumnOperators.notin_` operators
           now produce a "static" expression for an empty IN sequence
           by default.

        .. seealso::

            :meth:`.ColumnOperators.in_`

        """
        return self.operate(notin_op, other)

    def notlike(self, other, escape=None):
        """implement the ``NOT LIKE`` operator.

        This is equivalent to using negation with
        :meth:`.ColumnOperators.like`, i.e. ``~x.like(y)``.

        .. versionadded:: 0.8

        .. seealso::

            :meth:`.ColumnOperators.like`

        """
        return self.operate(notlike_op, other, escape=escape)

    def notilike(self, other, escape=None):
        """implement the ``NOT ILIKE`` operator.

        This is equivalent to using negation with
        :meth:`.ColumnOperators.ilike`, i.e. ``~x.ilike(y)``.

        .. versionadded:: 0.8

        .. seealso::

            :meth:`.ColumnOperators.ilike`

        """
        return self.operate(notilike_op, other, escape=escape)

    def is_(self, other):
        """Implement the ``IS`` operator.

        Normally, ``IS`` is generated automatically when comparing to a
        value of ``None``, which resolves to ``NULL``.  However, explicit
        usage of ``IS`` may be desirable if comparing to boolean values
        on certain platforms.

        .. versionadded:: 0.7.9

        .. seealso:: :meth:`.ColumnOperators.isnot`

        """
        return self.operate(is_, other)

    def isnot(self, other):
        """Implement the ``IS NOT`` operator.

        Normally, ``IS NOT`` is generated automatically when comparing to a
        value of ``None``, which resolves to ``NULL``.  However, explicit
        usage of ``IS NOT`` may be desirable if comparing to boolean values
        on certain platforms.

        .. versionadded:: 0.7.9

        .. seealso:: :meth:`.ColumnOperators.is_`

        """
        return self.operate(isnot, other)

    def startswith(self, other, **kwargs):
        r"""Implement the ``startswith`` operator.

        Produces a LIKE expression that tests against a match for the start
        of a string value::

            column LIKE <other> || '%'

        E.g.::

            stmt = select([sometable]).\
                where(sometable.c.column.startswith("foobar"))

        Since the operator uses ``LIKE``, wildcard characters
        ``"%"`` and ``"_"`` that are present inside the <other> expression
        will behave like wildcards as well.   For literal string
        values, the :paramref:`.ColumnOperators.startswith.autoescape` flag
        may be set to ``True`` to apply escaping to occurences of these
        characters within the string value so that they match as themselves
        and not as wildcard characters.  Alternatively, the
        :paramref:`.ColumnOperators.startswith.escape` parameter will establish
        a given character as an escape character which can be of use when
        the target expression is not a literal string.

        :param other: expression to be compared.   This is usually a plain
          string value, but can also be an arbitrary SQL expression.  LIKE
          wildcard characters ``%`` and ``_`` are not escaped by default unless
          the :paramref:`.ColumnOperators.startswith.autoescape` flag is
          set to True.

        :param autoescape: boolean; when True, establishes an escape character
          within the LIKE expression, then applies it to all occurrences of
          ``"%"``, ``"_"`` and the escape character itself within the
          comparison value, which is assumed to be a literal string and not a
          SQL expression.

          An expression such as::

            somecolumn.startswith("foo%bar", autoescape=True)

          Will render as::

            somecolumn LIKE :param || '%' ESCAPE '/'

          With the value of :param as ``"foo/%bar"``.

          .. versionadded:: 1.2

          .. versionchanged:: 1.2.0 The
            :paramref:`.ColumnOperators.startswith.autoescape` parameter is
             now a simple boolean rather than a character; the escape
             character itself is also escaped, and defaults to a forwards
             slash, which itself can be customized using the
             :paramref:`.ColumnOperators.startswith.escape` parameter.

        :param escape: a character which when given will render with the
          ``ESCAPE`` keyword to establish that character as the escape
          character.  This character can then be placed preceding occurrences
          of ``%`` and ``_`` to allow them to act as themselves and not
          wildcard characters.

          An expression such as::

            somecolumn.startswith("foo/%bar", escape="^")

          Will render as::

            somecolumn LIKE :param || '%' ESCAPE '^'

          The parameter may also be combined with
          :paramref:`.ColumnOperators.startswith.autoescape`::

            somecolumn.startswith("foo%bar^bat", escape="^", autoescape=True)

          Where above, the given literal parameter will be converted to
          ``"foo^%bar^^bat"`` before being passed to the database.

        .. seealso::

            :meth:`.ColumnOperators.endswith`

            :meth:`.ColumnOperators.contains`

            :meth:`.ColumnOperators.like`

        """
        return self.operate(startswith_op, other, **kwargs)

    def endswith(self, other, **kwargs):
        r"""Implement the 'endswith' operator.

        Produces a LIKE expression that tests against a match for the end
        of a string value::

            column LIKE '%' || <other>

        E.g.::

            stmt = select([sometable]).\
                where(sometable.c.column.endswith("foobar"))

        Since the operator uses ``LIKE``, wildcard characters
        ``"%"`` and ``"_"`` that are present inside the <other> expression
        will behave like wildcards as well.   For literal string
        values, the :paramref:`.ColumnOperators.endswith.autoescape` flag
        may be set to ``True`` to apply escaping to occurences of these
        characters within the string value so that they match as themselves
        and not as wildcard characters.  Alternatively, the
        :paramref:`.ColumnOperators.endswith.escape` parameter will establish
        a given character as an escape character which can be of use when
        the target expression is not a literal string.

        :param other: expression to be compared.   This is usually a plain
          string value, but can also be an arbitrary SQL expression.  LIKE
          wildcard characters ``%`` and ``_`` are not escaped by default unless
          the :paramref:`.ColumnOperators.endswith.autoescape` flag is
          set to True.

        :param autoescape: boolean; when True, establishes an escape character
          within the LIKE expression, then applies it to all occurrences of
          ``"%"``, ``"_"`` and the escape character itself within the
          comparison value, which is assumed to be a literal string and not a
          SQL expression.

          An expression such as::

            somecolumn.endswith("foo%bar", autoescape=True)

          Will render as::

            somecolumn LIKE '%' || :param ESCAPE '/'

          With the value of :param as ``"foo/%bar"``.

          .. versionadded:: 1.2

          .. versionchanged:: 1.2.0 The
            :paramref:`.ColumnOperators.endswith.autoescape` parameter is
             now a simple boolean rather than a character; the escape
             character itself is also escaped, and defaults to a forwards
             slash, which itself can be customized using the
             :paramref:`.ColumnOperators.endswith.escape` parameter.

        :param escape: a character which when given will render with the
          ``ESCAPE`` keyword to establish that character as the escape
          character.  This character can then be placed preceding occurrences
          of ``%`` and ``_`` to allow them to act as themselves and not
          wildcard characters.

          An expression such as::

            somecolumn.endswith("foo/%bar", escape="^")

          Will render as::

            somecolumn LIKE '%' || :param ESCAPE '^'

          The parameter may also be combined with
          :paramref:`.ColumnOperators.endswith.autoescape`::

            somecolumn.endswith("foo%bar^bat", escape="^", autoescape=True)

          Where above, the given literal parameter will be converted to
          ``"foo^%bar^^bat"`` before being passed to the database.

        .. seealso::

            :meth:`.ColumnOperators.startswith`

            :meth:`.ColumnOperators.contains`

            :meth:`.ColumnOperators.like`

        """
        return self.operate(endswith_op, other, **kwargs)

    def contains(self, other, **kwargs):
        r"""Implement the 'contains' operator.

        Produces a LIKE expression that tests against a match for the middle
        of a string value::

            column LIKE '%' || <other> || '%'

        E.g.::

            stmt = select([sometable]).\
                where(sometable.c.column.contains("foobar"))

        Since the operator uses ``LIKE``, wildcard characters
        ``"%"`` and ``"_"`` that are present inside the <other> expression
        will behave like wildcards as well.   For literal string
        values, the :paramref:`.ColumnOperators.contains.autoescape` flag
        may be set to ``True`` to apply escaping to occurences of these
        characters within the string value so that they match as themselves
        and not as wildcard characters.  Alternatively, the
        :paramref:`.ColumnOperators.contains.escape` parameter will establish
        a given character as an escape character which can be of use when
        the target expression is not a literal string.

        :param other: expression to be compared.   This is usually a plain
          string value, but can also be an arbitrary SQL expression.  LIKE
          wildcard characters ``%`` and ``_`` are not escaped by default unless
          the :paramref:`.ColumnOperators.contains.autoescape` flag is
          set to True.

        :param autoescape: boolean; when True, establishes an escape character
          within the LIKE expression, then applies it to all occurrences of
          ``"%"``, ``"_"`` and the escape character itself within the
          comparison value, which is assumed to be a literal string and not a
          SQL expression.

          An expression such as::

            somecolumn.contains("foo%bar", autoescape=True)

          Will render as::

            somecolumn LIKE '%' || :param || '%' ESCAPE '/'

          With the value of :param as ``"foo/%bar"``.

          .. versionadded:: 1.2

          .. versionchanged:: 1.2.0 The
            :paramref:`.ColumnOperators.contains.autoescape` parameter is
             now a simple boolean rather than a character; the escape
             character itself is also escaped, and defaults to a forwards
             slash, which itself can be customized using the
             :paramref:`.ColumnOperators.contains.escape` parameter.

        :param escape: a character which when given will render with the
          ``ESCAPE`` keyword to establish that character as the escape
          character.  This character can then be placed preceding occurrences
          of ``%`` and ``_`` to allow them to act as themselves and not
          wildcard characters.

          An expression such as::

            somecolumn.contains("foo/%bar", escape="^")

          Will render as::

            somecolumn LIKE '%' || :param || '%' ESCAPE '^'

          The parameter may also be combined with
          :paramref:`.ColumnOperators.contains.autoescape`::

            somecolumn.contains("foo%bar^bat", escape="^", autoescape=True)

          Where above, the given literal parameter will be converted to
          ``"foo^%bar^^bat"`` before being passed to the database.

        .. seealso::

            :meth:`.ColumnOperators.startswith`

            :meth:`.ColumnOperators.endswith`

            :meth:`.ColumnOperators.like`


        """
        return self.operate(contains_op, other, **kwargs)

    def match(self, other, **kwargs):
        """Implements a database-specific 'match' operator.

        :meth:`~.ColumnOperators.match` attempts to resolve to
        a MATCH-like function or operator provided by the backend.
        Examples include:

        * PostgreSQL - renders ``x @@ to_tsquery(y)``
        * MySQL - renders ``MATCH (x) AGAINST (y IN BOOLEAN MODE)``
        * Oracle - renders ``CONTAINS(x, y)``
        * other backends may provide special implementations.
        * Backends without any special implementation will emit
          the operator as "MATCH".  This is compatible with SQlite, for
          example.

        """
        return self.operate(match_op, other, **kwargs)

    def desc(self):
        """Produce a :func:`~.expression.desc` clause against the
        parent object."""
        return self.operate(desc_op)

    def asc(self):
        """Produce a :func:`~.expression.asc` clause against the
        parent object."""
        return self.operate(asc_op)

    def nullsfirst(self):
        """Produce a :func:`~.expression.nullsfirst` clause against the
        parent object."""
        return self.operate(nullsfirst_op)

    def nullslast(self):
        """Produce a :func:`~.expression.nullslast` clause against the
        parent object."""
        return self.operate(nullslast_op)

    def collate(self, collation):
        """Produce a :func:`~.expression.collate` clause against
        the parent object, given the collation string.

        .. seealso::

            :func:`~.expression.collate`

        """
        return self.operate(collate, collation)

    def __radd__(self, other):
        """Implement the ``+`` operator in reverse.

        See :meth:`.ColumnOperators.__add__`.

        """
        return self.reverse_operate(add, other)

    def __rsub__(self, other):
        """Implement the ``-`` operator in reverse.

        See :meth:`.ColumnOperators.__sub__`.

        """
        return self.reverse_operate(sub, other)

    def __rmul__(self, other):
        """Implement the ``*`` operator in reverse.

        See :meth:`.ColumnOperators.__mul__`.

        """
        return self.reverse_operate(mul, other)

    def __rdiv__(self, other):
        """Implement the ``/`` operator in reverse.

        See :meth:`.ColumnOperators.__div__`.

        """
        return self.reverse_operate(div, other)

    def __rmod__(self, other):
        """Implement the ``%`` operator in reverse.

        See :meth:`.ColumnOperators.__mod__`.

        """
        return self.reverse_operate(mod, other)

    def between(self, cleft, cright, symmetric=False):
        """Produce a :func:`~.expression.between` clause against
        the parent object, given the lower and upper range.

        """
        return self.operate(between_op, cleft, cright, symmetric=symmetric)

    def distinct(self):
        """Produce a :func:`~.expression.distinct` clause against the
        parent object.

        """
        return self.operate(distinct_op)

    def any_(self):
        """Produce a :func:`~.expression.any_` clause against the
        parent object.

        This operator is only appropriate against a scalar subquery
        object, or for some backends an column expression that is
        against the ARRAY type, e.g.::

            # postgresql '5 = ANY (somearray)'
            expr = 5 == mytable.c.somearray.any_()

            # mysql '5 = ANY (SELECT value FROM table)'
            expr = 5 == select([table.c.value]).as_scalar().any_()

        .. seealso::

            :func:`~.expression.any_` - standalone version

            :func:`~.expression.all_` - ALL operator

        .. versionadded:: 1.1

        """
        return self.operate(any_op)

    def all_(self):
        """Produce a :func:`~.expression.all_` clause against the
        parent object.

        This operator is only appropriate against a scalar subquery
        object, or for some backends an column expression that is
        against the ARRAY type, e.g.::

            # postgresql '5 = ALL (somearray)'
            expr = 5 == mytable.c.somearray.all_()

            # mysql '5 = ALL (SELECT value FROM table)'
            expr = 5 == select([table.c.value]).as_scalar().all_()

        .. seealso::

            :func:`~.expression.all_` - standalone version

            :func:`~.expression.any_` - ANY operator

        .. versionadded:: 1.1

        """
        return self.operate(all_op)

    def __add__(self, other):
        """Implement the ``+`` operator.

        In a column context, produces the clause ``a + b``
        if the parent object has non-string affinity.
        If the parent object has a string affinity,
        produces the concatenation operator, ``a || b`` -
        see :meth:`.ColumnOperators.concat`.

        """
        return self.operate(add, other)

    def __sub__(self, other):
        """Implement the ``-`` operator.

        In a column context, produces the clause ``a - b``.

        """
        return self.operate(sub, other)

    def __mul__(self, other):
        """Implement the ``*`` operator.

        In a column context, produces the clause ``a * b``.

        """
        return self.operate(mul, other)

    def __div__(self, other):
        """Implement the ``/`` operator.

        In a column context, produces the clause ``a / b``.

        """
        return self.operate(div, other)

    def __mod__(self, other):
        """Implement the ``%`` operator.

        In a column context, produces the clause ``a % b``.

        """
        return self.operate(mod, other)

    def __truediv__(self, other):
        """Implement the ``//`` operator.

        In a column context, produces the clause ``a / b``.

        """
        return self.operate(truediv, other)

    def __rtruediv__(self, other):
        """Implement the ``//`` operator in reverse.

        See :meth:`.ColumnOperators.__truediv__`.

        """
        return self.reverse_operate(truediv, other)


def from_():
    raise NotImplementedError()


def as_():
    raise NotImplementedError()


def exists():
    raise NotImplementedError()


def istrue(a):
    raise NotImplementedError()


def isfalse(a):
    raise NotImplementedError()


def is_distinct_from(a, b):
    return a.is_distinct_from(b)


def isnot_distinct_from(a, b):
    return a.isnot_distinct_from(b)


def is_(a, b):
    return a.is_(b)


def isnot(a, b):
    return a.isnot(b)


def collate(a, b):
    return a.collate(b)


def op(a, opstring, b):
    return a.op(opstring)(b)


def like_op(a, b, escape=None):
    return a.like(b, escape=escape)


def notlike_op(a, b, escape=None):
    return a.notlike(b, escape=escape)


def ilike_op(a, b, escape=None):
    return a.ilike(b, escape=escape)


def notilike_op(a, b, escape=None):
    return a.notilike(b, escape=escape)


def between_op(a, b, c, symmetric=False):
    return a.between(b, c, symmetric=symmetric)


def notbetween_op(a, b, c, symmetric=False):
    return a.notbetween(b, c, symmetric=symmetric)


def in_op(a, b):
    return a.in_(b)


def notin_op(a, b):
    return a.notin_(b)


def distinct_op(a):
    return a.distinct()


def any_op(a):
    return a.any_()


def all_op(a):
    return a.all_()


def _escaped_like_impl(fn, other, escape, autoescape):
    if autoescape:
        if autoescape is not True:
            util.warn(
                "The autoescape parameter is now a simple boolean True/False")
        if escape is None:
            escape = '/'

        if not isinstance(other, util.compat.string_types):
            raise TypeError("String value expected when autoescape=True")

        if escape not in ('%', '_'):
            other = other.replace(escape, escape + escape)

        other = (
            other.replace('%', escape + '%').
            replace('_', escape + '_')
        )

    return fn(other, escape=escape)


def startswith_op(a, b, escape=None, autoescape=False):
    return _escaped_like_impl(a.startswith, b, escape, autoescape)


def notstartswith_op(a, b, escape=None, autoescape=False):
    return ~_escaped_like_impl(a.startswith, b, escape, autoescape)


def endswith_op(a, b, escape=None, autoescape=False):
    return _escaped_like_impl(a.endswith, b, escape, autoescape)


def notendswith_op(a, b, escape=None, autoescape=False):
    return ~_escaped_like_impl(a.endswith, b, escape, autoescape)


def contains_op(a, b, escape=None, autoescape=False):
    return _escaped_like_impl(a.contains, b, escape, autoescape)


def notcontains_op(a, b, escape=None, autoescape=False):
    return ~_escaped_like_impl(a.contains, b, escape, autoescape)


def match_op(a, b, **kw):
    return a.match(b, **kw)


def notmatch_op(a, b, **kw):
    return a.notmatch(b, **kw)


def comma_op(a, b):
    raise NotImplementedError()


def empty_in_op(a, b):
    raise NotImplementedError()


def empty_notin_op(a, b):
    raise NotImplementedError()


def concat_op(a, b):
    return a.concat(b)


def desc_op(a):
    return a.desc()


def asc_op(a):
    return a.asc()


def nullsfirst_op(a):
    return a.nullsfirst()


def nullslast_op(a):
    return a.nullslast()


def json_getitem_op(a, b):
    raise NotImplementedError()


def json_path_getitem_op(a, b):
    raise NotImplementedError()


_commutative = {eq, ne, add, mul}

_comparison = {eq, ne, lt, gt, ge, le, between_op, like_op, is_,
               isnot, is_distinct_from, isnot_distinct_from}


def is_comparison(op):
    return op in _comparison or \
        isinstance(op, custom_op) and op.is_comparison


def is_commutative(op):
    return op in _commutative


def is_ordering_modifier(op):
    return op in (asc_op, desc_op,
                  nullsfirst_op, nullslast_op)


def is_natural_self_precedent(op):
    return op in _natural_self_precedent or \
        isinstance(op, custom_op) and op.natural_self_precedent

_mirror = {
    gt: lt,
    ge: le,
    lt: gt,
    le: ge
}


def mirror(op):
    """rotate a comparison operator 180 degrees.

    Note this is not the same as negation.

    """
    return _mirror.get(op, op)


_associative = _commutative.union([concat_op, and_, or_]).difference([eq, ne])

_natural_self_precedent = _associative.union([
    getitem, json_getitem_op, json_path_getitem_op])
"""Operators where if we have (a op b) op c, we don't want to
parenthesize (a op b).

"""


_asbool = util.symbol('_asbool', canonical=-10)
_smallest = util.symbol('_smallest', canonical=-100)
_largest = util.symbol('_largest', canonical=100)

_PRECEDENCE = {
    from_: 15,
    any_op: 15,
    all_op: 15,
    getitem: 15,
    json_getitem_op: 15,
    json_path_getitem_op: 15,

    mul: 8,
    truediv: 8,
    div: 8,
    mod: 8,
    neg: 8,
    add: 7,
    sub: 7,

    concat_op: 6,

    match_op: 5,
    notmatch_op: 5,

    ilike_op: 5,
    notilike_op: 5,
    like_op: 5,
    notlike_op: 5,
    in_op: 5,
    notin_op: 5,

    is_: 5,
    isnot: 5,

    eq: 5,
    ne: 5,
    is_distinct_from: 5,
    isnot_distinct_from: 5,
    empty_in_op: 5,
    empty_notin_op: 5,
    gt: 5,
    lt: 5,
    ge: 5,
    le: 5,

    between_op: 5,
    notbetween_op: 5,
    distinct_op: 5,
    inv: 5,
    istrue: 5,
    isfalse: 5,
    and_: 3,
    or_: 2,
    comma_op: -1,

    desc_op: 3,
    asc_op: 3,
    collate: 4,

    as_: -1,
    exists: 0,

    _asbool: -10,
    _smallest: _smallest,
    _largest: _largest
}


def is_precedent(operator, against):
    if operator is against and is_natural_self_precedent(operator):
        return False
    else:
        return (_PRECEDENCE.get(operator,
                                getattr(operator, 'precedence', _smallest)) <=
                _PRECEDENCE.get(against,
                                getattr(against, 'precedence', _largest)))
