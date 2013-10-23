# sql/default_comparator.py
# Copyright (C) 2005-2013 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Default implementation of SQL comparison operations.
"""

from .. import exc, util
from . import operators
from . import type_api
from .elements import BindParameter, True_, False_, BinaryExpression, \
        Null, _const_expr, _clause_element_as_expr, \
        ClauseList, ColumnElement, TextClause, UnaryExpression, \
        collate, _is_literal, _literal_as_text
from .selectable import SelectBase, Alias, Selectable, ScalarSelect

class _DefaultColumnComparator(operators.ColumnOperators):
    """Defines comparison and math operations.

    See :class:`.ColumnOperators` and :class:`.Operators` for descriptions
    of all operations.

    """

    @util.memoized_property
    def type(self):
        return self.expr.type

    def operate(self, op, *other, **kwargs):
        o = self.operators[op.__name__]
        return o[0](self, self.expr, op, *(other + o[1:]), **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        o = self.operators[op.__name__]
        return o[0](self, self.expr, op, other, reverse=True, *o[1:], **kwargs)

    def _adapt_expression(self, op, other_comparator):
        """evaluate the return type of <self> <op> <othertype>,
        and apply any adaptations to the given operator.

        This method determines the type of a resulting binary expression
        given two source types and an operator.   For example, two
        :class:`.Column` objects, both of the type :class:`.Integer`, will
        produce a :class:`.BinaryExpression` that also has the type
        :class:`.Integer` when compared via the addition (``+``) operator.
        However, using the addition operator with an :class:`.Integer`
        and a :class:`.Date` object will produce a :class:`.Date`, assuming
        "days delta" behavior by the database (in reality, most databases
        other than Postgresql don't accept this particular operation).

        The method returns a tuple of the form <operator>, <type>.
        The resulting operator and type will be those applied to the
        resulting :class:`.BinaryExpression` as the final operator and the
        right-hand side of the expression.

        Note that only a subset of operators make usage of
        :meth:`._adapt_expression`,
        including math operators and user-defined operators, but not
        boolean comparison or special SQL keywords like MATCH or BETWEEN.

        """
        return op, other_comparator.type

    def _boolean_compare(self, expr, op, obj, negate=None, reverse=False,
                        _python_is_types=(util.NoneType, bool),
                        **kwargs):

        if isinstance(obj, _python_is_types + (Null, True_, False_)):

            # allow x ==/!= True/False to be treated as a literal.
            # this comes out to "== / != true/false" or "1/0" if those
            # constants aren't supported and works on all platforms
            if op in (operators.eq, operators.ne) and \
                    isinstance(obj, (bool, True_, False_)):
                return BinaryExpression(expr,
                                _literal_as_text(obj),
                                op,
                                type_=type_api.BOOLEANTYPE,
                                negate=negate, modifiers=kwargs)
            else:
                # all other None/True/False uses IS, IS NOT
                if op in (operators.eq, operators.is_):
                    return BinaryExpression(expr, _const_expr(obj),
                            operators.is_,
                            negate=operators.isnot)
                elif op in (operators.ne, operators.isnot):
                    return BinaryExpression(expr, _const_expr(obj),
                            operators.isnot,
                            negate=operators.is_)
                else:
                    raise exc.ArgumentError(
                        "Only '=', '!=', 'is_()', 'isnot()' operators can "
                        "be used with None/True/False")
        else:
            obj = self._check_literal(expr, op, obj)

        if reverse:
            return BinaryExpression(obj,
                            expr,
                            op,
                            type_=type_api.BOOLEANTYPE,
                            negate=negate, modifiers=kwargs)
        else:
            return BinaryExpression(expr,
                            obj,
                            op,
                            type_=type_api.BOOLEANTYPE,
                            negate=negate, modifiers=kwargs)

    def _binary_operate(self, expr, op, obj, reverse=False, result_type=None,
                            **kw):
        obj = self._check_literal(expr, op, obj)

        if reverse:
            left, right = obj, expr
        else:
            left, right = expr, obj

        if result_type is None:
            op, result_type = left.comparator._adapt_expression(
                                                op, right.comparator)

        return BinaryExpression(left, right, op, type_=result_type)

    def _scalar(self, expr, op, fn, **kw):
        return fn(expr)

    def _in_impl(self, expr, op, seq_or_selectable, negate_op, **kw):
        seq_or_selectable = _clause_element_as_expr(seq_or_selectable)

        if isinstance(seq_or_selectable, ScalarSelect):
            return self._boolean_compare(expr, op, seq_or_selectable,
                                  negate=negate_op)
        elif isinstance(seq_or_selectable, SelectBase):

            # TODO: if we ever want to support (x, y, z) IN (select x,
            # y, z from table), we would need a multi-column version of
            # as_scalar() to produce a multi- column selectable that
            # does not export itself as a FROM clause

            return self._boolean_compare(
                expr, op, seq_or_selectable.as_scalar(),
                negate=negate_op, **kw)
        elif isinstance(seq_or_selectable, (Selectable, TextClause)):
            return self._boolean_compare(expr, op, seq_or_selectable,
                                  negate=negate_op, **kw)

        # Handle non selectable arguments as sequences
        args = []
        for o in seq_or_selectable:
            if not _is_literal(o):
                if not isinstance(o, operators.ColumnOperators):
                    raise exc.InvalidRequestError('in() function accept'
                            's either a list of non-selectable values, '
                            'or a selectable: %r' % o)
            elif o is None:
                o = Null()
            else:
                o = expr._bind_param(op, o)
            args.append(o)
        if len(args) == 0:

            # Special case handling for empty IN's, behave like
            # comparison against zero row selectable.  We use != to
            # build the contradiction as it handles NULL values
            # appropriately, i.e. "not (x IN ())" should not return NULL
            # values for x.

            util.warn('The IN-predicate on "%s" was invoked with an '
                      'empty sequence. This results in a '
                      'contradiction, which nonetheless can be '
                      'expensive to evaluate.  Consider alternative '
                      'strategies for improved performance.' % expr)
            if op is operators.in_op:
                return expr != expr
            else:
                return expr == expr

        return self._boolean_compare(expr, op,
                              ClauseList(*args).self_group(against=op),
                              negate=negate_op)

    def _unsupported_impl(self, expr, op, *arg, **kw):
        raise NotImplementedError("Operator '%s' is not supported on "
                            "this expression" % op.__name__)

    def _neg_impl(self, expr, op, **kw):
        """See :meth:`.ColumnOperators.__neg__`."""
        return UnaryExpression(expr, operator=operators.neg)

    def _match_impl(self, expr, op, other, **kw):
        """See :meth:`.ColumnOperators.match`."""
        return self._boolean_compare(expr, operators.match_op,
                              self._check_literal(expr, operators.match_op,
                              other))

    def _distinct_impl(self, expr, op, **kw):
        """See :meth:`.ColumnOperators.distinct`."""
        return UnaryExpression(expr, operator=operators.distinct_op,
                                type_=expr.type)

    def _between_impl(self, expr, op, cleft, cright, **kw):
        """See :meth:`.ColumnOperators.between`."""
        return BinaryExpression(
                expr,
                ClauseList(
                    self._check_literal(expr, operators.and_, cleft),
                    self._check_literal(expr, operators.and_, cright),
                    operator=operators.and_,
                    group=False, group_contents=False),
                operators.between_op)

    def _collate_impl(self, expr, op, other, **kw):
        return collate(expr, other)

    # a mapping of operators with the method they use, along with
    # their negated operator for comparison operators
    operators = {
        "add": (_binary_operate,),
        "mul": (_binary_operate,),
        "sub": (_binary_operate,),
        "div": (_binary_operate,),
        "mod": (_binary_operate,),
        "truediv": (_binary_operate,),
        "custom_op": (_binary_operate,),
        "concat_op": (_binary_operate,),
        "lt": (_boolean_compare, operators.ge),
        "le": (_boolean_compare, operators.gt),
        "ne": (_boolean_compare, operators.eq),
        "gt": (_boolean_compare, operators.le),
        "ge": (_boolean_compare, operators.lt),
        "eq": (_boolean_compare, operators.ne),
        "like_op": (_boolean_compare, operators.notlike_op),
        "ilike_op": (_boolean_compare, operators.notilike_op),
        "notlike_op": (_boolean_compare, operators.like_op),
        "notilike_op": (_boolean_compare, operators.ilike_op),
        "contains_op": (_boolean_compare, operators.notcontains_op),
        "startswith_op": (_boolean_compare, operators.notstartswith_op),
        "endswith_op": (_boolean_compare, operators.notendswith_op),
        "desc_op": (_scalar, UnaryExpression._create_desc),
        "asc_op": (_scalar, UnaryExpression._create_asc),
        "nullsfirst_op": (_scalar, UnaryExpression._create_nullsfirst),
        "nullslast_op": (_scalar, UnaryExpression._create_nullslast),
        "in_op": (_in_impl, operators.notin_op),
        "notin_op": (_in_impl, operators.in_op),
        "is_": (_boolean_compare, operators.is_),
        "isnot": (_boolean_compare, operators.isnot),
        "collate": (_collate_impl,),
        "match_op": (_match_impl,),
        "distinct_op": (_distinct_impl,),
        "between_op": (_between_impl, ),
        "neg": (_neg_impl,),
        "getitem": (_unsupported_impl,),
        "lshift": (_unsupported_impl,),
        "rshift": (_unsupported_impl,),
    }

    def _check_literal(self, expr, operator, other):
        if isinstance(other, (ColumnElement, TextClause)):
            if isinstance(other, BindParameter) and \
                    other.type._isnull:
                other = other._clone()
                other.type = expr.type
            return other
        elif hasattr(other, '__clause_element__'):
            other = other.__clause_element__()
        elif isinstance(other, type_api.TypeEngine.Comparator):
            other = other.expr

        if isinstance(other, (SelectBase, Alias)):
            return other.as_scalar()
        elif not isinstance(other, (ColumnElement, TextClause)):
            return expr._bind_param(operator, other)
        else:
            return other

