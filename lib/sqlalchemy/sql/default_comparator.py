# sql/default_comparator.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Default implementation of SQL comparison operations.
"""

from .. import exc, util
from . import type_api
from . import operators
from .elements import BindParameter, True_, False_, BinaryExpression, \
    Null, _const_expr, _clause_element_as_expr, \
    ClauseList, ColumnElement, TextClause, UnaryExpression, \
    collate, _is_literal, _literal_as_text, ClauseElement, and_, or_
from .selectable import SelectBase, Alias, Selectable, ScalarSelect


def _boolean_compare(expr, op, obj, negate=None, reverse=False,
                     _python_is_types=(util.NoneType, bool),
                     result_type = None,
                     **kwargs):

    if result_type is None:
        result_type = type_api.BOOLEANTYPE

    if isinstance(obj, _python_is_types + (Null, True_, False_)):

        # allow x ==/!= True/False to be treated as a literal.
        # this comes out to "== / != true/false" or "1/0" if those
        # constants aren't supported and works on all platforms
        if op in (operators.eq, operators.ne) and \
                isinstance(obj, (bool, True_, False_)):
            return BinaryExpression(expr,
                                    _literal_as_text(obj),
                                    op,
                                    type_=result_type,
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
        obj = _check_literal(expr, op, obj)

    if reverse:
        return BinaryExpression(obj,
                                expr,
                                op,
                                type_=result_type,
                                negate=negate, modifiers=kwargs)
    else:
        return BinaryExpression(expr,
                                obj,
                                op,
                                type_=result_type,
                                negate=negate, modifiers=kwargs)


def _binary_operate(expr, op, obj, reverse=False, result_type=None,
                    **kw):
    obj = _check_literal(expr, op, obj)

    if reverse:
        left, right = obj, expr
    else:
        left, right = expr, obj

    if result_type is None:
        op, result_type = left.comparator._adapt_expression(
            op, right.comparator)

    return BinaryExpression(
        left, right, op, type_=result_type, modifiers=kw)


def _conjunction_operate(expr, op, other, **kw):
    if op is operators.and_:
        return and_(expr, other)
    elif op is operators.or_:
        return or_(expr, other)
    else:
        raise NotImplementedError()


def _scalar(expr, op, fn, **kw):
    return fn(expr)


def _in_impl(expr, op, seq_or_selectable, negate_op, **kw):
    seq_or_selectable = _clause_element_as_expr(seq_or_selectable)

    if isinstance(seq_or_selectable, ScalarSelect):
        return _boolean_compare(expr, op, seq_or_selectable,
                                negate=negate_op)
    elif isinstance(seq_or_selectable, SelectBase):

        # TODO: if we ever want to support (x, y, z) IN (select x,
        # y, z from table), we would need a multi-column version of
        # as_scalar() to produce a multi- column selectable that
        # does not export itself as a FROM clause

        return _boolean_compare(
            expr, op, seq_or_selectable.as_scalar(),
            negate=negate_op, **kw)
    elif isinstance(seq_or_selectable, (Selectable, TextClause)):
        return _boolean_compare(expr, op, seq_or_selectable,
                                negate=negate_op, **kw)
    elif isinstance(seq_or_selectable, ClauseElement):
        raise exc.InvalidRequestError(
            'in_() accepts'
            ' either a list of expressions '
            'or a selectable: %r' % seq_or_selectable)

    # Handle non selectable arguments as sequences
    args = []
    for o in seq_or_selectable:
        if not _is_literal(o):
            if not isinstance(o, operators.ColumnOperators):
                raise exc.InvalidRequestError(
                    'in_() accepts'
                    ' either a list of expressions '
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

    return _boolean_compare(expr, op,
                            ClauseList(*args).self_group(against=op),
                            negate=negate_op)


def _unsupported_impl(expr, op, *arg, **kw):
    raise NotImplementedError("Operator '%s' is not supported on "
                              "this expression" % op.__name__)


def _inv_impl(expr, op, **kw):
    """See :meth:`.ColumnOperators.__inv__`."""
    if hasattr(expr, 'negation_clause'):
        return expr.negation_clause
    else:
        return expr._negate()


def _neg_impl(expr, op, **kw):
    """See :meth:`.ColumnOperators.__neg__`."""
    return UnaryExpression(expr, operator=operators.neg)


def _match_impl(expr, op, other, **kw):
    """See :meth:`.ColumnOperators.match`."""

    return _boolean_compare(
        expr, operators.match_op,
        _check_literal(
            expr, operators.match_op, other),
        result_type=type_api.MATCHTYPE,
        negate=operators.notmatch_op
        if op is operators.match_op else operators.match_op,
        **kw
    )


def _distinct_impl(expr, op, **kw):
    """See :meth:`.ColumnOperators.distinct`."""
    return UnaryExpression(expr, operator=operators.distinct_op,
                           type_=expr.type)


def _between_impl(expr, op, cleft, cright, **kw):
    """See :meth:`.ColumnOperators.between`."""
    return BinaryExpression(
        expr,
        ClauseList(
            _check_literal(expr, operators.and_, cleft),
            _check_literal(expr, operators.and_, cright),
            operator=operators.and_,
            group=False, group_contents=False),
        op,
        negate=operators.notbetween_op
        if op is operators.between_op
        else operators.between_op,
        modifiers=kw)


def _collate_impl(expr, op, other, **kw):
    return collate(expr, other)

# a mapping of operators with the method they use, along with
# their negated operator for comparison operators
operator_lookup = {
    "and_": (_conjunction_operate,),
    "or_": (_conjunction_operate,),
    "inv": (_inv_impl,),
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
    "notmatch_op": (_match_impl,),
    "distinct_op": (_distinct_impl,),
    "between_op": (_between_impl, ),
    "notbetween_op": (_between_impl, ),
    "neg": (_neg_impl,),
    "getitem": (_unsupported_impl,),
    "lshift": (_unsupported_impl,),
    "rshift": (_unsupported_impl,),
}


def _check_literal(expr, operator, other):
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

