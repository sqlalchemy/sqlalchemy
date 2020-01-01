# sql/default_comparator.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Default implementation of SQL comparison operations.
"""


from . import coercions
from . import operators
from . import roles
from . import type_api
from .elements import and_
from .elements import BinaryExpression
from .elements import ClauseList
from .elements import collate
from .elements import CollectionAggregate
from .elements import False_
from .elements import Null
from .elements import or_
from .elements import True_
from .elements import UnaryExpression
from .. import exc
from .. import util


def _boolean_compare(
    expr,
    op,
    obj,
    negate=None,
    reverse=False,
    _python_is_types=(util.NoneType, bool),
    result_type=None,
    **kwargs
):

    if result_type is None:
        result_type = type_api.BOOLEANTYPE

    if isinstance(obj, _python_is_types + (Null, True_, False_)):

        # allow x ==/!= True/False to be treated as a literal.
        # this comes out to "== / != true/false" or "1/0" if those
        # constants aren't supported and works on all platforms
        if op in (operators.eq, operators.ne) and isinstance(
            obj, (bool, True_, False_)
        ):
            return BinaryExpression(
                expr,
                coercions.expect(roles.ConstExprRole, obj),
                op,
                type_=result_type,
                negate=negate,
                modifiers=kwargs,
            )
        elif op in (operators.is_distinct_from, operators.isnot_distinct_from):
            return BinaryExpression(
                expr,
                coercions.expect(roles.ConstExprRole, obj),
                op,
                type_=result_type,
                negate=negate,
                modifiers=kwargs,
            )
        else:
            # all other None/True/False uses IS, IS NOT
            if op in (operators.eq, operators.is_):
                return BinaryExpression(
                    expr,
                    coercions.expect(roles.ConstExprRole, obj),
                    operators.is_,
                    negate=operators.isnot,
                    type_=result_type,
                )
            elif op in (operators.ne, operators.isnot):
                return BinaryExpression(
                    expr,
                    coercions.expect(roles.ConstExprRole, obj),
                    operators.isnot,
                    negate=operators.is_,
                    type_=result_type,
                )
            else:
                raise exc.ArgumentError(
                    "Only '=', '!=', 'is_()', 'isnot()', "
                    "'is_distinct_from()', 'isnot_distinct_from()' "
                    "operators can be used with None/True/False"
                )
    else:
        obj = coercions.expect(
            roles.BinaryElementRole, element=obj, operator=op, expr=expr
        )

    if reverse:
        return BinaryExpression(
            obj, expr, op, type_=result_type, negate=negate, modifiers=kwargs
        )
    else:
        return BinaryExpression(
            expr, obj, op, type_=result_type, negate=negate, modifiers=kwargs
        )


def _custom_op_operate(expr, op, obj, reverse=False, result_type=None, **kw):
    if result_type is None:
        if op.return_type:
            result_type = op.return_type
        elif op.is_comparison:
            result_type = type_api.BOOLEANTYPE

    return _binary_operate(
        expr, op, obj, reverse=reverse, result_type=result_type, **kw
    )


def _binary_operate(expr, op, obj, reverse=False, result_type=None, **kw):
    obj = coercions.expect(
        roles.BinaryElementRole, obj, expr=expr, operator=op
    )

    if reverse:
        left, right = obj, expr
    else:
        left, right = expr, obj

    if result_type is None:
        op, result_type = left.comparator._adapt_expression(
            op, right.comparator
        )

    return BinaryExpression(left, right, op, type_=result_type, modifiers=kw)


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
    seq_or_selectable = coercions.expect(
        roles.InElementRole, seq_or_selectable, expr=expr, operator=op
    )
    if "in_ops" in seq_or_selectable._annotations:
        op, negate_op = seq_or_selectable._annotations["in_ops"]

    return _boolean_compare(
        expr, op, seq_or_selectable, negate=negate_op, **kw
    )


def _getitem_impl(expr, op, other, **kw):
    if isinstance(expr.type, type_api.INDEXABLE):
        other = coercions.expect(
            roles.BinaryElementRole, other, expr=expr, operator=op
        )
        return _binary_operate(expr, op, other, **kw)
    else:
        _unsupported_impl(expr, op, other, **kw)


def _unsupported_impl(expr, op, *arg, **kw):
    raise NotImplementedError(
        "Operator '%s' is not supported on " "this expression" % op.__name__
    )


def _inv_impl(expr, op, **kw):
    """See :meth:`.ColumnOperators.__inv__`."""

    # undocumented element currently used by the ORM for
    # relationship.contains()
    if hasattr(expr, "negation_clause"):
        return expr.negation_clause
    else:
        return expr._negate()


def _neg_impl(expr, op, **kw):
    """See :meth:`.ColumnOperators.__neg__`."""
    return UnaryExpression(expr, operator=operators.neg, type_=expr.type)


def _match_impl(expr, op, other, **kw):
    """See :meth:`.ColumnOperators.match`."""

    return _boolean_compare(
        expr,
        operators.match_op,
        coercions.expect(
            roles.BinaryElementRole,
            other,
            expr=expr,
            operator=operators.match_op,
        ),
        result_type=type_api.MATCHTYPE,
        negate=operators.notmatch_op
        if op is operators.match_op
        else operators.match_op,
        **kw
    )


def _distinct_impl(expr, op, **kw):
    """See :meth:`.ColumnOperators.distinct`."""
    return UnaryExpression(
        expr, operator=operators.distinct_op, type_=expr.type
    )


def _between_impl(expr, op, cleft, cright, **kw):
    """See :meth:`.ColumnOperators.between`."""
    return BinaryExpression(
        expr,
        ClauseList(
            coercions.expect(
                roles.BinaryElementRole,
                cleft,
                expr=expr,
                operator=operators.and_,
            ),
            coercions.expect(
                roles.BinaryElementRole,
                cright,
                expr=expr,
                operator=operators.and_,
            ),
            operator=operators.and_,
            group=False,
            group_contents=False,
        ),
        op,
        negate=operators.notbetween_op
        if op is operators.between_op
        else operators.between_op,
        modifiers=kw,
    )


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
    "custom_op": (_custom_op_operate,),
    "json_path_getitem_op": (_binary_operate,),
    "json_getitem_op": (_binary_operate,),
    "concat_op": (_binary_operate,),
    "any_op": (_scalar, CollectionAggregate._create_any),
    "all_op": (_scalar, CollectionAggregate._create_all),
    "lt": (_boolean_compare, operators.ge),
    "le": (_boolean_compare, operators.gt),
    "ne": (_boolean_compare, operators.eq),
    "gt": (_boolean_compare, operators.le),
    "ge": (_boolean_compare, operators.lt),
    "eq": (_boolean_compare, operators.ne),
    "is_distinct_from": (_boolean_compare, operators.isnot_distinct_from),
    "isnot_distinct_from": (_boolean_compare, operators.is_distinct_from),
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
    "between_op": (_between_impl,),
    "notbetween_op": (_between_impl,),
    "neg": (_neg_impl,),
    "getitem": (_getitem_impl,),
    "lshift": (_unsupported_impl,),
    "rshift": (_unsupported_impl,),
    "contains": (_unsupported_impl,),
}
