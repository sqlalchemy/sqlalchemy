# sql/coercions.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import numbers
import re

from . import operators
from . import roles
from . import visitors
from .visitors import Visitable
from .. import exc
from .. import inspection
from .. import util
from ..util import collections_abc

if util.TYPE_CHECKING:
    from types import ModuleType

elements = None  # type: ModuleType
schema = None  # type: ModuleType
selectable = None  # type: ModuleType
sqltypes = None  # type: ModuleType


def _is_literal(element):
    """Return whether or not the element is a "literal" in the context
    of a SQL expression construct.

    """
    return not isinstance(
        element, (Visitable, schema.SchemaEventTarget)
    ) and not hasattr(element, "__clause_element__")


def _document_text_coercion(paramname, meth_rst, param_rst):
    return util.add_parameter_text(
        paramname,
        (
            ".. warning:: "
            "The %s argument to %s can be passed as a Python string argument, "
            "which will be treated "
            "as **trusted SQL text** and rendered as given.  **DO NOT PASS "
            "UNTRUSTED INPUT TO THIS PARAMETER**."
        )
        % (param_rst, meth_rst),
    )


def expect(role, element, **kw):
    # major case is that we are given a ClauseElement already, skip more
    # elaborate logic up front if possible
    impl = _impl_lookup[role]

    if not isinstance(element, (elements.ClauseElement, schema.SchemaItem)):
        resolved = impl._resolve_for_clause_element(element, **kw)
    else:
        resolved = element

    if impl._role_class in resolved.__class__.__mro__:
        if impl._post_coercion:
            resolved = impl._post_coercion(resolved, **kw)
        return resolved
    else:
        return impl._implicit_coercions(element, resolved, **kw)


def expect_as_key(role, element, **kw):
    kw["as_key"] = True
    return expect(role, element, **kw)


def expect_col_expression_collection(role, expressions):
    for expr in expressions:
        strname = None
        column = None

        resolved = expect(role, expr)
        if isinstance(resolved, util.string_types):
            strname = resolved = expr
        else:
            cols = []
            visitors.traverse(resolved, {}, {"column": cols.append})
            if cols:
                column = cols[0]
        add_element = column if column is not None else strname
        yield resolved, column, strname, add_element


class RoleImpl(object):
    __slots__ = ("_role_class", "name", "_use_inspection")

    def _literal_coercion(self, element, **kw):
        raise NotImplementedError()

    _post_coercion = None

    def __init__(self, role_class):
        self._role_class = role_class
        self.name = role_class._role_name
        self._use_inspection = issubclass(role_class, roles.UsesInspection)

    def _resolve_for_clause_element(self, element, argname=None, **kw):
        original_element = element
        is_clause_element = hasattr(element, "__clause_element__")

        if is_clause_element:
            while not isinstance(
                element, (elements.ClauseElement, schema.SchemaItem)
            ):
                try:
                    element = element.__clause_element__()
                except AttributeError:
                    break

        if not is_clause_element:
            if self._use_inspection:
                insp = inspection.inspect(element, raiseerr=False)
                if insp is not None:
                    try:
                        return insp.__clause_element__()
                    except AttributeError:
                        self._raise_for_expected(original_element, argname)

            return self._literal_coercion(element, argname=argname, **kw)
        else:
            return element

    def _implicit_coercions(self, element, resolved, argname=None, **kw):
        self._raise_for_expected(element, argname, resolved)

    def _raise_for_expected(
        self,
        element,
        argname=None,
        resolved=None,
        advice=None,
        code=None,
        err=None,
    ):
        if argname:
            msg = "%s expected for argument %r; got %r." % (
                self.name,
                argname,
                element,
            )
        else:
            msg = "%s expected, got %r." % (self.name, element)

        if advice:
            msg += " " + advice

        util.raise_(exc.ArgumentError(msg, code=code), replace_context=err)


class _Deannotate(object):
    def _post_coercion(self, resolved, **kw):
        from .util import _deep_deannotate

        return _deep_deannotate(resolved)


class _StringOnly(object):
    def _resolve_for_clause_element(self, element, argname=None, **kw):
        return self._literal_coercion(element, **kw)


class _ReturnsStringKey(object):
    def _implicit_coercions(
        self, original_element, resolved, argname=None, **kw
    ):
        if isinstance(original_element, util.string_types):
            return original_element
        else:
            self._raise_for_expected(original_element, argname, resolved)

    def _literal_coercion(self, element, **kw):
        return element


class _ColumnCoercions(object):
    def _warn_for_scalar_subquery_coercion(self):
        util.warn_deprecated(
            "coercing SELECT object to scalar subquery in a "
            "column-expression context is deprecated in version 1.4; "
            "please use the .scalar_subquery() method to produce a scalar "
            "subquery.  This automatic coercion will be removed in a "
            "future release."
        )

    def _implicit_coercions(
        self, original_element, resolved, argname=None, **kw
    ):
        if resolved._is_select_statement:
            self._warn_for_scalar_subquery_coercion()
            return resolved.scalar_subquery()
        elif resolved._is_from_clause and isinstance(
            resolved, selectable.Subquery
        ):
            self._warn_for_scalar_subquery_coercion()
            return resolved.element.scalar_subquery()
        else:
            self._raise_for_expected(original_element, argname, resolved)


def _no_text_coercion(
    element, argname=None, exc_cls=exc.ArgumentError, extra=None, err=None
):
    util.raise_(
        exc_cls(
            "%(extra)sTextual SQL expression %(expr)r %(argname)sshould be "
            "explicitly declared as text(%(expr)r)"
            % {
                "expr": util.ellipses_string(element),
                "argname": "for argument %s" % (argname,) if argname else "",
                "extra": "%s " % extra if extra else "",
            }
        ),
        replace_context=err,
    )


class _NoTextCoercion(object):
    def _literal_coercion(self, element, argname=None, **kw):
        if isinstance(element, util.string_types) and issubclass(
            elements.TextClause, self._role_class
        ):
            _no_text_coercion(element, argname)
        else:
            self._raise_for_expected(element, argname)


class _CoerceLiterals(object):
    _coerce_consts = False
    _coerce_star = False
    _coerce_numerics = False

    def _text_coercion(self, element, argname=None):
        return _no_text_coercion(element, argname)

    def _literal_coercion(self, element, argname=None, **kw):
        if isinstance(element, util.string_types):
            if self._coerce_star and element == "*":
                return elements.ColumnClause("*", is_literal=True)
            else:
                return self._text_coercion(element, argname)

        if self._coerce_consts:
            if element is None:
                return elements.Null()
            elif element is False:
                return elements.False_()
            elif element is True:
                return elements.True_()

        if self._coerce_numerics and isinstance(element, (numbers.Number)):
            return elements.ColumnClause(str(element), is_literal=True)

        self._raise_for_expected(element, argname)


class _SelectIsNotFrom(object):
    def _raise_for_expected(self, element, argname=None, resolved=None, **kw):
        if isinstance(element, roles.SelectStatementRole) or isinstance(
            resolved, roles.SelectStatementRole
        ):
            advice = (
                "To create a "
                "FROM clause from a %s object, use the .subquery() method."
                % (element.__class__)
            )
            code = "89ve"
        else:
            advice = code = None

        return super(_SelectIsNotFrom, self)._raise_for_expected(
            element,
            argname=argname,
            resolved=resolved,
            advice=advice,
            code=code,
            **kw
        )


class ExpressionElementImpl(
    _ColumnCoercions, RoleImpl, roles.ExpressionElementRole
):
    def _literal_coercion(
        self, element, name=None, type_=None, argname=None, **kw
    ):
        if element is None:
            return elements.Null()
        else:
            try:
                return elements.BindParameter(
                    name, element, type_, unique=True
                )
            except exc.ArgumentError as err:
                self._raise_for_expected(element, err=err)


class BinaryElementImpl(
    ExpressionElementImpl, RoleImpl, roles.BinaryElementRole
):
    def _literal_coercion(
        self, element, expr, operator, bindparam_type=None, argname=None, **kw
    ):
        try:
            return expr._bind_param(operator, element, type_=bindparam_type)
        except exc.ArgumentError as err:
            self._raise_for_expected(element, err=err)

    def _post_coercion(self, resolved, expr, **kw):
        if (
            isinstance(resolved, (elements.Grouping, elements.BindParameter))
            and resolved.type._isnull
            and not expr.type._isnull
        ):
            resolved = resolved._with_binary_element_type(expr.type)
        return resolved


class InElementImpl(RoleImpl, roles.InElementRole):
    def _implicit_coercions(
        self, original_element, resolved, argname=None, **kw
    ):
        if resolved._is_from_clause:
            if (
                isinstance(resolved, selectable.Alias)
                and resolved.element._is_select_statement
            ):
                return resolved.element
            else:
                return resolved.select()
        else:
            self._raise_for_expected(original_element, argname, resolved)

    def _literal_coercion(self, element, expr, operator, **kw):
        if isinstance(element, collections_abc.Iterable) and not isinstance(
            element, util.string_types
        ):
            non_literal_expressions = {}
            element = list(element)
            for o in element:
                if not _is_literal(o):
                    if not isinstance(o, operators.ColumnOperators):
                        self._raise_for_expected(element, **kw)
                    else:
                        non_literal_expressions[o] = o
                elif o is None:
                    non_literal_expressions[o] = elements.Null()

            if non_literal_expressions:
                return elements.ClauseList(
                    _tuple_values=isinstance(expr, elements.Tuple),
                    *[
                        non_literal_expressions[o]
                        if o in non_literal_expressions
                        else expr._bind_param(operator, o)
                        for o in element
                    ]
                )
            else:
                return expr._bind_param(operator, element, expanding=True)

        else:
            self._raise_for_expected(element, **kw)

    def _post_coercion(self, element, expr, operator, **kw):
        if element._is_select_statement:
            return element.scalar_subquery()
        elif isinstance(element, elements.ClauseList):
            assert not len(element.clauses) == 0
            return element.self_group(against=operator)

        elif isinstance(element, elements.BindParameter) and element.expanding:
            if isinstance(expr, elements.Tuple):
                element = element._with_expanding_in_types(
                    [elem.type for elem in expr]
                )

            return element
        else:
            return element


class WhereHavingImpl(
    _CoerceLiterals, _ColumnCoercions, RoleImpl, roles.WhereHavingRole
):

    _coerce_consts = True

    def _text_coercion(self, element, argname=None):
        return _no_text_coercion(element, argname)


class StatementOptionImpl(
    _CoerceLiterals, RoleImpl, roles.StatementOptionRole
):

    _coerce_consts = True

    def _text_coercion(self, element, argname=None):
        return elements.TextClause(element)


class ColumnArgumentImpl(_NoTextCoercion, RoleImpl, roles.ColumnArgumentRole):
    pass


class ColumnArgumentOrKeyImpl(
    _ReturnsStringKey, RoleImpl, roles.ColumnArgumentOrKeyRole
):
    pass


class ByOfImpl(_CoerceLiterals, _ColumnCoercions, RoleImpl, roles.ByOfRole):

    _coerce_consts = True

    def _text_coercion(self, element, argname=None):
        return elements._textual_label_reference(element)


class OrderByImpl(ByOfImpl, RoleImpl, roles.OrderByRole):
    def _post_coercion(self, resolved):
        if (
            isinstance(resolved, self._role_class)
            and resolved._order_by_label_element is not None
        ):
            return elements._label_reference(resolved)
        else:
            return resolved


class DMLColumnImpl(_ReturnsStringKey, RoleImpl, roles.DMLColumnRole):
    def _post_coercion(self, element, as_key=False):
        if as_key:
            return element.key
        else:
            return element


class ConstExprImpl(RoleImpl, roles.ConstExprRole):
    def _literal_coercion(self, element, argname=None, **kw):
        if element is None:
            return elements.Null()
        elif element is False:
            return elements.False_()
        elif element is True:
            return elements.True_()
        else:
            self._raise_for_expected(element, argname)


class TruncatedLabelImpl(_StringOnly, RoleImpl, roles.TruncatedLabelRole):
    def _implicit_coercions(
        self, original_element, resolved, argname=None, **kw
    ):
        if isinstance(original_element, util.string_types):
            return resolved
        else:
            self._raise_for_expected(original_element, argname, resolved)

    def _literal_coercion(self, element, argname=None, **kw):
        """coerce the given value to :class:`._truncated_label`.

        Existing :class:`._truncated_label` and
        :class:`._anonymous_label` objects are passed
        unchanged.
        """

        if isinstance(element, elements._truncated_label):
            return element
        else:
            return elements._truncated_label(element)


class DDLExpressionImpl(
    _Deannotate, _CoerceLiterals, RoleImpl, roles.DDLExpressionRole
):

    _coerce_consts = True

    def _text_coercion(self, element, argname=None):
        return elements.TextClause(element)


class DDLConstraintColumnImpl(
    _Deannotate, _ReturnsStringKey, RoleImpl, roles.DDLConstraintColumnRole
):
    pass


class DDLReferredColumnImpl(DDLConstraintColumnImpl):
    pass


class LimitOffsetImpl(RoleImpl, roles.LimitOffsetRole):
    def _implicit_coercions(self, element, resolved, argname=None, **kw):
        if resolved is None:
            return None
        else:
            self._raise_for_expected(element, argname, resolved)

    def _literal_coercion(self, element, name, type_, **kw):
        if element is None:
            return None
        else:
            value = util.asint(element)
            return selectable._OffsetLimitParam(
                name, value, type_=type_, unique=True
            )


class LabeledColumnExprImpl(
    ExpressionElementImpl, roles.LabeledColumnExprRole
):
    def _implicit_coercions(
        self, original_element, resolved, argname=None, **kw
    ):
        if isinstance(resolved, roles.ExpressionElementRole):
            return resolved.label(None)
        else:
            new = super(LabeledColumnExprImpl, self)._implicit_coercions(
                original_element, resolved, argname=argname, **kw
            )
            if isinstance(new, roles.ExpressionElementRole):
                return new.label(None)
            else:
                self._raise_for_expected(original_element, argname, resolved)


class ColumnsClauseImpl(
    _SelectIsNotFrom, _CoerceLiterals, RoleImpl, roles.ColumnsClauseRole
):

    _coerce_consts = True
    _coerce_numerics = True
    _coerce_star = True

    _guess_straight_column = re.compile(r"^\w\S*$", re.I)

    def _text_coercion(self, element, argname=None):
        element = str(element)

        guess_is_literal = not self._guess_straight_column.match(element)
        raise exc.ArgumentError(
            "Textual column expression %(column)r %(argname)sshould be "
            "explicitly declared with text(%(column)r), "
            "or use %(literal_column)s(%(column)r) "
            "for more specificity"
            % {
                "column": util.ellipses_string(element),
                "argname": "for argument %s" % (argname,) if argname else "",
                "literal_column": "literal_column"
                if guess_is_literal
                else "column",
            }
        )


class ReturnsRowsImpl(RoleImpl, roles.ReturnsRowsRole):
    pass


class StatementImpl(_NoTextCoercion, RoleImpl, roles.StatementRole):
    pass


class CoerceTextStatementImpl(_CoerceLiterals, RoleImpl, roles.StatementRole):
    def _text_coercion(self, element, argname=None):
        return elements.TextClause(element)


class SelectStatementImpl(
    _NoTextCoercion, RoleImpl, roles.SelectStatementRole
):
    def _implicit_coercions(
        self, original_element, resolved, argname=None, **kw
    ):
        if resolved._is_text_clause:
            return resolved.columns()
        else:
            self._raise_for_expected(original_element, argname, resolved)


class HasCTEImpl(ReturnsRowsImpl, roles.HasCTERole):
    pass


class FromClauseImpl(
    _SelectIsNotFrom, _NoTextCoercion, RoleImpl, roles.FromClauseRole
):
    def _implicit_coercions(
        self,
        original_element,
        resolved,
        argname=None,
        explicit_subquery=False,
        allow_select=True,
        **kw
    ):
        if resolved._is_select_statement:
            if explicit_subquery:
                return resolved.subquery()
            elif allow_select:
                util.warn_deprecated(
                    "Implicit coercion of SELECT and textual SELECT "
                    "constructs into FROM clauses is deprecated; please call "
                    ".subquery() on any Core select or ORM Query object in "
                    "order to produce a subquery object."
                )
                return resolved._implicit_subquery
        elif resolved._is_text_clause:
            return resolved
        else:
            self._raise_for_expected(original_element, argname, resolved)


class StrictFromClauseImpl(FromClauseImpl, roles.StrictFromClauseRole):
    def _implicit_coercions(
        self,
        original_element,
        resolved,
        argname=None,
        allow_select=False,
        **kw
    ):
        if resolved._is_select_statement and allow_select:
            util.warn_deprecated(
                "Implicit coercion of SELECT and textual SELECT constructs "
                "into FROM clauses is deprecated; please call .subquery() "
                "on any Core select or ORM Query object in order to produce a "
                "subquery object."
            )
            return resolved._implicit_subquery
        else:
            self._raise_for_expected(original_element, argname, resolved)


class AnonymizedFromClauseImpl(
    StrictFromClauseImpl, roles.AnonymizedFromClauseRole
):
    def _post_coercion(self, element, flat=False, name=None, **kw):
        return element.alias(name=name, flat=flat)


class DMLSelectImpl(_NoTextCoercion, RoleImpl, roles.DMLSelectRole):
    def _implicit_coercions(
        self, original_element, resolved, argname=None, **kw
    ):
        if resolved._is_from_clause:
            if (
                isinstance(resolved, selectable.Alias)
                and resolved.element._is_select_statement
            ):
                return resolved.element
            else:
                return resolved.select()
        else:
            self._raise_for_expected(original_element, argname, resolved)


class CompoundElementImpl(
    _NoTextCoercion, RoleImpl, roles.CompoundElementRole
):
    def _raise_for_expected(self, element, argname=None, resolved=None, **kw):
        if isinstance(element, roles.FromClauseRole):
            if element._is_subquery:
                advice = (
                    "Use the plain select() object without "
                    "calling .subquery() or .alias()."
                )
            else:
                advice = (
                    "To SELECT from any FROM clause, use the .select() method."
                )
        else:
            advice = None
        return super(CompoundElementImpl, self)._raise_for_expected(
            element, argname=argname, resolved=resolved, advice=advice, **kw
        )


_impl_lookup = {}


for name in dir(roles):
    cls = getattr(roles, name)
    if name.endswith("Role"):
        name = name.replace("Role", "Impl")
        if name in globals():
            impl = globals()[name](cls)
            _impl_lookup[cls] = impl
