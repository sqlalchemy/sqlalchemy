# Copyright (C) 2013-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
# mypy: ignore-errors

from __future__ import annotations

import dataclasses
from datetime import date
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Generic
from typing import Optional
from typing import TypeVar
from typing import Union

from ... import types as sqltypes
from ...util import py310
from ...util.typing import Literal

_T = TypeVar("_T", bound=Any)


if py310:
    dc_slots = {"slots": True}
    dc_kwonly = {"kw_only": True}
else:
    dc_slots = {}
    dc_kwonly = {}


@dataclasses.dataclass(frozen=True, **dc_slots)
class Range(Generic[_T]):
    """Represent a PostgreSQL range.

    E.g.::

        r = Range(10, 50, bounds="()")

    The calling style is similar to that of psycopg and psycopg2, in part
    to allow easier migration from previous SQLAlchemy versions that used
    these objects directly.

    :param lower: Lower bound value, or None
    :param upper: Upper bound value, or None
    :param bounds: keyword-only, optional string value that is one of
     ``"()"``, ``"[)"``, ``"(]"``, ``"[]"``.  Defaults to ``"[)"``.
    :param empty: keyword-only, optional bool indicating this is an "empty"
     range

    .. versionadded:: 2.0

    """

    lower: Optional[_T] = None
    """the lower bound"""

    upper: Optional[_T] = None
    """the upper bound"""

    bounds: Literal["()", "[)", "(]", "[]"] = dataclasses.field(
        default="[)", **dc_kwonly
    )
    empty: bool = dataclasses.field(default=False, **dc_kwonly)

    if not py310:

        def __init__(
            self, lower=None, upper=None, *, bounds="[)", empty=False
        ):
            # no __slots__ either so we can update dict
            self.__dict__.update(
                {
                    "lower": lower,
                    "upper": upper,
                    "bounds": bounds,
                    "empty": empty,
                }
            )

    def __bool__(self) -> bool:
        return self.empty

    def _contains_value(self, value: _T) -> bool:
        "Check whether this range contains the given `value`."

        if self.empty:
            return False

        if self.lower is None:
            return self.upper is None or (
                value < self.upper
                if self.bounds[1] == ")"
                else value <= self.upper
            )

        if self.upper is None:
            return (
                value > self.lower
                if self.bounds[0] == "("
                else value >= self.lower
            )

        return (
            value > self.lower
            if self.bounds[0] == "("
            else value >= self.lower
        ) and (
            value < self.upper
            if self.bounds[1] == ")"
            else value <= self.upper
        )

    def _get_discrete_step(self):
        "Determine the “step” for this range, if it is a discrete one."

        # See
        # https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-DISCRETE
        # for the rationale

        if isinstance(self.lower, int) or isinstance(self.upper, int):
            return 1
        elif isinstance(self.lower, datetime) or isinstance(
            self.upper, datetime
        ):
            # This is required, because a `isinstance(datetime.now(), date)`
            # is True
            return None
        elif isinstance(self.lower, date) or isinstance(self.upper, date):
            return timedelta(days=1)
        else:
            return None

    def _compare_edges(
        self,
        value1: Optional[_T],
        bound1: str,
        value2: Optional[_T],
        bound2: str,
        only_values: bool = False,
    ) -> int:
        """Compare two range bounds.

        Return -1, 0 or 1 respectively when `value1` is less than,
        equal to or greater than `value2`.

        When `only_value` is ``True``, do not consider the *inclusivity*
        of the edges, just their values.
        """

        value1_is_lower_bound = bound1 in {"[", "("}
        value2_is_lower_bound = bound2 in {"[", "("}

        # Infinite edges are equal when they are on the same side,
        # otherwise a lower edge is considered less than the upper end
        if value1 is value2 is None:
            if value1_is_lower_bound == value2_is_lower_bound:
                return 0
            else:
                return -1 if value1_is_lower_bound else 1
        elif value1 is None:
            return -1 if value1_is_lower_bound else 1
        elif value2 is None:
            return 1 if value2_is_lower_bound else -1

        value1_inc = bound1 in {"[", "]"}
        value2_inc = bound2 in {"[", "]"}
        step = self._get_discrete_step()

        if step is not None:
            # "Normalize" the two edges as '[)', to simplify successive logic
            if value1_is_lower_bound:
                if not value1_inc:
                    value1 -= step
                    value1_inc = True
            else:
                if value1_inc:
                    value1 += step
                    value1_inc = False
            if value2_is_lower_bound:
                if not value2_inc:
                    value2 -= step
                    value2_inc = True
            else:
                if value2_inc:
                    value2 += step
                    value2_inc = False

        if value1 < value2:
            return -1
        elif value1 > value2:
            return 1
        elif only_values:
            return 0
        else:
            # Neither one is infinite but are equal, so we
            # need to consider the respective inclusive/exclusive
            # flag

            if value1_inc and value2_inc:
                return 0
            elif not value1_inc and not value2_inc:
                if value1_is_lower_bound == value2_is_lower_bound:
                    return 0
                else:
                    return 1 if value1_is_lower_bound else -1
            elif not value1_inc:
                return 1 if value1_is_lower_bound else -1
            elif not value2_inc:
                return -1 if value2_is_lower_bound else 1
            else:
                return 0

    def contained_by(self, other: Range) -> bool:
        "Determine whether this range is a contained by `other`."

        # Any range contains the empty one
        if self.empty:
            return True

        # An empty range does not contain any range except the empty one
        if other.empty:
            return False

        slower = self.lower
        slower_b = self.bounds[0]
        olower = other.lower
        olower_b = other.bounds[0]

        if self._compare_edges(slower, slower_b, olower, olower_b) < 0:
            return False

        supper = self.upper
        supper_b = self.bounds[1]
        oupper = other.upper
        oupper_b = other.bounds[1]

        if self._compare_edges(supper, supper_b, oupper, oupper_b) > 0:
            return False

        return True

    def contains(self, value: Union[_T, Range]) -> bool:
        "Determine whether this range contains `value`."

        if isinstance(value, Range):
            return value.contained_by(self)
        else:
            return self._contains_value(value)

    def overlaps(self, other):
        """Boolean expression. Returns true if the column overlaps
        (has points in common with) the right hand operand.
        """
        raise NotImplementedError("not yet implemented")

    def strictly_left_of(self, other):
        """Boolean expression. Returns true if the column is strictly
        left of the right hand operand.
        """
        raise NotImplementedError("not yet implemented")

    __lshift__ = strictly_left_of

    def strictly_right_of(self, other):
        """Boolean expression. Returns true if the column is strictly
        right of the right hand operand.
        """
        raise NotImplementedError("not yet implemented")

    __rshift__ = strictly_right_of

    def not_extend_right_of(self, other):
        """Boolean expression. Returns true if the range in the column
        does not extend right of the range in the operand.
        """
        raise NotImplementedError("not yet implemented")

    def not_extend_left_of(self, other):
        """Boolean expression. Returns true if the range in the column
        does not extend left of the range in the operand.
        """
        raise NotImplementedError("not yet implemented")

    def adjacent_to(self, other):
        """Boolean expression. Returns true if the range in the column
        is adjacent to the range in the operand.
        """
        raise NotImplementedError("not yet implemented")

    def __add__(self, other):
        """Range expression. Returns the union of the two ranges.
        Will raise an exception if the resulting range is not
        contiguous.
        """
        raise NotImplementedError("not yet implemented")

    def __str__(self):
        return self._stringify()

    def _stringify(self):
        if self.empty:
            return "empty"

        l, r = self.lower, self.upper
        l = "" if l is None else l
        r = "" if r is None else r

        b0, b1 = self.bounds

        return f"{b0}{l},{r}{b1}"


class AbstractRange(sqltypes.TypeEngine):
    """
    Base for PostgreSQL RANGE types.

    .. seealso::

        `PostgreSQL range functions <https://www.postgresql.org/docs/current/static/functions-range.html>`_

    """  # noqa: E501

    render_bind_cast = True

    __abstract__ = True

    def adapt(self, impltype):
        """dynamically adapt a range type to an abstract impl.

        For example ``INT4RANGE().adapt(_Psycopg2NumericRange)`` should
        produce a type that will have ``_Psycopg2NumericRange`` behaviors
        and also render as ``INT4RANGE`` in SQL and DDL.

        """
        if issubclass(impltype, AbstractRangeImpl):
            # two ways to do this are:  1. create a new type on the fly
            # or 2. have AbstractRangeImpl(visit_name) constructor and a
            # visit_abstract_range_impl() method in the PG compiler.
            # I'm choosing #1 as the resulting type object
            # will then make use of the same mechanics
            # as if we had made all these sub-types explicitly, and will
            # also look more obvious under pdb etc.
            # The adapt() operation here is cached per type-class-per-dialect,
            # so is not much of a performance concern
            visit_name = self.__visit_name__
            return type(
                f"{visit_name}RangeImpl",
                (impltype, self.__class__),
                {"__visit_name__": visit_name},
            )()
        else:
            return super().adapt(impltype)

    class comparator_factory(sqltypes.Concatenable.Comparator):
        """Define comparison operations for range types."""

        def __ne__(self, other):
            "Boolean expression. Returns true if two ranges are not equal"
            if other is None:
                return super().__ne__(other)
            else:
                return self.expr.op("<>", is_comparison=True)(other)

        def contains(self, other, **kw):
            """Boolean expression. Returns true if the right hand operand,
            which can be an element or a range, is contained within the
            column.

            kwargs may be ignored by this operator but are required for API
            conformance.
            """
            return self.expr.op("@>", is_comparison=True)(other)

        def contained_by(self, other):
            """Boolean expression. Returns true if the column is contained
            within the right hand operand.
            """
            return self.expr.op("<@", is_comparison=True)(other)

        def overlaps(self, other):
            """Boolean expression. Returns true if the column overlaps
            (has points in common with) the right hand operand.
            """
            return self.expr.op("&&", is_comparison=True)(other)

        def strictly_left_of(self, other):
            """Boolean expression. Returns true if the column is strictly
            left of the right hand operand.
            """
            return self.expr.op("<<", is_comparison=True)(other)

        __lshift__ = strictly_left_of

        def strictly_right_of(self, other):
            """Boolean expression. Returns true if the column is strictly
            right of the right hand operand.
            """
            return self.expr.op(">>", is_comparison=True)(other)

        __rshift__ = strictly_right_of

        def not_extend_right_of(self, other):
            """Boolean expression. Returns true if the range in the column
            does not extend right of the range in the operand.
            """
            return self.expr.op("&<", is_comparison=True)(other)

        def not_extend_left_of(self, other):
            """Boolean expression. Returns true if the range in the column
            does not extend left of the range in the operand.
            """
            return self.expr.op("&>", is_comparison=True)(other)

        def adjacent_to(self, other):
            """Boolean expression. Returns true if the range in the column
            is adjacent to the range in the operand.
            """
            return self.expr.op("-|-", is_comparison=True)(other)

        def __add__(self, other):
            """Range expression. Returns the union of the two ranges.
            Will raise an exception if the resulting range is not
            contiguous.
            """
            return self.expr.op("+")(other)


class AbstractRangeImpl(AbstractRange):
    """marker for AbstractRange that will apply a subclass-specific
    adaptation"""


class AbstractMultiRange(AbstractRange):
    """base for PostgreSQL MULTIRANGE types"""

    __abstract__ = True


class AbstractMultiRangeImpl(AbstractRangeImpl, AbstractMultiRange):
    """marker for AbstractRange that will apply a subclass-specific
    adaptation"""


class INT4RANGE(AbstractRange):
    """Represent the PostgreSQL INT4RANGE type."""

    __visit_name__ = "INT4RANGE"


class INT8RANGE(AbstractRange):
    """Represent the PostgreSQL INT8RANGE type."""

    __visit_name__ = "INT8RANGE"


class NUMRANGE(AbstractRange):
    """Represent the PostgreSQL NUMRANGE type."""

    __visit_name__ = "NUMRANGE"


class DATERANGE(AbstractRange):
    """Represent the PostgreSQL DATERANGE type."""

    __visit_name__ = "DATERANGE"


class TSRANGE(AbstractRange):
    """Represent the PostgreSQL TSRANGE type."""

    __visit_name__ = "TSRANGE"


class TSTZRANGE(AbstractRange):
    """Represent the PostgreSQL TSTZRANGE type."""

    __visit_name__ = "TSTZRANGE"


class INT4MULTIRANGE(AbstractMultiRange):
    """Represent the PostgreSQL INT4MULTIRANGE type."""

    __visit_name__ = "INT4MULTIRANGE"


class INT8MULTIRANGE(AbstractMultiRange):
    """Represent the PostgreSQL INT8MULTIRANGE type."""

    __visit_name__ = "INT8MULTIRANGE"


class NUMMULTIRANGE(AbstractMultiRange):
    """Represent the PostgreSQL NUMMULTIRANGE type."""

    __visit_name__ = "NUMMULTIRANGE"


class DATEMULTIRANGE(AbstractMultiRange):
    """Represent the PostgreSQL DATEMULTIRANGE type."""

    __visit_name__ = "DATEMULTIRANGE"


class TSMULTIRANGE(AbstractMultiRange):
    """Represent the PostgreSQL TSRANGE type."""

    __visit_name__ = "TSMULTIRANGE"


class TSTZMULTIRANGE(AbstractMultiRange):
    """Represent the PostgreSQL TSTZRANGE type."""

    __visit_name__ = "TSTZMULTIRANGE"
