from collections import deque

from . import operators
from .. import util


SKIP_TRAVERSE = util.symbol("skip_traverse")


def compare(obj1, obj2, **kw):
    if kw.get("use_proxies", False):
        strategy = ColIdentityComparatorStrategy()
    else:
        strategy = StructureComparatorStrategy()

    return strategy.compare(obj1, obj2, **kw)


class StructureComparatorStrategy(object):
    __slots__ = "compare_stack", "cache"

    def __init__(self):
        self.compare_stack = deque()
        self.cache = set()

    def compare(self, obj1, obj2, **kw):
        stack = self.compare_stack
        cache = self.cache

        stack.append((obj1, obj2))

        while stack:
            left, right = stack.popleft()

            if left is right:
                continue
            elif left is None or right is None:
                # we know they are different so no match
                return False
            elif (left, right) in cache:
                continue
            cache.add((left, right))

            visit_name = left.__visit_name__

            # we're not exactly looking for identical types, because
            # there are things like Column and AnnotatedColumn.  So the
            # visit_name has to at least match up
            if visit_name != right.__visit_name__:
                return False

            meth = getattr(self, "compare_%s" % visit_name, None)

            if meth:
                comparison = meth(left, right, **kw)
                if comparison is False:
                    return False
                elif comparison is SKIP_TRAVERSE:
                    continue

            for c1, c2 in util.zip_longest(
                left.get_children(column_collections=False),
                right.get_children(column_collections=False),
                fillvalue=None,
            ):
                if c1 is None or c2 is None:
                    # collections are different sizes, comparison fails
                    return False
                stack.append((c1, c2))

        return True

    def compare_inner(self, obj1, obj2, **kw):
        stack = self.compare_stack
        try:
            self.compare_stack = deque()
            return self.compare(obj1, obj2, **kw)
        finally:
            self.compare_stack = stack

    def _compare_unordered_sequences(self, seq1, seq2, **kw):
        if seq1 is None:
            return seq2 is None

        completed = set()
        for clause in seq1:
            for other_clause in set(seq2).difference(completed):
                if self.compare_inner(clause, other_clause, **kw):
                    completed.add(other_clause)
                    break
        return len(completed) == len(seq1) == len(seq2)

    def compare_bindparam(self, left, right, **kw):
        # note the ".key" is often generated from id(self) so can't
        # be compared, as far as determining structure.
        return (
            left.type._compare_type_affinity(right.type)
            and left.value == right.value
            and left.callable == right.callable
            and left._orig_key == right._orig_key
        )

    def compare_clauselist(self, left, right, **kw):
        if left.operator is right.operator:
            if operators.is_associative(left.operator):
                if self._compare_unordered_sequences(
                    left.clauses, right.clauses
                ):
                    return SKIP_TRAVERSE
                else:
                    return False
            else:
                # normal ordered traversal
                return True
        else:
            return False

    def compare_unary(self, left, right, **kw):
        if left.operator:
            disp = self._get_operator_dispatch(
                left.operator, "unary", "operator"
            )
            if disp is not None:
                result = disp(left, right, left.operator, **kw)
                if result is not True:
                    return result
        elif left.modifier:
            disp = self._get_operator_dispatch(
                left.modifier, "unary", "modifier"
            )
            if disp is not None:
                result = disp(left, right, left.operator, **kw)
                if result is not True:
                    return result
        return (
            left.operator == right.operator and left.modifier == right.modifier
        )

    def compare_binary(self, left, right, **kw):
        disp = self._get_operator_dispatch(left.operator, "binary", None)
        if disp:
            result = disp(left, right, left.operator, **kw)
            if result is not True:
                return result

        if left.operator == right.operator:
            if operators.is_commutative(left.operator):
                if (
                    compare(left.left, right.left, **kw)
                    and compare(left.right, right.right, **kw)
                ) or (
                    compare(left.left, right.right, **kw)
                    and compare(left.right, right.left, **kw)
                ):
                    return SKIP_TRAVERSE
                else:
                    return False
            else:
                return True
        else:
            return False

    def _get_operator_dispatch(self, operator_, qualifier1, qualifier2):
        # used by compare_binary, compare_unary
        attrname = "visit_%s_%s%s" % (
            operator_.__name__,
            qualifier1,
            "_" + qualifier2 if qualifier2 else "",
        )
        return getattr(self, attrname, None)

    def visit_function_as_comparison_op_binary(
        self, left, right, operator, **kw
    ):
        return (
            left.left_index == right.left_index
            and left.right_index == right.right_index
        )

    def compare_function(self, left, right, **kw):
        return left.name == right.name

    def compare_column(self, left, right, **kw):
        if left.table is not None:
            self.compare_stack.appendleft((left.table, right.table))
        return (
            left.key == right.key
            and left.name == right.name
            and (
                left.type._compare_type_affinity(right.type)
                if left.type is not None
                else right.type is None
            )
            and left.is_literal == right.is_literal
        )

    def compare_collation(self, left, right, **kw):
        return left.collation == right.collation

    def compare_type_coerce(self, left, right, **kw):
        return left.type._compare_type_affinity(right.type)

    @util.dependencies("sqlalchemy.sql.elements")
    def compare_alias(self, elements, left, right, **kw):
        return (
            left.name == right.name
            if not isinstance(left.name, elements._anonymous_label)
            else isinstance(right.name, elements._anonymous_label)
        )

    def compare_cte(self, elements, left, right, **kw):
        raise NotImplementedError("TODO")

    def compare_extract(self, left, right, **kw):
        return left.field == right.field

    def compare_textual_label_reference(self, left, right, **kw):
        return left.element == right.element

    def compare_slice(self, left, right, **kw):
        return (
            left.start == right.start
            and left.stop == right.stop
            and left.step == right.step
        )

    def compare_over(self, left, right, **kw):
        return left.range_ == right.range_ and left.rows == right.rows

    @util.dependencies("sqlalchemy.sql.elements")
    def compare_label(self, elements, left, right, **kw):
        return left._type._compare_type_affinity(right._type) and (
            left.name == right.name
            if not isinstance(left.name, elements._anonymous_label)
            else isinstance(right.name, elements._anonymous_label)
        )

    def compare_typeclause(self, left, right, **kw):
        return left.type._compare_type_affinity(right.type)

    def compare_join(self, left, right, **kw):
        return left.isouter == right.isouter and left.full == right.full

    def compare_table(self, left, right, **kw):
        if left.name != right.name:
            return False

        self.compare_stack.extendleft(
            util.zip_longest(left.columns, right.columns)
        )

    def compare_compound_select(self, left, right, **kw):

        if not self._compare_unordered_sequences(
            left.selects, right.selects, **kw
        ):
            return False

        if left.keyword != right.keyword:
            return False

        if left._for_update_arg != right._for_update_arg:
            return False

        if not self.compare_inner(
            left._order_by_clause, right._order_by_clause, **kw
        ):
            return False

        if not self.compare_inner(
            left._group_by_clause, right._group_by_clause, **kw
        ):
            return False

        return SKIP_TRAVERSE

    def compare_select(self, left, right, **kw):
        if not self._compare_unordered_sequences(
            left._correlate, right._correlate
        ):
            return False
        if not self._compare_unordered_sequences(
            left._correlate_except, right._correlate_except
        ):
            return False

        if not self._compare_unordered_sequences(
            left._from_obj, right._from_obj
        ):
            return False

        if left._for_update_arg != right._for_update_arg:
            return False

        return True

    def compare_textual_select(self, left, right, **kw):
        self.compare_stack.extendleft(
            util.zip_longest(left.column_args, right.column_args)
        )
        return left.positional == right.positional


class ColIdentityComparatorStrategy(StructureComparatorStrategy):
    def compare_column_element(
        self, left, right, use_proxies=True, equivalents=(), **kw
    ):
        """Compare ColumnElements using proxies and equivalent collections.

        This is a comparison strategy specific to the ORM.
        """

        to_compare = (right,)
        if equivalents and right in equivalents:
            to_compare = equivalents[right].union(to_compare)

        for oth in to_compare:
            if use_proxies and left.shares_lineage(oth):
                return True
            elif hash(left) == hash(right):
                return True
        else:
            return False

    def compare_column(self, left, right, **kw):
        return self.compare_column_element(left, right, **kw)

    def compare_label(self, left, right, **kw):
        return self.compare_column_element(left, right, **kw)

    def compare_table(self, left, right, **kw):
        # tables compare on identity, since it's not really feasible to
        # compare them column by column with the above rules
        return left is right
